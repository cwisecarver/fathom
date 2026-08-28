defmodule Fathom.Shard.Replication.Shipper do
  @moduledoc """
  One connection to one follower **node** — the sending end of A2 replication.
  See `docs/a2-quorum-replication.md`.

  A shipper is per-node, not per-shard: every push names its shard and every reply names it back,
  so one socket carries every shard this node replicates to that follower. Per-shard sockets would
  be millions of connections at fathom's stated scale.

  ## Replies are messages, not call returns

  `push/2` does not block. A commit needs a **quorum** across several followers, so the caller has
  to wait on all of them at once and stop at the Q-th — which is impossible if each shipper is a
  blocking `GenServer.call`. Replies arrive as `{:repl_reply, shipper, result}` and
  `Fathom.Shard.Replication.ship_quorum/4` does the counting through
  `Fathom.Shard.Replication.Quorum`.

  This is also what makes a straggler cheap: gate 2 measured 2-of-4 at 1.6 ms against 4-of-4 at
  134 ms when two followers were 60 ms away. That win only exists if the primary can stop waiting,
  which means it must never be structurally blocked on the slowest follower.

  ## Correlating on shard id alone

  A shard has exactly one writer (the lease), so there is never more than one push in flight for it
  and the shard id is a sufficient correlation key. A second push for a shard already awaiting a
  reply is a bug in the caller, and is refused rather than silently replacing the waiter.

  ## Seeding streams; it does not interleave

  A seed is sent as `seed_begin` + N chunks + `seed_end`, which bounds MEMORY on both sides — a
  whole tenant database is never resident. It does **not** bound head-of-line blocking: one socket
  per follower *node* carries every shard, so a large seed still delays other shards' pushes on
  that link for its duration. Interleaving would need per-shard framing and a scheduler on top of
  this socket, and is deliberately not here. Do not read "chunked" as "concurrent".

  ## Disconnection is a rejection, not a crash

  If the socket drops, every in-flight waiter is told `:disconnected` immediately. A follower that
  has gone away must subtract from the quorum **now** — `Quorum` can then report `:impossible` as
  soon as too few remain, instead of every commit sitting on a timeout it could already prove will
  expire.
  """
  use GenServer

  require Logger

  alias Fathom.Shard.Replication.Budget
  alias Fathom.Shard.Replication.Protocol
  alias Fathom.Shard.Replication.Shipper.Writer

  @connect_timeout 5_000
  @reconnect_backoff_ms 500

  defstruct [:host, :port, :sock, :writer, :id, :name, waiters: %{}]

  # ------------------------------------------------------------------------------------------
  # api
  # ------------------------------------------------------------------------------------------

  # `fullsweep_after: 0`, for the same reason `Fathom.Shard` carries it (expert review 2026-07-24
  # #9) and with more force: this process handles WAL-frame PAYLOADS, and a binary over 64 bytes
  # lives off-heap, refcounted, freed only when the referencing process garbage-collects. A shipper's
  # own live set is tiny — a socket and a waiters map — so ERTS's default `fullsweep_after: 65535`
  # means it essentially never full-sweeps, and every frame it has ever forwarded stays reachable.
  #
  # MEASURED ON THE RIG, 2026-08-14, and this is why the option is here rather than a tidy-up: a
  # `tpc-fleet` sweep with replication on OOM-KILLED a node (fathom2, exit 137, `OOMKilled=true`)
  # about two minutes in, on a 94 GiB VM with no per-container limit. The survivors were carrying
  # 7–18 GiB each, of which `:erlang.memory()[:binary]` was 17,859 MB out of 18,034 MB on the worst
  # — against 71 MB of total PROCESS memory. Nothing was leaked: a full GC took that node from
  # 17,867 MB to 14 MB, and garbage-collecting ONLY the five shipper processes on another node
  # freed 8,105 MB → 3,995 MB. So the binaries were always releasable and simply never collected,
  # which is precisely what this flag fixes.
  #
  # No `max_heap_size`, deliberately, matching `Fathom.Shard`: killing a shipper at a heap limit
  # would fail every in-flight quorum wait on that peer rather than shed load.
  def start_link(opts) do
    GenServer.start_link(
      __MODULE__,
      opts,
      Keyword.take(opts, [:name]) ++ [spawn_opt: [fullsweep_after: 0]]
    )
  end

  @doc """
  Send a frame delta. The reply arrives as a message to the calling process:

      {:repl_reply, shipper_pid, {:ack, shard_id, next_offset}}
      {:repl_reply, shipper_pid, {:reject, shard_id, reason, expected_offset}}

  ## The node's byte budget is claimed HERE, in the caller

  `Fathom.Shard.Replication.Budget` is consulted before the payload is handed to a mailbox, and a
  refusal answers on the same reply channel without enqueueing anything. That placement IS the fix:
  `overloaded?/1` below checks a cap on DEQUEUE, which a burst outruns (one run reached a queue of
  12,828 against a cap of 8,192), and no dequeue-time check can bound a mailbox a cast can fill
  faster than the process drains it. The reservation is returned to the shipper in the cast so it
  can release exactly what was claimed.
  """
  @spec push(GenServer.server(), Protocol.Push.t()) :: :ok
  def push(shipper, %Protocol.Push{} = p) do
    case Budget.reserve(shipper, byte_size(p.payload)) do
      {:ok, reserved} ->
        GenServer.cast(shipper, {:push, p, self(), reserved})

      :rejected ->
        # Same answer, and for the same reason, as the `overloaded?` branch: a follower that cannot
        # take the work must subtract from the quorum NOW rather than absorb it. Reported as
        # `:overloaded` so the two paths are indistinguishable to `Quorum` and to the operator.
        count_reject(:overloaded)
        send(self(), {:repl_reply, shipper, {:reject, p.shard_id, :overloaded, 0}})
        :ok
    end
  end

  @doc """
  Open a streamed base copy. Registers the waiter for the WHOLE seed, not for this frame.

  A seed is many frames and exactly one reply, so the waiter is claimed here and released by the
  follower's answer to `seed_end/1` (or by a disconnect). Holding it across the stream is also what
  keeps the `already_in_flight` guard meaningful during a multi-second transfer: a `push/2` for the
  same shard mid-seed is refused rather than overwriting the seeder's waiter and stranding it.
  """
  @spec seed_begin(GenServer.server(), Protocol.SeedBegin.t()) :: :ok
  def seed_begin(shipper, %Protocol.SeedBegin{} = s),
    do: GenServer.cast(shipper, {:seed_begin, s, self()})

  @doc """
  Send one chunk. No reply, and no waiter bookkeeping — `seed_begin/2` already claimed it.

  A send failure here does not go unnoticed: it drops the socket, which fails every waiter
  including this seed's, so the sender learns immediately instead of waiting out its timeout.
  """
  @spec seed_chunk(GenServer.server(), String.t(), :db | :wal, non_neg_integer(), binary()) :: :ok
  def seed_chunk(shipper, shard_id, part, seq, bytes),
    do: GenServer.cast(shipper, {:seed_frame, shard_id, {:chunk, part, seq, bytes}})

  @doc "Commit the streamed seed. This is the frame the follower answers."
  @spec seed_end(GenServer.server(), String.t()) :: :ok
  def seed_end(shipper, shard_id),
    do: GenServer.cast(shipper, {:seed_frame, shard_id, :end})

  @doc "Abandon a partial seed — the follower drops it and answers, releasing the waiter."
  @spec seed_abort(GenServer.server(), String.t()) :: :ok
  def seed_abort(shipper, shard_id),
    do: GenServer.cast(shipper, {:seed_frame, shard_id, :abort})

  @doc "Whether the underlying socket is currently up. For tests and health reporting."
  @spec connected?(GenServer.server()) :: boolean()
  def connected?(shipper), do: GenServer.call(shipper, :connected?)

  # ------------------------------------------------------------------------------------------
  # server
  # ------------------------------------------------------------------------------------------

  @impl true
  def init(opts) do
    name = Keyword.get(opts, :name)

    # A FRESH counter per incarnation, and that is the point rather than initialisation hygiene.
    # A shipper killed with a full mailbox never runs the `Budget.release/2` calls its queued
    # messages owed; with one long-lived counter those bytes would be charged to the node forever
    # and replication would eventually refuse everything. Publishing a new ref here makes a
    # stranded count die with the process that stranded it.
    Budget.install(name)

    state = %__MODULE__{
      host: Keyword.fetch!(opts, :host),
      port: Keyword.fetch!(opts, :port),
      name: name,
      # THE IDENTITY EVERY REPLY CARRIES (expert review 2026-08-20 #24).
      #
      # A caller records what it expects back keyed by the term it used to ADDRESS this shipper;
      # every reply must therefore name that same term, or the expectation map and the answer key
      # on different things and a perfectly good ack is scored as a mismatch. `Fleet` passes
      # `id: name` because `Fleet.shippers/0` publishes names. The default is `self()`, which is
      # correct for anyone holding a bare pid.
      #
      # This replaced a `Process.whereis(name) || name` normalisation on the PRIMARY side, which
      # is wrong precisely when it matters: `whereis/1` returns `nil` for a shipper mid-restart
      # under the DynamicSupervisor or a node mid-`Membership` swap, so the expectation was filed
      # under the ATOM while the reply arrived under the PID. With N=3/Q=2 two such misses drive
      # `Quorum.settle/1` to `:impossible` and the tenant's write fails `FILO_NO_QUORUM` for no
      # reason, on a window that recurs on every restart and every roster swap.
      id: Keyword.get(opts, :id, self())
    }

    {:ok, state, {:continue, :connect}}
  end

  @impl true
  def handle_continue(:connect, state), do: {:noreply, connect(state)}

  @impl true
  def handle_call(:connected?, _from, state), do: {:reply, state.sock != nil, state}

  @impl true
  def handle_cast({:push, p, from, reserved}, %{sock: nil} = state) do
    # Not connected. Fail the push immediately rather than buffering: a queued frame delta is a
    # commit the tenant is still waiting on, and silently holding it converts a follower outage
    # into unbounded write latency.
    Budget.release(state.name, reserved)
    send(from, {:repl_reply, state.id, {:reject, p.shard_id, :disconnected, 0}})
    {:noreply, state}
  end

  def handle_cast({:push, p, from, reserved}, state) do
    # Released the moment the payload leaves the mailbox, on EVERY branch below — the budget bounds
    # what is queued, not what is in flight. Doing it here rather than per-branch is what keeps each
    # `Budget.reserve/2` matched by exactly one release.
    Budget.release(state.name, reserved)

    cond do
      # OVERLOADED: more work queued than this link can drain. Reject NOW, for exactly the reason
      # the `sock: nil` clause above rejects — a follower that cannot take the work must subtract
      # from the quorum immediately, not absorb it. `push/2` is a cast with no backpressure (it has
      # to be: a quorum waits on several followers at once and stops at the Q-th, so a blocking
      # call would structurally couple every commit to the slowest peer), and one socket per peer
      # node carries every shard — so if the aggregate write rate exceeds what that socket drains,
      # nothing bounds this mailbox.
      #
      # WHAT THIS BOUND IS FOR, AND WHY IT DOES NOT CLOSE THE 1024-TENANT OOM.
      #
      # The mailbox IS where the memory accumulates — that part is settled, by direct attribution
      # rather than inference. `deploy/chaos/bin_holders.sh` (which sums `Process.info(pid,
      # :binary)`, the payload `Process.info(pid, :memory)` omits) caught a node at 7,894 MB and
      # named the holders outright: two shippers at **6,475 MB / 6,263 queued** and **6,364 MB /
      # 6,453 queued**, i.e. ~1 MB per queued message.
      #
      # WHAT THIS BOUND GETS WRONG IS ARITHMETIC, not mechanism:
      #
      #     8,192 messages per shipper  x  ~1 MB per message  x  4 shippers per node  =  ~32 GB
      #
      # The cap is PER SHIPPER and a node runs one per peer, so the default permits ~32 GB before
      # it fires at all — and the observed peak queue of 6,263 is UNDER 8,192, so on that run it
      # never fired. That is the whole reason the OOM survived this fix.
      #
      # And no per-shipper message count fixes it, which is the useful conclusion: 1,024 throttles
      # a HEALTHY range (256 tenants legitimately queues 4,967; capping there turned 3,505 txn/s /
      # 0 errors into 1,580 / 5,333), while 8,192 permits 32 GB. The two constraints do not
      # overlap. A real bound has to be **in bytes and aggregated per node**, not messages per
      # shipper.
      #
      # THAT BOUND NOW EXISTS and is `Fathom.Shard.Replication.Budget`, claimed in `push/2` inside
      # the CALLER. Which is also why this one stays soft and stays: it is consulted on DEQUEUE, so
      # a burst outruns it — one run reached 12,828 against a cap of 8,192 — and a dequeue-time
      # check structurally cannot bound a mailbox a cast fills faster than the process drains it.
      # Keep it as the cheap message-count guard it is; the byte ceiling is enforced elsewhere.
      #
      # TWO WRONG READINGS ARE RECORDED HERE ON PURPOSE, because both are easy to reach again:
      #
      #   1. "The mailbox is the cause, so cap it." Right mechanism, no arithmetic — see above.
      #   2. "The mailbox is NOT the cause." Reached on 2026-08-16 from a SURVIVING node holding
      #      43,005 MB with a queue of **1**, which looked decisive. It was not: that queue had
      #      already DRAINED, leaving the binaries as garbage nothing had collected yet. Same
      #      mechanism, observed after the fact instead of during. The lesson is that a queue-depth
      #      reading taken after a burst says nothing about what filled the memory.
      #
      # The bound is KEPT: it costs nothing measurable (256 tenants runs 3,689 txn/s / 0 errors
      # with it, against 3,505 without) and an unbounded mailbox is a hazard on its own terms.
      #
      # Rejecting is cheap, so under overload the shipper becomes a fast rejector instead of a slow
      # accumulator. The cost is honest: writes fail with FILO_NO_QUORUM while a link is saturated.
      overloaded?(state) ->
        count_reject(:overloaded)
        send(from, {:repl_reply, state.id, {:reject, p.shard_id, :overloaded, 0}})
        {:noreply, state}

      Map.has_key?(state.waiters, p.shard_id) ->
        # One writer per shard means one push in flight. Two is a caller bug, and overwriting the
        # waiter would strand the first commit forever.
        count_reject(:already_in_flight)
        send(from, {:repl_reply, state.id, {:reject, p.shard_id, :already_in_flight, 0}})
        {:noreply, state}

      true ->
        # HANDED TO THE WRITER, NOT SENT HERE (expert review 2026-08-26 #19). `:gen_tcp.send/2`
        # blocks until the peer's receive window drains — up to `send_timeout`, 5 s — and this
        # process is also the socket's READER. So a blocking send here leaves incoming acks for
        # every OTHER shard on this link sitting unread in this mailbox: a shard whose ack has
        # already arrived cannot complete its quorum until an unrelated shard's send unblocks, and
        # `Budget.release/2` is delayed with it, so the node-wide byte budget stays claimed for the
        # stall. Absorbed at N=3/Q=2; it bites at N=2/Q=1, the documented development topology.
        #
        # The waiter is recorded BEFORE the send is attempted, which is the one semantic change: a
        # failure now arrives after further pushes have been accepted. `drop/2` fails all of them,
        # which is what today does too — the difference is only how many are in flight when it runs.
        Writer.frame(state.writer, encode(p))
        {:noreply, %{state | waiters: Map.put(state.waiters, p.shard_id, from)}}
    end
  end

  # Same shape as a push: claims the shard's single waiter, but holds it for the whole stream.
  def handle_cast({:seed_begin, s, from}, %{sock: nil} = state) do
    send(from, {:repl_reply, state.id, {:reject, s.shard_id, :disconnected, 0}})
    {:noreply, state}
  end

  def handle_cast({:seed_begin, s, from}, state) do
    if Map.has_key?(state.waiters, s.shard_id) do
      send(from, {:repl_reply, state.id, {:reject, s.shard_id, :already_in_flight, 0}})
      {:noreply, state}
    else
      case Writer.send_now(state.writer, Protocol.encode_seed_begin(s)) do
        :ok ->
          {:noreply, %{state | waiters: Map.put(state.waiters, s.shard_id, from)}}

        {:error, reason} ->
          send(from, {:repl_reply, state.id, {:reject, s.shard_id, :disconnected, 0}})
          {:noreply, drop(state, reason)}
      end
    end
  end

  # Chunks, end and abort all ride the waiter `seed_begin` claimed, so none of them registers or
  # answers anything. A failure drops the socket, and `drop/2` fails that waiter with
  # `:disconnected` — so the seeder is told, once, through the channel it is already listening on.
  def handle_cast({:seed_frame, _shard_id, _frame}, %{sock: nil} = state), do: {:noreply, state}

  def handle_cast({:seed_frame, shard_id, frame}, state) do
    case Writer.send_now(state.writer, seed_frame(shard_id, frame)) do
      :ok -> {:noreply, state}
      {:error, reason} -> {:noreply, drop(state, reason)}
    end
  end

  defp seed_frame(shard_id, {:chunk, part, seq, bytes}),
    do: Protocol.encode_seed_chunk(shard_id, part, seq, bytes)

  defp seed_frame(shard_id, :end), do: Protocol.encode_seed_end(shard_id)
  defp seed_frame(shard_id, :abort), do: Protocol.encode_seed_abort(shard_id)

  @impl true
  def handle_info({:tcp, _sock, bytes}, state) do
    case Protocol.decode(bytes) do
      {:ok, {:ack, shard, next}} ->
        {:noreply, reply_to(state, shard, {:ack, shard, next})}

      {:ok, {:reject, shard, reason, expected}} ->
        {:noreply, reply_to(state, shard, {:reject, shard, reason, expected})}

      {:ok, other} ->
        Logger.warning("shipper got an unexpected message: #{inspect(other)}")
        {:noreply, state}

      {:error, reason} ->
        # Framing may be out of sync; everything after this would be garbage read as replies.
        Logger.error("shipper closing connection: #{inspect(reason)}")
        {:noreply, drop(state, reason)}
    end
  end

  # MATCHED ON THE CURRENT SOCKET, and the guard is the whole point (expert review 2026-08-26 #19).
  #
  # `send_timeout_close: true` means a timed-out send CLOSES the socket, and this process is the
  # controlling process, so it receives `{:tcp_closed, _}` as well as the writer's
  # `{:send_failed, …}`. With a bare `_` here both arrive, `drop/2` runs TWICE and arms two
  # reconnect timers — two `connect/1` calls, two sockets, one leaked, and the leaked one is the
  # `active: true` reader for a link nobody drains. It shows up as a slow socket leak on a
  # long-lived link, not as a test failure.
  def handle_info({:tcp_closed, sock}, %{sock: sock} = state),
    do: {:noreply, drop(state, :closed)}

  def handle_info({:tcp_error, sock, reason}, %{sock: sock} = state),
    do: {:noreply, drop(state, reason)}

  # A WRITE FAILED ON THE CURRENT LINK. Same handling as an inline send error had: drop, which
  # fails every waiter with `:disconnected`. Tagged by the writer pid so a straggler from a
  # PREVIOUS incarnation — one that was still blocked in `send` when its socket was closed —
  # cannot tear down the socket that replaced it.
  def handle_info({:send_failed, writer, reason}, %{writer: writer} = state),
    do: {:noreply, drop(state, reason)}

  def handle_info(:reconnect, state), do: {:noreply, connect(state)}

  # Stale socket / stale writer events land here on purpose. See the guards above.
  def handle_info(_, state), do: {:noreply, state}

  @impl true
  def terminate(_reason, state) do
    # Best-effort, and correctness does not rest on it: a replacement's `Budget.install/1`
    # supersedes this ref anyway, and `Budget.queued/0` only sums the shippers `Fleet` currently
    # publishes. This just keeps a departed follower's entry from outliving it in `:persistent_term`.
    Budget.forget(state.name)
    Writer.stop(state.writer)
    if state.sock, do: :gen_tcp.close(state.sock)
    :ok
  end

  # ------------------------------------------------------------------------------------------

  defp connect(state) do
    host = if is_binary(state.host), do: String.to_charlist(state.host), else: state.host

    # `send_timeout` is what makes the mailbox bound in `overloaded?/1` actually hold. Without it
    # `:gen_tcp.send/2` blocks INDEFINITELY once the peer's receive window fills, and a shipper
    # blocked inside `send` dequeues nothing — so it cannot reject either, and the queue grows past
    # any cap regardless. Measured on the rig 2026-08-15: with the cap alone the queue still reached
    # 5,957 (peak 23 GB), because the cap is only consulted when a message is DEQUEUED.
    #
    # `send_timeout_close: true` because `packet: 4` framing makes a partially-sent frame
    # unrecoverable — the stream would resync mid-length-prefix. Closing routes into the existing
    # `{:error, reason} -> drop/2` path, which fails every waiter with `:disconnected` (the quorum
    # subtracts now, as the moduledoc requires) and schedules the ordinary reconnect.
    opts = [
      :binary,
      packet: 4,
      # See Protocol.max_frame_bytes/0: `packet: 4` allocates the DECLARED length before any body
      # arrives, and packet_size is the only bound. Applies to this direction too, because a
      # follower's reply frame is parsed the same way.
      packet_size: Fathom.Shard.Replication.Protocol.max_frame_bytes(),
      active: true,
      nodelay: true,
      send_timeout: send_timeout(),
      send_timeout_close: true
    ]

    case :gen_tcp.connect(host, state.port, opts, @connect_timeout) do
      {:ok, sock} ->
        # ONE WRITER PER SOCKET, not per shipper: a writer is meaningless without the socket it
        # owns, and tying their lifetimes together is what makes a stale writer's report
        # distinguishable from the live one's.
        %{state | sock: sock, writer: Writer.start_link(sock, self())}

      {:error, reason} ->
        Logger.warning(
          "shipper could not reach #{inspect(state.host)}:#{state.port}: #{inspect(reason)}"
        )

        Process.send_after(self(), :reconnect, @reconnect_backoff_ms)
        %{state | sock: nil, writer: nil}
    end
  end

  # Tear the socket down and fail every waiter NOW. See the moduledoc: a departed follower must
  # subtract from the quorum immediately so an unreachable quorum surfaces as an error, not a wait.
  # The mailbox bound. Counted in MESSAGES because that is what ERTS makes cheap to read
  # (`:message_queue_len` is O(1)); the number that actually matters is bytes, and the conversion
  # measured on the rig was **0.8–1.8 MB per queued push**, so the default 1024 bounds a saturated
  # link at roughly 1–2 GB. Tune with `:replication_max_queue`.
  #
  # It does NOT bound total node memory, and the 44 GB peak is not what it prevents. The reason is
  # arithmetic and is spelled out on the `overloaded?` branch in `handle_cast/2`: this cap is PER
  # SHIPPER, a node runs one per peer, so 8,192 x ~1 MB x 4 peers permits ~32 GB before it fires.
  #
  # THE DEFAULT IS DERIVED FROM MEASUREMENT, NOT INTUITION, and the first guess was wrong in a way
  # worth recording: 1024 seemed generous ("steady state should be near-empty — one push in flight
  # per shard, duplicates refused") and it was not. Sampling the rig showed a HEALTHY 256-tenant
  # step legitimately reaching queue depth **4,967**, because the cap is only consulted when a
  # message is dequeued and a burst outruns that. Capping at 1024 therefore shed load in a range
  # that had been clean, turning 3,505 txn/s / 0 errors into 1,580 / 5,333.
  #
  # So the bound has a floor and a ceiling, both observed on 2026-08-15:
  #   * floor   — above the deepest queue a healthy range produces (4,967 at 256 tenants)
  #   * ceiling — below the deepest queue seen while a node was dying (25,866)
  # 8192 sits ~65% above the floor, and the clean 256 run at 3,689 txn/s / 0 errors confirms it
  # does not shed in a healthy range.
  #
  # THE CEILING HALF OF THAT REASONING IS WRONG, and the correction is the point of this comment.
  # It treated 25,866 as a per-shipper threshold to stay under. But a node runs FOUR shippers, so
  # the number that matters is the node aggregate: at 8,192 each, four shippers permit ~32 GB — and
  # the 2026-08-16 run peaked at a queue of 6,263, comfortably UNDER the cap, so it never fired and
  # the node died anyway.
  #
  # No value works. 1,024 throttles a healthy range (256 tenants legitimately reaches 4,967);
  # 8,192 permits 32 GB. A correct bound must be in BYTES and aggregated PER NODE, not messages per
  # shipper — which is `Fathom.Shard.Replication.Budget`, claimed in `push/2` before the cast, and
  # is where the memory ceiling actually lives. Do not retune THIS number to chase memory; it
  # cannot get there, and two of the three failed fixes were exactly that attempt.
  #
  # Set to 0 to disable the bound. That restores an unbounded mailbox, which is a hazard on its own
  # terms.
  defp overloaded?(_state) do
    case max_queue() do
      0 ->
        false

      cap ->
        case Process.info(self(), :message_queue_len) do
          {:message_queue_len, n} -> n > cap
          # No info means the process is dying; do not add work to it.
          nil -> true
        end
    end
  end

  defp max_queue, do: Application.get_env(:fathom, :replication_max_queue, 8192)

  # How long a single frame may block in `:gen_tcp.send/2` before the link is declared BROKEN.
  #
  # This is a stuck-link detector, NOT a backpressure knob, and the difference was measured the
  # hard way. `send_timeout_close: true` tears down the socket and fails every waiter on it, so a
  # value low enough to fire on a merely-BUSY link converts ordinary burstiness into a storm of
  # `:disconnected` rejects plus reconnect churn. At 1 s that regressed a range that had been
  # clean: 256 tenants went from 3,505 txn/s / 0 errors to 1,748 txn/s / 2,309 errors on the rig
  # 2026-08-15. The mailbox bound in `overloaded?/1` is what handles a busy link; this only has to
  # stop an indefinitely-blocked send from pinning the process forever.
  #
  # Defaulted to the ship timeout, so it can never be the thing that fails a commit first — by the
  # time it fires the quorum has already given up on this peer on its own terms.
  defp send_timeout do
    Application.get_env(
      :fathom,
      :replication_send_timeout_ms,
      Application.get_env(:fathom, :replication_timeout_ms, 5_000)
    )
  end

  defp drop(state, reason) do
    if state.sock, do: :gen_tcp.close(state.sock)
    Writer.stop(state.writer)

    for {shard, from} <- state.waiters do
      send(from, {:repl_reply, state.id, {:reject, shard, :disconnected, 0}})
    end

    if reason != :closed do
      Logger.warning("shipper connection lost: #{inspect(reason)}")
    end

    Process.send_after(self(), :reconnect, @reconnect_backoff_ms)
    %{state | sock: nil, writer: nil, waiters: %{}}
  end

  # ONE EVENT PER REFUSED PUSH, tagged by REASON only.
  #
  # Deliberately NOT tagged by shard: a per-shard tag at fathom's stated scale is cardinality death,
  # the same reason `Fathom.ShardLoad` is a read API rather than a metric. The reason atom set is
  # small and fixed, so this is a handful of series per node.
  #
  # Until this existed the only trace of a refusal was a `Logger.warning`, which meant the signal
  # that tracks saturation most cleanly across the whole sweep — `:overloaded` at 0 / ~9k / ~17k for
  # 512 / 1024 / 2048 tenants — was only reachable by grepping container logs after the fact.
  defp count_reject(reason) do
    :telemetry.execute([:fathom, :replication, :reject], %{count: 1}, %{reason: reason})
  end

  defp encode(%Protocol.Push{} = p), do: Protocol.encode_push(p)

  defp reply_to(state, shard, msg) do
    case Map.pop(state.waiters, shard) do
      {nil, _} ->
        # A reply for a shard nobody is waiting on: a late reply after a disconnect already failed
        # the waiter. Dropping it is correct — the commit has already been answered.
        state

      {from, waiters} ->
        send(from, {:repl_reply, state.id, msg})
        %{state | waiters: waiters}
    end
  end
end
