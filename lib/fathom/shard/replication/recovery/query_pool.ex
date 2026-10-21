defmodule Fathom.Shard.Replication.Recovery.QueryPool do
  @moduledoc """
  Persistent per-peer connections for survivor selection's position ask (expert review 2026-10-21
  #9; the user chose this pool over multiplexing the ask onto the `Shipper` link).

  `Recovery.best_replica/3` asks every peer "where is your replica of this shard?" on every cold
  open that has a rankable stamp. It used to dial a fresh TCP connection for each ask: connect, the
  `@hello` nonce exchange, one query frame, close. That is 3 round trips where 1 would do (measured
  on loopback: 188 µs p50 fresh vs 63 µs on an open socket, round-6 panel 4 F3). It also leaves one
  TIME_WAIT socket per ask on the asking node, and in a failover wave every ask holds a slot of the
  follower's `:replication_max_connections` cap, which it shares with the shipper links.

  ## Shape

  Each peer gets `pool_size/0` **slots**. A slot is one process that owns at most one connection.
  An ask CLAIMS an idle slot, trying `phash2(shard_id, pool_size)` first and then the others, and
  a slot runs ONE ask at a time from start to finish. When every slot is busy, `ask/5` returns
  `:unavailable` and the caller dials a fresh connection, as it did before the pool existed.

  **An ask never queues behind a busy slot** (expert review 2026-10-21 rv6 F2). It used to: each
  shard had one fixed slot, and an ask that waited in that slot's mailbox past its budget answered
  `:silent` without touching the network. A probe with a 100 ms peer and 200 concurrent asks gave
  44 silent answers in 4 s on a 4-slot pool, against 0 silent in 115 ms with fresh dials. A healthy
  peer read as silent counts toward neither the responder floor nor the offers, so in a wake wave
  the queue could refuse opens under quorum. The claim is a busy-until deadline in the slot table:
  the caller sets it with a compare-and-swap, and the slot clears it (only if it is still that
  claim) before replying. A claim that leaks (a caller killed between the claim and the send)
  expires at its own deadline, so a slot is never stuck busy.

  One slot running one ask at a time gives the guarantees below:

    * **Authentication is unchanged.** The slot dials and runs `Protocol.handshake_connect/2`
      itself, so the connection's nonce binding lives in the process dictionary of the one process
      that encodes and decodes that connection's frames. That is the same one-connection-per-process
      rule the binding has always relied on. The handshake runs once per connection, not once per
      ask, and every frame is still sealed and verified under that binding exactly as before.
    * **Every answer matches its request.** At most one query is in flight on a socket. Any
      exchange that does not end cleanly closes the socket: a timeout, a reply that fails to decode
      or verify, or a reply naming another shard. So a reply that arrives late cannot be read as the
      next ask's answer, because the socket it would arrive on is already gone. Before a socket is
      reused, a zero-timeout `recv` checks that nothing is waiting on it. A frame nobody asked for,
      or a peer that has closed, discards the socket. The query frame carries no request id, so this
      no-pipelining rule is the matching guarantee.
    * **A peer restart costs one redial, not an answer.** If a reused socket turns out dead (closed
      or reset before any reply byte arrives), the slot redials once and re-asks within the same
      deadline. A position query is a read, so asking it twice is safe. Without the redial, every
      shard opened just after a peer restart would read that peer as silent and could be refused
      under quorum.
    * **Timeouts are unchanged.** The caller waits at most `timeout_ms + connect timeout`, the same
      bound as `Task.async_stream` had around the old fresh dial. Inside the slot, the connect is
      bounded by the connect timeout and the reply by `timeout_ms`, and both are clipped to the
      caller's deadline. Because an ask only reaches an idle slot, it starts with its whole budget.
      An ask that times out reads as silent, as it did before, and the under-quorum rules in
      `Recovery` see the same answers they always did.
    * **The unreachable cache is unchanged.** A dial that times out on its full connect bound marks
      the peer (`Recovery.note_unreachable/2`), and a successful connect clears it. A slot re-checks
      the mark before dialing, so an ask that follows one that found the peer blackholed does not
      pay the full connect timeout again. A dial cut well short of its connect bound marks nothing
      (see `Slot`).

  ## The follower's connection cap

  A node holds at most `pool_size/0` POOLED query connections per peer (default 4), and a slot
  closes its socket after `idle_ms/0` (default 30 s) without an ask. So a quiet fleet holds no
  query connections at all. In steady state each follower sees at most `pool_size * (nodes - 1)`
  query connections, plus the shipper links. A burst wider than the pool overflows to fresh dials,
  one short-lived connection per overflowing ask, which is exactly the pre-pool behaviour (rv6 F2):
  the pool takes the common case off the network, and it no longer bounds a wave by queueing it,
  because a queue that answers `:silent` is worse than the connection it saves. With the default
  cap of 256 and pool size 4, the pooled connections alone reach the follower cap at about 50
  nodes. Above that, lower `REPLICATION_RECOVERY_POOL_SIZE` or raise
  `:replication_max_connections`.

  ## Not running

  When this process is not running (a harness that starts a `Follower` by hand, or pool size 0 to
  roll back), `ask/4` returns `:unavailable` and `Recovery` dials a fresh connection exactly as it
  did before this existed. The same happens when a slot dies between the lookup and the ask.

  Pulls (`replica_request`) stay on dedicated connections. They are rare, they can last up to the
  60 s pull timeout, and a slot held for one would block that shard's asks behind it.
  """

  use GenServer

  alias Fathom.Shard.Replication.Recovery
  alias Fathom.Shard.Replication.Recovery.QueryPool.Slot

  @table __MODULE__.Slots
  @default_pool_size 4
  @default_idle_ms 30_000

  @doc false
  def start_link(opts \\ []), do: GenServer.start_link(__MODULE__, opts, name: __MODULE__)

  @doc """
  Slots per peer (`:replication_recovery_pool_size`, default #{@default_pool_size}). 0 turns the
  pool off, and every ask dials a fresh connection as it did before.
  """
  @spec pool_size() :: non_neg_integer()
  def pool_size,
    do: Application.get_env(:fathom, :replication_recovery_pool_size, @default_pool_size)

  @doc """
  How long a slot keeps an unused connection open (`:replication_recovery_pool_idle_ms`, default
  #{@default_idle_ms}).
  """
  @spec idle_ms() :: pos_integer()
  def idle_ms,
    do: Application.get_env(:fathom, :replication_recovery_pool_idle_ms, @default_idle_ms)

  @doc """
  Ask `endpoint` for its position on `shard_id` over a pooled connection.

  Returns `{:ok, {:answered, position | nil}}`, `{:ok, :silent}`, or `:unavailable` when the pool
  cannot take the ask (not running, turned off, every slot busy, or the slot is gone). On
  `:unavailable` the caller
  dials a fresh connection. `connect_ms` is the caller's connect bound, and the whole ask is bounded
  by `timeout_ms + connect_ms`.
  """
  @spec ask(Recovery.endpoint(), String.t(), non_neg_integer(), non_neg_integer(), boolean()) ::
          {:ok, {:answered, Recovery.position() | nil} | :silent} | :unavailable
  def ask(endpoint, shard_id, timeout_ms, connect_ms, honor_unreachable?) do
    budget = timeout_ms + connect_ms
    deadline = System.monotonic_time(:millisecond) + budget

    case claim(endpoint, shard_id, deadline) do
      nil ->
        :unavailable

      {pid, key} ->
        req = {:ask, key, shard_id, timeout_ms, connect_ms, deadline, honor_unreachable?}
        request = :gen_server.send_request(pid, req)

        # `receive_response/2` drops a reply that arrives after the timeout, so a slot that answers
        # late cannot leave a stray message in this process's mailbox.
        case :gen_server.receive_response(request, budget) do
          {:reply, answer} -> {:ok, answer}
          :timeout -> {:ok, :silent}
          {:error, {:noproc, _}} -> :unavailable
          {:error, _} -> {:ok, :silent}
        end
    end
  end

  @doc false
  # Clear a claim. Called by the slot that ran the ask, before it replies. Matches the claim's own
  # deadline, so a slot finishing an ask whose caller already gave up (and whose claim expired and
  # was retaken) cannot clear the NEXT caller's claim and let a third ask queue behind it.
  @spec release({Recovery.endpoint(), non_neg_integer()}, integer()) :: :ok
  def release(key, deadline) do
    _ =
      :ets.select_replace(@table, [
        {{key, self(), deadline}, [], [{{{:const, key}, self(), :idle}}]}
      ])

    :ok
  rescue
    ArgumentError -> :ok
  end

  # Claim an idle slot on `endpoint` until `deadline`: `{pid, key}`, or nil when the pool is not
  # running, is turned off, or every slot is busy (rv6 F2: the caller then dials fresh rather than
  # queueing). `phash2(shard_id)` is tried first, so a quiet fleet's asks for one shard keep landing
  # on one warm connection; the others are tried in order after it. A slot that does not exist yet
  # is started on first use.
  defp claim(endpoint, shard_id, deadline) do
    case pool_size() do
      n when is_integer(n) and n > 0 ->
        first = :erlang.phash2(shard_id, n)

        Enum.find_value(0..(n - 1), fn i ->
          try_claim({endpoint, rem(first + i, n)}, deadline)
        end)

      _ ->
        nil
    end
  rescue
    # No table: the pool is not running.
    ArgumentError -> nil
  end

  defp try_claim(key, deadline) do
    pid =
      case :ets.lookup(@table, key) do
        [{_, pid, _busy_until}] -> pid
        [] -> start_slot(key)
      end

    # Compare-and-swap: the slot is ours only if it is idle or its claim has expired. Idle is the
    # atom `:idle`, not 0: monotonic time can be negative, so no integer means "long ago".
    now = System.monotonic_time(:millisecond)
    free = {:orelse, {:==, :"$1", :idle}, {:<, :"$1", now}}
    ms = [{{key, pid, :"$1"}, [free], [{{{:const, key}, pid, deadline}}]}]

    if is_pid(pid) and :ets.select_replace(@table, ms) == 1, do: {pid, key}
  end

  defp start_slot(key) do
    GenServer.call(__MODULE__, {:slot, key})
  catch
    :exit, _ -> nil
  end

  # -- the manager: owns the slot table and the slots -----------------------------------------

  @impl true
  def init(_opts) do
    Process.flag(:trap_exit, true)
    # Public: callers claim a slot and slots release it, both straight on the table (rv6 F2).
    :ets.new(@table, [:named_table, :public, :set, read_concurrency: true])
    {:ok, %{}}
  end

  @impl true
  def handle_call({:slot, key}, _from, pids) do
    # Re-checked here: two callers can miss the same key at once, and only the first may start it.
    case :ets.lookup(@table, key) do
      [{_, pid, _busy_until}] ->
        {:reply, pid, pids}

      [] ->
        {endpoint, _index} = key
        {:ok, pid} = GenServer.start_link(Slot, endpoint)
        :ets.insert(@table, {key, pid, :idle})
        {:reply, pid, Map.put(pids, pid, key)}
    end
  end

  @impl true
  def handle_info({:EXIT, pid, _reason}, pids) do
    case Map.pop(pids, pid) do
      {nil, pids} ->
        {:noreply, pids}

      {key, pids} ->
        # A slot that crashed is replaced on the next ask for its key. Its socket closed with it.
        :ets.delete(@table, key)
        {:noreply, pids}
    end
  end

  def handle_info(_msg, pids), do: {:noreply, pids}
end
