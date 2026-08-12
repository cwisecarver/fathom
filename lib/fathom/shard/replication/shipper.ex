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

  alias Fathom.Shard.Replication.Protocol

  @connect_timeout 5_000
  @reconnect_backoff_ms 500

  defstruct [:host, :port, :sock, :id, waiters: %{}]

  # ------------------------------------------------------------------------------------------
  # api
  # ------------------------------------------------------------------------------------------

  def start_link(opts) do
    GenServer.start_link(__MODULE__, opts, Keyword.take(opts, [:name]))
  end

  @doc """
  Send a frame delta. The reply arrives as a message to the calling process:

      {:repl_reply, shipper_pid, {:ack, shard_id, next_offset}}
      {:repl_reply, shipper_pid, {:reject, shard_id, reason, expected_offset}}
  """
  @spec push(GenServer.server(), Protocol.Push.t()) :: :ok
  def push(shipper, %Protocol.Push{} = p), do: GenServer.cast(shipper, {:push, p, self()})

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
    state = %__MODULE__{
      host: Keyword.fetch!(opts, :host),
      port: Keyword.fetch!(opts, :port),
      id: Keyword.get(opts, :id, Keyword.get(opts, :name, self()))
    }

    {:ok, state, {:continue, :connect}}
  end

  @impl true
  def handle_continue(:connect, state), do: {:noreply, connect(state)}

  @impl true
  def handle_call(:connected?, _from, state), do: {:reply, state.sock != nil, state}

  @impl true
  def handle_cast({:push, p, from}, %{sock: nil} = state) do
    # Not connected. Fail the push immediately rather than buffering: a queued frame delta is a
    # commit the tenant is still waiting on, and silently holding it converts a follower outage
    # into unbounded write latency.
    send(from, {:repl_reply, self(), {:reject, p.shard_id, :disconnected, 0}})
    {:noreply, state}
  end

  def handle_cast({:push, p, from}, state) do
    if Map.has_key?(state.waiters, p.shard_id) do
      # One writer per shard means one push in flight. Two is a caller bug, and overwriting the
      # waiter would strand the first commit forever.
      send(from, {:repl_reply, self(), {:reject, p.shard_id, :already_in_flight, 0}})
      {:noreply, state}
    else
      case :gen_tcp.send(state.sock, encode(p)) do
        :ok ->
          {:noreply, %{state | waiters: Map.put(state.waiters, p.shard_id, from)}}

        {:error, reason} ->
          send(from, {:repl_reply, self(), {:reject, p.shard_id, :disconnected, 0}})
          {:noreply, drop(state, reason)}
      end
    end
  end

  # Same shape as a push: claims the shard's single waiter, but holds it for the whole stream.
  def handle_cast({:seed_begin, s, from}, %{sock: nil} = state) do
    send(from, {:repl_reply, self(), {:reject, s.shard_id, :disconnected, 0}})
    {:noreply, state}
  end

  def handle_cast({:seed_begin, s, from}, state) do
    if Map.has_key?(state.waiters, s.shard_id) do
      send(from, {:repl_reply, self(), {:reject, s.shard_id, :already_in_flight, 0}})
      {:noreply, state}
    else
      case :gen_tcp.send(state.sock, Protocol.encode_seed_begin(s)) do
        :ok ->
          {:noreply, %{state | waiters: Map.put(state.waiters, s.shard_id, from)}}

        {:error, reason} ->
          send(from, {:repl_reply, self(), {:reject, s.shard_id, :disconnected, 0}})
          {:noreply, drop(state, reason)}
      end
    end
  end

  # Chunks, end and abort all ride the waiter `seed_begin` claimed, so none of them registers or
  # answers anything. A failure drops the socket, and `drop/2` fails that waiter with
  # `:disconnected` — so the seeder is told, once, through the channel it is already listening on.
  def handle_cast({:seed_frame, _shard_id, _frame}, %{sock: nil} = state), do: {:noreply, state}

  def handle_cast({:seed_frame, shard_id, frame}, state) do
    case :gen_tcp.send(state.sock, seed_frame(shard_id, frame)) do
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

  def handle_info({:tcp_closed, _}, state), do: {:noreply, drop(state, :closed)}
  def handle_info({:tcp_error, _, reason}, state), do: {:noreply, drop(state, reason)}
  def handle_info(:reconnect, state), do: {:noreply, connect(state)}
  def handle_info(_, state), do: {:noreply, state}

  @impl true
  def terminate(_reason, %{sock: sock}) when sock != nil, do: :gen_tcp.close(sock)
  def terminate(_, _), do: :ok

  # ------------------------------------------------------------------------------------------

  defp connect(state) do
    host = if is_binary(state.host), do: String.to_charlist(state.host), else: state.host

    opts = [:binary, packet: 4, active: true, nodelay: true]

    case :gen_tcp.connect(host, state.port, opts, @connect_timeout) do
      {:ok, sock} ->
        %{state | sock: sock}

      {:error, reason} ->
        Logger.warning(
          "shipper could not reach #{inspect(state.host)}:#{state.port}: #{inspect(reason)}"
        )

        Process.send_after(self(), :reconnect, @reconnect_backoff_ms)
        %{state | sock: nil}
    end
  end

  # Tear the socket down and fail every waiter NOW. See the moduledoc: a departed follower must
  # subtract from the quorum immediately so an unreachable quorum surfaces as an error, not a wait.
  defp drop(state, reason) do
    if state.sock, do: :gen_tcp.close(state.sock)

    for {shard, from} <- state.waiters do
      send(from, {:repl_reply, self(), {:reject, shard, :disconnected, 0}})
    end

    if reason != :closed do
      Logger.warning("shipper connection lost: #{inspect(reason)}")
    end

    Process.send_after(self(), :reconnect, @reconnect_backoff_ms)
    %{state | sock: nil, waiters: %{}}
  end

  defp encode(%Protocol.Push{} = p), do: Protocol.encode_push(p)

  defp reply_to(state, shard, msg) do
    case Map.pop(state.waiters, shard) do
      {nil, _} ->
        # A reply for a shard nobody is waiting on: a late reply after a disconnect already failed
        # the waiter. Dropping it is correct — the commit has already been answered.
        state

      {from, waiters} ->
        send(from, {:repl_reply, self(), msg})
        %{state | waiters: waiters}
    end
  end
end
