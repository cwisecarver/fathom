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
  An ask for `shard_id` goes to slot `phash2(shard_id, pool_size)`, so asks for one shard are
  serialized and asks for different shards spread across the slots. A slot runs ONE ask at a time
  from start to finish, and that rule gives the guarantees below:

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
      caller's deadline. An ask still queued in the slot when its deadline passes is answered
      `:silent` without touching the network. An ask that times out reads as silent, as it did
      before, and the under-quorum rules in `Recovery` see the same answers they always did.
    * **The unreachable cache is unchanged.** A dial that times out on its full connect bound marks
      the peer (`Recovery.note_unreachable/2`), and a successful connect clears it. A slot re-checks
      the mark before dialing, so asks queued behind a blackholed peer do not each pay the full
      connect timeout. A dial cut short by the caller's deadline marks nothing (see `Slot`).

  ## The follower's connection cap

  A node holds at most `pool_size/0` query connections per peer (default 4), and a slot closes its
  socket after `idle_ms/0` (default 30 s) without an ask. So a quiet fleet holds no query
  connections at all. In a wake wave each follower sees at most `pool_size * (nodes - 1)` query
  connections, plus the shipper links. The old fresh-dial ask, by contrast, could hold one
  connection per concurrent cold open, without bound. With the default cap of 256 and pool size 4,
  the follower cap is reached at about 50 nodes. Above that, lower `REPLICATION_RECOVERY_POOL_SIZE`
  or raise `:replication_max_connections`.

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
  cannot take the ask (not running, turned off, or the slot is gone). On `:unavailable` the caller
  dials a fresh connection. `connect_ms` is the caller's connect bound, and the whole ask is bounded
  by `timeout_ms + connect_ms`.
  """
  @spec ask(Recovery.endpoint(), String.t(), non_neg_integer(), non_neg_integer(), boolean()) ::
          {:ok, {:answered, Recovery.position() | nil} | :silent} | :unavailable
  def ask(endpoint, shard_id, timeout_ms, connect_ms, honor_unreachable?) do
    case slot(endpoint, shard_id) do
      nil ->
        :unavailable

      pid ->
        budget = timeout_ms + connect_ms
        deadline = System.monotonic_time(:millisecond) + budget
        req = {:ask, shard_id, timeout_ms, connect_ms, deadline, honor_unreachable?}
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

  # The slot for `shard_id` on `endpoint`, started on first use. nil when the pool is not running
  # or is turned off.
  defp slot(endpoint, shard_id) do
    case pool_size() do
      n when is_integer(n) and n > 0 ->
        key = {endpoint, :erlang.phash2(shard_id, n)}

        case :ets.lookup(@table, key) do
          [{_, pid}] -> pid
          [] -> start_slot(key)
        end

      _ ->
        nil
    end
  rescue
    # No table: the pool is not running.
    ArgumentError -> nil
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
    :ets.new(@table, [:named_table, :protected, :set, read_concurrency: true])
    {:ok, %{}}
  end

  @impl true
  def handle_call({:slot, key}, _from, pids) do
    # Re-checked here: two callers can miss the same key at once, and only the first may start it.
    case :ets.lookup(@table, key) do
      [{_, pid}] ->
        {:reply, pid, pids}

      [] ->
        {endpoint, _index} = key
        {:ok, pid} = GenServer.start_link(Slot, endpoint)
        :ets.insert(@table, {key, pid})
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
