defmodule Fathom.Shard.Replication.Follower do
  @moduledoc """
  The receiving end of A2 replication — see `docs/a2-quorum-replication.md`.

  A thin shell over `Fathom.Shard.Replication.FollowerLog`: this module owns a listener, a socket
  and a file handle, and every accept/reject decision is delegated. That split is the point — the
  decisions are where a tenant database gets corrupted, and they are unit-tested without a network.

  **One connection serves every shard a primary replicates here.** Each push names its shard and
  each ack names it back, so a primary holds one socket per follower *node*, not per shard. At
  millions of shards the alternative is millions of sockets.

  ## Per-shard state, and why it lives in ETS

  A follower must remember `{epoch, wal_gen, next_offset}` per shard **across connections**: a
  primary that reconnects after a blip has not rewound, and losing the offset would force a full
  re-seed from S3 for what was a two-second network event. The table is owned by this GenServer, so
  a crash here drops the state and the next push is refused with `:unknown_shard` — the correct
  direction. Fabricating a resume point after losing it is how a follower ends up appending into
  the wrong position.

  ## Followers do not open the database

  Per the gate-1 finding, a clean close **checkpoints**: one open-and-close moved a follower's
  `.db` from 4096 to 8192 bytes and deleted its `-wal`, desynchronizing it from the primary's byte
  offsets. So this module only ever appends to the `-wal` file with a raw handle and never opens
  the shard through SQLite. Promotion is the first legitimate open, and it is not this module's job.

  ## Acking from RAM by default

  `:replication_fsync` (default **false**) decides whether a follower `fdatasync`s before acking.
  Off matches Waterpark, which acks from RAM and takes durability from replica count rather than
  disk. Measured cost of turning it on: ~300 µs against a ~96 µs floor, i.e. **2.4× fathom's whole
  current request round trip** (gate 2). Off is also never *worse* than today: if every replica
  holding an un-synced frame dies at once, the shard falls back to its S3 object, which is exactly
  the pre-A2 behaviour.
  """
  use GenServer

  require Logger

  alias Fathom.Shard.Replication.FollowerLog
  alias Fathom.Shard.Replication.Protocol

  # ------------------------------------------------------------------------------------------
  # api
  # ------------------------------------------------------------------------------------------

  def start_link(opts) do
    GenServer.start_link(__MODULE__, opts, name: Keyword.get(opts, :name, __MODULE__))
  end

  @doc """
  The ETS table backing `name`'s per-shard state.

  Derived from the process name rather than a single module-wide constant, so more than one
  follower can run in one VM. Production has exactly one per node and would not care — but a
  singleton table also made it impossible to stand four real followers up in a test, and an
  environment that cannot express the topology cannot catch bugs in it (AGENTS.md).
  """
  @spec table(atom()) :: atom()
  def table(name \\ __MODULE__), do: Module.concat(name, Shards)

  @doc "The port this follower is listening on (0 in config means 'pick one')."
  @spec port(GenServer.server()) :: {:ok, :inet.port_number()}
  def port(server \\ __MODULE__), do: GenServer.call(server, :port)

  @doc """
  Register a shard this node is following, with the state its seed left it in.

  Must be called before any push for that shard, or the push is refused — see `FollowerLog.decide/2`
  on why "never seen" is deliberately distinct from "at offset 0".
  """
  @spec seed(atom(), String.t(), non_neg_integer(), non_neg_integer(), non_neg_integer()) :: :ok
  def seed(name \\ __MODULE__, shard_id, epoch, wal_gen, wal_bytes) do
    :ets.insert(table(name), {shard_id, FollowerLog.seeded(epoch, wal_gen, wal_bytes)})
    :ok
  end

  @doc "Current replication state for a shard, or nil if it was never seeded."
  @spec state_of(atom(), String.t()) :: FollowerLog.t() | nil
  def state_of(name \\ __MODULE__, shard_id) do
    case :ets.lookup(table(name), shard_id) do
      [{^shard_id, s}] -> s
      [] -> nil
    end
  end

  @doc """
  Where this follower keeps the WAL files it receives.

  Per-instance, not global. Four followers sharing one directory would all write the SAME file for
  a given shard, so a test standing up four of them would pass while proving nothing about
  independent replicas.
  """
  @spec dir(atom()) :: Path.t()
  def dir(name \\ __MODULE__) do
    case :ets.lookup(table(name), :__dir__) do
      [{:__dir__, d}] -> d
      [] -> default_dir()
    end
  end

  @doc false
  def default_dir do
    Application.get_env(:fathom, :replication_dir) ||
      Path.join(System.tmp_dir!(), "fathom_replication")
  end

  @doc false
  def wal_path(name \\ __MODULE__, shard_id), do: Path.join(dir(name), shard_id <> ".db-wal")

  # ------------------------------------------------------------------------------------------
  # server
  # ------------------------------------------------------------------------------------------

  @impl true
  def init(opts) do
    name = Keyword.get(opts, :name, __MODULE__)
    dir = Keyword.get(opts, :dir) || default_dir()
    File.mkdir_p!(dir)

    # `write_concurrency` because every connection handler writes its own shards' rows; the
    # GenServer owns the table only so the state dies with it.
    tab =
      :ets.new(table(name), [
        :named_table,
        :public,
        :set,
        write_concurrency: true,
        read_concurrency: true
      ])

    :ets.insert(tab, {:__dir__, dir})

    {:ok, lsock} =
      :gen_tcp.listen(Keyword.get(opts, :port, 0), [
        :binary,
        packet: 4,
        active: false,
        reuseaddr: true,
        nodelay: true,
        backlog: 128
      ])

    {:ok, port} = :inet.port(lsock)
    Logger.info("replication follower listening on #{port}")

    {:ok, %{lsock: lsock, port: port, name: name}, {:continue, :accept}}
  end

  @impl true
  def handle_continue(:accept, state) do
    spawn_link(fn -> accept_loop(state.lsock, state.name) end)
    {:noreply, state}
  end

  @impl true
  def handle_call(:port, _from, state), do: {:reply, {:ok, state.port}, state}

  @impl true
  def terminate(_reason, %{lsock: lsock}), do: :gen_tcp.close(lsock)

  defp accept_loop(lsock, name) do
    case :gen_tcp.accept(lsock) do
      {:ok, sock} ->
        # Unlinked: one primary dropping its connection must not take down the listener, and a
        # malformed frame from one peer must not affect another's shards.
        {:ok, pid} = Task.start(fn -> serve(sock, name) end)
        :ok = :gen_tcp.controlling_process(sock, pid)
        accept_loop(lsock, name)

      {:error, :closed} ->
        :ok

      {:error, reason} ->
        Logger.warning("replication accept failed: #{inspect(reason)}")
        accept_loop(lsock, name)
    end
  end

  defp serve(sock, name) do
    case :gen_tcp.recv(sock, 0) do
      {:ok, bytes} ->
        case Protocol.decode(bytes) do
          {:ok, %Protocol.Push{} = push} ->
            reply = handle_push(name, push)
            :ok = :gen_tcp.send(sock, reply)
            serve(sock, name)

          {:ok, other} ->
            # A follower receiving an ack means someone pointed a primary at a primary.
            Logger.warning("replication follower got a non-push message: #{inspect(other)}")
            serve(sock, name)

          {:error, reason} ->
            # Do not keep reading a stream we cannot parse — the framing may be out of sync, and
            # every further read would be garbage interpreted as a push.
            Logger.error("replication follower closing connection: #{inspect(reason)}")
            :gen_tcp.close(sock)
        end

      {:error, _} ->
        :gen_tcp.close(sock)
    end
  end

  # Returns the iodata to send back. All the judgement is in FollowerLog; this only performs it.
  defp handle_push(name, %Protocol.Push{} = push) do
    case FollowerLog.decide(state_of(name, push.shard_id), push) do
      {:append, new_state} ->
        apply_write(name, push, new_state, :append)

      {:reset_then_append, new_state} ->
        # A new epoch or a checkpointed WAL: our bytes are meaningless now, so the file is replaced
        # rather than extended.
        apply_write(name, push, new_state, :truncate)

      {:reject, reason, expected} ->
        Protocol.encode_reject(push.shard_id, reason, expected)
    end
  end

  defp apply_write(name, %Protocol.Push{} = push, new_state, mode) do
    path = wal_path(name, push.shard_id)
    modes = if mode == :truncate, do: [:write, :raw, :binary], else: [:append, :raw, :binary]

    with {:ok, fd} <- :file.open(path, modes),
         :ok <- :file.write(fd, push.payload),
         :ok <- maybe_sync(fd),
         :ok <- :file.close(fd) do
      # State advances ONLY after the bytes are down. Advancing first and failing the write would
      # leave the follower claiming an offset it does not hold, which the primary would believe.
      :ets.insert(table(name), {push.shard_id, new_state})
      Protocol.encode_ack(push.shard_id, new_state.next_offset)
    else
      {:error, reason} ->
        Logger.error("replication write failed for #{push.shard_id}: #{inspect(reason)}")
        # Report where we actually are, so the primary retries rather than assuming we advanced.
        expected = (state_of(name, push.shard_id) || %{next_offset: 0}).next_offset
        Protocol.encode_reject(push.shard_id, :internal, expected)
    end
  end

  defp maybe_sync(fd) do
    if Application.get_env(:fathom, :replication_fsync, false), do: :file.datasync(fd), else: :ok
  end
end
