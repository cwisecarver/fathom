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
  alias Fathom.Shard.Replication.Wal
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
  @spec seed(
          atom(),
          String.t(),
          non_neg_integer(),
          non_neg_integer(),
          non_neg_integer(),
          non_neg_integer()
        ) :: :ok
  def seed(name \\ __MODULE__, shard_id, epoch, wal_gen, salt1, wal_bytes) do
    :ets.insert(table(name), {shard_id, FollowerLog.seeded(epoch, wal_gen, salt1, wal_bytes)})
    :ok
  end

  @doc """
  Stop following a shard: drop its replication state and its local files.

  Used by promotion, once the replica has become this node's primary. Keeping the row would leave
  the node believing it is still a replica of a shard it now serves, so the next push from a
  *deposed* primary would be measured against a position that no longer describes anything.

  Dropping the state is the safe direction by construction — the next push for this shard is
  refused `:unknown_shard`, which asks for a re-seed rather than guessing a resume point.
  """
  @spec forget(atom(), String.t()) :: :ok
  def forget(name \\ __MODULE__, shard_id) do
    :ets.delete(table(name), shard_id)
    File.rm(db_path(name, shard_id))
    File.rm(wal_path(name, shard_id))
    :ok
  rescue
    ArgumentError -> :ok
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

  # The bound interface, for the boot log only. "0.0.0.0" is spelled out rather than left blank
  # because "listening on :9100" reads like a local detail while "listening on 0.0.0.0:9100" reads
  # like the exposure it is — and this port is unauthenticated.
  defp bind_label(opts) do
    case Keyword.get(opts, :ip) do
      nil -> "0.0.0.0 (ALL interfaces — set REPLICATION_BIND_IP)"
      ip -> ip |> :inet.ntoa() |> to_string()
    end
  end

  @doc false
  def wal_path(name \\ __MODULE__, shard_id), do: Path.join(dir(name), shard_id <> ".db-wal")

  @doc false
  def db_path(name \\ __MODULE__, shard_id), do: Path.join(dir(name), shard_id <> ".db")

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
    recover(tab, name, dir)

    # `ip:` is a security control, not tuning. This socket takes raw WAL frames for a tenant's
    # database and the protocol has NO authentication — whoever reaches the port can write into
    # any shard this node follows. Without `ip:` `:gen_tcp` binds every interface, which on a
    # cloud host means the public one. Same trust posture as `hrana_auth: :disabled` (the network
    # IS the boundary), so it gets the same knob: `REPLICATION_BIND_IP`.
    listen_opts =
      [:binary, packet: 4, active: false, reuseaddr: true, nodelay: true, backlog: 128]
      |> then(fn base ->
        case Keyword.get(opts, :ip) do
          nil -> base
          ip -> [{:ip, ip} | base]
        end
      end)

    {:ok, lsock} = :gen_tcp.listen(Keyword.get(opts, :port, 0), listen_opts)

    {:ok, port} = :inet.port(lsock)
    Logger.info("replication follower listening on #{bind_label(opts)}:#{port} (dir #{dir})")

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

  # `seeds` holds the partial seeds in flight ON THIS CONNECTION, keyed by shard id. Connection-
  # scoped rather than in the ETS table on purpose: a partial seed belongs to the primary that
  # started it, so a dropped connection must abandon it, and `discard_seeds/2` on the way out makes
  # that automatic rather than something a later reconnect has to clean up.
  defp serve(sock, name, seeds \\ %{}) do
    case :gen_tcp.recv(sock, 0) do
      {:ok, bytes} ->
        case Protocol.decode(bytes) do
          {:ok, %Protocol.Push{} = push} ->
            reply = handle_push(name, push)
            :ok = :gen_tcp.send(sock, reply)
            serve(sock, name, seeds)

          {:ok, %Protocol.SeedBegin{} = begin} ->
            serve(sock, name, begin_seed(name, seeds, begin))

          {:ok, {:seed_chunk, shard, part, seq, chunk}} ->
            serve(sock, name, write_chunk(seeds, shard, part, seq, chunk))

          {:ok, {:seed_end, shard}} ->
            {reply, seeds} = finish_seed(name, seeds, shard)
            :ok = :gen_tcp.send(sock, reply)
            serve(sock, name, seeds)

          {:ok, {:seed_abort, shard}} ->
            # The primary found its two halves no longer belong together. Drop the partial files
            # and answer, so the sender is not left waiting out its seed timeout.
            seeds = discard_seed(seeds, shard)
            :ok = :gen_tcp.send(sock, Protocol.encode_reject(shard, :internal, 0))
            serve(sock, name, seeds)

          {:ok, other} ->
            # A follower receiving an ack means someone pointed a primary at a primary.
            Logger.warning("replication follower got a non-push message: #{inspect(other)}")
            serve(sock, name, seeds)

          {:error, reason} ->
            # Do not keep reading a stream we cannot parse — the framing may be out of sync, and
            # every further read would be garbage interpreted as a push.
            Logger.error("replication follower closing connection: #{inspect(reason)}")
            discard_seeds(seeds)
            :gen_tcp.close(sock)
        end

      {:error, _} ->
        discard_seeds(seeds)
        :gen_tcp.close(sock)
    end
  end

  # ------------------------------------------------------------------------------------------
  # streamed seeding
  # ------------------------------------------------------------------------------------------
  #
  # A seed is a whole database, so it arrives as `seed_begin` + N `seed_chunk`s + `seed_end`
  # rather than one frame. Bytes land in `.seeding` temp files and are installed by rename **only**
  # on `seed_end`.
  #
  # That deferral is the property worth having, and it is not just about memory. A seed interrupted
  # half-way — dropped connection, aborted by the primary, a chunk that never arrived — leaves the
  # follower with NO state for the shard, so the next push is refused `:unknown_shard` and it is
  # seeded again. The alternative is a database missing pages that opens cleanly and reads as
  # valid, which is the failure mode this whole module is built to avoid.
  #
  # Nothing is acked until the install succeeds, so the primary never records a follower as holding
  # bytes it does not have.

  defp begin_seed(name, seeds, %Protocol.SeedBegin{} = b) do
    # A second `seed_begin` for a shard already streaming means the previous one will never
    # complete; drop its files rather than leak them.
    seeds = discard_seed(seeds, b.shard_id)

    with {:ok, db_fd} <-
           :file.open(seeding_path(db_path(name, b.shard_id)), [:write, :raw, :binary]),
         {:ok, wal_fd} <-
           :file.open(seeding_path(wal_path(name, b.shard_id)), [:write, :raw, :binary]) do
      Map.put(seeds, b.shard_id, %{
        name: name,
        epoch: b.epoch,
        wal_gen: b.wal_gen,
        salt1: b.salt1,
        wal_offset: b.wal_offset,
        db_size: b.db_size,
        wal_size: b.wal_size,
        db_fd: db_fd,
        wal_fd: wal_fd,
        db_written: 0,
        wal_written: 0,
        db_seq: 0,
        wal_seq: 0,
        failed: false
      })
    else
      {:error, reason} ->
        Logger.error(
          "replication seed could not open temps for #{b.shard_id}: #{inspect(reason)}"
        )

        # Recorded as failed rather than absent so `seed_end` has something to refuse — an absent
        # entry and a broken one must not be told apart by the primary, but they must both reject.
        Map.put(seeds, b.shard_id, %{name: name, failed: true})
    end
  rescue
    ArgumentError -> Map.put(seeds, b.shard_id, %{name: name, failed: true})
  end

  defp write_chunk(seeds, shard_id, part, seq, bytes) do
    case Map.get(seeds, shard_id) do
      nil -> seeds
      %{failed: true} -> seeds
      state -> Map.put(seeds, shard_id, accept_chunk(state, part, seq, bytes))
    end
  end

  # Every refusal here marks the seed failed rather than raising: chunks carry no reply, so the
  # only way to report is to let `seed_end` reject. Silently accepting any of them would install a
  # database with a hole in it.
  defp accept_chunk(state, part, seq, bytes) do
    {seq_key, written_key, size_key, fd_key} = part_keys(part)

    cond do
      # Out of order or duplicated. TCP does not reorder, but a bug on either side can, and a
      # seed that loses a chunk silently is exactly what the sequence number exists to catch.
      seq != state[seq_key] ->
        fail(state, "chunk #{seq} out of order (expected #{state[seq_key]})")

      # The WAL must not start until the database is whole, or the two halves interleave and
      # `db_written` stops meaning what `seed_end` checks it against.
      part == :wal and state.db_written != state.db_size ->
        fail(state, "wal chunk before the database was complete")

      state[written_key] + byte_size(bytes) > state[size_key] ->
        fail(state, "#{part} overran its declared size")

      true ->
        case :file.write(state[fd_key], bytes) do
          :ok ->
            state
            |> Map.put(seq_key, seq + 1)
            |> Map.put(written_key, state[written_key] + byte_size(bytes))

          {:error, reason} ->
            fail(state, "write failed: #{inspect(reason)}")
        end
    end
  end

  defp part_keys(:db), do: {:db_seq, :db_written, :db_size, :db_fd}
  defp part_keys(:wal), do: {:wal_seq, :wal_written, :wal_size, :wal_fd}

  defp fail(state, why) do
    Logger.error("replication seed failed: #{why}")
    Map.put(state, :failed, true)
  end

  defp finish_seed(name, seeds, shard_id) do
    case Map.get(seeds, shard_id) do
      nil ->
        {Protocol.encode_reject(shard_id, :internal, 0), seeds}

      %{failed: true} ->
        {Protocol.encode_reject(shard_id, :internal, 0), discard_seed(seeds, shard_id)}

      %{db_written: db, db_size: db, wal_written: wal, wal_size: wal} = state ->
        {install(name, shard_id, state), Map.delete(seeds, shard_id)}

      _short ->
        # Declared sizes not met. The stream ended early — refuse rather than install a truncated
        # database, which would open cleanly and be missing pages.
        Logger.error("replication seed for #{shard_id} ended short of its declared size")
        {Protocol.encode_reject(shard_id, :internal, 0), discard_seed(seeds, shard_id)}
    end
  end

  # Install by rename, `-wal` cleared first.
  #
  # Ordering is the same reasoning the monolithic version carried: an interruption must never leave
  # a WAL whose salts disagree with the database beside it. Removing the old `-wal` before renaming
  # the new `.db` means every intermediate state is either the old pair, a database with no WAL
  # (which SQLite reads as valid if stale), or the new pair.
  #
  # ETS goes last: until it is written the follower reports `:unknown_shard` and gets re-seeded, so
  # a failure anywhere above costs a re-seed rather than a corrupt shard.
  defp install(name, shard_id, state) do
    db = db_path(name, shard_id)
    wal = wal_path(name, shard_id)

    :file.close(state.db_fd)
    :file.close(state.wal_fd)

    with :ok <- rm_if_present(wal),
         :ok <- File.rename(seeding_path(db), db),
         :ok <- File.rename(seeding_path(wal), wal) do
      :ets.insert(
        table(name),
        {shard_id, FollowerLog.seeded(state.epoch, state.wal_gen, state.salt1, state.wal_offset)}
      )

      Logger.info(
        "replication seeded #{shard_id}: #{state.db_size}B db + #{state.wal_size}B wal " <>
          "at gen #{state.wal_gen} offset #{state.wal_offset}"
      )

      Protocol.encode_ack(shard_id, state.wal_offset)
    else
      {:error, reason} ->
        Logger.error("replication seed install failed for #{shard_id}: #{inspect(reason)}")
        Protocol.encode_reject(shard_id, :internal, 0)
    end
  rescue
    ArgumentError -> Protocol.encode_reject(shard_id, :internal, 0)
  end

  defp discard_seeds(seeds), do: Enum.reduce(Map.keys(seeds), seeds, &discard_seed(&2, &1))

  defp discard_seed(seeds, shard_id) do
    case Map.pop(seeds, shard_id) do
      {nil, seeds} ->
        seeds

      {state, rest} ->
        if state[:db_fd], do: :file.close(state.db_fd)
        if state[:wal_fd], do: :file.close(state.wal_fd)

        if name = state[:name] do
          File.rm(seeding_path(db_path(name, shard_id)))
          File.rm(seeding_path(wal_path(name, shard_id)))
        end

        rest
    end
  end

  defp rm_if_present(path) do
    case File.rm(path) do
      :ok -> :ok
      {:error, :enoent} -> :ok
      other -> other
    end
  end

  defp seeding_path(path), do: path <> ".seeding"

  # Returns the iodata to send back. All the judgement is in FollowerLog; this only performs it.
  #
  # The rescue covers a real shutdown race, not just a noisy test: if this follower stops while a
  # push is in flight, its ETS table is gone and every `:ets` call raises, taking down the handler
  # Task with an unhandled exit. A primary is waiting on a reply, so the right answer is to refuse
  # the push — which subtracts from its quorum immediately — rather than to die silently and make
  # it wait out the timeout.
  defp handle_push(name, %Protocol.Push{} = push) do
    do_handle_push(name, push)
  rescue
    ArgumentError ->
      Logger.warning("replication follower is shutting down; refusing #{push.shard_id}")
      Protocol.encode_reject(push.shard_id, :internal, 0)
  end

  # REBUILD THE PER-SHARD STATE FROM THE FILES ON BOOT.
  #
  # This table is ETS, so it dies with the process while the replicas on disk do not. A restarted
  # node therefore held a full copy of every shard it followed and did not know it: `state_of/2`
  # returned nil, so every push was refused `:unknown_shard` and the primary re-sent an entire
  # DATABASE per shard, and `Promote` — which checks `state_of/2` first — would not promote from
  # bytes sitting right there. At any real follower count that is a re-seed storm after every
  # deploy.
  #
  # THE FILES ARE THE SOURCE OF TRUTH, deliberately, rather than a sidecar counter. Frames are
  # applied with `:file.write` in APPEND mode, so a persisted offset that lagged the file by even
  # one frame would make the primary re-send a range the follower then appends a SECOND time —
  # silent duplication inside the WAL. A WAL header cannot disagree with its own file that way:
  # `Wal.read/1` reports `ckpt_seq`, `salt1` and `size`, which are exactly `wal_gen`, `salt1` and
  # `next_offset`.
  #
  # EPOCH IS RECOVERED AS 0, deliberately low. It is the primary's lease epoch and appears nowhere
  # on disk. Guessing high would make this node reject the real primary as `:stale_epoch` and stall
  # the shard until the epoch caught up; guessing low costs exactly one round trip — the first push
  # is `pushed > epoch`, which routes to `decide_fresh`, which rejects a non-zero offset asking for
  # 0, and the primary answers with `{:reset, 0, _}`. That re-sends the current WAL, not the
  # database, and is what "cheap and self-correcting" looks like next to a full re-seed.
  #
  # A shard whose WAL cannot be read is simply not recovered: it stays unknown and gets a normal
  # seed. Skipping is always safe here; claiming bytes we cannot verify is not.
  defp recover(tab, name, dir) do
    recovered =
      dir
      |> File.ls()
      |> case do
        {:ok, entries} -> entries
        {:error, _} -> []
      end
      |> Enum.filter(&String.ends_with?(&1, ".db"))
      |> Enum.map(&String.replace_suffix(&1, ".db", ""))
      |> Enum.count(fn shard_id -> recover_shard(tab, name, shard_id) end)

    if recovered > 0 do
      Logger.info("replication follower recovered #{recovered} shard(s) from #{dir}")
    end
  end

  defp recover_shard(tab, name, shard_id) do
    with {:ok, %{ckpt_seq: gen, salt1: salt, size: size}} <- Wal.read(wal_path(name, shard_id)),
         true <- File.exists?(db_path(name, shard_id)) do
      :ets.insert(tab, {shard_id, FollowerLog.seeded(0, gen, salt, size)})
      true
    else
      _ -> false
    end
  end

  defp do_handle_push(name, %Protocol.Push{} = push) do
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
