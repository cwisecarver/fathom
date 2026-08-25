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

  alias Fathom.Shard.Connection
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
    put_state(name, shard_id, FollowerLog.seeded(epoch, wal_gen, salt1, wal_bytes))
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
    # And the torn marker (expert review 2026-08-20 #11b), or a promoted-then-re-followed shard
    # inherits a quarantine flag from a replica that no longer exists.
    File.rm(torn_path(name, shard_id))
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
  The replica we are willing to OFFER a peer — `state_of/2`, or `nil` when it is torn.

  Separate from `state_of/2` on purpose. `state_of/2` answers "what is our replication state",
  which the shipper and the local promote path both need to see truthfully including the torn
  flag; this answers the narrower "do we hold a copy worth pulling", where torn and never-seeded
  are the same answer. Collapsing the two would mean either lying to the shipper or offering a
  peer an incoherent `.db`/`-wal` pair.
  """
  @spec offerable(atom(), String.t()) :: FollowerLog.t() | nil
  def offerable(name \\ __MODULE__, shard_id) do
    case state_of(name, shard_id) do
      %{torn: true} -> nil
      other -> other
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

  # Path-traversal / isolation gate (expert review 2026-08-20 #1), the same fail-closed assertion
  # `Fathom.Shard.WarmFollower.cache_path/1` carries and for the same reason: shard_id becomes a
  # FILE NAME here, so a `..` or `/` id escapes dir/1. `Path.join/2` neutralizes a leading `/` but
  # NOT `..` — `Path.join("/a/b", "../c")` is `/a/b/../c`, which the OS resolves.
  #
  # The real gate is `decode_validated/1` at the frame boundary, which refuses the id before any
  # handler sees it. This raise is the backstop for a caller that bypasses it, and should never
  # fire on a validated id.
  @doc false
  def wal_path(name \\ __MODULE__, shard_id) do
    assert_valid_shard_id!(shard_id)
    Path.join(dir(name), shard_id <> ".db-wal")
  end

  @doc false
  def db_path(name \\ __MODULE__, shard_id) do
    assert_valid_shard_id!(shard_id)
    Path.join(dir(name), shard_id <> ".db")
  end

  @doc false
  def torn_path(name \\ __MODULE__, shard_id) do
    assert_valid_shard_id!(shard_id)
    Path.join(dir(name), shard_id <> ".db.torn")
  end

  defp assert_valid_shard_id!(shard_id) do
    Fathom.ShardId.valid?(shard_id) ||
      raise(ArgumentError, "invalid shard id: #{inspect(shard_id)}")
  end

  # ------------------------------------------------------------------------------------------
  # server
  # ------------------------------------------------------------------------------------------

  @impl true
  def init(opts) do
    # Without this, an exit signal from the linked accept loop kills this GenServer OUTRIGHT and
    # `terminate/2` — which closes the listen socket — never runs (expert review 2026-08-20 #21).
    Process.flag(:trap_exit, true)

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
      [
        :binary,
        packet: 4,
        # Bounds the pre-body allocation `packet: 4` would otherwise make from an attacker-
        # declared length. See Protocol.max_frame_bytes/0 — this is a security control.
        packet_size: Protocol.max_frame_bytes(),
        active: false,
        reuseaddr: true,
        nodelay: true,
        backlog: 128
      ]
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

  # With `trap_exit` set, the linked accept loop's exit arrives here instead of killing us. A
  # normal exit is the listen socket closing during shutdown; anything else means the loop died and
  # nothing is accepting, so stop and let the supervisor start a fresh listener rather than sit
  # there with an open port and no acceptor (expert review 2026-08-20 #21).
  @impl true
  def handle_info({:EXIT, _pid, :normal}, state), do: {:noreply, state}

  def handle_info({:EXIT, _pid, reason}, state) do
    Logger.error(
      "replication accept loop exited (#{inspect(reason)}); stopping the listener so it is " <>
        "restarted with a fresh socket rather than left bound with nothing accepting"
    )

    {:stop, {:accept_loop_exited, reason}, state}
  end

  def handle_info(_msg, state), do: {:noreply, state}

  @impl true
  def terminate(_reason, %{lsock: lsock}), do: :gen_tcp.close(lsock)

  # NO `fullsweep_after: 0` HERE, and that is a measured decision rather than an oversight.
  #
  # `Fathom.Shard.Replication.Shipper` carries it because a GenServer holding WAL-frame payloads in
  # its mailbox retained 7-18 GiB per node (expert review 2026-07-24 #9, then `e0fda94`), and the
  # obvious next question was whether the RECEIVE side does the same. It does not, and the reason is
  # structural: `serve/3` below is a tail-recursive `:gen_tcp.recv` loop, so it holds ONE frame at a
  # time, decodes it, writes it straight to disk (seed chunks stream to an fd — see `accept_chunk`,
  # nothing is buffered) and drops it on the tail call. There is no mailbox to accumulate in, and
  # one process per PEER rather than per shard.
  #
  # MEASURED on the rig 2026-08-19, 1024 replicating tenants, sampled twice during the run with
  # `deploy/chaos/follower_mem.sh` (which classifies binary holders by role):
  #
  #     shipper        1,031 MB / 1,078 MB across 4 procs
  #     follower_task      3 MB /     2 MB across 5 procs
  #
  # The receive side held ~0.2% of the send side's binary memory and stayed flat (1 -> 3 MB) while
  # the shippers went 0 -> 1,031 MB. Adding the flag here would be optimizing from analogy against a
  # measurement that says there is nothing to collect.
  defp accept_loop(lsock, name) do
    case :gen_tcp.accept(lsock) do
      {:ok, sock} ->
        # NEITHER OF THESE MAY BE A BARE `=` MATCH (expert review 2026-08-20 #21).
        #
        # `:gen_tcp.controlling_process/2` returns `{:error, :closed}` when the accepted socket has
        # already been reset by the peer — an ORDINARY event, no attacker required. As a MatchError
        # inside this `spawn_link`ed loop, whose owner does not trap exits, it killed the entire
        # listener GenServer and took its ETS table with it: every follower connection dropped and
        # every replicated shard on the node needed a full DATABASE re-seed. Four peer resets in
        # five seconds then exhausted the supervisor's restart budget and terminated `Fleet` itself.
        #
        # Unlinked handler, deliberately: one primary dropping its connection must not take down
        # the listener, and a malformed frame from one peer must not affect another's shards.
        {:ok, pid} = Task.start(fn -> serve(sock, name) end)

        case :gen_tcp.controlling_process(sock, pid) do
          :ok ->
            :ok

          {:error, reason} ->
            # The peer is already gone. Close our end and let the handler find a dead socket.
            Logger.debug("replication accept: handing off socket failed (#{inspect(reason)})")
            :gen_tcp.close(sock)
        end

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
        case decode_validated(bytes) do
          {:ok, %Protocol.Push{} = push} ->
            reply = handle_push(name, push)
            :ok = :gen_tcp.send(sock, reply)
            serve(sock, name, seeds)

          {:ok, %Protocol.SeedBegin{} = begin} ->
            serve(sock, name, begin_seed(name, seeds, begin))

          {:ok, {:seed_chunk, shard, part, seq, chunk}} ->
            serve(sock, name, write_chunk(seeds, shard, part, seq, chunk))

          {:ok, {:seed_end, shard}} ->
            {result, seeds} = finish_seed(name, seeds, shard)
            :ok = :gen_tcp.send(sock, encode_seed_result(shard, result))
            serve(sock, name, seeds)

          {:ok, {:position_query, shard}} ->
            # How far along our replica is, so a node taking over the shard can decide whether we
            # are worth pulling from. Read-only and cheap on purpose: this is asked of every peer
            # on a cold open, so it must never touch the database or the object store.
            #
            # A TORN replica offers NOTHING rather than its position — same answer as never having
            # seen the shard. `Promote.fresher?/2` would refuse it on arrival anyway, but a peer
            # would first pay a whole database transfer to be told so, and the bytes it pulled
            # would be the incoherent `.db`/`-wal` pair. Answering "nothing" is both cheaper and
            # honest: we do not hold a copy of this shard right now.
            :ok = :gen_tcp.send(sock, Protocol.encode_position(shard, offerable(name, shard)))
            serve(sock, name, seeds)

          {:ok, {:replica_request, shard}} ->
            :ok = send_replica(sock, name, shard)
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

  # THE ISOLATION GATE (expert review 2026-08-20 #1).
  #
  # `Protocol.decode/1` extracts `shard_id` as an arbitrary length-prefixed binary — any bytes at
  # all, including `..`, `/` and NUL — and every handler below turns it straight into a filesystem
  # path via wal_path/2 or db_path/2. Unauthenticated: this listener accepts any connection, and
  # the rollout runbook (`config/runtime.exs`) tells operators to enable REPLICATION_LISTEN
  # fleet-wide BEFORE any node ships, so on an A2 fleet it is live on every node.
  #
  # A `seed_begin(shard_id: "../fathom_shards/victim")` + chunks + `seed_end` had `install/3`
  # File.rename an attacker-supplied SQLite file over another tenant's LIVE database — and the
  # defaults make that a single `../`, since Follower.default_dir/0 and Shard.data_dir/0 are
  # literal siblings under System.tmp_dir!(). `forget/2` gave the same primitive for File.rm/1 and
  # `apply_write/4` for arbitrary append.
  #
  # Validating here rather than in each handler is deliberate: the frame boundary is the ONE place
  # every path flows through, and `Fathom.ShardId.valid?/1` is the allowlist the rest of the
  # codebase already enforces at exactly this boundary (`WarmFollower.cache_path/1`,
  # `Snapshots`' id pattern, `Recovery`'s pinned `^shard_id`). Deliberately NOT `Path.expand` +
  # prefix comparison: a second mechanism would drift from the first.
  #
  # An unparseable IDENTIFIER is as much a framing failure as an unparseable frame, so this returns
  # the shape the existing `{:error, reason}` clause already handles — log, discard partial seeds,
  # close. Reading on would mean trusting the rest of a stream a hostile or desynced peer produced.
  defp decode_validated(bytes) do
    case Protocol.decode(bytes) do
      {:ok, frame} = ok ->
        case frame_shard_id(frame) do
          nil -> ok
          id -> if Fathom.ShardId.valid?(id), do: ok, else: {:error, {:invalid_shard_id, id}}
        end

      other ->
        other
    end
  end

  defp frame_shard_id(%Protocol.Push{shard_id: id}), do: id
  defp frame_shard_id(%Protocol.SeedBegin{shard_id: id}), do: id
  defp frame_shard_id({:seed_chunk, id, _part, _seq, _chunk}), do: id
  defp frame_shard_id({:seed_end, id}), do: id
  defp frame_shard_id({:position_query, id}), do: id
  defp frame_shard_id({:replica_request, id}), do: id
  defp frame_shard_id({:seed_abort, id}), do: id
  defp frame_shard_id(_), do: nil

  # ------------------------------------------------------------------------------------------
  # serving a pull — the survivor-selection direction
  # ------------------------------------------------------------------------------------------
  #
  # Answering `replica_request` streams OUR replica back to the asker, using the same
  # `seed_begin` / chunks / `seed_end` frames a primary uses to seed us. Nothing new is invented on
  # either side: the asker drives this module's own sink, so a pulled copy is indistinguishable
  # from a pushed one and `Promote` needs no new case.
  #
  # THE FILES ARE THE SOURCE OF TRUTH for generation, salt and offset — the same rule `recover/3`
  # follows, and for the same reason. Only `epoch` comes from ETS, because it is the primary's
  # lease epoch and appears nowhere on disk.
  #
  # What can move under us while we read, and what catches it:
  #
  #   * a PUSH APPENDS to the `-wal`. Harmless: we declared a byte count up front and stream that
  #     prefix. The extra bytes are simply not ours to send.
  #   * a PUSH RESETS the `-wal` (new epoch or new generation — `apply_write` truncates). The
  #     header's generation and salt both move, which is what the before/after check reads.
  #   * a full RE-SEED renames a new `.db` into place. The `-wal` is rewritten with it, so the same
  #     check fires. A re-seed that landed on byte-identical generation AND salt would slip
  #     through, and that is accepted: it means the two copies agree on lineage and position, and
  #     the asker still runs `quick_check` before serving a byte of it.
  defp send_replica(sock, name, shard_id) do
    case replica_offer(name, shard_id) do
      {:ok, offer} -> stream_replica(sock, name, shard_id, offer)
      :none -> :gen_tcp.send(sock, Protocol.encode_reject(shard_id, :unknown_shard, 0))
      {:error, _} -> :gen_tcp.send(sock, Protocol.encode_reject(shard_id, :internal, 0))
    end
  end

  # `offerable/2`, NOT `state_of/2` (expert review 2026-08-24 #13). `torn` was filtered in exactly
  # ONE place on the serving side — the `position_query` handler — while `replica_request` came
  # through here and applied no torn check at all. So a peer that became torn between answering
  # the position query and receiving the replica request handed over the incoherent `.db`/`-wal`
  # pair, and `Recovery` uses two separate TCP connections for those, with a torn transition
  # happening on every generation reset (i.e. every primary checkpoint).
  #
  # The pulling node could not tell: `torn` is not a field in the position frame, so
  # `Recovery.choose/3` cannot re-derive it, and the installer records the bytes through
  # `Follower.finish_seed/3` → `FollowerLog.seeded/4`, which stamps `torn: false` on the reasoning
  # that "a seed is the ONE event that rebuilds `.db` and `-wal` together" — true when the source
  # is a PRIMARY, false when the source is a follower. `Promote.stage/3`'s `quick_check` then
  # passes (it did in the 2026-08-12 rig failure, which is why `torn` exists at all) and the
  # tenant is served an empty or partial database over a working stored object, which is then
  # published to S3 under the lease fence.
  #
  # `Promote.fresher?/2`'s comment already claims this clause "keeps a torn replica from being
  # promoted locally AND from being pulled across the fleet". The fleet half rested on a filter
  # the pull path did not run; now both frames sit behind the same predicate, which is what
  # `offerable/2` was split out of `state_of/2` for.
  #
  # The panel also floated carrying `torn` in `SeedBegin` so the PULLER could verify. Not done:
  # that is a wire change, and it defends against a source that lies about itself, which is not
  # the threat model here — peers are trusted, and the race described is closed at the source.
  defp replica_offer(name, shard_id) do
    with state when state != nil <- offerable(name, shard_id),
         {:ok, header} <- Wal.read(wal_path(name, shard_id)),
         {:ok, %{size: db_size}} <- File.stat(db_path(name, shard_id)) do
      {:ok,
       %{
         epoch: state.epoch,
         wal_gen: gen_of(header),
         salt1: salt_of(header),
         wal_size: size_of(header),
         db_size: db_size,
         header: header
       }}
    else
      nil -> :none
      {:error, :enoent} -> :none
      {:error, reason} -> {:error, reason}
    end
  rescue
    ArgumentError -> :none
  end

  defp stream_replica(sock, name, shard_id, offer) do
    begin = %Protocol.SeedBegin{
      shard_id: shard_id,
      epoch: offer.epoch,
      wal_gen: offer.wal_gen,
      salt1: offer.salt1,
      wal_offset: offer.wal_size,
      db_size: offer.db_size,
      wal_size: offer.wal_size
    }

    with :ok <- :gen_tcp.send(sock, Protocol.encode_seed_begin(begin)),
         :ok <- send_part(sock, shard_id, :db, db_path(name, shard_id), offer.db_size),
         :ok <- send_part(sock, shard_id, :wal, wal_path(name, shard_id), offer.wal_size),
         {:ok, after_} <- Wal.read(wal_path(name, shard_id)),
         :ok <- stable?(offer.header, after_) do
      :gen_tcp.send(sock, Protocol.encode_seed_end(shard_id))
    else
      other ->
        Logger.warning("replication pull of #{shard_id} aborted: #{inspect(other)}")
        # The asker is mid-stream, so the honest answer is the abort frame it already understands
        # rather than silence — it drops the partial files and falls back to the stored object.
        _ = :gen_tcp.send(sock, Protocol.encode_seed_abort(shard_id))
        :ok
    end
  end

  defp send_part(_sock, _shard_id, _part, _path, 0), do: :ok

  defp send_part(sock, shard_id, part, path, size) do
    case :file.open(path, [:read, :raw, :binary]) do
      {:ok, fd} ->
        try do
          send_chunks(sock, shard_id, part, fd, 0, 0, size)
        after
          :file.close(fd)
        end

      {:error, reason} ->
        {:error, reason}
    end
  end

  defp send_chunks(_sock, _shard_id, _part, _fd, offset, _seq, size) when offset >= size, do: :ok

  defp send_chunks(sock, shard_id, part, fd, offset, seq, size) do
    len = min(chunk_bytes(), size - offset)

    case :file.pread(fd, offset, len) do
      {:ok, bin} when byte_size(bin) == len ->
        case :gen_tcp.send(sock, Protocol.encode_seed_chunk(shard_id, part, seq, bin)) do
          :ok -> send_chunks(sock, shard_id, part, fd, offset + len, seq + 1, size)
          {:error, reason} -> {:error, reason}
        end

      {:ok, _short} ->
        {:error, :short_read}

      :eof ->
        {:error, :short_read}

      {:error, reason} ->
        {:error, reason}
    end
  end

  defp chunk_bytes,
    do: Application.get_env(:fathom, :replication_seed_chunk_bytes, 4 * 1024 * 1024)

  defp gen_of(:empty), do: 0
  defp gen_of(%{ckpt_seq: seq}), do: seq
  defp salt_of(:empty), do: 0
  defp salt_of(%{salt1: s}), do: s
  defp size_of(:empty), do: 0
  defp size_of(%{size: s}), do: s

  defp stable?(:empty, :empty), do: :ok
  defp stable?(%{ckpt_seq: g, salt1: s}, %{ckpt_seq: g, salt1: s}), do: :ok
  defp stable?(_, _), do: {:error, :changed_during_pull}

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

  @doc false
  @spec begin_seed(atom(), map(), Protocol.SeedBegin.t()) :: map()
  def begin_seed(name, seeds, %Protocol.SeedBegin{} = b) do
    # A second `seed_begin` for a shard already streaming means the previous one will never
    # complete; drop its files rather than leak them.
    seeds = discard_seed(seeds, b.shard_id)

    if headroom?(name, b) do
      open_seed_temps(name, seeds, b)
    else
      refuse_seed(name, seeds, b)
    end
  end

  # DISK BACK-PRESSURE ON THE REPLICA STORE (expert review 2026-08-20 #23).
  #
  # A node acting as a follower stores a full `.db` + `-wal` copy of EVERY shard it follows, and
  # this store had no bound of any kind: no count cap, no byte cap, no free-space floor, no
  # retention. `forget/2` is called from exactly two places, both promotions. It grows monotonically
  # from OTHER NODES' write traffic, and both directories default under `System.tmp_dir!()`, so out
  # of the box the replica store and the live shard data dir SHARE A VOLUME.
  #
  # The asymmetry with the warm cache is the tell. That cache holds a READ copy whose loss costs
  # only failover latency, and it has three independent bounds plus a disk_pressure event. This one
  # holds acked, quorum-durable writes and had a gauge and an alert rule.
  #
  # What a full volume does here is the unbounded-RPO failure AGENTS.md already documents: every
  # cold-open `pull` and every dirty shard's `VACUUM INTO` fails, while SQLite's own small WAL
  # appends keep succeeding — so this node's OWN tenants keep being ACKED writes that can never be
  # made durable, caused by its role as somebody else's follower, with no local signal.
  #
  # Refusing a NEW seed is the half that is unambiguous: a shard we do not yet hold is one whose
  # RPO simply stays at the stored object, which is the pre-A2 behaviour and always correct. What
  # to SHED once already full is a policy question (which replica, and what it does to the quorum's
  # effective redundancy) and is deliberately not decided here.
  defp headroom?(name, %Protocol.SeedBegin{} = b) do
    floor = disk_free_floor_bytes()
    incoming = (b.db_size || 0) + (b.wal_size || 0)

    case Fathom.Admin.Measurements.disk_info(dir(name)) do
      # Cannot read the volume — fail OPEN, exactly as WarmFollower.headroom?/4 does. Refusing to
      # replicate because a stat failed would turn an observability gap into a durability one.
      :error ->
        true

      {:ok, %{free_bytes: free}} ->
        free - incoming >= floor
    end
  end

  defp refuse_seed(name, seeds, %Protocol.SeedBegin{} = b) do
    Logger.warning(
      "replication follower REFUSING to seed #{b.shard_id}: the replica volume (#{dir(name)}) is " <>
        "below its free-space floor. That shard's RPO stays at its stored object — the pre-A2 " <>
        "behaviour — which is correct; a full volume would instead break THIS node's own tenants, " <>
        "whose flushes and cold-open pulls all need the same disk. Raise " <>
        "REPLICATION_DISK_FREE_FLOOR_BYTES, give REPLICATION_DIR its own volume, or add space."
    )

    :telemetry.execute([:fathom, :replication, :disk_pressure], %{count: 1}, %{
      shard_id: b.shard_id
    })

    seeds
  end

  defp disk_free_floor_bytes,
    do: Application.get_env(:fathom, :replication_disk_free_floor_bytes, 1024 * 1024 * 1024)

  defp open_seed_temps(name, seeds, %Protocol.SeedBegin{} = b) do
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

  @doc false
  @spec write_chunk(map(), String.t(), :db | :wal, non_neg_integer(), binary()) :: map()
  def write_chunk(seeds, shard_id, part, seq, bytes) do
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

  @doc """
  Commit the seed accumulated for `shard_id`, returning `{result, seeds}`.

  `{:ok, wal_offset}` or `{:error, reason}` rather than the encoded reply it used to return, so the
  **pull** side can drive this same sink without a socket to answer on
  (`Fathom.Shard.Replication.Recovery`). The install path is where a partial or transposed stream
  turns into a corrupt tenant database, and a second copy of it written for the pull direction is
  the kind of duplicate that stays subtly out of step until it matters.
  """
  @spec finish_seed(atom(), map(), String.t()) ::
          {{:ok, non_neg_integer()} | {:error, term()}, map()}
  def finish_seed(name, seeds, shard_id) do
    case Map.get(seeds, shard_id) do
      nil ->
        {{:error, :no_seed_in_flight}, seeds}

      %{failed: true} ->
        {{:error, :seed_failed}, discard_seed(seeds, shard_id)}

      %{db_written: db, db_size: db, wal_written: wal, wal_size: wal} = state ->
        {install(name, shard_id, state), Map.delete(seeds, shard_id)}

      _short ->
        # Declared sizes not met. The stream ended early — refuse rather than install a truncated
        # database, which would open cleanly and be missing pages.
        Logger.error("replication seed for #{shard_id} ended short of its declared size")
        {{:error, :short_stream}, discard_seed(seeds, shard_id)}
    end
  end

  # The wire answer is unchanged: every failure above is still one opaque `:internal` reject. The
  # sender cannot act differently on any of them — all of them mean "re-seed" — and naming the
  # internal reason on the wire would only tell an unauthenticated peer about our filesystem.
  defp encode_seed_result(shard_id, {:ok, offset}), do: Protocol.encode_ack(shard_id, offset)

  defp encode_seed_result(shard_id, {:error, _}),
    do: Protocol.encode_reject(shard_id, :internal, 0)

  # Install by rename, `-wal` cleared first.
  #
  # Ordering is the same reasoning the monolithic version carried: an interruption must never leave
  # a WAL whose salts disagree with the database beside it. Removing the old `-wal` before renaming
  # the new `.db` means every intermediate state is either the old pair, a database with no WAL
  # (which SQLite reads as valid if stale), or the new pair.
  #
  # ETS goes last: until it is written the follower reports `:unknown_shard` and gets re-seeded, so
  # a failure anywhere above costs a re-seed rather than a corrupt shard.
  # DURABLE `torn` (expert review 2026-08-20 #11b).
  #
  # `torn` lived only in ETS, which dies with this GenServer. `recover/3` deliberately rebuilds
  # state from the files on boot — the right fix for the re-seed storm it documents — but torn
  # CANNOT be derived from the files: nothing on disk records that the `.db` and `-wal` are a
  # generation apart, and the `-shm` that would hint at it is deleted by `apply_write`. So every
  # recovered shard came back `torn: false`, and a node restart — a deploy, a crash, an OOM-kill,
  # all documented as routine — laundered every quarantined replica on that node into a promotable
  # one. `Promote.fresher?/2` then lets it through and `offerable/2` offers it to the whole fleet.
  #
  # The moduledoc reasons that losing ETS is safe because "the next push is refused with
  # `:unknown_shard`". `recover/3` removed exactly that safety; this restores it for the one bit
  # that mattered.
  #
  # A zero-byte marker beside the replica, written before the ETS row so a crash between the two
  # leaves the SAFE state (marker present, replica treated as torn) rather than the unsafe one.
  # ONLY TOUCHES THE FILE WHEN `torn` ACTUALLY CHANGES. This runs on EVERY push, and the ordinary
  # push is an `:append` that carries the previous torn value forward — so an unconditional
  # write/rm here would add a syscall to the replication hot path for no state change. It did, in
  # the first draft, and the pausable-peer transport test started intermittently reporting
  # `:disconnected` where it expects `:already_in_flight`. An `:ets.lookup` on a
  # `read_concurrency` table is strictly cheaper than the syscall it replaces.
  defp put_state(name, shard_id, state) do
    tab = table(name)

    was_torn? =
      case :ets.lookup(tab, shard_id) do
        [{^shard_id, %{torn: t}}] -> t
        _ -> false
      end

    # Marker BEFORE the ETS row, so a crash between the two leaves the SAFE state: the marker
    # present and the replica treated as torn, never the reverse.
    if was_torn? != state.torn, do: sync_torn_marker(name, shard_id, state)
    :ets.insert(tab, {shard_id, state})
    state
  end

  defp sync_torn_marker(name, shard_id, %{torn: true}) do
    File.write(torn_path(name, shard_id), "")
  end

  defp sync_torn_marker(name, shard_id, _state) do
    File.rm(torn_path(name, shard_id))
    :ok
  end

  defp install(name, shard_id, state) do
    db = db_path(name, shard_id)
    wal = wal_path(name, shard_id)

    :file.close(state.db_fd)
    :file.close(state.wal_fd)

    with :ok <- rm_if_present(wal),
         :ok <- File.rename(seeding_path(db), db),
         :ok <- File.rename(seeding_path(wal), wal) do
      put_state(
        name,
        shard_id,
        FollowerLog.seeded(state.epoch, state.wal_gen, state.salt1, state.wal_offset)
      )

      Logger.info(
        "replication seeded #{shard_id}: #{state.db_size}B db + #{state.wal_size}B wal " <>
          "at gen #{state.wal_gen} offset #{state.wal_offset}"
      )

      {:ok, state.wal_offset}
    else
      {:error, reason} ->
        Logger.error("replication seed install failed for #{shard_id}: #{inspect(reason)}")
        {:error, reason}
    end
  rescue
    ArgumentError -> {:error, :follower_stopped}
  end

  @doc false
  @spec discard_seeds(map()) :: map()
  def discard_seeds(seeds), do: Enum.reduce(Map.keys(seeds), seeds, &discard_seed(&2, &1))

  @doc false
  @spec discard_seed(map(), String.t()) :: map()
  def discard_seed(seeds, shard_id) do
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
      # Carry `torn` across the restart (expert review 2026-08-20 #11b). `FollowerLog.seeded/4`
      # stamps `torn: false` because a real SEED is the one event that rebuilds both files
      # together — but this is not a seed, it is a recovery, and the marker on disk is the only
      # thing that remembers the two files were a generation apart when we went down.
      torn? = File.exists?(torn_path(name, shard_id))
      :ets.insert(tab, {shard_id, %{FollowerLog.seeded(0, gen, salt, size) | torn: torn?}})
      true
    else
      _ -> false
    end
  end

  defp do_handle_push(name, %Protocol.Push{} = push) do
    # Read ONCE and keep it: the pre-reset state is what says how much of the OUTGOING generation
    # this follower actually holds, and `absorb_before_reset/4` needs that to decide whether it is
    # entitled to clear `torn` (#11a).
    prev = state_of(name, push.shard_id)

    case FollowerLog.decide(prev, push) do
      {:append, new_state} ->
        apply_write(name, push, new_state, :append)

      {:reset_then_append, new_state} ->
        # A new epoch or a checkpointed WAL: our bytes are meaningless now, so the file is replaced
        # rather than extended — but the pages in the WAL we are about to throw away are NOT
        # meaningless, and absorbing them first is what keeps this replica whole.
        absorbed =
          absorb_before_reset(name, push.shard_id, new_state, complete_through_reset?(prev, push))

        apply_write(name, push, absorbed, :truncate)

      {:reject, reason, expected} ->
        Protocol.encode_reject(push.shard_id, reason, expected)
    end
  end

  # Absorb our own WAL into our own `.db` before the reset discards it.
  #
  # THE POINT. A reset means the primary checkpointed: it drained its WAL into ITS `.db` and started
  # a new generation. We hold the same pages — they are in the WAL we received — so we can perform
  # the identical move locally. Nothing is fetched from a peer and nothing is fetched from S3; the
  # cost is one local checkpoint, proportional to WRITE VOLUME (a WAL bounded by one flush interval)
  # rather than to database size, which is what makes it affordable per shard per flush.
  #
  # Without it the reset orphans the `.db` in the old generation and every page the checkpoint moved
  # is in NEITHER file — the 2026-08-12 rig failure, where a promotion served a tenant an EMPTY
  # database over a working stored object
  # (`docs/reviews/a2-checkpoint-torn-replica-2026-08-12.md`).
  #
  # `Follower` otherwise NEVER opens the database, deliberately: opening checkpoints it, which
  # breaks the byte-offset alignment with the primary. This is the one moment that reasoning does
  # not apply — the alignment is being discarded by this very reset — which is exactly why the
  # exception is safe here and nowhere else.
  #
  # FAIL-SAFE: any failure leaves `torn: true` (what `FollowerLog.decide_fresh/2` set) and today's
  # behaviour. The replica keeps replicating and is simply not promotable until a seed rebuilds it,
  # which is strictly better than serving a torn pair. Success clears the flag, because the two
  # files are now back in step.
  # Did this follower hold the WHOLE of the generation the reset is replacing? (expert review
  # 2026-08-20 #11a — the mechanism #11b did not fix.)
  #
  # `prev_extent` is the primary's last shipped extent in the OUTGOING generation. A follower short
  # of it never received that generation's tail, so the WAL it is about to absorb is INCOMPLETE —
  # and absorbing an incomplete WAL yields a database that opens cleanly, passes `quick_check`, and
  # is quietly missing writes. `absorb_before_reset/4` then cleared `torn` and laundered it into a
  # promotable replica.
  #
  # **The follower cannot detect this alone**, which is the entire reason the field had to cross
  # the wire: a reset carries no statement about the generation it replaces, so "my offset is 4096"
  # is indistinguishable from complete and from four frames short.
  #
  # 0 means the primary made NO STATEMENT — an un-upgraded peer that never sets the field, or a
  # generation in which nothing was ever shipped. Both must read as "no evidence of a gap": marking
  # a replica torn on absence would make every replica in the fleet un-promotable for the length of
  # a rolling upgrade, which is the failure this subsystem exists to prevent, arriving by the door
  # marked safety.
  #
  # The PRIMARY-side half of #11a — sending a full seed instead of a reset when a follower is known
  # short — is deliberately NOT here. Seeds are the expensive operation A2 exists to avoid, and
  # AGENTS.md records ~10k `:already_in_flight` rejects per node at 512 tenants; a rule that turns
  # "behind at a generation boundary" into "ship the whole database" needs the rig's seed rate
  # measured first or it converts a lag spike into a seed storm. The follower-side half stands
  # alone: it refuses to CLAIM completeness it cannot prove, which is strictly safe on its own.
  defp complete_through_reset?(_prev, %Protocol.Push{prev_extent: prev}) when prev in [0, nil],
    do: true

  defp complete_through_reset?(%{next_offset: held}, %Protocol.Push{prev_extent: prev})
       when is_integer(held),
       do: held >= prev

  # No prior state at all: the reset IS the first frame for this shard, so there is no partial
  # generation to be short of.
  defp complete_through_reset?(_prev, _push), do: true

  defp absorb_before_reset(name, shard_id, new_state, complete?) do
    db = db_path(name, shard_id)
    wal = wal_path(name, shard_id)

    # Nothing to absorb: a shard we hold no files for (the reset IS the first frame) is not torn,
    # it is simply new — `apply_write` is about to lay down a whole generation from offset 0.
    if File.exists?(db) and wal_bytes(wal) > 0 do
      case checkpoint_into_db(db) do
        :ok when not complete? ->
          # The checkpoint SUCCEEDED and the replica is still torn, which is the whole point: the
          # local move was fine, the INPUT to it was short. Absorbing incomplete pages produces a
          # database that looks healthy, so the flag is the only thing standing between it and a
          # promotion.
          Logger.warning(
            "shard #{shard_id}: absorbed a SHORT WAL before a reset — this replica was behind at " <>
              "the generation boundary and stays torn; it will not be promoted until it is re-seeded"
          )

          new_state

        :ok ->
          %{new_state | torn: false}

        {:error, reason} ->
          Logger.warning(
            "shard #{shard_id}: could not absorb the WAL before a reset (#{inspect(reason)}); " <>
              "the replica stays torn and will not be promoted until it is re-seeded"
          )

          new_state
      end
    else
      %{new_state | torn: false}
    end
  end

  defp wal_bytes(path) do
    case File.stat(path) do
      {:ok, %{size: size}} -> size
      _ -> 0
    end
  end

  # `TRUNCATE` rather than PASSIVE so the WAL is actually emptied and cannot be re-applied on top of
  # the new generation we are about to write into the same file. The `-shm` goes with it: a stale
  # shared-memory index describes a WAL that no longer exists, and SQLite trusts it.
  defp checkpoint_into_db(db) do
    case Connection.open(db) do
      {:ok, conn} ->
        try do
          case Connection.query(conn, "PRAGMA wal_checkpoint(TRUNCATE)", []) do
            {:ok, _} -> :ok
            {:error, reason} -> {:error, {:checkpoint_failed, reason}}
          end
        after
          Connection.close(conn)
          File.rm(db <> "-shm")
        end

      {:error, reason} ->
        {:error, {:open_failed, reason}}
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
      put_state(name, push.shard_id, new_state)
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
