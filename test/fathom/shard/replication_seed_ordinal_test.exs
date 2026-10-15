defmodule Fathom.Shard.ReplicationSeedOrdinalTest do
  @moduledoc """
  A seed states the WAL ordinal it ships (rig 2026-10-14 B1).

  ## The bug (real data loss, found by `./chaos.sh rpo` and bisected 2026-10-14)

  A shard that reopens under a new lineage (idle drop, handoff, failover) re-seeds every follower:
  the first push from the new ownership is answered `:unknown_shard` and `Session` seeds from inside
  that commit. The seed copies the primary's live `.db` + `-wal`, which ALREADY hold the commit's
  frames, so the post-seed retry plans nothing for any follower and no push is sent. And a seed
  installed `FollowerLog.seeded/5`'s `wal_ordinal: 0` — "not stated" — because `SeedBegin` carried
  no ordinal.

  So the quorum-acked write lived on every follower, and every one of those replicas was
  unrankable: `Promote.fresher?(%{wal_ordinal: 0}, _)` is false BY DESIGN (an unstated replica must
  never outrank an object). Promote-on-open and `Recovery` both declined them, the survivor
  cold-opened the older stored object, and the acked write was gone. On the rig: keeper replica
  `wal_ordinal: 0, lineage 3` against an object stamped `epoch 2, wal_ordinal 2`.

  ## The fix

  The seed carries the ordinal the primary's coordinator assigned to the WAL it ships
  (`@seed_begin_ord`), and the follower installs it. The "0 means unknown" rule is untouched — the
  last test here pins that an unstated seed still installs an unrankable replica.

  Real followers, a real replicating shard driven through `ShardExecutor` (the production commit
  path), `FaultyStorage` for the lease contract, both liveness modes.
  """
  use ExUnit.Case, async: false

  alias Fathom.Shard.Heartbeat
  alias Fathom.Shard.Replication.Fleet
  alias Fathom.Shard.Replication.Follower
  alias Fathom.Shard.Replication.Promote
  alias Fathom.Shard.Replication.Protocol
  alias Fathom.Shard.Replication.Recovery
  alias Fathom.Shard.Replication.Shipper
  alias Fathom.Shard.Storage
  alias Fathom.ShardExecutor
  alias Fathom.Shards

  @modes [:legacy, :heartbeat]

  @env [
    :shard_storage,
    :shard_idle_ms,
    :replication_enabled,
    :replication_followers,
    :replication_quorum,
    :replication_promote_on_open,
    :replication_recover_from_peers,
    :replication_lineage_wire,
    :replication_ordinal_wire
  ]

  setup do
    id = "seedord_#{System.unique_integer([:positive])}"
    root = Path.join(System.tmp_dir!(), "seedord_#{System.unique_integer([:positive])}")
    File.mkdir_p!(root)
    prev = Map.new(@env, &{&1, Application.get_env(:fathom, &1)})

    on_exit(fn ->
      Shards.drain(id, 5_000)

      for {k, v} <- prev do
        if is_nil(v),
          do: Application.delete_env(:fathom, k),
          else: Application.put_env(:fathom, k, v)
      end

      File.rm_rf(root)
      for e <- ["", "-wal", "-shm", ".etag"], do: File.rm(Fathom.Shard.db_path(id) <> e)
    end)

    Recovery.init_table()
    Application.put_env(:fathom, :shard_storage, Fathom.Test.FaultyStorage)
    # Pinned high: the test drives every close/stop itself, and an idle-stop racing an assertion
    # is the classic flake shape (AGENTS.md § Testing).
    Application.put_env(:fathom, :shard_idle_ms, 600_000)
    # Both wire gates ON, as in prod: the lineage and the ordinal only travel with them, and with
    # them off every replica is unrankable for a reason that has nothing to do with this bug.
    Application.put_env(:fathom, :replication_lineage_wire, true)
    Application.put_env(:fathom, :replication_ordinal_wire, true)
    # Off while the history is built; turned on only for the survivor's open.
    Application.put_env(:fathom, :replication_promote_on_open, false)
    Application.put_env(:fathom, :replication_recover_from_peers, false)

    # The SURVIVOR's own follower: the default-named instance a coordinator looks in. It is not a
    # replication target, so it holds nothing — the node the LB fails over to.
    start_supervised!({Follower, name: Follower, port: 0, dir: Path.join(root, "survivor")})

    %{id: id, root: root}
  end

  defp set_mode!(:legacy) do
    refute Heartbeat.running?()
    :ok
  end

  defp set_mode!(:heartbeat) do
    hb = start_supervised!({Heartbeat, ttl_ms: 30_000})
    _ = :sys.get_state(hb)
    :ok
  end

  defp assert_mode!(:legacy, coordinator),
    do: assert(is_nil(Fathom.Shard.acquire_gen(coordinator)), "expected LEGACY liveness")

  defp assert_mode!(:heartbeat, coordinator),
    do: refute(is_nil(Fathom.Shard.acquire_gen(coordinator)), "expected HEARTBEAT liveness")

  defp start_follower!(root) do
    name = :"seedord_f_#{System.unique_integer([:positive])}"

    pid =
      start_supervised!({Follower, name: name, port: 0, dir: Path.join(root, "#{name}")},
        id: name
      )

    {:ok, port} = Follower.port(pid)
    {name, port}
  end

  # N = 3, Q = 2 (Fleet refuses Q = N). After the reopen every follower answers the new ownership
  # `:unknown_shard`, so the ack for row 999 can only have come through seeds.
  defp enable!(followers) do
    Application.put_env(:fathom, :replication_enabled, true)
    Application.put_env(:fathom, :replication_quorum, 2)

    Application.put_env(
      :fathom,
      :replication_followers,
      for({_n, port} <- followers, do: {~c"127.0.0.1", port})
    )

    start_supervised!(Fleet)

    await!(
      fn -> Enum.all?(Fleet.shippers(), &Shipper.connected?/1) end,
      "shippers never connected"
    )
  end

  defp await!(fun, msg, timeout \\ 5_000) do
    deadline = System.monotonic_time(:millisecond) + timeout

    Stream.repeatedly(fn -> fun.() || Process.sleep(20) end)
    |> Enum.find(fn
      v when v in [nil, false, :ok] -> System.monotonic_time(:millisecond) > deadline
      _ -> true
    end)
    |> case do
      v when v in [nil, false, :ok] -> flunk(msg)
      v -> v
    end
  end

  defp stmt(sql, args \\ []), do: %Filo.Stmt{sql: sql, args: args}

  defp exec!(conn, sql, args \\ []) do
    assert {:ok, _} = ShardExecutor.execute(conn, stmt(sql, args))
  end

  defp rows(conn) do
    {:ok, %{rows: rows}} = ShardExecutor.execute(conn, stmt("SELECT a FROM t ORDER BY a"))
    Enum.map(rows, fn [a] -> a end)
  end

  defp coordinator!(id) do
    {:ok, pid} = Shards.ensure(id)
    pid
  end

  defp close(conn) do
    ShardExecutor.close(conn)
  catch
    :exit, _ -> :ok
  end

  # A graceful stop (drop-flush, lease released): the idle-drop half of the rig sequence. The local
  # files go too, so the reopen is a cold open from the object under a NEW lineage.
  defp stop_shard!(id, conn) do
    :ok = close(conn)
    coordinator = coordinator!(id)
    mon = Process.monitor(coordinator)
    :ok = Shards.stop(id)
    assert_receive {:DOWN, ^mon, :process, ^coordinator, _}, 10_000
    for e <- ["", "-wal", "-shm", ".etag"], do: File.rm(Fathom.Shard.db_path(id) <> e)
  end

  # NODE LOSS: no drop-flush, so the object never sees row 999, and the dead node's disk is gone.
  defp kill_primary!(id, conn) do
    coordinator = coordinator!(id)
    mon = Process.monitor(coordinator)
    Process.exit(coordinator, :kill)
    assert_receive {:DOWN, ^mon, :process, ^coordinator, _}, 5_000
    _ = close(conn)
    for e <- ["", "-wal", "-shm", ".etag"], do: File.rm(Fathom.Shard.db_path(id) <> e)
  end

  for mode <- @modes do
    describe "#{mode} mode" do
      setup do: set_mode!(unquote(mode))

      test "a write acked only through a re-seed is rankable on every replica and survives failover",
           %{id: id, root: root} do
        followers = [start_follower!(root), start_follower!(root), start_follower!(root)]
        enable!(followers)

        # Ownership 1: row 1, replicated and flushed, so the object carries a RANKABLE stamp.
        {:ok, conn} = ShardExecutor.open(id)
        c1 = coordinator!(id)
        assert_mode!(unquote(mode), c1)
        lineage1 = Fathom.Shard.lineage(c1)
        exec!(conn, "CREATE TABLE t (a INTEGER PRIMARY KEY)")
        exec!(conn, "INSERT INTO t VALUES (1)")
        :ok = Shards.flush(id)
        assert {:ok, %{wal_ordinal: n1}} = Storage.object_position(id)
        assert n1 > 0, "precondition: the flush stamped no ordinal, so nothing here is rankable"

        stop_shard!(id, conn)

        # Ownership 2: a cold reopen claims a new lineage, and its first write re-seeds the
        # followers from inside its own commit. Q = 2, so this :ok means at least two seeds landed.
        {:ok, conn} = ShardExecutor.open(id)
        c2 = coordinator!(id)
        assert_mode!(unquote(mode), c2)
        lineage2 = Fathom.Shard.lineage(c2)
        assert lineage2 > lineage1, "precondition: the reopen did not claim a new lineage"
        exec!(conn, "INSERT INTO t VALUES (999)")

        {:ok, stamp} = Storage.object_position(id)

        reseeded =
          for {name, _} <- followers,
              replica = Follower.state_of(name, id),
              match?(%{lineage: ^lineage2, torn: false}, replica),
              do: {name, replica}

        assert length(reseeded) >= 2,
               "precondition: the acked write did not reach a quorum of re-seeded followers: " <>
                 inspect(Enum.map(followers, fn {n, _} -> Follower.state_of(n, id) end))

        for {name, replica} <- reseeded do
          assert replica.wal_ordinal > 0,
                 "follower #{name} holds the acked write (it arrived inside the re-seed) at " <>
                   "wal_ordinal 0 — 'unknown' — so Promote.fresher?/2 can never rank it and a " <>
                   "failover cold-opens the older object: the acked write is lost (rig " <>
                   "2026-10-14 B1). #{inspect(replica)}"

          assert Promote.fresher?(replica, stamp),
                 "follower #{name}'s replica does not outrank the object it is ahead of: " <>
                   "#{inspect(replica)} vs #{inspect(stamp)}"
        end

        # The node dies before any flush. The survivor holds nothing locally and must pull the
        # acked write from a peer.
        kill_primary!(id, conn)

        assert is_nil(Follower.state_of(Follower, id)),
               "precondition: the survivor holds a replica"

        Application.put_env(:fathom, :replication_promote_on_open, true)
        Application.put_env(:fathom, :replication_recover_from_peers, true)

        {:ok, conn} = ShardExecutor.open(id)

        assert rows(conn) == [1, 999],
               "the survivor served the stored object without the quorum-acked row 999"

        :ok = close(conn)
      end
    end
  end

  describe "the wire" do
    test "a seed_begin round-trips its ordinal under the ordinal gate" do
      begin = %Protocol.SeedBegin{
        shard_id: "s",
        epoch: 1,
        wal_gen: 2,
        salt1: 3,
        wal_offset: 4,
        db_size: 5,
        wal_size: 4,
        lineage: 6,
        wal_ordinal: 7
      }

      assert {:ok, ^begin} =
               begin |> Protocol.encode_seed_begin() |> IO.iodata_to_binary() |> Protocol.decode()
    end

    test "with the ordinal gate off the seed states none (the peer may not know the frame)" do
      Application.put_env(:fathom, :replication_ordinal_wire, false)

      begin = %Protocol.SeedBegin{
        shard_id: "s",
        epoch: 1,
        wal_gen: 2,
        salt1: 3,
        wal_offset: 4,
        db_size: 5,
        wal_size: 4,
        lineage: 6,
        wal_ordinal: 7
      }

      assert {:ok, %Protocol.SeedBegin{wal_ordinal: 0, lineage: 6}} =
               begin |> Protocol.encode_seed_begin() |> IO.iodata_to_binary() |> Protocol.decode()
    end
  end

  describe "an UNSTATED seed is still unrankable" do
    # The "0 means unknown" rule is load-bearing (expert review 2026-08-26 #2): a replica nobody
    # stated an ordinal for must never outrank an object. The fix makes seeds STATE one; it must not
    # make an unstated one rankable.
    test "a seed_begin with wal_ordinal 0 installs a replica fresher?/2 refuses", %{
      id: id,
      root: root
    } do
      {name, port} = start_follower!(root)
      db = :binary.copy(<<0>>, 4096)

      {:ok, sock} =
        :gen_tcp.connect(~c"127.0.0.1", port, [:binary, packet: 4, active: false], 5_000)

      try do
        begin = %Protocol.SeedBegin{
          shard_id: id,
          epoch: 1,
          wal_gen: 0,
          salt1: 9,
          wal_offset: 0,
          db_size: byte_size(db),
          wal_size: 0,
          lineage: 50,
          wal_ordinal: 0
        }

        :ok = :gen_tcp.send(sock, Protocol.encode_seed_begin(begin))
        :ok = :gen_tcp.send(sock, Protocol.encode_seed_chunk(id, :db, 0, db))
        :ok = :gen_tcp.send(sock, Protocol.encode_seed_end(id))
        assert {:ok, bytes} = :gen_tcp.recv(sock, 0, 5_000)
        assert {:ok, {:ack, ^id, _}} = Protocol.decode(bytes)
      after
        :gen_tcp.close(sock)
      end

      replica = Follower.state_of(name, id)
      assert %{lineage: 50, wal_ordinal: 0} = replica
      refute Promote.fresher?(replica, %{epoch: 1, wal_gen: 0, wal_ordinal: 1, offset: 0})
    end
  end
end
