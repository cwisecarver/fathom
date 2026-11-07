defmodule Fathom.Shard.Replication.FollowerSeedRefusalTest do
  @moduledoc """
  Expert review 2026-11-07 #43: a follower below its free-space floor must stop the primary
  re-streaming the whole database on every seed cycle.

  The refusal used to surface only at `seed_end`: below the floor every push for a torn or unknown
  replica was answered `:unknown_shard` (the seed request), `Session.start_seeds/3` pinned a snapshot
  and streamed `seed_begin` + every chunk + `seed_end`, the follower had dropped the begin so every
  chunk was discarded, and only `seed_end` answered `:internal`. The next commit asked again. A
  floor event on one node turned every active shard's commit rhythm into whole-database streams to it.

  Two halves, both on the FOLLOWER (the side that knows why it refuses; the primary only ever sees
  `:internal`, which it also gets for causes that SHOULD be retried at once, so a primary-side backoff
  on it would turn a transient `replica_busy` into seconds of lost quorum):

    * **early refusal** — below the floor (bytes or inodes, the cached reading `reserve_headroom?/2`
      reads) a push that would be answered `:unknown_shard` is answered `:internal` instead, a settled
      reject that starts no seed;
    * **refusal memory** — a seed_begin refused for ITS SIZE (the floor would hold for a small one)
      is remembered per shard for `:replication_seed_refusal_ms`, and pushes in that window get the
      same `:internal`: at most one whole-database stream per window, not one per commit.
  """
  use ExUnit.Case, async: false

  import Fathom.Test.ReplicationCase

  alias Fathom.Shard.Connection
  alias Fathom.Shard.Replication.{Follower, Protocol, Session}
  alias Fathom.Shards
  alias Fathom.Test.{Await, Env}

  describe "answering the push" do
    setup do
      name = :"seedrefuse_#{System.unique_integer([:positive])}"
      dir = Path.join(System.tmp_dir!(), to_string(name))
      pid = start_supervised!({Follower, name: name, port: 0, dir: dir}, id: name)
      on_exit(fn -> File.rm_rf(dir) end)

      Env.put!(:replication_disk_free_floor_bytes, 1_000_000)
      Env.put!(:replication_disk_free_floor_inodes, 1)

      # Boot housekeeping (which starts the priming refresh) has run once this returns; wait for the
      # reading to land, or it would overwrite the injected one.
      _ = :sys.get_state(pid)
      locks = Follower.locks(name)

      Await.until(
        fn ->
          match?([{_, _, _}], :ets.lookup(locks, {:disk, dir})) and
            :ets.lookup(locks, {:disk_refresh, dir}) == []
        end,
        "no volume reading landed"
      )

      {:ok, l} = :gen_tcp.listen(0, [:binary, packet: 4, active: false])
      {:ok, port} = :inet.port(l)
      {:ok, client} = :gen_tcp.connect(~c"127.0.0.1", port, [:binary, packet: 4, active: false])
      {:ok, server} = :gen_tcp.accept(l)
      :gen_tcp.close(l)

      worker = Follower.spawn_worker(server, name, nil, self(), :atomics.new(1, []))
      Process.unlink(worker)
      on_exit(fn -> Process.exit(worker, :kill) end)

      handler = "seedrefuse-#{System.unique_integer([:positive])}"
      test_pid = self()

      :ok =
        :telemetry.attach(
          handler,
          [:fathom, :replication, :disk_pressure],
          fn _e, _m, meta, _ -> send(test_pid, {:disk_pressure, meta.shard_id}) end,
          nil
        )

      on_exit(fn -> :telemetry.detach(handler) end)

      id = "seedrefuse_#{System.unique_integer([:positive])}"
      %{name: name, dir: dir, worker: worker, client: client, id: id}
    end

    # A reading that is never refreshed inside the test (its stamp is in the future), saying the
    # volume has `free` bytes.
    defp volume!(%{name: name, dir: dir}, free) do
      at = System.monotonic_time(:millisecond) + 600_000
      :ets.insert(Follower.locks(name), {{:disk, dir}, at, {:ok, free, nil}})
    end

    defp push(id, offset),
      do: %Protocol.Push{
        shard_id: id,
        epoch: 1,
        wal_gen: 0,
        salt1: 0,
        offset: offset,
        payload: "frames"
      }

    defp ask(%{worker: worker, client: client}, push) do
      send(worker, {:frame, push, 10})
      assert_receive {:frame_done, 10}, 5_000
      {:ok, reply} = :gen_tcp.recv(client, 0, 5_000)
      {:ok, decoded} = Protocol.decode(reply)
      decoded
    end

    test "below the floor an unknown shard is refused :internal (settled), above it :unknown_shard",
         ctx do
      volume!(ctx, 100)

      assert {:reject, _, :internal, 0} = ask(ctx, push(ctx.id, 0)),
             "a follower under its floor still asked the primary for a whole-database seed"

      assert_receive {:disk_pressure, id}, 1_000
      assert id == ctx.id

      # Control, same follower, same push: headroom restored => the seed request, so the fixture
      # really reaches the `:unknown_shard` path the refusal replaces.
      volume!(ctx, 10_000_000)
      assert {:reject, _, :unknown_shard, 0} = ask(ctx, push(ctx.id, 0))
    end

    test "a torn replica asking for a re-seed is refused the same way", ctx do
      %{name: name, id: id} = ctx
      :ok = Follower.seed(name, id, 1, 0, 0, 4096)
      File.write!(Follower.db_path(name, id), "the replica's database")
      refute File.exists?(Follower.wal_path(name, id)), "fixture: the WAL must be missing"

      volume!(ctx, 100)

      # The append finds its WAL gone: marks the replica torn and used to ask for a re-seed.
      assert {:reject, _, :internal, _} = ask(ctx, push(id, 4096))
      assert %{torn: true} = Follower.state_of(name, id)

      # Torn now, so the next push is the seed request (`request_seed/4`) — refused too.
      assert {:reject, _, :internal, _} = ask(ctx, push(id, 4096))

      volume!(ctx, 10_000_000)
      assert {:reject, _, :unknown_shard, _} = ask(ctx, push(id, 4096))
    end

    test "a seed refused for its SIZE is remembered: pushes in the window are not seed requests",
         ctx do
      %{name: name, id: id} = ctx
      # Headroom for a small seed but not for this one: 10 MB free over a 1 MB floor.
      volume!(ctx, 10_000_000)
      Env.put!(:replication_seed_refusal_ms, 50)

      big = %Protocol.SeedBegin{
        shard_id: id,
        epoch: 1,
        wal_gen: 0,
        salt1: 0,
        wal_offset: 0,
        db_size: 9_500_000,
        wal_size: 0
      }

      # Before any refusal the push IS the seed request.
      assert {:reject, _, :unknown_shard, 0} = ask(ctx, push(id, 0))

      assert %{} == Follower.begin_seed(name, %{}, big), "the oversized begin was admitted"
      assert_receive {:disk_pressure, ^id}, 1_000

      assert {:reject, _, :internal, 0} = ask(ctx, push(id, 0)),
             "the primary was invited to re-stream a database the follower just refused"

      # The memory ends: the next push may ask again (once per window, not once per commit).
      Await.until(
        fn -> match?({:reject, _, :unknown_shard, 0}, ask(ctx, push(id, 0))) end,
        "the refusal memory never expired"
      )
    end

    test "the memory is per shard", ctx do
      %{name: name} = ctx
      volume!(ctx, 10_000_000)
      other = ctx.id <> "_other"

      big = %Protocol.SeedBegin{
        shard_id: ctx.id,
        epoch: 1,
        wal_gen: 0,
        salt1: 0,
        wal_offset: 0,
        db_size: 9_500_000,
        wal_size: 0
      }

      assert %{} == Follower.begin_seed(name, %{}, big)
      assert {:reject, _, :internal, 0} = ask(ctx, push(ctx.id, 0))
      assert {:reject, _, :unknown_shard, 0} = ask(ctx, push(other, 0))
    end
  end

  describe "end to end: a primary against a follower under its floor" do
    setup do
      id = "seedrefuse_e2e_#{System.unique_integer([:positive])}"
      root = Path.join(System.tmp_dir!(), "seedrefuse_#{System.unique_integer([:positive])}")
      File.mkdir_p!(root)

      on_exit(fn ->
        Session.stop(id)
        Shards.drain(id, 5_000)
        for e <- ["", "-wal", "-shm", ".etag"], do: File.rm(Fathom.Shard.db_path(id) <> e)
        File.rm_rf(root)
      end)

      %{id: id, root: root}
    end

    # `seed_begin`s that reach a follower for `id`, per follower, counted by tracing
    # `Follower.begin_seed/4`.
    defp begins_for(id), do: receive_traces(id, %{})

    defp receive_traces(id, acc) do
      receive do
        {:trace, _pid, :call, {Follower, :begin_seed, [name, _seeds, %{shard_id: ^id}, _opts]}} ->
          receive_traces(id, Map.update(acc, name, 1, &(&1 + 1)))

        {:trace, _, _, _} ->
          receive_traces(id, acc)
      after
        0 -> acc
      end
    end

    test "N commits stream NO database to it, and one commit seeds it once the floor clears",
         ctx do
      %{id: id, root: root} = ctx
      [{name, _port} | _] = followers = start_followers!(root, 2, "seedrefuse")
      enable_replication!(followers, quorum: 1, connected: :all)

      {:ok, coordinator, ref, path} = Shards.checkout(id)
      {:ok, conn} = Connection.open(path)
      {:ok, _} = Connection.query(conn, "CREATE TABLE t (a BLOB)", [])

      # Floor no volume clears: the follower refuses every seed. The refusal memory is short here so
      # the control below does not wait out the production 30 s once the floor clears.
      Env.put!(:replication_disk_free_floor_bytes, :erlang.bsl(1, 60))
      Env.put!(:replication_seed_refusal_ms, 50)

      :erlang.trace_pattern({Follower, :begin_seed, 4}, true, [:local])
      :erlang.trace(:all, true, [:call, {:tracer, self()}])

      try do
        for _ <- 1..4 do
          {:ok, _} = Connection.query(conn, "INSERT INTO t VALUES (randomblob(20000))", [])
          assert {:error, _} = Session.commit(id, path <> "-wal", coordinator)
        end

        # The FIRST commit seeds a never-seeded follower directly, with no push to answer, so one
        # begin per follower is legitimate (and is refused, and remembered). Every later commit asks
        # by push, which the follower now answers `:internal`: pre-fix each commit streamed again.
        refused = begins_for(id)

        assert Enum.all?(refused, fn {_follower, begins} -> begins <= 1 end),
               "a follower under its floor was streamed a whole database more than once: " <>
                 inspect(refused)

        # Control: the floor clears and the very next commit seeds it, so the count above is
        # measuring a real path (the tracer works, and the refusal was the floor, not a broken seed).
        Env.put!(:replication_disk_free_floor_bytes, 1)
        {:ok, _} = Connection.query(conn, "INSERT INTO t VALUES (randomblob(20000))", [])

        Await.until(
          fn ->
            _ = Session.commit(id, path <> "-wal", coordinator)
            match?(%{next_offset: o} when o > 0, Follower.state_of(name, id))
          end,
          "the follower was never seeded once its floor cleared"
        )

        assert map_size(begins_for(id)) >= 1
      after
        :erlang.trace(:all, false, [:call])
        :erlang.trace_pattern({Follower, :begin_seed, 4}, false, [:local])
      end

      Connection.close(conn)
      Fathom.Shard.checkin(coordinator, ref)
    end
  end
end
