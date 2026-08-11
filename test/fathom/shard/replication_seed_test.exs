defmodule Fathom.Shard.ReplicationSeedTest do
  @moduledoc """
  Seeding a follower — Phase 2 A2. See `docs/a2-quorum-replication.md`.

  A follower with no base copy refuses every push with `:unknown_shard`, so until this works
  replication cannot start at all. The primary treats that reject not as a fault but as the signal
  to send a base copy, out of band, and the next commit finds the follower ready.

  ## The seed CANNOT come from S3, and this is the reason

  Fathom's durable object is a `VACUUM INTO` snapshot — a rebuilt, defragmented database. Measured
  on the same data: **65,536 bytes against 118,784 live**, not byte-identical. WAL frames reference
  page numbers in the *primary's* layout, so appending them to a VACUUM'd copy applies the right
  frames to the wrong pages, silently.

  That also means A1's `WarmFollower`, which pulls from S3, is **not** a valid A2 seed source
  despite the design doc's table describing A2 as "the same component with the data path reversed".
  The base copy has to be the primary's live bytes. `seeds_from_live_bytes_not_a_vacuum_snapshot`
  below pins that, so nobody re-optimises the seed into an S3 pull.
  """
  use ExUnit.Case, async: false

  alias Fathom.Shard.Connection
  alias Fathom.Shard.Replication.Fleet
  alias Fathom.Shard.Replication.Follower
  alias Fathom.Shard.Replication.Session
  alias Fathom.Shard.Replication.Shipper
  alias Fathom.Shard.Replication.Wal
  alias Fathom.Shards

  setup do
    id = "repl_seed_#{System.unique_integer([:positive])}"
    root = Path.join(System.tmp_dir!(), "replseed_#{System.unique_integer([:positive])}")
    File.mkdir_p!(root)

    prev = %{
      enabled: Application.get_env(:fathom, :replication_enabled),
      followers: Application.get_env(:fathom, :replication_followers),
      quorum: Application.get_env(:fathom, :replication_quorum),
      chunk: Application.get_env(:fathom, :replication_seed_chunk_bytes)
    }

    on_exit(fn ->
      Session.stop(id)

      for {k, v} <- [
            replication_enabled: prev.enabled,
            replication_followers: prev.followers,
            replication_quorum: prev.quorum,
            replication_seed_chunk_bytes: prev.chunk
          ] do
        if is_nil(v),
          do: Application.delete_env(:fathom, k),
          else: Application.put_env(:fathom, k, v)
      end

      File.rm_rf(root)
      for s <- ["", "-wal", "-shm"], do: File.rm(Fathom.Shard.db_path(id) <> s)
    end)

    %{id: id, root: root}
  end

  defp start_followers!(root, n) do
    for i <- 1..n do
      name = :"seed_f#{i}_#{System.unique_integer([:positive])}"
      dir = Path.join(root, to_string(name))
      pid = start_supervised!({Follower, name: name, port: 0, dir: dir}, id: name)
      {:ok, port} = Follower.port(pid)
      {name, port}
    end
  end

  defp enable!(followers, q) do
    Application.put_env(:fathom, :replication_enabled, true)
    Application.put_env(:fathom, :replication_quorum, q)

    Application.put_env(
      :fathom,
      :replication_followers,
      for({_n, port} <- followers, do: {~c"127.0.0.1", port})
    )

    start_supervised!(Fleet)
    await_connected!()
  end

  # Shippers connect in `handle_continue`, so without this the FIRST commit races the connect and
  # every follower answers `:disconnected` — not `:unknown_shard`, so no seed is triggered and the
  # test looks like seeding is broken. Production self-heals (the next commit finds them connected
  # and seeds), but a test that depends on that race is a flake.
  defp await_connected!(timeout \\ 5_000) do
    deadline = System.monotonic_time(:millisecond) + timeout

    Stream.repeatedly(fn ->
      if Enum.all?(Fleet.shippers(), &Shipper.connected?/1),
        do: :connected,
        else: Process.sleep(20)
    end)
    |> Enum.find(fn
      :connected -> true
      _ -> System.monotonic_time(:millisecond) > deadline
    end)
    |> case do
      :connected -> :ok
      _ -> flunk("shippers never connected")
    end
  end

  defp open_shard!(id) do
    {:ok, coordinator, ref, path} = Shards.checkout(id)
    on_exit(fn -> Fathom.Shard.checkin(coordinator, ref) end)
    {:ok, conn} = Connection.open(path)
    on_exit(fn -> Connection.close(conn) end)
    {coordinator, conn, path}
  end

  # Seeding is asynchronous AND converges over a few commits rather than one: `ship_quorum/4`
  # returns as soon as the outcome is known, so a straggler's `:unknown_shard` arrives after the
  # fact and is picked up by the next commit's drain. So this drives commits while it waits, which
  # is exactly what a live shard under write traffic does.
  defp await_seeded(name, id, commit, timeout \\ 5_000) do
    deadline = System.monotonic_time(:millisecond) + timeout

    Stream.repeatedly(fn ->
      if Follower.state_of(name, id) do
        :seeded
      else
        commit.()
        Process.sleep(25)
      end
    end)
    |> Enum.find(fn
      :seeded -> true
      _ -> System.monotonic_time(:millisecond) > deadline
    end)
    |> case do
      :seeded -> :ok
      _ -> flunk("follower #{name} was never seeded for #{id}")
    end
  end

  # THE FIRST WRITE IS THE ONE THAT MATTERS, and nothing pinned it until the chaos rig found it
  # failing on 2026-08-11. Every other seed test here reaches its interesting state THROUGH the
  # first commit, so all of them passed while that commit returned an error to the tenant — the
  # suite proved seeding worked and said nothing about what the client saw.
  #
  # A tenant's very first INSERT is the most visible request fathom serves: it is what an unchanged
  # Django app does on its first save for a new tenant, and returning FILO_NO_QUORUM there is a
  # crash in the application, once per tenant, forever.
  test "the FIRST commit to a shard succeeds, rather than failing while the seed runs", ctx do
    %{id: id, root: root} = ctx
    followers = start_followers!(root, 3)
    enable!(followers, 2)

    {coordinator, conn, path} = open_shard!(id)
    {:ok, _} = Connection.query(conn, "CREATE TABLE t (a INTEGER, b TEXT)", [])
    {:ok, _} = Connection.query(conn, "INSERT INTO t VALUES (1, 'first')", [])

    wal = path <> "-wal"

    # No follower has ever heard of this shard. Assert that precondition, or a seeded fixture would
    # make this pass without exercising the path at all.
    for {name, _} <- followers do
      refute Follower.state_of(name, id),
             "#{name} already knows #{id}; this test cannot observe the first-commit path"
    end

    assert :ok = Session.commit(id, wal, coordinator),
           "the first commit to a shard returned an error while its seed ran — this is a " <>
             "tenant's first INSERT failing, once per tenant"

    # And it really did replicate, rather than reporting success on an empty quorum.
    #
    # A QUORUM's worth, not all three — and the difference is the design, not slack in the test.
    # `ship_quorum/3` returns at the Q-th ack and stragglers catch up asynchronously (the whole
    # reason Q=N is refused), so the third follower may still be seeding when the commit returns.
    # Asserting all three passed only because an earlier draft waited for every seed; that would
    # have quietly made the first write pay for the SLOWEST replica, which is exactly the cost
    # quorum replication exists to avoid.
    holders =
      Enum.count(followers, fn {name, _} ->
        File.exists?(Follower.db_path(name, id)) and
          File.read!(Follower.db_path(name, id)) == File.read!(path)
      end)

    assert holders >= 2,
           "only #{holders} follower(s) hold the primary's bytes; the commit reported success on " <>
             "a quorum that was not actually met"
  end

  # THE RIG BUG, REPRODUCED LOCALLY (2026-08-11). On the chaos rig every commit after the first
  # failed `{:no_quorum, :impossible}` — so no replicated WRITE had ever succeeded multi-node,
  # while this suite was green.
  #
  # The environment gap is a CHECKPOINT BETWEEN COMMITS. Every test here holds one connection for
  # its whole run, so the WAL grows monotonically and the generation never moves. On the rig each
  # statement is its own Hrana stream: the connection closes, the WAL is checkpointed, and the next
  # commit ships from a new generation with fresh salts. `Primary.plan/2` and `FollowerLog.decide/2`
  # both have a case for exactly that, and neither had ever been exercised END TO END through a
  # commit — only as pure-function unit tests.
  #
  # Per AGENTS.md: an environment that cannot express the topology cannot catch bugs in it. This
  # closes that gap for good, so the class stays visible without a Docker rig.
  test "commits keep replicating ACROSS a WAL checkpoint, not just within one generation", ctx do
    %{id: id, root: root} = ctx
    followers = start_followers!(root, 3)
    enable!(followers, 2)

    {coordinator, conn, path} = open_shard!(id)
    wal = path <> "-wal"

    {:ok, _} = Connection.query(conn, "CREATE TABLE t (a INTEGER)", [])
    {:ok, _} = Connection.query(conn, "INSERT INTO t VALUES (1)", [])
    assert :ok = Session.commit(id, wal, coordinator)

    gen_before = Wal.read(wal) |> then(fn {:ok, h} -> h.ckpt_seq end)

    # The seam. TRUNCATE rewrites the WAL with fresh salts and bumps the checkpoint sequence, which
    # is exactly what a stream close does on the rig.
    {:ok, _} = Connection.query(conn, "PRAGMA wal_checkpoint(TRUNCATE)", [])
    {:ok, _} = Connection.query(conn, "INSERT INTO t VALUES (2)", [])

    gen_after = Wal.read(wal) |> then(fn {:ok, h} -> h.ckpt_seq end)

    assert gen_after != gen_before,
           "the checkpoint did not move the generation — this test would prove nothing"

    assert :ok = Session.commit(id, wal, coordinator),
           "a commit after a WAL checkpoint was not replicated. This is the rig failure: every " <>
             "write after the first returned FILO_NO_QUORUM because each Hrana stream close " <>
             "checkpoints, so every commit crosses a generation boundary"

    # And a third, to prove it keeps working rather than alternating.
    {:ok, _} = Connection.query(conn, "PRAGMA wal_checkpoint(TRUNCATE)", [])
    {:ok, _} = Connection.query(conn, "INSERT INTO t VALUES (3)", [])
    assert :ok = Session.commit(id, wal, coordinator)
  end

  # THE RIG BLOCKER, REPRODUCED (2026-08-11). On the chaos rig every commit after the first failed
  # `{:no_quorum, :impossible}`, forever, with all four followers answering
  # `offset_mismatch: 8272` — the same number every time, so a deadlock rather than a lost frame.
  #
  # The environment gap is CONNECTION CLOSE, not the checkpoint the test above covers. Every test
  # in this file holds one connection for its whole run. On the rig each statement is its own Hrana
  # stream, and when the last connection to a shard closes SQLite DELETES the `-wal`; the next
  # stream creates a fresh one with new random salts and `ckpt_seq` back at ZERO.
  #
  # That is the one transition where the two sides disagree. `Primary.plan/2` treats a salt change
  # as a new generation (`when seq != gen or s != salt`) and ships `{:reset, 0, size}`.
  # `FollowerLog.decide/2` compares only `wal_gen` — the wire has no `salt1` at all — so it sees
  # the same generation, demands its own `next_offset`, and rejects offset 0. Neither side can
  # move: the primary will only send 0, the follower will only accept 8272.
  #
  # A checkpoint does NOT reproduce it, which is why the test above passes: TRUNCATE bumps
  # `ckpt_seq` too, so both sides agree a generation boundary happened.
  test "replication survives the WAL being RECREATED when all connections close", ctx do
    %{id: id, root: root} = ctx
    followers = start_followers!(root, 3)
    enable!(followers, 2)

    {coordinator, conn, path} = open_shard!(id)
    wal = path <> "-wal"

    {:ok, _} = Connection.query(conn, "CREATE TABLE t (a INTEGER)", [])
    {:ok, _} = Connection.query(conn, "INSERT INTO t VALUES (1)", [])
    assert :ok = Session.commit(id, wal, coordinator)

    {:ok, before} = Wal.read(wal)

    # The seam: drop every connection, so SQLite removes the WAL, then open a fresh one.
    :ok = Connection.close(conn)
    {:ok, conn2} = Connection.open(path)
    on_exit(fn -> Connection.close(conn2) end)

    {:ok, _} = Connection.query(conn2, "INSERT INTO t VALUES (2)", [])
    {:ok, now} = Wal.read(wal)

    assert now.salt1 != before.salt1,
           "the WAL was not recreated with fresh salts — this test would prove nothing"

    assert :ok = Session.commit(id, wal, coordinator),
           "a commit after the WAL was recreated was not replicated. Primary and follower " <>
             "disagree about whether a new WAL lineage began: the primary ships from 0 on a salt " <>
             "change, the follower only compares wal_gen and demands its old offset. Deadlock."

    # It must keep working, not alternate.
    {:ok, _} = Connection.query(conn2, "INSERT INTO t VALUES (3)", [])
    assert :ok = Session.commit(id, wal, coordinator)
  end

  test "an unseeded follower is seeded automatically, then replicates", ctx do
    %{id: id, root: root} = ctx
    followers = start_followers!(root, 3)
    enable!(followers, 2)

    {coordinator, conn, path} = open_shard!(id)
    {:ok, _} = Connection.query(conn, "CREATE TABLE t (a INTEGER, b TEXT)", [])
    {:ok, _} = Connection.query(conn, "INSERT INTO t VALUES (1, 'hello')", [])

    wal = path <> "-wal"

    # NOTHING is seeded. Every follower answers :unknown_shard, the commit starts a seed for each,
    # WAITS for them inside the call, and then succeeds.
    #
    # THIS ASSERTION USED TO BE `{:error, {:no_quorum, _}}` — the seed was started out of band and
    # the triggering commit failed, on the reasoning that a tenant's write is the worst place to
    # put a multi-megabyte transfer. The chaos rig measured the consequence (2026-08-11): the FIRST
    # write to every shard returned FILO_NO_QUORUM, i.e. an OperationalError on an unchanged Django
    # app's first INSERT for that tenant, once per tenant. The wait is bounded by the caller's own
    # deadline, so a shard too large to seed inside it still fails exactly as before.
    assert :ok = Session.commit(id, wal, coordinator)

    # ...but that reject started a seed for each follower, out of band.
    for {name, _} <- followers,
        do: await_seeded(name, id, fn -> Session.commit(id, wal, coordinator) end)

    # Now the same commit path succeeds, and the followers hold the primary's bytes exactly.
    assert :ok = Session.commit(id, wal, coordinator)

    primary_db = File.read!(path)
    primary_wal = File.read!(wal)

    for {name, _} <- followers do
      assert File.read!(Follower.db_path(name, id)) == primary_db,
             "follower #{name}'s database is not the primary's live bytes"

      assert File.read!(Follower.wal_path(name, id)) == primary_wal,
             "follower #{name}'s WAL diverged after seeding"
    end
  end

  # A commit returns at the Q-th ack, so a follower outside the quorum may still be writing when it
  # does. Convergence is what is promised; "converged by the time the call returned" is not.
  defp await_wal(name, id, expected, timeout \\ 5_000) do
    deadline = System.monotonic_time(:millisecond) + timeout

    Stream.repeatedly(fn ->
      if File.read(Follower.wal_path(name, id)) == {:ok, expected},
        do: :converged,
        else: Process.sleep(10)
    end)
    |> Enum.find(fn
      :converged -> true
      _ -> System.monotonic_time(:millisecond) > deadline
    end)
    |> case do
      :converged -> :ok
      _ -> flunk("#{name} never converged on the primary's WAL")
    end
  end

  test "a follower that joins late is seeded mid-stream and converges", ctx do
    %{id: id, root: root} = ctx
    followers = start_followers!(root, 3)
    enable!(followers, 2)

    {coordinator, conn, path} = open_shard!(id)
    {:ok, _} = Connection.query(conn, "CREATE TABLE t (a INTEGER)", [])
    wal = path <> "-wal"

    # Seed only TWO of the three, by hand, at the start of the WAL. The third joins the stream
    # already in progress: its `:unknown_shard` starts a seed out of band, which captures the WAL
    # at whatever size it had reached by then — BEHIND a and b, who keep committing meanwhile. One
    # delta cannot be correct for both positions, which is the whole reason offsets are per-follower.
    [{a, _}, {b, _}, {laggard, _}] = followers
    for name <- [a, b], do: Follower.seed(name, id, 0, 0, 0, 0)

    for n <- 1..25 do
      {:ok, _} = Connection.query(conn, "INSERT INTO t VALUES (?1)", [n])

      # Quorum of 2 is satisfied by a and b, so these SUCCEED while the third is still catching up.
      assert :ok = Session.commit(id, wal, coordinator)
    end

    # This deliberately does NOT assert the laggard is still unseeded first. It used to, and that
    # precondition contradicted the design: an unseeded follower is seeded automatically on its
    # first reject, so the assertion could only ever hold if seeding was broken.
    await_seeded(laggard, id, fn -> Session.commit(id, wal, coordinator) end)
    assert byte_size(File.read!(Follower.wal_path(a, id))) > 0

    for n <- 26..40 do
      {:ok, _} = Connection.query(conn, "INSERT INTO t VALUES (?1)", [n])
      assert :ok = Session.commit(id, wal, coordinator)
    end

    primary = File.read!(wal)

    for {name, _} <- followers do
      await_wal(name, id, primary)
    end
  end

  # The companion to the test above, and the deterministic half. Auto-seeding always lands a new
  # follower at the CURRENT offset, so how far behind a late joiner starts is a matter of timing —
  # good coverage of the mechanism, but it cannot pin the arithmetic. Here the gap is created by
  # hand and its size is known, so the delta the primary must compute is a specific one.
  test "a follower that falls behind is caught up from ITS OWN offset", ctx do
    %{id: id, root: root} = ctx
    followers = start_followers!(root, 3)
    enable!(followers, 2)

    {coordinator, conn, path} = open_shard!(id)
    {:ok, _} = Connection.query(conn, "CREATE TABLE t (a INTEGER)", [])
    wal = path <> "-wal"

    [{_a, _}, {_b, _}, {laggard, _}] = followers
    for {name, _} <- followers, do: Follower.seed(name, id, 0, 0, 0, 0)

    sizes =
      for n <- 1..5 do
        {:ok, _} = Connection.query(conn, "INSERT INTO t VALUES (?1)", [n])
        assert :ok = Session.commit(id, wal, coordinator)
        byte_size(File.read!(wal))
      end

    converged = File.read!(wal)
    for {name, _} <- followers, do: await_wal(name, id, converged)

    # Rewind ONE follower to where it stood four commits ago: a replica restored from an older copy,
    # or one that missed frames while its link was down. `early` is a WAL size observed right after
    # a commit, so it is a real frame boundary rather than an arbitrary byte.
    [early | _] = sizes
    {:ok, epoch} = Fathom.Shard.epoch(coordinator)
    {:ok, %{ckpt_seq: gen}} = Wal.read(wal)
    File.write!(Follower.wal_path(laggard, id), binary_part(converged, 0, early))
    Follower.seed(laggard, id, epoch, gen, 0, early)

    # The primary still believes the laggard is current, and that is the point: this push lands at
    # the offset a and b are at, which the laggard refuses — reporting where it actually is. a and b
    # still carry the quorum, so the tenant's commit succeeds while the correction is recorded.
    {:ok, _} = Connection.query(conn, "INSERT INTO t VALUES (100)", [])
    assert :ok = Session.commit(id, wal, coordinator)

    refute File.read!(Follower.wal_path(laggard, id)) == File.read!(wal),
           "the laggard accepted a delta computed from another follower's offset — a single " <>
             "shared offset would splice frames into the wrong position and never error"

    # ...and THIS commit ships it a delta from `early`, a different range from the one a and b get.
    {:ok, _} = Connection.query(conn, "INSERT INTO t VALUES (101)", [])
    assert :ok = Session.commit(id, wal, coordinator)

    primary = File.read!(wal)
    for {name, _} <- followers, do: await_wal(name, id, primary)
  end

  # The one arrangement where the quorum is satisfied entirely by followers nobody has to wait for:
  # a and b already hold every byte, and the laggard is the only follower with anything to receive.
  #
  # This is where treating "already current" as "did not answer" broke a tenant's write. The push
  # list shrinks to the single laggard while the configured quorum is still 2, and `Quorum.new/2`
  # refuses `q >= n` — correctly, as a CONFIG guard — so the commit died on a fleet that was in fact
  # over-replicated. The laggard must still be sent its delta, though: a follower only leaves the
  # laggard set by receiving bytes, so skipping it would strand it there for good.
  test "a quiet shard catches a laggard up without waiting on it", ctx do
    %{id: id, root: root} = ctx
    followers = start_followers!(root, 3)
    enable!(followers, 2)

    {coordinator, conn, path} = open_shard!(id)
    {:ok, _} = Connection.query(conn, "CREATE TABLE t (a INTEGER)", [])
    wal = path <> "-wal"

    [{_a, _}, {_b, _}, {laggard, _}] = followers
    for {name, _} <- followers, do: Follower.seed(name, id, 0, 0, 0, 0)

    {:ok, _} = Connection.query(conn, "INSERT INTO t VALUES (1)", [])
    assert :ok = Session.commit(id, wal, coordinator)
    early = byte_size(File.read!(wal))

    for n <- 2..4 do
      {:ok, _} = Connection.query(conn, "INSERT INTO t VALUES (?1)", [n])
      assert :ok = Session.commit(id, wal, coordinator)
    end

    converged = File.read!(wal)
    for {name, _} <- followers, do: await_wal(name, id, converged)

    File.write!(Follower.wal_path(laggard, id), binary_part(converged, 0, early))
    {:ok, epoch} = Fathom.Shard.epoch(coordinator)
    {:ok, %{ckpt_seq: gen}} = Wal.read(wal)
    Follower.seed(laggard, id, epoch, gen, 0, early)

    # One write, so the primary learns from the laggard's refusal where it actually is.
    {:ok, _} = Connection.query(conn, "INSERT INTO t VALUES (5)", [])
    assert :ok = Session.commit(id, wal, coordinator)

    # NO write before this one. a and b plan nothing at all; the laggard is the whole push list.
    assert :ok = Session.commit(id, wal, coordinator)

    await_wal(laggard, id, File.read!(wal))
  end

  # The wire-level guarantees are pinned in `replication_seed_chunk_test.exs`, which drives the
  # protocol directly. This one covers what that cannot: the SENDING half — `Session.do_seed/5`
  # walking a real shard file with `pread` a chunk at a time, in the right part order, and the
  # consistency check now sitting after the last chunk rather than after a `File.read`.
  #
  # The chunk size is set well below the database size so a seed here is genuinely many frames;
  # at the 4 MiB default every test shard is one chunk and the streaming loop never runs past its
  # first iteration.
  test "a real shard seeds across many chunks and lands byte-identical", ctx do
    %{id: id, root: root} = ctx
    followers = start_followers!(root, 3)
    Application.put_env(:fathom, :replication_seed_chunk_bytes, 4_096)
    enable!(followers, 2)

    {coordinator, conn, path} = open_shard!(id)
    {:ok, _} = Connection.query(conn, "CREATE TABLE t (a INTEGER PRIMARY KEY, b TEXT)", [])

    for i <- 1..500 do
      {:ok, _} =
        Connection.query(conn, "INSERT INTO t VALUES (?1, ?2)", [i, String.duplicate("x", 400)])
    end

    # Checkpoint, or this fixture proves nothing: in WAL mode every row above is still IN the
    # `-wal` and the `.db` is a 4 KB stub, so the database half would be a single chunk. The
    # TRUNCATE moves it all into the main file, and the inserts after it leave a small live WAL —
    # which is also the realistic shape, a large base plus a short tail.
    {:ok, _} = Connection.query(conn, "PRAGMA wal_checkpoint(TRUNCATE)", [])

    for i <- 501..505 do
      {:ok, _} = Connection.query(conn, "INSERT INTO t VALUES (?1, ?2)", [i, "tail"])
    end

    wal = path <> "-wal"

    assert File.stat!(path).size > 8 * 4_096,
           "the fixture is too small to span chunks — this test would prove nothing"

    assert File.stat!(wal).size > 0, "the WAL half of the seed is empty; only the db is covered"

    # Previously `{:error, {:no_quorum, _}}`: the commit that triggers a seed now waits for it.
    # See the note at the top of this file's first seed test.
    assert :ok = Session.commit(id, wal, coordinator)

    for {name, _} <- followers,
        do: await_seeded(name, id, fn -> Session.commit(id, wal, coordinator) end)

    assert :ok = Session.commit(id, wal, coordinator)

    primary_db = File.read!(path)
    primary_wal = File.read!(wal)

    for {name, _} <- followers do
      assert File.read!(Follower.db_path(name, id)) == primary_db,
             "#{name}'s database did not survive a multi-chunk seed intact"

      assert File.read!(Follower.wal_path(name, id)) == primary_wal
    end

    # No `.seeding` temp should outlive a successful seed on any follower.
    for {name, _} <- followers do
      leftovers =
        Follower.dir(name) |> File.ls!() |> Enum.filter(&String.ends_with?(&1, ".seeding"))

      assert leftovers == [], "#{name} leaked seed temps: #{inspect(leftovers)}"
    end
  end

  test "seeds from LIVE bytes, not a VACUUM INTO snapshot", ctx do
    %{id: id, root: root} = ctx
    followers = start_followers!(root, 3)
    enable!(followers, 2)

    {coordinator, conn, path} = open_shard!(id)
    {:ok, _} = Connection.query(conn, "CREATE TABLE t (a INTEGER PRIMARY KEY, b TEXT)", [])

    for i <- 1..300 do
      {:ok, _} =
        Connection.query(conn, "INSERT INTO t VALUES (?1, ?2)", [i, String.duplicate("x", 200)])
    end

    # Churn, so a VACUUM'd copy would be visibly smaller than the live file — without free pages
    # to reclaim the two could coincidentally match and this test would prove nothing.
    {:ok, _} = Connection.query(conn, "DELETE FROM t WHERE a % 2 = 0", [])
    {:ok, _} = Connection.query(conn, "PRAGMA wal_checkpoint(TRUNCATE)", [])
    {:ok, _} = Connection.query(conn, "INSERT INTO t VALUES (99999, 'after')", [])

    wal = path <> "-wal"
    # Previously `{:error, {:no_quorum, _}}`: the commit that triggers a seed now waits for it.
    # See the note at the top of this file's first seed test.
    assert :ok = Session.commit(id, wal, coordinator)

    for {name, _} <- followers,
        do: await_seeded(name, id, fn -> Session.commit(id, wal, coordinator) end)

    {name, _} = hd(followers)
    seeded = File.read!(Follower.db_path(name, id))
    live = File.read!(path)

    assert seeded == live,
           "the seed is not the primary's live file — if this became an S3/VACUUM pull, WAL " <>
             "frames would reference page numbers the copy does not share"

    vacuumed = Path.join(root, "vacuumed.db")
    {:ok, _} = Connection.query(conn, "VACUUM INTO ?1", [vacuumed])

    refute File.read!(vacuumed) == live,
           "the VACUUM'd snapshot matched the live file byte-for-byte, so this fixture no longer " <>
             "demonstrates the layout difference the seed design depends on"
  end
end
