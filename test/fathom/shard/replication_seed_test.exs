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
  alias Fathom.Shard.Replication.Protocol
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

  # FIX B for the 2026-08-12 rig failure, and the DATA claim rather than the flag claim.
  #
  # The test above proves commits keep FLOWING across a checkpoint. It says nothing about whether
  # what the follower holds is still a database — and it was not. A reset truncated the follower's
  # WAL to the new generation and left its `.db` in the old one, so every page the primary's
  # checkpoint drained into ITS `.db` was in NEITHER of the follower's files. Promotion then served
  # a tenant an EMPTY database over a working stored object.
  #
  # The fix is that the follower absorbs its own WAL into its own `.db` before the reset discards
  # it. It already HAS those pages — they are the frames it received — so this is a local
  # checkpoint, not a re-seed: no peer transfer, no S3 request, and a cost proportional to write
  # volume rather than to database size.
  #
  # `docs/reviews/a2-checkpoint-torn-replica-2026-08-12.md`.
  test "a follower ABSORBS its WAL at a reset, so the replica is still a database", ctx do
    %{id: id, root: root} = ctx
    followers = start_followers!(root, 3)
    enable!(followers, 2)

    {coordinator, conn, path} = open_shard!(id)
    wal = path <> "-wal"
    {name, _port} = hd(followers)

    {:ok, _} = Connection.query(conn, "CREATE TABLE t (a INTEGER)", [])
    {:ok, _} = Connection.query(conn, "INSERT INTO t VALUES (1)", [])
    {:ok, _} = Connection.query(conn, "INSERT INTO t VALUES (2)", [])
    assert :ok = Session.commit(id, wal, coordinator)
    await_seeded(name, id, fn -> Session.commit(id, wal, coordinator) end)

    # PRECONDITION, asserted on file size rather than by querying: the schema and both rows are in
    # the follower's WAL and NOT yet in its `.db`. Opening the replica to check would checkpoint it,
    # which is the very thing under test. One page means an empty database — and that is exactly
    # what the rig promoted.
    assert File.stat!(Follower.db_path(name, id)).size <= 4096,
           "the follower's .db already holds the rows, so a lost checkpoint would prove nothing"

    # The seam: TRUNCATE rewrites the WAL with fresh salts, so the next commit ships a reset.
    {:ok, _} = Connection.query(conn, "PRAGMA wal_checkpoint(TRUNCATE)", [])
    {:ok, _} = Connection.query(conn, "INSERT INTO t VALUES (3)", [])
    assert :ok = Session.commit(id, wal, coordinator)

    # THE ASSERTION, and it is checked BEFORE the flag so a regression reports the data loss rather
    # than the bookkeeping. Open the replica the way promotion does: 1 and 2 come back out of the
    # WAL we absorbed, 3 out of the generation that replaced it.
    {:ok, r} = Connection.open(Follower.db_path(name, id))
    rows = Connection.query(r, "SELECT a FROM t ORDER BY a", [])
    :ok = Connection.close(r)

    assert {:ok, %{rows: [[1], [2], [3]]}} = rows,
           "the replica lost the rows the primary's checkpoint moved into its own .db — got " <>
             inspect(rows)

    refute Follower.state_of(name, id).torn,
           "the rows survived but the replica is still flagged torn, so nothing will promote it"
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

  # A RESTARTED FOLLOWER MUST NOT RE-SEED. Its per-shard state is ETS and dies with the process;
  # the replicas on disk do not. Before boot recovery, a node that restarted held a full copy of
  # every shard it followed and did not know it — `state_of/2` returned nil, so every push was
  # refused `:unknown_shard` and the primary re-sent an entire DATABASE per shard. At any real
  # follower count that is a re-seed storm after every deploy, and `Promote` would not promote from
  # bytes sitting right there (it checks `state_of/2` first), which is how a survivor that had
  # restarted lost an acked write on the chaos rig.
  test "a follower that restarts recovers its position from the files, without a re-seed", ctx do
    %{id: id, root: root} = ctx
    followers = start_followers!(root, 2)
    [{name, _port} | _] = followers
    dir = Path.join(root, to_string(name))
    enable!(followers, 1)

    # Drive one real seed + commit so the follower holds genuine bytes.
    {coordinator, conn, path} = open_shard!(id)
    {:ok, _} = Connection.query(conn, "CREATE TABLE t (a INTEGER)", [])
    {:ok, _} = Connection.query(conn, "INSERT INTO t VALUES (1)", [])
    Session.commit(id, path <> "-wal", coordinator)
    await_seeded(name, id, fn -> Session.commit(id, path <> "-wal", coordinator) end)

    before = Follower.state_of(name, id)
    assert before, "the fixture never seeded; this test would prove nothing"

    # Restart it on the SAME directory — a deploy, not a fresh node.
    stop_supervised!(name)
    pid = start_supervised!({Follower, name: name, port: 0, dir: dir}, id: :"#{name}_again")
    _ = :sys.get_state(pid)

    after_restart = Follower.state_of(name, id)

    assert after_restart,
           "the follower forgot #{id} across a restart while still holding its files — every " <>
             "push will be refused :unknown_shard and re-seed a whole database"

    # Position must match the FILES, which is what makes it safe to trust.
    {:ok, hdr} = Wal.read(Follower.wal_path(name, id))
    assert after_restart.next_offset == hdr.size
    assert after_restart.wal_gen == hdr.ckpt_seq
    assert after_restart.salt1 == hdr.salt1
  end

  # THE QUARANTINE MUST SURVIVE A RESTART (expert review 2026-08-20 #11b).
  #
  # The test above proves a restart RECOVERS position from the files, which is the fix for a
  # re-seed storm. But `torn` cannot be derived from the files — nothing on disk records that the
  # `.db` and `-wal` are a generation apart, and the `-shm` that would hint at it is deleted by
  # apply_write. So recovery stamped `torn: false` for every shard, and a node restart (a deploy,
  # a crash, an OOM-kill — all routine) laundered every quarantined replica on that node into a
  # promotable one. `Promote.fresher?/2` then lets it through and `offerable/2` offers it fleet-wide.
  #
  # That is the 2026-08-12 rig failure — a tenant served an EMPTY database over a working stored
  # object — with its guard silently removed by an unrelated fix.
  test "a torn replica is STILL torn after the follower restarts", ctx do
    %{id: id, root: root} = ctx
    followers = start_followers!(root, 2)
    [{name, _port} | _] = followers
    dir = Path.join(root, to_string(name))
    enable!(followers, 1)

    {coordinator, conn, path} = open_shard!(id)
    {:ok, _} = Connection.query(conn, "CREATE TABLE t (a INTEGER)", [])
    {:ok, _} = Connection.query(conn, "INSERT INTO t VALUES (1)", [])
    Session.commit(id, path <> "-wal", coordinator)
    await_seeded(name, id, fn -> Session.commit(id, path <> "-wal", coordinator) end)

    # Mark it torn the way a generation change does, through the public seam.
    torn = %{Follower.state_of(name, id) | torn: true}
    Follower.seed(name, id, torn.epoch, torn.wal_gen, torn.salt1, torn.next_offset)
    :ets.insert(Follower.table(name), {id, torn})
    File.write!(Follower.torn_path(name, id), "")

    assert Follower.state_of(name, id).torn, "the fixture did not actually mark the replica torn"
    refute Follower.offerable(name, id), "a torn replica must not be offered to peers"

    stop_supervised!(name)
    pid = start_supervised!({Follower, name: name, port: 0, dir: dir}, id: :"#{name}_torn_again")
    _ = :sys.get_state(pid)

    after_restart = Follower.state_of(name, id)
    assert after_restart, "the follower forgot the shard entirely across the restart"

    assert after_restart.torn,
           "a restart laundered a TORN replica into a promotable one — its .db is a generation " <>
             "behind its -wal, and Promote.fresher?/2 will now let it be served"

    refute Follower.offerable(name, id),
           "a replica that was torn before the restart is being offered to peers after it"
  end

  # The other direction: a HEALTHY replica must not come back torn, or every restart would force a
  # full re-seed of every shard the node follows.
  test "a healthy replica is still promotable after the follower restarts", ctx do
    %{id: id, root: root} = ctx
    followers = start_followers!(root, 2)
    [{name, _port} | _] = followers
    dir = Path.join(root, to_string(name))
    enable!(followers, 1)

    {coordinator, conn, path} = open_shard!(id)
    {:ok, _} = Connection.query(conn, "CREATE TABLE t (a INTEGER)", [])
    {:ok, _} = Connection.query(conn, "INSERT INTO t VALUES (1)", [])
    Session.commit(id, path <> "-wal", coordinator)
    await_seeded(name, id, fn -> Session.commit(id, path <> "-wal", coordinator) end)

    refute Follower.state_of(name, id).torn, "the fixture started torn; it proves nothing"
    refute File.exists?(Follower.torn_path(name, id)), "a healthy seed left a torn marker behind"

    stop_supervised!(name)
    pid = start_supervised!({Follower, name: name, port: 0, dir: dir}, id: :"#{name}_ok_again")
    _ = :sys.get_state(pid)

    refute Follower.state_of(name, id).torn,
           "a healthy replica came back torn, which forces a needless full re-seed"

    assert Follower.offerable(name, id)
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

  # DETERMINISTIC REPRODUCTION of the CI flake in the test above
  # ("a quiet shard catches a laggard up without waiting on it", OTP 28, seed 212274, open since
  # 2026-08-14 and never reproducible on demand).
  #
  # The flake is neither of the two candidates originally recorded (a stalled catch-up, or too tight
  # a deadline). It is a straggler race, and it needed a follower that RECEIVES a push and withholds
  # its reply — impossible before `Fathom.Test.PausablePeer`, because `Follower` answers from an
  # unlinked `Task` that `:sys.suspend/1` cannot stop.
  #
  # The mechanism, forced here rather than waited for:
  #   1. a and b ack the catch-up commit; the laggard's reply is HELD by the peer.
  #   2. The next commit has NO write, so a and b plan `:nothing`, the quorum is already satisfied
  #      by them, and `deliver/5` takes the fire-and-forget `ship_async/1` branch with the laggard
  #      as the whole push list.
  #   3. The laggard's shipper still holds that shard's single waiter, so the push is refused
  #      `:already_in_flight` BEFORE the socket — and nothing retries it
  #      (`handle_info/2`'s reject path only re-seeds on `:unknown_shard`).
  #
  # On loopback the held reply normally arrives in microseconds, which is why this passes locally
  # and only bit a contended CI runner.
  # Was :flaky-tagged for one commit. Its failures were all in the SETUP step (the initial
  # three-way convergence, before the strand is even arranged) and all three causes turned out to be
  # in `PausablePeer` or this scenario's setup, not the product — see the note in
  # `replication_transport_test.exs`. Un-tagged at 0 failures in 28 runs.
  test "a laggard refused by our own shipper is caught up without waiting for the next write",
       ctx do
    %{id: id, root: root} = ctx
    [{a, pa}, {b, pb}, {laggard, plag}] = start_followers!(root, 3)

    # Only the laggard is proxied; a and b answer directly, exactly as in the flaky test.
    peer = start_supervised!({Fathom.Test.PausablePeer, upstream_port: plag, notify: self()})
    enable!([{a, pa}, {b, pb}, {laggard, Fathom.Test.PausablePeer.port(peer)}], 2)

    {coordinator, conn, path} = open_shard!(id)
    {:ok, _} = Connection.query(conn, "CREATE TABLE t (a INTEGER)", [])
    wal = path <> "-wal"

    for n <- [a, b, laggard], do: Follower.seed(n, id, 0, 0, 0, 0)

    {:ok, _} = Connection.query(conn, "INSERT INTO t VALUES (1)", [])
    assert :ok = Session.commit(id, wal, coordinator)
    early = byte_size(File.read!(wal))

    for n <- 2..4 do
      {:ok, _} = Connection.query(conn, "INSERT INTO t VALUES (?1)", [n])
      assert :ok = Session.commit(id, wal, coordinator)
    end

    # DRIVE to the precondition; do not WAIT for it. This setup used to be the flakiest line in the
    # file, and the cause is the bug this very test is about:
    #
    # the laggard is reached through `PausablePeer`, so the extra hop makes it a straggler on EVERY
    # commit — its shipper still holds that shard's waiter when the next commit ships, so it is
    # refused `:already_in_flight` and misses that delta. It normally catches up on a later commit
    # (a bigger delta from its real position). But if it is refused on the LAST warm-up commit there
    # is no later one, and nothing re-ships — so the shard stays behind forever and the setup fails
    # having proven nothing.
    #
    # That is precisely the strand this test exists to demonstrate, hit accidentally during its own
    # arrangement. Waiting longer cannot fix it (observed failing at 20 s, which is an age on
    # loopback); only another write can, which is exactly how the strand self-heals on a busy shard.
    # So the setup keeps writing until the precondition actually holds.
    converged = drive_until_converged(conn, wal, coordinator, id, [a, b, laggard])

    # Rewind the laggard, exactly as the flaky test does.
    File.write!(Follower.wal_path(laggard, id), binary_part(converged, 0, early))
    {:ok, epoch} = Fathom.Shard.epoch(coordinator)
    {:ok, %{ckpt_seq: gen}} = Wal.read(wal)
    Follower.seed(laggard, id, epoch, gen, 0, early)

    # HOLD the laggard's answer to the next commit. a and b carry the quorum, so the commit still
    # succeeds — which is precisely the trap: nothing about the tenant's write looks wrong.
    :ok = Fathom.Test.PausablePeer.pause(peer)

    {:ok, _} = Connection.query(conn, "INSERT INTO t VALUES (5)", [])
    assert :ok = Session.commit(id, wal, coordinator)
    assert_receive {:peer_frame, :to_primary, _}, 2_000, "the laggard never answered at all"
    assert Fathom.Test.PausablePeer.held(peer) >= 1

    # NO write before this one — the `ship_async/1` path, with the laggard as the whole push list.
    assert :ok = Session.commit(id, wal, coordinator)

    {:ok, _} = Fathom.Test.PausablePeer.release(peer)
    target = File.read!(wal)

    # THE PROPERTY. NO further write on this shard — the deferred retry must re-enter the commit
    # path and re-ship what our own shipper refused. Verified to discriminate by setting
    # `:replication_catchup_ms` to 0, which fails here.
    #
    # WHAT THE 8 s ACTUALLY BUYS, because the previous wording here ("armed at 1 s and may need a
    # second pass") described a schedule the product does not have, and this test went red on CI
    # 2026-08-23 with no way to tell whether the window was the reason.
    #
    # `Session.retry_delay_ms/2` is EXPONENTIAL from `:replication_catchup_ms` (1 s), capped at
    # `:shard_flush_interval_ms`. Neither is set by this file's `setup` or by `config/test.exs`, so
    # the defaults apply: cap = 5 s, and successive refusals land at 1 s, 3 s, 7 s, 12 s cumulative.
    # 8 s is therefore THREE attempts, not "one plus a spare" — and a fourth would need 12 s.
    #
    # Left at 8 s deliberately: on this machine the retry has never needed more than the first
    # attempt (73 local runs, plus 4 in a 4-vCPU Ubuntu container, zero failures), so raising it
    # would be widening a deadline to cure a failure nobody has yet attributed — exactly what
    # AGENTS.md forbids. If CI red-lines HERE again, the uploaded failure log will say so, and the
    # question to answer first is why a THIRD retry was refused, not what number to type.
    assert await_wal_quiet(laggard, id, target, 8_000),
           "the laggard never caught up on a quiet shard. The commit reported :ok, so nothing " <>
             "anywhere looks wrong — this is the silent under-replication window the deferred " <>
             "retry exists to close."

    # Converged on the RIGHT bytes, not merely on some bytes: a retry shipping from a wrong offset
    # would splice, which is far worse than being behind.
    assert File.read!(Follower.wal_path(laggard, id)) == target
  end

  # AN OUTSTANDING EXPECTATION IS NOT OVERWRITTEN (expert review 2026-08-20 #27).
  #
  # `@settled_rejects` deliberately EXCLUDES `:already_in_flight` and `:overloaded` so
  # `reconcile/2` keeps the entry "so that reply can still be reconciled" — its own comment says
  # clearing it strands a laggard permanently. But `deliver/5` did
  # `Map.merge(state.inflight, expectations(...))` BEFORE the pushes were sent, and `Map.merge/2`
  # lets the new map win — so the earlier push's expectation, the one the pending reply needs, was
  # already destroyed by the time the local refusal happened.
  #
  # The genuine ack for the earlier push then failed `settle_late_ack/3`'s offset check (it was
  # compared against the NEWER expectation), declined to advance, and `forget_inflight/2` dropped
  # the entry anyway. So a follower that acked bytes it really holds was not advanced, the next
  # commit planned a delta from a stale position, and only that follower's `:offset_mismatch`
  # corrected the record: the exact symptom the `@settled_rejects` narrowing was written to
  # eliminate, so the fix was believed to be in place when it was not.
  #
  # `:replication_catchup_ms` is 0 for this test ON PURPOSE. The deferred retry (#25) would also
  # repair the position a second later, which would make this pass either way — the point here is
  # that the ACK ITSELF is enough, with no extra round trip.
  test "a held ack still advances the follower after our own shipper refused a later push", ctx do
    %{id: id, root: root} = ctx
    [{a, pa}, {b, pb}, {laggard, plag}] = start_followers!(root, 3)

    prev_catchup = Application.get_env(:fathom, :replication_catchup_ms)
    Application.put_env(:fathom, :replication_catchup_ms, 0)

    on_exit(fn ->
      if is_nil(prev_catchup),
        do: Application.delete_env(:fathom, :replication_catchup_ms),
        else: Application.put_env(:fathom, :replication_catchup_ms, prev_catchup)
    end)

    peer = start_supervised!({Fathom.Test.PausablePeer, upstream_port: plag, notify: self()})
    enable!([{a, pa}, {b, pb}, {laggard, Fathom.Test.PausablePeer.port(peer)}], 2)

    {coordinator, conn, path} = open_shard!(id)
    {:ok, _} = Connection.query(conn, "CREATE TABLE t (a INTEGER)", [])
    wal = path <> "-wal"

    for n <- [a, b, laggard], do: Follower.seed(n, id, 0, 0, 0, 0)

    {:ok, _} = Connection.query(conn, "INSERT INTO t VALUES (1)", [])
    assert :ok = Session.commit(id, wal, coordinator)
    _ = drive_until_converged(conn, wal, coordinator, id, [a, b, laggard])

    [{session, _}] = Registry.lookup(Fathom.Shard.Replication.SessionRegistry, id)
    shippers = Fleet.shippers()
    lag_shipper = List.last(shippers)

    # HOLD the laggard's answer. a and b carry the quorum, so the tenant sees :ok — which is
    # precisely what makes this invisible in production.
    :ok = Fathom.Test.PausablePeer.pause(peer)

    {:ok, _} = Connection.query(conn, "INSERT INTO t VALUES (2)", [])
    assert :ok = Session.commit(id, wal, coordinator)
    assert_receive {:peer_frame, :to_primary, _}, 2_000, "the laggard never answered at all"

    # The position the HELD ack describes. Read from the session's own record so the test does not
    # re-derive the planner's arithmetic.
    first = :sys.get_state(session).inflight[lag_shipper]
    assert first, "the first push left no expectation, so there is nothing to overwrite"

    # A SECOND commit while the first reply is still held. The laggard's shipper is holding this
    # shard's single waiter, so this push is refused `:already_in_flight` before the socket — and
    # it is that refusal path whose expectation must not have been clobbered.
    {:ok, _} = Connection.query(conn, "INSERT INTO t VALUES (3)", [])
    assert :ok = Session.commit(id, wal, coordinator)

    assert :sys.get_state(session).inflight[lag_shipper] == first,
           "the second push overwrote the outstanding expectation the held reply needs"

    {:ok, _} = Fathom.Test.PausablePeer.release(peer)

    # THE PROPERTY: the released ack, on its own, moves the primary's record of the laggard to the
    # position it really holds. With the expectation clobbered this never happens — the ack is
    # compared against the newer offset, declined, and dropped.
    deadline = System.monotonic_time(:millisecond) + 3_000

    advanced =
      Stream.repeatedly(fn ->
        if :sys.get_state(session).followers[lag_shipper] == first,
          do: :advanced,
          else: Process.sleep(10)
      end)
      |> Enum.find(fn
        :advanced -> true
        _ -> System.monotonic_time(:millisecond) > deadline
      end)

    assert advanced == :advanced,
           "the laggard acked bytes it really holds and the primary did not record them. The " <>
             "next commit then plans from a stale position, the follower rejects " <>
             ":offset_mismatch, and only THEN is the record corrected — one wasted round trip " <>
             "and one wasted payload, at the ~10k :already_in_flight per node AGENTS.md records."
  end

  # Commit until every follower holds the primary's WAL, or give up loudly. Bounded, and each round
  # is a real write — the same thing that makes a strand self-heal in production.
  defp drive_until_converged(conn, wal, coordinator, id, followers, rounds \\ 12) do
    Enum.reduce_while(1..rounds, nil, fn n, _acc ->
      target = File.read!(wal)

      if Enum.all?(followers, fn f ->
           File.read(Follower.wal_path(f, id)) == {:ok, target}
         end) do
        {:halt, target}
      else
        if n == rounds do
          flunk(
            "followers never converged in #{rounds} write rounds — this is the SETUP, so the " <>
              "scenario below never ran"
          )
        end

        {:ok, _} = Connection.query(conn, "INSERT INTO t VALUES (?1)", [1000 + n])
        assert :ok = Session.commit(id, wal, coordinator)
        Process.sleep(50)
        {:cont, nil}
      end
    end)
  end

  # `await_wal/4` flunks on timeout; this reports instead, so a test can assert NON-convergence
  # without the failure being the thing it is measuring.
  defp await_wal_quiet(name, id, expected, timeout) do
    deadline = System.monotonic_time(:millisecond) + timeout

    Stream.repeatedly(fn ->
      cond do
        File.read(Follower.wal_path(name, id)) == {:ok, expected} -> true
        System.monotonic_time(:millisecond) > deadline -> false
        true -> Process.sleep(25) && nil
      end
    end)
    |> Enum.find(&is_boolean/1)
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

  # THE ISOLATION GATE (expert review 2026-08-20 #1, and the test gap #37 named).
  #
  # Every other test in this file drives the receive path through a real primary, which only ever
  # sends legitimate shard ids. Nothing drove it with a HOSTILE one — and the listener is
  # unauthenticated, so a hostile id is exactly what an attacker supplies. The follower built every
  # filesystem path straight from `shard_id`, so `seed_begin(shard_id: "../fathom_shards/victim")`
  # + chunks + `seed_end` had `install/3` File.rename an attacker-supplied database over another
  # tenant's LIVE file. AGENTS.md's shard-isolation gate calls a cross-tenant leak a release
  # blocker, so this is pinned at the wire, not at the helper.
  #
  # Asserts on a DIRECTORY LISTING rather than on the absence of one path: a traversal that lands
  # somewhere unanticipated must fail this too.
  describe "the receive path validates shard ids off the wire (#1)" do
    # `../victim_data/escaped` is the load-bearing one: it aims the traversal squarely at the
    # neighbouring directory's file, so the "another tenant's database was overwritten" assertion
    # below is the one that fires when the gate is removed — not merely "stray files appeared".
    @hostile [
      "../victim_data/escaped",
      "../escaped",
      "../../escaped",
      "a/b",
      "..",
      ".",
      "has space",
      "acme.evil",
      <<"nul", 0, "byte">>,
      "",
      String.duplicate("a", 5_000)
    ]

    test "a hostile shard_id creates nothing and closes the connection", %{root: root} do
      dir = Path.join(root, "gate_follower")
      name = :"gate_f#{System.unique_integer([:positive])}"
      pid = start_supervised!({Follower, name: name, port: 0, dir: dir}, id: name)
      {:ok, port} = Follower.port(pid)

      # A neighbouring directory standing in for the live shard data dir. Under the DEFAULTS these
      # are literal siblings (System.tmp_dir!()/fathom_replication and .../fathom_shards), which is
      # what makes a single `../` reach a tenant database.
      victim_dir = Path.join(root, "victim_data")
      File.mkdir_p!(victim_dir)
      victim = Path.join(victim_dir, "escaped.db")
      File.write!(victim, "REAL TENANT BYTES")

      before_repl = File.ls!(dir) |> Enum.sort()

      for bad <- @hostile do
        {:ok, sock} =
          :gen_tcp.connect(~c"127.0.0.1", port, [:binary, packet: 4, active: false], 2_000)

        begin = %Protocol.SeedBegin{
          shard_id: bad,
          epoch: 1,
          wal_gen: 0,
          salt1: 0,
          wal_offset: 0,
          db_size: byte_size("ATTACKER"),
          wal_size: 0
        }

        :ok = :gen_tcp.send(sock, Protocol.encode_seed_begin(begin))
        _ = :gen_tcp.send(sock, Protocol.encode_seed_chunk(bad, :db, 0, "ATTACKER"))
        _ = :gen_tcp.send(sock, Protocol.encode_seed_end(bad))

        # The gate treats a bad identifier as a framing failure, so the connection is closed. The
        # sends above may therefore fail — that is the point, not an error.
        _ = :gen_tcp.recv(sock, 0, 500)
        :gen_tcp.close(sock)
      end

      assert File.read!(victim) == "REAL TENANT BYTES",
             "a hostile shard_id off the replication wire overwrote another tenant's live database"

      assert File.ls!(victim_dir) |> Enum.sort() == ["escaped.db"],
             "a hostile shard_id created files in a directory outside the follower's"

      assert File.ls!(dir) |> Enum.sort() == before_repl,
             "a hostile shard_id created files inside the follower dir"

      refute File.exists?(Path.join(root, "escaped.db")),
             "a hostile shard_id escaped one level out of the follower dir"

      # The listener is still alive and serving — a rejected frame must not take it down.
      assert {:ok, _} = Follower.port(pid)
    end

    test "the path builders themselves fail closed", %{root: root} do
      dir = Path.join(root, "gate_paths")
      name = :"gate_p#{System.unique_integer([:positive])}"
      start_supervised!({Follower, name: name, port: 0, dir: dir}, id: name)

      for bad <- @hostile do
        assert_raise ArgumentError, fn -> Follower.db_path(name, bad) end
        assert_raise ArgumentError, fn -> Follower.wal_path(name, bad) end
      end

      # A legitimate id still resolves inside the follower dir.
      assert Follower.db_path(name, "acme") == Path.join(dir, "acme.db")
    end
  end

  # DISK BACK-PRESSURE ON THE REPLICA STORE (expert review 2026-08-20 #23).
  #
  # A follower holds a full .db + -wal copy of EVERY shard it follows, with no count cap, no byte
  # cap, no free-space floor and no retention — and it grows from OTHER NODES' write traffic. Both
  # it and the live shard data dir default under System.tmp_dir!(), so out of the box they share a
  # volume, and a peer's write rate can fill the disk this node's own durability flushes and
  # cold-open pulls depend on. That is the unbounded-RPO failure AGENTS.md documents: SQLite's small
  # WAL appends keep succeeding while every flush fails, so writes are acked and never made durable.
  #
  # Refusing a NEW seed is the unambiguous half: that shard's RPO simply stays at its stored object,
  # which is the pre-A2 behaviour and always correct. What to SHED once already full is a policy
  # question and is deliberately parked.
  describe "the replica store refuses to grow past its free-space floor (#23)" do
    setup do
      prev = Application.get_env(:fathom, :replication_disk_free_floor_bytes)

      on_exit(fn ->
        if is_nil(prev),
          do: Application.delete_env(:fathom, :replication_disk_free_floor_bytes),
          else: Application.put_env(:fathom, :replication_disk_free_floor_bytes, prev)
      end)

      :ok
    end

    test "a seed is refused when the volume is below the floor, and the listener stays up", ctx do
      %{root: root} = ctx
      dir = Path.join(root, "pressure")
      name = :"press_f#{System.unique_integer([:positive])}"
      pid = start_supervised!({Follower, name: name, port: 0, dir: dir}, id: name)
      {:ok, port} = Follower.port(pid)

      # A floor no real volume can clear. This is the same shape as the warm cache's
      # :warm_disk_free_floor_bytes, which is the bound this store never had.
      Application.put_env(:fathom, :replication_disk_free_floor_bytes, 1_000_000_000_000_000)

      {:ok, sock} =
        :gen_tcp.connect(~c"127.0.0.1", port, [:binary, packet: 4, active: false], 2_000)

      begin = %Protocol.SeedBegin{
        shard_id: "pressured",
        epoch: 1,
        wal_gen: 0,
        salt1: 0,
        wal_offset: 0,
        db_size: 4096,
        wal_size: 0
      }

      :ok = :gen_tcp.send(sock, Protocol.encode_seed_begin(begin))

      _ =
        :gen_tcp.send(
          sock,
          Protocol.encode_seed_chunk("pressured", :db, 0, :binary.copy(<<0>>, 4096))
        )

      :ok = :gen_tcp.send(sock, Protocol.encode_seed_end("pressured"))
      _ = :gen_tcp.recv(sock, 0, 1_000)
      :gen_tcp.close(sock)

      refute File.exists?(Follower.db_path(name, "pressured")),
             "the follower installed a replica while its volume was below the free-space floor — " <>
               "this store grows from other nodes' traffic and has no retention, so it fills the " <>
               "disk THIS node's flushes need"

      refute Follower.state_of(name, "pressured"),
             "the follower recorded replication state for a shard it refused to seed"

      # Back-pressure is not a crash: the listener is still serving.
      assert {:ok, ^port} = Follower.port(pid)
    end

    test "with headroom the same seed is accepted", ctx do
      %{root: root} = ctx
      dir = Path.join(root, "headroom")
      name = :"head_f#{System.unique_integer([:positive])}"
      pid = start_supervised!({Follower, name: name, port: 0, dir: dir}, id: name)
      {:ok, port} = Follower.port(pid)

      # One byte: any real volume clears it. Proves the refusal above is the FLOOR talking and not
      # the seed path being broken.
      Application.put_env(:fathom, :replication_disk_free_floor_bytes, 1)

      {:ok, sock} =
        :gen_tcp.connect(~c"127.0.0.1", port, [:binary, packet: 4, active: false], 2_000)

      begin = %Protocol.SeedBegin{
        shard_id: "roomy",
        epoch: 1,
        wal_gen: 0,
        salt1: 0,
        wal_offset: 0,
        db_size: 4096,
        wal_size: 0
      }

      :ok = :gen_tcp.send(sock, Protocol.encode_seed_begin(begin))

      :ok =
        :gen_tcp.send(
          sock,
          Protocol.encode_seed_chunk("roomy", :db, 0, :binary.copy(<<0>>, 4096))
        )

      :ok = :gen_tcp.send(sock, Protocol.encode_seed_end("roomy"))
      assert {:ok, _} = :gen_tcp.recv(sock, 0, 2_000)
      :gen_tcp.close(sock)

      assert File.exists?(Follower.db_path(name, "roomy")),
             "the floor refused a seed on a volume with room, which would stop replication entirely"

      assert {:ok, ^port} = Follower.port(pid)
    end
  end
end
