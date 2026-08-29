defmodule Fathom.Shard.ReplicationCommitTest do
  @moduledoc """
  The commit-path integration for A2 — see `docs/a2-quorum-replication.md`.

  Everything below this point has been tested in pieces: the wire format, the two pure decisions,
  the socket shells, and the WAL header reader. This is the one test that runs the whole chain on a
  **real shard with a real coordinator lease** — plan the delta, read it out of SQLite's own WAL,
  ship it to two followers, wait for the quorum, and land the identical bytes on the other side.

  The assertion that matters is byte equality between the primary's `-wal` and each follower's. Any
  off-by-one in the offset arithmetic, any confusion about the header, and the files differ.
  """
  use ExUnit.Case, async: false

  alias Fathom.Shard.Connection
  alias Fathom.Shard.Replication.Fleet
  alias Fathom.Shard.Replication.Follower
  alias Fathom.Shard.Replication.Session
  alias Fathom.Shards

  setup do
    id = "repl_commit_#{System.unique_integer([:positive])}"
    root = Path.join(System.tmp_dir!(), "replcommit_#{System.unique_integer([:positive])}")
    File.mkdir_p!(root)

    prev = [
      enabled: Application.get_env(:fathom, :replication_enabled),
      followers: Application.get_env(:fathom, :replication_followers),
      quorum: Application.get_env(:fathom, :replication_quorum)
    ]

    on_exit(fn ->
      Session.stop(id)

      for {k, v} <- [
            replication_enabled: prev[:enabled],
            replication_followers: prev[:followers],
            replication_quorum: prev[:quorum]
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

  # THREE followers with a quorum of two. Not two-and-two: `Q < N` is enforced at construction (and
  # now at boot), because Q=N tolerates zero failures — the first version of this test configured
  # 2-of-2 and every commit raised, which is the guard working exactly as intended.
  defp start_followers!(root, n) do
    for i <- 1..n do
      name = :"rc_f#{i}_#{System.unique_integer([:positive])}"
      dir = Path.join(root, to_string(name))
      pid = start_supervised!({Follower, name: name, port: 0, dir: dir}, id: name)
      {:ok, port} = Follower.port(pid)
      {name, port}
    end
  end

  # A commit returns at the Q-th ack, so the follower that did NOT make the quorum can still be
  # writing when it does. Asserting on all N immediately asserts a guarantee A2 deliberately does
  # not make — stopping at Q is the entire measured value of a quorum (2-of-4 at 1.6 ms against
  # 4-of-4 at 134 ms). Written that way this test failed about half its runs, and the diagnosis
  # looked exactly like a replication bug: one follower short by one delta.
  #
  # So the two real promises are asserted separately: **a quorum's worth hold the bytes when the
  # commit returns**, and **every follower converges** shortly after.
  defp holders(followers, id, expected) do
    Enum.count(followers, fn {name, _} ->
      File.read(Follower.wal_path(name, id)) == {:ok, expected}
    end)
  end

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
      _ -> flunk("follower #{name} never converged on the primary's WAL")
    end
  end

  test "a committed write reaches a follower quorum, byte-identical", ctx do
    %{id: id, root: root} = ctx

    followers = start_followers!(root, 3)

    Application.put_env(:fathom, :replication_enabled, true)
    Application.put_env(:fathom, :replication_quorum, 2)

    Application.put_env(
      :fathom,
      :replication_followers,
      for({_n, port} <- followers, do: {~c"127.0.0.1", port})
    )

    start_supervised!(Fleet)

    # A real shard with a real coordinator, so the epoch shipped is a real lease epoch.
    {:ok, coordinator, ref, path} = Shards.checkout(id)
    on_exit(fn -> Fathom.Shard.checkin(coordinator, ref) end)

    {:ok, conn} = Connection.open(path)
    on_exit(fn -> Connection.close(conn) end)

    {:ok, _} = Connection.query(conn, "CREATE TABLE t (a INTEGER, b TEXT)", [])
    {:ok, _} = Connection.query(conn, "INSERT INTO t VALUES (1, 'hello')", [])

    wal = path <> "-wal"
    # Every follower must be seeded before frames mean anything — the deliberate distinction
    # between "never seen this shard" and "at offset 0".
    for {name, _} <- followers, do: Follower.seed(name, id, 0, 0, 0, 0)

    assert :ok = Session.commit(id, wal, coordinator)

    primary_bytes = File.read!(wal)
    assert byte_size(primary_bytes) > 0, "the primary wrote no WAL — this test measured nothing"

    assert holders(followers, id, primary_bytes) >= 2,
           "the commit returned without a quorum's worth of followers holding the bytes"

    for {name, _} <- followers, do: await_wal(name, id, primary_bytes)

    # A second commit ships only the delta, and the files stay identical.
    {:ok, _} = Connection.query(conn, "INSERT INTO t VALUES (2, 'world')", [])
    assert :ok = Session.commit(id, wal, coordinator)

    grown = File.read!(wal)
    assert byte_size(grown) > byte_size(primary_bytes), "the second commit appended nothing"

    assert holders(followers, id, grown) >= 2

    for {name, _} <- followers, do: await_wal(name, id, grown)
  end

  test "committing with nothing new ships nothing and still succeeds", ctx do
    %{id: id, root: root} = ctx
    followers = start_followers!(root, 3)

    Application.put_env(:fathom, :replication_enabled, true)
    Application.put_env(:fathom, :replication_quorum, 2)

    Application.put_env(
      :fathom,
      :replication_followers,
      for({_n, port} <- followers, do: {~c"127.0.0.1", port})
    )

    start_supervised!(Fleet)

    {:ok, coordinator, ref, path} = Shards.checkout(id)
    on_exit(fn -> Fathom.Shard.checkin(coordinator, ref) end)
    {:ok, conn} = Connection.open(path)
    on_exit(fn -> Connection.close(conn) end)
    {:ok, _} = Connection.query(conn, "CREATE TABLE t (a)", [])

    wal = path <> "-wal"
    for {name, _} <- followers, do: Follower.seed(name, id, 0, 0, 0, 0)

    assert :ok = Session.commit(id, wal, coordinator)
    # No write in between: a no-op commit must not cost a round trip, and must not error.
    assert :ok = Session.commit(id, wal, coordinator)
  end

  test "an unreachable quorum fails the commit rather than reporting success", ctx do
    %{id: id, root: root} = ctx
    [{name, port} | _] = start_followers!(root, 1)

    Application.put_env(:fathom, :replication_enabled, true)
    Application.put_env(:fathom, :replication_quorum, 2)
    # One real follower, one pointed at a closed port: a quorum of 2 can never form.
    # One live follower, two pointed at a closed port: a quorum of 2 can never form.
    Application.put_env(:fathom, :replication_followers, [
      {~c"127.0.0.1", port},
      {~c"127.0.0.1", 1},
      {~c"127.0.0.1", 2}
    ])

    start_supervised!(Fleet)

    {:ok, coordinator, ref, path} = Shards.checkout(id)
    on_exit(fn -> Fathom.Shard.checkin(coordinator, ref) end)
    {:ok, conn} = Connection.open(path)
    on_exit(fn -> Connection.close(conn) end)
    {:ok, _} = Connection.query(conn, "CREATE TABLE t (a)", [])

    Follower.seed(name, id, 0, 0, 0, 0)

    assert {:error, {:no_quorum, :impossible}} =
             Session.commit(id, path <> "-wal", coordinator)
  end

  # REGRESSION — `state.inflight` retained followers that had already answered.
  #
  # `inflight` means "this follower still owes us a reply to the push we sent it". `advance/2`
  # cleared it on an ack and `reconcile/2` consumed it for `:offset_mismatch`, but EVERY other
  # reject reason — `:disconnected`, `:already_in_flight`, `:stale_wal_gen`, `:overloaded`, all of
  # which the 2026-08-17 rig runs produced in the thousands — left the entry behind forever.
  #
  # Two consequences, both quiet. `settle_inflight/3` (the catch-up loop's between-round wait)
  # burns the caller's whole deadline waiting on a follower that already spoke. And any future
  # "skip the followers that are busy" optimisation reads a permanently-stale map and skips a
  # follower that is in fact free — silently under-replicating it.
  test "a follower that rejects is no longer recorded as owing a reply", ctx do
    %{id: id, root: root} = ctx
    [{name, port} | _] = start_followers!(root, 1)

    Application.put_env(:fathom, :replication_enabled, true)
    Application.put_env(:fathom, :replication_quorum, 2)

    # Two live followers would both ack and clear their own entries via `advance/2`, proving
    # nothing. The closed ports reject `:disconnected` — a reason `reconcile/2` used to ignore.
    Application.put_env(:fathom, :replication_followers, [
      {~c"127.0.0.1", port},
      {~c"127.0.0.1", 1},
      {~c"127.0.0.1", 2}
    ])

    start_supervised!(Fleet)

    {:ok, coordinator, ref, path} = Shards.checkout(id)
    on_exit(fn -> Fathom.Shard.checkin(coordinator, ref) end)
    {:ok, conn} = Connection.open(path)
    on_exit(fn -> Connection.close(conn) end)
    {:ok, _} = Connection.query(conn, "CREATE TABLE t (a)", [])

    Follower.seed(name, id, 0, 0, 0, 0)

    # The quorum cannot form (2 of the 3 are closed ports), which is not what is under test — it is
    # simply the cheapest way to make two followers reject for a reason that is not
    # `:offset_mismatch`.
    assert {:error, {:no_quorum, _}} = Session.commit(id, path <> "-wal", coordinator)

    [{session, _}] = Registry.lookup(Fathom.Shard.Replication.SessionRegistry, id)

    # POLL, do not sample once. `ship_quorum/4` returns the instant the outcome is decided — here as
    # soon as the quorum is `:impossible` — so a straggler's `:disconnected` can still be on the
    # wire when the call returns. It is cleared by `handle_info/2` a moment later. Reading the state
    # exactly once made this test fail about one run in twenty (seed 12244).
    #
    # This still discriminates: WITHOUT the fix the entries are never cleared for any reason but
    # `:offset_mismatch`, so the poll exhausts its deadline and fails rather than converging.
    inflight = await_empty_inflight(session)

    assert inflight == %{},
           "followers that already answered are still recorded as mid-flight: " <>
             "#{inspect(Map.keys(inflight))}. `settle_inflight/3` will wait out the full " <>
             "deadline on them, and a skip-the-busy optimisation would never ship to them again."
  end

  defp await_empty_inflight(session, timeout \\ 2_000) do
    deadline = System.monotonic_time(:millisecond) + timeout

    Stream.repeatedly(fn ->
      inflight = :sys.get_state(session).inflight

      if inflight == %{} or System.monotonic_time(:millisecond) > deadline,
        do: inflight,
        else: nil
    end)
    |> Enum.find(&(&1 != nil))
  end

  # REGRESSION — the 1024-tenant OOM. See `docs/reviews/a2-shipper-feedback-loop-2026-08-16.md`.
  #
  # THE SYMPTOM: nodes OOM-killed (exit 137) about two minutes into a 1024-tenant `tpc-fleet` run
  # with replication on, on a 94 GiB VM. The cause is a positive feedback loop, not a leak — a push
  # carries the WAL since the follower's last ACK, so a delayed send makes the next payload bigger,
  # which delays it further. `Primary.plan/3` caps the delta, which breaks the loop but means one
  # round no longer necessarily hands a follower everything.
  #
  # THE INVARIANT THIS PINS is the one that cap could plausibly have broken: a commit must not ack
  # until the quorum holds every byte through the commit point. `Session.ship/4` therefore ships in
  # bounded ROUNDS until the followers are current. Delete that loop and this test fails — the
  # commit returns `:ok` having shipped only the first `@small_cap` bytes, and the follower WALs
  # differ from the primary's forever after.
  @small_cap 4096

  test "a WAL far larger than the per-push cap still converges byte-identically", ctx do
    %{id: id, root: root} = ctx
    followers = start_followers!(root, 3)

    prev_cap = Application.get_env(:fathom, :replication_max_push_bytes)
    Application.put_env(:fathom, :replication_max_push_bytes, @small_cap)

    on_exit(fn ->
      if prev_cap,
        do: Application.put_env(:fathom, :replication_max_push_bytes, prev_cap),
        else: Application.delete_env(:fathom, :replication_max_push_bytes)
    end)

    Application.put_env(:fathom, :replication_enabled, true)
    Application.put_env(:fathom, :replication_quorum, 2)

    Application.put_env(
      :fathom,
      :replication_followers,
      for({_n, port} <- followers, do: {~c"127.0.0.1", port})
    )

    start_supervised!(Fleet)

    {:ok, coordinator, ref, path} = Shards.checkout(id)
    on_exit(fn -> Fathom.Shard.checkin(coordinator, ref) end)
    {:ok, conn} = Connection.open(path)
    on_exit(fn -> Connection.close(conn) end)

    {:ok, _} = Connection.query(conn, "CREATE TABLE t (a INTEGER, b TEXT)", [])
    blob = String.duplicate("x", 1024)

    for i <- 1..120 do
      {:ok, _} = Connection.query(conn, "INSERT INTO t VALUES (?, ?)", [i, blob])
    end

    wal = path <> "-wal"
    primary_bytes = File.read!(wal)

    # PRECONDITION. Without this the test can silently degenerate into the ordinary one-round case
    # — a WAL under the cap ships whole, the loop never runs a second round, and a deleted loop
    # would pass. Asserted rather than assumed, because that is exactly the shape of the "test
    # passes both ways" failure this project keeps finding.
    assert byte_size(primary_bytes) > @small_cap * 3,
           "the WAL is #{byte_size(primary_bytes)}B against a #{@small_cap}B cap — this test " <>
             "needs a delta big enough to force several rounds, or it measures nothing"

    for {name, _} <- followers, do: Follower.seed(name, id, 0, 0, 0, 0)

    assert :ok = Session.commit(id, wal, coordinator)

    assert holders(followers, id, primary_bytes) >= 2,
           "the commit returned before a quorum held the whole WAL — the catch-up loop stopped " <>
             "short, so the ack claims durability the followers do not have"

    for {name, _} <- followers, do: await_wal(name, id, primary_bytes)

    # And it keeps working once caught up: the next commit is an ordinary small append, planned
    # from the position the capped rounds left behind. An off-by-one in the resume arithmetic
    # shows up here as a permanent `:offset_mismatch` rather than at the seam above.
    {:ok, _} = Connection.query(conn, "INSERT INTO t VALUES (999, 'tail')", [])
    assert :ok = Session.commit(id, wal, coordinator)

    grown = File.read!(wal)
    assert byte_size(grown) > byte_size(primary_bytes), "the follow-up commit appended nothing"
    assert holders(followers, id, grown) >= 2
    for {name, _} <- followers, do: await_wal(name, id, grown)
  end

  # THE PUSH PATH'S TWO READS (expert review 2026-08-20 #16).
  #
  # `ship/5` reads the WAL header, `Primary.plan/3` decides a range from it, and `build_pushes/5`
  # then opens the file AGAIN to read the bytes — while stamping the push with the FIRST read's
  # ckpt_seq and salt1. Nothing serialises that against the coordinator's flush task, which runs
  # off-process by design, or against the tenant's own commit thread, which `fathom_udf` lets
  # checkpoint at 4000 frames.
  #
  # HONESTY ABOUT WHAT THESE TWO TESTS DO. Forcing a checkpoint to land BETWEEN the two reads needs
  # a hook inside `Session` that does not exist, and a probabilistic guard on a data-correctness
  # path is worse than an honest one. So this follows the precedent `flush_position_test` set for
  # the same shape of problem: pin the PREMISE behaviourally (the hazard is real and the existing
  # guard cannot see it) and pin the FIX structurally (the re-read is where it has to be).
  describe "the header must still describe the bytes when they ship (#16)" do
    test "PREMISE: a checkpoint moves the header while the byte range stays readable" do
      path = Path.join(System.tmp_dir!(), "wal16_#{System.unique_integer([:positive])}.db")
      wal = path <> "-wal"
      on_exit(fn -> for sfx <- ["", "-wal", "-shm"], do: File.rm(path <> sfx) end)

      {:ok, conn} = Connection.open(path)
      {:ok, _} = Connection.query(conn, "PRAGMA wal_autocheckpoint=0", [])
      {:ok, _} = Connection.query(conn, "CREATE TABLE t (a INTEGER, b TEXT)", [])

      for i <- 1..50 do
        {:ok, _} = Connection.query(conn, "INSERT INTO t VALUES (?1, ?2)", [i, "row#{i}"])
      end

      {:ok, before} = Fathom.Shard.Replication.Wal.read(wal)

      # A range the primary would legitimately plan from `before`.
      off = Fathom.Shard.Replication.Wal.header_bytes()
      len = before.commit_extent - off
      assert len > 0, "the fixture wrote no frames"

      # PASSIVE checkpoint, then a write — the sequence Wal's own moduledoc says need not shrink
      # the file. This is what the periodic durability flush does, off-process, on any dirty shard.
      {:ok, _} = Connection.query(conn, "PRAGMA wal_checkpoint(PASSIVE)", [])
      {:ok, _} = Connection.query(conn, "INSERT INTO t VALUES (999, 'after')", [])

      {:ok, after_} = Fathom.Shard.Replication.Wal.read(wal)

      assert {after_.ckpt_seq, after_.salt1} != {before.ckpt_seq, before.salt1},
             "the checkpoint did not change the WAL's identity, so this fixture demonstrates " <>
               "nothing — check whether PASSIVE still restarts the log"

      # AND THE EXISTING GUARD CANNOT SEE IT. read_delta/3 only fails on a SHORT read, so the same
      # range reads back fine and would have shipped stamped with `before`'s identity.
      assert {:ok, bin} = Fathom.Shard.Replication.Wal.read_delta(wal, off, len)

      assert byte_size(bin) == len,
             "read_delta refused the range, which would mean the file shrank — the dangerous " <>
               "case is precisely the one where it does not"

      :ok = Connection.close(conn)
    end

    test "FIX: build_pushes re-reads the header and checks it after reading the payloads" do
      source = File.read!("lib/fathom/shard/replication/session.ex")

      [_, body] =
        String.split(source, "defp build_pushes(state, wal_path, epoch, header, plans) do",
          parts: 2
        )

      [body, _] = String.split(body, "\n  defp ", parts: 2)

      # The call names changed with expert review 2026-08-26 #32 — both reads now go through a
      # HELD, inode-revalidated fd (`read_delta_held/4`, `read_held/2`) instead of opening the file
      # each time. The invariant this test pins is the ORDER of the two reads and the check, which
      # is unchanged; only the names moved. A `:nomatch` here means the names moved again, not that
      # the ordering broke — fix the strings, then re-read the assertions below to confirm the
      # ordering still holds.
      payload_at = :binary.match(body, "Wal.read_delta_held(fd, wal_path, off, len)") |> elem(0)
      reread_at = :binary.match(body, "Wal.read_held(fd, wal_path)") |> elem(0)

      check_at =
        :binary.match(body, "stable?(header, after_, :checkpoint_during_push)") |> elem(0)

      assert reread_at > payload_at,
             "build_pushes/5 re-reads the WAL header BEFORE reading the payloads, which proves " <>
               "nothing: the whole point is that the header may move while the bytes are read."

      assert check_at > reread_at,
             "build_pushes/5 does not compare the re-read header against the one the push is " <>
               "stamped with. Without that, a checkpoint landing between the two reads ships " <>
               "new-generation bytes under the old generation's identity — the follower records " <>
               "our salt while holding frames whose per-frame salts differ, SQLite's checksums " <>
               "read them as a torn tail, and the frames are silently dropped on a replica we " <>
               "already acked as quorum-durable."
    end
  end

  # CATCH-UP BACKOFF (expert review 2026-08-20 #25).
  #
  # `arm_if_rejected/2` fires on ANY non-empty reject list, and `:disconnected` is in that list --
  # `Shipper` produces one for every push to a follower whose socket is down, and `drop/2` produces
  # one for every waiter when a link fails. There was no backoff, no attempt cap and no "this peer
  # is not coming back" condition, so with one peer down EVERY session on the node re-entered the
  # full plan-and-read commit path once per second, indefinitely, on shards with zero tenant
  # traffic. At the ~200 shards/node the rig holds at 1024 tenants that is ~200 pointless cycles a
  # second, each blocking its session for up to 5 s inside `handle_info`, so real client commits
  # queue behind a retry for a peer that is not coming back.
  #
  # Tested at the pure-function level because the defect is arithmetic and the alternative --
  # observing wall-clock timer intervals -- is the flaky shape AGENTS.md warns about.
  describe "the catch-up retry backs off (#25)" do
    setup do
      prev_catchup = Application.get_env(:fathom, :replication_catchup_ms)
      prev_flush = Application.get_env(:fathom, :shard_flush_interval_ms)
      Application.put_env(:fathom, :replication_catchup_ms, 1_000)
      Application.put_env(:fathom, :shard_flush_interval_ms, 30_000)

      on_exit(fn ->
        restore = fn k, v ->
          if is_nil(v),
            do: Application.delete_env(:fathom, k),
            else: Application.put_env(:fathom, k, v)
        end

        restore.(:replication_catchup_ms, prev_catchup)
        restore.(:shard_flush_interval_ms, prev_flush)
      end)

      :ok
    end

    defp transient(n), do: for(i <- 1..n, do: {:"s#{i}", :already_in_flight, 0})
    defp gone(n), do: for(i <- 1..n, do: {:"s#{i}", :disconnected, 0})

    test "the FIRST refusal still retries fast -- that is what makes a quiet shard's laggard good" do
      assert Session.retry_delay_ms(1, transient(1)) == 1_000
    end

    test "consecutive refusals double, and stop at the flush interval" do
      assert Session.retry_delay_ms(2, transient(1)) == 2_000
      assert Session.retry_delay_ms(3, transient(1)) == 4_000
      assert Session.retry_delay_ms(4, transient(1)) == 8_000
      assert Session.retry_delay_ms(5, transient(1)) == 16_000

      # Capped at :shard_flush_interval_ms, and it STAYS capped -- Integer.pow with an uncapped
      # exponent would overflow into an absurd timer rather than converge.
      assert Session.retry_delay_ms(6, transient(1)) == 30_000
      assert Session.retry_delay_ms(50, transient(1)) == 30_000
      assert Session.retry_delay_ms(5_000, transient(1)) == 30_000
    end

    test "a peer with no socket goes straight to the cap -- retrying sooner cannot help" do
      assert Session.retry_delay_ms(1, gone(1)) == 30_000
      assert Session.retry_delay_ms(1, gone(3)) == 30_000
    end

    test "a MIXED list is treated as transient, because part of it can still be made good" do
      mixed = gone(1) ++ transient(1)
      assert Session.retry_delay_ms(1, mixed) == 1_000
    end

    test "catchup_ms 0 still disables the retry entirely" do
      Application.put_env(:fathom, :replication_catchup_ms, 0)
      assert Session.retry_delay_ms(1, transient(1)) == 0
      assert Session.retry_delay_ms(9, gone(1)) == 0
    end

    test "a flush interval BELOW the base delay never shortens the first retry" do
      # The rig defaults the flush interval 60x tighter than production (AGENTS.md #33). A cap
      # under the base would otherwise make the backoff a speed-up.
      Application.put_env(:fathom, :shard_flush_interval_ms, 200)
      assert Session.retry_delay_ms(1, transient(1)) == 1_000
      assert Session.retry_delay_ms(9, transient(1)) == 1_000
      assert Session.retry_delay_ms(1, gone(1)) == 1_000
    end

    # The COUNTER half, on a real session. The arithmetic above is only useful if something
    # actually drives `catchup_fails`, and the reset is what stops a shard that recovered from
    # sitting at the capped delay forever.
    test "the failure count climbs on rejects and is cleared by a clean commit", ctx do
      %{id: id, root: root} = ctx
      # Three live followers, quorum one, and only TWO of them seeded. The quorum then SUCCEEDS
      # while the third refuses `:unknown_shard` — which is the branch that matters and the one
      # the sibling arms describe: `{:ok, _, rejects}`, tenant sees `:ok`, one follower silently
      # behind. A commit whose quorum FAILS seeds and retries synchronously instead, so it never
      # reaches the deferred retry at all.
      followers = start_followers!(root, 3)

      Application.put_env(:fathom, :replication_enabled, true)
      Application.put_env(:fathom, :replication_quorum, 1)

      Application.put_env(
        :fathom,
        :replication_followers,
        for({_n, p} <- followers, do: {~c"127.0.0.1", p})
      )

      start_supervised!(Fleet)

      {:ok, coordinator, ref, path} = Shards.checkout(id)
      on_exit(fn -> Fathom.Shard.checkin(coordinator, ref) end)
      {:ok, conn} = Connection.open(path)
      on_exit(fn -> Connection.close(conn) end)
      {:ok, _} = Connection.query(conn, "CREATE TABLE t (a)", [])

      for {n, _} <- Enum.take(followers, 2), do: Follower.seed(n, id, 0, 0, 0, 0)

      # The third is deliberately left unseeded: it answers `:unknown_shard`, which is a reject,
      # which is what arms the deferred retry in the first place.
      _ = Session.commit(id, path <> "-wal", coordinator)
      [{session, _}] = Registry.lookup(Fathom.Shard.Replication.SessionRegistry, id)
      assert :sys.get_state(session).catchup_fails > 0

      # The first commit started the seed; once it lands the follower answers cleanly and the
      # count must go back to zero, or the next genuine straggler waits a whole flush interval
      # to be made good instead of the 1 s the retry was tuned for.
      expected = File.read!(path <> "-wal")
      for {n, _} <- followers, do: await_wal(n, id, expected)

      {:ok, _} = Connection.query(conn, "INSERT INTO t VALUES (1)", [])
      assert :ok = Session.commit(id, path <> "-wal", coordinator)
      assert :sys.get_state(session).catchup_fails == 0
    end
  end

  # A LOST SEED TASK MUST NOT BE PERMANENT (expert review 2026-08-20 #26).
  #
  # `start_seeds/3` recorded the shipper in `state.seeding` and spawned the transfer with a bare
  # `Task.start/1` — unlinked, unmonitored, no timeout. The ONLY thing that removed the entry was a
  # `{:seeded, ...}` message from that task, and the guard then refused to start another. So a task
  # killed under memory pressure, an unforeseen raise inside `do_seed/5`, or a reap left that
  # `{shard, follower}` pair unseedable for the life of the Session, with every subsequent
  # `:unknown_shard` from that follower silently swallowed: silent, PERMANENT under-replication of
  # one shard while the quorum reports healthy.
  #
  # The seed is made slow by pointing one shipper at a black hole — a listener that accepts and
  # never answers — so `do_seed/5` blocks in `await_seed_reply/4` and the entry is genuinely
  # in flight while the test operates on it. Nothing here sleeps or races.
  describe "a seed task that dies does not block re-seeding (#26)" do
    defp seed_black_hole! do
      me = self()

      pid =
        spawn(fn ->
          {:ok, listen} = :gen_tcp.listen(0, [:binary, packet: 4, active: true, reuseaddr: true])
          {:ok, port} = :inet.port(listen)
          send(me, {:bh_port, self(), port})
          {:ok, sock} = :gen_tcp.accept(listen)
          bh_loop(sock, listen)
        end)

      assert_receive {:bh_port, ^pid, port}, 2_000
      on_exit(fn -> Process.exit(pid, :kill) end)
      port
    end

    defp bh_loop(sock, listen) do
      receive do
        {:tcp, ^sock, _} -> bh_loop(sock, listen)
        _ -> :ok
      end
    end

    defp await_seeding(session, fun, msg, timeout \\ 3_000) do
      deadline = System.monotonic_time(:millisecond) + timeout

      Stream.repeatedly(fn ->
        seeding = :sys.get_state(session).seeding
        if fun.(seeding), do: {:ok, seeding}, else: Process.sleep(10)
      end)
      |> Enum.find(fn
        {:ok, _} -> true
        _ -> System.monotonic_time(:millisecond) > deadline
      end)
      |> case do
        {:ok, seeding} -> seeding
        _ -> flunk(msg)
      end
    end

    test "a killed seed task is forgotten, and the next request seeds again", ctx do
      %{id: id, root: root} = ctx

      live = start_followers!(root, 2)
      bh_port = seed_black_hole!()

      Application.put_env(:fathom, :replication_enabled, true)
      Application.put_env(:fathom, :replication_quorum, 1)

      Application.put_env(
        :fathom,
        :replication_followers,
        for({_n, p} <- live, do: {~c"127.0.0.1", p}) ++ [{~c"127.0.0.1", bh_port}]
      )

      start_supervised!(Fleet)

      {:ok, coordinator, ref, path} = Shards.checkout(id)
      on_exit(fn -> Fathom.Shard.checkin(coordinator, ref) end)
      {:ok, conn} = Connection.open(path)
      on_exit(fn -> Connection.close(conn) end)
      {:ok, _} = Connection.query(conn, "CREATE TABLE t (a)", [])

      # The two live followers are pre-seeded so the quorum forms without them; the black hole is
      # the one whose seed will hang.
      for {n, _} <- live, do: Follower.seed(n, id, 0, 0, 0, 0)

      assert :ok = Session.commit(id, path <> "-wal", coordinator)
      [{session, _}] = Registry.lookup(Fathom.Shard.Replication.SessionRegistry, id)

      # The black-hole shipper is the last one Fleet published.
      bh = List.last(Fleet.shippers())

      # Drive the seed directly with the reject a never-seeded follower sends. This is the exact
      # message `handle_info/2` receives from a real follower; it just does not depend on the
      # black hole being able to produce one.
      send(session, {:repl_reply, bh, {:reject, id, :unknown_shard, 0}})

      seeding =
        await_seeding(session, &(map_size(&1) == 1), "the reject never started a seed at all")

      %{pid: task} = Map.fetch!(seeding, bh)

      assert Process.alive?(task),
             "the seed task finished; the black hole answered, which it must not"

      # THE BUG: kill it without letting it answer.
      Process.exit(task, :kill)

      await_seeding(
        session,
        &(map_size(&1) == 0),
        "a seed task died without answering and its entry was never cleared — that follower " <>
          "could never be seeded again for the life of this Session, silently, while the " <>
          "quorum kept reporting healthy"
      )

      # And the pair is genuinely usable again, not merely tidy.
      send(session, {:repl_reply, bh, {:reject, id, :unknown_shard, 0}})

      seeding2 =
        await_seeding(
          session,
          &(map_size(&1) == 1),
          "a follower whose seed task died was never re-seeded"
        )

      %{pid: task2} = Map.fetch!(seeding2, bh)
      assert task2 != task, "the same dead task was recorded again"
      assert Process.alive?(task2)
    end

    test "a seed that is alive but WEDGED expires, so the pair is not stuck forever either",
         ctx do
      # The monitor covers a task that DIED. It does not cover one that is alive and stuck --
      # `do_seed/5` streams over a socket and nothing below it is guaranteed to return, which is
      # precisely what a black hole models. Without the sweep that pair is unseedable for the life
      # of the Session just as surely as with a dead task, only with no DOWN to notice it by.
      %{id: id, root: root} = ctx

      prev = Application.get_env(:fathom, :replication_seed_expiry_ms)

      on_exit(fn ->
        if is_nil(prev),
          do: Application.delete_env(:fathom, :replication_seed_expiry_ms),
          else: Application.put_env(:fathom, :replication_seed_expiry_ms, prev)
      end)

      live = start_followers!(root, 2)
      bh_port = seed_black_hole!()

      Application.put_env(:fathom, :replication_enabled, true)
      Application.put_env(:fathom, :replication_quorum, 1)

      Application.put_env(
        :fathom,
        :replication_followers,
        for({_n, p} <- live, do: {~c"127.0.0.1", p}) ++ [{~c"127.0.0.1", bh_port}]
      )

      start_supervised!(Fleet)

      {:ok, coordinator, ref, path} = Shards.checkout(id)
      on_exit(fn -> Fathom.Shard.checkin(coordinator, ref) end)
      {:ok, conn} = Connection.open(path)
      on_exit(fn -> Connection.close(conn) end)
      {:ok, _} = Connection.query(conn, "CREATE TABLE t (a)", [])
      for {n, _} <- live, do: Follower.seed(n, id, 0, 0, 0, 0)

      assert :ok = Session.commit(id, path <> "-wal", coordinator)
      [{session, _}] = Registry.lookup(Fathom.Shard.Replication.SessionRegistry, id)
      bh = List.last(Fleet.shippers())

      send(session, {:repl_reply, bh, {:reject, id, :unknown_shard, 0}})
      seeding = await_seeding(session, &(map_size(&1) == 1), "the reject never started a seed")
      %{pid: task} = Map.fetch!(seeding, bh)
      assert Process.alive?(task), "the black hole answered, which it must not"

      # Anything already outstanding is now past its expiry. Nothing about the task changed --
      # it is still alive and still stuck, which is the whole point.
      Application.put_env(:fathom, :replication_seed_expiry_ms, 0)

      send(session, {:repl_reply, bh, {:reject, id, :unknown_shard, 0}})

      seeding2 =
        await_seeding(
          session,
          fn m -> match?(%{pid: p} when p != task, Map.get(m, bh, %{})) end,
          "a seed task that hung forever was never expired, so that follower could never be " <>
            "seeded again -- silently, while the quorum kept reporting healthy"
        )

      refute Process.alive?(task), "the expired task was forgotten but left running"

      assert is_pid(Map.fetch!(seeding2, bh).pid),
             "the expiry forgot the wedged seed without recording a replacement"

      # THIS USED TO ALSO ASSERT `Process.alive?` ON THE REPLACEMENT, and that assertion could not
      # hold (CI, OTP 27, run 33230841000; the same test went red a different way on 2026-08-21,
      # `logs/test-failures-20260821-054319.log`).
      #
      # `:replication_seed_expiry_ms` is pinned to **0** above so the FIRST seed expires. But
      # `expire_seeds/1` compares `now - started_at >= seed_expiry_ms()`, which at 0 is true for
      # EVERY seed — including the replacement, which is therefore born already expired. It runs on
      # every `{:repl_reply, …}`, and the two LIVE followers in this fixture answer on their own
      # schedule, so an unrelated reply landing between `await_seeding/4` and this line kills the
      # replacement. On a fast box the read wins; on a loaded 2-core runner it does not. Nothing
      # about that is a defect — it is the sweep doing exactly what the test configured it to do.
      #
      # What the test is FOR is carried by the three assertions above: the wedged task was alive
      # (not merely dead), a DIFFERENT pid was recorded, and the old one was actually killed rather
      # than only forgotten. Liveness of the replacement is a window this fixture does not control,
      # so asserting it was measuring the scheduler, not the sweep.
    end

    test "a stopped session does not leave a seed streaming for a shard it no longer owns", ctx do
      %{id: id, root: root} = ctx

      live = start_followers!(root, 2)
      bh_port = seed_black_hole!()

      Application.put_env(:fathom, :replication_enabled, true)
      Application.put_env(:fathom, :replication_quorum, 1)

      Application.put_env(
        :fathom,
        :replication_followers,
        for({_n, p} <- live, do: {~c"127.0.0.1", p}) ++ [{~c"127.0.0.1", bh_port}]
      )

      start_supervised!(Fleet)

      {:ok, coordinator, ref, path} = Shards.checkout(id)
      on_exit(fn -> Fathom.Shard.checkin(coordinator, ref) end)
      {:ok, conn} = Connection.open(path)
      on_exit(fn -> Connection.close(conn) end)
      {:ok, _} = Connection.query(conn, "CREATE TABLE t (a)", [])
      for {n, _} <- live, do: Follower.seed(n, id, 0, 0, 0, 0)

      assert :ok = Session.commit(id, path <> "-wal", coordinator)
      [{session, _}] = Registry.lookup(Fathom.Shard.Replication.SessionRegistry, id)
      bh = List.last(Fleet.shippers())
      send(session, {:repl_reply, bh, {:reject, id, :unknown_shard, 0}})

      seeding = await_seeding(session, &(map_size(&1) == 1), "the reject never started a seed")
      %{pid: task} = Map.fetch!(seeding, bh)
      task_ref = Process.monitor(task)

      # The shard moved or drained. An unsupervised task would go on streaming an entire tenant
      # database to a follower for a shard this node no longer owns, stamped with an epoch it no
      # longer holds.
      Session.stop(id)

      # The REASON matters: `:killed` is `terminate/2` cancelling it. Any other reason means the
      # task ended on its own and this test would pass with the cancellation removed.
      assert_receive {:DOWN, ^task_ref, :process, ^task, :killed},
                     3_000,
                     "the Session stopped and left its seed task streaming"
    end
  end
end
