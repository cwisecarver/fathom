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
end
