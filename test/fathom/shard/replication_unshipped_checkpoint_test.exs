defmodule Fathom.Shard.ReplicationUnshippedCheckpointTest do
  @moduledoc """
  A checkpoint must never erase committed frames the replication `Session` has not shipped yet
  (expert review 2026-10-15 #1).

  ## The bug

  A stream commits, THEN calls `Session.commit/3`, which reads the WAL itself. Between the two, any
  checkpoint can backfill every frame into the `.db`, and the next writer on another stream then
  RESTARTS the WAL in place (new salt, offset 0). The Session sees a new generation, plans a reset
  carrying only the new frames, and the followers — which never saw the first commit — ack it.
  The first commit is reported quorum-durable and exists on no replica; the later frames sit on a
  base that lacks it.

  Each test below drives that exact interleaving deterministically, with three connections on one
  shard: the committer (C1), a checkpointer, and a second writer (C2) whose write restarts the log.
  C1 and C2 touch DIFFERENT tables, so C2's page images cannot carry C1's row to the replica by
  accident — the replica holds C1 only if C1's own frames were shipped.

  The checkpoint sources covered are the ones fathom runs itself: a tenant `PRAGMA
  wal_checkpoint(PASSIVE)` (allowed by the executor's pragma guard), the coordinator's
  post-snapshot PASSIVE (`Fathom.Shard.do_snapshot/2`), and the extension's 4000-frame commit hook
  (`native/fathom_udf/src/wal.rs`). They all reduce to "a PASSIVE checkpoint runs between C1's
  commit and its ship", which is what the first test does directly.
  """
  use ExUnit.Case, async: false

  alias Fathom.Shard.Connection
  alias Fathom.Shard.Replication.Fleet
  alias Fathom.Shard.Replication.Follower
  alias Fathom.Shard.Replication.Session
  alias Fathom.Shard.Replication.Wal
  alias Fathom.Shards

  setup do
    id = "repl_unshipped_#{System.unique_integer([:positive])}"
    root = Path.join(System.tmp_dir!(), "replunshipped_#{System.unique_integer([:positive])}")
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

  defp start_followers!(root, n) do
    for i <- 1..n do
      name = :"us_f#{i}_#{System.unique_integer([:positive])}"
      dir = Path.join(root, to_string(name))
      pid = start_supervised!({Follower, name: name, port: 0, dir: dir}, id: name)
      {:ok, port} = Follower.port(pid)
      {name, port}
    end
  end

  defp enable!(followers) do
    Application.put_env(:fathom, :replication_enabled, true)
    Application.put_env(:fathom, :replication_quorum, 2)

    Application.put_env(
      :fathom,
      :replication_followers,
      for({_n, port} <- followers, do: {~c"127.0.0.1", port})
    )

    start_supervised!(Fleet)
  end

  defp open!(path) do
    {:ok, conn} = Connection.open(path)
    on_exit(fn -> Connection.close(conn) end)
    conn
  end

  # Read a follower's replica the way a promotion would: its `.db` plus its WAL, opened together.
  # Copied aside first so opening it cannot run recovery or a checkpoint on the follower's files.
  defp replica_rows(name, id, table, root) do
    dir = Path.join(root, "probe_#{System.unique_integer([:positive])}")
    File.mkdir_p!(dir)
    db = Path.join(dir, "r.db")
    # A follower pre-seeded on an empty base has no `.db` until it first absorbs a WAL.
    case File.read(Follower.db_path(name, id)) do
      {:ok, bin} -> File.write!(db, bin)
      _ -> :ok
    end

    case File.read(Follower.wal_path(name, id)) do
      {:ok, bin} -> File.write!(db <> "-wal", bin)
      _ -> :ok
    end

    {:ok, conn} = Connection.open(db)

    try do
      case Connection.query(conn, "SELECT a FROM #{table} ORDER BY a", []) do
        {:ok, %{rows: rows}} -> List.flatten(rows)
        {:ok, rows} when is_list(rows) -> List.flatten(rows)
        other -> other
      end
    after
      Connection.close(conn)
    end
  end

  defp await_rows(followers, id, table, expected, root, timeout \\ 5_000) do
    deadline = System.monotonic_time(:millisecond) + timeout

    Stream.repeatedly(fn ->
      got = for {name, _} <- followers, do: {name, replica_rows(name, id, table, root)}
      held = Enum.count(got, fn {_n, rows} -> rows == expected end)

      cond do
        held == length(followers) -> {:ok, got}
        System.monotonic_time(:millisecond) > deadline -> {:timeout, got}
        true -> Process.sleep(20) && :wait
      end
    end)
    |> Enum.find(&(&1 != :wait))
  end

  # A connection and a shard set up for the race: three followers seeded from a real `.db`, one
  # replicated commit already shipped (so the Session exists and has registered the WAL).
  defp setup_shard!(ctx) do
    %{id: id, root: root} = ctx
    followers = start_followers!(root, 3)
    enable!(followers)

    {:ok, coordinator, ref, path} = Shards.checkout(id)
    on_exit(fn -> Fathom.Shard.checkin(coordinator, ref) end)
    wal = path <> "-wal"

    c1 = open!(path)
    c2 = open!(path)

    # The schema goes into the `.db` before replication starts, so the replicas get a real base by
    # a real seed: SQLite deletes a WAL that sits next to a ZERO-byte database on open, so a
    # replica built on an empty base cannot be read back at all.
    {:ok, _} = Connection.query(c1, "CREATE TABLE t1 (a INTEGER, b BLOB)", [])
    {:ok, _} = Connection.query(c1, "CREATE TABLE t2 (a INTEGER)", [])
    {:ok, _} = Connection.query(c2, "PRAGMA wal_checkpoint(TRUNCATE)", [])

    # Pre-seeded at epoch 0, below the real ownership, so the first push is answered with a seed
    # request and the Session ships the `.db` itself (see replication_commit_test's preseed note).
    for {name, _} <- followers, do: Follower.seed(name, id, 0, 0, 0, 0)

    {:ok, _} = Connection.query(c1, "INSERT INTO t2 VALUES (0)", [])
    assert :ok = Session.commit(id, wal, coordinator)

    %{
      followers: followers,
      coordinator: coordinator,
      path: path,
      wal: wal,
      c1: c1,
      c2: c2
    }
  end

  # The interleaving: C1 commits (`c1_write`), `checkpoint` runs before C1's ship, then C2 — a
  # different stream — writes, which restarts the log iff the checkpoint backfilled everything.
  defp race!(ctx, c1_write, checkpoint) do
    s = setup_shard!(ctx)

    # C1: committed locally, not shipped yet.
    c1_write.(s.c1)
    {:ok, before} = Wal.read(s.wal)

    checkpoint.(s)

    {:ok, _} = Connection.query(s.c2, "INSERT INTO t2 VALUES (2)", [])
    {:ok, after_} = Wal.read(s.wal)

    # C1's ship (and C2's — one ship covers both).
    c1_result = Session.commit(ctx.id, s.wal, s.coordinator)
    c2_result = Session.commit(ctx.id, s.wal, s.coordinator)

    Map.merge(s, %{before: before, after: after_, c1_result: c1_result, c2_result: c2_result})
  end

  defp small_c1(conn),
    do: {:ok, _} = Connection.query(conn, "INSERT INTO t1 VALUES (1, NULL)", [])

  # The invariant: C1 was acked, so the replicas must hold it. Either the commit is refused (honest)
  # or the replica has the row — never acked-and-missing.
  defp assert_c1_replicated!(ctx, r) do
    if r.c1_result == :ok do
      got = await_rows(r.followers, ctx.id, "t1", [1], ctx.root)

      assert match?({:ok, _}, got),
             "replicas #{inspect(got)}: C1 was acked quorum-durable but the replicas do not hold it: a checkpoint erased " <>
               "its frames before the Session read them (WAL before C2 #{inspect(r.before)}, " <>
               "after #{inspect(r.after)})"
    end

    assert r.c2_result == :ok
    assert {:ok, _} = await_rows(r.followers, ctx.id, "t2", [0, 2], ctx.root)
  end

  test "CONTROL: with no checkpoint in the window the replicas hold both commits", ctx do
    r = race!(ctx, &small_c1/1, fn _s -> :ok end)
    assert r.c1_result == :ok
    assert_c1_replicated!(ctx, r)
  end

  # Source 1: the coordinator's post-snapshot PASSIVE (`Fathom.Shard.do_snapshot/3`), reached the
  # way production reaches it — a durability flush of the dirty shard.
  test "the coordinator's flush checkpoint cannot erase an unshipped commit", ctx do
    r =
      race!(ctx, &small_c1/1, fn _s ->
        Fathom.Shard.WriteCounter.bump(ctx.id)
        assert :ok = Shards.flush(ctx.id)
      end)

    assert_c1_replicated!(ctx, r)
  end

  # Source 2: a tenant's own `PRAGMA wal_checkpoint(PASSIVE)`, which the executor's text gate and
  # the engine authorizer both allow on a non-replicated shard. On a replicated one the engine
  # refuses it; the race then cannot happen.
  test "a tenant's PASSIVE checkpoint cannot erase an unshipped commit", ctx do
    r =
      race!(ctx, &small_c1/1, fn s ->
        {:ok, tenant} = Connection.open(s.path, tenant?: true, scope: :rw)
        on_exit(fn -> Connection.close(tenant) end)
        _ = Connection.query(tenant, "PRAGMA wal_checkpoint(PASSIVE)", [])
      end)

    assert_c1_replicated!(ctx, r)
  end

  # Source 3: the extension's commit hook, which checkpoints INLINE in the commit that takes the WAL
  # past 4000 frames — so C1 itself is the checkpoint. A ~17 MiB row is what crosses it.
  test "the 4000-frame commit hook cannot erase the commit that crossed it", ctx do
    big = :binary.copy(<<7>>, 4_200 * 4_096)

    r =
      race!(
        ctx,
        fn conn -> {:ok, _} = Connection.query(conn, "INSERT INTO t1 VALUES (1, ?1)", [big]) end,
        fn _s -> :ok end
      )

    assert r.before.commit_extent >= 4_000 * (4_096 + 24),
           "C1 did not take the WAL past the hook's threshold, so the hook never fired"

    assert_c1_replicated!(ctx, r)
  end

  # Replication OFF is unchanged: the hook still checkpoints at 4000 frames, so the next write
  # restarts the log (the property `wal_hook_test` pins for a non-shard file, here on a shard path).
  test "replication off: the commit hook still checkpoints and the WAL still restarts", ctx do
    {:ok, coordinator, ref, path} = Shards.checkout(ctx.id)
    on_exit(fn -> Fathom.Shard.checkin(coordinator, ref) end)
    conn = open!(path)
    {:ok, _} = Connection.query(conn, "CREATE TABLE t1 (a INTEGER, b BLOB)", [])

    big = :binary.copy(<<7>>, 4_200 * 4_096)
    {:ok, _} = Connection.query(conn, "INSERT INTO t1 VALUES (1, ?1)", [big])
    {:ok, before} = Wal.read(path <> "-wal")
    {:ok, _} = Connection.query(conn, "INSERT INTO t1 VALUES (2, NULL)", [])
    {:ok, after_} = Wal.read(path <> "-wal")

    assert {after_.ckpt_seq, after_.salt1} != {before.ckpt_seq, before.salt1},
           "the hook no longer checkpoints a non-replicated WAL"
  end

  # THE BOUND. The hook steps aside for a replicated WAL, so the Session must checkpoint it instead
  # or the WAL grows without limit. Threshold lowered so a few small commits cross it.
  test "the Session checkpoints a replicated WAL past the threshold, and the log restarts", ctx do
    prev = Application.get_env(:fathom, :replication_checkpoint_bytes)
    Application.put_env(:fathom, :replication_checkpoint_bytes, 64 * 1024)

    on_exit(fn ->
      if prev,
        do: Application.put_env(:fathom, :replication_checkpoint_bytes, prev),
        else: Application.delete_env(:fathom, :replication_checkpoint_bytes)
    end)

    s = setup_shard!(ctx)
    blob = :binary.copy(<<1>>, 100 * 1024)
    {:ok, _} = Connection.query(s.c1, "INSERT INTO t1 VALUES (1, ?1)", [blob])
    assert :ok = Session.commit(ctx.id, s.wal, s.coordinator)
    {:ok, grown} = Wal.read(s.wal)
    assert grown.commit_extent >= 64 * 1024

    # This commit finds the WAL past the threshold: it pins, ships, and checkpoints under the pin.
    {:ok, _} = Connection.query(s.c2, "INSERT INTO t2 VALUES (2)", [])
    assert :ok = Session.commit(ctx.id, s.wal, s.coordinator)

    # Everything was shipped and backfilled, so the next writer restarts the log.
    {:ok, _} = Connection.query(s.c1, "INSERT INTO t1 VALUES (3, NULL)", [])
    {:ok, restarted} = Wal.read(s.wal)

    assert {restarted.ckpt_seq, restarted.salt1} != {grown.ckpt_seq, grown.salt1},
           "the Session never checkpointed the replicated WAL — it would grow without bound"

    assert :ok = Session.commit(ctx.id, s.wal, s.coordinator)
    assert {:ok, _} = await_rows(s.followers, ctx.id, "t1", [1, 3], ctx.root)
    assert {:ok, _} = await_rows(s.followers, ctx.id, "t2", [0, 2], ctx.root)
  end

  test "a tenant handle cannot call the replication registration function", ctx do
    s = setup_shard!(ctx)
    {:ok, tenant} = Connection.open(s.path, tenant?: true, scope: :rw)
    on_exit(fn -> Connection.close(tenant) end)

    # A trusted handle can (delta 0 only reads the count) — so the refusal below is the authorizer,
    # not a missing function.
    assert {:ok, _} = Connection.query(s.c1, "SELECT fathom_wal_replicated(?1, 0)", [s.path])

    assert {:error, reason} =
             Connection.query(tenant, "SELECT fathom_wal_replicated(?1, -1)", [s.path])

    assert inspect(reason) =~ "not authorized"
  end
end
