defmodule Fathom.Migrator.RevertLoopTest do
  @moduledoc """
  The documented revert loop — canary vN, revert (yanks vN), walk the template back, fix, capture
  vN+1 — must not wedge the reverted fleet (expert review 2026-09-29 #16; decided 2026-09-29).

  Pre-fix a shard reverted to vN-1 needed `vN-1 → vN+1`, the chain halted on the yanked vN, and the
  job cancelled it as an unbuildable chain forever. The fix skips a yanked version ONLY when the
  next release was provably captured on a walked-back template (its pre-capture django_migrations
  count, = count − bookkeeping inserts, equals the last live release's count). Skipping when the
  operator did NOT walk the template back would silently cut a shard over to vN+1 without the
  schema vN+1 was authored on — so that case must keep halting, and capture must hold it for
  review in a form `approve_review/1` refuses.
  """
  use Fathom.DataCase, async: false

  alias Fathom.Migrator.{Capture, ShardMigration}
  alias Fathom.Shard.{Connection, Storage}
  alias Fathom.{Directory, Migrator}

  @dm "INSERT INTO django_migrations (app, name, applied) VALUES "

  setup do
    shard = "revloop_#{System.unique_integer([:positive])}"

    on_exit(fn ->
      for path <- Path.wildcard(Path.join(remote_dir(), "#{shard}*")), do: File.rm(path)

      for path <- Path.wildcard(Path.join([Fathom.Shard.data_dir(), "#{shard}*"])),
          do: File.rm(path)
    end)

    %{shard: shard}
  end

  defp remote_dir, do: Storage.Local.dir()

  # A shard at v1: one Django migration applied, so the template count at v1 is 1.
  defp seed_v1!(shard) do
    seed = Path.join(System.tmp_dir!(), "rl_#{shard}_#{System.unique_integer([:positive])}.db")
    {:ok, conn} = Connection.open(seed)
    :ok = Connection.exec(conn, "CREATE TABLE app_thing (id INTEGER PRIMARY KEY, name TEXT)")

    :ok =
      Connection.exec(
        conn,
        "CREATE TABLE django_migrations (id INTEGER PRIMARY KEY, app TEXT, name TEXT, applied TEXT)"
      )

    :ok = Connection.exec(conn, @dm <> "('app', '0001', 'now')")
    :ok = Connection.exec(conn, "PRAGMA user_version = 1")
    :ok = Connection.exec(conn, "PRAGMA wal_checkpoint(TRUNCATE)")
    Connection.close(conn)
    :ok = Storage.flush(shard, seed)
    for s <- ["", "-wal", "-shm"], do: File.rm(seed <> s)

    {:ok, _} = Directory.resolve(shard)
    {:ok, _} = Directory.cutover(shard, 1)
    :ok
  end

  defp columns(shard) do
    tmp = Path.join(System.tmp_dir!(), "rlq_#{shard}_#{System.unique_integer([:positive])}.db")
    {:ok, _} = Storage.pull(shard, tmp)
    {:ok, conn} = Connection.open(tmp)

    {:ok, %{rows: rows}} =
      Connection.query(conn, "SELECT name FROM pragma_table_info('app_thing')", [])

    Connection.close(conn)
    for s <- ["", "-wal", "-shm"], do: File.rm(tmp <> s)
    List.flatten(rows)
  end

  # v1 (count 1) live; v2 (count 2) canaried then yanked by a revert.
  defp release_and_yank_v2! do
    {:ok, _} = Migrator.release(1, "v1", [], 1)

    {:ok, _} =
      Migrator.release(
        2,
        "v2",
        ["ALTER TABLE app_thing ADD COLUMN bad TEXT", @dm <> "('app', '0002', 'now')"],
        2
      )

    :ok = Migrator.yank(2)
  end

  test "walked back: the reverted shard skips the yanked version and reaches the fix", %{
    shard: shard
  } do
    seed_v1!(shard)
    release_and_yank_v2!()

    # Template walked back to 1, then the fix migrated it to 2 again: pre-capture count 1.
    {:ok, _} =
      Migrator.release(
        3,
        "v3",
        ["ALTER TABLE app_thing ADD COLUMN good TEXT", @dm <> "('app', '0003', 'now')"],
        2
      )

    assert Migrator.skippable_yanked(1, 3) == MapSet.new([2])
    assert {:ok, _} = ShardMigration.run(shard, 3)
    assert {:ok, %{schema_version: 3}} = Directory.get(shard)
    assert "good" in columns(shard)
    refute "bad" in columns(shard), "the yanked version's DDL was applied"
  end

  test "NOT walked back: the chain still halts on the yanked version, shard untouched", %{
    shard: shard
  } do
    seed_v1!(shard)
    release_and_yank_v2!()

    # Captured on top of v2's schema: pre-capture count 2 ≠ v1's 1.
    {:ok, _} =
      Migrator.release(
        3,
        "v3",
        ["ALTER TABLE app_thing ADD COLUMN good TEXT", @dm <> "('app', '0003', 'now')"],
        3
      )

    assert Migrator.skippable_yanked(1, 3) == MapSet.new()
    assert {:error, {:unknown_version, 2}} = ShardMigration.run(shard, 3)
    assert {:ok, %{schema_version: 1}} = Directory.get(shard)
  end

  test "a middle version yanked after later versions were captured on it is not skippable" do
    {:ok, _} = Migrator.release(1, "v1", [], 1)
    {:ok, _} = Migrator.release(2, "v2", [@dm <> "('app', '0002', 'now')"], 2)
    {:ok, _} = Migrator.release(3, "v3", [@dm <> "('app', '0003', 'now')"], 3)
    :ok = Migrator.yank(2)

    assert Migrator.skippable_yanked(1, 3) == MapSet.new()
  end

  test "unknown counts (hand-authored releases) are never skippable" do
    {:ok, _} = Migrator.release(1, "v1", [])
    {:ok, _} = Migrator.release(2, "v2", [])
    :ok = Migrator.yank(2)
    {:ok, _} = Migrator.release(3, "v3", [])

    assert Migrator.skippable_yanked(1, 3) == MapSet.new()
  end

  describe "capture on a template that was not walked back" do
    test "is recorded but held for review, and approve_review refuses it" do
      release_and_yank_v2!()

      conn = make_ref()
      # The template still carries v2 (count 2) when the fix is captured.
      Capture.begin(conn, 2)
      Capture.append(conn, "ALTER TABLE app_thing ADD COLUMN good TEXT", [])
      Capture.append(conn, @dm <> "('app', '0003', 'now')", [])
      assert {:recorded, 3} = Capture.commit(conn, 3)

      assert [%{version: 3, review_reason: "template_drift"}] = Migrator.pending_review()
      assert Migrator.head() == 1, "a drift-held version must not become HEAD"
      assert {:error, :template_drift_requires_recapture} = Migrator.approve_review(3)
    end

    test "a walked-back template captures normally" do
      release_and_yank_v2!()

      conn = make_ref()
      Capture.begin(conn, 1)
      Capture.append(conn, "ALTER TABLE app_thing ADD COLUMN good TEXT", [])
      Capture.append(conn, @dm <> "('app', '0003', 'now')", [])
      assert {:recorded, 3} = Capture.commit(conn, 2)

      assert Migrator.pending_review() == []
      assert Migrator.head() == 3
    end
  end
end
