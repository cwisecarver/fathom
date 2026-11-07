defmodule Fathom.Migrator.ShardMigrationDirtyReleaseTest do
  @moduledoc """
  A migration (and a revert) must not steal a DIRTY-release marker (expert review 2026-11-07 #22).

  Node K's drop flush fails, so `keep_local_release_lease` keeps K's local `.db` — which holds writes
  the client was ACKED for and the store never received — and releases the lock as a DIRTY marker
  (`Storage.with_dirty_release/1`). A migration job that then lands on node M sees `Shards.drain` find
  nothing (M has no local copy), `acquire_lease` steals the marker, the job pulls the STALE stored
  object, replays, flushes and cuts over; K's next open reads `:diverged` and quarantines its acked
  writes. `RemoteFlush` already fails closed on the marker; the migrator did not read it.

  ## The fixture

  A REAL coordinator produces the marker: its idle drop's flush fails (`storage_fault: :flush`), the
  lock is released with `with_dirty_release/1`, and `Storage.released_dirty?/1` is asserted `true`
  before anything is attempted. "Another node" is modelled by moving K's local files aside — M has
  no copy, which is the whole situation. `Storage.Local` implements the marker with the real
  semantics (`dirty: true` on the released marker, read by `released_dirty?/1`).

  Both liveness modes are exercised and asserted (`acquire_gen`): the dirty release is reached
  through different fence and release paths in heartbeat and legacy mode. The migrator itself holds
  a TTL lease in both (its owner has no heartbeat), so it is the keeper's mode that varies.

  Not async: global storage-fault env, `:shard_storage` and the shared Postgres sandbox.
  """
  use Fathom.DataCase, async: false

  import ExUnit.CaptureLog

  alias Fathom.{Directory, Migrator, Shard, ShardExecutor, Shards}
  alias Fathom.Migrator.ShardMigration
  alias Fathom.Shard.{Connection, Heartbeat, Storage}
  alias Fathom.Shard.Storage.Local
  alias Fathom.Test.Env
  alias Filo.Stmt

  @modes [:legacy, :heartbeat]

  @v2_statements [
    "ALTER TABLE app_thing ADD COLUMN created_at TEXT",
    "INSERT INTO django_migrations (app, name, applied) VALUES ('app', '0002_add_created_at', 'now')"
  ]

  setup do
    shard = "dirtymig_#{System.unique_integer([:positive])}"

    Env.put!(:shard_storage, Fathom.Test.FaultyStorage)
    Env.put!(:shard_idle_ms, 600_000)
    Env.put!(:shard_flush_interval_ms, 600_000)
    Env.put!(:replication_enabled, false)
    Env.put!(:replication_promote_on_open, false)
    Env.put!(:storage_fault, nil)
    Env.put!(:faulty_before, nil)

    on_exit(fn ->
      case Registry.lookup(Fathom.ShardRegistry, shard) do
        [{pid, _}] -> Process.exit(pid, :kill)
        _ -> :ok
      end

      for f <- Path.wildcard(Path.join(Shard.data_dir(), "#{shard}*")), do: File.rm_rf(f)
      Local.purge_shard(shard)
    end)

    %{shard: shard}
  end

  defp set_mode!(:legacy), do: refute(Heartbeat.running?(), "legacy mode needs the heartbeat OFF")

  defp set_mode!(:heartbeat) do
    hb = start_supervised!({Heartbeat, ttl_ms: 30_000})
    _ = :sys.get_state(hb)
    assert Heartbeat.running?()
  end

  defp assert_mode!(pid, :heartbeat), do: assert(is_integer(:sys.get_state(pid).acquire_gen))
  defp assert_mode!(pid, :legacy), do: assert(is_nil(:sys.get_state(pid).acquire_gen))

  defp stmt(sql), do: %Stmt{sql: sql, args: []}
  defp exec!(conn, sql), do: {:ok, _} = ShardExecutor.execute(conn, stmt(sql))

  # A v1 shard in storage: its stored object carries the schema, one row and the Django ledger.
  defp seed_v1!(shard) do
    seed = Path.join(System.tmp_dir!(), "seeddm_#{shard}.db")
    {:ok, conn} = Connection.open(seed)
    :ok = Connection.exec(conn, "CREATE TABLE app_thing (id INTEGER PRIMARY KEY, name TEXT)")
    :ok = Connection.exec(conn, "INSERT INTO app_thing (id, name) VALUES (1, 'alice')")

    :ok =
      Connection.exec(
        conn,
        "CREATE TABLE django_migrations (id INTEGER PRIMARY KEY, app TEXT, name TEXT, applied TEXT)"
      )

    :ok =
      Connection.exec(
        conn,
        "INSERT INTO django_migrations (app, name, applied) VALUES ('app', '0001_initial', 'now')"
      )

    :ok = Connection.exec(conn, "PRAGMA user_version = 1")
    :ok = Connection.exec(conn, "PRAGMA wal_checkpoint(TRUNCATE)")
    Connection.close(conn)
    :ok = Storage.flush(shard, seed)
    for s <- ["", "-wal", "-shm"], do: File.rm(seed <> s)

    {:ok, _} = Directory.resolve(shard)
    {:ok, _} = Directory.cutover(shard, 1)
    :ok
  end

  # Node K: cold-open the shard, ACK a write, then an idle drop whose flush FAILS — the coordinator
  # keeps the local copy (it holds the acked write) and releases the lock as a DIRTY marker. Then
  # K's local files are moved aside so the migrator (on "node M") finds nothing to drain.
  defp keeper_leaves_dirty_marker!(shard, mode) do
    {:ok, conn} = ShardExecutor.open(shard)
    {:ok, pid} = Shards.ensure(shard)
    assert_mode!(pid, mode)

    exec!(conn, "INSERT INTO app_thing (id, name) VALUES (2, 'acked-only-on-K')")

    Application.put_env(:fathom, :storage_fault, :flush)
    :ok = ShardExecutor.close(conn)

    {drained, _log} = with_log(fn -> Shards.drain(shard, 5_000) end)

    assert drained == {:error, :unflushed_local_copy},
           "fixture: the failed drop flush must keep the local copy (got #{inspect(drained)})"

    Application.put_env(:fathom, :storage_fault, nil)

    assert Storage.released_dirty?(shard) == true,
           "fixture: the lock must be a DIRTY released marker"

    assert File.exists?(Shard.db_path(shard)), "fixture: K's copy holding the acked write"

    # "Another node": move K's local files out of the way, restore them in on_exit.
    for f <- Path.wildcard(Path.join(Shard.data_dir(), "#{shard}.db*")) do
      File.rename!(f, f <> ".keeper")
    end

    :ok
  end

  for mode <- @modes do
    describe "#{mode} keeper" do
      setup do
        set_mode!(unquote(mode))
        :ok
      end

      test "a forward migration snoozes on the dirty marker and leaves the stored object alone",
           %{shard: shard} do
        seed_v1!(shard)
        {:ok, _} = Migrator.release(2, "v2", @v2_statements)
        {:ok, etag_before} = Storage.object_etag(shard)

        keeper_leaves_dirty_marker!(shard, unquote(mode))

        assert {:retry, :released_dirty} = ShardMigration.run(shard, 2)

        assert {:ok, ^etag_before} = Storage.object_etag(shard),
               "the migrator replaced the stored object under a dirty marker"

        assert {:ok, %{schema_version: 1, status: "active"}} = Directory.get(shard)
        assert Storage.released_dirty?(shard) == true, "the marker must still be there"
        refute File.exists?(Path.join(Local.dir(), "#{shard}@1.db")), "no backup was taken either"
      end

      test "a revert snoozes on the dirty marker too", %{shard: shard} do
        seed_v1!(shard)
        {:ok, _} = Migrator.release(2, "v2", @v2_statements)
        assert {:ok, %{from: 1, to: 2}} = ShardMigration.run(shard, 2)
        {:ok, etag_before} = Storage.object_etag(shard)

        keeper_leaves_dirty_marker!(shard, unquote(mode))

        assert {:retry, :released_dirty} = ShardMigration.revert(shard, 1)

        assert {:ok, ^etag_before} = Storage.object_etag(shard)
        assert {:ok, %{schema_version: 2}} = Directory.get(shard)
        assert Storage.released_dirty?(shard) == true
      end

      # The marker clears when somebody opens the shard (the keeper's reopen flushes its tail; a
      # peer open overwrites it) — and from then on the migration proceeds as it always did.
      test "once the marker is gone the migration proceeds", %{shard: shard} do
        seed_v1!(shard)
        {:ok, _} = Migrator.release(2, "v2", @v2_statements)

        keeper_leaves_dirty_marker!(shard, unquote(mode))
        assert {:retry, :released_dirty} = ShardMigration.run(shard, 2)

        # The keeper's disk comes back and its reopen flushes the acked tail (the fault is cleared).
        for f <- Path.wildcard(Path.join(Shard.data_dir(), "#{shard}.db*.keeper")) do
          File.rename!(f, String.replace_suffix(f, ".keeper", ""))
        end

        {:ok, conn} = ShardExecutor.open(shard)
        {:ok, _} = Shards.ensure(shard)
        :ok = ShardExecutor.close(conn)
        assert :ok = Shards.drain(shard, 5_000)
        assert Storage.released_dirty?(shard) == false

        assert {:ok, %{from: 1, to: 2}} = ShardMigration.run(shard, 2)
      end
    end
  end
end
