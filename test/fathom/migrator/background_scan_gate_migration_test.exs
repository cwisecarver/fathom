defmodule Fathom.Migrator.BackgroundScanGateMigrationTest do
  # Expert review 2026-10-29 #3: the migration replay (`Copy.migrate_chain`) and `verify_migrated`
  # hold a `BackgroundScanGate` permit, and a node whose permits are all taken answers
  # `{:error, :background_scan_busy}` INSTEAD of scanning anyway. The Oban job treats that as
  # environmental (it snoozes, burns no attempt, never quarantines the shard), and the inline
  # migrate-on-touch checkout degrades to the async rollout rather than failing the tenant's request.
  #
  # Runs against the app's own gate (a global, so not async) with the permit count and the wait
  # pinned small.
  use Fathom.DataCase, async: false
  use Oban.Testing, repo: Fathom.Repo

  import ExUnit.CaptureLog

  alias Fathom.Migrator
  alias Fathom.Migrator.{Copy, ShardMigration, ShardMigrationJob}
  alias Fathom.Shard.{BackgroundScanGate, Connection, Storage}
  alias Fathom.Directory

  @v2_statements [
    "ALTER TABLE app_thing ADD COLUMN created_at TEXT",
    "INSERT INTO django_migrations (app, name, applied) VALUES ('app', '0002_add_created_at', 'now')"
  ]

  # A transform runs INSIDE the replay, i.e. while the migrating process holds its permit. It takes
  # the node's other permit and queues a second holder behind it, so if the run ever gave its permit
  # up between the replay and `verify_migrated`, the queued holder would take it and the node would
  # be full for the verify. Runs in the migrating (test) process.
  defmodule SaturateAfterReplay do
    @moduledoc false
    @behaviour Fathom.Migrator.Transform

    @impl true
    def run(_conn, _shard_id) do
      parent = self()

      for _ <- 1..2 do
        holder =
          spawn(fn ->
            :ok = BackgroundScanGate.acquire(:infinity)
            send(parent, {:holding, self()})

            receive do
              :stop -> :ok
            end
          end)

        send(parent, {:holder, holder})
      end

      wait_queued(BackgroundScanGate.stats())
    end

    # Both holders are in the gate's books (one held, one queued behind it) before the replay returns.
    defp wait_queued(%{held: 2, waiting: 1}), do: :ok

    defp wait_queued(_) do
      receive do
      after
        5 -> wait_queued(BackgroundScanGate.stats())
      end
    end
  end

  setup do
    shard = "bsg_#{System.unique_integer([:positive])}"
    keys = [:background_scan_permits, :background_scan_wait_ms, :migration_transforms]
    prev = for k <- keys, do: {k, Application.fetch_env(:fathom, k)}

    Application.put_env(:fathom, :background_scan_wait_ms, 50)

    on_exit(fn ->
      for {k, v} <- prev do
        case v do
          {:ok, val} -> Application.put_env(:fathom, k, val)
          :error -> Application.delete_env(:fathom, k)
        end
      end

      for path <- Path.wildcard(Path.join(remote_dir(), "#{shard}*")), do: File.rm(path)

      for path <- Path.wildcard(Path.join([Fathom.Shard.data_dir(), "#{shard}*"])),
          do: File.rm(path)
    end)

    %{shard: shard}
  end

  defp seed_v1!(shard) do
    seed =
      Path.join(System.tmp_dir!(), "seedbsg_#{shard}_#{System.unique_integer([:positive])}.db")

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

  defp live_user_version!(shard) do
    tmp = Path.join(System.tmp_dir!(), "bsgchk_#{shard}_#{System.unique_integer([:positive])}.db")
    {:ok, _etag} = Storage.pull(shard, tmp)
    {:ok, conn} = Connection.open(tmp)
    {:ok, %{rows: [[v]]}} = Connection.query(conn, "PRAGMA user_version", [])
    Connection.close(conn)
    for s <- ["", "-wal", "-shm"], do: File.rm(tmp <> s)
    v
  end

  defp remote_dir, do: Fathom.Shard.Storage.Local.dir()

  # Take every permit the node has, from processes that keep them until `free_holders!/1` (or the
  # end of the test). Returns the holders.
  defp saturate!(permits) do
    Application.put_env(:fathom, :background_scan_permits, permits)
    parent = self()

    holders =
      for _ <- 1..permits do
        pid =
          spawn(fn ->
            :ok = BackgroundScanGate.acquire(:infinity)
            send(parent, {:holding, self()})

            receive do
              :stop -> :ok
            end
          end)

        on_exit(fn -> Process.exit(pid, :kill) end)
        assert_receive {:holding, ^pid}
        pid
      end

    assert %{held: ^permits} = BackgroundScanGate.stats()
    holders
  end

  defp free_holders!(pids), do: for(pid <- pids, do: send(pid, :stop))

  test "Copy.migrate_chain answers busy without replaying when no permit is free", %{shard: shard} do
    src = Path.join(System.tmp_dir!(), "bsgsrc_#{shard}.db")
    dest = Path.join(System.tmp_dir!(), "bsgdst_#{shard}.db")

    on_exit(fn ->
      for p <- [src, dest], s <- ["", "-wal", "-shm"], do: File.rm(p <> s)
    end)

    {:ok, conn} = Connection.open(src)
    :ok = Connection.exec(conn, "CREATE TABLE app_thing (id INTEGER PRIMARY KEY, name TEXT)")
    Connection.close(conn)

    holders = saturate!(1)

    statements = [{"ALTER TABLE app_thing ADD COLUMN created_at TEXT", []}]
    assert {:error, :background_scan_busy} = Copy.migrate(src, dest, 2, statements)

    # Nothing was replayed: the copy is still the old schema, version unchanged.
    {:ok, c} = Connection.open(dest)
    assert {:ok, %{columns: ["id", "name"]}} = Connection.query(c, "SELECT * FROM app_thing", [])
    assert {:ok, %{rows: [[0]]}} = Connection.query(c, "PRAGMA user_version", [])
    Connection.close(c)

    # A permit frees up: the same call now succeeds, and holds nothing afterwards.
    free_holders!(holders)
    wait_until(fn -> BackgroundScanGate.stats().held == 0 end)
    assert :ok = Copy.migrate(src, dest, 2, statements)
    assert %{held: 0, waiting: 0} = BackgroundScanGate.stats()
  end

  test "a saturated node leaves the shard at v1 and snoozes the job; a free permit migrates it",
       %{shard: shard} do
    seed_v1!(shard)
    {:ok, _} = Migrator.release(2, "add created_at", @v2_statements)
    holders = saturate!(1)

    assert {:error, :background_scan_busy} = ShardMigration.run(shard, 2)
    assert {:ok, %{schema_version: 1, status: "active"}} = Directory.get(shard)
    assert live_user_version!(shard) == 1

    # Environmental, like a held lease: a snooze burns no attempt and never quarantines the shard.
    assert ShardMigrationJob.transient?(:background_scan_busy)

    assert {:snooze, _} =
             perform_job(ShardMigrationJob, %{"shard_id" => shard, "target" => 2}, attempt: 1)

    assert {:ok, %{schema_version: 1, status: "active"}} = Directory.get(shard)

    free_holders!(holders)
    wait_until(fn -> BackgroundScanGate.stats().held == 0 end)

    assert :ok = perform_job(ShardMigrationJob, %{"shard_id" => shard, "target" => 2})
    assert {:ok, %{schema_version: 2, status: "active"}} = Directory.get(shard)
    assert live_user_version!(shard) == 2
    assert %{held: 0, waiting: 0} = BackgroundScanGate.stats()
  end

  # Expert review of e5f45291 (2026-10-31). Symptom: the replay and `verify_migrated` each waited
  # for a permit INSIDE the lease, after the live coordinator was drained, so a busy node kept the
  # tenant offline (`:shard_migrating`) for the whole wait, and each snooze re-did the drain and the
  # S3 pull. The run now takes its permit before draining, so a busy node must leave the live
  # coordinator untouched. Pre-fix the drain ran first and the coordinator went DOWN.
  test "a saturated node answers busy BEFORE draining the live coordinator", %{shard: shard} do
    seed_v1!(shard)
    {:ok, _} = Migrator.release(2, "add created_at", @v2_statements)

    prev_idle = Application.fetch_env(:fathom, :shard_idle_ms)
    Application.put_env(:fathom, :shard_idle_ms, 60_000)

    on_exit(fn ->
      case prev_idle do
        {:ok, v} -> Application.put_env(:fathom, :shard_idle_ms, v)
        :error -> Application.delete_env(:fathom, :shard_idle_ms)
      end

      Fathom.Shards.drain(shard, 2_000)
    end)

    {:ok, coord, ref, _path} = Fathom.Shards.checkout(shard)
    :ok = Fathom.Shard.checkin(coord, ref)
    mon = Process.monitor(coord)

    holders = saturate!(1)

    assert {:error, :background_scan_busy} = ShardMigration.run(shard, 2)
    refute_receive {:DOWN, ^mon, :process, ^coord, _}, 200, "the run drained a served tenant"
    assert {:ok, %{schema_version: 1, status: "active"}} = Directory.get(shard)

    free_holders!(holders)
  end

  # The other half of the same fix: once a run holds its permit, the scans below it do not wait
  # again. Pre-fix the replay released its permit, the node filled up, and `verify_migrated`
  # answered busy with the tenant drained and the work thrown away.
  test "a run keeps its permit through verify_migrated; a node filling up mid-run does not defer it",
       %{shard: shard} do
    seed_v1!(shard)
    Application.put_env(:fathom, :background_scan_permits, 2)
    Application.put_env(:fathom, :migration_transforms, [SaturateAfterReplay])
    {:ok, _} = Migrator.release(2, "add created_at", @v2_statements)
    :ok = Migrator.attach_transform(2, SaturateAfterReplay)

    {result, log} = with_log(fn -> ShardMigration.run(shard, 2) end)

    # Reap the transform's holders before any assertion can fail: a leaked holder keeps the app's
    # global gate full and wedges every later test in the run.
    for _ <- 1..2 do
      assert_receive {:holder, pid}
      on_exit(fn -> Process.exit(pid, :kill) end)
    end

    assert {:ok, %{from: 1, to: 2}} = result
    refute log =~ "failed quick_check"
    assert {:ok, %{schema_version: 2, status: "active"}} = Directory.get(shard)
    assert live_user_version!(shard) == 2
  end

  defp wait_until(fun, tries \\ 200) do
    cond do
      fun.() ->
        :ok

      tries == 0 ->
        flunk("condition never held")

      true ->
        receive do
        after
          5 -> wait_until(fun, tries - 1)
        end
    end
  end
end
