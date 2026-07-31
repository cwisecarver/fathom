defmodule Fathom.Migrator.ForkTest do
  # Fork-from-template (finding #10): a novel shard is born AT the fleet HEAD by
  # copying the retained `template@HEAD` snapshot, instead of empty at v0 — so a new
  # tenant's first ORM query finds its schema. Not async: shards are addressed by a
  # global Registry, back onto real files, and these tests flip global config
  # (:template_shard_id, :fork_from_template, :shard_storage).
  use Fathom.DataCase, async: false

  # These build fixtures by releasing versions with a capture template configured, which is
  # exactly the configuration `Migrator.release/6` warns about (novel tenants born empty).
  # The warning is correct here and not what these tests are about, so capture it: ExUnit
  # still prints captured logs when a test FAILS, so this hides noise without hiding signal.
  @moduletag :capture_log

  alias Fathom.Directory
  alias Fathom.Migrator
  alias Fathom.Shard.{Connection, Storage}
  alias Fathom.ShardExecutor
  alias Filo.{Error, Stmt, StmtResult}

  setup do
    template = "tmplfork#{System.unique_integer([:positive])}"
    prev = Application.get_env(:fathom, :template_shard_id)
    Application.put_env(:fathom, :template_shard_id, template)

    on_exit(fn ->
      if prev,
        do: Application.put_env(:fathom, :template_shard_id, prev),
        else: Application.delete_env(:fathom, :template_shard_id)

      cleanup_shard(template)
    end)

    %{template: template}
  end

  # --- helpers ---------------------------------------------------------------

  defp stmt(sql, args \\ []), do: %Stmt{sql: sql, args: args}

  # Seed the template's STORED object with a schema-only database: Django's
  # bookkeeping table (with one row — schema metadata, not tenant data) plus an
  # empty application table. This is the template contract: schema, zero tenant rows.
  defp seed_template!(template) do
    seed = Path.join(System.tmp_dir!(), "seedt_#{template}.db")
    {:ok, conn} = Connection.open(seed)

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

    :ok = Connection.exec(conn, "CREATE TABLE app_user (id INTEGER PRIMARY KEY, name TEXT)")
    :ok = Connection.exec(conn, "PRAGMA wal_checkpoint(TRUNCATE)")
    Connection.close(conn)
    :ok = Storage.flush(template, seed)
    for s <- ["", "-wal", "-shm"], do: File.rm(seed <> s)
    :ok
  end

  # A tenant id unique to this test run, with its files cleaned up afterward.
  defp new_tenant do
    tenant = "tenantfork#{System.unique_integer([:positive])}"
    on_exit(fn -> cleanup_shard(tenant) end)
    tenant
  end

  defp cleanup_shard(id) do
    # Stop any coordinator first (flushes + releases), then remove every artifact:
    # local file + sidecars, stored object, retained @version copies, lock.
    _ = Fathom.Shards.drain(id, 1_000)

    for base <- [Path.join(local_dir(), "#{id}.db"), Path.join(remote_dir(), "#{id}.db")],
        suffix <- ["", "-wal", "-shm", ".etag"] do
      File.rm(base <> suffix)
    end

    for p <- Path.wildcard(Path.join(remote_dir(), "#{id}@*")), do: File.rm(p)
    File.rm(Path.join(remote_dir(), "#{id}.lock"))
    :ok
  end

  # The stored object's PRAGMA user_version, read off a COPY so the check never
  # mutates the "remote" object (opening SQLite in WAL mode rewrites the header).
  defp stored_version(id) do
    copy = Path.join(System.tmp_dir!(), "storedv_#{id}_#{System.unique_integer([:positive])}.db")
    File.cp!(Path.join(remote_dir(), "#{id}.db"), copy)
    {:ok, conn} = Connection.open(copy)
    {:ok, %{rows: [[version]]}} = Connection.query(conn, "PRAGMA user_version", [])
    Connection.close(conn)
    for s <- ["", "-wal", "-shm"], do: File.rm(copy <> s)
    version
  end

  defp remote_object(id), do: Path.join(remote_dir(), "#{id}.db")

  defp count!(conn, table) do
    {:ok, %StmtResult{rows: [[n]]}} =
      ShardExecutor.execute(conn, stmt("SELECT count(*) FROM #{table}"))

    n
  end

  # --- retain_template_head/1 -------------------------------------------------

  describe "retain_template_head/1" do
    test "drains the template and retains its stored object at HEAD", %{template: template} do
      seed_template!(template)
      {:ok, _} = Migrator.release(2, "v2", ["SELECT 1"])

      assert {:ok, 2} = Migrator.retain_template_head()
      assert File.exists?(Path.join(remote_dir(), "#{template}@2.db"))
    end

    test "returns :no_template when no template shard is configured" do
      Application.delete_env(:fathom, :template_shard_id)
      {:ok, _} = Migrator.release(1, "v1", ["SELECT 1"])

      assert {:error, :no_template} = Migrator.retain_template_head()
    end

    test "returns :no_head when no fleet version is released" do
      assert {:error, :no_head} = Migrator.retain_template_head()
    end

    test "returns :no_template_object when the template has never flushed", %{template: template} do
      {:ok, _} = Migrator.release(1, "v1", ["SELECT 1"])

      assert {:error, :no_template_object} = Migrator.retain_template_head()
      refute File.exists?(Path.join(remote_dir(), "#{template}@1.db"))
    end
  end

  # --- fork_from_template/1 (the forward path) --------------------------------

  describe "fork_from_template/1" do
    test "births a novel shard AT HEAD: schema present, user_version stamped, directory at HEAD",
         %{template: template} do
      seed_template!(template)
      {:ok, _} = Migrator.release(2, "v2", ["SELECT 1"])
      {:ok, 2} = Migrator.retain_template_head()

      tenant = new_tenant()
      assert {:ok, %{version: 2}} = Migrator.fork_from_template(tenant)

      # The stored object exists and is stamped at HEAD.
      assert File.exists?(remote_object(tenant))
      assert stored_version(tenant) == 2

      # The directory row is registered AT HEAD (the third stamp).
      assert {:ok, %{schema_version: 2, status: "active"}} = Directory.get(tenant)

      # The shard serves through the normal data path with the template's schema:
      # the new tenant's first query finds its tables (the finding-#10 contract).
      {:ok, conn} = ShardExecutor.open(tenant)
      assert count!(conn, "app_user") == 0
      assert count!(conn, "django_migrations") == 1

      assert {:ok, %StmtResult{rows: [[2]]}} =
               ShardExecutor.execute(conn, stmt("PRAGMA user_version"))

      :ok = ShardExecutor.close(conn)
    end

    # ISOLATION (multi-tenant safety, the critical gate): a forked tenant inherits the
    # SCHEMA only — zero tenant-data rows — and two independently-forked tenants share
    # nothing: a write to one is invisible to the other and to later forks.
    test "forked tenants carry zero tenant rows and never cross-contaminate", %{
      template: template
    } do
      seed_template!(template)
      {:ok, _} = Migrator.release(1, "v1", ["SELECT 1"])
      {:ok, 1} = Migrator.retain_template_head()

      tenant_a = new_tenant()
      tenant_b = new_tenant()
      assert {:ok, _} = Migrator.fork_from_template(tenant_a)
      assert {:ok, _} = Migrator.fork_from_template(tenant_b)

      {:ok, a} = ShardExecutor.open(tenant_a)
      {:ok, b} = ShardExecutor.open(tenant_b)

      # Born with schema but ZERO tenant rows (only django_migrations bookkeeping).
      assert count!(a, "app_user") == 0
      assert count!(b, "app_user") == 0

      # A write to tenant A…
      assert {:ok, %StmtResult{affected_row_count: 1}} =
               ShardExecutor.execute(a, stmt("INSERT INTO app_user (name) VALUES (?)", ["alice"]))

      assert count!(a, "app_user") == 1

      # …never appears in tenant B (shard isolation)…
      assert count!(b, "app_user") == 0

      # …and never leaks back into the fork source: a THIRD tenant forked after the
      # write is still born empty (the snapshot is immutable).
      tenant_c = new_tenant()
      assert {:ok, _} = Migrator.fork_from_template(tenant_c)
      {:ok, c} = ShardExecutor.open(tenant_c)
      assert count!(c, "app_user") == 0

      for conn <- [a, b, c], do: :ok = ShardExecutor.close(conn)
    end

    test "is idempotent: re-forking an already-forked shard re-registers without clobbering data",
         %{template: template} do
      seed_template!(template)
      {:ok, _} = Migrator.release(1, "v1", ["SELECT 1"])
      {:ok, 1} = Migrator.retain_template_head()

      tenant = new_tenant()
      assert {:ok, %{version: 1}} = Migrator.fork_from_template(tenant)

      # The tenant writes data, which flushes to its stored object on drain.
      {:ok, conn} = ShardExecutor.open(tenant)

      {:ok, _} =
        ShardExecutor.execute(conn, stmt("INSERT INTO app_user (name) VALUES (?)", ["bob"]))

      :ok = ShardExecutor.close(conn)
      :ok = Fathom.Shards.drain(tenant, 2_000)

      # A re-fork (a crashed-after-flush retry, or a double-mint race) must be a
      # crash-forward no-op — never a re-copy that would erase the tenant's rows.
      assert {:ok, %{version: 1}} = Migrator.fork_from_template(tenant)

      {:ok, conn} = ShardExecutor.open(tenant)
      assert count!(conn, "app_user") == 1
      :ok = ShardExecutor.close(conn)
    end

    test "refuses a dst whose stored object is at another version (not ours to overwrite)",
         %{template: template} do
      seed_template!(template)
      {:ok, _} = Migrator.release(2, "v2", ["SELECT 1"])
      {:ok, 2} = Migrator.retain_template_head()

      # An existing shard object stamped at v1 (e.g. a real tenant whose directory
      # row was lost) must never be overwritten by a fork.
      tenant = new_tenant()
      seed = Path.join(System.tmp_dir!(), "seedx_#{tenant}.db")
      {:ok, conn} = Connection.open(seed)
      :ok = Connection.exec(conn, "CREATE TABLE app_thing (id INTEGER PRIMARY KEY)")
      :ok = Connection.exec(conn, "INSERT INTO app_thing (id) VALUES (7)")
      :ok = Connection.exec(conn, "PRAGMA user_version = 1")
      :ok = Connection.exec(conn, "PRAGMA wal_checkpoint(TRUNCATE)")
      Connection.close(conn)
      :ok = Storage.flush(tenant, seed)
      for s <- ["", "-wal", "-shm"], do: File.rm(seed <> s)

      before = File.read!(remote_object(tenant))
      assert {:error, :dst_exists} = Migrator.fork_from_template(tenant)
      assert File.read!(remote_object(tenant)) == before, "the existing object must be untouched"
    end

    test "returns :no_template_snapshot when HEAD is 0, template unset, or no snapshot retained",
         %{template: template} do
      tenant = new_tenant()

      # HEAD 0 (no release).
      assert {:error, :no_template_snapshot} = Migrator.fork_from_template(tenant)

      # Released but no template@HEAD snapshot object retained.
      {:ok, _} = Migrator.release(1, "v1", ["SELECT 1"])
      assert {:error, :no_template_snapshot} = Migrator.fork_from_template(tenant)
      refute File.exists?(remote_object(tenant)), "a failed fork must leave no live object"

      # No template configured at all.
      Application.put_env(:fathom, :template_shard_id, nil)
      assert {:error, :no_template_snapshot} = Migrator.fork_from_template(tenant)
      Application.put_env(:fathom, :template_shard_id, template)
    end

    test "refuses forking the template onto itself", %{template: template} do
      {:ok, _} = Migrator.release(1, "v1", ["SELECT 1"])
      assert {:error, :template_shard} = Migrator.fork_from_template(template)
    end

    # If the fork fails AFTER the snapshot copy landed but BEFORE the stamp flushed,
    # the dst must be left with NO live object — an unstamped schema copy at v0 would
    # be corrupted by the next rollout (replaying DDL over already-present tables).
    test "a fork that fails mid-stamp leaves no live object and the shard is born empty",
         %{template: template} do
      prev_backend = Application.get_env(:fathom, :shard_storage)
      Application.put_env(:fathom, :shard_storage, Fathom.Test.FaultyStorage)

      on_exit(fn ->
        Application.delete_env(:fathom, :storage_fault)

        if prev_backend,
          do: Application.put_env(:fathom, :shard_storage, prev_backend),
          else: Application.delete_env(:fathom, :shard_storage)
      end)

      seed_template!(template)
      {:ok, _} = Migrator.release(1, "v1", ["SELECT 1"])
      {:ok, 1} = Migrator.retain_template_head()

      tenant = new_tenant()

      # The stamped flush fails (S3 unreachable): the fork must clean up the copied
      # object rather than leave an unstamped fork.
      Application.put_env(:fathom, :storage_fault, :flush)
      assert {:error, :s3_unreachable} = Migrator.fork_from_template(tenant)
      refute File.exists?(remote_object(tenant)), "a failed fork must leave NO live object"

      # With the fault cleared, the retry forks cleanly.
      Application.delete_env(:fathom, :storage_fault)
      assert {:ok, %{version: 1}} = Migrator.fork_from_template(tenant)
      assert stored_version(tenant) == 1
    end
  end

  # --- the admission hook (Fathom.Shards) --------------------------------------

  describe "novel-shard admission" do
    test "with :fork_from_template on, a novel checkout is born at HEAD end to end",
         %{template: template} do
      Application.put_env(:fathom, :fork_from_template, true)
      on_exit(fn -> Application.put_env(:fathom, :fork_from_template, false) end)

      seed_template!(template)
      {:ok, _} = Migrator.release(3, "v3", ["SELECT 1"])
      {:ok, 3} = Migrator.retain_template_head()

      tenant = new_tenant()
      {:ok, conn} = ShardExecutor.open(tenant)

      # First query on a brand-new tenant finds the schema — no `no such table`.
      assert count!(conn, "app_user") == 0

      assert {:ok, %StmtResult{rows: [[3]]}} =
               ShardExecutor.execute(conn, stmt("PRAGMA user_version"))

      assert {:ok, %{schema_version: 3, status: "active"}} = Directory.get(tenant)
      :ok = ShardExecutor.close(conn)
    end

    test "with :fork_from_template on but HEAD 0, a novel shard is born empty and still serves",
         %{template: template} do
      Application.put_env(:fathom, :fork_from_template, true)
      on_exit(fn -> Application.put_env(:fathom, :fork_from_template, false) end)

      seed_template!(template)

      tenant = new_tenant()

      # This IS a fork fallback (HEAD 0 ⇒ nothing to fork from), so it now logs loudly.
      # Captured to keep it out of the suite's output; the alarm itself is asserted below.
      ExUnit.CaptureLog.capture_log(fn ->
        {:ok, conn} = ShardExecutor.open(tenant)

        # Born empty: no template schema…
        assert {:error, %Error{}} =
                 ShardExecutor.execute(conn, stmt("SELECT count(*) FROM app_user"))

        # …but serving works (no crash, a usable empty shard — today's behavior).
        assert {:ok, %StmtResult{}} =
                 ShardExecutor.execute(conn, stmt("CREATE TABLE kv (k INTEGER PRIMARY KEY)"))

        assert {:ok, %StmtResult{affected_row_count: 1}} =
                 ShardExecutor.execute(conn, stmt("INSERT INTO kv VALUES (1)"))

        :ok = ShardExecutor.close(conn)
      end)
    end

    # With :fork_from_template ON the fork IS the birth path, so a fallback is not a
    # harmless degrade: the tenant serves with NO schema, its first ORM query fails, and the
    # rollout cannot rescue it either (replaying v1 onto an empty file dies on `no such table:
    # django_migrations` — see django_replay_test.exs). Before this, `fork_novel/1` discarded
    # the outcome entirely, so a born-empty tenant was indistinguishable from a healthy one.
    # The invariant: every fallback under an enabled flag is observable, and a healthy birth
    # is never noise.
    test "a fork fallback emits [:fathom, :migrator, :fork_fallback] and still serves",
         %{template: template} do
      Application.put_env(:fathom, :fork_from_template, true)
      on_exit(fn -> Application.put_env(:fathom, :fork_from_template, false) end)

      seed_template!(template)
      # No release ⇒ HEAD 0 ⇒ no `template@HEAD` snapshot to fork from. This is the
      # misconfiguration an operator hits by enabling the flag without ever running
      # `mix fathom.snapshot template-head`.
      attach_fork_fallback!()

      tenant = new_tenant()

      log =
        ExUnit.CaptureLog.capture_log(fn ->
          {:ok, conn} = ShardExecutor.open(tenant)

          # Born empty and still SERVING — the checkout is never failed for a fork.
          assert {:error, %Error{}} =
                   ShardExecutor.execute(conn, stmt("SELECT count(*) FROM app_user"))

          :ok = ShardExecutor.close(conn)
        end)

      assert_receive {:fork_fallback, %{shard_id: ^tenant, reason: :no_template_snapshot}}
      assert log =~ "born EMPTY"
      assert log =~ "mix fathom.snapshot template-head"
    end

    test "a SUCCESSFUL fork is silent — no fallback alarm on the healthy path",
         %{template: template} do
      Application.put_env(:fathom, :fork_from_template, true)
      on_exit(fn -> Application.put_env(:fathom, :fork_from_template, false) end)

      seed_template!(template)
      {:ok, _} = Migrator.release(1, "v1", ["SELECT 1"])
      {:ok, 1} = Migrator.retain_template_head()
      attach_fork_fallback!()

      tenant = new_tenant()
      {:ok, conn} = ShardExecutor.open(tenant)
      assert count!(conn, "app_user") == 0
      :ok = ShardExecutor.close(conn)

      refute_receive {:fork_fallback, _}, 200
    end

    test "with the flag OFF, born-empty is the configured behavior and does NOT alarm",
         %{template: template} do
      seed_template!(template)
      {:ok, _} = Migrator.release(1, "v1", ["SELECT 1"])
      {:ok, 1} = Migrator.retain_template_head()
      attach_fork_fallback!()

      tenant = new_tenant()
      {:ok, conn} = ShardExecutor.open(tenant)
      :ok = ShardExecutor.close(conn)

      # The alarm means "the fork was supposed to birth this tenant and did not". With the
      # flag off nothing was supposed to, so firing here would be pure noise on the default
      # configuration.
      refute_receive {:fork_fallback, _}, 200
    end

    test "with the flag OFF (default), a novel shard is born empty even with a snapshot ready",
         %{template: template} do
      seed_template!(template)
      {:ok, _} = Migrator.release(1, "v1", ["SELECT 1"])
      {:ok, 1} = Migrator.retain_template_head()

      tenant = new_tenant()
      {:ok, conn} = ShardExecutor.open(tenant)

      # Unchanged current behavior: no fork happened.
      assert {:error, %Error{}} =
               ShardExecutor.execute(conn, stmt("SELECT count(*) FROM app_user"))

      assert {:ok, %StmtResult{rows: [[0]]}} =
               ShardExecutor.execute(conn, stmt("PRAGMA user_version"))

      :ok = ShardExecutor.close(conn)
      assert :error = Directory.get(tenant), "no directory row is force-registered"
    end
  end

  # Forwards [:fathom, :migrator, :fork_fallback] to the test process. Detached on exit so a
  # non-async test that follows never receives another test's event.
  defp attach_fork_fallback! do
    test_pid = self()
    handler = "fork-fallback-#{System.unique_integer([:positive])}"

    :telemetry.attach(
      handler,
      [:fathom, :migrator, :fork_fallback],
      fn _e, _m, meta, _ -> send(test_pid, {:fork_fallback, meta}) end,
      nil
    )

    on_exit(fn -> :telemetry.detach(handler) end)
  end

  defp local_dir, do: Fathom.Shard.data_dir()
  defp remote_dir, do: Fathom.Shard.Storage.Local.dir()
end
