defmodule Fathom.Migrator.PostMigrateEngineTest do
  @moduledoc """
  Expert review 2026-11-07 #53, END TO END and under the AGENTS.md migration gate: a real
  `manage.py migrate` stream (verbatim, `test/support/fixtures/post_migrate_capture.json`: adding
  `widgets.0002_gadget`, then Django's post_migrate content-type / permission `bulk_create`s) is
  driven through `Fathom.ShardExecutor` against a real template shard, and the captured version is
  then replayed onto real tenants.

  Before the fix a tenant migrated by rollout lacked the new model's content type and its four
  permissions (5 / 20 vs 6 / 24 for a fork from `template@HEAD`), because the post_migrate
  transactions were parked as "awaiting bookkeeping" DDL, superseded by the next BEGIN, and never
  recorded.

    * (a) forward copy+transform on seeded v15 tenants, validating v16 (row counts, natural-key
      sets, ledger/`user_version`/directory agreement) — including a tenant whose
      `django_content_type` ids DIFFER from the template's, which is the case a verbatim replay of the
      captured literal `content_type_id` would silently corrupt;
    * (b) the revert flip, and re-running the rollout afterwards (idempotent);
    * (c) cross-version tolerance: a mixed v15/v16 fleet, no orphaned permission on either side.

  The version number equals the template's `django_migrations` count here on purpose: the agreement
  `PRAGMA user_version == count(django_migrations) == shards.schema_version` is the invariant that
  minting a version for the post_migrate transactions would have broken.
  """
  use Fathom.DataCase, async: false

  import ExUnit.CaptureLog

  alias Fathom.{Directory, Migrator, ShardExecutor}
  alias Fathom.Migrator.{PostMigrate, ShardMigration}
  alias Fathom.Shard.{Connection, Storage}
  alias Filo.Stmt

  @moduletag :capture_log
  @fixture "test/support/fixtures/post_migrate_capture.json"

  @ddl [
    ~s|CREATE TABLE "django_migrations" ("id" integer NOT NULL PRIMARY KEY AUTOINCREMENT, "app" varchar(255) NOT NULL, "name" varchar(255) NOT NULL, "applied" datetime NOT NULL)|,
    ~s|CREATE TABLE "django_content_type" ("id" integer NOT NULL PRIMARY KEY AUTOINCREMENT, "app_label" varchar(100) NOT NULL, "model" varchar(100) NOT NULL)|,
    ~s|CREATE UNIQUE INDEX "django_content_type_app_label_model_76bd3d3b_uniq" ON "django_content_type" ("app_label", "model")|,
    ~s|CREATE TABLE "auth_permission" ("id" integer NOT NULL PRIMARY KEY AUTOINCREMENT, "content_type_id" integer NOT NULL REFERENCES "django_content_type" ("id") DEFERRABLE INITIALLY DEFERRED, "codename" varchar(100) NOT NULL, "name" varchar(255) NOT NULL)|,
    ~s|CREATE INDEX "auth_permission_content_type_id_2f476e4b" ON "auth_permission" ("content_type_id")|,
    ~s|CREATE UNIQUE INDEX "auth_permission_content_type_id_codename_01ab375a_uniq" ON "auth_permission" ("content_type_id", "codename")|,
    ~s|CREATE TABLE "widgets_widget" ("id" integer NOT NULL PRIMARY KEY AUTOINCREMENT, "name" varchar(100) NOT NULL)|
  ]

  @natural_keys """
  SELECT c.app_label, c.model, p.codename FROM auth_permission p
  JOIN django_content_type c ON c.id = p.content_type_id ORDER BY 1, 2, 3
  """

  setup do
    template = "pmtemplate#{System.unique_integer([:positive])}"
    prev = Application.get_env(:fathom, :template_shard_id)
    Application.put_env(:fathom, :template_shard_id, template)

    me = self()
    handler = "pm-engine-#{System.unique_integer([:positive])}"

    :telemetry.attach_many(
      handler,
      [
        [:fathom, :migrator, :capture_awaiting_dropped],
        [:fathom, :migrator, :post_migrate_unattached],
        [:fathom, :migrator, :migration_gap]
      ],
      fn event, meas, _meta, _ -> send(me, {:alarm, List.last(event), meas}) end,
      nil
    )

    on_exit(fn ->
      :telemetry.detach(handler)

      if prev,
        do: Application.put_env(:fathom, :template_shard_id, prev),
        else: Application.delete_env(:fathom, :template_shard_id)

      cleanup(template)
    end)

    fixture = @fixture |> File.read!() |> Jason.decode!()
    %{template: template, fixture: fixture}
  end

  # --- fixtures -----------------------------------------------------------------------------

  # A Django SQLite database as `migrate` leaves it at count 15: ledger, content types, four
  # permissions per type. `cts` is `[[id, app_label, model], …]` — the ids are the point.
  defp build_db!(cts, user_version) do
    path = Path.join(System.tmp_dir!(), "pm_#{System.unique_integer([:positive])}.db")
    {:ok, conn} = Connection.open(path)
    :ok = Connection.exec(conn, "PRAGMA journal_mode=WAL")
    for sql <- @ddl, do: :ok = Connection.exec(conn, sql)

    for n <- 1..15 do
      {:ok, _} =
        Connection.query(
          conn,
          "INSERT INTO django_migrations (app, name, applied) VALUES (?, ?, ?)",
          ["baseline", String.pad_leading("#{n}", 4, "0"), "2026-11-01 00:00:00"]
        )
    end

    for [id, app, model] <- cts do
      {:ok, _} =
        Connection.query(
          conn,
          "INSERT INTO django_content_type (id, app_label, model) VALUES (?, ?, ?)",
          [id, app, model]
        )

      for verb <- ~w(add change delete view) do
        {:ok, _} =
          Connection.query(
            conn,
            "INSERT INTO auth_permission (name, content_type_id, codename) VALUES (?, ?, ?)",
            ["Can #{verb} #{model}", id, "#{verb}_#{model}"]
          )
      end
    end

    {:ok, _} = Connection.query(conn, "INSERT INTO widgets_widget (name) VALUES (?)", ["w1"])
    :ok = Connection.exec(conn, "PRAGMA user_version = #{user_version}")
    :ok = Connection.exec(conn, "PRAGMA wal_checkpoint(TRUNCATE)")
    Connection.close(conn)
    path
  end

  defp flush_db!(shard, path) do
    :ok = Storage.flush(shard, path)
    for s <- ["", "-wal", "-shm"], do: File.rm(path <> s)
    :ok
  end

  # The template's ids are the fixture's (1..5; widgets.gadget will get 6).
  defp seed_template!(template, fixture),
    do: flush_db!(template, build_db!(fixture["template"]["content_types"], 0))

  # A v15 tenant at the directory + storage level, like every rollout test seeds one. Same TYPES as
  # the template, ids from `cts`.
  defp seed_tenant!(prefix, cts) do
    shard = "#{prefix}#{System.unique_integer([:positive])}"
    on_exit(fn -> cleanup(shard) end)
    flush_db!(shard, build_db!(cts, 15))
    {:ok, _} = Directory.resolve(shard)
    {:ok, _} = Directory.cutover(shard, 15)
    shard
  end

  # Same five models, but the ids are NOT the template's: a tenant whose content types were created
  # in another order (or that took the manual `manage.py migrate` remedy) has exactly this.
  defp shifted_ids(fixture) do
    fixture["template"]["content_types"]
    |> Enum.reverse()
    |> Enum.with_index(11)
    |> Enum.map(fn {[_id, app, model], new_id} -> [new_id, app, model] end)
  end

  defp cleanup(id) do
    _ = Fathom.Shards.drain(id, 1_000)

    for base <- [
          Path.join(Fathom.Shard.data_dir(), "#{id}.db"),
          Path.join(remote_dir(), "#{id}.db")
        ],
        suffix <- ["", "-wal", "-shm", ".etag"],
        do: File.rm(base <> suffix)

    for p <- Path.wildcard(Path.join(remote_dir(), "#{id}@*")), do: File.rm(p)
    File.rm(Path.join(remote_dir(), "#{id}.lock"))
    :ok
  end

  defp remote_dir, do: Storage.Local.dir()

  # --- driving the template stream ----------------------------------------------------------

  defp exec!(conn, sql, args \\ []) do
    assert {:ok, _} = ShardExecutor.execute(conn, %Stmt{sql: sql, args: args})
  end

  # The fixture's statements, in order, through the REAL executor against the template shard — the
  # same stream Django's `migrate` put on the wire.
  defp run_stream!(template, fixture) do
    {:ok, conn} = ShardExecutor.open(template)

    for e <- fixture["events"] do
      case e["op"] do
        "begin" -> exec!(conn, "BEGIN")
        "commit" -> exec!(conn, "COMMIT")
        _ -> exec!(conn, e["sql"], e["args"])
      end
    end

    ShardExecutor.close(conn)
  end

  # Capture the template stream; returns the log it produced.
  defp capture_template!(template, fixture) do
    seed_template!(template, fixture)
    {:ok, _} = Migrator.release(15, "baseline", [], 15)
    assert Migrator.next_version() == 16, "fixture precondition: v16 is the next version"

    capture_log(fn -> run_stream!(template, fixture) end)
  end

  # --- reading a shard ----------------------------------------------------------------------

  defp query!(shard, sql) do
    tmp = Path.join(System.tmp_dir!(), "pmq_#{System.unique_integer([:positive])}.db")
    {:ok, _etag} = Storage.pull(shard, tmp)
    {:ok, conn} = Connection.open(tmp)
    {:ok, result} = Connection.query(conn, sql, [])
    Connection.close(conn)
    for s <- ["", "-wal", "-shm"], do: File.rm(tmp <> s)
    result.rows
  end

  defp scalar!(shard, sql), do: shard |> query!(sql) |> hd() |> hd()
  defp natural_keys(shard), do: query!(shard, @natural_keys)

  # Everything `migrate` would tell you about a shard's schema state.
  defp state!(shard) do
    {:ok, %{schema_version: schema_version}} = Directory.get(shard)

    %{
      user_version: scalar!(shard, "PRAGMA user_version"),
      ledger: scalar!(shard, "SELECT count(*) FROM django_migrations"),
      schema_version: schema_version,
      content_types: scalar!(shard, "SELECT count(*) FROM django_content_type"),
      permissions: scalar!(shard, "SELECT count(*) FROM auth_permission"),
      orphans:
        scalar!(
          shard,
          "SELECT count(*) FROM auth_permission p LEFT JOIN django_content_type c ON c.id = p.content_type_id WHERE c.id IS NULL"
        )
    }
  end

  # --- the tests ----------------------------------------------------------------------------

  test "the stream is captured as ONE version: post_migrate rows ride with it, no alarm", ctx do
    log = capture_template!(ctx.template, ctx.fixture)

    # No false alarm for post_migrate-shaped transactions, and no gap alarm (the count invariant).
    refute log =~ "AWAITING"
    refute log =~ "OUTSIDE capture"
    refute_received {:alarm, _, _}

    assert Migrator.head() == 16
    assert Migrator.next_version() == 17, "post_migrate must NOT mint a version of its own"
    assert Migrator.last_template_count() == 16
    assert Migrator.pending_review() == []

    statements = Migrator.statements(16)
    # CREATE TABLE, the django_migrations row, 1 content type, 4 permissions.
    assert length(statements) == 7
    assert Enum.count(statements, &PostMigrate.rewritten?/1) == 5

    # Nothing template-literal survives: the captured id 6 appears in no recorded arg.
    [{pairs, nil}] = Map.values(Migrator.statement_steps([16]))
    refute Enum.any?(pairs, fn {_sql, args} -> 6 in args end)
  end

  test "migration gate (a): rollout and fork converge on 6 content types / 24 permissions", ctx do
    %{template: template, fixture: fixture} = ctx
    capture_template!(template, fixture)

    # Two v15 tenants. `same` has the template's ids; `shifted` has different ones for the SAME types.
    same = seed_tenant!("pmsame", fixture["template"]["content_types"])
    shifted = seed_tenant!("pmshift", shifted_ids(fixture))

    # Preconditions the assertions below rest on (AGENTS.md: assert the fixture did its job).
    assert state!(same) |> Map.take([:content_types, :permissions]) == %{
             content_types: 5,
             permissions: 20
           }

    assert state!(shifted) |> Map.take([:content_types, :permissions]) == %{
             content_types: 5,
             permissions: 20
           }

    assert scalar!(same, "SELECT id FROM django_content_type WHERE model = 'widget'") !=
             scalar!(shifted, "SELECT id FROM django_content_type WHERE model = 'widget'"),
           "the two tenants must disagree about content-type ids, or this proves nothing"

    # Pre-existing tenant data is part of what must survive.
    assert scalar!(same, "SELECT count(*) FROM widgets_widget") == 1

    # The rollout path: the normal per-shard migration, v15 -> v16.
    assert {:ok, %{from: 15, to: 16}} = ShardMigration.run(same, 16)
    assert {:ok, %{from: 15, to: 16}} = ShardMigration.run(shifted, 16)

    # The fork path: `template@HEAD`.
    assert {:ok, 16} = Migrator.retain_template_head()
    forked = "pmfork#{System.unique_integer([:positive])}"
    on_exit(fn -> cleanup(forked) end)
    assert {:ok, _} = Migrator.fork_from_template(forked)

    template_keys = natural_keys(template)
    assert length(template_keys) == 24
    assert ["widgets", "gadget", "add_gadget"] in template_keys

    for {label, shard} <- [same: same, shifted: shifted, forked: forked] do
      # `PRAGMA user_version == count(django_migrations) == shards.schema_version` — the agreement a
      # version minted for post_migrate would have broken — on the rollout-migrated AND fork-born tenants.
      assert %{
               user_version: 16,
               ledger: 16,
               schema_version: 16,
               content_types: 6,
               permissions: 24,
               orphans: 0
             } = state!(shard),
             "#{label}: wrong state"

      # Same (app_label, model, codename) set as the template — i.e. each permission is attached to
      # ITS content type on this tenant, whatever ids it has.
      assert natural_keys(shard) == template_keys, "#{label}: permission set diverged"
    end

    # The shifted tenant's new content type took ITS next id, not the template's 6.
    assert scalar!(shifted, "SELECT id FROM django_content_type WHERE model = 'gadget'") != 6

    # Tenant data is untouched.
    assert scalar!(same, "SELECT count(*) FROM widgets_widget") == 1

    refute_received {:alarm, _, _}
  end

  test "idempotent: re-running the post_migrate statements on a converged tenant changes nothing",
       ctx do
    %{template: template, fixture: fixture} = ctx
    capture_template!(template, fixture)
    tenant = seed_tenant!("pmidem", shifted_ids(fixture))
    assert {:ok, %{to: 16}} = ShardMigration.run(tenant, 16)

    before = {state!(tenant), natural_keys(tenant)}

    [{pairs, nil}] = Map.values(Migrator.statement_steps([16]))
    rows = Enum.filter(pairs, fn {sql, _} -> PostMigrate.rewritten?(sql) end)
    assert length(rows) == 5

    tmp = Path.join(System.tmp_dir!(), "pmidem_#{System.unique_integer([:positive])}.db")
    {:ok, _etag} = Storage.pull(tenant, tmp)
    {:ok, conn} = Connection.open(tmp)

    for _ <- 1..2, {sql, args} <- rows do
      assert {:ok, %{rows: []}} = Connection.query(conn, sql, args)
    end

    {:ok, %{rows: [[perms]]}} = Connection.query(conn, "SELECT count(*) FROM auth_permission", [])

    {:ok, %{rows: [[cts]]}} =
      Connection.query(conn, "SELECT count(*) FROM django_content_type", [])

    Connection.close(conn)
    for s <- ["", "-wal", "-shm"], do: File.rm(tmp <> s)

    assert {cts, perms} == {6, 24}
    assert before == {state!(tenant), natural_keys(tenant)}
  end

  test "migration gate (b)+(c): revert is a pointer flip; a mixed v15/v16 fleet reads cleanly",
       ctx do
    %{template: template, fixture: fixture} = ctx
    capture_template!(template, fixture)

    migrated = seed_tenant!("pmrev", shifted_ids(fixture))
    laggard = seed_tenant!("pmlag", fixture["template"]["content_types"])

    assert {:ok, %{from: 15, to: 16}} = ShardMigration.run(migrated, 16)

    # (c) Mixed fleet: v16 and v15 tenants side by side. Both are internally consistent (no
    # permission pointing at a content type that is not there) and the laggard simply lacks the new
    # model's rows — the app tolerates both, as expand-contract requires.
    assert %{content_types: 6, permissions: 24, orphans: 0, user_version: 16} = state!(migrated)
    assert %{content_types: 5, permissions: 20, orphans: 0, user_version: 15} = state!(laggard)
    assert length(natural_keys(laggard)) == 20
    assert Enum.all?(natural_keys(laggard), fn row -> row in natural_keys(migrated) end)

    # (b) The revert flip restores the retained pre-migration object: the post_migrate rows are
    # simply absent from it (a revert cannot leave rows behind on the live file; the v16 copy that
    # held them is the retained backup, dropped by the retirement job).
    assert {:ok, %{from: 16, to: 15}} = ShardMigration.revert(migrated, 15)

    assert %{
             user_version: 15,
             ledger: 15,
             schema_version: 15,
             content_types: 5,
             permissions: 20,
             orphans: 0
           } = state!(migrated)

    # Re-rolling after a revert converges again (the statements are idempotent and v16 is intact).
    assert {:ok, %{from: 15, to: 16}} = ShardMigration.run(migrated, 16)

    assert %{user_version: 16, ledger: 16, schema_version: 16, content_types: 6, permissions: 24} =
             state!(migrated)
  end

  # The capture hook (and the PostMigrate rewrite inside it) runs for the TEMPLATE stream only. A
  # tenant executing the very same post_migrate statements — a Django app pointed at a tenant with
  # `manage.py migrate`, the documented per-tenant remedy — is neither captured nor rewritten, so
  # the tenant path pays nothing and the registry is untouched.
  test "a tenant stream carrying post_migrate statements is not captured", ctx do
    %{template: template, fixture: fixture} = ctx
    seed_template!(template, fixture)
    {:ok, _} = Migrator.release(15, "baseline", [], 15)

    tenant = seed_tenant!("pmtenantstream", fixture["template"]["content_types"])
    {:ok, conn} = ShardExecutor.open(tenant)

    for e <- fixture["events"],
        e["op"] in ["begin", "commit"] or post_migrate_insert?(e["sql"]) do
      case e["op"] do
        "begin" -> exec!(conn, "BEGIN")
        "commit" -> exec!(conn, "COMMIT")
        _ -> exec!(conn, e["sql"], e["args"])
      end
    end

    ShardExecutor.close(conn)

    assert Migrator.head() == 15
    assert Migrator.statements(15) == []
    refute_received {:alarm, _, _}
  end

  defp post_migrate_insert?(sql) do
    is_binary(sql) and
      (String.starts_with?(sql, ~s|INSERT INTO "django_content_type"|) or
         String.starts_with?(sql, ~s|INSERT INTO "auth_permission"|))
  end
end
