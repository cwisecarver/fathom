defmodule Fathom.Migrator.DjangoReplayTest do
  @moduledoc """
  The migration engine driven by REAL `manage.py migrate` output, end to end.

  Every other test in this suite writes its SQL by hand. That is how six separate bugs survived in
  a subsystem that looked well covered: hand-written SQL never has Django's shapes in it, so no test
  ever exercised the seams that actually break. Running a real migrate through the engine once broke
  it at capture, at the review lint, at migrate-on-touch, at the HEAD cache, and twice at replay.

  `test/support/fixtures/django_migrate_capture.json` is the VERBATIM capture of
  `manage.py migrate` for djathom's `finance` app under Django 5.0 (taken 2026-07-31): v1 is
  `0001_initial` (24 statements) and v2 is `0002_budget` (5). It carries the shapes that broke
  things:

    * a **parameterized** `INSERT INTO django_migrations … VALUES (?, ?, ?)` — the values ride in
      args, and replaying the text alone bound NULL against a NOT NULL column, aborting every copy
    * another parameterized statement (`SELECT QUOTE(?), QUOTE(?), QUOTE(?)`) from Django's schema
      editor
    * the SQLite **table-rebuild** (`CREATE TABLE new__x` / `INSERT … SELECT` / `DROP` / `RENAME`)
      that the data-migration lint used to flag, freezing the fleet below almost every real migration
    * `PRAGMA foreign_key_check` inside the replay transaction — kept in the fixture because it is a
      verbatim record of what Django sent in July. Capture STRIPS it now (`Capture.pure_read?/1`),
      so a version captured today would not carry it; "dropping Django's introspection leaves the
      replayed database identical" below is the evidence that the two are equivalent.

  If Django changes what it emits, re-capture the fixture rather than hand-editing it — the point of
  this file is that nothing in it was written by us.
  """
  use Fathom.DataCase, async: false

  alias Fathom.Migrator
  alias Fathom.Migrator.{Capture, Copy, ShardMigration}
  alias Fathom.Shard.{Connection, Storage}

  @fixture "test/support/fixtures/django_migrate_capture.json"

  setup do
    shard = "djrep_#{System.unique_integer([:positive])}"

    on_exit(fn ->
      for path <- Path.wildcard(Path.join(Storage.Local.dir(), "#{shard}*")), do: File.rm(path)

      for path <- Path.wildcard(Path.join(Fathom.Shard.data_dir(), "#{shard}*")),
          do: File.rm(path)
    end)

    %{shard: shard, capture: load_fixture()}
  end

  defp load_fixture do
    @fixture
    |> File.read!()
    |> Jason.decode!()
    |> Enum.map(fn v ->
      args =
        (v["statement_args"] || [])
        |> Enum.map(fn %{"args" => list} -> Enum.map(list, &Filo.Value.decode/1) end)

      %{
        version: v["version"],
        statements: v["statements"],
        args: args,
        count: v["template_migration_count"],
        requires_review: v["requires_review"]
      }
    end)
    |> Enum.sort_by(& &1.version)
  end

  defp release!(v) do
    {:ok, _} =
      Migrator.release(v.version, "fixture", v.statements, v.count, v.requires_review, v.args)

    :ok
  end

  defp pairs(v), do: Migrator.statement_pairs(v.version)

  defp query!(path, sql) do
    {:ok, conn} = Connection.open(path)
    {:ok, result} = Connection.query(conn, sql, [])
    Connection.close(conn)
    result
  end

  # A database in the state Django leaves one BEFORE any migration runs: `django_migrations` exists
  # (Django's recorder creates it in autocommit, so it is NOT part of any captured migration) and
  # nothing else does. Replay assumes that table is already there — its own bookkeeping INSERT
  # targets it — so this is the realistic starting point, and it is what a tenant forked from the
  # template inherits. See `replay onto a database with no django_migrations` below for what happens
  # when it is genuinely absent.
  defp fresh_django_db!(name) do
    path = Path.join(System.tmp_dir!(), "#{name}_#{System.unique_integer([:positive])}.db")
    {:ok, conn} = Connection.open(path)
    :ok = Connection.exec(conn, "PRAGMA journal_mode=WAL")

    :ok =
      Connection.exec(
        conn,
        ~s|CREATE TABLE "django_migrations" ("id" integer NOT NULL PRIMARY KEY AUTOINCREMENT, | <>
          ~s|"app" varchar(255) NOT NULL, "name" varchar(255) NOT NULL, "applied" datetime NOT NULL)|
      )

    :ok = Connection.exec(conn, "PRAGMA wal_checkpoint(TRUNCATE)")
    Connection.close(conn)
    on_exit(fn -> for s <- ["", "-wal", "-shm"], do: File.rm(path <> s) end)
    path
  end

  defp bare_db!(name) do
    path = Path.join(System.tmp_dir!(), "#{name}_#{System.unique_integer([:positive])}.db")
    {:ok, conn} = Connection.open(path)
    :ok = Connection.exec(conn, "PRAGMA journal_mode=WAL")
    Connection.close(conn)
    on_exit(fn -> for s <- ["", "-wal", "-shm"], do: File.rm(path <> s) end)
    path
  end

  test "a real Django migration is released unflagged (the lint tolerates its real shapes)",
       %{capture: [v1, v2]} do
    :ok = release!(v1)
    :ok = release!(v2)

    # Neither is held for review. `0001_initial` contains a table REBUILD whose copy step is an
    # INSERT … SELECT; flagging that as a template-literal data migration used to cap HEAD below
    # essentially every real migration, so the rollout could never proceed unattended.
    assert Migrator.pending_review() == []
    assert Migrator.head() == 2
  end

  test "replaying v1 onto an empty file produces the schema AND a correct django_migrations row",
       %{capture: [v1, _]} do
    :ok = release!(v1)
    source = fresh_django_db!("src")
    dest = fresh_django_db!("dst")

    assert :ok = Copy.migrate(source, dest, v1.version, pairs(v1))

    tables =
      query!(dest, "SELECT name FROM sqlite_master WHERE type='table' ORDER BY name").rows
      |> List.flatten()

    assert "finance_account" in tables
    assert "finance_transaction" in tables
    assert "django_migrations" in tables

    # THE regression: Django's bookkeeping row is parameterized, so replaying the statement text
    # with no args bound NULL and died on `NOT NULL constraint failed: django_migrations.app`,
    # rolling back the whole copy. Every Django migration ends with this row, so nothing could ever
    # be replayed. The values must arrive intact — and as BOUND values, never interpolated.
    assert %{rows: [["finance", "0001_initial"]]} =
             query!(dest, "SELECT app, name FROM django_migrations")

    assert %{rows: [[1]]} = query!(dest, "PRAGMA user_version")
  end

  test "replaying v2 onto a v1 tenant adds the new table and keeps the tenant's rows",
       %{capture: [v1, v2]} do
    :ok = release!(v1)
    :ok = release!(v2)

    source = fresh_django_db!("src")
    at_v1 = fresh_django_db!("v1")
    assert :ok = Copy.migrate(source, at_v1, v1.version, pairs(v1))

    # Tenant data written while the shard is at v1 — this is what a blue/green copy must carry.
    {:ok, conn} = Connection.open(at_v1)

    :ok =
      Connection.exec(
        conn,
        "INSERT INTO finance_account (id, name, type, institution, currency, opening_balance_cents, archived) " <>
          "VALUES (1, 'Checking', 'checking', 'Bank', 'USD', 12345, 0)"
      )

    :ok = Connection.exec(conn, "PRAGMA wal_checkpoint(TRUNCATE)")
    Connection.close(conn)

    at_v2 = fresh_django_db!("v2")
    assert :ok = Copy.migrate(at_v1, at_v2, v2.version, pairs(v2))

    # The new table exists…
    assert %{rows: [[1]]} =
             query!(
               at_v2,
               "SELECT count(*) FROM sqlite_master WHERE type='table' AND name='finance_budget'"
             )

    # …the tenant's own row survived the copy untouched…
    assert %{rows: [[1, "Checking", 12345]]} =
             query!(at_v2, "SELECT id, name, opening_balance_cents FROM finance_account")

    # …and Django's ledger carries BOTH migrations with real values, not NULLs.
    assert %{rows: [["finance", "0001_initial"], ["finance", "0002_budget"]]} =
             query!(at_v2, "SELECT app, name FROM django_migrations ORDER BY name")

    assert %{rows: [[2]]} = query!(at_v2, "PRAGMA user_version")
  end

  test "the full chain walks a stored shard 0 -> HEAD and agrees on all three version stamps",
       %{shard: shard, capture: [v1, v2]} do
    :ok = release!(v1)
    :ok = release!(v2)

    # A brand-new tenant: an empty stored object at version 0.
    seed = fresh_django_db!("seed")
    :ok = Storage.flush(shard, seed)
    {:ok, _} = Fathom.Directory.resolve(shard)

    # One job walks it the whole way (0 -> 1 -> 2), each version its own transaction.
    assert {:ok, %{from: 0, to: 2}} = ShardMigration.run(shard, 2)

    # Stamp #1: the Postgres directory.
    assert {:ok, %{schema_version: 2, status: "active"}} = Fathom.Directory.get(shard)

    local = Path.join(Fathom.Shard.data_dir(), "#{shard}.db")
    {:ok, _} = Storage.pull(shard, local)

    # Stamp #2: the file's own PRAGMA user_version. These disagreeing is what made a forked tenant
    # replay from scratch and hit "table already exists".
    assert %{rows: [[2]]} = query!(local, "PRAGMA user_version")

    # Stamp #3: Django's own ledger inside the shard — the one the docs call the truth.
    assert %{rows: [["finance", "0001_initial"], ["finance", "0002_budget"]]} =
             query!(local, "SELECT app, name FROM django_migrations ORDER BY name")

    # Idempotent: re-running against a shard already at HEAD is a no-op, not a re-replay.
    assert :ok = ShardMigration.run(shard, 2)

    assert {:ok, %{schema_version: 2}} = Fathom.Directory.get(shard)
  end

  # A tenant born EMPTY cannot be migrated from 0, because `django_migrations` is not part of any
  # captured migration — Django's recorder creates it in autocommit, before migrations run. fathom
  # births a tenant by forking the template (which carries the table), but it falls back to
  # born-empty on ANY fork failure and never fails a checkout for it, so this state is reachable in
  # production. Pinned as CHARACTERIZATION, not approval: the valuable half is that the failure is
  # clean — the copy transaction rolls back, so the shard is left untouched rather than half-built.
  #
  # RESOLVED 2026-07-31 — this stays characterization ON PURPOSE. The call (user's): fork-from-
  # template IS the intended birth path, so a born-empty tenant is a FAILED birth, not a state the
  # rollout should quietly heal. Teaching replay to create `django_migrations` would make an
  # already-broken tenant (no schema, first ORM query fails) look recoverable and hide the real
  # fault. So the fix went the other way: `Fathom.Shards.fork_novel/1` no longer swallows the fork
  # outcome — it logs loudly and emits `[:fathom, :migrator, :fork_fallback]`, so a born-empty
  # tenant is alertable instead of silent. See test/fathom/migrator/fork_test.exs.
  test "replay onto a database with no django_migrations fails cleanly and leaves nothing behind",
       %{capture: [v1, _]} do
    :ok = release!(v1)
    source = bare_db!("bare_src")
    dest = bare_db!("bare_dst")

    assert {:error, message} = Copy.migrate(source, dest, v1.version, pairs(v1))
    assert message =~ "django_migrations"

    # Rolled back whole: no half-applied schema, and the version was never stamped.
    assert %{rows: []} =
             query!(dest, "SELECT name FROM sqlite_master WHERE type='table'")

    assert %{rows: [[0]]} = query!(dest, "PRAGMA user_version")
  end

  # The fixture is the capture as it was RECORDED in July, so it still carries Django's schema-editor
  # introspection: `SELECT … sqlite_master`, `SELECT QUOTE(?)…` and `PRAGMA foreign_key_check`.
  # Capture now drops those (`Capture.pure_read?/1`). This is the evidence that doing so is safe —
  # replaying the fixture with them and without them must produce the same database, or the
  # optimization is a schema change wearing a performance change's clothes.
  #
  # It has to run on the REAL capture rather than hand-written SQL for the reason this whole file
  # exists: the reads only appear in what Django actually sends.
  test "dropping Django's introspection leaves the replayed database identical",
       %{capture: [v1, v2]} do
    :ok = release!(v1)
    :ok = release!(v2)

    verbatim = replay_chain!([{v1.version, pairs(v1)}, {v2.version, pairs(v2)}])

    stripped =
      replay_chain!([
        {v1.version, Enum.reject(pairs(v1), &Capture.pure_read?(elem(&1, 0)))},
        {v2.version, Enum.reject(pairs(v2), &Capture.pure_read?(elem(&1, 0)))}
      ])

    # The filter must actually have removed something, or this test passes by doing nothing — the
    # failure mode AGENTS.md calls "a number for work that never happened".
    assert length(pairs(v1)) + length(pairs(v2)) == 29
    assert Enum.count(pairs(v1) ++ pairs(v2), &Capture.pure_read?(elem(&1, 0))) == 4

    assert verbatim == stripped
  end

  # THE TRANSFORM SEAM, DRIVEN BY REAL DJANGO SQL (expert review 2026-08-24 #26).
  #
  # AGENTS.md's migration gate requires a forward copy+transform test, and the two halves existed in
  # separate files that never met. This file is the only one driven by verbatim `manage.py migrate`
  # output — real `?`-parameterized statements whose values ride in args — and it never attached a
  # transform. Every transform test used hand-written SQL with `[]` args, which is exactly the shape
  # AGENTS.md warns hid the NULL-bind bug for the whole life of the engine: replaying the text alone
  # bound NULL against a NOT NULL column and aborted every copy.
  #
  # And `Transform`'s own `@callback` doc mandates idempotency — "a shard whose migration job
  # retried after a transient failure will run this again" — while nothing ran a chain twice against
  # the same destination. So the contract transform authors are told to honour was unverifiable.
  #
  # Both halves here: a transform attached to a step of the REAL chain, validated with
  # `Keystone.dump/1`-style full row equality, then the same chain replayed onto the
  # already-migrated destination to pin that its effect does not change.
  defmodule BackfillTransform do
    @moduledoc false
    @behaviour Fathom.Migrator.Transform

    @impl true
    def run(conn, shard_id) do
      # Idempotent BY CONSTRUCTION — the `WHERE` makes a second run a no-op — which is what the
      # callback contract asks for and what the re-run below actually checks.
      {:ok, _} =
        Fathom.Shard.Connection.query(
          conn,
          "UPDATE django_migrations SET applied = ? WHERE applied IS NULL OR applied = ''",
          ["backfilled-by-#{shard_id}"]
        )

      :ok
    end
  end

  test "a transform runs inside a REAL Django chain, and is idempotent across a replay",
       %{capture: [v1, v2]} do
    :ok = release!(v1)
    :ok = release!(v2)
    prev = Application.get_env(:fathom, :migration_transforms)
    Application.put_env(:fathom, :migration_transforms, [BackfillTransform])
    on_exit(fn -> restore_transforms(prev) end)

    chain = [
      {v1.version, pairs(v1), nil},
      # A STRING, the way a release row stores it — `Transform.resolve/1` deliberately
      # never converts a data column to an atom, so this is the real shape.
      {v2.version, pairs(v2), to_string(BackfillTransform)}
    ]

    source = fresh_django_db!("xform_src")
    dest = fresh_django_db!("xform_dst")

    assert :ok = Copy.migrate_chain(source, dest, chain, shard_id: "xform_tenant")

    # The transform ran, on rows the REAL parameterized INSERT put there — so this covers the
    # args-carrying path, not just the transform.
    rows = query!(dest, "SELECT app, name, applied FROM django_migrations ORDER BY name").rows

    assert rows != [],
           "the real chain inserted no migration rows; the fixture is not being replayed"

    assert Enum.all?(rows, fn [_app, _name, applied] -> applied != nil and applied != "" end),
           "the transform did not run against the real chain's rows"

    after_first = full_dump(dest)

    # THE IDEMPOTENCY HALF: replay the same chain onto the ALREADY-MIGRATED destination, which is
    # what a retried migration job does. The DDL steps will fail on an already-migrated file, so
    # run the transform-carrying step alone — that is the part whose contract is idempotency.
    assert :ok =
             Copy.migrate_chain(dest, dest, [{v2.version, [], to_string(BackfillTransform)}],
               shard_id: "xform_tenant"
             )

    assert full_dump(dest) == after_first,
           "the transform changed the database on a second run. Transform's @callback doc requires " <>
             "idempotency precisely because a retried job runs it again, and nothing checked it."
  end

  defp restore_transforms(nil), do: Application.delete_env(:fathom, :migration_transforms)
  defp restore_transforms(v), do: Application.put_env(:fathom, :migration_transforms, v)

  # Full value equality over every user table, the strongest comparison available here — the same
  # shape `Keystone.dump/1` uses, which until now was applied only to a ZERO-statement copy.
  defp full_dump(path) do
    tables =
      query!(path, "SELECT name FROM sqlite_master WHERE type = 'table' ORDER BY name").rows
      |> Enum.map(&hd/1)
      |> Enum.reject(&String.starts_with?(&1, "sqlite_"))

    for t <- tables, into: %{} do
      {t, query!(path, "SELECT * FROM #{t}").rows}
    end
  end

  # Every object in sqlite_master, both version stamps, and Django's ledger — the whole observable
  # result of a migration chain.
  defp replay_chain!(steps) do
    source = fresh_django_db!("chain_src")
    dest = fresh_django_db!("chain_dst")
    assert :ok = Copy.migrate_chain(source, dest, steps)

    {query!(dest, "SELECT type, name, sql FROM sqlite_master ORDER BY type, name").rows,
     query!(dest, "PRAGMA user_version").rows,
     query!(dest, "SELECT app, name FROM django_migrations ORDER BY name").rows}
  end
end
