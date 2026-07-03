defmodule Fathom.TemplateCaptureTest do
  # End-to-end: a Django-style migrate driven through Fathom.ShardExecutor against
  # the reserved template shard is auto-captured as a fleet version. Not async
  # (shared sandbox so the Capture process can write; shards are global).
  use Fathom.DataCase, async: false

  alias Fathom.{Migrator, ShardExecutor}
  alias Filo.Stmt

  @local_dir Path.join(System.tmp_dir!(), "fathom_shards")
  @remote_dir Path.join(System.tmp_dir!(), "fathom_remote_test")

  setup do
    shard = "template_#{System.unique_integer([:positive])}"
    prev = Application.get_env(:fathom, :template_shard_id)
    Application.put_env(:fathom, :template_shard_id, shard)

    on_exit(fn ->
      if prev,
        do: Application.put_env(:fathom, :template_shard_id, prev),
        else: Application.delete_env(:fathom, :template_shard_id)

      for dir <- [@local_dir, @remote_dir],
          suffix <- [".db", ".db-wal", ".db-shm", ".lock"],
          do: File.rm(Path.join(dir, shard <> suffix))
    end)

    %{shard: shard}
  end

  defp exec!(conn, sql) do
    assert {:ok, _} = ShardExecutor.execute(conn, %Stmt{sql: sql, args: []})
  end

  test "a Django migrate against the template shard is captured as a version", %{shard: shard} do
    # Assert the DELTA (one new version), not an absolute head: the global Capture
    # GenServer's release write isn't always rolled back cleanly across the
    # shared-sandbox boundary between these two tests, so an absolute head==1 is
    # flaky. The invariant we actually mean is "a template migrate adds exactly one
    # version."
    before = Migrator.head()
    {:ok, conn} = ShardExecutor.open(shard)

    create_table = "CREATE TABLE app_thing (id INTEGER PRIMARY KEY, name TEXT)"

    create_django =
      "CREATE TABLE django_migrations (id INTEGER PRIMARY KEY, app TEXT, name TEXT, applied TEXT)"

    insert_migration =
      "INSERT INTO django_migrations (app, name, applied) VALUES ('app', '0001_initial', 'now')"

    # The statement stream Django emits for one atomic migration.
    exec!(conn, "BEGIN")
    exec!(conn, create_table)
    exec!(conn, create_django)
    exec!(conn, insert_migration)
    exec!(conn, "COMMIT")

    version = before + 1
    assert Migrator.head() == version
    assert Migrator.statements(version) == [create_table, create_django, insert_migration]

    ShardExecutor.close(conn)
  end

  test "a non-template shard's statements are not captured", %{shard: template} do
    other = "tenant_#{System.unique_integer([:positive])}"

    # Clean BOTH the local working dir and the remote store. `other` is
    # `System.unique_integer`-derived, which recycles across VM boots, so a leftover
    # local `<other>.db` from a prior run would be treated as an authoritative warm
    # restart (present local copy wins) and its existing `django_migrations` table
    # would fail this test's CREATE with "table already exists". The template on_exit
    # already cleans both dirs; this one previously cleaned only @remote_dir.
    on_exit(fn ->
      for dir <- [@local_dir, @remote_dir],
          s <- [".db", ".db-wal", ".db-shm", ".lock"],
          do: File.rm(Path.join(dir, other <> s))
    end)

    _ = template

    # The invariant: a non-template shard captures NOTHING — head is unchanged by its
    # statements (delta 0), regardless of any prior test's leaked release.
    before = Migrator.head()
    {:ok, conn} = ShardExecutor.open(other)

    exec!(conn, "BEGIN")
    exec!(conn, "CREATE TABLE django_migrations (id INTEGER PRIMARY KEY)")
    exec!(conn, "INSERT INTO django_migrations (id) VALUES (1)")
    exec!(conn, "COMMIT")

    assert Migrator.head() == before

    ShardExecutor.close(conn)
  end
end
