defmodule Fathom.Shard.ConnectionLimitsTest do
  @moduledoc """
  SQLite size limits on tenant handles (expert review 2026-10-10 #2).

  Symptom: no `sqlite3_limit` anywhere, so `SQLITE_LIMIT_LENGTH` was 1e9 and
  `SELECT length(randomblob(999999999))` allocated ~917 MB on the node from any token (`:ro`
  included) — the deadline bounds time, not memory. Invariant: a TENANT handle caps result/blob
  length at 64 MiB (`SQLITE_TOOBIG` beyond it) while INTERNAL handles (coordinator, migrator,
  `VACUUM INTO`) keep SQLite's defaults, so fathom's own copy/snapshot of a large row still works.

  Also covers expert review 2026-10-10 #15: under `block_user_version?` the engine PRAGMA
  authorizer refuses `user_version` assignment however the statement is spelled.
  """
  use ExUnit.Case, async: false

  alias Fathom.Shard.Connection
  alias Fathom.Shard.Extension

  setup do
    assert Extension.available?(), "the fathom_udf extension is not built"

    dir = Path.join(System.tmp_dir!(), "fathom_limits_#{System.unique_integer([:positive])}")
    File.mkdir_p!(dir)
    on_exit(fn -> File.rm_rf!(dir) end)
    %{path: Path.join(dir, "shard.db")}
  end

  defp open!(path, opts) do
    {:ok, conn} = Connection.open(path, opts)
    on_exit(fn -> Connection.close(conn) end)
    conn
  end

  # 100 MB: above the 64 MiB tenant cap, far below the 1e9 default.
  @big "SELECT length(zeroblob(100000000))"

  for scope <- [:rw, :ro] do
    test "a tenant handle refuses a 100 MB value (#{scope})", %{path: path} do
      # materialize the file via an internal handle first
      _ = open!(path, [])
      conn = open!(path, tenant?: true, scope: unquote(scope))
      assert {:error, reason} = Connection.query(conn, @big, [])
      assert inspect(reason) =~ ~r/too big/i
    end
  end

  test "a tenant handle still serves a value under the cap", %{path: path} do
    conn = open!(path, tenant?: true)

    assert {:ok, %{rows: [[1_000_000]]}} =
             Connection.query(conn, "SELECT length(zeroblob(1000000))", [])
  end

  test "a tenant handle caps SQL text length", %{path: path} do
    conn = open!(path, tenant?: true)
    sql = "SELECT 1 /*" <> String.duplicate("x", 17 * 1024 * 1024) <> "*/"
    assert {:error, reason} = Connection.query(conn, sql, [])
    assert inspect(reason) =~ ~r/too big/i
  end

  # EXPR_DEPTH stays at SQLite's default: Django's left-associative OR chains are deep (the
  # follow-up to expert review 2026-10-10 #2).
  test "a 600-term OR chain still runs on a tenant handle", %{path: path} do
    conn = open!(path, tenant?: true)
    terms = Enum.map_join(1..600, " OR ", &"1 = #{&1}")
    assert {:ok, %{rows: [[1]]}} = Connection.query(conn, "SELECT #{terms}", [])
  end

  test "an internal handle keeps the default limits", %{path: path} do
    conn = open!(path, [])
    assert {:ok, %{rows: [[100_000_000]]}} = Connection.query(conn, @big, [])
  end

  test "a tenant cannot lift the cap by calling the guard again", %{path: path} do
    conn = open!(path, tenant?: true)
    # DIRECTONLY + set-once: either an error or a 0, never a raised limit.
    _ = Connection.query(conn, "SELECT fathom_pragma_guard('', '')", [])
    assert {:error, _} = Connection.query(conn, @big, [])
  end

  describe "block_user_version? (expert review 2026-10-10 #15)" do
    test "the engine refuses user_version assignment, in any spelling", %{path: path} do
      conn = open!(path, tenant?: true, block_user_version?: true)

      for sql <- [
            "PRAGMA user_version=7",
            "PRAGMA main.user_version = 7",
            "/* c */ PRAGMA user_version=7",
            "EXPLAIN PRAGMA user_version=7"
          ] do
        assert {:error, reason} = Connection.query(conn, sql, [])
        assert inspect(reason) =~ "not authorized", sql
      end

      assert {:ok, 0} = Connection.pragma(conn, "user_version")
    end

    # The wiring: ShardExecutor.open under :block_tenant_ddl hands the engine guard the flag for a
    # non-template shard, so a statement that evades the text gate still dies in SQLite. The
    # handle's own conn is queried directly to skip the (parser) text gate.
    test "ShardExecutor.open sets it from :block_tenant_ddl for a non-template shard" do
      prev = Application.get_env(:fathom, :block_tenant_ddl, false)
      Application.put_env(:fathom, :block_tenant_ddl, true)
      id = "test_uvlimit_#{System.unique_integer([:positive])}"
      {:ok, h} = Fathom.ShardExecutor.open(id)

      on_exit(fn ->
        Fathom.ShardExecutor.close(h)
        Application.put_env(:fathom, :block_tenant_ddl, prev)

        for base <- [
              Path.join(Fathom.Shard.data_dir(), "#{id}.db"),
              Path.join(Fathom.Shard.Storage.Local.dir(), "#{id}.db")
            ],
            suffix <- ["", "-wal", "-shm"],
            do: File.rm(base <> suffix)
      end)

      conn = elem(h, 2)
      assert {:error, reason} = Connection.query(conn, "PRAGMA user_version=5", [])
      assert inspect(reason) =~ "not authorized"
    end

    test "without the flag (template / ddl not blocked) user_version stays assignable", %{
      path: path
    } do
      conn = open!(path, tenant?: true)
      assert {:ok, _} = Connection.query(conn, "PRAGMA user_version=7", [])
      assert {:ok, 7} = Connection.pragma(conn, "user_version")
    end
  end
end
