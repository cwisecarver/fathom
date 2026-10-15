defmodule Fathom.Shard.VacuumIntoIndexTest do
  # Expert review 2026-10-15 #11 probe: does `VACUUM INTO`, run through fathom's own SQLite
  # (exqlite via Connection, not python sqlite3), rebuild indexes from table content, or copy a
  # divergent index verbatim?  The result decides whether `quick_check` (flush gate) can ever see
  # index/table divergence and whether `integrity_check` is worth running off the hot path.
  # See the result recorded in AGENTS.md / component-notes and the progress file.
  use ExUnit.Case, async: true

  alias Fathom.Shard.{Connection, Integrity}

  @tag :tmp_dir
  test "integrity_check sees index/table divergence that quick_check cannot; VACUUM INTO result",
       %{
         tmp_dir: dir
       } do
    src = Path.join(dir, "src.db")
    out = Path.join(dir, "out.db")
    {:ok, conn} = Connection.open(src)
    {:ok, _} = Connection.query(conn, "CREATE TABLE t (a INTEGER, b INTEGER)", [])
    {:ok, _} = Connection.query(conn, "CREATE INDEX i ON t (a)", [])
    for n <- 1..50, do: Connection.query(conn, "INSERT INTO t VALUES (?, ?)", [n, 1000 - n])

    # Diverge: redefine the index over b without rebuilding it (index b-tree still holds `a` keys).
    :ok = Connection.exec(conn, "PRAGMA writable_schema=ON")

    {:ok, _} =
      Connection.query(
        conn,
        "UPDATE sqlite_master SET sql='CREATE INDEX i ON t (b)' WHERE name='i'",
        []
      )

    :ok = Connection.exec(conn, "PRAGMA writable_schema=OFF")
    Connection.close(conn)

    {:ok, conn} = Connection.open(src)
    {:ok, ic} = Connection.query(conn, "PRAGMA integrity_check", [])
    refute ic.rows == [["ok"]], "fixture did not create index/table divergence"
    {:ok, _} = Connection.query(conn, "VACUUM INTO ?", [out])
    Connection.close(conn)

    {:ok, conn} = Connection.open(out)
    {:ok, qc} = Connection.query(conn, "PRAGMA quick_check", [])
    {:ok, ic2} = Connection.query(conn, "PRAGMA integrity_check", [])
    Connection.close(conn)

    # RESULT (exqlite's bundled SQLite, 2026-10-15): VACUUM INTO copies the divergent index
    # verbatim (it does NOT rebuild it from table content), quick_check on the copy is blind to it,
    # integrity_check sees it. AGENTS.md's "VACUUM rebuilds indexes" line is wrong for content drift.
    assert qc.rows == [["ok"]]
    refute ic2.rows == [["ok"]], "VACUUM INTO rebuilt the index: finding #11 would be moot"

    # The off-hot-path detector built for it:
    assert :ok = Integrity.verify(out)
    assert {:error, {:integrity_check, [_ | _]}} = Integrity.verify_full(out)
  end
end
