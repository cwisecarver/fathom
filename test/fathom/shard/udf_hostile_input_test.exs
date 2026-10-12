defmodule Fathom.Shard.UdfHostileInputTest do
  @moduledoc """
  The fathom_udf extension must turn hostile tenant input into a SQL error, never a node abort
  (expert review 2026-10-12 #1, #2).

  Symptom: `SELECT django_format_dtdelta('+','2026-01-01 00:00:00',9223372036854775807)` panicked
  inside chrono and `panic = "abort"` turned it into a SIGABRT of the whole BEAM; `REPEAT('x',
  4000000000)` asked the allocator for 4 GB before SQLite's `SQLITE_LIMIT_LENGTH` (which checks
  only the RETURNED value) could apply. Invariant: each returns an error from the statement, and
  the connection keeps working afterwards. (Pre-fix these tests don't fail cleanly: the VM dies.
  The per-function Rust table is `native/fathom_udf/tests/fuzz.rs`; this file pins the SQL wiring,
  including that the length cap is read from the handle.)
  """
  use ExUnit.Case, async: false

  alias Fathom.Shard.Connection
  alias Fathom.Shard.Extension

  setup do
    assert Extension.available?(), "the fathom_udf extension is not built"

    dir = Path.join(System.tmp_dir!(), "fathom_udf_hostile_#{System.unique_integer([:positive])}")
    File.mkdir_p!(dir)
    on_exit(fn -> File.rm_rf!(dir) end)

    {:ok, conn} = Connection.open(Path.join(dir, "shard.db"), tenant?: true)
    on_exit(fn -> Connection.close(conn) end)
    %{conn: conn}
  end

  defp assert_errors(conn, sql) do
    assert {:error, _} = Connection.query(conn, sql, []), sql
    # The handle survives: the statement failed, the node did not.
    assert {:ok, %{rows: [[1]]}} = Connection.query(conn, "SELECT 1", [])
  end

  test "date arithmetic that overflows is a SQL error, not an abort", %{conn: conn} do
    for sql <- [
          "SELECT django_format_dtdelta('+','2026-01-01 00:00:00',9223372036854775807)",
          "SELECT django_format_dtdelta('-','2026-01-01 00:00:00',-9223372036854775808)",
          "SELECT django_format_dtdelta('+',9223372036854775807,'2026-01-01 00:00:00')",
          "SELECT django_format_dtdelta('+','2026-01-01',9223372036854775807)",
          "SELECT django_format_dtdelta('-','2026-01-01',-9223372036854775808)",
          "SELECT django_format_dtdelta('+','9999-12-31 23:59:59',86400000000)",
          "SELECT django_format_dtdelta('-','0001-01-01',86400000000)"
        ] do
      assert_errors(conn, sql)
    end
  end

  test "date arithmetic inside the range still works", %{conn: conn} do
    assert {:ok, %{rows: [["2026-01-02 00:00:00"]]}} =
             Connection.query(
               conn,
               "SELECT django_format_dtdelta('+','2026-01-01 00:00:00',86400000000)",
               []
             )
  end

  test "REPEAT / LPAD / RPAD refuse an oversize result before allocating", %{conn: conn} do
    for sql <- [
          "SELECT REPEAT('abcdefghij', 4000000000)",
          "SELECT REPEAT('x', 9223372036854775807)",
          "SELECT LPAD('x', 9223372036854775807, 'ab')",
          "SELECT RPAD('x', 9223372036854775807, 'ab')",
          # just over the 64 MiB tenant cap
          "SELECT REPEAT('x', 67108865)",
          "SELECT LPAD('', 67108865, 'x')"
        ] do
      assert_errors(conn, sql)
    end

    assert {:error, reason} = Connection.query(conn, "SELECT REPEAT('x', 67108865)", [])
    assert inspect(reason) =~ ~r/too big/i
  end

  test "REPEAT / LPAD / RPAD at normal sizes still work, and a pad builds only what it needs",
       %{conn: conn} do
    assert {:ok, %{rows: [["xxx", "00x", "x00", "abax"]]}} =
             Connection.query(
               conn,
               "SELECT REPEAT('x', 3), LPAD('x', 3, '0'), RPAD('x', 3, '0'), LPAD('x', 4, 'ab')",
               []
             )

    assert {:ok, %{rows: [[1_000_000]]}} =
             Connection.query(conn, "SELECT length(REPEAT('x', 1000000))", [])
  end
end
