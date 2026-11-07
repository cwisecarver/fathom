defmodule Fathom.BlobTaggingTest do
  @moduledoc """
  Regression for expert review 2026-11-04 #1: a BLOB whose bytes happen to be valid UTF-8 (ASCII,
  JSON, empty) went out as Hrana `text`, because exqlite returns a bare binary for BOTH
  `SQLITE_BLOB` and `SQLITE_TEXT` and Filo had to guess with `String.valid?`.

  The fix is exqlite's `multi_step_tagged/3` (the carried patch in `native/exqlite_patch/`),
  which returns BLOB cells as `{:blob, bin}`; `Connection.query(..., tag_blobs: true)` opts in and
  only the tenant query path (`ShardExecutor`) does. Filo already encodes `{:blob, bin}` as a
  Hrana blob over both JSON and protobuf, so no Filo change is involved.

  Without the patch `Connection.tagged_reads?/0` is false and `tag_blobs: true` degrades to the
  old bare-binary behaviour; the tests assert whichever applies, so an unpatched build (the
  public hex build) stays green and still pins the fallback.
  """
  use ExUnit.Case, async: true

  alias Fathom.Shard.Connection
  alias Filo.{Protobuf, StmtResult}

  @patched? Connection.tagged_reads?()

  setup do
    # `System.unique_integer/1` restarts at 1 in every VM, and this file used to be left behind: 125
    # stale `blobtag-N.db` accumulated in the tmp dir and a later run whose counter reached the same
    # N opened a database that already had `b` (`{:error, "table b already exists"}` in setup, seed
    # 292350). The name now carries the wall clock too and every file this test creates is removed.
    path =
      Path.join(
        System.tmp_dir!(),
        "blobtag-#{System.os_time(:nanosecond)}-#{System.unique_integer([:positive])}.db"
      )

    on_exit(fn -> for s <- ["", "-wal", "-shm"], do: File.rm(path <> s) end)
    {:ok, conn} = Connection.open(path)
    on_exit(fn -> Connection.close(conn) end)

    :ok = Connection.exec(conn, "CREATE TABLE b (id INTEGER PRIMARY KEY, v)")

    for {id, v} <- [
          {1, {:blob, "abc"}},
          {2, {:blob, ""}},
          {3, "abc"},
          {4, {:blob, <<0xFF, 0xFE>>}}
        ] do
      {:ok, _} = Connection.query(conn, "INSERT INTO b VALUES (?, ?)", [id, v])
    end

    # Invalid UTF-8 stored as TEXT (SQLite allows it): must stay a bare binary.
    {:ok, _} = Connection.query(conn, "INSERT INTO b VALUES (5, CAST(x'fffe' AS TEXT))", [])
    %{conn: conn}
  end

  defp read(conn, opts) do
    {:ok, %{rows: rows}} = Connection.query(conn, "SELECT v FROM b ORDER BY id", [], opts)
    Enum.map(rows, fn [v] -> v end)
  end

  test "without tag_blobs every cell is a bare binary (migration copy, snapshots, Capture)", %{
    conn: conn
  } do
    assert read(conn, []) == ["abc", "", "abc", <<0xFF, 0xFE>>, <<0xFF, 0xFE>>]
  end

  test "tag_blobs: BLOB cells are tagged, TEXT stays bare, empty blob is a blob", %{conn: conn} do
    got = read(conn, tag_blobs: true)

    if @patched? do
      assert got == [{:blob, "abc"}, {:blob, ""}, "abc", {:blob, <<0xFF, 0xFE>>}, <<0xFF, 0xFE>>]
    else
      assert got == ["abc", "", "abc", <<0xFF, 0xFE>>, <<0xFF, 0xFE>>]
    end
  end

  test "golden wire: a tagged BLOB \"abc\" is Hrana blob over JSON and protobuf; TEXT stays text",
       %{conn: conn} do
    rows = Enum.map(read(conn, tag_blobs: true), &[&1])

    json = rows |> StmtResult.rows_iodata() |> IO.iodata_to_binary() |> Jason.decode!()
    [blob_abc, blob_empty, text_abc, blob_ff, text_ff] = Enum.map(json, fn [cell] -> cell end)

    if @patched? do
      assert blob_abc == %{"type" => "blob", "base64" => "YWJj"}
      assert blob_empty == %{"type" => "blob", "base64" => ""}
    end

    # TEXT "abc" is text on every build.
    assert text_abc == %{"type" => "text", "value" => "abc"}
    # Invalid-UTF-8 cells, TEXT or BLOB, are not raised on and go out as blob (as before).
    assert blob_ff == %{"type" => "blob", "base64" => "//4"}
    assert text_ff == %{"type" => "blob", "base64" => "//4"}

    # Protobuf: field 5 (blob) vs 4 (text) of the Hrana Value message.
    if @patched? do
      [first | _] = rows
      pb = IO.iodata_to_binary(Protobuf.encode_native_rows([first]))
      assert pb =~ <<0x2A, 3, "abc">>, "blob is protobuf Value field 5"
      refute pb =~ <<0x22, 3, "abc">>
    end

    # Plain TEXT is Value field 4.
    assert IO.iodata_to_binary(Protobuf.encode_native_rows([["abc"]])) =~ <<0x22, 3, "abc">>
  end
end
