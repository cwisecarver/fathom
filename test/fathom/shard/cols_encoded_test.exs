defmodule Fathom.Shard.ColsEncodedTest do
  @moduledoc """
  The pre-built column wire form cached beside each prepared statement (expert review 2026-10-19
  #10; needs filo's `Filo.StmtResult.cols_encoded`).

  Not async: the stale-cache test drives the node-global `Fathom.Shard.SchemaGen`, like
  `schema_gen_test.exs`.

  Invariant: whatever the cache holds, the bytes on the wire (JSON and protobuf) equal what filo
  produces from `cols` alone. A stale `cols_encoded` after another stream's DDL would put the OLD
  column list on the wire over NEW-width rows, the 2026-08-26 #7 mis-mapping, through the one path
  that no longer reads `cols`.
  """
  use ExUnit.Case, async: false

  alias Fathom.ShardExecutor
  alias Filo.{Protobuf, Stmt, StmtResult}

  defp wire(%StmtResult{} = r) do
    json = r |> StmtResult.encode(rows: :json) |> Jason.encode!()

    pb =
      r
      |> StmtResult.encode(rows: :protobuf)
      |> Protobuf.encode_stmt_result()
      |> IO.iodata_to_binary()

    {json, pb}
  end

  defp run(conn, sql), do: ShardExecutor.execute(conn, %Stmt{sql: sql, args: []})

  defp open_pair do
    shard = "colsenc_#{System.unique_integer([:positive])}"

    on_exit(fn ->
      for base <- [
            Path.join([Fathom.Shard.data_dir(), "#{shard}.db"]),
            Path.join([Fathom.Shard.Storage.Local.dir(), "#{shard}.db"])
          ],
          suffix <- ["", "-wal", "-shm"],
          do: File.rm(base <> suffix)
    end)

    {:ok, a} = ShardExecutor.open(shard)
    {:ok, b} = ShardExecutor.open(shard)

    on_exit(fn ->
      ShardExecutor.close(a)
      ShardExecutor.close(b)
    end)

    {a, b}
  end

  test "cols_encoded is set and wire bytes equal the cols-only encoding for 0/1/8 columns" do
    {a, _b} = open_pair()

    {:ok, _} =
      run(
        a,
        "CREATE TABLE w (c1 INTEGER PRIMARY KEY, c2 TEXT, c3 TEXT, c4 TEXT, c5 TEXT, c6 TEXT, c7 TEXT, c8 TEXT)"
      )

    {:ok, _} = run(a, "INSERT INTO w VALUES (1,'a','b','c','d','e','f','g')")

    for {sql, ncols} <- [
          {"SELECT c1 FROM w", 1},
          {"SELECT * FROM w", 8},
          {"SELECT * FROM w WHERE 0", 8},
          {"INSERT INTO w(c1) VALUES (2)", 0}
        ] do
      assert {:ok, %StmtResult{cols_encoded: enc} = r} = run(a, sql)
      assert %{json: _, protobuf: _} = enc, "no cols_encoded for #{sql}"
      assert length(r.cols) == ncols
      assert wire(r) == wire(%{r | cols_encoded: nil}), "wire bytes differ for #{sql}"
    end
  end

  # Symptom: after a SIBLING stream's ALTER TABLE ADD COLUMN, the re-executed cached SELECT * must
  # put the NEW columns on the wire in JSON and protobuf, via the post-step refresh path.
  test "a cached statement's wire columns follow another stream's ADD COLUMN" do
    {a, b} = open_pair()
    {:ok, _} = run(a, "CREATE TABLE t (id INTEGER PRIMARY KEY, x TEXT)")
    {:ok, _} = run(a, "INSERT INTO t VALUES (1, 'hello')")

    assert {:ok, %StmtResult{cols: [_, _]} = first} = run(b, "SELECT * FROM t")
    assert wire(first) == wire(%{first | cols_encoded: nil})

    {:ok, _} = run(a, "ALTER TABLE t ADD COLUMN y TEXT DEFAULT ('0')")

    assert {:ok, %StmtResult{rows: [[1, "hello", "0"]]} = second} = run(b, "SELECT * FROM t")
    {json, pb} = wire(second)
    assert json =~ ~s("name":"y"), "JSON wire still carries the pre-ALTER column list"
    assert pb =~ "y", "protobuf wire still carries the pre-ALTER column list"
    assert {json, pb} == wire(%{second | cols_encoded: nil})

    # The refreshed entry is what the next (ordinary hit) execution splices.
    assert {:ok, %StmtResult{} = third} = run(b, "SELECT * FROM t")
    assert wire(third) == wire(%{third | cols_encoded: nil})
    assert third.cols_encoded == second.cols_encoded
  end
end
