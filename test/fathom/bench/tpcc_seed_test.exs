defmodule Fathom.Bench.TpccSeedTest do
  @moduledoc """
  Validates the TPC-C schema + seed (Phase 3, unit 1): the recursive-CTE bulk inserts produce the
  right per-table cardinalities and the referential shape the transactions depend on. Runs the SQL
  directly against a raw `Connection` (no wire) — fast; the full wire path is smoke-tested in U3.
  """
  use ExUnit.Case, async: true

  alias Fathom.Bench.Tpcc
  alias Fathom.Shard.Connection

  @w 2
  @scale 0.001

  setup do
    path = Path.join(System.tmp_dir!(), "tpcc_seed_#{System.unique_integer([:positive])}.db")
    {:ok, conn} = Connection.open(path)

    on_exit(fn ->
      Connection.close(conn)
      Enum.each(["", "-wal", "-shm"], &File.rm(path <> &1))
    end)

    {:ok, conn: conn}
  end

  test "schema + seed produce the expected per-table cardinalities", %{conn: conn} do
    Enum.each(Tpcc.schema_ddl(), &(:ok = Connection.exec(conn, &1)))
    Enum.each(Tpcc.seed_sql(@w, @scale), &(:ok = Connection.exec(conn, &1)))

    %{items: items, per_district: p, new_orders: no, districts: d} = Tpcc.cardinalities(@scale)

    expected = %{
      "item" => items,
      "warehouse" => @w,
      "district" => d * @w,
      "customer" => p * d * @w,
      "history" => p * d * @w,
      "stock" => items * @w,
      "oorder" => p * d * @w,
      "new_order" => no * d * @w,
      "order_line" => 10 * p * d * @w
    }

    for {table, count} <- expected do
      assert row_count(conn, table) == count,
             "#{table}: expected #{count}, got #{row_count(conn, table)}"
    end

    # Referential shape the transactions rely on: every seeded order maps to a real customer
    # (o_c_id ∈ 1..P), and d_next_o_id is the unused next order id (P + 1).
    assert row_count(conn, "oorder WHERE o_c_id > #{p}") == 0
    assert row_count(conn, "oorder WHERE o_c_id < 1") == 0

    {:ok, %{rows: [[next_oid]]}} =
      Connection.query(conn, "SELECT d_next_o_id FROM district WHERE d_w_id = 1 AND d_id = 1", [])

    assert next_oid == p + 1

    # The pending new-orders are exactly the un-delivered tail of orders (carrier NULL).
    assert row_count(conn, "oorder WHERE o_carrier_id IS NULL") == no * d * @w
  end

  defp row_count(conn, table) do
    {:ok, %{rows: [[n]]}} = Connection.query(conn, "SELECT COUNT(*) FROM #{table}", [])
    n
  end
end
