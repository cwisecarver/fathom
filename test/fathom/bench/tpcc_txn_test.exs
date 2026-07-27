defmodule Fathom.Bench.TpccTxnTest do
  @moduledoc """
  Exercises the five TPC-C transactions (Phase 3, unit 2) end-to-end over the loopback Hrana-WS
  wire against a seeded shard. Asserts each transaction's write effect via delta invariants — a
  committed New-Order adds exactly one order + one pending new_order, a Payment adds one history
  row, Delivery consumes pending new_orders — which also pins the rollback correctness (a
  rolled-back New-Order must add nothing, so oorder delta == committed count).
  """
  use ExUnit.Case, async: false

  alias Fathom.Bench.{HranaClient, Tpcc}
  alias Fathom.Shards

  @w 1
  @scale 0.001

  setup do
    {:ok, sup, port} = HranaClient.start_listener()
    id = "tpcc_txn_#{System.unique_integer([:positive])}"
    :ok = Tpcc.seed_storage_shard(id, @w, @scale)

    on_exit(fn ->
      Shards.drain(id, 5_000)

      for dir <- [Fathom.Shard.data_dir(), Fathom.Shard.Storage.Local.dir()],
          s <- ["", "-wal", "-shm"] do
        File.rm(Path.join([dir, "#{id}.db"]) <> s)
      end

      HranaClient.stop_listener(sup)
    end)

    {:ok, port: port, id: id}
  end

  test "New-Order: each committed txn adds exactly one order + one pending new_order", ctx_data do
    {:ok, c} = HranaClient.connect(ctx_data.port, ctx_data.id)
    ctx = Tpcc.context(@w, @scale)

    {c, orders_before} = count(c, "oorder")
    {c, pending_before} = count(c, "new_order")

    {c, committed} =
      Enum.reduce(1..20, {c, 0}, fn _, {c, n} ->
        {c, status} = Tpcc.run(:new_order, c, ctx)
        assert status in [:committed, :rolled_back]
        {c, n + if(status == :committed, do: 1, else: 0)}
      end)

    {c, orders_after} = count(c, "oorder")
    {c, pending_after} = count(c, "new_order")
    HranaClient.close(c)

    assert committed > 0, "expected most New-Orders to commit"
    assert orders_after - orders_before == committed, "one oorder per committed New-Order"
    assert pending_after - pending_before == committed, "one new_order per committed New-Order"
  end

  test "Payment: each txn adds one history row and commits", ctx_data do
    {:ok, c} = HranaClient.connect(ctx_data.port, ctx_data.id)
    ctx = Tpcc.context(@w, @scale)

    {c, before} = count(c, "history")

    c =
      Enum.reduce(1..15, c, fn _, c ->
        {c, status} = Tpcc.run(:payment, c, ctx)
        assert status == :committed
        c
      end)

    {c, after_} = count(c, "history")
    HranaClient.close(c)

    assert after_ - before == 15, "one history row per Payment"
  end

  test "Delivery consumes pending new_orders (up to one per district)", ctx_data do
    {:ok, c} = HranaClient.connect(ctx_data.port, ctx_data.id)
    ctx = Tpcc.context(@w, @scale)

    {c, before} = count(c, "new_order")
    {c, status} = Tpcc.run(:delivery, c, ctx)
    {c, after_} = count(c, "new_order")
    HranaClient.close(c)

    assert status == :committed
    consumed = before - after_

    assert consumed > 0 and consumed <= 10,
           "delivery consumes 1..10 pending orders, got #{consumed}"
  end

  test "read-only Order-Status and Stock-Level complete over the wire", ctx_data do
    {:ok, c} = HranaClient.connect(ctx_data.port, ctx_data.id)
    ctx = Tpcc.context(@w, @scale)

    c =
      Enum.reduce(1..10, c, fn _, c ->
        {c, s1} = Tpcc.run(:order_status, c, ctx)
        {c, s2} = Tpcc.run(:stock_level, c, ctx)
        assert s1 == :committed and s2 == :committed
        c
      end)

    HranaClient.close(c)
  end

  test "the weighted deck runs a full mix without error", ctx_data do
    {:ok, c} = HranaClient.connect(ctx_data.port, ctx_data.id)
    ctx = Tpcc.context(@w, @scale)

    c =
      Enum.reduce(1..100, c, fn _, c ->
        {c, status} = Tpcc.run(Tpcc.random_type(), c, ctx)
        assert status in [:committed, :rolled_back, :skipped]
        c
      end)

    HranaClient.close(c)
  end

  defp count(c, table) do
    {:ok, c, %{rows: [[n]]}} = HranaClient.execute(c, "SELECT COUNT(*) FROM #{table}", [])
    {c, n}
  end
end
