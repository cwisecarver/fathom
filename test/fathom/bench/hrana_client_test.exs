defmodule Fathom.Bench.HranaClientTest do
  @moduledoc """
  Proves the Phase-1 wire loopback: a real WebSocket client (Mint.WebSocket) driving the
  full Filo.Socket path — WS framing, hello, request/response, client stream — through to a
  real shard and back. This is the harness the wire benches (tpcb_wire_overhead_us,
  hrana_rt_us, cold_open_wire_p50_us) sit on top of.
  """
  use ExUnit.Case, async: false

  alias Fathom.Bench.HranaClient

  defp rm_shard(id) do
    for s <- ["", "-wal", "-shm"] do
      File.rm(Path.join([Fathom.Shard.data_dir(), "#{id}.db"]) <> s)
    end
  end

  setup do
    {:ok, sup, port} = HranaClient.start_listener()
    on_exit(fn -> HranaClient.stop_listener(sup) end)
    %{port: port}
  end

  test "a query round-trips through the full Hrana WS wire to a shard", %{port: port} do
    shard = "wire_#{System.unique_integer([:positive])}"
    on_exit(fn -> rm_shard(shard) end)

    {:ok, c} = HranaClient.connect(port, shard)

    {:ok, c, _} = HranaClient.execute(c, "CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT)")
    {:ok, c, ins} = HranaClient.execute(c, "INSERT INTO t (id, v) VALUES (?, ?)", [1, "hello"])
    assert ins.affected_row_count == 1

    {:ok, c, sel} = HranaClient.execute(c, "SELECT id, v FROM t ORDER BY id")
    assert sel.cols == ["id", "v"]
    assert sel.rows == [[1, "hello"]]

    # A bound-arg scalar round-trips too (Hrana value encode/decode).
    {:ok, c, one} = HranaClient.execute(c, "SELECT ?", [42])
    assert one.rows == [[42]]

    assert :ok = HranaClient.close(c)
  end

  test "isolation holds over the wire: Host routing sends each shard to its own file", %{
    port: port
  } do
    a = "wirea_#{System.unique_integer([:positive])}"
    b = "wireb_#{System.unique_integer([:positive])}"
    on_exit(fn -> Enum.each([a, b], &rm_shard/1) end)

    {:ok, ca} = HranaClient.connect(port, a)
    {:ok, cb} = HranaClient.connect(port, b)

    {:ok, ca, _} = HranaClient.execute(ca, "CREATE TABLE kv (v TEXT)")
    {:ok, ca, _} = HranaClient.execute(ca, "INSERT INTO kv VALUES ('a-secret')")

    {:ok, cb, _} = HranaClient.execute(cb, "CREATE TABLE kv (v TEXT)")
    {:ok, cb, sel} = HranaClient.execute(cb, "SELECT count(*) FROM kv")
    # b sees only its own (empty) table — never a's row. If Host routing collapsed both to
    # one shard, this would be 1.
    assert sel.rows == [[0]]

    HranaClient.close(ca)
    HranaClient.close(cb)
  end
end
