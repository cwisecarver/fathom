defmodule Fathom.ShardDrainHoldersTest do
  @moduledoc """
  A coordinator drain asks its connection holders to let go (expert review 2026-10-10 panel2 H4).

  Symptom: a drain only WAITED for streams to check in. A django-libsql WebSocket, or an HTTP baton
  stream, stays open for as long as the client keeps it, so a migration or node drain of a served
  shard always ran out its window and aborted `:busy`, even with nothing in flight.

  Invariant: a holder that is not inside a transaction is closed by the drain, so the drain
  completes; a holder inside a transaction keeps the window; a holder that is not a Filo transport
  (a migration job, a harness) is never sent a Filo message.
  """
  use ExUnit.Case, async: false

  alias Fathom.Bench.HranaClient
  alias Fathom.{ShardExecutor, Shards}

  defp rm_shard(id) do
    for s <- ["", "-wal", "-shm"] do
      File.rm(Path.join([Fathom.Shard.data_dir(), "#{id}.db"]) <> s)
    end
  end

  defp shard(prefix) do
    id = "#{prefix}_#{System.unique_integer([:positive])}"
    on_exit(fn -> rm_shard(id) end)
    id
  end

  setup do
    {:ok, sup, port} = HranaClient.start_listener(streams_name: __MODULE__.Streams)
    on_exit(fn -> HranaClient.stop_listener(sup) end)
    %{port: port}
  end

  describe "holder_kind/1" do
    test "classifies the Filo transports and leaves everything else alone" do
      assert ShardExecutor.holder_kind({Filo.Stream, :init, 1}) == :filo_stream
      assert ShardExecutor.holder_kind({Bandit.DelegatingHandler, :init, 1}) == :bandit_connection
      assert ShardExecutor.holder_kind({Bandit.InitialHandler, :init, 1}) == :bandit_connection
      assert ShardExecutor.holder_kind({Oban.Queue.Executor, :init, 1}) == :other
      assert ShardExecutor.holder_kind({:proc_lib, :init_p, 5}) == :other
    end
  end

  describe "WebSocket holder" do
    test "an idle WebSocket stream is closed and the drain completes", %{port: port} do
      id = shard("drainws")
      {:ok, c} = HranaClient.connect(port, id)
      {:ok, _c, _} = HranaClient.execute(c, "CREATE TABLE t (v INTEGER)")

      # The socket's process is the holder the coordinator sees: pin that the classifier matches
      # what a real Bandit WebSocket looks like, not just the literal above.
      [{pid, _}] = Registry.lookup(Fathom.ShardRegistry, id)
      holders = for {_ref, {caller, _op}} <- :sys.get_state(pid).conns, do: caller
      assert [holder] = Enum.uniq(holders)

      assert ShardExecutor.holder_kind(:proc_lib.translate_initial_call(holder)) ==
               :bandit_connection

      # Pre-fix this waited the whole window and returned {:error, :busy}.
      assert :ok = Shards.drain(id, 5_000)
    end

    test "a WebSocket inside a transaction keeps the window and its writes", %{port: port} do
      id = shard("drainwstx")
      {:ok, c} = HranaClient.connect(port, id)
      {:ok, c, _} = HranaClient.execute(c, "CREATE TABLE t (v INTEGER)")
      {:ok, c, _} = HranaClient.execute(c, "BEGIN")
      {:ok, c, _} = HranaClient.execute(c, "INSERT INTO t VALUES (1)")

      assert {:error, :busy} = Shards.drain(id, 300)

      # The transaction was not cut: it still commits on the same stream.
      assert {:ok, c, _} = HranaClient.execute(c, "COMMIT")
      assert :ok = Shards.drain(id, 5_000)
      Mint.HTTP.close(c.conn)

      {:ok, conn} = ShardExecutor.open(id, :trusted)

      assert {:ok, %Filo.StmtResult{rows: [[1]]}} =
               ShardExecutor.execute(conn, %Filo.Stmt{sql: "SELECT count(*) FROM t"})

      ShardExecutor.close(conn)
    end
  end

  describe "HTTP baton stream holder" do
    test "an idle baton stream is closed and the drain completes", %{port: port} do
      id = shard("drainhttp")
      url = "http://127.0.0.1:#{port}/v3/pipeline"

      body = %{
        "baton" => nil,
        "requests" => [%{"type" => "execute", "stmt" => %{"sql" => "SELECT 1"}}]
      }

      resp = Req.post!(url, json: body, headers: [{"host", "#{id}.local"}], retry: false)
      assert resp.status == 200
      assert is_binary(resp.body["baton"]), "the stream was not kept open, so nothing is held"

      assert :ok = Shards.drain(id, 5_000)
    end
  end

  describe "non-Filo holder" do
    test "a holder that is not a Filo transport is not messaged" do
      id = shard("drainother")
      {:ok, conn} = ShardExecutor.open(id, :trusted)

      assert {:error, :busy} = Shards.drain(id, 200)
      refute_received :filo_drain
      refute_received {:"$gen_cast", :drain}

      ShardExecutor.close(conn)
    end
  end
end
