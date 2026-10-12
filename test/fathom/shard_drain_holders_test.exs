defmodule Fathom.ShardDrainHoldersTest do
  @moduledoc """
  A coordinator drain asks its connection holders to let go (expert review 2026-10-10 panel2 H4).

  Symptom: a drain only WAITED for streams to check in. A django-libsql WebSocket, or an HTTP baton
  stream, stays open for as long as the client keeps it, so a migration or node drain of a served
  shard always ran out its window and aborted `:busy`, even with nothing in flight.

  Invariant: a holder that is not inside a transaction is closed by the drain, so the drain
  completes; a holder inside a transaction keeps the window; a holder that is not a Filo transport
  (a migration job, a harness) is never sent a Filo message.

  Expert review 2026-10-12 #34 added: an aborted drain resumes serving, a reconnect during a drain
  is refused and then served, and every scenario runs in both liveness modes (`acquire_gen` nil =
  legacy, non-nil = heartbeat; the suite default is legacy, production is heartbeat).
  """
  use ExUnit.Case, async: false

  alias Fathom.Bench.HranaClient
  alias Fathom.{Shard, ShardExecutor, Shards}
  alias Fathom.Shard.Heartbeat

  @modes [:legacy, :heartbeat]

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

  # Put the NEXT coordinator open into `mode` (captured at open as `acquire_gen`); same pattern as
  # shard_lease_release_test.exs. config/test.exs defaults to legacy (heartbeat_server: false).
  defp set_mode!(:legacy) do
    refute Heartbeat.running?(), "legacy mode needs the heartbeat OFF"
    :ok
  end

  defp set_mode!(:heartbeat) do
    hb = start_supervised!({Heartbeat, ttl_ms: 30_000})
    _ = :sys.get_state(hb)
    assert Heartbeat.running?()
    :ok
  end

  defp coordinator(id) do
    [{pid, _}] = Registry.lookup(Fathom.ShardRegistry, id)
    pid
  end

  # Assert the coordinator opened in the mode asked for: a scenario that silently ran legacy twice
  # looks like two-mode coverage and is one.
  defp assert_mode!(id, :heartbeat) do
    assert is_integer(:sys.get_state(coordinator(id)).acquire_gen), "heartbeat mode did not take"
  end

  defp assert_mode!(id, :legacy) do
    assert is_nil(:sys.get_state(coordinator(id)).acquire_gen), "legacy mode did not take"
  end

  defp wait_until(fun, tries \\ 100) do
    cond do
      fun.() ->
        :ok

      tries == 0 ->
        flunk("condition never held")

      true ->
        Process.sleep(20)
        wait_until(fun, tries - 1)
    end
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

  for mode <- @modes do
    describe "WebSocket holder (#{mode})" do
      setup do: set_mode!(unquote(mode))

      test "an idle WebSocket stream is closed and the drain completes", %{port: port} do
        id = shard("drainws")
        {:ok, c} = HranaClient.connect(port, id)
        {:ok, _c, _} = HranaClient.execute(c, "CREATE TABLE t (v INTEGER)")
        assert_mode!(id, unquote(mode))

        # The socket's process is the holder the coordinator sees: pin that the classifier matches
        # what a real Bandit WebSocket looks like, not just the literal above.
        pid = coordinator(id)
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
        assert_mode!(id, unquote(mode))

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

    describe "HTTP baton stream holder (#{mode})" do
      setup do: set_mode!(unquote(mode))

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
        assert_mode!(id, unquote(mode))

        assert :ok = Shards.drain(id, 5_000)
      end
    end

    describe "non-Filo holder (#{mode})" do
      setup do: set_mode!(unquote(mode))

      test "a holder that is not a Filo transport is not messaged" do
        id = shard("drainother")
        {:ok, conn} = ShardExecutor.open(id, :trusted)
        assert_mode!(id, unquote(mode))

        assert {:error, :busy} = Shards.drain(id, 200)
        refute_received :filo_drain
        refute_received {:"$gen_cast", :drain}

        ShardExecutor.close(conn)
      end
    end

    describe "aborted drain (#{mode})" do
      setup do: set_mode!(unquote(mode))

      # Expert review 2026-10-12 #34: nothing pinned that a drain that runs out its window RESUMES
      # serving. Invariant: draining is reset, a new checkout is granted (not {:error, :draining}),
      # the abort is signalled exactly once, and the in-transaction holder is untouched.
      test "resumes serving and signals the abort once", %{port: port} do
        id = shard("drainabort")
        {:ok, c} = HranaClient.connect(port, id)
        {:ok, c, _} = HranaClient.execute(c, "CREATE TABLE t (v INTEGER)")
        {:ok, c, _} = HranaClient.execute(c, "BEGIN")
        {:ok, c, _} = HranaClient.execute(c, "INSERT INTO t VALUES (1)")

        pid = coordinator(id)
        assert_mode!(id, unquote(mode))

        Shard.request_drain(pid, 200, self())
        assert_receive {:drain_aborted, ^pid}, 5_000
        refute_receive {:drain_aborted, _}, 300

        st = :sys.get_state(pid)
        refute st.draining
        assert st.drain_timer == nil
        assert st.drain_reply_to == nil

        # A fresh checkout is granted, not refused as :draining.
        assert {:ok, conn} = ShardExecutor.open(id, :trusted)
        ShardExecutor.close(conn)

        # The transaction survived the aborted drain.
        assert {:ok, c, _} = HranaClient.execute(c, "COMMIT")
        Mint.HTTP.close(c.conn)
        assert :ok = Shards.drain(id, 5_000)
      end
    end

    describe "reconnect during a drain (#{mode})" do
      setup do: set_mode!(unquote(mode))

      test "an idle socket is closed, a new connect is refused in-window, served after",
           %{port: port} do
        id = shard("drainreconn")
        {:ok, tx} = HranaClient.connect(port, id)
        {:ok, tx, _} = HranaClient.execute(tx, "CREATE TABLE t (v INTEGER)")
        {:ok, tx, _} = HranaClient.execute(tx, "BEGIN")
        {:ok, tx, _} = HranaClient.execute(tx, "INSERT INTO t VALUES (1)")

        {:ok, idle} = HranaClient.connect(port, id)
        {:ok, _idle, _} = HranaClient.execute(idle, "SELECT 1")

        url = "http://127.0.0.1:#{port}/v3/pipeline"

        body = %{
          "baton" => nil,
          "requests" => [%{"type" => "execute", "stmt" => %{"sql" => "SELECT 1"}}]
        }

        pid = coordinator(id)
        assert_mode!(id, unquote(mode))
        ref = Process.monitor(pid)
        Shard.request_drain(pid, 30_000, self())

        # The idle holder is cut loose; the transaction holder keeps the window open.
        wait_until(fn -> map_size(:sys.get_state(pid).conns) == 1 end)
        assert :sys.get_state(pid).draining

        # The idle socket got a close frame, code 1001 (going away). The test process owns both
        # client sockets, so consume it here or it is mistaken for the next reply on `tx`.
        assert_receive {:tcp, _, <<136, _len, 3, 233, _::binary>>}, 2_000
        assert {:ok, tx, _} = HranaClient.execute(tx, "SELECT count(*) FROM t")

        # A reconnect inside the window is refused.
        assert {:error, _} = ShardExecutor.open(id, :trusted)
        resp = Req.post!(url, json: body, headers: [{"host", "#{id}.local"}], retry: false)
        assert resp.status >= 400, "a stream was opened on a draining shard"

        # Committing releases the last holder: the drain completes and the coordinator stops.
        {:ok, tx, _} = HranaClient.execute(tx, "COMMIT")
        Mint.HTTP.close(tx.conn)
        assert_receive {:DOWN, ^ref, :process, ^pid, _}, 5_000

        # After the drain the same shard is served again (cold reopen) with the data intact.
        assert {:ok, again} = HranaClient.connect(port, id)
        assert {:ok, again, %{rows: [[1]]}} = HranaClient.execute(again, "SELECT count(*) FROM t")
        Mint.HTTP.close(again.conn)
        assert :ok = Shards.drain(id, 5_000)
      end
    end
  end
end
