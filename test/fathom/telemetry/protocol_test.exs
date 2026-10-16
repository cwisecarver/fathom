defmodule Fathom.Telemetry.ProtocolTest do
  # Not async: the handler and its counters are node-global, and the S3 config is Application env.
  use ExUnit.Case, async: false

  alias Fathom.Shard.Storage.S3
  alias Fathom.Telemetry.Protocol

  setup do
    Protocol.reset()
    Protocol.attach()
    on_exit(fn -> Protocol.detach() end)
    :ok
  end

  defp ms(n), do: System.convert_time_unit(n, :millisecond, :native)

  defp request_stop(plug, status, dur_ms) do
    :telemetry.execute(
      [:bandit, :request, :stop],
      %{duration: ms(dur_ms)},
      %{plug: plug, conn: %{status: status}}
    )
  end

  # Lifetime counts over a 1 s window: a nil baseline reports zeros by design, so diff against an
  # explicit all-zero snapshot instead.
  defp stats do
    {_, snap} = Protocol.window(nil, 1.0)
    {s, _} = Protocol.window(List.duplicate(0, length(snap)), 1.0)
    s
  end

  test "Hrana HTTP requests are counted by status class with a p99" do
    for _ <- 1..98, do: request_stop({Filo.Plug, []}, 200, 3)
    request_stop({Filo.Plug, []}, 404, 3)
    request_stop({Filo.Plug, []}, 500, 900)

    s = stats()
    assert s.http.rate == %{"2xx" => 98.0, "4xx" => 1.0, "5xx" => 1.0}
    # 100 samples: the 99th is <= 5 ms, so p99 is the 5 ms bucket bound
    assert s.http.p99_ms == 5.0
  end

  test "the :4000 Phoenix endpoint and a WS upgrade are NOT counted as Hrana HTTP" do
    request_stop({FathomWeb.Endpoint, []}, 200, 1)
    request_stop({Filo.Plug, []}, 101, 1)

    :telemetry.execute([:bandit, :websocket, :start], %{}, %{websock: Phoenix.LiveView.Socket})

    :telemetry.execute(
      [:bandit, :websocket, :stop],
      %{recv_text_frame_count: 9},
      %{websock: Phoenix.LiveView.Socket}
    )

    s = stats()
    assert s.http.rate == %{"2xx" => 0.0, "4xx" => 0.0, "5xx" => 0.0}
    assert s.ws == %{opens_per_s: 0.0, closes_per_s: 0.0, errors_per_s: 0.0, frames_per_s: 0.0}
  end

  test "Filo WebSocket connections: opens, closes, error closes, received frames" do
    :telemetry.execute([:bandit, :websocket, :start], %{}, %{websock: Filo.Socket})
    :telemetry.execute([:bandit, :websocket, :start], %{}, %{websock: Filo.Socket})

    :telemetry.execute(
      [:bandit, :websocket, :stop],
      %{recv_text_frame_count: 5, recv_binary_frame_count: 2},
      %{websock: Filo.Socket}
    )

    :telemetry.execute(
      [:bandit, :websocket, :stop],
      %{recv_text_frame_count: 1},
      %{websock: Filo.Socket, error: "boom"}
    )

    assert stats().ws == %{
             opens_per_s: 2.0,
             closes_per_s: 2.0,
             errors_per_s: 1.0,
             frames_per_s: 8.0
           }
  end

  test "a request that ended in error with no conn counts as 5xx; empty window is zeros" do
    assert stats().http.p99_ms == 0.0
    :telemetry.execute([:bandit, :request, :stop], %{duration: ms(1)}, %{plug: {Filo.Plug, []}})
    assert stats().http.rate["5xx"] == 1.0
  end

  test "window/2 reports a delta, and the first tick (nil baseline) reports zeros" do
    request_stop({Filo.Plug, []}, 200, 1)
    {s0, snap} = Protocol.window(nil, 1.0)
    assert s0.http.rate["2xx"] == 0.0
    request_stop({Filo.Plug, []}, 200, 1)
    request_stop({Filo.Plug, []}, 200, 1)
    {s1, _} = Protocol.window(snap, 2.0)
    assert s1.http.rate["2xx"] == 1.0
  end

  test "S3 events: errors/s counts 5xx + transport errors, p99 from duration" do
    for c <- ["2xx", "5xx", "error"] do
      :telemetry.execute([:fathom, :s3, :op], %{count: 1, bytes: 0, duration: ms(40)}, %{
        op: :get,
        status_class: c
      })
    end

    s = stats()
    assert s.s3.errors_per_s == 2.0
    assert s.s3.p99_ms == 50.0
  end

  describe "S3 Req steps" do
    setup do
      old = {Application.get_env(:fathom, :shard_storage), Application.get_env(:fathom, S3)}

      on_exit(fn ->
        # restore OR delete: leaving our S3 stub config behind sends later tests to a dead endpoint
        for {key, val} <- [{:shard_storage, elem(old, 0)}, {S3, elem(old, 1)}] do
          if val,
            do: Application.put_env(:fathom, key, val),
            else: Application.delete_env(:fathom, key)
        end
      end)
    end

    defp use_s3(plug) do
      Application.put_env(:fathom, :shard_storage, S3)

      Application.put_env(:fathom, S3,
        bucket: "b",
        region: "us-east-1",
        access_key_id: "k",
        secret_access_key: "s",
        endpoint: "https://s3.example",
        path_style: true,
        req_plug: plug
      )
    end

    test "a response carries a duration; a transport error emits status_class error" do
      id = "tel-#{System.unique_integer([:positive])}"
      test = self()
      hid = "protocol-test-#{id}"

      :telemetry.attach(
        hid,
        [:fathom, :s3, :op],
        fn _e, m, meta, _ -> send(test, {:s3, m, meta}) end,
        nil
      )

      on_exit(fn -> :telemetry.detach(hid) end)

      use_s3(fn conn -> Plug.Conn.send_resp(conn, 200, "") end)
      _ = S3.object_etag(id)
      assert_receive {:s3, %{duration: d}, %{status_class: "2xx"}}
      assert is_integer(d) and d >= 0

      use_s3(fn conn -> Req.Test.transport_error(conn, :econnrefused) end)
      _ = S3.object_etag(id)
      assert_receive {:s3, %{duration: d2}, %{status_class: "error"}}, 5_000
      assert is_integer(d2) and d2 >= 0
    end
  end

  describe "real Bandit listeners" do
    defmodule Other do
      def init(o), do: o
      def call(conn, _), do: Plug.Conn.send_resp(conn, 200, "ok")
    end

    test "a request to a Filo.Plug listener counts; to any other Bandit plug does not" do
      filo =
        {Filo.Plug, executor: Fathom.ShardExecutor, streams: :none, key: Filo.Baton.new_key()}

      {:ok, a} = Bandit.start_link(plug: filo, port: 0, ip: :loopback, startup_log: false)
      {:ok, b} = Bandit.start_link(plug: Other, port: 0, ip: :loopback, startup_log: false)
      {:ok, {_, pa}} = ThousandIsland.listener_info(a)
      {:ok, {_, pb}} = ThousandIsland.listener_info(b)

      Req.get!("http://127.0.0.1:#{pb}/x", retry: false)
      assert stats().http.rate |> Map.values() |> Enum.sum() == 0.0

      # Filo answers GET /v2 with some status; any class counts.
      Req.get!("http://127.0.0.1:#{pa}/v2", retry: false, http_errors: :return)
      assert_eventually(fn -> stats().http.rate |> Map.values() |> Enum.sum() == 1.0 end)
    end

    defp assert_eventually(fun, tries \\ 50) do
      cond do
        fun.() -> :ok
        tries == 0 -> flunk("condition never held")
        true -> Process.sleep(20) && assert_eventually(fun, tries - 1)
      end
    end
  end

  @tag :bench
  test "handler cost per event (microbench)" do
    n = 100_000
    meas = %{duration: ms(3)}
    meta = %{plug: {Filo.Plug, []}, conn: %{status: 200}}
    other = %{meta | plug: {FathomWeb.Endpoint, []}}

    run = fn m ->
      for _ <- 1..n, do: Protocol.handle_event([:bandit, :request, :stop], meas, m, nil)
    end

    {us, _} = :timer.tc(fn -> run.(meta) end)
    {us_other, _} = :timer.tc(fn -> run.(other) end)

    IO.puts(
      "protocol handler: #{Float.round(us / n * 1000, 1)} ns/counted event, " <>
        "#{Float.round(us_other / n * 1000, 1)} ns/ignored event"
    )

    assert us / n < 20.0
  end
end
