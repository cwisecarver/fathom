defmodule Fathom.Bench.HranaClient do
  @moduledoc """
  A minimal, synchronous Hrana-over-WebSocket **client** for the wire benches (Phase 1,
  docs/tpc-benchmark-plan.md). It exists because the ecosystem ships no in-process Elixir
  libSQL client — Filo's own WS test shells to Python — so the benches build one on
  `Mint.WebSocket` to drive the *full* `Filo.Socket` path (WS framing, `hello`, request/
  response, client-allocated `stream_id`) exactly as `django-libsql` does.

  Lives in `test/support` (dev/test only, like `mint_web_socket`), so it never ships in a
  prod release. It's a bench/test tool, not production code.

  ## Protocol (JSON, `hrana3`)

  Open the socket → send `hello` → `hello_ok` → `open_stream` → N × `execute` → `close_stream`.
  The shard is chosen by the upgrade request's `Host` header (`<shard>.local`), routed by
  `Fathom.ShardExecutor.shard_from_conn/1` just like a real client.

  ## Usage

      {:ok, sup, port} = HranaClient.start_listener()
      {:ok, c} = HranaClient.connect(port, "acme")
      {:ok, %{rows: [[1]]}} = HranaClient.execute(c, "SELECT 1")
      :ok = HranaClient.close(c)
      HranaClient.stop_listener(sup)
  """

  alias Fathom.ShardExecutor

  @enforce_keys [:conn, :ref, :websocket, :stream_id]
  defstruct [:conn, :ref, :websocket, :stream_id, next_id: 0]

  @recv_timeout 15_000

  # --- listener (server side) ----------------------------------------------

  @doc """
  Starts an in-process Filo listener (Bandit + `Filo.Plug` + `Filo.Streams`) on `127.0.0.1`,
  a free port, auth disabled — the loopback the client connects to. Returns
  `{:ok, supervisor_pid, port}`. Usable outside ExUnit (the bench task starts it this way);
  tests may prefer `start_supervised!/1` on the same children.
  """
  @spec start_listener(keyword()) :: {:ok, pid(), pos_integer()}
  def start_listener(opts \\ []) do
    streams = Keyword.get(opts, :streams_name, __MODULE__.Streams)
    port = Keyword.get(opts, :port, free_port())

    plug_opts = [
      executor: ShardExecutor,
      streams: streams,
      key: Filo.Baton.new_key(),
      open_arg: &ShardExecutor.shard_from_conn/1
      # No :authorize — the bench runs with Hrana auth disabled.
    ]

    children = [
      {Filo.Streams, name: streams},
      {Bandit, plug: {Filo.Plug, plug_opts}, scheme: :http, ip: {127, 0, 0, 1}, port: port}
    ]

    {:ok, sup} = Supervisor.start_link(children, strategy: :one_for_one)
    {:ok, sup, port}
  end

  @doc "Stops a listener started by `start_listener/1`. Tolerant of an already-down supervisor."
  @spec stop_listener(pid()) :: :ok
  def stop_listener(sup) do
    if Process.alive?(sup), do: Supervisor.stop(sup)
    :ok
  catch
    # The listener supervisor is linked to its starter; if that process already exited
    # (e.g. an ExUnit test whose on_exit runs after teardown), the stop races the shutdown.
    :exit, _ -> :ok
  end

  # --- client (drive one stream) -------------------------------------------

  @doc """
  Connects to the loopback listener, upgrades to WS (`hrana3`, `Host: <shard>.local`),
  handshakes `hello`, and opens one stream. Returns `{:ok, client}`.
  """
  @spec connect(pos_integer(), String.t(), keyword()) :: {:ok, %__MODULE__{}} | {:error, term()}
  def connect(port, shard, _opts \\ []) do
    host = "#{shard}.local"

    with {:ok, conn} <- Mint.HTTP.connect(:http, "127.0.0.1", port, protocols: [:http1]),
         headers = [{"host", host}, {"sec-websocket-protocol", "hrana3"}],
         {:ok, conn, ref} <- Mint.WebSocket.upgrade(:ws, conn, "/", headers),
         {:ok, conn, websocket} <- await_upgrade(conn, ref) do
      client = %__MODULE__{conn: conn, ref: ref, websocket: websocket, stream_id: 0}

      with {:ok, client, %{"type" => "hello_ok"}} <- rpc_raw(client, %{"type" => "hello"}),
           {:ok, client, _} <- request(client, %{"type" => "open_stream", "stream_id" => 0}) do
        {:ok, client}
      end
    end
  end

  @doc """
  Runs `sql` (with native `args`) on the client's stream through the wire. Returns
  `{:ok, %{rows: rows, cols: cols, affected_row_count: n}}` with rows as native Elixir values.
  """
  @spec execute(%__MODULE__{}, String.t(), list()) :: {:ok, map()} | {:error, term()}
  def execute(client, sql, args \\ []) do
    stmt = %{"sql" => sql, "args" => Enum.map(args, &encode_value/1)}

    case request(client, %{"type" => "execute", "stream_id" => client.stream_id, "stmt" => stmt}) do
      {:ok, client, %{"result" => result}} ->
        {:ok, client,
         %{
           rows:
             Enum.map(Map.get(result, "rows", []), fn row -> Enum.map(row, &decode_value/1) end),
           cols: Enum.map(Map.get(result, "cols", []), &Map.get(&1, "name")),
           affected_row_count: Map.get(result, "affected_row_count", 0)
         }}

      other ->
        other
    end
  end

  @doc "Closes the stream and the socket."
  @spec close(%__MODULE__{}) :: :ok
  def close(client) do
    {:ok, client, _} =
      request(client, %{"type" => "close_stream", "stream_id" => client.stream_id})

    Mint.HTTP.close(client.conn)
    :ok
  end

  # --- internals -----------------------------------------------------------

  # A `request` frame (post-hello): wraps the payload with a monotonic request_id and
  # unwraps the response_ok's inner response (or surfaces a response_error).
  defp request(client, payload) do
    id = client.next_id
    frame = %{"type" => "request", "request_id" => id, "request" => payload}

    case rpc_raw(%{client | next_id: id + 1}, frame) do
      {:ok, client, %{"type" => "response_ok", "response" => response}} ->
        {:ok, client, response}

      {:ok, _client, %{"type" => "response_error", "error" => error}} ->
        {:error, {:hrana_error, error}}

      other ->
        other
    end
  end

  # Send one JSON frame, block for exactly one JSON frame back, return the decoded map.
  defp rpc_raw(client, message) do
    {:ok, websocket, data} =
      Mint.WebSocket.encode(client.websocket, {:text, Jason.encode!(message)})

    {:ok, conn} = Mint.WebSocket.stream_request_body(client.conn, client.ref, data)
    recv_frame(%{client | conn: conn, websocket: websocket})
  end

  defp recv_frame(client) do
    receive do
      message ->
        case Mint.WebSocket.stream(client.conn, message) do
          {:ok, conn, responses} ->
            case decode_ws(client.websocket, client.ref, responses) do
              {:ok, websocket, text} ->
                {:ok, %{client | conn: conn, websocket: websocket}, Jason.decode!(text)}

              {:more, websocket} ->
                recv_frame(%{client | conn: conn, websocket: websocket})
            end

          {:error, _conn, reason, _} ->
            {:error, reason}
        end
    after
      @recv_timeout -> {:error, :timeout}
    end
  end

  # Pull frames out of Mint responses; return the first text frame's payload.
  defp decode_ws(websocket, ref, responses) do
    frames =
      for {:data, ^ref, data} <- responses, reduce: {websocket, []} do
        {ws, acc} ->
          {:ok, ws, decoded} = Mint.WebSocket.decode(ws, data)
          {ws, acc ++ decoded}
      end

    case frames do
      {ws, decoded} ->
        case Enum.find(decoded, fn {op, _} -> op == :text end) do
          {:text, text} -> {:ok, ws, text}
          _ -> {:more, ws}
        end
    end
  end

  # Consume the 101 upgrade response and build the websocket.
  defp await_upgrade(conn, ref) do
    receive do
      message ->
        case Mint.WebSocket.stream(conn, message) do
          {:ok, conn, responses} ->
            status = for({:status, ^ref, s} <- responses, do: s) |> List.last()
            resp_headers = for({:headers, ^ref, h} <- responses, do: h) |> List.last()

            if status && resp_headers do
              Mint.WebSocket.new(conn, ref, status, resp_headers)
            else
              await_upgrade(conn, ref)
            end

          {:error, _conn, reason, _} ->
            {:error, reason}
        end
    after
      @recv_timeout -> {:error, :upgrade_timeout}
    end
  end

  # Hrana value encoding: native Elixir → Hrana value (integers travel as strings).
  # Delegated to `Filo.Value` — the SERVER's own codec — rather than reimplemented here.
  # The hand-rolled pair this replaces could neither send nor receive a BLOB:
  #
  #   * `decode_value` used `Base.decode64!/1`, the PADDED decoder, while Hrana (and
  #     `Filo.Value.encode_json/1`) emits **unpadded** base64. Any blob whose length was not a
  #     multiple of 3 crashed the client with `ArgumentError: incorrect padding`.
  #   * `encode_value` had no `{:blob, _}` clause at all, so binding a blob argument raised
  #     `FunctionClauseError`.
  #
  # Both survived because every wire benchmark is TPC-B or TPC-C — INTEGER, REAL and TEXT only —
  # so no bench ever put a blob on the wire. Delegating removes the whole drift class: the
  # client cannot disagree with the server about the encoding if it uses the server's encoder.
  # Behavior is unchanged for null/integer/float/text; blobs decode to `{:blob, binary}`, the
  # same tagged shape `Fathom.Shard.Connection` uses.
  defp encode_value(v), do: Filo.Value.encode(v)

  defp decode_value(v), do: Filo.Value.decode(v)

  defp free_port do
    {:ok, socket} = :gen_tcp.listen(0, [])
    {:ok, port} = :inet.port(socket)
    :gen_tcp.close(socket)
    port
  end
end
