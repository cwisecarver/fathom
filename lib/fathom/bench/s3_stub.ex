defmodule Fathom.Bench.S3Stub do
  @moduledoc """
  A loopback S3 stand-in for the storage-IO benches (`Fathom.Bench.StorageIO`): a Plug answering
  HEAD/GET/PUT/DELETE on any path from an ETS table, with S3-shaped headers (quoted `etag`, the
  `x-amz-meta-*` user metadata). It is served by a real Bandit listener so requests cross a real
  socket and the real S3 Finch pool, which a `req_plug` double would bypass.

  It honours exactly what the S3 client's fences need and nothing more: `If-Match` /
  `If-None-Match: *` on PUT (412 on a mismatch, as S3 answers), a fresh etag per PUT, and
  `x-amz-meta-*` request headers echoed on HEAD/GET. DELETE (what `release_lease` and a drop send) is
  S3-shaped: 204 whether or not the key exists, except that an `If-Match` that does not match the
  current etag is a 412 (and a missing key under `If-Match` is a 404). No auth check, no multipart,
  no listing.
  """
  @behaviour Plug

  import Plug.Conn

  @max_body 128_000_000

  @doc "Starts a stub on a free loopback port. Returns `{supervisor, port, table}`."
  @spec start() :: {pid(), :inet.port_number(), :ets.tid()}
  def start do
    table = :ets.new(:s3_stub, [:set, :public])

    {:ok, sup} =
      Supervisor.start_link(
        [{Bandit, plug: {__MODULE__, table: table}, scheme: :http, ip: {127, 0, 0, 1}, port: 0}],
        strategy: :one_for_one
      )

    [{_id, bandit, _, _}] = Supervisor.which_children(sup)
    {:ok, {_ip, port}} = ThousandIsland.listener_info(bandit)
    {sup, port, table}
  end

  @doc "Stores an object directly (bypassing PUT): `headers` are the response headers to serve."
  @spec put_object(:ets.tid(), String.t(), binary(), [{String.t(), String.t()}]) :: :ok
  def put_object(table, path, body, headers) do
    :ets.insert(table, {path, body, headers})
    :ok
  end

  @impl Plug
  def init(opts), do: Keyword.fetch!(opts, :table)

  @impl Plug
  def call(conn, table) do
    case {conn.method, :ets.lookup(table, conn.request_path)} do
      {"PUT", existing} ->
        put(conn, table, existing)

      {"DELETE", existing} ->
        delete(conn, table, existing)

      {m, [{_, body, headers}]} when m in ["GET", "HEAD"] ->
        conn
        |> prepend_resp_headers(headers)
        |> send_resp(200, if(m == "HEAD", do: "", else: body))

      {m, []} when m in ["GET", "HEAD"] ->
        send_resp(conn, 404, "")
    end
  end

  defp delete(conn, table, existing) do
    current = with [{_, _, headers}] <- existing, do: List.keyfind(headers, "etag", 0)
    current = if is_tuple(current), do: elem(current, 1)

    case get_req_header(conn, "if-match") |> List.first() do
      nil ->
        :ets.delete(table, conn.request_path)
        send_resp(conn, 204, "")

      _ when current == nil ->
        send_resp(conn, 404, "")

      etag when etag != current ->
        send_resp(conn, 412, "")

      _ ->
        :ets.delete(table, conn.request_path)
        send_resp(conn, 204, "")
    end
  end

  defp put(conn, table, existing) do
    current = with [{_, _, headers}] <- existing, do: List.keyfind(headers, "etag", 0)
    current = if is_tuple(current), do: elem(current, 1)
    if_match = get_req_header(conn, "if-match") |> List.first()
    if_none = get_req_header(conn, "if-none-match") |> List.first()

    cond do
      if_match && if_match != current -> send_resp(conn, 412, "")
      if_none == "*" && current != nil -> send_resp(conn, 412, "")
      true -> store(conn, table)
    end
  end

  defp store(conn, table) do
    {body, conn} = read_all(conn, [])

    etag =
      ~s("#{Base.encode16(:crypto.hash(:md5, body), case: :lower)}-#{System.unique_integer([:positive])}")

    meta =
      for {k, v} <- conn.req_headers, String.starts_with?(k, "x-amz-meta-"), do: {k, v}

    :ets.insert(table, {conn.request_path, body, [{"etag", etag} | meta]})

    conn
    |> put_resp_header("etag", etag)
    |> send_resp(200, "")
  end

  defp read_all(conn, acc) do
    case read_body(conn, length: @max_body) do
      {:ok, chunk, conn} -> {IO.iodata_to_binary(Enum.reverse([chunk | acc])), conn}
      {:more, chunk, conn} -> read_all(conn, [chunk | acc])
    end
  end
end
