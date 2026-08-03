defmodule Fathom.Test.S3EtagStore do
  @moduledoc """
  A stateful `req_plug` double of an **MD5-etag** S3 store (round-2 #4): etags are
  DERIVED FROM CONTENT exactly like real S3 — a single-form etag is `md5(body)`, a
  one-part multipart etag is `md5(md5(body))-1`. A plain self-copy of identical
  bytes therefore does NOT rotate the etag (the honest production behavior the
  original shape-only stubs missed), while the alternating-form touch does.

  Usage: `store = start_supervised!({Agent, fn -> S3EtagStore.initial(%{"k.db" => "bytes"}) end})`
  then `req_plug: fn conn -> S3EtagStore.serve(conn, store) end`. Objects live under
  bucket-stripped keys (`/b/<key>` → `<key>`). Conditional PUT/copy semantics
  (If-Match / If-None-Match / x-amz-copy-source-if-match) are enforced.
  """

  def initial(objects) do
    %{
      objects: Map.new(objects, fn {k, body} -> {k, %{body: body, form: :single, meta: %{}}} end),
      uploads: %{},
      seq: 0,
      # Optional: the epoch-ms this store reports as its response `Date` (the store's own clock). nil ⇒
      # no Date header, so callers fall back to their local clock. Used to model clock skew (#13).
      date_ms: nil
    }
  end

  @doc "Set the store's reported `Date` clock (epoch-ms), or nil for no Date header (#13)."
  def set_date_ms(agent, ms), do: Agent.update(agent, &%{&1 | date_ms: ms})

  @doc "The store's metadata map for `key` (nil when absent)."
  def meta_of(agent, key) do
    case Agent.get(agent, &Map.get(&1.objects, key)) do
      nil -> nil
      obj -> obj.meta
    end
  end

  @doc "The store's current etag for `key` (nil when absent) — for test assertions."
  def etag_of(agent, key) do
    case Agent.get(agent, &Map.get(&1.objects, key)) do
      nil -> nil
      obj -> etag(obj)
    end
  end

  @doc "The store's current body for `key` (nil when absent)."
  def body_of(agent, key) do
    case Agent.get(agent, &Map.get(&1.objects, key)) do
      nil -> nil
      obj -> obj.body
    end
  end

  @doc """
  Force-set `key` to `body` at single (MD5) form, unconditionally (no If-Match).

  Models an out-of-band write LANDING in the store between two of a caller's reads —
  a dead owner's in-flight fenced flush, or another node's acquire→flush→release cycle
  in the gap the takeover-revalidation lattice guards (expert review #30). Metadata is
  cleared, as a fresh single-part PUT would.
  """
  def set_body(agent, key, body) do
    Agent.update(agent, fn s ->
      %{s | objects: Map.put(s.objects, key, %{body: body, form: :single, meta: %{}})}
    end)
  end

  @doc """
  Copy `src` to `dst` **including metadata**, the way S3's CopyObject does by default
  (metadata directive `COPY`).

  Unlike `set_body/3`, this preserves `x-amz-meta-*` — which is the whole point for expert
  review 2026-08-01 #25: it is how a steal sentinel came to sit at a `@version` / `@snap-` key
  in the first place, and a test for the guard has to be able to stage that already-durable
  state. Returns `:ok`, or raises if `src` is absent (a silent no-op here would produce a test
  that asserts against an object that was never created).
  """
  def copy(agent, src, dst) do
    Agent.update(agent, fn s ->
      case Map.get(s.objects, src) do
        nil -> raise ArgumentError, "S3EtagStore.copy/3: no object at #{inspect(src)}"
        obj -> %{s | objects: Map.put(s.objects, dst, obj)}
      end
    end)
  end

  def serve(%Plug.Conn{} = conn, agent) do
    {:ok, body, conn} = Plug.Conn.read_body(conn)
    key = strip_bucket(conn.request_path)
    query = URI.decode_query(conn.query_string || "")

    cond do
      conn.method == "HEAD" ->
        head(conn, agent, key)

      # ListObjectsV2, which `purge_shard/1` pages through. Without it the whole erase path was
      # unreachable against this double, so the delimiter rule that stops purging `acme` from
      # destroying `acme2` could only ever be asserted for Local — exactly the one-backend blind
      # spot expert review 2026-08-01 #30 is about.
      conn.method == "GET" and Map.get(query, "list-type") == "2" ->
        list_objects(conn, agent, query)

      conn.method == "GET" ->
        get(conn, agent, key)

      conn.method == "POST" and Map.has_key?(query, "uploads") ->
        create_upload(conn, agent, key)

      conn.method == "POST" and Map.has_key?(query, "uploadId") ->
        complete_upload(conn, agent, key, query["uploadId"])

      conn.method == "PUT" and Map.has_key?(query, "uploadId") ->
        part_copy(conn, agent, key, query["uploadId"])

      conn.method == "PUT" and copy_source(conn) != nil ->
        object_copy(conn, agent, key)

      conn.method == "PUT" ->
        put_object(conn, agent, key, body)

      conn.method == "DELETE" and Map.has_key?(query, "uploadId") ->
        Agent.update(agent, &%{&1 | uploads: Map.delete(&1.uploads, query["uploadId"])})
        Plug.Conn.send_resp(conn, 204, "")

      conn.method == "DELETE" ->
        delete_object(conn, agent, key)

      true ->
        Plug.Conn.send_resp(conn, 500, "unexpected #{conn.method} #{conn.request_path}")
    end
  end

  # --- handlers ---

  defp head(conn, agent, key) do
    conn = put_date(conn, agent)

    case Agent.get(agent, &Map.get(&1.objects, key)) do
      nil ->
        Plug.Conn.send_resp(conn, 404, "")

      obj ->
        conn
        |> Plug.Conn.put_resp_header("etag", etag(obj))
        |> put_meta_headers(obj)
        |> Plug.Conn.send_resp(200, "")
    end
  end

  defp get(conn, agent, key) do
    conn = put_date(conn, agent)
    if_none_match = Plug.Conn.get_req_header(conn, "if-none-match")

    case Agent.get(agent, &Map.get(&1.objects, key)) do
      nil ->
        Plug.Conn.send_resp(conn, 404, "")

      obj ->
        # Conditional GET. This double had no If-None-Match handling at all, so `pull_if_changed/3`
        # — the warm-standby freshness check, and the entire reason a failover can promote a warm
        # copy instead of re-transferring the body — could never take its 304 path here. Its S3
        # coverage lived only behind `@tag :s3` (MinIO), which the default suite EXCLUDES, so the
        # S3 side of that primitive was unreachable from `mix test`. Found by the shared storage
        # contract; it is the same one-backend blind spot as expert review 2026-08-01 #30.
        cond do
          if_none_match == ["*"] ->
            not_modified(conn, obj)

          if_none_match != [] and hd(if_none_match) == etag(obj) ->
            not_modified(conn, obj)

          true ->
            conn
            |> Plug.Conn.put_resp_header("etag", etag(obj))
            |> put_meta_headers(obj)
            |> Plug.Conn.send_resp(200, obj.body)
        end
    end
  end

  # A 304 carries the etag but NO body, which is the byte-transfer saving the warm path exists for.
  defp not_modified(conn, obj) do
    conn
    |> Plug.Conn.put_resp_header("etag", etag(obj))
    |> Plug.Conn.send_resp(304, "")
  end

  # The store's `Date` header (its own clock), when configured — models clock skew for #13.
  defp put_date(conn, agent) do
    case Agent.get(agent, & &1.date_ms) do
      nil ->
        conn

      ms ->
        # Format as an RFC 1123 GMT string directly (NOT :httpd_util.rfc1123_date/1, which treats its
        # argument as LOCAL time and shifts by the machine's tz offset — corrupting the Date under test).
        {:ok, dt} = DateTime.from_unix(ms, :millisecond)

        Plug.Conn.put_resp_header(
          conn,
          "date",
          Calendar.strftime(dt, "%a, %d %b %Y %H:%M:%S GMT")
        )
    end
  end

  defp put_meta_headers(conn, obj) do
    Enum.reduce(obj.meta, conn, fn {k, v}, c -> Plug.Conn.put_resp_header(c, k, v) end)
  end

  defp req_meta(conn) do
    for {"x-amz-meta-" <> _ = k, v} <- conn.req_headers, into: %{}, do: {k, v}
  end

  defp put_object(conn, agent, key, body) do
    if_match = Plug.Conn.get_req_header(conn, "if-match")
    if_none_match = Plug.Conn.get_req_header(conn, "if-none-match")
    meta = req_meta(conn)

    Agent.get_and_update(agent, fn s ->
      current = Map.get(s.objects, key)

      cond do
        if_none_match == ["*"] and current != nil ->
          {{412, nil}, s}

        if_match != [] and (current == nil or hd(if_match) != etag(current)) ->
          {{412, nil}, s}

        true ->
          obj = %{body: body, form: :single, meta: meta}
          {{200, etag(obj)}, %{s | objects: Map.put(s.objects, key, obj)}}
      end
    end)
    |> case do
      {200, new_etag} ->
        conn |> Plug.Conn.put_resp_header("etag", new_etag) |> Plug.Conn.send_resp(200, "")

      {412, _} ->
        Plug.Conn.send_resp(conn, 412, "")
    end
  end

  # Plain CopyObject: the result is a SINGLE-form (MD5) etag — identical bytes at
  # single form stay at the SAME etag, the honest MD5-store behavior.
  defp object_copy(conn, agent, dst_key) do
    src_key = strip_bucket(copy_source(conn))
    src_if_match = Plug.Conn.get_req_header(conn, "x-amz-copy-source-if-match")

    Agent.get_and_update(agent, fn s ->
      src = Map.get(s.objects, src_key)

      cond do
        src == nil ->
          {{404, nil}, s}

        src_if_match != [] and hd(src_if_match) != etag(src) ->
          {{412, nil}, s}

        true ->
          # Model S3's metadata directive: REPLACE takes the request's x-amz-meta-* (so a self-copy
          # touch must RE-SEND the integrity md5 to keep it — #12); COPY/absent preserves the source's.
          meta =
            case Plug.Conn.get_req_header(conn, "x-amz-metadata-directive") do
              ["REPLACE"] -> req_meta(conn)
              _ -> src.meta
            end

          obj = %{body: src.body, form: :single, meta: meta}
          {{200, etag(obj)}, %{s | objects: Map.put(s.objects, dst_key, obj)}}
      end
    end)
    |> case do
      {200, new_etag} ->
        Plug.Conn.send_resp(
          conn,
          200,
          ~s(<CopyObjectResult><ETag>&quot;#{String.trim(new_etag, ~s("))}&quot;</ETag></CopyObjectResult>)
        )

      {412, _} ->
        Plug.Conn.send_resp(conn, 412, "")

      {404, _} ->
        Plug.Conn.send_resp(conn, 404, "")
    end
  end

  defp create_upload(conn, agent, key) do
    # CreateMultipartUpload is where a multipart object's user metadata is set (#12) — capture it.
    meta = req_meta(conn)

    id =
      Agent.get_and_update(agent, fn s ->
        id = "up-#{s.seq + 1}"

        {id,
         %{
           s
           | seq: s.seq + 1,
             uploads: Map.put(s.uploads, id, %{key: key, body: nil, meta: meta})
         }}
      end)

    Plug.Conn.send_resp(
      conn,
      200,
      "<InitiateMultipartUploadResult><UploadId>#{id}</UploadId></InitiateMultipartUploadResult>"
    )
  end

  defp part_copy(conn, agent, _key, upload_id) do
    src_key = strip_bucket(copy_source(conn))
    src_if_match = Plug.Conn.get_req_header(conn, "x-amz-copy-source-if-match")

    Agent.get_and_update(agent, fn s ->
      src = Map.get(s.objects, src_key)

      cond do
        src == nil or not Map.has_key?(s.uploads, upload_id) ->
          {{404, nil}, s}

        src_if_match != [] and hd(src_if_match) != etag(src) ->
          {{412, nil}, s}

        true ->
          part_etag = md5hex(src.body)
          uploads = Map.update!(s.uploads, upload_id, &%{&1 | body: src.body})
          {{200, part_etag}, %{s | uploads: uploads}}
      end
    end)
    |> case do
      {200, part_etag} ->
        Plug.Conn.send_resp(
          conn,
          200,
          ~s(<CopyPartResult><ETag>&quot;#{part_etag}&quot;</ETag></CopyPartResult>)
        )

      {412, _} ->
        Plug.Conn.send_resp(conn, 412, "")

      {404, _} ->
        Plug.Conn.send_resp(conn, 404, "")
    end
  end

  defp complete_upload(conn, agent, key, upload_id) do
    Agent.get_and_update(agent, fn s ->
      case Map.get(s.uploads, upload_id) do
        %{body: body, meta: meta} when is_binary(body) ->
          obj = %{body: body, form: :multipart, meta: meta}

          {{200, etag(obj)},
           %{
             s
             | objects: Map.put(s.objects, key, obj),
               uploads: Map.delete(s.uploads, upload_id)
           }}

        _ ->
          {{404, nil}, s}
      end
    end)
    |> case do
      {200, new_etag} ->
        Plug.Conn.send_resp(
          conn,
          200,
          "<CompleteMultipartUploadResult><ETag>&quot;#{String.trim(new_etag, ~s("))}&quot;</ETag></CompleteMultipartUploadResult>"
        )

      {404, _} ->
        Plug.Conn.send_resp(conn, 404, "")
    end
  end

  defp delete_object(conn, agent, key) do
    Agent.update(agent, &%{&1 | objects: Map.delete(&1.objects, key)})
    Plug.Conn.send_resp(conn, 204, "")
  end

  # ListObjectsV2. Only `prefix` and the paging pair are modelled — that is all `purge_shard/1`
  # and `stored_usage/0` send. Keys are returned SORTED so paging is deterministic; the page size
  # is small on purpose so the continuation-token loop is actually exercised rather than always
  # completing in one shot (a one-page fake would leave `next_token/1` untested forever).
  @page_size 3

  defp list_objects(conn, agent, query) do
    prefix = Map.get(query, "prefix", "")
    after_key = Map.get(query, "continuation-token")

    keys =
      agent
      |> Agent.get(& &1.objects)
      |> Map.keys()
      |> Enum.filter(&String.starts_with?(&1, prefix))
      |> Enum.sort()
      |> then(fn ks -> if after_key, do: Enum.filter(ks, &(&1 > after_key)), else: ks end)

    {page, rest} = Enum.split(keys, @page_size)

    contents = Enum.map_join(page, "", fn k -> "<Contents><Key>#{k}</Key></Contents>" end)

    next =
      case {rest, List.last(page)} do
        {[], _} -> ""
        {_, last} -> "<NextContinuationToken>#{last}</NextContinuationToken>"
      end

    Plug.Conn.send_resp(conn, 200, "<ListBucketResult>#{contents}#{next}</ListBucketResult>")
  end

  # --- etag math (the point of this double) ---

  defp etag(%{body: body, form: :single}), do: ~s(") <> md5hex(body) <> ~s(")

  defp etag(%{body: body, form: :multipart}),
    do: ~s(") <> md5hex(:crypto.hash(:md5, body)) <> ~s(-1")

  defp md5hex(data), do: Base.encode16(:crypto.hash(:md5, data), case: :lower)

  defp copy_source(conn) do
    case Plug.Conn.get_req_header(conn, "x-amz-copy-source") do
      [src | _] -> src
      [] -> nil
    end
  end

  defp strip_bucket("/b/" <> key), do: key
  defp strip_bucket("/" <> key), do: key
  defp strip_bucket(other), do: other
end
