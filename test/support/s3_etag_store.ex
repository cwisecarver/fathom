defmodule Fathom.Test.S3EtagStore do
  @moduledoc """
  A stateful `req_plug` double of an **MD5-etag** S3 store (round-2 #4): etags are
  DERIVED FROM CONTENT exactly like real S3 — a single-form etag is `md5(body)`, an
  N-part multipart etag is `md5(md5(part1) <> ... <> md5(partN))-N`. A plain self-copy
  of identical bytes therefore does NOT rotate the etag (the honest production behavior
  the original shape-only stubs missed).

  Multipart is modelled the way S3 does it, not the way one caller happens to use it
  (expert review 2026-10-12 N1): parts are keyed by `partNumber`, `UploadPartCopy` honours
  `x-amz-copy-source-range` (416 on a range outside the source), and Complete takes the
  part list from its XML body, checks each listed part's etag, and refuses a non-last part
  under 5 MiB with `EntityTooSmall`. The one-part-only model this replaced could not express
  a boundary-split touch at all, so it could neither show that touch working nor failing.

  HEAD reports `content-length` (the touch picks its path by object size), and GET honours
  `If-Match` (the small-object touch reads the exact version it rewrites).

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
  The DATABASE bytes stored at `key`: `body_of/2` decoded by the object's own encoding marker, the
  way a real pull decodes. Stored objects are zstd-compressed by default (2026-10-05), so a test that
  asserts on content or opens the object as SQLite wants this, not the raw stored body.
  """
  def plain_body_of(agent, key) do
    case {body_of(agent, key), meta_of(agent, key)} do
      {nil, _} -> nil
      {body, meta} -> decode(body, meta && meta[Fathom.Shard.Storage.Codec.meta_header()])
    end
  end

  defp decode(body, "zstd"), do: body |> :zstd.decompress() |> IO.iodata_to_binary()
  defp decode(body, "zlib"), do: :zlib.uncompress(body)
  defp decode(body, nil), do: body

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
    {:ok, body, conn} = read_full_body(conn, "")
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

      # ListMultipartUploads (expert review 2026-10-13 #21): the in-progress uploads ListObjectsV2
      # cannot see, which `purge_shard/1` must abort.
      conn.method == "GET" and Map.has_key?(query, "uploads") ->
        list_uploads(conn, agent, query)

      conn.method == "GET" ->
        get(conn, agent, key)

      conn.method == "POST" and Map.has_key?(query, "uploads") ->
        create_upload(conn, agent, key)

      conn.method == "POST" and Map.has_key?(query, "uploadId") ->
        complete_upload(conn, agent, key, query["uploadId"], body)

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
        |> Plug.Conn.put_resp_header("content-length", Integer.to_string(byte_size(obj.body)))
        |> put_meta_headers(obj)
        |> Plug.Conn.send_resp(200, "")
    end
  end

  defp get(conn, agent, key) do
    conn = put_date(conn, agent)
    if_none_match = Plug.Conn.get_req_header(conn, "if-none-match")
    if_match = Plug.Conn.get_req_header(conn, "if-match")

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
          # S3 answers 412 when If-Match names a version other than the current one.
          if_match != [] and hd(if_match) != etag(obj) ->
            Plug.Conn.send_resp(conn, 412, "")

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
             uploads: Map.put(s.uploads, id, %{key: key, parts: %{}, meta: meta})
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
    part_number = conn.query_string |> URI.decode_query() |> Map.get("partNumber", "1")
    range = Plug.Conn.get_req_header(conn, "x-amz-copy-source-range")

    Agent.get_and_update(agent, fn s ->
      src = Map.get(s.objects, src_key)

      cond do
        src == nil or not Map.has_key?(s.uploads, upload_id) ->
          {{404, nil}, s}

        src_if_match != [] and hd(src_if_match) != etag(src) ->
          {{412, nil}, s}

        true ->
          case copy_range(src.body, range) do
            {:ok, bytes} ->
              uploads =
                Map.update!(s.uploads, upload_id, fn up ->
                  %{up | parts: Map.put(up.parts, String.to_integer(part_number), bytes)}
                end)

              {{200, md5hex(bytes)}, %{s | uploads: uploads}}

            :invalid ->
              {{416, nil}, s}
          end
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

      {416, _} ->
        Plug.Conn.send_resp(conn, 416, "<Error><Code>InvalidRange</Code></Error>")
    end
  end

  # `x-amz-copy-source-range: bytes=first-last` (inclusive), which S3 requires to lie inside the
  # source object. No header copies the whole source.
  defp copy_range(body, []), do: {:ok, body}

  defp copy_range(body, ["bytes=" <> spec]) do
    with [a, b] <- String.split(spec, "-"),
         {first, ""} <- Integer.parse(a),
         {last, ""} <- Integer.parse(b),
         true <- first >= 0 and last >= first and last < byte_size(body) do
      {:ok, binary_part(body, first, last - first + 1)}
    else
      _ -> :invalid
    end
  end

  defp copy_range(_body, _other), do: :invalid

  # S3's minimum size for every part but the last.
  @min_part 5 * 1024 * 1024

  defp complete_upload(conn, agent, key, upload_id, req_body) do
    listed = completed_parts(req_body)
    if_match = Plug.Conn.get_req_header(conn, "if-match")

    Agent.get_and_update(agent, fn s ->
      current = Map.get(s.objects, key)

      # A conditional Complete (expert review 2026-10-13 #3): S3 evaluates If-Match against the
      # object currently at the key. A missing key is 404 NoSuchKey, a different etag 412.
      refused =
        cond do
          if_match == [] -> nil
          current == nil -> 404
          hd(if_match) != etag(current) -> 412
          true -> nil
        end

      case Map.get(s.uploads, upload_id) do
        %{} when refused != nil ->
          {{refused, nil}, s}

        %{parts: parts, meta: meta} ->
          case assemble(listed, parts) do
            {:ok, bodies} ->
              obj = %{
                body: IO.iodata_to_binary(bodies),
                form: :multipart,
                meta: meta,
                parts: Enum.map(bodies, &:crypto.hash(:md5, &1))
              }

              {{200, etag(obj)},
               %{
                 s
                 | objects: Map.put(s.objects, key, obj),
                   uploads: Map.delete(s.uploads, upload_id)
               }}

            {:error, code} ->
              {{400, code}, s}
          end

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

      {400, code} ->
        Plug.Conn.send_resp(conn, 400, "<Error><Code>#{code}</Code></Error>")

      {412, _} ->
        Plug.Conn.send_resp(conn, 412, "")

      {404, _} ->
        Plug.Conn.send_resp(conn, 404, "")
    end
  end

  # Listing page size for both ListObjectsV2 and ListMultipartUploads.
  @page_size 3

  @doc "The `{key, upload_id}` of every upload still open (created, never completed or aborted)."
  def open_uploads(agent) do
    agent |> Agent.get(& &1.uploads) |> Enum.map(fn {id, up} -> {up.key, id} end) |> Enum.sort()
  end

  # ListMultipartUploads: `prefix`, and the `key-marker` / `upload-id-marker` paging pair, with a
  # small page so the marker loop is exercised.
  defp list_uploads(conn, agent, query) do
    prefix = Map.get(query, "prefix", "")
    after_pair = {Map.get(query, "key-marker", ""), Map.get(query, "upload-id-marker", "")}

    all =
      agent
      |> open_uploads()
      |> Enum.filter(fn {k, _} -> String.starts_with?(k, prefix) end)
      |> Enum.filter(fn pair -> after_pair == {"", ""} or pair > after_pair end)

    {page, rest} = Enum.split(all, @page_size)

    uploads =
      Enum.map_join(page, "", fn {k, id} ->
        "<Upload><Key>#{k}</Key><UploadId>#{id}</UploadId></Upload>"
      end)

    next =
      case {rest, List.last(page)} do
        {[], _} ->
          "<IsTruncated>false</IsTruncated>"

        {_, {k, id}} ->
          "<IsTruncated>true</IsTruncated><NextKeyMarker>#{k}</NextKeyMarker>" <>
            "<NextUploadIdMarker>#{id}</NextUploadIdMarker>"
      end

    Plug.Conn.send_resp(
      conn,
      200,
      "<ListMultipartUploadsResult>#{next}#{uploads}</ListMultipartUploadsResult>"
    )
  end

  # The `<Part><PartNumber>/<ETag>` list of a CompleteMultipartUpload body, parsed with OTP's xmerl.
  defp completed_parts(xml) do
    {doc, _} = xml |> to_charlist() |> :xmerl_scan.string(quiet: true)

    for part <- :xmerl_xpath.string(~c"//Part", doc) do
      [num] = :xmerl_xpath.string(~c"PartNumber/text()", part)
      [tag] = :xmerl_xpath.string(~c"ETag/text()", part)

      {num |> xml_text() |> String.trim() |> String.to_integer(),
       tag |> xml_text() |> String.trim(~s("))}
    end
  end

  defp xml_text(node), do: node |> elem(4) |> to_string()

  # S3's Complete rules: listed parts ascend, each exists with the etag the caller names, and every
  # part but the last is at least 5 MiB.
  defp assemble([], _parts), do: {:error, "MalformedXML"}

  defp assemble(listed, parts) do
    numbers = Enum.map(listed, &elem(&1, 0))

    cond do
      numbers != Enum.sort(Enum.uniq(numbers)) ->
        {:error, "InvalidPartOrder"}

      Enum.any?(listed, fn {n, tag} ->
        not Map.has_key?(parts, n) or md5hex(parts[n]) != tag
      end) ->
        {:error, "InvalidPart"}

      true ->
        bodies = Enum.map(listed, fn {n, _} -> parts[n] end)

        if Enum.any?(Enum.drop(bodies, -1), &(byte_size(&1) < @min_part)),
          do: {:error, "EntityTooSmall"},
          else: {:ok, bodies}
    end
  end

  # Conditional DELETE (expert review 2026-09-05 #8). The lock-release fence (`release_lease/2`) is
  # a `DELETE ... If-Match: <etag>`, and the lock-leak fix (`resolve_412_release`, finding #22) is
  # load-bearing on the store answering 412 to a stale etag. This double USED to delete
  # unconditionally and return 204 regardless of If-Match, so no S3-path test could observe a DELETE
  # 412 — the double-vs-real contract gap AGENTS.md § Testing names. Now it enforces the header:
  # a matching etag deletes (204), a mismatch (or a conditional delete of an absent object) is
  # refused (412), and an unconditional DELETE (no header) still deletes.
  defp delete_object(conn, agent, key) do
    if_match = Plug.Conn.get_req_header(conn, "if-match")
    current = Agent.get(agent, &Map.get(&1.objects, key))

    cond do
      if_match == [] ->
        Agent.update(agent, &%{&1 | objects: Map.delete(&1.objects, key)})
        Plug.Conn.send_resp(conn, 204, "")

      current != nil and hd(if_match) == etag(current) ->
        Agent.update(agent, &%{&1 | objects: Map.delete(&1.objects, key)})
        Plug.Conn.send_resp(conn, 204, "")

      true ->
        # Absent object or a mismatched etag: the precondition failed, exactly as S3 answers a
        # conditional DELETE whose If-Match does not name the current object.
        Plug.Conn.send_resp(conn, 412, "")
    end
  end

  # ListObjectsV2. Only `prefix` and the paging pair are modelled — that is all `purge_shard/1`
  # and `stored_usage/0` send. Keys are returned SORTED so paging is deterministic; the page size
  # is small on purpose so the continuation-token loop is actually exercised rather than always
  # completing in one shot (a one-page fake would leave `next_token/1` untested forever).

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

  defp etag(%{form: :multipart, parts: parts}),
    do: ~s(") <> md5hex(IO.iodata_to_binary(parts)) <> "-#{length(parts)}" <> ~s(")

  # An object staged by hand as `form: :multipart` with no part list is a one-part upload.
  defp etag(%{body: body, form: :multipart}),
    do: ~s(") <> md5hex(:crypto.hash(:md5, body)) <> ~s(-1")

  # `Plug.Conn.read_body/1` answers `{:more, ...}` past its default 8 MB, and the large-object
  # touch tests PUT bodies of that order.
  defp read_full_body(conn, acc) do
    case Plug.Conn.read_body(conn) do
      {:ok, chunk, conn} -> {:ok, acc <> chunk, conn}
      {:more, chunk, conn} -> read_full_body(conn, acc <> chunk)
    end
  end

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
