defmodule Fathom.Shard.Storage.S3ListPageTest do
  @moduledoc """
  ListObjectsV2 parsing (expert review 2026-10-10 #25). The S3 backend scraped these responses with
  regexes, which (a) never decoded entities, so a key containing `&` came back as `&amp;` and a
  purge DELETEd a key that does not exist, and (b) ended the pagination loop with `:ok` whenever
  the continuation token went unseen, so `purge_shard` could report a tenant erased with later
  pages unvisited. These tests pin the parser, then the four paginated callers through the
  `:req_plug` seam (no network).
  """
  use ExUnit.Case, async: false

  alias Fathom.Shard.Storage.S3
  alias Fathom.Shard.Storage.S3.ListPage

  defp xml(inner, opts \\ []) do
    truncated = Keyword.get(opts, :truncated, false)
    token = Keyword.get(opts, :token)

    ~s(<?xml version="1.0" encoding="UTF-8"?>) <>
      ~s(<ListBucketResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">) <>
      "<Name>b</Name><Prefix></Prefix><KeyCount>1</KeyCount>" <>
      "<IsTruncated>#{truncated}</IsTruncated>" <>
      if(token, do: "<NextContinuationToken>#{token}</NextContinuationToken>", else: "") <>
      inner <> "</ListBucketResult>"
  end

  defp contents(key, size), do: "<Contents><Key>#{key}</Key><Size>#{size}</Size></Contents>"

  describe "ListPage.parse/1" do
    test "decodes entity-encoded keys (the regex scrape returned them verbatim)" do
      body =
        xml(
          contents("a&amp;b.db", 1) <>
            contents("lt&lt;x&gt;.db", 2) <>
            contents("q&quot;&apos;.db", 3) <>
            contents("num&#38;ref&#x26;.db", 4)
        )

      assert {:ok, %{entries: entries, next: nil}} = ListPage.parse(body)

      assert Enum.map(entries, & &1.key) == ["a&b.db", "lt<x>.db", "q\"'.db", "num&ref&.db"]
      assert Enum.map(entries, & &1.size) == [1, 2, 3, 4]
    end

    test "keeps non-ASCII keys intact (UTF-8 in, codepoints out)" do
      assert {:ok, %{entries: [%{key: "é/日本.db"}]}} =
               ListPage.parse(xml(contents("é/日本.db", 7)))
    end

    test "returns the continuation token entity-decoded, and ignores CommonPrefixes" do
      body =
        xml(
          "<CommonPrefixes><Prefix>dir/</Prefix></CommonPrefixes>" <> contents("k.db", 1),
          truncated: true,
          token: "tok&amp;en=="
        )

      assert {:ok, %{entries: [%{key: "k.db"}], next: "tok&en=="}} = ListPage.parse(body)
    end

    test "an empty, complete listing is ok with no entries" do
      assert {:ok, %{entries: [], next: nil}} = ListPage.parse(xml(""))
    end

    test "parses a response with no namespace" do
      body =
        "<ListBucketResult><IsTruncated>false</IsTruncated>#{contents("k.db", 5)}</ListBucketResult>"

      assert {:ok, %{entries: [%{key: "k.db", size: 5}]}} = ListPage.parse(body)
    end

    test "IsTruncated=true with NO token is an error, never a short complete listing" do
      # The invariant: stopping here would let a caller report success (purge: "erased") with
      # pages unvisited.
      assert {:error, :list_truncated_without_token} =
               ListPage.parse(xml(contents("k.db", 1), truncated: true))
    end

    test "malformed or truncated XML is an error" do
      assert {:error, {:list_xml, _}} = ListPage.parse("<ListBucketResult><Contents>")
      assert {:error, {:list_xml, _}} = ListPage.parse("<ListBucketResult><b></ListBucketResult>")
    end

    test "a DTD is refused (entity expansion is how a small body becomes a large one)" do
      evil =
        ~s(<?xml version="1.0"?><!DOCTYPE r [<!ENTITY a "AAAA"><!ENTITY b "&a;&a;&a;&a;">]>) <>
          "<ListBucketResult><Contents><Key>&b;</Key></Contents></ListBucketResult>"

      assert {:error, :list_xml_dtd_refused} = ListPage.parse(evil)
    end

    test "a non-binary body is an error, not a silently empty listing" do
      assert {:error, {:list_body_not_xml, _}} = ListPage.parse(%{"decoded" => "json"})
    end
  end

  describe "the paginated callers" do
    setup do
      prev = Application.get_env(:fathom, S3)
      test_pid = self()

      on_exit(fn ->
        if prev,
          do: Application.put_env(:fathom, S3, prev),
          else: Application.delete_env(:fathom, S3)
      end)

      %{test_pid: test_pid}
    end

    # `pages` maps continuation-token (nil for the first request) -> response body. DELETEs are
    # reported to the test as {:deleted, decoded_key}; each object LIST as {:listed, token} and
    # {:listed_prefix, prefix}. The `?uploads` listing (the purge's abort step) is answered with a real
    # empty ListMultipartUploadsResult: the parser now refuses any other root (expert review
    # 2026-11-07 #82), and an object-listing body there was only ever accepted by accident.
    defp serve(test_pid, pages) do
      plug = fn conn ->
        conn = Plug.Conn.fetch_query_params(conn)

        case conn.method do
          "DELETE" ->
            key = conn.request_path |> String.replace_prefix("/b/", "") |> URI.decode()
            send(test_pid, {:deleted, key})
            Plug.Conn.send_resp(conn, 204, "")

          "GET" ->
            if Map.has_key?(conn.query_params, "uploads") do
              Plug.Conn.send_resp(conn, 200, "<ListMultipartUploadsResult/>")
            else
              token = conn.query_params["continuation-token"]
              send(test_pid, {:listed, token})
              send(test_pid, {:listed_prefix, conn.query_params["prefix"]})
              Plug.Conn.send_resp(conn, 200, Map.fetch!(pages, token))
            end
        end
      end

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

    test "purge_shard visits every page and deletes entity-decoded keys",
         %{test_pid: pid} do
      serve(pid, %{
        nil =>
          xml(contents("acme.db", 1) <> contents("acme@snap-a&amp;b.db", 1),
            truncated: true,
            token: "T1"
          ),
        "T1" => xml(contents("acme.lock", 1) <> contents("acme2.db", 1))
      })

      assert :ok = S3.purge_shard("acme")

      assert_received {:listed, nil}
      assert_received {:listed, "T1"}

      # The decoded key - pre-fix the DELETE went to ".../acme@snap-a&amp;b.db", which does not exist.
      assert_received {:deleted, "acme.db"}
      assert_received {:deleted, "acme@snap-a&b.db"}
      assert_received {:deleted, "acme.lock"}
      refute_received {:deleted, "acme2.db"}
    end

    test "purge_shard does NOT report success when a truncated page has no token",
         %{test_pid: pid} do
      serve(pid, %{nil => xml(contents("acme.db", 1), truncated: true)})

      assert {:error, {:s3_list_parse, :list_truncated_without_token}} = S3.purge_shard("acme")
    end

    test "tombstoned_ids decodes keys and paginates", %{test_pid: pid} do
      serve(pid, %{
        nil => xml(contents("tombstones/a&amp;b", 0), truncated: true, token: "T1"),
        "T1" => xml(contents("tombstones/c", 0))
      })

      assert {:ok, ["a&b", "c"]} = S3.tombstoned_ids()
    end

    # expert review 2026-10-13 #20: the accumulator is now built by prepending each page's reversed ids
    # and reversing once. This pins the invariant that matters for the re-mint guard: every id from
    # every page is returned exactly once, in the listing's own order. The quadratic cost (repeated
    # `acc ++ ids`) is not observable from a result, so this test does NOT discriminate the old code
    # from the new; it guards the reordering against regressing the output.
    test "tombstoned_ids keeps every id from every page, in listing order", %{test_pid: pid} do
      serve(pid, %{
        nil =>
          xml(contents("tombstones/t1", 0) <> contents("tombstones/t2", 0),
            truncated: true,
            token: "T1"
          ),
        "T1" =>
          xml(contents("tombstones/t3", 0) <> contents("tombstones/t4", 0),
            truncated: true,
            token: "T2"
          ),
        "T2" => xml(contents("tombstones/t5", 0), truncated: true, token: "T3"),
        "T3" => xml(contents("tombstones/t6", 0) <> contents("tombstones/t7", 0))
      })

      assert {:ok, ["t1", "t2", "t3", "t4", "t5", "t6", "t7"]} = S3.tombstoned_ids()
      assert_received {:listed, nil}
      assert_received {:listed, "T3"}
    end

    test "tombstoned_ids errors on a truncated page with no token", %{test_pid: pid} do
      serve(pid, %{nil => xml(contents("tombstones/a", 0), truncated: true)})
      assert {:error, {:s3_list_parse, :list_truncated_without_token}} = S3.tombstoned_ids()
    end

    test "list_snapshots parses ids and sizes across pages", %{test_pid: pid} do
      serve(pid, %{
        nil => xml(contents("acme@snap-001.db", 11), truncated: true, token: "T1"),
        "T1" => xml(contents("acme@snap-002.db", 22) <> contents("acme@snap-x.lock", 3))
      })

      assert {:ok, [%{id: "002", bytes: 22}, %{id: "001", bytes: 11}]} =
               S3.list_snapshots("acme")
    end

    test "stored_usage tallies live .db objects across pages and errors when truncated blind",
         %{test_pid: pid} do
      serve(pid, %{
        nil => xml(contents("a.db", 10) <> contents("a@1.db", 99), truncated: true, token: "T1"),
        "T1" => xml(contents("b.db", 5) <> contents("b.lock", 1))
      })

      assert {2, 15} = S3.stored_usage()

      serve(pid, %{nil => xml(contents("a.db", 10), truncated: true)})
      assert {:error, {:s3_list_parse, :list_truncated_without_token}} = S3.stored_usage()
    end
  end

  describe "ListPage / UploadsPage refuse a document that is not their listing (expert review 2026-11-07 #82)" do
    alias Fathom.Shard.Storage.S3.UploadsPage

    test "an <Error>, an HTML page or a wrong root is an error, never an empty complete listing" do
      # Pre-fix each of these was `{:ok, %{entries: [], next: nil}}`: a broken proxy's 200 became
      # "the tenant has no objects" (purge: erased; any_shard_object?: false; tombstoned_ids: []).
      for body <- [
            "<Error><Code>AccessDenied</Code></Error>",
            "<html/>",
            ~s(<ListAllMyBucketsResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/"/>),
            "<ListMultipartUploadsResult/>"
          ] do
        assert {:error, {:list_unexpected_root, _}} = ListPage.parse(body), body
      end

      for body <- ["<Error/>", "<html/>", "<ListBucketResult/>"] do
        assert {:error, {:uploads_unexpected_root, _}} = UploadsPage.parse(body), body
      end
    end

    test "the real roots, with and without a namespace, still parse" do
      assert {:ok, %{entries: [], next: nil}} = ListPage.parse("<ListBucketResult/>")

      assert {:ok, %{entries: [], next: nil}} =
               ListPage.parse(
                 ~s(<ListBucketResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/"/>)
               )

      assert {:ok, %{uploads: [], next: nil}} = UploadsPage.parse("<ListMultipartUploadsResult/>")
    end

    # Not a property test (no StreamData in the project): every strict PREFIX of a valid page is
    # malformed XML and must be an error, and no single-byte corruption may raise. A parser that
    # exits on hostile input would kill the DeleteJob / the boot gate instead of failing the call.
    test "every truncation is an error and no byte flip raises" do
      body =
        xml(
          contents("acme.db", 1) <> contents("acme@v1.db", 2),
          truncated: true,
          token: "T1"
        )

      for n <- 0..(byte_size(body) - 1) do
        assert {:error, _} = ListPage.parse(binary_part(body, 0, n)), "prefix of #{n} bytes"
      end

      for i <- 0..(byte_size(body) - 1), flip <- [0xFF, 0x00, ?<] do
        head = binary_part(body, 0, i)
        tail = binary_part(body, i + 1, byte_size(body) - i - 1)

        assert match?(
                 {tag, _} when tag in [:ok, :error],
                 ListPage.parse(<<head::binary, flip, tail::binary>>)
               )
      end
    end
  end

  describe "purge_shard lists only the delimiter-qualified prefixes (expert review 2026-11-07 #61)" do
    # A tiny S3: the keys it holds, ListObjectsV2 by `prefix` with 1000-key pages and numeric
    # continuation tokens, and DELETE. Every LIST is reported to the test.
    defp fleet_store(test_pid, keys) do
      plug = fn conn ->
        conn = Plug.Conn.fetch_query_params(conn)

        case conn.method do
          "DELETE" ->
            key = conn.request_path |> String.replace_prefix("/b/", "") |> URI.decode()
            send(test_pid, {:deleted, key})
            Plug.Conn.send_resp(conn, 204, "")

          "GET" ->
            if Map.has_key?(conn.query_params, "uploads") do
              Plug.Conn.send_resp(conn, 200, "<ListMultipartUploadsResult/>")
            else
              prefix = conn.query_params["prefix"]
              send(test_pid, {:listed_prefix, prefix})
              start = String.to_integer(conn.query_params["continuation-token"] || "0")
              matching = keys |> Enum.filter(&String.starts_with?(&1, prefix)) |> Enum.sort()
              page = matching |> Enum.drop(start) |> Enum.take(1000)
              more? = start + 1000 < length(matching)

              body =
                "<ListBucketResult><IsTruncated>#{more?}</IsTruncated>" <>
                  if(more?,
                    do: "<NextContinuationToken>#{start + 1000}</NextContinuationToken>",
                    else: ""
                  ) <>
                  Enum.map_join(page, &"<Contents><Key>#{&1}</Key><Size>1</Size></Contents>") <>
                  "</ListBucketResult>"

              Plug.Conn.send_resp(conn, 200, body)
            end
        end
      end

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

    setup do
      prev = Application.get_env(:fathom, S3)

      on_exit(fn ->
        if prev,
          do: Application.put_env(:fathom, S3, prev),
          else: Application.delete_env(:fathom, S3)
      end)

      %{test_pid: self()}
    end

    test "erasing `acme` among 2,500 sibling ids lists two prefixes and deletes only its own keys",
         %{test_pid: pid} do
      siblings = for i <- 1..2_500, do: "acme#{String.pad_leading("#{i}", 4, "0")}.db"
      own = ["acme.db", "acme.lock", "acme@v1.db", "acme@snap-1.db"]
      fleet_store(pid, siblings ++ own ++ ["other.db"])

      assert :ok = S3.purge_shard("acme")

      messages = drain([])
      listed = for {:listed_prefix, p} <- messages, do: p
      # Pre-fix: the bare prefix `acme`, three pages (2,504 keys) of which 2,500 are discarded.
      assert listed == ["acme.", "acme@"], "LISTed #{inspect(listed)}"

      deleted = for {:deleted, k} <- messages, do: k
      assert Enum.sort(deleted) == Enum.sort(own)
    end

    defp drain(acc) do
      receive do
        msg -> drain([msg | acc])
      after
        0 -> Enum.reverse(acc)
      end
    end
  end
end
