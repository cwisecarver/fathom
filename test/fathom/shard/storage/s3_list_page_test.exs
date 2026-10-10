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
      assert {:error, {:list_xml, _}} = ListPage.parse("<a><b></a>")
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
    # reported to the test as {:deleted, decoded_key}.
    defp serve(test_pid, pages) do
      plug = fn conn ->
        conn = Plug.Conn.fetch_query_params(conn)

        case conn.method do
          "DELETE" ->
            key = conn.request_path |> String.replace_prefix("/b/", "") |> URI.decode()
            send(test_pid, {:deleted, key})
            Plug.Conn.send_resp(conn, 204, "")

          "GET" ->
            token = conn.query_params["continuation-token"]
            send(test_pid, {:listed, token})
            Plug.Conn.send_resp(conn, 200, Map.fetch!(pages, token))
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
end
