defmodule Fathom.Shard.Storage.S3MultipartHygieneTest do
  @moduledoc """
  The large-object steal-touch's multipart fence and hygiene (expert review 2026-10-13 #3, #21).

  ## #3 — CompleteMultipartUpload had no If-Match

  `split_copy_touch` fences both `UploadPartCopy` calls on the touched etag, but its Complete sent
  no precondition. A zombie's late `If-Match: E0` flush that lands after a part was read and before
  the Complete (E0 is still current, so the store accepts it) was then published OVER by the
  touch's stale bytes at a new etag: the zombie's acked flush erased from the object the stealer
  serves. The Complete now carries `If-Match: E0`; a mid-touch write makes it 412/404, mapped to
  `:touch_precondition`, so `touch_object` re-HEADs and re-splits against the zombie's bytes.

  The boot probe also never exercised the split path's primitives. It now runs a single-small-part
  multipart self-copy and refuses to boot on a store that ignores `x-amz-copy-source-if-match`,
  `x-amz-copy-source-range`, or `If-Match` on Complete.

  ## #21 — incomplete multipart uploads leaked and survived erasure

  A kill between create and complete leaves server-side copies of the shard's data in an open
  upload that ListObjectsV2 cannot see, so `purge_shard` never removed it. It now lists and aborts
  the uploads under the shard's key prefix (same delimiter rule as objects).

  Driven against `Fathom.Test.S3EtagStore`, which enforces If-Match on Complete like S3 does.
  """
  use ExUnit.Case, async: false

  import ExUnit.CaptureLog

  alias Fathom.Shard
  alias Fathom.Shard.Storage
  alias Fathom.Shard.Storage.{HeartbeatCache, S3}
  alias Fathom.Test.S3EtagStore

  setup do
    shard = "mphyg_#{System.unique_integer([:positive])}"

    for k <- [:shard_storage, S3, :steal_margin_ms] do
      prev = Application.get_env(:fathom, k)

      on_exit(fn ->
        if is_nil(prev),
          do: Application.delete_env(:fathom, k),
          else: Application.put_env(:fathom, k, prev)
      end)
    end

    Application.put_env(:fathom, :steal_margin_ms, 0)
    HeartbeatCache.reset()

    on_exit(fn ->
      for f <- Path.wildcard(Path.join(Shard.data_dir(), "#{shard}*")), do: File.rm_rf(f)
      for f <- Path.wildcard(Path.join(System.tmp_dir!(), "mphyg_#{shard}*")), do: File.rm_rf(f)
    end)

    %{shard: shard}
  end

  defp use_s3!(plug) do
    Application.put_env(:fathom, S3,
      bucket: "b",
      region: "us-east-1",
      access_key_id: "k",
      secret_access_key: "s",
      endpoint: "https://s3.example",
      path_style: true,
      req_plug: plug
    )

    Application.put_env(:fathom, :shard_storage, S3)
  end

  defp start_store(objects \\ %{}) do
    start_supervised!({Agent, fn -> S3EtagStore.initial(objects) end})
  end

  defp tmp_file(shard, bytes) do
    path = Path.join(System.tmp_dir!(), "mphyg_#{shard}_#{System.unique_integer([:positive])}")
    File.write!(path, bytes)
    path
  end

  # Random bytes: incompressible, so the STORED object stays above the 6 MiB split-touch floor.
  defp large_payload, do: :crypto.strong_rand_bytes(7 * 1024 * 1024 + 123)

  defp expire_holder!(store, shard) do
    {:ok, lease} = Storage.decode_lease(S3EtagStore.body_of(store, "#{shard}.lock"))
    past = Storage.now_ms() - 120_000

    S3EtagStore.set_body(
      store,
      "#{shard}.lock",
      Storage.encode_lease(%{lease | expires_at_ms: past})
    )

    S3EtagStore.set_body(
      store,
      "heartbeats/" <> URI.encode_www_form(lease.owner),
      Storage.encode_heartbeat(%{owner: lease.owner, expires_at_ms: past})
    )

    HeartbeatCache.reset()
  end

  defp steal!(store, shard, owner) do
    expire_holder!(store, shard)
    assert {:ok, %{took_over: true} = lease} = S3.acquire_lease(shard, owner, 30_000)
    lease
  end

  defp complete?(conn), do: conn.method == "POST" and conn.query_string =~ "uploadId"

  describe "#3: the Complete is fenced" do
    # Symptom: the zombie's flush landed between the touch's part copies and its Complete, and
    # the touch published its stale parts over it. The hook below lands that flush (a write that
    # replaces E0) immediately before the FIRST Complete reaches the store.
    test "a flush landing between the part copies and the Complete is not erased", %{shard: shard} do
      store = start_store()
      armed = :atomics.new(1, [])
      zombie_bytes = large_payload()

      use_s3!(fn conn ->
        if complete?(conn) and :atomics.add_get(armed, 1, 1) == 1 do
          S3EtagStore.set_body(store, "#{shard}.db", zombie_bytes)
        end

        S3EtagStore.serve(conn, store)
      end)

      {:ok, _} = S3.acquire_lease(shard, "z@n1#1", 30_000)
      {:ok, _e0, _} = S3.flush(shard, tmp_file(shard, large_payload()), nil)

      a = steal!(store, shard, "a@n2#1")

      assert :atomics.get(armed, 1) >= 2,
             "the Complete was never retried after the mid-touch write (fence not exercised)"

      assert S3EtagStore.plain_body_of(store, "#{shard}.db") == zombie_bytes,
             "the touch's stale parts were published over the zombie's flush"

      assert a.touch_post_etag == S3EtagStore.etag_of(store, "#{shard}.db")
      assert S3EtagStore.open_uploads(store) == [], "the refused attempt left its upload open"
    end

    test "a Complete refused on every attempt fails the steal closed and aborts the upload",
         %{shard: shard} do
      store = start_store()

      use_s3!(fn conn ->
        if complete?(conn) do
          # Something rewrites the object under every attempt.
          S3EtagStore.set_body(store, "#{shard}.db", large_payload())
        end

        S3EtagStore.serve(conn, store)
      end)

      {:ok, _} = S3.acquire_lease(shard, "z@n1#1", 30_000)
      {:ok, _, _} = S3.flush(shard, tmp_file(shard, large_payload()), nil)
      expire_holder!(store, shard)

      assert {:error, _} = S3.acquire_lease(shard, "a@n2#1", 30_000)
      assert S3EtagStore.open_uploads(store) == []
    end

    test "the Complete request carries the touched etag as If-Match", %{shard: shard} do
      store = start_store()
      seen = :ets.new(:seen, [:public, :bag])

      use_s3!(fn conn ->
        if complete?(conn),
          do: :ets.insert(seen, {:if_match, Plug.Conn.get_req_header(conn, "if-match")})

        S3EtagStore.serve(conn, store)
      end)

      {:ok, _} = S3.acquire_lease(shard, "z@n1#1", 30_000)
      {:ok, e0, _} = S3.flush(shard, tmp_file(shard, large_payload()), nil)
      steal!(store, shard, "a@n2#1")

      assert [{:if_match, [^e0]}] = :ets.lookup(seen, :if_match)
    end
  end

  describe "#3: the boot probe covers the split path's primitives" do
    test "passes against a store that enforces all three" do
      store = start_store()
      use_s3!(fn conn -> S3EtagStore.serve(conn, store) end)

      assert S3.verify_conditional_writes!() == :ok
      assert S3EtagStore.open_uploads(store) == [], "the probe left an upload open"
    end

    defp drop_header(conn, name),
      do: %{conn | req_headers: List.keydelete(conn.req_headers, name, 0)}

    test "refuses to boot when Complete ignores If-Match" do
      store = start_store()

      use_s3!(fn conn ->
        conn = if complete?(conn), do: drop_header(conn, "if-match"), else: conn
        S3EtagStore.serve(conn, store)
      end)

      assert_raise RuntimeError, ~r/CompleteMultipartUpload If-Match/, fn ->
        S3.verify_conditional_writes!()
      end

      assert S3EtagStore.open_uploads(store) == [], "a failed probe must still abort its upload"
    end

    test "refuses to boot when UploadPartCopy ignores x-amz-copy-source-if-match" do
      store = start_store()

      use_s3!(fn conn ->
        conn = drop_header(conn, "x-amz-copy-source-if-match")
        S3EtagStore.serve(conn, store)
      end)

      assert_raise RuntimeError, ~r/x-amz-copy-source-if-match/, fn ->
        S3.verify_conditional_writes!()
      end
    end

    test "refuses to boot when UploadPartCopy ignores x-amz-copy-source-range" do
      store = start_store()

      use_s3!(fn conn ->
        conn = drop_header(conn, "x-amz-copy-source-range")
        S3EtagStore.serve(conn, store)
      end)

      assert_raise RuntimeError, ~r/x-amz-copy-source-range/, fn ->
        S3.verify_conditional_writes!()
      end
    end
  end

  describe "#21: incomplete uploads" do
    defp create_upload!(store, key) do
      conn = Plug.Test.conn(:post, "/b/#{key}?uploads", "")
      resp = S3EtagStore.serve(conn, store)
      assert resp.status == 200
    end

    test "purge_shard aborts the shard's open uploads, and only that shard's", %{shard: shard} do
      store = start_store()
      use_s3!(fn conn -> S3EtagStore.serve(conn, store) end)

      # More uploads than one listing page (3), so the marker loop runs.
      for key <- List.duplicate("#{shard}.db", 5),
          do: create_upload!(store, key)

      sibling = "#{shard}2.db"
      create_upload!(store, sibling)
      assert length(S3EtagStore.open_uploads(store)) == 6

      assert :ok = S3.purge_shard(shard)

      assert S3EtagStore.open_uploads(store) |> Enum.map(&elem(&1, 0)) == [sibling],
             "purge must abort every upload of the shard and none of a sibling id's"
    end

    test "purge_shard fails closed when it cannot list uploads", %{shard: shard} do
      store = start_store()

      use_s3!(fn conn ->
        if conn.method == "GET" and conn.query_string =~ "uploads",
          do: Plug.Conn.send_resp(conn, 501, ""),
          else: S3EtagStore.serve(conn, store)
      end)

      assert {:error, {:s3_list_uploads_status, 501}} = S3.purge_shard(shard)
    end

    test "a failed abort of a refused touch is logged, not swallowed", %{shard: shard} do
      store = start_store()

      use_s3!(fn conn ->
        cond do
          complete?(conn) ->
            S3EtagStore.set_body(store, "#{shard}.db", large_payload())
            S3EtagStore.serve(conn, store)

          conn.method == "DELETE" and conn.query_string =~ "uploadId" ->
            Plug.Conn.send_resp(conn, 500, "")

          true ->
            S3EtagStore.serve(conn, store)
        end
      end)

      {:ok, _} = S3.acquire_lease(shard, "z@n1#1", 30_000)
      {:ok, _, _} = S3.flush(shard, tmp_file(shard, large_payload()), nil)
      expire_holder!(store, shard)

      log = capture_log(fn -> assert {:error, _} = S3.acquire_lease(shard, "a@n2#1", 30_000) end)
      assert log =~ "could not abort multipart upload"
    end
  end

  describe "UploadsPage" do
    alias Fathom.Shard.Storage.S3.UploadsPage

    test "parses uploads and the continuation markers" do
      xml =
        "<ListMultipartUploadsResult><IsTruncated>true</IsTruncated>" <>
          "<NextKeyMarker>a&amp;b.db</NextKeyMarker><NextUploadIdMarker>u2</NextUploadIdMarker>" <>
          "<Upload><Key>a&amp;b.db</Key><UploadId>u1</UploadId></Upload>" <>
          "<Upload><Key>c.db</Key><UploadId>u2</UploadId></Upload></ListMultipartUploadsResult>"

      assert {:ok,
              %{
                uploads: [%{key: "a&b.db", upload_id: "u1"}, %{key: "c.db", upload_id: "u2"}],
                next: {"a&b.db", "u2"}
              }} = UploadsPage.parse(xml)
    end

    test "a truncated page without markers is an error, not a short listing" do
      xml =
        "<ListMultipartUploadsResult><IsTruncated>true</IsTruncated></ListMultipartUploadsResult>"

      assert {:error, :uploads_truncated_without_marker} = UploadsPage.parse(xml)
    end

    test "a DTD is refused" do
      xml = "<!DOCTYPE x [<!ENTITY a \"b\">]><ListMultipartUploadsResult/>"
      assert {:error, :uploads_xml_dtd_refused} = UploadsPage.parse(xml)
    end
  end
end
