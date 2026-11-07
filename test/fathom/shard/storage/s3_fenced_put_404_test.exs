defmodule Fathom.Shard.Storage.S3FencedPut404Test do
  @moduledoc """
  Expert review 2026-10-12 #22. AWS S3 may answer a PUT carrying `If-Match` against a key that no
  longer exists with 404 NoSuchKey rather than 412. `flush/6` mapped only 412 to `:superseded`, so
  a fenced flush against a purged object surfaced `{:s3_put_status, 404}` — a generic transient
  error — instead of the self-fence/reconcile verdict. The invariant: a fenced (If-Match) PUT
  answered 404 is `:superseded`; a create-only (If-None-Match) PUT answered 404 (missing bucket)
  stays a plain error. The boot probe accepts 412, or 404 only with a `NoSuchKey` error document
  (the runtime's own predicate), for a fenced PUT to a missing key, and refuses anything else.

  The in-process doubles (`Storage.Local`, hence `FaultyStorage`) already model the missing-object
  fenced flush as `:superseded` (`expected_etag != current`), so this S3-wire case is the one only
  a Req plug can express.
  """
  use ExUnit.Case, async: false

  alias Fathom.Shard.Storage.S3
  alias Fathom.Test.S3EtagStore

  setup do
    prev_enc = Application.get_env(:fathom, :shard_object_encoding)
    Application.put_env(:fathom, :shard_object_encoding, :none)
    prev = Application.get_env(:fathom, S3)

    on_exit(fn ->
      if prev_enc,
        do: Application.put_env(:fathom, :shard_object_encoding, prev_enc),
        else: Application.delete_env(:fathom, :shard_object_encoding)

      if prev,
        do: Application.put_env(:fathom, S3, prev),
        else: Application.delete_env(:fathom, S3)
    end)

    tmp = Path.join(System.tmp_dir!(), "s3_404_#{System.unique_integer([:positive])}.db")
    File.write!(tmp, "body")
    on_exit(fn -> File.rm(tmp) end)
    %{tmp: tmp}
  end

  defp put_s3_config(plug) do
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

  # Expert review 2026-11-07 #11 tightened this: these two used to answer a BARE 404 (empty body)
  # and expect `:superseded`, which pinned the conflation of NoSuchKey with NoSuchBucket. A fenced
  # PUT answered 404 is `:superseded` only when the error document says the KEY is gone; a missing
  # bucket must not mass-self-fence every shard that writes.
  test "a fenced flush answered 404 NoSuchKey is :superseded", %{tmp: tmp} do
    put_s3_config(fn conn ->
      Plug.Conn.send_resp(conn, 404, S3EtagStore.error_body("NoSuchKey"))
    end)

    assert {:error, :superseded} = S3.flush("shard_x", tmp, "\"etag-1\"")
  end

  test "a fenced flush answered 404 NoSuchBucket is a plain error, NOT a self-fence", %{tmp: tmp} do
    put_s3_config(fn conn ->
      Plug.Conn.send_resp(conn, 404, S3EtagStore.error_body("NoSuchBucket"))
    end)

    assert {:error, {:s3_put_status, 404}} = S3.flush("shard_x", tmp, "\"etag-1\"")
  end

  test "a fenced flush answered a bare 404 is not trusted to mean NoSuchKey", %{tmp: tmp} do
    put_s3_config(fn conn -> Plug.Conn.send_resp(conn, 404, "") end)
    assert {:error, {:s3_put_status, 404}} = S3.flush("shard_x", tmp, "\"etag-1\"")
  end

  test "a create-only flush answered 404 stays a plain error", %{tmp: tmp} do
    put_s3_config(fn conn ->
      Plug.Conn.send_resp(conn, 404, S3EtagStore.error_body("NoSuchKey"))
    end)

    assert {:error, {:s3_put_status, 404}} = S3.flush("shard_x", tmp, nil)
  end

  # Expert review 2026-11-07 #11 follow-up: the probe accepted ANY 404 here, but the runtime maps
  # a fenced-PUT 404 to a self-fence verdict only for NoSuchKey. A store answering a bare or
  # differently-coded 404 booted healthy and its deposed owners never self-fenced. The first test
  # here previously answered a BARE 404 and expected boot to pass — it pinned the defect.
  describe "boot probe: If-Match on a missing key" do
    defp absent_put?(conn),
      do: conn.method == "PUT" and String.ends_with?(conn.request_path, ".absent")

    test "accepts a store that answers 404 NoSuchKey (AWS)" do
      store = start_supervised!({Agent, fn -> S3EtagStore.initial(%{}) end})

      put_s3_config(fn conn ->
        if absent_put?(conn),
          do: Plug.Conn.send_resp(conn, 404, S3EtagStore.error_body("NoSuchKey")),
          else: S3EtagStore.serve(conn, store)
      end)

      assert S3.verify_conditional_writes!() == :ok
    end

    test "accepts a store that answers 412" do
      store = start_supervised!({Agent, fn -> S3EtagStore.initial(%{}) end})

      put_s3_config(fn conn ->
        if absent_put?(conn),
          do: Plug.Conn.send_resp(conn, 412, S3EtagStore.error_body("PreconditionFailed")),
          else: S3EtagStore.serve(conn, store)
      end)

      assert S3.verify_conditional_writes!() == :ok
    end

    test "refuses a store that answers a bare 404 (runtime would not self-fence)" do
      store = start_supervised!({Agent, fn -> S3EtagStore.initial(%{}) end})

      put_s3_config(fn conn ->
        if absent_put?(conn),
          do: Plug.Conn.send_resp(conn, 404, ""),
          else: S3EtagStore.serve(conn, store)
      end)

      assert_raise RuntimeError, ~r/If-Match on a missing key.*WITHOUT a <Code>NoSuchKey/s, fn ->
        S3.verify_conditional_writes!()
      end
    end

    test "refuses a store that answers 404 with a different code (NoSuchBucket)" do
      store = start_supervised!({Agent, fn -> S3EtagStore.initial(%{}) end})

      put_s3_config(fn conn ->
        if absent_put?(conn),
          do: Plug.Conn.send_resp(conn, 404, S3EtagStore.error_body("NoSuchBucket")),
          else: S3EtagStore.serve(conn, store)
      end)

      assert_raise RuntimeError, ~r/If-Match on a missing key.*WITHOUT a <Code>NoSuchKey/s, fn ->
        S3.verify_conditional_writes!()
      end
    end

    test "refuses a store that lets a fenced PUT create a missing key" do
      store = start_supervised!({Agent, fn -> S3EtagStore.initial(%{}) end})

      put_s3_config(fn conn ->
        if conn.method == "PUT" and String.ends_with?(conn.request_path, ".absent"),
          do: Plug.Conn.send_resp(conn, 200, ""),
          else: S3EtagStore.serve(conn, store)
      end)

      assert_raise RuntimeError, ~r/If-Match on a missing key/, fn ->
        S3.verify_conditional_writes!()
      end
    end
  end
end
