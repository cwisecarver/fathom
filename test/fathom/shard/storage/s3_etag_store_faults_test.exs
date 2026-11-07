defmodule Fathom.Shard.Storage.S3EtagStoreFaultsTest do
  @moduledoc """
  Pins the fault capabilities of `Fathom.Test.S3EtagStore` (expert review 2026-11-07,
  test-harness-fidelity #5 / s3-storage #7) so the double cannot drift under the tests built on it.

  A double that quietly stops applying an "apply-then-fail" write turns every test built on it into
  a happy-path test that still passes: the lease-ambiguity tests (`S3LeaseAmbiguityTest`,
  `ShardAcquireLandsThenErrorsTest`) assert their own fault fired, and THIS file asserts that firing
  means what it says.

  The double used to express only failures that happen BEFORE a write applies, so a lock PUT that
  LANDED with its reply lost (Req never retries a PUT; a reset connection and a 5xx from a store that
  applied it are both real) could not be staged at all.
  """
  use ExUnit.Case, async: true

  alias Fathom.Test.S3EtagStore

  setup do
    store = start_supervised!({Agent, fn -> S3EtagStore.initial(%{}) end})

    req =
      Req.new(
        base_url: "http://s3.test",
        plug: fn conn -> S3EtagStore.serve(conn, store) end,
        retry: false
      )

    %{store: store, req: req}
  end

  defp put(req, key, body, headers \\ []),
    do: Req.put(req, url: "/b/" <> key, body: body, headers: headers)

  test "apply: true — the write LANDS and the caller is told it was lost", %{
    store: store,
    req: req
  } do
    id =
      S3EtagStore.inject(store, %{method: "PUT", key: "k.lock"}, {:transport_error, :timeout},
        apply: true
      )

    assert {:error, %Req.TransportError{reason: :timeout}} = put(req, "k.lock", "v1")

    assert S3EtagStore.fired(store, id) == 1, "the fault never fired: this tested the happy path"
    assert S3EtagStore.body_of(store, "k.lock") == "v1", "apply: true must perform the write"
  end

  test "apply: false — the request is refused and changes NOTHING", %{store: store, req: req} do
    id = S3EtagStore.inject(store, %{method: "PUT", key: "k.lock"}, {:status, 503})

    assert {:ok, %{status: 503}} = put(req, "k.lock", "v1")

    assert S3EtagStore.fired(store, id) == 1
    assert S3EtagStore.body_of(store, "k.lock") == nil
  end

  test "a fault fires `times` times and then the store behaves", %{store: store, req: req} do
    id = S3EtagStore.inject(store, %{method: "PUT"}, {:status, 500}, times: 2)

    assert {:ok, %{status: 500}} = put(req, "a", "1")
    assert {:ok, %{status: 500}} = put(req, "a", "1")
    assert {:ok, %{status: 200}} = put(req, "a", "1")
    assert S3EtagStore.fired(store, id) == 2
  end

  test "a fault is scoped by method, key and condition", %{store: store, req: req} do
    id =
      S3EtagStore.inject(
        store,
        %{method: "PUT", key: "k.lock", cond: :if_none_match},
        {:status, 503},
        times: :infinity
      )

    assert {:ok, %{status: 200}} = put(req, "other.lock", "x", [{"if-none-match", "*"}])
    assert {:ok, %{status: 200}} = put(req, "k.lock", "x")
    assert {:ok, %{status: 200}} = Req.get(req, url: "/b/k.lock")
    assert S3EtagStore.fired(store, id) == 0

    S3EtagStore.clear_faults(store)
    assert {:ok, %{status: 412}} = put(req, "k.lock", "y", [{"if-none-match", "*"}])
  end

  test "`after:` arms a fault only once an earlier one has fired", %{store: store, req: req} do
    first = S3EtagStore.inject(store, %{method: "PUT", key: "k.lock"}, {:status, 500})

    second =
      S3EtagStore.inject(store, %{method: "GET", key: "k.lock", after: first}, {:status, 403},
        times: :infinity
      )

    assert {:ok, %{status: 200}} = put(req, "other", "x")

    assert {:ok, %{status: 404}} = Req.get(req, url: "/b/k.lock"),
           "armed before its trigger fired"

    assert {:ok, %{status: 500}} = put(req, "k.lock", "x")
    assert {:ok, %{status: 403}} = Req.get(req, url: "/b/k.lock")
    assert S3EtagStore.fired(store, second) == 1
  end

  describe "a conditional PUT against a key that is not there" do
    test "answers 412 by default (MinIO)", %{req: req} do
      assert {:ok, %{status: 412}} = put(req, "gone.lock", "x", [{"if-match", ~s("e")}])
    end

    test "answers 404 NoSuchKey when the store is told to be AWS", %{store: store, req: req} do
      S3EtagStore.set_missing_if_match_status(store, 404)

      assert {:ok, %{status: 404, body: body}} =
               put(req, "gone.lock", "x", [{"if-match", ~s("e")}])

      assert body =~ "<Code>NoSuchKey</Code>"
      assert S3EtagStore.body_of(store, "gone.lock") == nil, "a refused PUT must not create"
    end

    test "a key that EXISTS at another etag is 412 in both modes", %{store: store, req: req} do
      S3EtagStore.set_body(store, "here.lock", "v1")
      S3EtagStore.set_missing_if_match_status(store, 404)

      assert {:ok, %{status: 412}} = put(req, "here.lock", "v2", [{"if-match", ~s("stale")}])
      assert S3EtagStore.body_of(store, "here.lock") == "v1"
    end
  end

  test "error_body/2 is an AWS error document whose Code tells a missing key from a missing bucket" do
    assert S3EtagStore.error_body("NoSuchKey") =~ "<Code>NoSuchKey</Code>"
    assert S3EtagStore.error_body("NoSuchBucket") =~ "<Code>NoSuchBucket</Code>"
  end
end
