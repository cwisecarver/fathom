defmodule Fathom.Shard.Storage.S3TokenFloorCasTest do
  @moduledoc """
  Expert review 2026-11-07 #30 (s3-storage#3, isolation-security#8). `S3.put_token_floor/2` was one
  blind PUT, so two bumps reaching the store out of order (v3's PUT landing after v4's) LOWERED the
  durable revocation floor, and a Postgres PITR then restored the directory to the lowered value —
  a revoked token verified again. It is now GET → compare → conditional PUT (`If-Match` the etag
  read; `If-None-Match: *` when absent), re-reading on a lost race.

  The sequential monotonic property is pinned for BOTH backends in `Fathom.Test.StorageContract`.
  This file pins what only the S3 wire can express: the lost-race re-reads, driven through
  `S3EtagStore` faults (the double enforces the real conditional-PUT statuses, so a blind PUT would
  succeed where these expect the fence to hold).
  """
  use ExUnit.Case, async: false

  alias Fathom.Shard.Storage.S3
  alias Fathom.Test.S3EtagStore

  setup do
    prev = Application.get_env(:fathom, S3)
    store = start_supervised!({Agent, fn -> S3EtagStore.initial(%{}) end})

    Application.put_env(:fathom, S3,
      bucket: "b",
      region: "us-east-1",
      access_key_id: "k",
      secret_access_key: "s",
      endpoint: "https://s3.example",
      path_style: true,
      req_plug: fn conn -> S3EtagStore.serve(conn, store) end
    )

    on_exit(fn ->
      if prev,
        do: Application.put_env(:fathom, S3, prev),
        else: Application.delete_env(:fathom, S3)
    end)

    %{store: store, id: "floorcas_#{System.unique_integer([:positive])}"}
  end

  defp key(id), do: "tokenfloors/" <> id

  test "a lower floor never replaces a higher one, and writes nothing", %{store: store, id: id} do
    :ok = S3.put_token_floor(id, 6)
    etag = S3EtagStore.etag_of(store, key(id))
    assert is_binary(etag), "fixture: the floor object must exist"

    assert :ok = S3.put_token_floor(id, 5)
    assert S3EtagStore.body_of(store, key(id)) == "6"
    assert S3EtagStore.etag_of(store, key(id)) == etag, "an already-covered write must not PUT"
  end

  test "a creator that lost the race to a HIGHER value reads it back instead of overwriting",
       %{store: store, id: id} do
    # The reordering case: v6's writer got in between our GET (which saw nothing) and our PUT. A blind
    # PUT would store 5 over it. The first GET is faulted to a 404 while the object already exists, so
    # the `If-None-Match: *` create then fails 412, exactly as it would after a real interleaving.
    S3EtagStore.set_body(store, key(id), "6")

    fault =
      S3EtagStore.inject(store, %{method: "GET", key: key(id)}, {:status, 404}, times: 1)

    assert :ok = S3.put_token_floor(id, 5)

    assert S3EtagStore.fired(store, fault) == 1, "fixture: the stale read was never served"
    assert S3EtagStore.body_of(store, key(id)) == "6", "the floor was lowered by a lost race"
  end

  test "an If-Match PUT that loses to a concurrent bump re-reads and still raises the floor",
       %{store: store, id: id} do
    :ok = S3.put_token_floor(id, 3)

    # The first conditional PUT answers 412 (someone else moved the object first); the retry re-reads.
    fault =
      S3EtagStore.inject(store, %{method: "PUT", key: key(id), cond: :if_match}, {:status, 412},
        times: 1
      )

    assert :ok = S3.put_token_floor(id, 5)

    assert S3EtagStore.fired(store, fault) == 1
    assert S3EtagStore.body_of(store, key(id)) == "5"
  end

  test "AWS answers 404 NoSuchKey to an If-Match PUT on a key deleted under us: re-read, create",
       %{store: store, id: id} do
    :ok = S3.put_token_floor(id, 3)

    fault =
      S3EtagStore.inject(
        store,
        %{method: "PUT", key: key(id), cond: :if_match},
        {:status, 404, S3EtagStore.error_body("NoSuchKey")},
        times: 1
      )

    assert :ok = S3.put_token_floor(id, 5)

    assert S3EtagStore.fired(store, fault) == 1
    assert S3EtagStore.body_of(store, key(id)) == "5"
  end

  test "a store that keeps losing the race is an error, never a blind overwrite",
       %{store: store, id: id} do
    :ok = S3.put_token_floor(id, 3)

    S3EtagStore.inject(store, %{method: "PUT", key: key(id), cond: :if_match}, {:status, 412},
      times: :infinity
    )

    assert {:error, :token_floor_contended} = S3.put_token_floor(id, 5)
    assert S3EtagStore.body_of(store, key(id)) == "3"
  end

  test "an unparseable stored body is replaced (it reads as no floor), under If-Match",
       %{store: store, id: id} do
    S3EtagStore.set_body(store, key(id), "garbage")

    assert :ok = S3.put_token_floor(id, 4)
    assert S3EtagStore.body_of(store, key(id)) == "4"
  end
end
