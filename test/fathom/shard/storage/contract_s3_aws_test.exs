defmodule Fathom.Shard.Storage.ContractS3AwsTest do
  @moduledoc """
  `Fathom.Test.StorageContract` against the S3 backend with the `S3EtagStore` double answering the
  way AWS does rather than the way MinIO does: a conditional PUT (`If-Match`) against a key that no
  longer exists is 404 `NoSuchKey`, not 412 (expert review 2026-11-07 #11).

  The contract is identical to `ContractS3Test`'s, so a backend that only understands MinIO's
  status codes fails here, in the default suite, instead of in a deposed owner that never
  self-fences. The suite predates the distinction and the default double still answers 412, which is
  why this is a second run and not a change to the first.
  """
  use ExUnit.Case, async: false

  alias Fathom.Shard.Storage.S3
  alias Fathom.Test.S3EtagStore

  setup do
    store = start_supervised!({Agent, fn -> S3EtagStore.initial(%{}) end})
    S3EtagStore.set_missing_if_match_status(store, 404)
    prev = Application.get_env(:fathom, S3)

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

    :ok
  end

  use Fathom.Test.StorageContract, backend: S3
end
