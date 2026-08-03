defmodule Fathom.Shard.Storage.ContractS3Test do
  @moduledoc """
  `Fathom.Test.StorageContract` against the S3 backend, driven through the `S3EtagStore` double
  (content-derived etags, real conditional-PUT/copy semantics) rather than MinIO — so it runs in
  the default suite with no credentials.

  This is the half that matters for expert review 2026-08-01 #30: the contract assertions are
  identical to `ContractLocalTest`'s, so a divergence between the two backends' MEANING shows up
  as a failure here instead of as a production incident the rig finds later (#24).
  """
  use ExUnit.Case, async: false

  alias Fathom.Shard.Storage.S3
  alias Fathom.Test.S3EtagStore

  setup do
    store = start_supervised!({Agent, fn -> S3EtagStore.initial(%{}) end})
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
