defmodule Fathom.Bench.S3Env do
  @moduledoc """
  The `FATHOM_S3_TEST_*` opt-in shared by `Fathom.Bench` and `Fathom.Rpo`
  (expert review 2026-11-06 #17: it was copy-pasted in both).

  S3 is opt-in only when `FATHOM_S3_TEST_ENDPOINT` is explicitly set, which keeps `mix fathom.bench`,
  `mix fathom.rpo` and the commit gate S3-free and offline-runnable by default. The defaults below
  point at the local MinIO the test suite uses.
  """

  @doc "True when an S3 endpoint is explicitly configured via `FATHOM_S3_TEST_ENDPOINT`."
  @spec opt_in?() :: boolean()
  def opt_in?, do: System.get_env("FATHOM_S3_TEST_ENDPOINT") != nil

  @doc """
  `Fathom.Shard.Storage.S3` config from the `FATHOM_S3_TEST_*` variables, with every object key
  under `prefix` (each harness keeps its own, so their objects never collide).
  """
  @spec config(String.t()) :: keyword()
  def config(prefix) do
    [
      bucket: System.get_env("FATHOM_S3_TEST_BUCKET", "fathom-shards-test"),
      region: System.get_env("FATHOM_S3_TEST_REGION", "us-east-1"),
      endpoint: System.get_env("FATHOM_S3_TEST_ENDPOINT", "http://localhost:9100"),
      path_style: true,
      prefix: prefix,
      access_key_id: System.get_env("FATHOM_S3_TEST_ACCESS_KEY", "fathomtest"),
      secret_access_key: System.get_env("FATHOM_S3_TEST_SECRET_KEY", "fathomtest123")
    ]
  end
end
