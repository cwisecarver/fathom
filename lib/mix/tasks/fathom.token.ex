defmodule Mix.Tasks.Fathom.Token do
  @shortdoc "Mints a Hrana bearer token for one shard"

  @moduledoc """
  Mints a bearer token granting Hrana access to one shard (see `Fathom.HranaAuth`):

      mix fathom.token acme

  Clients present it as libSQL's `authToken`. Signing uses the configured
  `secret_key_base`, so mint with the environment whose secret the target node
  runs under (a token minted against the dev secret won't verify in prod). In a
  release (no Mix), mint from the remote console instead:
  `Fathom.HranaAuth.token_for("acme")`.
  """

  use Mix.Task

  @impl true
  def run(args) do
    # Loads the app's config (the endpoint secret) without starting the app —
    # signing needs no running processes.
    Mix.Task.run("app.config")

    case args do
      [shard_id] ->
        case Fathom.HranaAuth.token_for(shard_id) do
          {:ok, token} -> Mix.shell().info(token)
          {:error, :invalid_shard_id} -> Mix.raise("invalid shard id: #{inspect(shard_id)}")
        end

      _ ->
        Mix.raise("usage: mix fathom.token <shard_id>")
    end
  end
end
