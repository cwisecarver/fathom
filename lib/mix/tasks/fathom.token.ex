defmodule Mix.Tasks.Fathom.Token do
  @shortdoc "Mints a Hrana bearer token for one shard"

  @moduledoc """
  Mints a bearer token granting Hrana access to one shard (see `Fathom.HranaAuth`):

      mix fathom.token acme

  Clients present it as libSQL's `authToken`. Signing uses the dedicated
  `:hrana_token_secret` (or `secret_key_base` when unset), so mint with the
  environment whose secret the target node runs under (a token minted against the
  dev secret won't verify in prod). In a release (no Mix), mint from the remote
  console instead: `Fathom.HranaAuth.token_for("acme")`. Revoke a shard's tokens
  with `Fathom.HranaAuth.revoke("acme")`.
  """

  use Mix.Task

  @impl true
  def run(args) do
    # Loads the app's config (the endpoint secret) without starting the app —
    # SIGNING needs no running processes. But the embedded revocation version does
    # (expert review round-2 #31): without the Repo, token_version raised and the
    # rescue defaulted to v=1, so a shard revoked to floor ≥ 2 got a DEAD-ON-ARRIVAL
    # token printed as success. Start just the Repo (no listeners, no heartbeat
    # side effects) and verify the floor is readable before minting.
    Mix.Task.run("app.config")

    case args do
      [shard_id] ->
        start_repo!()
        check_floor_readable!(shard_id)

        case Fathom.HranaAuth.token_for(shard_id) do
          {:ok, token} -> Mix.shell().info(token)
          {:error, :invalid_shard_id} -> Mix.raise("invalid shard id: #{inspect(shard_id)}")
        end

      _ ->
        Mix.raise("usage: mix fathom.token <shard_id>")
    end
  end

  defp start_repo! do
    {:ok, _} = Application.ensure_all_started(:ecto_sql)

    case Fathom.Repo.start_link() do
      {:ok, _} -> :ok
      {:error, {:already_started, _}} -> :ok
      {:error, reason} -> refuse(reason)
    end
  end

  # Repo.start_link succeeds even with Postgres down (lazy connections), so probe
  # the actual read the mint depends on. A loud refusal beats a maybe-dead token.
  defp check_floor_readable!(shard_id) do
    case Fathom.ShardId.cast(shard_id) do
      {:ok, canonical} ->
        _ = Fathom.Directory.token_version(canonical)
        :ok

      # token_for reports invalid ids with its own message.
      :error ->
        :ok
    end
  rescue
    e -> refuse(e)
  catch
    :exit, reason -> refuse(reason)
  end

  defp refuse(reason) do
    Mix.raise(
      "cannot read the shard's revocation floor (Postgres unreachable: " <>
        "#{inspect(reason)}) — a token minted blind would embed v=1 and be dead on " <>
        "arrival for any previously-revoked shard. Fix the DATABASE_URL/connection, " <>
        "or mint from a running node's console: Fathom.HranaAuth.token_for(\"<shard>\")"
    )
  end
end
