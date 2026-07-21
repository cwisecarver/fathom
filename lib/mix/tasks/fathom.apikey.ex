defmodule Mix.Tasks.Fathom.Apikey do
  @shortdoc "Mint / list / revoke scoped control-plane API keys (#8)"
  @moduledoc """
  Scoped, revocable control-plane API keys for `/api` (expert review #8).

      mix fathom.apikey mint <name> [--scope read|manage|destroy]   # default scope: read
      mix fathom.apikey list
      mix fathom.apikey revoke <id>

  **mint** prints the plaintext token ONCE — only its hash is stored, so copy it now. Present it as
  `Authorization: Bearer <token>` to `/api`. `read` grants the inspective GETs; `manage` adds
  create/fork/suspend/token operations; `destroy` adds the irreversible ones (delete, restore,
  drop-snapshot). Revoking takes effect on the next request (no restart).
  """
  use Mix.Task

  alias Fathom.ApiKeys

  @requirements ["app.start"]

  @impl Mix.Task
  def run(argv) do
    {opts, rest, _} = OptionParser.parse(argv, strict: [scope: :string])

    case rest do
      ["mint", name] -> mint(name, opts[:scope] || "read")
      ["list"] -> list()
      ["revoke", id] -> revoke(id)
      _ -> usage()
    end
  end

  defp mint(name, scope) do
    case ApiKeys.mint(name, scope) do
      {:ok, token, key} ->
        Mix.shell().info("Minted API key ##{key.id} \"#{key.name}\" (scope: #{key.scope}).")
        Mix.shell().info("Token (shown once — store it now):\n\n  #{token}\n")
        Mix.shell().info("Use it as:  Authorization: Bearer #{token}")

      {:error, changeset} ->
        Mix.shell().error("Failed to mint key: #{inspect(changeset.errors)}")
        exit({:shutdown, 1})
    end
  end

  defp list do
    case ApiKeys.list() do
      [] ->
        Mix.shell().info(
          "No API keys. Mint one with: mix fathom.apikey mint <name> --scope <scope>"
        )

      keys ->
        Mix.shell().info("id  scope    revoked  name")

        Enum.each(keys, fn k ->
          revoked = if k.revoked_at, do: "yes", else: "no "
          Mix.shell().info("#{pad(k.id, 3)} #{pad(k.scope, 8)} #{revoked}      #{k.name}")
        end)
    end
  end

  defp revoke(id) do
    case ApiKeys.revoke(id) do
      {:ok, key} -> Mix.shell().info("Revoked API key ##{key.id} \"#{key.name}\".")
      {:error, :not_found} -> Mix.shell().error("No API key with id #{id}.")
      {:error, changeset} -> Mix.shell().error("Failed to revoke: #{inspect(changeset.errors)}")
    end
  end

  defp pad(v, n), do: String.pad_trailing(to_string(v), n)

  defp usage do
    Mix.shell().error("""
    usage:
      mix fathom.apikey mint <name> [--scope read|manage|destroy]
      mix fathom.apikey list
      mix fathom.apikey revoke <id>
    """)
  end
end
