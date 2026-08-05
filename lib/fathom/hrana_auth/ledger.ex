defmodule Fathom.HranaAuth.Ledger do
  @moduledoc """
  The issuance ledger for Hrana tokens (expert review 2026-08-01 #37).

  The per-shard lifecycle was complete — mint, zero-downtime `rotate/1`, immediate `revoke/1`, a
  read-only scope — but nothing above one shard: no record of which tokens were ever issued, so
  "what is outstanding?" had no answer and the only fleet-wide lever was rotating
  `secret_key_base`, which invalidates every tenant's token simultaneously (an outage, not a
  revocation).

  Two properties this deliberately keeps:

    * **Recording never fails a mint.** `record/1` rescues everything. `mix fathom.token` runs with
      config only and no `Repo` at all, and a Postgres blip must not stop an operator issuing a
      credential. An incomplete ledger under-reports what is outstanding, which is the safe
      direction: the bulk revoke built on it then revokes LESS than it might, never more.
    * **Claims only, never the secret.** See `Fathom.HranaAuth.Issuance`.
  """
  import Ecto.Query

  require Logger

  alias Fathom.HranaAuth.Issuance
  alias Fathom.Repo

  @doc """
  Record a mint. Best-effort by design — see the moduledoc. Returns `:ok` regardless so no caller
  is tempted to treat a ledger outage as a mint failure.
  """
  @spec record(keyword()) :: :ok
  def record(attrs) do
    %Issuance{}
    |> Issuance.changeset(%{
      shard_id: Keyword.fetch!(attrs, :shard_id),
      token_version: Keyword.fetch!(attrs, :token_version),
      scope: to_string(Keyword.get(attrs, :scope, :rw)),
      actor: Keyword.get(attrs, :actor),
      minted_at: Keyword.get(attrs, :minted_at, DateTime.utc_now())
    })
    |> Repo.insert()
    |> case do
      {:ok, _} -> :ok
      {:error, changeset} -> log_skip(attrs, changeset)
    end
  rescue
    # No Repo (the mix-task path), Postgres unreachable, table missing on an un-migrated node.
    e -> log_skip(attrs, e)
  catch
    :exit, reason -> log_skip(attrs, reason)
  end

  defp log_skip(attrs, reason) do
    Logger.warning(
      "hrana token ledger: NOT recording the mint for #{inspect(Keyword.get(attrs, :shard_id))} " <>
        "(#{brief(reason)}). The token is valid; the audit record is missing, so a later " <>
        "revoke_issued_before/2 will not know about it."
    )

    :ok
  end

  # One line, not the whole term. A `DBConnection.OwnershipError` inspects to roughly a kilobyte of
  # sandbox advice, and this warning fires once per affected mint — enough volume on a green test
  # run to train a reader into skipping warnings, which is the opposite of what a security-audit
  # gap should do. The exception type plus its first line identifies every realistic cause (no Repo,
  # Postgres down, table missing on an un-migrated node) without the wall of text.
  defp brief(%Ecto.Changeset{} = cs) do
    "invalid: " <> (cs |> Ecto.Changeset.traverse_errors(fn {msg, _} -> msg end) |> inspect())
  end

  defp brief(%{__struct__: mod} = e) when is_exception(e) do
    first_line = e |> Exception.message() |> String.split("\n") |> hd()
    "#{inspect(mod)}: #{String.slice(first_line, 0, 120)}"
  end

  defp brief(other), do: other |> inspect() |> String.slice(0, 160)

  @doc """
  Issuances for `shard_id` that are still **outstanding** — their embedded `token_version` is at or
  above the shard's current revocation floor, so a token from that mint still verifies.

  This is the question the ledger exists to answer, and it is why the rows are append-only: history
  is not edited by a revoke, it is reinterpreted against the floor.
  """
  @spec outstanding(String.t()) :: [Issuance.t()]
  def outstanding(shard_id) do
    floor = Fathom.Directory.token_version(shard_id) || 1

    Repo.all(
      from i in Issuance,
        where: i.shard_id == ^shard_id and i.token_version >= ^floor,
        order_by: [desc: i.minted_at]
    )
  end

  @doc "Every issuance for `shard_id`, newest first — the audit view, including revoked history."
  @spec history(String.t(), pos_integer()) :: [Issuance.t()]
  def history(shard_id, limit \\ 100) do
    Repo.all(
      from i in Issuance,
        where: i.shard_id == ^shard_id,
        order_by: [desc: i.minted_at],
        limit: ^limit
    )
  end

  @doc """
  Distinct shard ids with at least one token minted before `cutoff` that is **still outstanding**.

  The input to the time-scoped bulk revoke. Scoped to outstanding issuances so a repeat sweep over
  the same window is a no-op instead of bumping every shard's floor again — a revoke is cheap but
  not free (it invalidates live clients), and an idempotent sweep is what makes it safe to run from
  a cron or to retry after a partial failure.
  """
  @spec shards_issued_before(DateTime.t()) :: [String.t()]
  def shards_issued_before(%DateTime{} = cutoff) do
    Repo.all(
      from i in Issuance,
        join: s in Fathom.Directory.Shard,
        on: s.shard_id == i.shard_id,
        where: i.minted_at < ^cutoff and i.token_version >= s.token_version,
        distinct: true,
        select: i.shard_id
    )
  end
end
