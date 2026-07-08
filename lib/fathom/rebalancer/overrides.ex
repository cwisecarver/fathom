defmodule Fathom.Rebalancer.Overrides do
  @moduledoc """
  The LB exception table — the source of truth for per-shard pins (deviations from the
  pure ketama hash). The control plane pins a hot shard to a chosen node here; the LB map
  renderer reads `all/0` to regenerate the nginx map. One pin per shard (upserted).
  """
  import Ecto.Query, only: [from: 2]

  alias Fathom.Rebalancer.Override
  alias Fathom.Repo

  @doc """
  Pins `shard_id` to `pinned_node` (upsert — one override per shard). `opts` may carry
  `:reason`, `:q_per_s_at_pin`, `:from_node` for observability. Returns `{:ok, override}`.
  """
  @spec pin(String.t(), String.t(), keyword()) ::
          {:ok, Override.t()} | {:error, Ecto.Changeset.t()}
  def pin(shard_id, pinned_node, opts \\ []) do
    attrs =
      %{
        shard_id: shard_id,
        pinned_node: pinned_node,
        reason: opts[:reason],
        q_per_s_at_pin: opts[:q_per_s_at_pin],
        from_node: opts[:from_node],
        # A fresh successful pin clears any prior failure marker so it renders + routes.
        failed_at: nil
      }

    existing = Repo.get_by(Override, shard_id: shard_id) || %Override{}

    existing
    |> Override.changeset(attrs)
    |> Repo.insert_or_update()
  end

  @doc "Removes the pin for `shard_id` (returns to pure hash). Idempotent."
  @spec unpin(String.t()) :: :ok
  def unpin(shard_id) do
    Repo.delete_all(from o in Override, where: o.shard_id == ^shard_id)
    :ok
  end

  @doc """
  Marks `shard_id`'s pin as failed-and-reverted (finding #4): the row is RETAINED (not
  deleted) with `failed_at` stamped, so `LbMap` skips it — traffic returns to the source —
  while its bumped `updated_at` keeps the shard in the Policy cooldown, so an un-drainable
  hot shard backs off instead of thrashing every tick. Idempotent; a no-op if the row is
  gone (nothing to cool). Returns `:ok`.
  """
  @spec mark_failed(String.t()) :: :ok
  def mark_failed(shard_id) do
    now = DateTime.utc_now()

    case Repo.get_by(Override, shard_id: shard_id) do
      nil ->
        :ok

      %Override{} = o ->
        o |> Override.changeset(%{failed_at: now}) |> Repo.update!()
        :ok
    end
  end

  @doc "Every current pin, ordered by shard for a stable rendered map."
  @spec all() :: [Override.t()]
  def all, do: Repo.all(from o in Override, order_by: [asc: o.shard_id])

  @doc "The pin for `shard_id`, or nil."
  @spec for_shard(String.t()) :: Override.t() | nil
  def for_shard(shard_id), do: Repo.get_by(Override, shard_id: shard_id)

  @doc "The set of shard_ids actively pinned to `node_key` (failed/reverted rows excluded)."
  @spec pinned_to(String.t()) :: [String.t()]
  def pinned_to(node_key) do
    Repo.all(
      from o in Override,
        where: o.pinned_node == ^node_key and is_nil(o.failed_at),
        select: o.shard_id
    )
  end
end
