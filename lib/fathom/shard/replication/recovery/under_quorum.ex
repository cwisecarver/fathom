defmodule Fathom.Shard.Replication.Recovery.UnderQuorum do
  @moduledoc """
  A cold open refused because too few peers answered to rule out a fresher replica (expert review
  2026-10-12 #5). See `Fathom.Shard.Replication.Recovery.best_replica/3`.

  Raised by `Fathom.Shard.PromoteOnOpen` while the coordinator holds the lease, so it takes the
  ordinary failed-open path: the lease is released, the coordinator stops, and the checkout gets
  `{:error, {:open_failed, %UnderQuorum{}}}`, which `Fathom.ShardExecutor.open_error/1` maps to a
  retryable 503. Nothing is held for the window; each retry is a fresh open that asks again.
  """

  defexception [:shard_id, :answered, :needed, :waited_ms]

  @type t :: %__MODULE__{
          shard_id: String.t(),
          answered: non_neg_integer(),
          needed: pos_integer(),
          waited_ms: non_neg_integer()
        }

  @impl true
  def message(%__MODULE__{} = e) do
    "shard #{e.shard_id}: peer recovery heard #{e.answered} definite answers of the " <>
      "#{e.needed} needed to rule out a fresher replica (waited #{e.waited_ms} ms so far); " <>
      "refusing the open so the client retries"
  end
end
