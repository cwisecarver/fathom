defmodule Fathom.Admin.AdmissionEvents do
  @moduledoc """
  Feeds `Fathom.Admin.EventRing` (kind `:admission`) with WHICH shard was refused or evicted
  (expert review 2026-10-15 #10). The Prometheus counters for these events say how many; a
  shard id is unbounded cardinality, so it cannot be a label, and the ring is where it goes.

  A telemetry handler on the EXISTING admission events, so `Fathom.Shards` is untouched and the
  success path pays nothing: these events fire only on the refusal and eviction branches.
  """
  alias Fathom.Admin.EventRing

  @handler_id "fathom-admin-admission-ring"
  @events [
    [:fathom, :shards, :at_capacity],
    [:fathom, :shards, :novel_rate_limited],
    [:fathom, :shards, :evicted]
  ]

  @doc "Attach the handler (idempotent)."
  @spec attach() :: :ok
  def attach do
    :telemetry.detach(@handler_id)
    :telemetry.attach_many(@handler_id, @events, &__MODULE__.handle_event/4, nil)
    :ok
  end

  @doc "Detach the handler."
  @spec detach() :: :ok
  def detach do
    _ = :telemetry.detach(@handler_id)
    :ok
  end

  @doc false
  @spec handle_event([atom()], map(), map(), term()) :: :ok
  def handle_event([:fathom, :shards, kind], _measurements, meta, _config) do
    EventRing.push(:admission, %{kind: kind, shard_id: Map.get(meta, :shard_id)})
  end
end
