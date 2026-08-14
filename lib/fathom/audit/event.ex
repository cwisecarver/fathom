defmodule Fathom.Audit.Event do
  @moduledoc """
  An append-only audit record of one control-plane / admin action (expert review #9): who (`actor`),
  what (`action`), which tenant (`shard_id`), from where (`source_ip`), the `outcome`, and safe
  `detail`. Never mutated after insert.
  """
  use Ecto.Schema
  import Ecto.Changeset

  @type t :: %__MODULE__{}

  schema "audit_events" do
    field :actor, :string
    field :action, :string
    field :shard_id, :string
    field :source_ip, :string
    field :outcome, :string, default: "ok"
    field :detail, :map, default: %{}

    timestamps(type: :utc_datetime_usec, updated_at: false)
  end

  def changeset(event, attrs) do
    event
    |> cast(attrs, [:actor, :action, :shard_id, :source_ip, :outcome, :detail])
    |> validate_required([:actor, :action, :outcome])
  end
end
