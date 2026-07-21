defmodule Fathom.Repo.Migrations.CreateAuditEvents do
  use Ecto.Migration

  # Append-only audit log of control-plane / admin actions (expert review #9). Every mutating,
  # high-blast-radius operation — delete, restore, fork, export, suspend, token mint/rotate/revoke —
  # records who (actor), what (action), which tenant (shard_id), from where (source_ip), and the
  # outcome. Insert-only (no updated_at); never mutated after write.
  def change do
    create table(:audit_events) do
      add :actor, :string, null: false
      add :action, :string, null: false
      add :shard_id, :string
      add :source_ip, :string
      add :outcome, :string, null: false, default: "ok"
      add :detail, :map, null: false, default: %{}

      timestamps(type: :utc_datetime_usec, updated_at: false)
    end

    create index(:audit_events, [:shard_id])
    create index(:audit_events, [:actor])
    create index(:audit_events, [:action])
    create index(:audit_events, [:inserted_at])
  end
end
