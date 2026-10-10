defmodule Fathom.Repo.Migrations.AddQuarantineColumnsToShards do
  @moduledoc """
  Fix-review R3-2. The reconcile job's cool-off requeue filtered quarantined shards on `updated_at`,
  which every touch bumps (the recorder's upsert, resolve, snapshot/retention stamps), so a hot
  quarantined shard never cooled off.

    * `quarantined_at` is stamped ONLY by `Directory.mark_failed/1`.
    * `requeue_count` counts AUTOMATIC cool-off requeues, so a deterministically broken shard stops
      flapping after a few rounds; a successful cutover or an operator `retry_failed/0` resets it.

  Plain columns, no index: the quarantined slice is read by status with a keyset scan.
  """
  use Ecto.Migration

  def change do
    alter table(:shards) do
      add :quarantined_at, :utc_datetime_usec
      add :requeue_count, :integer, null: false, default: 0
    end
  end
end
