defmodule Fathom.Repo.Migrations.AddRetainingVersionToShards do
  use Ecto.Migration

  # THE RETAIN INTENT, RECORDED BEFORE THE FLUSH (expert review 2026-09-29 #27; decided 2026-09-29).
  #
  # `retained_version` is written at cutover. A forward migration that flushes the migrated object
  # and then crashes before cutover leaves nothing saying what it retained, so the crash-forward
  # `finalize/2` had to GUESS from storage: the highest `<shard>@v` below target (2026-09-05 #18).
  # After a revert, `@N` still holds pre-revert bytes for the retention window, so a later
  # N-1 → N+1 migration that crashes in that gap found the stale `@N` first, recorded it, scheduled
  # its retirement, and orphaned the real `@N-1` — a later revert restored the old generation.
  # Storage cannot tell the two objects apart; only a record made at retain time can.
  #
  # Set together with `migrating` (to the FILE version being retained), read by `finalize/2`,
  # cleared at cutover. NULLABLE: null means no migration in flight, or a row from before this
  # column, and `finalize/2` then falls back to the storage heuristic exactly as before.
  def change do
    alter table(:shards) do
      add :retaining_version, :integer
    end
  end
end
