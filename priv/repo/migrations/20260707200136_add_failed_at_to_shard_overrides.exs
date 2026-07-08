defmodule Fathom.Repo.Migrations.AddFailedAtToShardOverrides do
  use Ecto.Migration

  # Failure cooldown (finding #4): on a failed handoff the revert used to DELETE the
  # override row, so the next RebalanceJob tick saw the still-hot shard with no override →
  # not cooling → re-proposed → thrash every tick. Instead the revert now RETAINS the row
  # and stamps `failed_at`; the LB renderer skips failed rows (traffic returns to the hash
  # home / source) while the retained row's fresh `updated_at` keeps it in the Policy's
  # cooldown, so a wedged (un-drainable) hot shard backs off instead of thrashing. A later
  # successful pin clears `failed_at`.
  def change do
    alter table(:shard_overrides) do
      add :failed_at, :utc_datetime_usec
    end
  end
end
