defmodule Fathom.Repo.Migrations.AddRetainedVersionToShards do
  use Ecto.Migration

  # WHICH VERSION THIS SHARD ACTUALLY HAS A RETAINED COPY OF (expert review 2026-08-24 #16b).
  #
  # A forward migration retains exactly ONE object, `<shard>@<current>`, where `current` is the
  # version the shard came FROM. A cold-tail shard walks `current+1 … target` in a SINGLE job, so a
  # shard that sat at v5 while the fleet reached v9 retains only `<shard>@5`. A fleet revert picks
  # one fleet-wide target — `Migrator.revert(9, 8)` — and asks every shard for `<shard>@8`, which
  # the chain-jumpers never created. Nothing recorded that, so the only way to find out was to ask
  # storage and get an error.
  #
  # NULLABLE, and null is meaningful: "no retained copy, or we do not know". Existing rows get null
  # rather than a guessed backfill — a wrong value here sends a revert at an object that may not
  # exist, and the pre-existing absent-object path already handles null correctly by quarantining
  # with a diagnosis. It fills in on each shard's next cutover.
  #
  # Deliberately NOT derived by listing storage: there is no `list_versions` callback, adding one
  # would mean implementing it on Local, S3 and the test double, and it would answer a question
  # Postgres can answer from a column we write at the moment we do the retaining — which is also
  # the more truthful record, since it states what we retained rather than what happens to be
  # lying in a bucket.
  def change do
    alter table(:shards) do
      add :retained_version, :integer
    end
  end
end
