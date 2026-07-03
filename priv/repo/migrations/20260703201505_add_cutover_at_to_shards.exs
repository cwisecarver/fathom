defmodule Fathom.Repo.Migrations.AddCutoverAtToShards do
  use Ecto.Migration

  def change do
    alter table(:shards) do
      # When the shard last cut over to its current schema_version (stamped by
      # Directory.cutover with the same instant as last_active_at). The revert
      # force-guard compares last_active_at against this to detect post-cutover
      # activity a revert would discard (fable-review #13). Nil = a row that
      # predates the column / never cut over — the guard treats that age as
      # unknown and refuses without force.
      add :cutover_at, :utc_datetime_usec
    end
  end
end
