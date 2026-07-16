defmodule Fathom.Repo.Migrations.AddLastFlushedAtToShards do
  @moduledoc """
  Post-node-loss tenant-level loss accounting (expert review 2026-07-14 #28). The old
  `Fathom.Admin.FlushWatermark` is node-local ETS and dies with the node, so after a node loss —
  the exact scenario the RPO contract covers — there was no surviving record of when each shard
  last flushed to storage. This column persists it (via `Fathom.Directory.Recorder`, off the hot
  path), so the report is `last_active_at > last_flushed_at` ⇒ dirty at loss, with the window
  bounded per tenant.

  Nullable + additive: old rows / never-flushed shards read NULL (treated as "never flushed" — the
  safe direction: a shard with writes but no recorded flush is reported as potentially-lost).
  """
  use Ecto.Migration

  def change do
    alter table(:shards) do
      add :last_flushed_at, :utc_datetime_usec
    end
  end
end
