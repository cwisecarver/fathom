defmodule Fathom.Repo.Migrations.DropUnusedShardLoadSamplesIndex do
  use Ecto.Migration

  # The composite (shard_id, sampled_at) index (finding #18): no query uses it — the
  # reader (`LoadSamples.since/1`) filters on sampled_at alone (served by the separate
  # [:sampled_at] index) and reduces per-shard in Elixir. So it's pure insert overhead on
  # a hot-write table. `change/0` drops it (reversible: rolls back by recreating it).
  def change do
    drop index(:shard_load_samples, [:shard_id, :sampled_at])
  end
end
