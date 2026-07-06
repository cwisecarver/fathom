defmodule Fathom.Release do
  @moduledoc """
  Release tasks, runnable without Mix (inside a `mix release` image):

      bin/fathom eval "Fathom.Release.migrate"

  Runs the Postgres (orchestration-store) migrations in `priv/repo/migrations/` —
  the shards directory, shard_migrations, and Oban tables. Per-shard SQLite schema
  migration is a different machinery entirely (`Fathom.Migrator`); this only sets up
  the control plane.
  """
  @app :fathom

  def migrate do
    load_app()

    for repo <- repos() do
      {:ok, _, _} = Ecto.Migrator.with_repo(repo, &Ecto.Migrator.run(&1, :up, all: true))
    end
  end

  def rollback(repo, version) do
    load_app()
    {:ok, _, _} = Ecto.Migrator.with_repo(repo, &Ecto.Migrator.run(&1, :down, to: version))
  end

  defp repos, do: Application.fetch_env!(@app, :ecto_repos)

  defp load_app, do: Application.load(@app)
end
