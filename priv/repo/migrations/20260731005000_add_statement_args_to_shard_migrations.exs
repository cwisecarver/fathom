defmodule Fathom.Repo.Migrations.AddStatementArgsToShardMigrations do
  use Ecto.Migration

  # Captured statements were stored as SQL TEXT ONLY, but Django sends parameterized SQL — the
  # bookkeeping row crosses the wire as `INSERT INTO django_migrations (...) VALUES (?, ?, ?)` with
  # the values carried separately. Replay ran `Connection.exec(conn, sql)` with no args, so SQLite
  # bound NULL and every replay died on `NOT NULL constraint failed: django_migrations.app`,
  # aborting the whole copy transaction. Every Django migration ends with that row, so NO captured
  # migration could be replayed onto a tenant at all.
  #
  # Args are STORED, never interpolated into the SQL: substituting values into the statement text
  # would be exactly the injection/quoting hazard AGENTS.md forbids, and a migration NAME is
  # attacker-influenceable (it is a filename) — one apostrophe would be enough.
  #
  # Shape: parallel to `statements`, one element per statement, each `%{"args" => [<value>, ...]}`
  # where a value is `Filo.Value`'s tagged Hrana encoding (`%{"type" => "text", "value" => …}`,
  # `%{"type" => "blob", "base64" => …}`, …). Reusing the wire encoding means blobs survive JSON and
  # there is no new serialization to get wrong.
  #
  # NULLABLE on purpose: releases captured before this column replay with no args, which is exactly
  # the old behavior, so an existing registry keeps working untouched.
  def change do
    alter table(:shard_migrations) do
      add :statement_args, {:array, :map}
    end
  end
end
