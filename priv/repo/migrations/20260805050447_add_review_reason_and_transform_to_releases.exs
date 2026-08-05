defmodule Fathom.Repo.Migrations.AddReviewReasonAndTransformToReleases do
  @moduledoc """
  Expert review 2026-08-01 #26, both halves.

  `review_reason` / `review_detail` make the block **legible**: `requires_review` was one boolean
  set by `data_migration_statements(statements) != [] or gap != nil`, so `GET /api/migrations/status`
  could only say `pending_review: [7]` and an operator had no way to learn *why* 7 was held or what
  their options were — while every later migration stacked behind it.

  `transform` is the **third path**. Until now an operator's only choices were to approve the
  version (replaying the TEMPLATE's row values onto every tenant — exactly the corruption the flag
  exists to prevent) or to never advance. It names a server-side module that runs per shard inside
  the same transaction as the replayed DDL, which is how "backfill per tenant" gets expressed
  without executing Python or trusting template literals.
  """
  use Ecto.Migration

  def change do
    # The releases table is `shard_migrations` — `Fathom.Migrator.Release` maps onto it. (Named for
    # what it records rather than for the struct, which is easy to get wrong from the module name.)
    alter table(:shard_migrations) do
      # Why the version is held: "data_migration", "migration_gap", or both. NULL for a release
      # that was never flagged.
      add :review_reason, :string

      # The evidence — the flagged statements, or the gap's before/expected counts — so the API can
      # show an operator what it actually saw rather than making them re-derive it.
      add :review_detail, :map
      # Module name implementing `Fathom.Migrator.Transform`. Resolved against an ALLOWLIST at
      # execution time, never `String.to_atom`'d and applied directly: a release row is data, and
      # the capture template is already a documented fleet-wide poisoning vector.
      add :transform, :string
    end
  end
end
