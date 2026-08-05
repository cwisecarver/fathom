defmodule Fathom.Migrator.Release do
  @moduledoc """
  A released shard-schema version in the registry (`shard_migrations`). The fleet
  HEAD is the max released `version`; a shard whose `schema_version` is below HEAD
  is a laggard the rollout will migrate. The data transform for a version lives in
  code (keyed by version), not here — this row just records that the version was
  released.
  """
  use Ecto.Schema
  import Ecto.Changeset

  @type t :: %__MODULE__{}

  schema "shard_migrations" do
    field :version, :integer
    field :name, :string
    field :statements, {:array, :string}, default: []
    # Expert review #12: a yanked release is dead — excluded from HEAD, its statements
    # never applied again. Set by Migrator.yank/1 (a revert yanks by default).
    field :yanked, :boolean, default: false
    # Expert review #32: the template's django_migrations count when this version was captured
    # (Capture already computes it on commit). Lets the post-revert drift check tell whether the
    # template's linear graph was left ahead of the yanked-away fleet, without opening the template
    # or parsing SQL. Nullable — pre-feature releases have none, and the check skips them.
    field :template_migration_count, :integer
    # Expert review #1: set true when capture detects template-literal DATA migrations in the
    # buffer (INSERT/UPDATE/DELETE on non-django_migrations tables) — a fleet-wide-corruption risk.
    # Caps HEAD below this version (Migrator.head/0) so the rollout can't replay it until an operator
    # reviews and clears the flag (Migrator.approve_review/1).
    field :requires_review, :boolean, default: false
    # The bind values for each entry in `statements`, parallel and same-length; `nil` for releases
    # captured before this existed (they replay with no args — the old behavior). Django sends
    # parameterized SQL, so storing statement TEXT alone made every replay bind NULL and die on
    # `django_migrations.app NOT NULL`. Each element is `%{"args" => [<value>, ...]}` with values in
    # `Filo.Value`'s tagged Hrana encoding, so blobs survive JSON. Args are stored rather than
    # interpolated into the SQL — substituting them into the text is the injection/quoting hazard
    # AGENTS.md forbids, and a migration name is attacker-influenceable (it is a filename).
    field :statement_args, {:array, :map}

    # WHY this version is held (expert review 2026-08-01 #26). `requires_review` was one boolean set
    # by `data_migration_statements(statements) != [] or gap != nil`, so the status API could say
    # `pending_review: [7]` and nothing more — an operator had no way to learn what tripped it or
    # what their options were, while every later migration stacked behind it.
    # One of "data_migration" / "migration_gap" / "data_migration_and_gap"; NULL if never flagged.
    field :review_reason, :string

    # The evidence: the flagged statements, or the gap's counts. Shown by `Migrator.status/0` so the
    # operator sees what capture actually saw.
    field :review_detail, :map

    # Name of a module implementing `Fathom.Migrator.Transform` — the per-shard data-migration seam
    # (#26). Resolved against an ALLOWLIST at execution time, never `String.to_atom`'d: a release row
    # is data written by the capture path, whose source template is a documented fleet-wide
    # poisoning vector.
    field :transform, :string

    timestamps(type: :utc_datetime_usec, updated_at: false)
  end

  @doc false
  def changeset(release, attrs) do
    release
    |> cast(attrs, [
      :version,
      :name,
      :statements,
      :template_migration_count,
      :requires_review,
      :statement_args
    ])
    |> validate_required([:version, :name])
    |> validate_number(:version, greater_than: 0)
    |> validate_args_align()
    |> unique_constraint(:version)
  end

  # Two parallel arrays can desync, and a desync would silently bind the WRONG values into a
  # statement — worse than binding none. Refuse to store a release whose args don't line up.
  defp validate_args_align(changeset) do
    statements = get_field(changeset, :statements) || []

    case get_field(changeset, :statement_args) do
      nil ->
        changeset

      args when length(args) == length(statements) ->
        changeset

      args ->
        add_error(
          changeset,
          :statement_args,
          "must be nil or the same length as statements " <>
            "(got #{length(args)} for #{length(statements)} statements)"
        )
    end
  end
end
