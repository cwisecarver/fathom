defmodule Fathom.Migrator.Transform do
  @moduledoc """
  The per-shard data-migration seam (expert review 2026-08-01 #26).

  ## The problem this exists for

  The engine's model is "record the SQL Django sent to the template, replay it verbatim on every
  tenant". That is exactly right for DDL and exactly wrong for a `RunPython` backfill: the ORM's
  writes cross the wire as literal `INSERT`/`UPDATE`/`DELETE` carrying **the template's row
  values**. `Capture` detects that shape and sets `requires_review`, capping HEAD below the version
  — and an operator then had two options, both bad:

    * **Approve it** — replay the template's row values onto every tenant, i.e. commit the exact
      corruption the flag exists to prevent.
    * **Never advance** — `converged: false` forever, with every subsequent migration stacked
      behind it.

  `AddField` + `RunPython` is the most common two-step Django migration, so this was certain to bite
  in month one.

  ## The third path

  A fleet version may carry a **transform**: a module, named on the release row and resolved
  against an allowlist, whose `run/2` executes per shard **inside the same transaction as the
  replayed DDL**. It receives a live connection to that tenant's database and the shard id, so it
  can compute the backfill from *that tenant's* rows.

      defmodule MyApp.Backfills.V7DenormalizeTotals do
        @behaviour Fathom.Migrator.Transform

        @impl true
        def run(conn, _shard_id) do
          case Fathom.Shard.Connection.query(conn, "UPDATE orders SET total = qty * price", []) do
            {:ok, _} -> :ok
            {:error, reason} -> {:error, reason}
          end
        end
      end

  Registered in config, which is what makes it safe:

      config :fathom, :migration_transforms, [MyApp.Backfills.V7DenormalizeTotals]

  ## Why an allowlist and not just the module name

  A release row is **data**. It is written by the capture path, whose source — the template shard —
  `AGENTS.md` already documents as a fleet-wide poisoning vector ("a captured migration is replayed
  verbatim onto every tenant"). Resolving an arbitrary string to a module and calling it would turn
  a write to that row into remote code execution on every node in the fleet. So `resolve/1` never
  calls `String.to_atom/1`: it matches the name against the configured list and refuses anything
  else, and the refusal fails the migration rather than skipping the transform — a backfill that
  silently did not run is worse than one that failed loudly.

  ## What this deliberately does NOT do

  It does not execute Python, and it does not unblock the template-literal DML path. The captured
  DML statements stay blocked; the operator's move is to attach a transform that expresses the same
  intent per tenant. `Fathom.Migrator.attach_transform/2` therefore refuses a release whose
  statements still contain flagged DML — otherwise a version could run both, applying the template's
  literals *and* the transform.
  """

  @doc """
  Runs the per-shard data migration.

  `conn` is an open `Fathom.Shard.Connection` to the destination copy, already inside the
  migration's transaction with that version's DDL applied. Returning `{:error, _}` rolls the whole
  step back, so the shard stays at the previous version rather than half-migrated.

  Must be **idempotent where it can be**: a shard whose migration job retried after a transient
  failure will run this again from the pre-transaction state, and the rollout's per-shard job is
  unique-but-retryable.
  """
  @callback run(conn :: reference(), shard_id :: String.t()) :: :ok | {:error, term()}

  @doc """
  Resolves a transform name from a release row to a module, against the configured allowlist.

  Returns `{:ok, module}`, `{:error, :not_allowed}` for a name that is not registered (including
  one that names a real, loaded module — being loaded is not permission), or `{:error, :no_transform}`
  for `nil`.
  """
  @spec resolve(String.t() | nil) :: {:ok, module()} | {:error, :not_allowed | :no_transform}
  def resolve(nil), do: {:error, :no_transform}
  def resolve(""), do: {:error, :no_transform}

  def resolve(name) when is_binary(name) do
    # Compare on the STRING form of each allowlisted module rather than converting `name` to an
    # atom. `String.to_existing_atom/1` would still be a leak (any loaded module's atom exists), and
    # `to_atom/1` is an unbounded atom-table write from a data column.
    case Enum.find(allowlist(), fn mod -> to_string(mod) == normalize(name) end) do
      nil -> {:error, :not_allowed}
      mod -> {:ok, mod}
    end
  end

  @doc """
  The configured allowlist (`config :fathom, :migration_transforms`). Empty by default: a fleet with
  no data migrations never needs one, and an empty allowlist means a stray `transform` value on a
  release row cannot execute anything.
  """
  @spec allowlist() :: [module()]
  def allowlist do
    case Application.get_env(:fathom, :migration_transforms, []) do
      mods when is_list(mods) -> mods
      _ -> []
    end
  end

  @doc """
  Whether `module` is registered AND actually implements this behaviour.

  Both halves matter: the allowlist is the security boundary, and the `run/2` export check is what
  turns "you registered the wrong module" into an error at attach time instead of an
  `UndefinedFunctionError` in the middle of a fleet rollout.
  """
  @spec valid?(module()) :: boolean()
  def valid?(module) when is_atom(module) do
    module in allowlist() and Code.ensure_loaded?(module) and
      function_exported?(module, :run, 2)
  end

  def valid?(_), do: false

  # Elixir modules are `Elixir.`-prefixed atoms; accept either spelling on the row so an operator
  # writing `MyApp.Backfills.V7` by hand matches `to_string(MyApp.Backfills.V7)`.
  defp normalize("Elixir." <> _ = name), do: name
  defp normalize(name), do: "Elixir." <> name
end
