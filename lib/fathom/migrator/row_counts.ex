defmodule Fathom.Migrator.RowCounts do
  @moduledoc """
  Row-count conservation for a migration replay (expert review 2026-11-07 #64, the part batch L
  parked).

  A replay is DDL plus Django's own bookkeeping row, and DDL does not change how many rows a table
  holds. A step that does is almost always a bug the other checks cannot see: Django rebuilds a
  table for most `AlterField`/`RemoveField` (`CREATE new__x` / `INSERT … SELECT` / `DROP x` /
  `RENAME`), and an `INSERT OR IGNORE`, a lossy `SELECT` or a `UNION` that drops or duplicates rows
  leaves `quick_check`, `foreign_key_check` and the ledger all green while the tenant has silently
  lost (or doubled) data.

  `Fathom.Migrator.Copy` takes a `snapshot/1` after `BEGIN` and `verify/4` before `COMMIT`: for every
  ordinary table that exists on both sides, the count must be unchanged, except where the step
  says otherwise (`expectations/2`). A mismatch rolls the step back.

  ## What a step may change

    * **Tables its own statements write** (`INSERT`/`REPLACE`/`UPDATE`/`DELETE` naming an existing
      table, an approved data migration or a captured seed row): any count.
    * **A table it drops and creates again by name**: any count.
    * **Everything, when the step carries a `Fathom.Migrator.Transform`** that does not declare
      `c:Fathom.Migrator.Transform.row_count_deltas/0` (the default, so no existing data migration
      starts failing): an opaque `run/2` can touch any table. A transform that declares deltas has
      exactly those tables exempt (an integer is an exact expected delta, `:any` is anything), and
      every other table must conserve.
    * **Everything, when the step's effect cannot be read**: a statement the parser does not
      understand that could write (a `WITH … INSERT`), or any DML in a database that has triggers
      (a trigger writes tables the statement does not name).

  `django_migrations` is excluded (the replay writes its rows by design; `verify_ledger` checks them),
  as are `sqlite_*` tables and virtual tables with their shadow tables.

  The check is the same scan `quick_check` is: one `count(*)` per table before and after, inside the
  replay's `BackgroundScanGate` permit, on the connection that already has the pages cached. It
  follows `:verify_flush_integrity`: a deployment that turned verification off does not pay it.
  """

  alias Fathom.Migrator.Transform
  alias Fathom.Shard.Connection

  @type counts :: %{optional(String.t()) => non_neg_integer()}
  @type snapshot :: %{counts: counts(), triggers?: boolean()}
  @type expectations :: :unchecked | %{optional(String.t()) => integer() | :any}

  @ledger "django_migrations"

  @doc "Whether the check runs (`:verify_flush_integrity`, default on)."
  @spec enabled?() :: boolean()
  def enabled?, do: Application.get_env(:fathom, :verify_flush_integrity, true)

  @doc "Per-table row counts of `conn`'s ordinary tables, and whether the database has triggers."
  @spec snapshot(reference()) :: {:ok, snapshot()} | {:error, term()}
  def snapshot(conn) do
    with {:ok, %{rows: tables}} <-
           Connection.query(
             conn,
             "SELECT name, sql FROM sqlite_master WHERE type = 'table' " <>
               "AND name NOT LIKE 'sqlite\\_%' ESCAPE '\\'",
             []
           ),
         {:ok, %{rows: [[triggers]]}} <-
           Connection.query(
             conn,
             "SELECT count(*) FROM sqlite_master WHERE type = 'trigger'",
             []
           ),
         {:ok, counts} <- count_tables(conn, countable(tables)) do
      {:ok, %{counts: counts, triggers?: triggers > 0}}
    end
  end

  # Virtual tables and their shadow tables (`<vt>_data`, `<vt>_content`, …) are the module's
  # business; the ledger is checked by `verify_ledger`.
  defp countable(tables) do
    virtual =
      for [name, sql] <- tables, is_binary(sql), virtual_sql?(sql), do: name

    for [name, _sql] <- tables,
        name != @ledger,
        name not in virtual,
        not Enum.any?(virtual, &String.starts_with?(name, &1 <> "_")),
        do: name
  end

  defp virtual_sql?(sql), do: String.match?(sql, ~r/^\s*create\s+virtual\s+table/i)

  defp count_tables(conn, names) do
    Enum.reduce_while(names, {:ok, %{}}, fn name, {:ok, acc} ->
      case Connection.query(conn, "SELECT count(*) FROM #{quote_ident(name)}", []) do
        {:ok, %{rows: [[n]]}} -> {:cont, {:ok, Map.put(acc, name, n)}}
        {:error, _} = error -> {:halt, error}
      end
    end)
  end

  defp quote_ident(name), do: ~s|"#{String.replace(name, "\"", "\"\"")}"|

  @doc """
  What the step is allowed to do to row counts: `:unchecked`, or a map of table => `:any` | delta
  (every table not in it must conserve).

  `statements` are the replayed `{sql, args}` pairs, `transform` the step's transform module (or
  `nil`), `before` the snapshot taken at the top of the step.
  """
  @spec expectations([{String.t(), [term()]}], module() | nil, snapshot()) :: expectations()
  def expectations(statements, transform, %{counts: counts, triggers?: triggers?}) do
    parsed = Enum.map(statements, fn {sql, _args} -> classify(sql) end)
    writes? = Enum.any?(parsed, &match?({:write, _}, &1))

    cond do
      Enum.any?(parsed, &(&1 == :opaque)) ->
        :unchecked

      # A trigger writes tables the statement does not name.
      writes? and triggers? ->
        :unchecked

      true ->
        declared =
          case transform do
            nil -> %{}
            module -> Transform.row_count_deltas(module)
          end

        if declared == :unchecked do
          :unchecked
        else
          written = for {:write, table} <- parsed, into: %{}, do: {table, :any}

          recreated =
            for {:create, table} <- parsed,
                Map.has_key?(counts, table),
                into: %{},
                do: {table, :any}

          declared |> Map.merge(recreated) |> Map.merge(written)
        end
    end
  end

  @doc """
  `:ok` when every table present before AND after has the expected count, else
  `{:error, {:row_count_mismatch, version, tables}}` naming up to 5 of them as
  `%{table:, before:, after:, expected_delta:}`. A table that exists on one side only (created,
  dropped, renamed) is not compared.
  """
  @spec verify(snapshot(), snapshot(), expectations(), non_neg_integer()) ::
          :ok | {:error, {:row_count_mismatch, non_neg_integer(), [map()]}}
  def verify(_before, _after, :unchecked, _version), do: :ok

  def verify(%{counts: before}, %{counts: now}, expected, version) do
    bad =
      for {table, was} <- Enum.sort(before),
          Map.has_key?(now, table),
          delta = Map.get(expected, table, 0),
          delta != :any,
          now[table] != was + delta do
        %{table: table, before: was, after: now[table], expected_delta: delta}
      end

    case bad do
      [] -> :ok
      _ -> {:error, {:row_count_mismatch, version, Enum.take(bad, 5)}}
    end
  end

  # --- statement classification --------------------------------------------------------------

  @ident ~S{("(?:[^"]|"")+"|`[^`]+`|\[[^\]]+\]|[A-Za-z_][\w$]*)}
  @target "(?:" <> @ident <> "\\s*\\.\\s*)?" <> @ident

  @insert Regex.compile!(
            "^\\s*(?:insert|replace)\\b(?:\\s+or\\s+\\w+)?\\s+into\\s+" <> @target,
            "i"
          )
  @update Regex.compile!("^\\s*update\\b(?:\\s+or\\s+\\w+)?\\s+" <> @target, "i")
  @delete Regex.compile!("^\\s*delete\\s+from\\s+" <> @target, "i")
  @create Regex.compile!(
            "^\\s*create\\s+(?:temp\\s+|temporary\\s+)?table\\s+(?:if\\s+not\\s+exists\\s+)?" <>
              @target,
            "i"
          )

  # `{:write, table}` (DML on a named table), `{:create, table}`, `:opaque` (could write, cannot be
  # read: a CTE-led statement, or DML the patterns above do not match), or `:other` (DDL, pragmas,
  # selects: no row-count effect of their own).
  @spec classify(String.t()) :: {:write, String.t()} | {:create, String.t()} | :opaque | :other
  defp classify(sql) do
    cond do
      match = Regex.run(@insert, sql) -> {:write, target(match)}
      match = Regex.run(@update, sql) -> {:write, target(match)}
      match = Regex.run(@delete, sql) -> {:write, target(match)}
      match = Regex.run(@create, sql) -> {:create, target(match)}
      String.match?(sql, ~r/^\s*(?:with|insert|replace|update|delete)\b/i) -> :opaque
      true -> :other
    end
  end

  # The table is the LAST captured identifier (the first, when present, is the schema).
  defp target([_whole | idents]),
    do: idents |> Enum.reject(&(&1 == "")) |> List.last() |> unwrap()

  defp unwrap("\"" <> _ = q), do: q |> String.slice(1..-2//1) |> String.replace("\"\"", "\"")
  defp unwrap("`" <> _ = q), do: String.slice(q, 1..-2//1)
  defp unwrap("[" <> _ = q), do: String.slice(q, 1..-2//1)
  defp unwrap(plain), do: plain
end
