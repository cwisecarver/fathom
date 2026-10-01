defmodule Fathom.Migrator.Ledger do
  @moduledoc """
  Django's own migration ledger (`django_migrations`) checked against a shard's version stamp —
  by migration NAME, not by row count.

  A shard records its schema version in three places: `django_migrations` (the truth),
  `PRAGMA user_version` (the O(1) gate every checkout reads) and `shards.schema_version` in
  Postgres. The other legs were compared; this one was compared only by COUNT, and only in the
  off-by-default full restore drill. A count cannot tell "one migration missing, one extra" from
  "correct", and cannot say WHICH migration is wrong.

  ## Where the names come from

  Every captured release stores Django's own bookkeeping `INSERT INTO django_migrations …`
  statements with their bound values, because the replay has to write them onto every shard. The
  names are read back by EXECUTING those statements into an empty in-memory `django_migrations`
  table and selecting `(app, name)` — SQLite parses its own SQL, so no column list is ever parsed
  by hand. A release with no bookkeeping rows (hand-authored, or pre-capture) contributes no
  names: it is simply no evidence either way.

  ## The classification (`classify/4`, pure)

  A shard at label `L` is consistent at version `k` when every name of every release `<= k` is
  present and no name of any release `> k` is. Names that belong to NO release (the template's
  pre-capture baseline, e.g. `contenttypes.0001_initial`) are ignored by this rule and caught by
  the absolute `template_migration_count`, when the release recorded one.

    * `:ok` — consistent at `L` (and the count, when known, agrees).
    * `{:behind, k}` — NOT consistent at `L`, but consistent at exactly one `k < L`: the file
      carries `L` but Django applied only through `k`. The migrator replays `k+1 … target` IN
      ORDER from there. Replaying only the "missing" versions on top of later ones would apply
      DDL out of order, which can succeed silently on the wrong schema.
    * `{:mismatch, detail}` — anything else: names from a version above `L`, a partially present
      release, a gap that is not a clean prefix, several candidate `k` (unnamed releases between
      them make the true version ambiguous), or a count that disagrees. Not repairable by a
      machine — someone ran `migrate` against the shard directly, or bytes were restored wrong.
  """

  alias Fathom.Migrator.Capture
  alias Fathom.Migrator.Release
  alias Fathom.Repo
  alias Fathom.Shard.Connection

  import Ecto.Query

  @type name :: {String.t(), String.t()}
  @type verdict :: :ok | {:behind, non_neg_integer()} | {:mismatch, map()}

  # Django's columns, deliberately WITHOUT its NOT NULLs: this table only has to accept the
  # release's own INSERTs so their names can be read back, and a hand-authored row that omits
  # `applied` is still a name.
  @ledger_ddl """
  CREATE TABLE django_migrations (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    app varchar(255),
    name varchar(255),
    applied datetime
  )
  """

  @doc """
  Check the SQLite file at `path` against its own `PRAGMA user_version`, considering EVERY
  non-yanked release. Releases above the label matter as much as those below it: a name from any
  of them in the ledger means the shard ran a migration its label does not admit to. (The first
  draft considered only releases up to the migration's target, and a shard carrying a name from a
  release beyond it read that name as unknown baseline and was migrated anyway.)
  """
  @spec check_file(Path.t()) :: {non_neg_integer(), verdict()}
  def check_file(path) do
    {:ok, conn} = Connection.open(path)

    try do
      label = user_version(conn)
      {present, count} = shard_ledger(conn)
      {label, classify(label, present, count, release_facts())}
    after
      Connection.close(conn)
    end
  end

  @doc """
  The pure decision. `facts` is `%{version => %{names: MapSet.t(name), count: integer | nil}}` for
  every non-yanked release; see the moduledoc for what each verdict means.
  """
  @spec classify(non_neg_integer(), MapSet.t(name), non_neg_integer(), map()) :: verdict()
  def classify(label, present, count, facts) do
    named =
      facts
      |> Enum.filter(fn {_v, f} -> MapSet.size(f.names) > 0 end)
      |> Enum.sort_by(fn {v, _} -> v end)

    candidates = Enum.filter(0..label//1, &consistent_at?(&1, named, present))

    cond do
      label in candidates ->
        count_verdict(label, count, facts, :ok)

      match?([_], candidates) ->
        [k] = candidates
        count_verdict(k, count, facts, {:behind, k})

      true ->
        {:mismatch,
         %{
           label: label,
           candidates: candidates,
           missing: missing_through(label, named, present),
           unexpected: unexpected_above(label, named, present) |> Enum.sort()
         }}
    end
  end

  defp consistent_at?(k, named, present) do
    Enum.all?(named, fn {v, f} ->
      if v <= k,
        do: MapSet.subset?(f.names, present),
        else: MapSet.disjoint?(f.names, present)
    end)
  end

  # The absolute count is the only thing that sees names belonging to NO release (an extra
  # `migrate` run against the shard, or a missing baseline row). Unknown count ⇒ no statement.
  defp count_verdict(version, count, facts, verdict) do
    case facts do
      %{^version => %{count: expected}} when is_integer(expected) and expected != count ->
        {:mismatch, %{label: version, expected_count: expected, count: count}}

      _ ->
        verdict
    end
  end

  defp missing_through(label, named, present) do
    for {v, f} <- named, v <= label, n <- f.names, not MapSet.member?(present, n), do: {v, n}
  end

  defp unexpected_above(label, named, present) do
    for {v, f} <- named, v > label, n <- f.names, MapSet.member?(present, n), do: {v, n}
  end

  # The `(app, name)` set and row count of the shard's ledger; an absent table is empty.
  @spec shard_ledger(reference()) :: {MapSet.t(name), non_neg_integer()}
  defp shard_ledger(conn) do
    case Connection.query(
           conn,
           "SELECT name FROM sqlite_master WHERE type='table' AND name='django_migrations'",
           []
         ) do
      {:ok, %{rows: []}} ->
        {MapSet.new(), 0}

      {:ok, %{rows: [_ | _]}} ->
        {:ok, %{rows: rows}} =
          Connection.query(conn, "SELECT app, name FROM django_migrations", [])

        {MapSet.new(rows, fn [app, name] -> {app, name} end), length(rows)}
    end
  end

  # `%{version => %{names, count}}` for every non-yanked release. Yanked releases are left out: the
  # chain may skip them (2026-09-29 #16), so their names are neither required nor forbidden.
  #
  # NAMES ARE DERIVED ONCE PER RELEASE PER NODE (expert review 2026-10-01 perf #12). This runs twice
  # per migrated shard (before and after the replay), and deriving a release's names opens an
  # in-memory SQLite connection and replays its bookkeeping INSERTs. Doing that for every release on
  # every call made a fleet rollout pay O(releases) SQLite opens per shard on the dirty-IO pool the
  # tenants share. A release's statements are written once at capture and never updated, so its
  # names are a pure function of `{id, version}` and are memoised in `:persistent_term` (a NEW key
  # never triggers the global GC an update or erase does; nothing here is ever updated). The
  # MUTABLE columns (`yanked`, the count) are re-read on every call, and only the narrow columns:
  # the statement payloads are fetched only for releases this node has not seen.
  @spec release_facts() :: map()
  defp release_facts do
    rows =
      Repo.all(
        from(r in Release,
          where: not r.yanked,
          order_by: [asc: r.version],
          select: {r.id, r.version, r.template_migration_count}
        )
      )

    names = names_for(rows)

    Map.new(rows, fn {id, version, count} ->
      {version, %{names: Map.fetch!(names, id), count: count}}
    end)
  end

  defp names_for(rows) do
    {cached, missing} =
      Enum.reduce(rows, {%{}, []}, fn {id, version, _}, {hit, miss} ->
        case :persistent_term.get({__MODULE__, :names, id, version}, nil) do
          nil -> {hit, [id | miss]}
          names -> {Map.put(hit, id, names), miss}
        end
      end)

    if missing == [] do
      cached
    else
      from(r in Release, where: r.id in ^missing)
      |> Repo.all()
      |> Enum.reduce(cached, fn r, acc ->
        names = release_names(r)
        :persistent_term.put({__MODULE__, :names, r.id, r.version}, names)
        Map.put(acc, r.id, names)
      end)
    end
  end

  @doc "The `(app, name)` rows a release's bookkeeping statements insert."
  @spec release_names(Release.t()) :: MapSet.t(name)
  def release_names(%Release{statements: statements, statement_args: args}) do
    rows =
      statements
      |> Enum.zip(args || List.duplicate(nil, length(statements)))
      |> Enum.filter(fn {sql, _} -> Capture.bookkeeping?(sql) end)

    if rows == [], do: MapSet.new(), else: names_via_sqlite(rows)
  end

  # Let SQLite parse Django's INSERTs: run them into an empty ledger and read the names back.
  defp names_via_sqlite(rows) do
    {:ok, conn} = Connection.open(":memory:")

    try do
      :ok = Connection.exec(conn, @ledger_ddl)

      # A statement SQLite cannot run here contributes nothing — no evidence, never a crash: this
      # runs inside a tenant's migration, and a malformed bookkeeping row is not a reason to fail it.
      for {sql, arg} <- rows, do: _ = Connection.query(conn, sql, decode_args(arg))

      {:ok, %{rows: names}} =
        Connection.query(
          conn,
          "SELECT app, name FROM django_migrations WHERE app IS NOT NULL AND name IS NOT NULL",
          []
        )

      MapSet.new(names, fn [app, name] -> {app, name} end)
    after
      Connection.close(conn)
    end
  end

  defp decode_args(%{"args" => list}) when is_list(list), do: Enum.map(list, &Filo.Value.decode/1)
  defp decode_args(_), do: []

  defp user_version(conn) do
    {:ok, %{rows: [[v]]}} = Connection.query(conn, "PRAGMA user_version", [])
    v
  end
end
