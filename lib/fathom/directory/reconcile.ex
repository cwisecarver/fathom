defmodule Fathom.Directory.Reconcile do
  @moduledoc """
  Cross-store DR reconciliation (expert review 2026-07-19 #6). Fathom's correctness state is split
  across two stores that can be restored independently: the Postgres **directory** and object
  **storage** (shard bytes, tombstones, token floors). A Postgres point-in-time restore rolls the
  directory back and can desync it from storage — resurrecting deleted tenants, un-revoking tokens,
  and rewinding `schema_version` so the laggard sweep replays a migration onto an already-migrated
  file and quarantines it.

  This sweep realigns the directory to the authoritative durable facts in storage, and is the
  DR-completion step: **run it (with `--fix`) after a Postgres restore, before reopening traffic.**
  It reconciles three facts, each already backstopped in storage:

    * **`schema_version`** ← the shard file's `PRAGMA user_version`. The file lives in storage (not
      Postgres), so its version survives a directory restore and is the source of truth.
    * **`token_version`** ← the durable revocation floor (`Storage.read_token_floor/1`, #6b). Raised,
      never lowered, so a restore can't un-revoke.
    * **deleted status** ← the durable tombstones (`Storage.tombstoned_ids/0`, #6a). A storage
      tombstone with no `deleted` directory row is re-tombstoned.

  `run/1` returns a list of finding maps; with `fix: true` it applies the corrections. Read-only by
  default (a dry run). `deleted` rows are skipped for the object/version checks (their file is gone).
  Operator tooling — pulls each shard's object to read its header — so scope with `:limit` or run off
  the hot path.
  """

  alias Fathom.Directory
  alias Fathom.Shard.Storage
  alias Fathom.Tenants

  # Rows reconciled at once. Each is a couple of small ranged reads (expert review 2026-10-12 #19),
  # so this is bounded well under the HTTP pool; the old serial whole-object pull took
  # rows x (RTT + object size / bandwidth) in the one run that has to finish before traffic reopens.
  @concurrency 16
  # Per row: a ranged GET plus the token-floor read. Never `:infinity`.
  @row_timeout_ms 60_000

  @type finding :: %{
          required(:shard_id) => String.t(),
          required(:kind) => atom(),
          optional(any) => any
        }

  @spec run(keyword()) :: [finding()]
  def run(opts \\ []) do
    fix? = Keyword.get(opts, :fix, false)

    # Stream, don't materialize (expert review 2026-07-24 #15). `Directory.all/0` loaded EVERY row
    # as a struct and `:limit` was then applied in Elixir — so `--limit 100` still pulled the whole
    # table, and at fleet scale that is hundreds of MB in one process: an OOM, not a slow query.
    # This is the documented DR-completion step ("run it with --fix after a Postgres restore, before
    # reopening traffic"), so it failed exactly when the fleet was already down.
    # Chunked so a task that exits (timeout/kill) can be attributed to its row: `async_stream`
    # reports `{:exit, reason}` without the input, and "skipped silently" made a brownout look like
    # a clean fleet (expert review 2026-10-12 #6).
    findings =
      Directory.all_paged()
      |> apply_limit(Keyword.get(opts, :limit))
      |> Stream.chunk_every(@concurrency * 4)
      |> Enum.flat_map(fn rows ->
        rows
        |> Task.async_stream(&reconcile_row(&1, fix?),
          max_concurrency: @concurrency,
          timeout: @row_timeout_ms,
          on_timeout: :kill_task,
          ordered: true
        )
        |> Enum.zip(rows)
        |> Enum.flat_map(fn
          {{:ok, findings}, _row} ->
            findings

          {{:exit, reason}, row} ->
            [%{shard_id: row.shard_id, kind: :unreadable, fact: :row, reason: unreadable(reason)}]
        end)
      end)

    findings ++ reconcile_tombstones(deleted_ids(), fix?)
  end

  # The deleted set comes from its own indexed query rather than from the walked rows. Two reasons:
  # the stream is consumed once (rebuilding the set would mean a second full walk), and under
  # `:limit` the old shape only saw the deleted rows inside the limit window, so a storage tombstone
  # whose directory row fell outside it was reported as missing. This selects ids only — binaries,
  # not structs — and is served by `shards_deleted_index`.
  defp deleted_ids, do: MapSet.new(Directory.deleted_shard_ids())

  defp apply_limit(stream, n) when is_integer(n) and n > 0, do: Stream.take(stream, n)
  defp apply_limit(stream, _), do: stream

  # A deleted row's object is purged — nothing to read; it's covered by the tombstone check instead.
  defp reconcile_row(%{status: "deleted"}, _fix?), do: []

  defp reconcile_row(row, fix?), do: reconcile_schema(row, fix?) ++ reconcile_token(row, fix?)

  defp reconcile_schema(row, fix?) do
    case stored_user_version(row.shard_id) do
      :missing ->
        [%{shard_id: row.shard_id, kind: :missing_object}]

      {:ok, uv} when is_integer(uv) and uv != row.schema_version ->
        fixed? =
          fix? and match?({:ok, _}, Directory.reconcile_schema_version(row.shard_id, uv))

        [
          %{
            shard_id: row.shard_id,
            kind: :schema_drift,
            from: row.schema_version,
            to: uv,
            fixed: fixed?
          }
        ]

      {:error, reason} ->
        [%{shard_id: row.shard_id, kind: :unreadable, fact: :schema, reason: reason}]

      _ ->
        []
    end
  end

  defp reconcile_token(row, fix?) do
    current = row.token_version || 0

    case Storage.read_token_floor(row.shard_id) do
      {:ok, floor} when is_integer(floor) and floor > current ->
        fixed? = fix? and Directory.raise_token_version(row.shard_id, floor) == 1
        [%{shard_id: row.shard_id, kind: :token_drift, from: current, to: floor, fixed: fixed?}]

      {:error, reason} ->
        [%{shard_id: row.shard_id, kind: :unreadable, fact: :token, reason: reason}]

      _ ->
        []
    end
  end

  defp reconcile_tombstones(deleted, fix?) do
    case Storage.tombstoned_ids() do
      {:ok, ids} ->
        for id <- ids, not MapSet.member?(deleted, id) do
          fixed? = fix? and match?({:ok, _}, Directory.tombstone(id))
          %{shard_id: id, kind: :orphan_tombstone, fixed: fixed?}
        end

      {:error, reason} ->
        [%{shard_id: "(tombstones)", kind: :unreadable, fact: :tombstones, reason: reason}]
    end
  end

  defp unreadable(:timeout), do: :timeout
  defp unreadable(reason), do: reason

  # The stored object's header `user_version`, via a RANGED read (`Storage.object_user_version/1`)
  # instead of pulling the whole object to a scratch file (expert review 2026-10-12 #19: at fleet
  # scale a DR sweep downloaded every shard in full, serially, to read four bytes). `:missing` when
  # the object is absent (a dangling directory row, or a steal sentinel — `{:absent, _}` never reads
  # as a version 0, expert review 2026-08-01 #24). Best-effort: a storage error yields
  # `{:error, _}`, which reconcile_schema reports as an `:unreadable` finding (never a false drift,
  # and never a silent skip: expert review 2026-10-12 #6). `{:header_unreachable, _}` takes the same
  # full-pull fallback as `Tenants.copied_version/1`.
  defp stored_user_version(shard_id) do
    case Tenants.copied_version(shard_id) do
      {:ok, uv} -> {:ok, uv}
      {:error, :dst_object_missing} -> :missing
      {:error, reason} -> {:error, reason}
    end
  end
end
