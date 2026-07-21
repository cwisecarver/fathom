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
  alias Fathom.Shard.{Connection, Storage}

  @type finding :: %{
          required(:shard_id) => String.t(),
          required(:kind) => atom(),
          optional(any) => any
        }

  @spec run(keyword()) :: [finding()]
  def run(opts \\ []) do
    fix? = Keyword.get(opts, :fix, false)
    rows = Directory.all() |> apply_limit(Keyword.get(opts, :limit))
    deleted = MapSet.new(for r <- rows, r.status == "deleted", do: r.shard_id)

    Enum.flat_map(rows, &reconcile_row(&1, fix?)) ++ reconcile_tombstones(deleted, fix?)
  end

  defp apply_limit(rows, nil), do: rows
  defp apply_limit(rows, n) when is_integer(n) and n > 0, do: Enum.take(rows, n)
  defp apply_limit(rows, _), do: rows

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

      {:error, _} ->
        []
    end
  end

  # Pull the shard's stored object to a temp and read its header `user_version`. `:missing` when the
  # object is absent (a dangling directory row). Best-effort: a transient storage/open error yields
  # `{:error, _}`, which reconcile_schema treats as "skip" rather than a false drift.
  defp stored_user_version(shard_id) do
    tmp =
      Path.join(
        System.tmp_dir!(),
        "fathom_reconcile_#{shard_id}_#{System.unique_integer([:positive])}.db"
      )

    try do
      case Storage.pull(shard_id, tmp) do
        {:ok, nil} -> :missing
        {:ok, _etag} -> read_user_version(tmp)
        {:error, reason} -> {:error, reason}
      end
    after
      for s <- ["", "-wal", "-shm"], do: File.rm(tmp <> s)
    end
  end

  defp read_user_version(path) do
    case Connection.open(path) do
      {:ok, conn} ->
        result =
          case Connection.query(conn, "PRAGMA user_version", []) do
            {:ok, %{rows: [[v]]}} when is_integer(v) -> {:ok, v}
            other -> {:error, other}
          end

        Connection.close(conn)
        result

      {:error, reason} ->
        {:error, reason}
    end
  end
end
