defmodule Fathom.RestoreDrillJob do
  @moduledoc """
  Automated sampled restore drill (expert review #24) — "a backup you haven't restored is a
  hypothesis," run continuously against the cold tail.

  The upload/download MD5 chain verifies *transport* against the upload-time hash; it cannot detect
  that a durable object went absent, was clobbered, is a stranded steal **sentinel** at the data key,
  or is a valid-checksum file of the wrong lineage. Cold-tail tenants (the majority at "millions of
  shards") may go months between opens, so their only copy's health is otherwise unobserved until the
  tenant returns — the worst discovery time.

  A fleet-singleton Oban cron (peer leadership, like `ReconcileJob`) that each run samples the
  least-recently-verified **active** shards (`Fathom.Directory.sample_for_drill/1`), pulls each
  durable object, and classifies it:

    * `:ok`              — pulled, `quick_check`-clean, and its `PRAGMA user_version` matches the
                           directory's `schema_version`.
    * `:corrupt`         — pulled but `quick_check` failed (a bad stored object).
    * `:schema_mismatch` — clean, but `user_version` disagrees with the directory (the three-place
                           version stamp diverged).
    * `:sentinel`        — a steal-time brand-new sentinel sits at the data key (a stealer died
                           before its first flush): the "backup" is a placeholder, not data.
    * `:absent`          — no stored object at all (a never-flushed brand-new tenant, or a lost object).
    * `:error`           — the pull failed (transport / reachability).

  Each result stamps `last_verified_at` + `last_verify_status` on the row (so the sample rotates and a
  failure is queryable) and emits `[:fathom, :restore_drill, :result]` telemetry (an alert fires on
  `corrupt` / `schema_mismatch` / `error`). This is also the DR runbook's verification step (#6).

  **Gated + off by default.** Inert unless `:restore_drill_sample` is a positive integer (the per-run
  sample size) — set it (and the daily `Oban` crontab entry's cadence) to size drill coverage to your
  fleet + S3 GET budget. Read-only: it only pulls objects, never writes them.
  """
  use Oban.Worker, queue: :migrations, max_attempts: 1

  require Logger

  alias Fathom.{Directory, Shard}
  alias Fathom.Shard.{Connection, Storage}

  # Outcomes that mean the durable object is bad/unreachable (the alertable set) vs merely
  # "no data yet" (absent/sentinel — a brand-new tenant that never flushed).
  @failures [:corrupt, :schema_mismatch, :error]

  @impl Oban.Worker
  def perform(%Oban.Job{}) do
    case sample_size() do
      n when is_integer(n) and n > 0 -> run_drill(n)
      _ -> :ok
    end
  end

  @doc """
  Drill up to `n` least-recently-verified active shards now and return the outcome frequencies.
  Public so an operator can run it from a node console (`Fathom.RestoreDrillJob.run_drill(50)`) and
  so tests drive it directly.
  """
  @spec run_drill(pos_integer()) :: {:ok, map()}
  def run_drill(n) when is_integer(n) and n > 0 do
    results = Enum.map(Directory.sample_for_drill(n), &drill_one/1)
    summary = Enum.frequencies(results)
    fails = Enum.count(results, &(&1 in @failures))

    Logger.info("restore drill: verified #{length(results)} shard(s): #{inspect(summary)}")

    if fails > 0,
      do:
        Logger.error("restore drill: #{fails} shard(s) FAILED verification (#{inspect(summary)})")

    {:ok, summary}
  end

  defp drill_one(%{shard_id: id, schema_version: schema_version}) do
    status = verify(id, schema_version)
    Directory.record_verification(id, Atom.to_string(status))
    :telemetry.execute([:fathom, :restore_drill, :result], %{count: 1}, %{status: status})
    status
  end

  defp verify(id, schema_version) do
    tmp =
      Path.join(System.tmp_dir!(), "fathom_drill_#{id}_#{System.unique_integer([:positive])}.db")

    try do
      case Storage.pull(id, tmp) do
        # No bytes written — a never-flushed brand-new tenant, a lost object, or a steal
        # sentinel (expert review 2026-08-01 #24; a sentinel used to read as a healthy pull
        # of an empty database, so the drill passed on nothing).
        {:absent, _} ->
          :absent

        {:ok, _etag} ->
          # A sentinel writes NO local file (Storage.pull maps {:sentinel,_} -> {:ok, etag}); a real
          # object does. So {:ok, etag} + a missing temp is a sentinel-at-data-key.
          if File.exists?(tmp) do
            case Shard.verify_integrity(tmp) do
              :ok -> check_schema(id, tmp, schema_version)
              {:error, _reason} -> :corrupt
            end
          else
            :sentinel
          end

        {:error, reason} ->
          Logger.warning("restore drill: pull failed for #{id} (#{inspect(reason)})")
          :error
      end
    rescue
      e ->
        Logger.warning("restore drill: #{id} raised (#{inspect(e)})")
        :error
    after
      for s <- ["", "-wal", "-shm"], do: File.rm(tmp <> s)
    end
  end

  defp check_schema(id, tmp, schema_version) do
    case read_user_version(tmp) do
      ^schema_version ->
        :ok

      other ->
        Logger.warning(
          "restore drill: #{id} schema mismatch (object user_version=#{inspect(other)} vs " <>
            "directory schema_version=#{schema_version})"
        )

        :schema_mismatch
    end
  end

  defp read_user_version(path) do
    case Connection.open(path) do
      {:ok, conn} ->
        version =
          case Connection.query(conn, "PRAGMA user_version", []) do
            {:ok, %{rows: [[v]]}} -> v
            _ -> nil
          end

        Connection.close(conn)
        version

      _ ->
        nil
    end
  end

  defp sample_size, do: Application.get_env(:fathom, :restore_drill_sample)
end
