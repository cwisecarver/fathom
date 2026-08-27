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

    # The FULL drill (#48) runs after, and on its own much smaller sample: it forks each shard to a
    # scratch tenant, so it costs a real object copy per sample where the read-only drill costs a
    # GET. Separately gated so raising read-only coverage never silently multiplies storage I/O.
    case full_sample_size() do
      n when is_integer(n) and n > 0 -> run_full_drill(n)
      _ -> :ok
    end

    :ok
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

    # Snapshot verdicts reach the DURABLE column too (expert review 2026-08-26 #37).
    #
    # `record_verification/2` used to run here, BEFORE `verify_snapshots/1`, and the snapshot
    # statuses were discarded. So a shard whose only snapshot was corrupt durably recorded
    # `last_verify_status = "ok"` — and this module's moduledoc claims "a failure is queryable",
    # which held for the live object only. A DR audit query therefore returned a clean fleet while
    # snapshots rotted.
    snapshot_status = verify_snapshots(id)
    overall = worst_status(status, snapshot_status)

    Directory.record_verification(id, Atom.to_string(overall))
    :telemetry.execute([:fathom, :restore_drill, :result], %{count: 1}, %{status: overall})
    overall
  end

  # `:ok` only when BOTH the live object and every snapshot are clean, so a corrupt live object is
  # never masked by healthy snapshots or the reverse.
  #
  # FAILS SAFE ON AN UNKNOWN STATUS. The first draft of this used
  # `Enum.find(@status_rank, :ok, ...)`, which returned `:ok` for anything not in the list — and
  # the list was incomplete, because `verify/2`, `check_schema/2` and `restore_one/1` between them
  # also produce `:sentinel`, `:fork_failed` and `:restored_mismatch`. A status this function had
  # not heard of would have been silently recorded as healthy, which is the same
  # swallow-a-failure-as-ok defect #37 is about, reintroduced inside its own fix. The clauses below
  # treat "not `:ok`" as the answer regardless of whether the rank list knows the atom, so adding a
  # new status elsewhere can never downgrade it to healthy here.
  @status_rank [
    :corrupt,
    :restored_mismatch,
    :fork_failed,
    :error,
    :list_failed,
    :sentinel,
    :absent
  ]

  defp worst_status(:ok, :ok), do: :ok
  defp worst_status(:ok, other), do: other
  defp worst_status(other, :ok), do: other

  defp worst_status(a, b) do
    # Neither is :ok. Prefer the more severe of the two; an atom the rank does not list still wins
    # over :ok because of the clauses above, and defaults to `a` here rather than to anything
    # healthy.
    Enum.find(@status_rank, a, fn s -> s == a or s == b end)
  end

  # Snapshots had NO health signal at all (expert review 2026-08-01 #48). `sample_for_drill/1`
  # samples directory rows, and a snapshot is a storage object (`@snap-<id>`) with no row — so the
  # one class of stored data an operator would reach for during a point-in-time recovery was the
  # one class nothing ever checked. A corrupt snapshot would be discovered at exactly the moment it
  # was needed, which is the worst possible discovery time and the reason the drill exists at all.
  #
  # Verified alongside the shard that owns them rather than through a separate sampler: they share
  # the shard's rotation, so coverage follows the same least-recently-verified order for free, and a
  # shard with no snapshots costs one LIST.
  defp verify_snapshots(id) do
    # `list_snapshots/1` returns `{:ok, [%{id: ..., bytes: ...}]}`. An earlier draft matched a bare
    # list, so the clause never fired and snapshot verification silently did nothing — the exact
    # shape of bug this whole finding is about, caught only because the test corrupts a snapshot and
    # demands the drill notice.
    case Storage.list_snapshots(id) do
      {:ok, snapshots} when is_list(snapshots) ->
        snapshots
        |> Enum.map(fn %{id: snapshot_id} -> verify_snapshot(id, snapshot_id) end)
        |> Enum.reduce(:ok, &worst_status/2)

      # A FAILED LIST IS NOT A CLEAN SHARD (expert review 2026-08-26 #37). This used to match `_`
      # and return `:ok` with no log, no telemetry and no counter — so a bucket-policy change, an
      # endpoint misconfig or a credential rotation could kill snapshot verification FLEET-WIDE
      # while every run reported the shard fine. AGENTS.md records the same class from `bump/1`
      # rescuing to `:ok`, and the comment above records an earlier draft of THIS function doing
      # it too.
      other ->
        list_failed(id, other)
    end
  rescue
    e -> list_failed(id, {:raised, e})
  catch
    :exit, reason -> list_failed(id, {:exit, reason})
  end

  defp list_failed(id, reason) do
    Logger.error(
      "restore drill: could not LIST snapshots for #{id} (#{inspect(reason)}) — snapshot " <>
        "verification did not run, so this shard's point-in-time recovery is UNVERIFIED"
    )

    :telemetry.execute(
      [:fathom, :restore_drill, :snapshot_result],
      %{count: 1},
      %{status: :list_failed, shard_id: id, snapshot_id: nil}
    )

    :list_failed
  end

  defp verify_snapshot(id, snapshot_id) do
    tmp =
      Path.join(
        System.tmp_dir!(),
        "fathom_snapdrill_#{id}_#{System.unique_integer([:positive])}.db"
      )

    status =
      try do
        case Storage.pull_snapshot(id, snapshot_id, tmp) do
          {:ok, _} ->
            if File.exists?(tmp) do
              case Shard.verify_integrity(tmp) do
                :ok -> :ok
                {:error, _} -> :corrupt
              end
            else
              :absent
            end

          _ ->
            :error
        end
      rescue
        _ -> :error
      catch
        :exit, _ -> :error
      after
        for suffix <- ["", "-wal", "-shm"], do: File.rm(tmp <> suffix)
      end

    if status != :ok do
      Logger.error(
        "restore drill: SNAPSHOT #{snapshot_id} of #{id} is #{status} — a point-in-time recovery " <>
          "that reached for it would fail"
      )
    end

    :telemetry.execute(
      [:fathom, :restore_drill, :snapshot_result],
      %{count: 1},
      %{status: status, shard_id: id, snapshot_id: snapshot_id}
    )

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

  defp full_sample_size, do: Application.get_env(:fathom, :restore_drill_full_sample)

  @doc """
  Drill the **procedure**, not just the object (expert review 2026-08-01 #48).

  `verify/2` above pulls, `quick_check`s, and compares `user_version` — that is the whole drill, and
  it proves the stored bytes are readable. What it never did was RESTORE: it does not invoke
  `Tenants.fork/2`, does not cold-open a coordinator from the pulled bytes, and does not touch the
  directory reconcile. So the multi-step chain an operator actually runs under maximum pressure —
  fork to a scratch id, verify it, drop it — had only ever executed in unit tests against
  `Storage.Local`, never against the real backend a real incident would use.

  "A backup you haven't restored is a hypothesis" was the moduledoc's own framing; this closes the
  gap between verifying a file and rehearsing the recovery.

  Each sampled shard is forked to a **throwaway id**, the fork is opened and row-counted against the
  source, and then deleted. Deliberately small and off by default (`:restore_drill_full_sample`): a
  fork is a full object copy plus a directory row, so this costs real storage I/O per sample in a way
  the read-only drill does not.

  Failure classes are distinct from `verify/2`'s on purpose — `:fork_failed` and `:restored_mismatch`
  say the RECOVERY PATH is broken, which is a different alarm from "this stored object is corrupt"
  and wants a different response.
  """
  @spec run_full_drill(pos_integer()) :: {:ok, map()}
  def run_full_drill(n) when is_integer(n) and n > 0 do
    results = Enum.map(Directory.sample_for_drill(n), &restore_one/1)
    summary = Enum.frequencies(results)
    fails = Enum.count(results, &(&1 != :ok))

    Logger.info("restore drill (FULL): #{length(results)} shard(s): #{inspect(summary)}")

    if fails > 0 do
      Logger.error(
        "restore drill (FULL): #{fails} shard(s) could not be RESTORED (#{inspect(summary)}) — " <>
          "the stored bytes may be fine; it is the recovery procedure that is broken"
      )
    end

    {:ok, summary}
  end

  defp restore_one(%{shard_id: id}) do
    scratch = "restoredrill#{System.unique_integer([:positive])}"
    status = restore_and_compare(id, scratch)

    :telemetry.execute(
      [:fathom, :restore_drill, :full_result],
      %{count: 1},
      %{status: status, shard_id: id}
    )

    status
  end

  defp restore_and_compare(id, scratch) do
    case Fathom.Tenants.fork(id, scratch) do
      {:ok, _} ->
        compare_then_drop(id, scratch)

      {:error, reason} ->
        Logger.warning(
          "restore drill (FULL): fork of #{id} -> #{scratch} failed: #{inspect(reason)}"
        )

        :fork_failed
    end
  rescue
    e ->
      Logger.warning("restore drill (FULL): #{id} raised (#{inspect(e)})")
      :fork_failed
  catch
    :exit, reason ->
      Logger.warning("restore drill (FULL): #{id} exited (#{inspect(reason)})")
      :fork_failed
  end

  # The comparison that makes the rehearsal mean something: a fork that produced an EMPTY database
  # would pass `quick_check` and prove nothing. Compare the restored copy's user table row counts to
  # the source's — if they disagree, the recovery path silently loses data, which is the failure
  # this drill exists to catch before an incident does.
  defp compare_then_drop(id, scratch) do
    try do
      case {table_counts(id), table_counts(scratch)} do
        {{:ok, src}, {:ok, dst}} when src == dst -> :ok
        {{:ok, _src}, {:ok, _dst}} -> :restored_mismatch
        _ -> :fork_failed
      end
    after
      drop_scratch(scratch)
    end
  end

  # {table_name => row_count} for the shard's USER tables, read through a normal checkout so the
  # read goes down the same path a tenant's would. `sqlite_%` is excluded: internal tables differ
  # between a freshly-written copy and a long-lived original for reasons that are not data loss.
  defp table_counts(shard_id) do
    with {:ok, pid, ref, path} <- Fathom.Shards.checkout(shard_id) do
      try do
        {:ok, conn} = Connection.open(path)

        try do
          {:ok, %{rows: rows}} =
            Connection.query(
              conn,
              "SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%' ORDER BY name",
              []
            )

          counts =
            Map.new(rows, fn [table] ->
              {:ok, %{rows: [[n]]}} = Connection.query(conn, count_sql(table), [])
              {table, n}
            end)

          {:ok, counts}
        after
          Connection.close(conn)
        end
      after
        Fathom.Shard.checkin(pid, ref)
      end
    end
  rescue
    _ -> :error
  catch
    :exit, _ -> :error
  end

  # Clean up the scratch tenant. Deliberately NOT `Tenants.delete/1`, which is the supported way to
  # remove a real tenant and therefore TOMBSTONES: a permanent row plus an entry in the public
  # `Fathom.Tenants.Tombstones` ETS set that admission checks on every checkout. One tombstone per
  # sample per drill run would grow that set without bound, so the routine that exists to prove
  # recovery works would slowly degrade the admission path — a self-inflicted version of exactly
  # the kind of unbounded growth #36 was about.
  #
  # A drill fork was never a tenant: nothing was ever routed to it and no client ever held a token
  # for it, so there is nothing to tombstone AGAINST. Stop the coordinator, purge the objects, drop
  # the directory row. Best-effort — a leaked scratch is a tidiness problem, and raising here would
  # turn a successful rehearsal into a reported failure.
  defp drop_scratch(scratch) do
    _ = Fathom.Shards.stop(scratch)
    _ = Storage.purge_shard(scratch)
    _ = Directory.hard_delete(scratch)
    :ok
  rescue
    e ->
      Logger.warning("restore drill (FULL): scratch cleanup of #{scratch} failed: #{inspect(e)}")
  catch
    :exit, r ->
      Logger.warning("restore drill (FULL): scratch cleanup of #{scratch} exited: #{inspect(r)}")
  end

  # A table NAME cannot be a bound parameter in SQLite, so this is the one place the drill builds
  # SQL by interpolation. The name comes from `sqlite_master` (not from a client), and it is quoted
  # as an IDENTIFIER with embedded quotes doubled — the standard escape — so an exotic-but-legal
  # table name cannot terminate the quoting early. AGENTS.md's rule is about never interpolating
  # VALUES; identifiers have no parameter form.
  defp count_sql(table) do
    quoted = String.replace(table, "\"", "\"\"")
    "SELECT count(*) FROM \"" <> quoted <> "\""
  end
end
