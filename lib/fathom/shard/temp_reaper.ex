defmodule Fathom.Shard.TempReaper do
  @moduledoc """
  Amortized janitor for orphaned shard temp files.

  Externally-killed pulls/snapshots (`Task.shutdown` brutal_kill, pull timeouts, a hard
  coordinator crash) strand uniquely-suffixed `.dl.*` / `.snap.*` / `.tmp.*` temps next
  to the shard's `.db` in the shard data dir — an unbounded, shard-sized disk leak
  (expert review round-2 #27) that eats the density budget, and one no fixed-suffix
  sweeper can enumerate without listing the directory.

  This cleanup used to run on the cold-open hot path (`Fathom.Shard.handle_continue/2`
  called `Storage.reap_stale_temps/2`), but `Path.wildcard` can't prefix-optimize a
  pattern whose filename component holds a `*`, so it full-`readdir`d the FLEET-sized
  shard data dir on EVERY open — tens-to-hundreds of ms at 30k–105k shards, re-paid on
  every idle re-open (expert review 2026-07-14 #2). So the coordinator now clears only
  its own DETERMINISTIC `.pull` family O(1) by direct name, and this process does the
  one directory scan for the uniquely-suffixed orphans ONCE every
  `:temp_reaper_interval_ms` (default #{div(300_000, 60_000)} minutes), amortized across
  all opens. Coverage is strictly better than the old per-open reap: a shard that
  strands a temp and never re-opens is still swept within the interval.

  Age-gated to `2 * Fathom.Shard`'s pull timeout so a concurrent pull/snapshot's live
  temp is never touched. Gated `:temp_reaper` (default on; off in test, where tests
  drive `sweep/0` synchronously).
  """
  use GenServer

  require Logger

  alias Fathom.Admin.Measurements
  alias Fathom.Shard
  alias Fathom.Shard.Storage

  @default_interval_ms 300_000
  # Match the coordinator's per-open age gate (2 * Fathom.Shard @pull_timeout of 60_000).
  @stale_after_ms 120_000
  # Expert review #23: age-cap on quarantine files (.db.fenced/.forked/.corrupt). They preserve
  # acked-but-unflushed / corrupt local copies for recovery, but without retention they leak on
  # local disk forever (swept only by a tenant delete). 30 days gives an operator a generous window
  # to recover via `mix fathom.shard quarantines`; set `:quarantine_retention_ms` to 0 to keep
  # forever. Deletion only touches files older than the cap — a fresh quarantine is never swept.
  @default_quarantine_retention_ms 30 * 24 * 60 * 60 * 1000

  # Expert review 2026-10-12 #17: shard-sized temps that live OUTSIDE the shard data dir (snapshot
  # restore, restore drill, DR reconcile, migration copies, tenant exports, the S3 restore temp).
  # Their try/after cleanup does not run on a brutal kill, OOM or node death, so they leaked
  # forever. They all live under this one fathom-owned subdirectory, which `sweep/0` reaps.
  #
  # The gate is an hour, not `@stale_after_ms`: a scratch entry can be a DIRECTORY (a tenant export
  # held while the file is streamed to the operator) whose mtime does not advance while it is read,
  # and these jobs legitimately run for minutes. An hour is still a bounded leak.
  @scratch_stale_after_ms 3_600_000
  # Free-space floor for the scratch volume before a scratch pull (expert review 2026-10-13 #37).
  @default_scratch_min_free_bytes 1024 * 1024 * 1024

  @doc """
  The fathom-owned scratch directory for shard-sized temp files outside the shard data dir
  (created on demand). Put every such temp here so `sweep/0` can reap it after a kill.
  """
  #
  # Expert review 2026-10-12 #25: `mkdir_p!` alone leaves the directory at the process umask
  # (0755), and the shard-sized temps inside it (0644) were readable by any local user, so the
  # tenant data in a restore or export temp was world-readable. The mode is set explicitly on every
  # call, not only on create, so a directory left 0755 by an earlier build is tightened on the next
  # use.
  #
  # expert review 2026-10-13 #37: the location is `:fathom_scratch_dir` when set (point it at the
  # data volume, where the free-space budget is watched), and `System.tmp_dir!()` otherwise.
  @spec scratch_dir() :: Path.t()
  def scratch_dir do
    dir =
      Application.get_env(:fathom, :fathom_scratch_dir) ||
        Path.join(System.tmp_dir!(), "fathom_scratch")

    File.mkdir_p!(dir)
    File.chmod!(dir, 0o700)
    dir
  end

  # expert review 2026-10-13 #37: a scratch pull writes a shard-sized file, and the scratch volume was
  # never checked. Refuse before a pull when the scratch volume has less than `:scratch_min_free_bytes`
  # free (default 1 GiB, a floor: the object size is not known before the pull). The reading comes
  # from `Admin.Measurements.disk_info/1`, not its poller cache: that cache tracks only the data volume,
  # and the scratch dir is normally on the system temp volume. `disksup` runs `df` per call, which is
  # acceptable here because scratch pulls are rare admin and DR operations, never the request path.
  # An unreadable volume fails OPEN: the pull then either succeeds or fails on its own, as before.
  @spec check_scratch_headroom() :: :ok | {:error, {:scratch_low_disk, integer(), integer()}}
  def check_scratch_headroom do
    min = Application.get_env(:fathom, :scratch_min_free_bytes, @default_scratch_min_free_bytes)

    case Measurements.disk_info(scratch_dir()) do
      {:ok, %{free_bytes: free}} when free < min ->
        Logger.warning(
          "scratch volume low on space: #{free} bytes free, floor #{min}; refusing the scratch pull"
        )

        {:error, {:scratch_low_disk, free, min}}

      _ ->
        :ok
    end
  end

  @doc "Start the reaper (registered under the module name)."
  def start_link(opts \\ []), do: GenServer.start_link(__MODULE__, opts, name: __MODULE__)

  @doc """
  Run one sweep synchronously and return the number of orphaned temps reaped. The test
  seam (the periodic timer is gated off in test); safe to call at any time.
  """
  @spec sweep() :: non_neg_integer()
  def sweep, do: GenServer.call(__MODULE__, :sweep)

  @impl true
  def init(_opts) do
    # Sweep once at boot (a previous incarnation's crash may have stranded temps),
    # then on the timer. The scan runs off any request path, so a fleet-sized dir is fine.
    {:ok, %{}, {:continue, :sweep}}
  end

  @impl true
  def handle_continue(:sweep, state) do
    do_sweep()
    {:noreply, schedule(state)}
  end

  @impl true
  def handle_info(:sweep, state) do
    do_sweep()
    {:noreply, schedule(state)}
  end

  @impl true
  def handle_call(:sweep, _from, state), do: {:reply, do_sweep(), state}

  # One directory scan of the shard data dir for stale, uniquely-suffixed orphan temps.
  #
  # RESCUED because this is a best-effort janitor and a raise here is catastrophic out of all
  # proportion (expert review 2026-08-31 #17). It runs from `handle_continue(:sweep)` at init and
  # from the periodic `:sweep`, so a raise — a persistent local-FS fault on the fleet-sized
  # `Path.wildcard` / `File.stat` sweep — crash-loops the reaper; the DataPlane's 30-in-10s restart
  # budget absorbs a transient burst but a persistent fault exhausts it and cascades to EVERY open
  # coordinator, inverting "a wobble in one plane can't restart another". Every other reap path in
  # the codebase rescues; this now does too. Returns 0 on a failed pass — the next interval retries.
  defp do_sweep do
    # ONE directory listing for every family -- the orphan temps and the three quarantine kinds
    # used to be four separate `Path.wildcard` readdirs of the fleet-sized dir (expert review
    # 2026-10-10 #P6). A listing that fails reads as an empty dir: this pass is a no-op, the next
    # interval retries.
    dir = Shard.data_dir()

    names =
      case File.ls(dir) do
        {:ok, names} -> names
        {:error, _} -> []
      end

    temps = for name <- names, Storage.temp_orphan?(name), do: Path.join(dir, name)
    reaped = Storage.reap_named_temps(temps, @stale_after_ms)
    if reaped > 0, do: Logger.info("shard temp reaper: reaped #{reaped} orphaned temp(s)")
    sweep_quarantines(Shard.quarantine_files_in(names, dir))
    reaped + sweep_scratch()
  rescue
    e ->
      Logger.warning(
        "shard temp reaper: sweep raised (#{Exception.message(e)}); skipping this pass"
      )

      0
  catch
    :exit, reason ->
      Logger.warning("shard temp reaper: sweep exited (#{inspect(reason)}); skipping this pass")
      0
  end

  # Age-gated reap of the scratch dir (see `scratch_dir/0`). Entries may be files or directories.
  defp sweep_scratch do
    dir = scratch_dir()
    cutoff = System.system_time(:second) - div(@scratch_stale_after_ms, 1000)

    case File.ls(dir) do
      {:ok, names} ->
        Enum.count(names, fn name ->
          path = Path.join(dir, name)

          match?({:ok, %File.Stat{mtime: m}} when m < cutoff, File.stat(path, time: :posix)) and
            match?({:ok, _}, File.rm_rf(path))
        end)

      {:error, _} ->
        0
    end
  end

  # Quarantine inventory gauge + age-capped retention (expert review #23). The coordinator preserves
  # acked-but-unflushed / corrupt local copies in `.db.fenced/.forked/.corrupt` files; without
  # retention they leak on local disk forever. Emit a GAUGE of the standing count (an alert can fire
  # on a growing backlog — complements the per-event `[:fathom,:shard,:fenced_quarantine]` counter),
  # then delete any older than the retention cap (default 30d; 0 keeps forever). Returns the count.
  defp sweep_quarantines(files) do
    :telemetry.execute([:fathom, :shard, :quarantines], %{count: length(files)}, %{})

    case quarantine_retention_ms() do
      ms when is_integer(ms) and ms > 0 ->
        now = System.system_time(:millisecond)
        removed = for f <- files, quarantine_age_ms(f, now) > ms, File.rm(f) == :ok, do: f

        if removed != [],
          do:
            Logger.info(
              "shard temp reaper: swept #{length(removed)} quarantine file(s) past retention"
            )

      _ ->
        :ok
    end

    length(files)
  end

  # Age from the QUARANTINE time embedded in the name — `<id>.db.<kind>.<ms>[-<unique>][-wal|-shm]`
  # — not the mtime (expert review 2026-09-29 #32). A quarantine is a RENAME, and rename preserves
  # mtime, so mtime is when the content was last WRITTEN: a `.forked`/`.corrupt` copy of a shard idle
  # for more than the retention window was swept within one reaper interval of being quarantined —
  # the recovery copy gone before anyone could look. The old comment here claimed mtime was the
  # creation time; it is not. The `-wal`/`-shm` siblings carry the same `<ms>`, so a recovery set
  # ages as a unit instead of being split by their independent mtimes.
  #
  # A name without the stamp (none is written today) falls back to mtime; unstattable ⇒ age 0 ⇒
  # never swept (fail-safe: keep the data).
  @quarantine_stamp ~r/\.db\.(?:fenced|forked|corrupt)\.(\d+)(?:-\d+)?(?:-wal|-shm)?$/

  @doc """
  Milliseconds since `file` was QUARANTINED, read from the stamp in its name (mtime only as a
  fallback — a rename preserves it, so it is the content's age, not the quarantine's). Shared with
  `mix fathom.shard quarantines` so the listing an operator reads agrees with what retention does.
  """
  @spec quarantine_age_ms(Path.t(), integer()) :: number()
  def quarantine_age_ms(file, now_ms) do
    case Regex.run(@quarantine_stamp, Path.basename(file), capture: :all_but_first) do
      [ms] ->
        now_ms - String.to_integer(ms)

      nil ->
        case File.stat(file, time: :posix) do
          {:ok, %File.Stat{mtime: mtime_sec}} -> now_ms - mtime_sec * 1000
          _ -> 0
        end
    end
  end

  defp quarantine_retention_ms,
    do: Application.get_env(:fathom, :quarantine_retention_ms, @default_quarantine_retention_ms)

  defp schedule(state) do
    Process.send_after(self(), :sweep, interval_ms())
    state
  end

  defp interval_ms,
    do: Application.get_env(:fathom, :temp_reaper_interval_ms, @default_interval_ms)
end
