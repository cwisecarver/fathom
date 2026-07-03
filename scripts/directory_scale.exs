# Directory / control-plane scale probe: does the Postgres directory hold up at
# target fleet cardinality (default 3.1M shard rows)?
#
#     mix run scripts/directory_scale.exs [rows]
#
# Seeds N synthetic rows (ids `dirscale_…`, ~1% laggards behind HEAD, ~0.1%
# stale-`migrating`), then measures the real control-plane entry points at that
# table size — the same functions the request path, the Recorder, the hourly
# ReconcileJob, and a fleet rollout call:
#
#   * `Directory.resolve/1`        — the per-request upsert (p50/p99 of 1k random ids)
#   * `Directory.get/1`            — the pure read (p50/p99)
#   * `Directory.record_batch/1`   — one Recorder flush of 10k coalesced accesses
#   * `Directory.count_laggards/1` + `laggards/2` — the reconcile gauge + sweep page
#   * `Directory.active_recent/1`  — the warm-follower hot-set query
#   * `Directory.reclaim_stale_migrating/1` — the crash-reclaim UPDATE
#   * `Migrator.rollout/1`         — the fleet bulk-enqueue (chunked dedup + insert_all)
#
# The Oban :migrations queue is paused for the run (the enqueued jobs name
# nonexistent shards), and the script deletes everything it created — rows and
# jobs — before exiting. Requires the app's Postgres; does not touch shard files.

alias Fathom.{Directory, Repo}
import Ecto.Query

# Per-query debug logging would distort the timings (and drown the report).
Logger.configure(level: :info)

defmodule DirScale do
  def pad(i), do: String.pad_leading(Integer.to_string(i), 7, "0")
  def id(i), do: "dirscale_" <> pad(i)

  def percentile(sorted, p) do
    idx = min(round(p / 100 * length(sorted)), length(sorted) - 1)
    Enum.at(sorted, idx)
  end

  def time_each(ids, fun) do
    times = for id <- ids, do: elem(:timer.tc(fn -> fun.(id) end), 0)
    sorted = Enum.sort(times)
    {percentile(sorted, 50), percentile(sorted, 99)}
  end

  def ms(us), do: Float.round(us / 1000, 2)
end

rows =
  case System.argv() do
    [n] -> String.to_integer(n)
    _ -> 3_100_000
  end

batch = 5_000
IO.puts("== directory scale probe: #{rows} rows ==")

# Never let the measured enqueues execute — they name nonexistent shards.
Oban.pause_queue(queue: :migrations)

# Clean any leftovers from a previous crashed run.
Repo.delete_all(from(s in "shards", where: like(s.shard_id, "dirscale%")))

now = DateTime.utc_now() |> DateTime.truncate(:microsecond)
stale = DateTime.add(now, -7200, :second)

{seed_us, _} =
  :timer.tc(fn ->
    0..(rows - 1)
    |> Stream.chunk_every(batch)
    |> Stream.with_index()
    |> Enum.each(fn {chunk, bi} ->
      entries =
        for i <- chunk do
          # ~1% laggards (v1 behind HEAD=2); ~0.1% mid-migration, half of those stale.
          {version, status, migrating_since} =
            cond do
              rem(i, 1000) == 0 -> {1, "migrating", stale}
              rem(i, 1000) == 1 -> {1, "migrating", now}
              rem(i, 100) < 1 -> {1, "active", nil}
              true -> {2, "active", nil}
            end

          %{
            shard_id: DirScale.id(i),
            schema_version: version,
            status: status,
            migrating_since: migrating_since,
            last_active_at: DateTime.add(now, -rem(i, 86_400), :second),
            inserted_at: now,
            updated_at: now
          }
        end

      Repo.insert_all("shards", entries)
      if rem(bi + 1, 100) == 0, do: IO.puts("  seeded #{(bi + 1) * batch}…")
    end)
  end)

IO.puts(
  "seeded #{rows} rows in #{Float.round(seed_us / 1_000_000, 1)}s (#{round(rows / (seed_us / 1_000_000))} rows/s)\n"
)

sample = fn n -> for _ <- 1..n, do: DirScale.id(:rand.uniform(rows) - 1) end

# -- per-request paths ---------------------------------------------------------
{p50, p99} = DirScale.time_each(sample.(1_000), &Directory.resolve/1)
IO.puts("resolve (per-request upsert)     p50 #{DirScale.ms(p50)}ms  p99 #{DirScale.ms(p99)}ms")

{p50, p99} = DirScale.time_each(sample.(1_000), &Directory.get/1)
IO.puts("get (pure read)                  p50 #{DirScale.ms(p50)}ms  p99 #{DirScale.ms(p99)}ms")

# record_batch's contract is UNIQUE ids per flush (the Recorder coalesces repeated
# accesses in its ETS buffer before flushing), so dedupe the random sample.
flush_ids = sample.(10_000) |> Enum.uniq()

{flush_us, written} =
  :timer.tc(fn ->
    Directory.record_batch(for id <- flush_ids, do: {id, now})
  end)

IO.puts(
  "record_batch (Recorder flush)    #{DirScale.ms(flush_us)}ms for #{written} coalesced accesses"
)

# -- control-plane sweeps ------------------------------------------------------
{count_us, laggard_count} = :timer.tc(fn -> Directory.count_laggards(2) end)
IO.puts("count_laggards (reconcile gauge) #{DirScale.ms(count_us)}ms → #{laggard_count} laggards")

{page_us, page} = :timer.tc(fn -> Directory.laggards(2, 100) end)
IO.puts("laggards page (sweep cursor)     #{DirScale.ms(page_us)}ms → #{length(page)} rows")

{recent_us, recent} = :timer.tc(fn -> Directory.active_recent(500) end)
IO.puts("active_recent (warm follower)    #{DirScale.ms(recent_us)}ms → #{length(recent)} rows")

{reclaim_us, reclaimed} = :timer.tc(fn -> Directory.reclaim_stale_migrating(3_600) end)

IO.puts(
  "reclaim_stale_migrating (UPDATE) #{DirScale.ms(reclaim_us)}ms → #{length(reclaimed)} reclaimed"
)

# -- rollout bulk-enqueue: the REAL production path -----------------------------
# Migrator.rollout(limit) → laggards fetch → chunked dedup + chunked Oban.insert_all
# (one unpartitioned insert_all crashed past ~7,281 jobs on the 65,535-bind-param
# cap — this run pins the chunked fix at fleet scale). Needs a fleet HEAD, so
# release a synthetic one; cleanup removes it and every job this enqueue created
# (tracked by job-id watermark — laggards legitimately includes any real
# behind-HEAD shards too, e.g. dev's demo tenants, so no id-prefix guesswork).
{:ok, _} = Fathom.Migrator.release(2, "dirscale-probe", ["SELECT 1"])
job_watermark = Repo.one(from(j in Oban.Job, select: max(j.id))) || 0

{rollout_us, {:ok, enqueued}} = :timer.tc(fn -> Fathom.Migrator.rollout(10_000) end)

IO.puts(
  "rollout(10_000) enqueue          #{DirScale.ms(rollout_us)}ms → #{enqueued} jobs (chunked dedup + insert_all)"
)

# -- footprint -----------------------------------------------------------------
%{rows: [[table_sz, index_sz]]} =
  Repo.query!(
    "SELECT pg_size_pretty(pg_relation_size('shards')), pg_size_pretty(pg_indexes_size('shards'))"
  )

IO.puts("shards table #{table_sz}, indexes #{index_sz}")

# -- cleanup -------------------------------------------------------------------
IO.puts("\ncleaning up…")

{_, njobs} =
  :timer.tc(fn ->
    Repo.delete_all(from(j in Oban.Job, where: j.id > ^job_watermark))
  end)

Repo.delete_all(
  from(m in "shard_migrations", where: m.version == 2 and m.name == "dirscale-probe")
)

{del_us, {nrows, _}} =
  :timer.tc(fn -> Repo.delete_all(from(s in "shards", where: like(s.shard_id, "dirscale%"))) end)

Oban.resume_queue(queue: :migrations)
{njobs_count, _} = njobs

IO.puts(
  "deleted #{nrows} rows (#{Float.round(del_us / 1_000_000, 1)}s) + #{njobs_count} jobs; queue resumed"
)
