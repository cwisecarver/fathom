# Fleet-migration wall-clock probe: what does a schema change cost per shard, and
# how does rollout throughput scale with Oban concurrency?
#
#     mix run scripts/migration_scale.exs [shards] [size_mb]
#
# Seeds N realistic shards (default 2,020 × 4MB — an `items` table at
# user_version 1, cloned from one template), registers them in the directory at
# v1, releases a fleet v2 whose transform does real O(rows) work (ALTER + CREATE
# INDEX — the same shape the copy bench uses), then measures the REAL blue/green
# machinery end to end (drain → lease+renewer → pull → retain → copy+transform →
# fence → flush → cutover → retirement scheduling):
#
#   1. per-shard cycle: 20 sequential `ShardMigration.run/3` calls, p50/p99
#   2. rollout throughput at Oban :migrations concurrency 10 (the default)
#   3. rollout throughput at concurrency 50
#
# Local storage + local Postgres, so this is the engine-mechanics FLOOR; S3 adds
# ~5 storage round-trips + two ~size_mb transfers per shard (extrapolate with the
# measured S3 primitives). Cleans up completely: shard objects + retained copies,
# directory rows, Oban jobs, the synthetic release; any real behind-HEAD shards
# (e.g. dev demo tenants) are version-bumped out of the sweep and restored after.

alias Fathom.{Directory, Migrator, Repo}
alias Fathom.Migrator.ShardMigration
alias Fathom.Shard.Connection
import Ecto.Query

# Per-shard "migrated" info-lines and query logs would drown the report.
Logger.configure(level: :warning)

defmodule MigScale do
  def id(i), do: "migsc" <> String.pad_leading(Integer.to_string(i), 5, "0")

  def pct(sorted, p) do
    Enum.at(sorted, min(round(p / 100 * length(sorted)), length(sorted) - 1))
  end

  # Await true completion of the jobs enqueued after `watermark`. NOTE: polling the
  # laggard count instead is WRONG — mark_migrating removes a shard from the laggard
  # query when its job STARTS, so a laggard-based poll returns with up to
  # `concurrency` jobs still mid-flight.
  def await_jobs(repo, watermark, timeout_ms) do
    import Ecto.Query
    deadline = System.monotonic_time(:millisecond) + timeout_ms

    Stream.repeatedly(fn ->
      Process.sleep(500)

      repo.one(
        from(j in Oban.Job,
          where:
            j.id > ^watermark and
              j.state in ["available", "scheduled", "executing", "retryable"] and
              j.worker == "Fathom.Migrator.ShardMigrationJob",
          select: count()
        )
      )
    end)
    |> Enum.find(fn in_flight ->
      in_flight == 0 or System.monotonic_time(:millisecond) > deadline
    end)
  end
end

{shards, size_mb} =
  case System.argv() do
    [n, s] -> {String.to_integer(n), String.to_integer(s)}
    [n] -> {String.to_integer(n), 4}
    _ -> {2_020, 4}
  end

remote = Path.join(System.tmp_dir!(), "fathom_remote")
IO.puts("== migration scale probe: #{shards} shards × #{size_mb}MB ==")

# -- 0. keep real behind-HEAD shards (demo tenants) out of the sweep -------------
displaced =
  Repo.all(
    from(s in "shards",
      where: s.schema_version < 2 and s.status == "active" and not like(s.shard_id, "migsc%"),
      select: {s.shard_id, s.schema_version}
    )
  )

if displaced != [] do
  ids = Enum.map(displaced, &elem(&1, 0))
  Repo.update_all(from(s in "shards", where: s.shard_id in ^ids), set: [schema_version: 2])
  IO.puts("displaced #{length(displaced)} real laggard(s) out of the sweep (restored after)")
end

# -- 1. template: items table, ~size_mb, user_version 1 --------------------------
template = Path.join(System.tmp_dir!(), "migscale_template.db")
File.rm(template)
{:ok, conn} = Connection.open(template)
:ok = Connection.exec(conn, "CREATE TABLE items (id INTEGER PRIMARY KEY, name TEXT, val BLOB)")

rows = size_mb * 1000

:ok =
  Connection.exec(
    conn,
    "WITH RECURSIVE cnt(x) AS (SELECT 1 UNION ALL SELECT x+1 FROM cnt WHERE x < #{rows}) " <>
      "INSERT INTO items (name, val) SELECT 'item-' || x, randomblob(1000) FROM cnt"
  )

:ok = Connection.exec(conn, "PRAGMA user_version = 1")
:ok = Connection.exec(conn, "PRAGMA wal_checkpoint(TRUNCATE)")
Connection.close(conn)
tpl_mb = File.stat!(template).size / 1_000_000

# -- 2. clone the fleet + register it at v1 --------------------------------------
{clone_us, _} =
  :timer.tc(fn ->
    clone_cmd =
      "cd \"#{remote}\" && for i in $(seq 0 #{shards - 1}); do " <>
        "cp -c \"#{template}\" \"$(printf 'migsc%05d.db' $i)\"; done"

    {_, 0} = System.cmd("/bin/sh", ["-c", clone_cmd])
  end)

now = DateTime.utc_now() |> DateTime.truncate(:microsecond)

0..(shards - 1)
|> Enum.map(
  &%{
    shard_id: MigScale.id(&1),
    schema_version: 1,
    status: "active",
    last_active_at: now,
    inserted_at: now,
    updated_at: now
  }
)
|> Enum.chunk_every(5_000)
|> Enum.each(&Repo.insert_all("shards", &1))

IO.puts("fleet: #{shards} × #{Float.round(tpl_mb, 1)}MB cloned in #{div(clone_us, 1000)}ms\n")

# -- 3. release v2: a transform that does O(rows) work ----------------------------
{:ok, _} =
  Migrator.release(2, "migscale-probe", [
    "ALTER TABLE items ADD COLUMN added_col TEXT",
    "CREATE INDEX items_name_idx ON items (name)"
  ])

job_watermark = Repo.one(from(j in Oban.Job, select: max(j.id))) || 0

# -- 4. per-shard cycle, sequential -----------------------------------------------
seq_times =
  for i <- 0..19 do
    {us, {:ok, %{from: 1, to: 2}}} = :timer.tc(fn -> ShardMigration.run(MigScale.id(i), 2) end)
    us / 1000
  end
  |> Enum.sort()

IO.puts(
  "per-shard cycle (sequential, n=20)  p50 #{Float.round(MigScale.pct(seq_times, 50), 1)}ms  " <>
    "p99 #{Float.round(MigScale.pct(seq_times, 99), 1)}ms"
)

# -- 5. rollout throughput at two Oban concurrencies ------------------------------
run_phase = fn label, limit, batch ->
  Oban.scale_queue(queue: :migrations, limit: limit)
  before = Directory.count_laggards(2)
  phase_watermark = Repo.one(from(j in Oban.Job, select: max(j.id))) || 0
  {:ok, ^batch} = Migrator.rollout(batch)

  t0 = System.monotonic_time(:millisecond)
  0 = MigScale.await_jobs(Repo, phase_watermark, 900_000)
  wall_s = (System.monotonic_time(:millisecond) - t0) / 1000

  failed =
    Repo.one(
      from(j in Oban.Job,
        where: j.id > ^phase_watermark and j.state in ["discarded", "cancelled"],
        select: count()
      )
    )

  done = before - Directory.count_laggards(2)

  IO.puts(
    "rollout #{batch} @ concurrency #{label}     #{Float.round(wall_s, 1)}s  " <>
      "→ #{Float.round(done / wall_s, 1)} shards/s" <>
      if(failed > 0, do: "  (#{failed} jobs FAILED — INVESTIGATE)", else: "")
  )
end

remaining = shards - 20
run_phase.("10 (default)", 10, div(remaining, 2))
run_phase.("50", 50, remaining - div(remaining, 2))

# -- cleanup ----------------------------------------------------------------------
IO.puts("\ncleaning up…")
Oban.scale_queue(queue: :migrations, limit: 10)
Repo.delete_all(from(j in Oban.Job, where: j.id > ^job_watermark))
Repo.delete_all(from(m in "shard_migrations", where: m.name == "migscale-probe"))
{ndir, _} = Repo.delete_all(from(s in "shards", where: like(s.shard_id, "migsc%")))

Enum.each(displaced, fn {id, version} ->
  Repo.update_all(from(s in "shards", where: s.shard_id == ^id), set: [schema_version: version])
end)

{_, 0} = System.cmd("/bin/sh", ["-c", "find \"#{remote}\" -name 'migsc*' -delete"])
File.rm(template)

IO.puts(
  "deleted #{ndir} directory rows, all migsc objects + retains; displaced laggards restored"
)
