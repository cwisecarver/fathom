# Expert review 2026-10-20 #32 — does `PRAGMA mmap_size` speed the flush scan on Linux?
#
# Measures, on a synthetic shard, the two page scans a durability flush does on a dirty-IO thread:
# `PRAGMA quick_check` and `VACUUM INTO`, once with mmap_size=0 (today) and once with mmap_size=N,
# ALTERNATING the two arms so cache and thermal drift hit both equally. Each rep opens a fresh
# connection (a cold SQLite page cache; the OS page cache is warm after the first rep, like a
# shard that was just written).
#
#   mix run --no-start scripts/flush_scan_mmap_probe.exs [size_mb] [reps] [mmap_mb]
#   # defaults: 50 MB shard, 9 reps, 256 MB mmap window
#
# Run it inside the rig's Linux VM (the finding measured macOS only). Decision rule from the
# review: enable `config :fathom, :flush_scan_mmap_bytes` only if the win is large, because a mapped
# page that vanishes is SIGBUS (kills the node) where a pread fails one shard's flush.
#
# Output: one line per arm with the median ms for each scan and the mmap/no-mmap ratio.

alias Exqlite.Sqlite3

[size_mb, reps, mmap_mb] =
  case System.argv() do
    [] -> [50, 9, 256]
    args -> args |> Enum.map(&String.to_integer/1) |> Kernel.++([50, 9, 256]) |> Enum.take(3)
  end

dir = Path.join(System.tmp_dir!(), "mmap_probe_#{System.unique_integer([:positive])}")
File.mkdir_p!(dir)
db = Path.join(dir, "shard.db")

# Build the shard: ~size_mb of rows with an index, WAL mode, then checkpoint so the main file
# holds it (the folded state a flush scans).
{:ok, c} = Sqlite3.open(db)
:ok = Sqlite3.execute(c, "PRAGMA journal_mode=WAL")
:ok = Sqlite3.execute(c, "CREATE TABLE t (id INTEGER PRIMARY KEY, k TEXT, v BLOB)")
:ok = Sqlite3.execute(c, "CREATE INDEX t_k ON t (k)")
:ok = Sqlite3.execute(c, "BEGIN")
{:ok, ins} = Sqlite3.prepare(c, "INSERT INTO t (k, v) VALUES (?1, ?2)")
rows = div(size_mb * 1024 * 1024, 1_024)

for i <- 1..rows do
  :ok =
    Sqlite3.bind(ins, ["key-#{:erlang.phash2(i, 1_000_000)}-#{i}", :crypto.strong_rand_bytes(900)])

  :done = Sqlite3.step(c, ins)
  :ok = Sqlite3.reset(ins)
end

:ok = Sqlite3.release(c, ins)
:ok = Sqlite3.execute(c, "COMMIT")
{:ok, ck} = Sqlite3.prepare(c, "PRAGMA wal_checkpoint(TRUNCATE)")
{:row, _} = Sqlite3.step(c, ck)
:ok = Sqlite3.release(c, ck)
:ok = Sqlite3.close(c)

IO.puts(
  "shard: #{Float.round(File.stat!(db).size / 1_048_576, 1)} MB, #{rows} rows, #{reps} reps/arm"
)

time = fn fun ->
  t0 = System.monotonic_time(:microsecond)
  fun.()
  (System.monotonic_time(:microsecond) - t0) / 1_000
end

scan = fn mmap_bytes, n ->
  {:ok, c} = Sqlite3.open(db)
  :ok = Sqlite3.execute(c, "PRAGMA mmap_size=#{mmap_bytes}")

  qc =
    time.(fn ->
      {:ok, st} = Sqlite3.prepare(c, "PRAGMA quick_check")
      {:row, ["ok"]} = Sqlite3.step(c, st)
      :ok = Sqlite3.release(c, st)
    end)

  dest = Path.join(dir, "snap_#{n}_#{mmap_bytes}.db")
  :ok = Sqlite3.execute(c, "PRAGMA synchronous=OFF")
  vac = time.(fn -> :ok = Sqlite3.execute(c, "VACUUM INTO '#{dest}'") end)
  :ok = Sqlite3.close(c)
  File.rm(dest)
  {qc, vac}
end

median = fn xs -> xs |> Enum.sort() |> Enum.at(div(length(xs), 2)) end

# Warm-up pair (discarded), then alternate off/on.
scan.(0, 0)
scan.(mmap_mb * 1_048_576, 0)

results =
  for n <- 1..reps, reduce: %{off: [], on: []} do
    acc ->
      off = scan.(0, n)
      on = scan.(mmap_mb * 1_048_576, n)
      %{acc | off: [off | acc.off], on: [on | acc.on]}
  end

stat = fn arm, idx -> results[arm] |> Enum.map(&elem(&1, idx)) |> median.() end

for {label, idx} <- [{"quick_check", 0}, {"VACUUM INTO", 1}] do
  off = stat.(:off, idx)
  on = stat.(:on, idx)

  IO.puts(
    "#{String.pad_trailing(label, 12)} mmap off #{Float.round(off, 1)} ms | " <>
      "mmap #{mmap_mb} MB #{Float.round(on, 1)} ms | on/off #{Float.round(on / off, 2)}"
  )
end

File.rm_rf!(dir)
