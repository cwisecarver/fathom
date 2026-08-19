# :s3 tests hit a live S3-compatible store (MinIO) and are excluded by default.
# Run them with: mix test --include s3  (see test/fathom/shard_storage_s3_test.exs).
# :bench tests are hot-path floor/ceiling guards (seconds of setup each) and are
# excluded by default. Run with: mix test --include bench  (see test/fathom/bench_test.exs).
#
# :flaky is a scenario that reproduces a real behaviour but not RELIABLY, and is excluded so it
# cannot become the unattributable CI failure it is meant to explain. It is not a parking space for
# tests nobody wants to fix: each one must say, in the test itself, what it reproduces, at what
# rate, and what would have to be understood to un-tag it. Run with: mix test --include flaky.
#
# :wal_probe tests need the FATHOM_WAL_PROBE=1 read-back functions, which the loadable extension
# registers from OS env — and `System.put_env` CANNOT turn them on, because it updates the BEAM's
# internal environment table, not the C `environ` that Rust's `std::env::var` reads. So the flag
# must be set before the VM starts and cannot be set from inside a test:
#     FATHOM_WAL_PROBE=1 mix test --include wal_probe
# CI does exactly that (.github/workflows/ci.yml), so these do not rot. See
# test/fathom/shard/wal_hook_test.exs.

# Clear stale storage DR artifacts from prior runs (#6). Deleted-tenant tests write durable
# `tombstones/<id>` (and token tests `tokenfloors/<id>`) markers that are, by design, never swept —
# so they accumulate across runs. `System.unique_integer` resets per BEAM run, so a leftover
# `tombstones/ten_<N>` from a previous run collides with a fresh `ten_<N>` this run, and the
# boot-time storage-union then falsely tombstones an unrelated test's tenant. Clean the dirs and drop
# the entries the app's boot already unioned in, so every run starts from a clean re-mint gate.
remote_dir = Fathom.Shard.Storage.Local.dir()

for sub <- ["tombstones", "tokenfloors"], do: File.rm_rf(Path.join(remote_dir, sub))

# Same cause, worse symptom: STALE `.lock` OBJECTS.
#
# A leftover `<shard>.lock` makes `acquire_lease` report `{:held, <a previous run's owner>}`, so a
# colliding id fails to open at all — `FILO_SHARD_OPEN` / `{:shard_held, "nonode@nohost#..."}`. And
# it does not need the previous run to be recent in human terms: a lock reads as LIVE for
# `shard_lease_ttl_ms + steal_margin_ms` (30s + 5s by default) after it was written, so a lock left
# in the closing seconds of one run is still "held" when the next run starts ~15s later. That is
# exactly the window that back-to-back runs (a `precommit` loop, a seed sweep) sit in, which is why
# this only ever showed up in rapid successive runs and never in a re-run afterwards — by then the
# stale lock had aged out. Diagnosed 2026-07-26 after `Fathom.ShardExecutorTest` failed on seed
# 126081 with 2,050 leaked `test_exec_*.lock` files on disk.
#
# Locks and shard objects are per-run state by definition — at test_helper time no coordinator has
# started — so clearing them is always correct, never merely convenient. `heartbeats/` is left
# alone: the app is already booted here and the Heartbeat process owns that key for THIS run.
for f <- Path.wildcard(Path.join(remote_dir, "*.lock")), do: File.rm(f)
for f <- Path.wildcard(Path.join(remote_dir, "*.db")), do: File.rm(f)

# The LOCAL shard dir has the same "per-run state by definition" property, and the same failure
# mode one step earlier: a leftover `<shard>.db` is treated as an authoritative un-flushed local
# copy (`Fathom.Shard` pulls only on cold start), so a colliding id from a previous run serves that
# file's contents instead of cold-opening from storage. It went unswept until now only because the
# dir was shared with dev, where deleting a file could discard real un-flushed writes; as of
# 2026-07-27 it is test-owned (`config/test.exs` :shard_data_dir), so clearing it is always
# correct. It had accumulated 5,047 files.
#
# Whole-dir rm_rf rather than a wildcard sweep: the coordinator also writes `-wal`/`-shm`
# sidecars, `*.tmp.*` pull temps, and `.db.{fenced,forked,corrupt}.*` quarantine files, and a
# sweep that enumerates kinds is one new kind away from leaking again.
File.rm_rf(Fathom.Shard.data_dir())

try do
  :ets.delete_all_objects(Fathom.Tenants.Tombstones)
rescue
  ArgumentError -> :ok
end

# Fathom.FailureCaptureFormatter writes any failure to logs/test-failures-<ts>.log with the seed
# and a rerun command. Two intermittent failures (2026-07-25) lost their identity permanently
# because the run's output was piped through `tail` and the next run overwrote ExUnit's `--failed`
# manifest — a flake you can't name is a flake you can't fix. Alongside the CLI formatter, so
# console output is unchanged; writes nothing on a green run.
ExUnit.start(
  exclude: [:s3, :bench, :wal_probe, :flaky],
  formatters: [ExUnit.CLIFormatter, Fathom.FailureCaptureFormatter]
)

Ecto.Adapters.SQL.Sandbox.mode(Fathom.Repo, :manual)
