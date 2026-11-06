# scripts/

Index of everything in this directory, one line each. Bench-related scripts are described in
[`docs/benchmark-plan.md`](../docs/benchmark-plan.md); run them from the repo root.

## Bench and commit gate

- `benchmark.sh` — runs `mix fathom.bench` prod-compiled and appends one JSON line to `perf_history.jsonl`.
- `commit_with_bench.sh` — bench-then-commit gate: refuses the commit on a 20% or worse regression in any metric vs the parent's same-host entry.
- `benchmark_s3_latency.sh` — S3 cold-open bench against MinIO behind toxiproxy, with injected latency/bandwidth.
- `benchmark_s3_sweep.sh` — sweeps that S3 bench across several injected latencies and prints a table.
- `perf_history.jsonl` — data: one line per `benchmark.sh` run (read by the gate; do not hand-edit).
- `tpc_history.jsonl` — data: history of TPC-C-style benchmark runs (`tpcc_*` metrics).
- `wire_history.jsonl` — data: history of Hrana wire-overhead runs (`hrana_rt_us`, `tpcb_wire_overhead_us`).

## Test and build helpers

- `minio_test.sh` — starts an ephemeral MinIO and runs the `:s3` integration tests against it.
- `apply_exqlite_patch.sh` — applies the carried exqlite BLOB-tagging patch to `deps/exqlite` and rebuilds its NIF.

## Probes (run with `mix run scripts/<name>.exs`)

- `directory_scale.exs` — does the Postgres directory hold up at target fleet cardinality (default 3.1M shard rows)?
- `flush_scan_mmap_probe.exs` — does `PRAGMA mmap_size` speed the flush scan on Linux (expert review 2026-10-20 #32)?

## Historical one-off probes (kept, not maintained)

Nothing else references these two. They were written for a single investigation or demo and are
kept for reference; they may need updating before they run against current code.

- `hotspot_demo.exs` — continuous Zipf-skewed hotspot load against a serving node, to demo the admin dashboard.
- `migration_scale.exs` — fleet-migration wall-clock probe: per-shard cost of a schema change and rollout throughput vs Oban concurrency.
