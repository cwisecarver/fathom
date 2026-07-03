# Fathom

Fathom is a multi-tenant sharded data platform built on Phoenix: **one SQLite database per shard** (targeting millions), served to unchanged libSQL clients — e.g. an unchanged Django app via `django-libsql` — over the [Hrana wire protocol](https://github.com/libsql/hrana-client-ts/blob/main/HRANA_3_SPEC.md) via the [Filo](https://github.com/cwisecarver/filo) library.

Filo speaks HTTP Hrana v1/v2/v3 (including cursor) and WebSocket hrana1/2/3. The Hrana listener runs on port 8080 (separate from the Phoenix dashboard on 4000). The target shard is derived from the `Host` subdomain (`acme.fathom.example` → shard `acme`), with `?db=` and `x-fathom-shard` as **dev-only** fallbacks (gated by `:allow_shard_override`, off in prod). Per-shard bearer-token auth (`Phoenix.Token`, presented as libSQL's `authToken`) is available via `HRANA_AUTH=required`; with it disabled (the default) the trust boundary is the network — the port must be LB-only-reachable (see `docs/deploy-cluster.md`).

## What's built

### Shard data path

Each Hrana stream gets its own `Fathom.Shard.Connection` (a WAL-mode `exqlite` connection to the shard file), so transactions are fully isolated. `Fathom.Shard` is the per-shard coordinator — one GenServer per active shard — that owns the file lifecycle:

- On **cold start** it pulls the shard file from `Fathom.Shard.Storage`.
- It tracks checked-out connections (by monitoring the stream processes).
- When **idle with zero connections** it checkpoints, flushes the file back to storage, drops the local copy, and stops — so flush never races a write and the local copy is always bottomless-backed.
- A **write-gated dirty flag** means durability PUTs track writes, not open-shard count. A clean (read-only or idle) shard skips the upload.

`Fathom.Shards` is the find-or-start router (backed by `Fathom.ShardRegistry` + `Fathom.ShardSupervisor`). `Fathom.Shards.checkout/1` resolves a `shard_id` to its coordinator, starting it on demand, and returns `{:ok, pid, ref, path}`.

### Bottomless storage

`Fathom.Shard.Storage` is a `pull/2` + `flush/2` behaviour. The backend is chosen via `config :fathom, :shard_storage`:

- `Fathom.Shard.Storage.Local` — filesystem object store, the default for dev/test.
- `Fathom.Shard.Storage.S3` — Req + `aws_sigv4` (no AWS SDK dep); compatible with S3, R2, Tigris, and MinIO.

The S3 backend runs a dedicated, config-driven Finch connection pool (default 200 connections, `pool_size` and `pool_count` configurable), and overlaps the lease acquire with the shard pull to minimize round-trips to ~1 RTT on cold-open.

### Cross-node single-writer (lease + epoch fence + heartbeat)

`Fathom.Shard.Storage` carries a per-shard lock (`{owner, epoch}`) and a per-node liveness object renewed by `Fathom.Shard.Heartbeat` every `ttl/3`. Heartbeat cost is O(nodes), not O(shards). A shard's owner is live iff its heartbeat is fresh; `acquire_lease` consults the owner's heartbeat to distinguish held-vs-steal and fails closed on a read error. Before a flush, coordinators fence via `Heartbeat.valid_for_write?/1` and self-fence if superseded — a remapped shard never double-writes. All cross-node coordination goes through S3; there is no BEAM mesh.

### Cluster layer

The L7 load balancer consistent-hashes the `Host` subdomain to one backend node; each node is an independent single-node fathom. `Fathom.HealthPlug` serves `GET /health` on port 8081 (the per-node LB probe). `Fathom.Telemetry` instruments shard/lease/checkout events, runs an active-shard poller, and bridges to OpenTelemetry spans (trace export is opt-in via `OTEL_EXPORTER_OTLP_ENDPOINT`). LB configuration and a chaos rig live in `deploy/lb/`; the cluster runbook is in `docs/runbooks/cluster.md`.

### Directory / control plane

`Fathom.Directory` (backed by `Fathom.Repo`, the Postgres orchestration store) records each shard's `schema_version`, lifecycle `status` (`active`/`migrating`/`retired`/`migration_failed`), `last_active_at`, and `retain_until`. Per-checkout accesses are buffered and batch-flushed off the hot path by `Fathom.Directory.Recorder` (a coalescing ETS buffer). The directory drives the migration and lifecycle machinery; routing today is still Host-based (the directory is not yet a per-request resolve).

### Blue/green migration engine

`Fathom.Migrator` runs blue/green per-shard schema migration:

- `Migrator.Capture` records template migrations into fleet versions.
- `Migrator.Copy` + `Migrator.ShardMigration` perform the copy+transform and stamp `PRAGMA user_version`.
- `Migrator.Release` releases a fleet HEAD.
- Oban jobs drive the fleet: `ShardMigrationJob` (unique per shard), `ReconcileJob` (hourly cron sweep so the cold tail converges), `RetirementJob` (drop expired retained versions), `RevertJob` (fleet pointer-flip back to `vN-1`).

Schema version truth lives in three places: `_fathom_migrations` in each shard, `PRAGMA user_version` (O(1) gate), and `shards.schema_version` in Postgres (laggard queries without opening shards).

### Bench and scale harnesses

- `mix fathom.bench` (+ `scripts/benchmark.sh`, regression-gated via `scripts/commit_with_bench.sh`) measures the hot paths: `cold_open_p50_us`, `cold_open_s3_p50_us` (opt-in), `dir_resolve_p50_us`, `copy_rows_per_s`, `fanout_kb_per_shard`.
- `mix fathom.scale [--shards N] [--shard-size-mb S]` provisions realistically-sized shards and measures cold-open latency at size and fan-out node density. `--ramp` opens empty shards to find the fd ceiling.

See `docs/benchmark-plan.md` for the full harness description.

## Architecture (current data path)

```
   libSQL client (django-libsql / WebSocket, libsql-experimental / HTTP)
                         │  Hrana, shard = Host subdomain
                         ▼
   Filo.Plug / Filo.Socket  (Hrana HTTP v1/v2/v3 + WebSocket hrana1/2/3)
                         │  Filo.Executor callback
                         ▼
   Fathom.ShardExecutor → Fathom.Shards.checkout/1 (find-or-start → file path)
                         ▼
   Fathom.Shard.Connection (one exqlite conn per stream) → SQLite file
                         ▲ pull on cold start / checkpoint+flush+drop on idle
   Fathom.Shard (coordinator: tracks conns, dirty flag, idle)
                         │
                         ├── Fathom.Shard.Storage (Local | S3 via Req sigv4)
                         └── Fathom.Shard.Heartbeat (per-node liveness, O(nodes))

   Fathom.Directory (Postgres) ← Fathom.Directory.Recorder (ETS buffer, off hot path)
   Fathom.Migrator + Oban jobs  (blue/green migration, reconcile, retire, revert)
   Fathom.HealthPlug (:8081)    (LB probe)
   Fathom.Telemetry + OTel      (metrics + trace spans)
```

## What's next (Phase 2)

Phase 2 is scoped in `docs/phase2-scoping.md`. The highest-value item is **A1 — S3-warm-cache standby** (`Fathom.Shard.WarmFollower`, in progress): a standby node proactively pre-pulls recently-active shards from S3 into a local cache (no lease, no serving) so that on failover the cold-open becomes a warm open (~2 ms instead of ~200 ms cross-region). It is additive, model-consistent, and reuses the warming infra already built.

Not yet built: dynamic rebalancing (B1 — LB override + control-plane), WAL streaming (A2), per-shard load telemetry for hot-spot detection, a `fathom_native` Rust NIF, or a cached/PubSub-invalidated directory resolve on the request path.

## Build and run

```bash
mix setup            # deps.get + ecto.setup (Postgres) + assets
mix compile          # build
mix test             # creates+migrates test Postgres DB, then runs tests
mix test --failed    # rerun last failures
mix format           # format
mix precommit        # gate: compile --warnings-as-errors, deps.unlock --unused, format, test
iex -S mix phx.server   # start app (dashboard :4000, Hrana :8080, health :8081)
```

For the bench harness, run against a prod-compiled binary:

```bash
MIX_ENV=prod mix compile
scripts/benchmark.sh        # runs mix fathom.bench and appends to perf_history.jsonl
mix fathom.scale            # fan-out density at real shard sizes
```

## Examples

- [django-fathom-example](https://github.com/cwisecarver/django-fathom-example) —
  a sample Django app with invisible per-tenant shard routing: a `shard_id` in
  ordinary queryset parameters routes every query to the tenant's own shard
  (custom QuerySet + database router, fail-closed, proven end-to-end against a
  live fathom).

## Further reading

| Document | What it covers |
|---|---|
| [AGENTS.md](AGENTS.md) | Full working agreement: workflow, testing discipline, benchmarking, gates, principles, architecture detail |
| [docs/migration-plan.md](docs/migration-plan.md) | Blue/green per-shard schema migration design |
| [docs/phase2-scoping.md](docs/phase2-scoping.md) | Phase 2 options (warm standby, rebalancing, locality) and recommendation |
| [docs/benchmark-plan.md](docs/benchmark-plan.md) | Bench harness design, hot paths, regression gate |
| [docs/deploy-cluster.md](docs/deploy-cluster.md) | Cluster deployment, LB config, chaos rig |
| [docs/runbooks/cluster.md](docs/runbooks/cluster.md) | On-call runbook for the cluster layer |
