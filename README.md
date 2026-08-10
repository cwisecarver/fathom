# Fathom

**Multi-tenant databases usually mean a shared table with a `tenant_id` column and a
prayer.** One forgotten `WHERE` clause is a cross-tenant leak. One noisy tenant is
everyone's outage. "Delete my data" is a query you hope was right.

**Fathom gives every tenant its own physically separate SQLite database** — served over
the network to *unchanged* libSQL clients, backed by object storage, with one writer per
tenant enforced by a lease. Isolation is a **file boundary**, not a query convention.
Deleting a tenant deletes a file. Forking one is a copy.

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

*One SQLite file per tenant, one writer at a time, S3 underneath. Everything else is bookkeeping.*

## Why this is different

- **Isolation is a file boundary.** No `tenant_id`, no `WHERE` clause standing between
  tenants. A leak requires opening the wrong *file*, not forgetting a filter.
- **Delete and export are file operations.** GDPR erasure is a purge of every stored
  object, not a `deleted_at` flag you hope nothing else reads. Portability is a download.
- **Forking a tenant is a copy.** Clone production into staging in one object copy —
  measured ~21× faster than migrating a fresh shard.
- **One writer per tenant, enforced by a lease.** Cross-node safety through S3, with no
  consensus system to operate.
- **Density is the business model.** ~16 KiB of RAM per idle tenant, and a stored-but-idle
  tenant costs object storage rather than memory — so a long tail of small tenants is cheap.

## Proof points

Every number below is a dated run, not an estimate. Where something is unmeasured, the
docs say so.

| Claim | Measured | Evidence |
|---|---|---|
| Tenant density | **29,956 shards held across 3 nodes** (of 30,000 minted — 0.15% drops), ~16 KiB RSS each idle, spread 1.22×; stress-pushed to 105,530 held | [`fleet-density-2026-07-10.md`](docs/reviews/fleet-density-2026-07-10.md), [`served-data-2026-07-23.md`](docs/reviews/served-data-2026-07-23.md) |
| Throughput | **4,096 concurrent tenants, 3,414 txn/s, zero errors**; load follows the shard partition | [`tpc-fleet-elixir-driver-2026-07-24.md`](docs/reviews/tpc-fleet-elixir-driver-2026-07-24.md) |
| Autonomous rebalancing | Against a genuine imbalance, the policy proposed a move and the control loop ran the handoff **autonomously** (warm → pin → LB flip → drain), isolation intact | [`chaos-run-2026-07-09.md`](docs/reviews/chaos-run-2026-07-09.md) |
| Durability (RPO) | Bounded and **measured**: 452 rows p50 / 800 p99 lost at the 5 s default under 200 writes/s | [`durability.md`](docs/durability.md) |
| Cold-open latency | **~1 S3 round-trip** — 26 / 70 / 133 ms at 10 / 30 / 60 ms one-way | [`latency-cost-2026-07-23.md`](docs/reviews/latency-cost-2026-07-23.md) |
| vs. Turso's own server | **On par per-DB** against `sqld` on the same box and wire — while carrying the lease fence and multi-tenant routing | [`turso-headtohead-2026-07-10.md`](docs/reviews/turso-headtohead-2026-07-10.md) |

**How much has to change in your app?** In a Django app built on fathom, the entire
tenancy story — runtime shard-alias registration, a fail-closed database router, and a
`shard_id=` queryset kwarg — costs **~300 lines**, and the domain models need no fathom
imports at all. See [`docs/quickstart-django.md`](docs/quickstart-django.md).

## How it works

Fathom is a multi-tenant sharded data platform built on Phoenix: **one SQLite database per shard** (targeting millions), served to unchanged libSQL clients — e.g. an unchanged Django app via `django-libsql` — over the [Hrana wire protocol](https://github.com/libsql/hrana-client-ts/blob/main/HRANA_3_SPEC.md) via the [Filo](https://github.com/cwisecarver/filo) library.

Filo speaks HTTP Hrana v1/v2/v3 (including cursor) and WebSocket hrana1/2/3. The Hrana listener runs on port 8080 (separate from the Phoenix dashboard on 4000). The target shard is derived from the `Host` subdomain (`acme.fathom.example` → shard `acme`), with `?db=` and `x-fathom-shard` as **dev-only** fallbacks (gated by `:allow_shard_override`, off in prod). Per-shard bearer-token auth (`Phoenix.Token`, presented as libSQL's `authToken`) is available via `HRANA_AUTH=required`; with it disabled (the default) the trust boundary is the network — the port must be LB-only-reachable (see [`docs/deploy-cluster.md`](docs/deploy-cluster.md)).

**New here?** Jump to [Getting started](#getting-started). Contributing? See [`CONTRIBUTING.md`](CONTRIBUTING.md). The full map of the project is the docs index, [`docs/README.md`](docs/README.md).

## Getting started

### Fast path — Docker (recommended for a first look)

One `docker compose up` brings up fathom + PostgreSQL + MinIO (as S3) + nginx with safe defaults and walks you from an empty system to a served, isolated tenant in a single sitting — no Elixir toolchain required. (The first build compiles a prod release, so it's a few minutes of build before anything is up.) See **[`deploy/compose/README.md`](deploy/compose/README.md)**.

### Native dev

**Prerequisites**

- **Elixir ≥ 1.15** on a compatible OTP (the project builds and tests on Elixir 1.19 / OTP 27).
- A **C toolchain** — the `exqlite` SQLite NIF compiles from source (`build-essential` on Linux, Xcode Command Line Tools on macOS).
- A running local **PostgreSQL** — the control-plane / directory store (shard *data* is SQLite). By default fathom connects as your OS user with no password; set `PGUSER` / `PGPASSWORD` / `PGHOST` / `PGDATABASE` to override, or edit `config/dev.exs`.
- **The `filo` sibling repo.** fathom depends on [Filo](https://github.com/cwisecarver/filo) (the Hrana/libSQL protocol server) as a **path dependency at `../filo`**, so check both out side by side — a clone of `fathom` alone will fail `mix deps.get`:

  ```
  parent/
  ├── fathom/   # this repo
  └── filo/     # git clone https://github.com/cwisecarver/filo
  ```

**Set up and run**

```bash
mix setup                 # deps.get + create/migrate the dev DB + install/build assets
iex -S mix phx.server     # web/dashboard/API :4000 · Hrana/libSQL :8080 · health :8081
```

**Provision a tenant and connect a client**

```bash
# Admin BasicAuth is admin:admin in dev.
curl -su admin:admin -X POST http://localhost:4000/api/tenants \
  -H 'content-type: application/json' -d '{"shard_id":"acme"}'
```

Then point any libSQL / `django-libsql` client at **`ws://acme.localhost:8080`** — the Host subdomain selects the shard. (In dev, `ws://localhost:8080` with no subdomain routes to the `demo` default shard.) See [`docs/quickstart-django.md`](docs/quickstart-django.md) for the Django walkthrough and [`docs/configuration.md`](docs/configuration.md) for every configuration knob.

## What's built

### Shard data path

Each Hrana stream gets its own `Fathom.Shard.Connection` (a WAL-mode `exqlite` connection to the shard file), so transactions are fully isolated. `Fathom.Shard` is the per-shard coordinator — one GenServer per active shard — that owns the file lifecycle:

- On **cold start** it pulls the shard file from `Fathom.Shard.Storage`.
- It tracks checked-out connections (by monitoring the stream processes).
- When **idle with zero connections** it checkpoints, flushes the file back to storage, drops the local copy, and stops — so flush never races a write and the local copy is always bottomless-backed.
- A **write-gated dirty flag** means durability PUTs track writes, not open-shard count. A clean (read-only or idle) shard skips the upload.

`Fathom.Shards` is the find-or-start router (backed by `Fathom.ShardRegistry` + `Fathom.ShardSupervisor`). `Fathom.Shards.checkout/1` resolves a `shard_id` to its coordinator, starting it on demand, and returns `{:ok, pid, ref, path}`. Full story: [`docs/data-path.md`](docs/data-path.md).

### Admission & capacity

The front door is fail-closed: the shard comes from the `Host` subdomain, and an unresolved request falls back to `:default_shard` (unset in prod ⇒ 400, never commingled). Novel-shard creation is double-gated — `:max_open_shards` caps how many shards a node holds open (soft cap: evict the LRU *idle* shard; hard cap: 503), and `Fathom.Shards.NovelLimiter` rate-limits how fast unseen ids mint new shards. Per-query resource bounds (`:query_timeout_ms`, `:query_max_rows`, `:max_checkouts_per_shard`, all off by default) protect co-located tenants from one runaway query, and boot guards refuse template-poisoning misconfig. Full story: [`docs/admission.md`](docs/admission.md).

### Bottomless storage

`Fathom.Shard.Storage` is a `pull/2` + `flush/2` behaviour. The backend is chosen via `config :fathom, :shard_storage`:

- `Fathom.Shard.Storage.Local` — filesystem object store, the default for dev/test.
- `Fathom.Shard.Storage.S3` — Req + `aws_sigv4` (no AWS SDK dep); compatible with S3, R2, Tigris, and MinIO.

The S3 backend runs a dedicated, config-driven Finch connection pool (default 200 connections, `pool_size` and `pool_count` configurable), and overlaps the lease acquire with the shard pull to minimize round-trips to ~1 RTT on cold-open. Durability contract (WAL + `synchronous=FULL` + the write-gated periodic flush): [`docs/durability.md`](docs/durability.md).

### Cross-node single-writer (lease + epoch fence + heartbeat)

`Fathom.Shard.Storage` carries a per-shard lock (`{owner, epoch}`) and a per-node liveness object renewed by `Fathom.Shard.Heartbeat` every `ttl/3`. Heartbeat cost is O(nodes), not O(shards). A shard's owner is live iff its heartbeat is fresh; `acquire_lease` consults the owner's heartbeat to distinguish held-vs-steal and fails closed on a read error. Before a flush, coordinators fence via `Heartbeat.valid_for_write?/1` and self-fence if superseded — a remapped shard never double-writes. All cross-node coordination goes through S3; there is no BEAM mesh. Full story: [`docs/single-writer.md`](docs/single-writer.md).

### Cluster layer

The L7 load balancer consistent-hashes the `Host` subdomain to one backend node; each node is an independent single-node fathom. `Fathom.HealthPlug` serves `GET /health` on port 8081 (the per-node LB probe). `Fathom.Telemetry` instruments shard/lease/checkout events, runs an active-shard poller, and bridges to OpenTelemetry spans (trace export is opt-in via `OTEL_EXPORTER_OTLP_ENDPOINT`). LB config lives in `deploy/lb/`; the chaos rig (prod-release nodes behind nginx + MinIO + toxiproxy) lives in `deploy/chaos/`. See [`docs/cluster-architecture.md`](docs/cluster-architecture.md) and the runbook [`docs/runbooks/cluster.md`](docs/runbooks/cluster.md).

### Directory / control plane

`Fathom.Directory` (backed by `Fathom.Repo`, the Postgres orchestration store) records each shard's `schema_version`, lifecycle `status` (`active` / `migrating` / `retired` / `migration_failed` / `deleted` / `suspended`), `last_active_at`, and `retain_until`. Per-checkout accesses are buffered and batch-flushed off the hot path by `Fathom.Directory.Recorder` (a coalescing ETS buffer, so a Postgres outage drops a flush, never a checkout). The directory drives the migration and lifecycle machinery; routing today is still Host-based (the directory is not yet a per-request resolve). Full story: [`docs/directory.md`](docs/directory.md).

### Blue/green migration engine

`Fathom.Migrator` runs blue/green per-shard schema migration:

- `Migrator.Capture` records template migrations into fleet versions.
- `Migrator.Copy` + `Migrator.ShardMigration` perform the copy+transform and stamp `PRAGMA user_version`.
- `Migrator.Release` releases a fleet HEAD.
- Oban jobs drive the fleet: `ShardMigrationJob` (unique per shard), `ReconcileJob` (hourly cron sweep so the cold tail converges), `RetirementJob` (drop expired retained versions), `RevertJob` (fleet pointer-flip back to `vN-1`).

Schema version truth lives in three places: `django_migrations` in each shard (Django's own migration ledger), `PRAGMA user_version` (O(1) gate), and `shards.schema_version` in Postgres (laggard queries without opening shards). Full story: [`docs/migration.md`](docs/migration.md); the operator workflow (Django) is [`docs/django-migrations.md`](docs/django-migrations.md).

### Tenant lifecycle

`Fathom.Tenants` is the whole-shard control plane the migration engine leaves out of scope — a tenant *is* one SQLite file:

- **provision** — explicit create (directory row + optional fork-from-template + token), returning the `libsql://<id>.<zone>` URL.
- **delete** — GDPR-erasure/offboarding: a synchronous tombstone + fleet-wide re-mint guard, then a durable purge of every stored object (force-stop-then-purge so no `.fenced` copy is stranded).
- **export** — portability (the whole SQLite file), flush-first so the newest writes are included.
- **suspend / resume** — administrative offline via a reversible ETS deny gate.
- **fork** — clone a live tenant to a new id.

Full story: [`docs/tenant-lifecycle.md`](docs/tenant-lifecycle.md).

### Auth

`Fathom.HranaAuth` gates the data path, controlled by `:hrana_auth` (`:disabled` by default; `HRANA_AUTH=required` in prod). A per-shard `Phoenix.Token` (minted via `mix fathom.token <shard>`) is presented as libSQL's `authToken` on Filo's `:authorize` seam. Tokens support **zero-downtime rotation** (a grace window keeps the previous version valid), immediate **revoke**, and a **read-only scope** (`token_for(id, scope: :ro)` → any write on that token is 403). With auth disabled the trust boundary is the network. Full story: [`docs/auth.md`](docs/auth.md).

### Control-plane API & admin dashboard

A JSON control plane under `/api` (on `:4000`, separate from the Hrana data port) lets a platform operator create/list/get/delete tenants, suspend/resume, fork, flush, mint/rotate/revoke tokens, manage snapshots/restore, and read `/api/migrations/status` (the deploy gate a CI/CD reads). It's guarded by **scoped API keys** (`read < manage < destroy`, `mix fathom.apikey`) with the shared admin BasicAuth as a legacy fallback, a per-IP brute-force / rate-limit throttle, and an append-only audit trail of every mutating action. The realtime `/admin` LiveView dashboard rides the same auth — see [`docs/runbooks/admin-dashboard.md`](docs/runbooks/admin-dashboard.md).

### Warm standby (Phase 2, A1)

`Fathom.Shard.WarmFollower` (gated by `:warm_follower`, off by default) pre-pulls the fleet's recently-active shards this node doesn't own into a separate cache dir — no lease, no serving. On failover the coordinator promotes the warm copy only after a freshness check (`Storage.pull_if_changed/3`, a conditional `If-None-Match` GET), so a stale copy is never served. The warm win is the object **body transfer avoided**, so it scales with shard size × bandwidth-delay and is marginal for a small shard on a fat pipe — the warm path still pays one S3 round-trip for the freshness check either way. Measured 2026-07-01 (dev build, MinIO + toxiproxy, relative) at 1 MB / 30 ms one-way / **100 Mbps cap**: cold ~162 ms → warm ~72 ms, ≈2.3×. With **no** bandwidth cap the two paths converge (2026-07-23 A/B at 30 ms: cold 607 ms / warm 619 ms) because the avoided body transfer is ~free on loopback. Those absolutes also predate the steal-touch takeover machinery, so don't compare them across dates — the *shape* (warm wins in proportion to size × bandwidth-delay) is the durable claim. Warm capacity is disk-bound (~0 BEAM/fd per cached shard), so a standby warms far more than it can serve open. Full story: [`docs/warm-standby.md`](docs/warm-standby.md).

### Dynamic rebalancing (Phase 2, B1)

`Fathom.Rebalancer.*` moves a persistently-hot shard off an overloaded node by layering a per-subdomain exception table on the LB's consistent hash. Each node **reports** its hot set to Postgres; an Oban cron fleet-singleton **decides** (hotness = an absolute q/s floor or `K×p99`, never `K×median`, with anti-flap + cooldown + an improvement guard) and enqueues a **handoff** — warm the target → pin the override + reload the LB map → drain the source's lease → the target acquires. All gates are **off by default**; the enable path is [`docs/runbooks/rebalancer.md`](docs/runbooks/rebalancer.md). Proven live on the chaos rig. Full story: [`docs/rebalancing.md`](docs/rebalancing.md).

### Bench and scale harnesses

- `mix fathom.bench` (+ `scripts/benchmark.sh`, regression-gated via `scripts/commit_with_bench.sh`) measures the hot paths: `cold_open_p50_us`, `cold_open_s3_p50_us` (opt-in), `dir_resolve_p50_us`, `copy_rows_per_s`, `fanout_kb_per_shard`.
- `mix fathom.scale [--shards N] [--shard-size-mb S]` provisions realistically-sized shards and measures cold-open latency at size and fan-out node density. `--ramp` opens empty shards to find the fd ceiling.
- `mix fathom.rpo` quantifies the node-loss window (lost rows/seconds vs the flush interval).

See [`docs/benchmark-plan.md`](docs/benchmark-plan.md) for the full harness description. Other operator tasks: `mix fathom.shard` (pull/inspect/fork), `mix fathom.snapshot` (PITR snapshots), `mix fathom.directory` (cross-store DR reconcile).


## What's next

Phase 2 is scoped in [`docs/phase2-scoping.md`](docs/phase2-scoping.md). Warm standby (A1) and dynamic rebalancing (B1) are **built** (see above), as is the affinity-aware placement piece of locality (C) — all gated off by default.

Not on `main`: **live WAL streaming** (A2 — no longer deferred; **built on the `a2-quorum-replication` branch** and not merged here. The blocker, "exqlite exposes no WAL seam," was disproved 2026-08-09: a loadable extension reaches `sqlite3_wal_hook`, the same move as the Django UDFs. Node-loss RPO ~300 s → ~0, at +225 µs per write when on and noise when off), the remaining locality work (**C1** rendezvous/bounded-load hashing, **C2** multi-region affinity), a **`fathom_native`** Rust NIF, and a **cached / PubSub-invalidated directory resolve** on the request path.

## Contributing

Setup, the local dev loop, the `mix precommit` gate, the hot-path bench gate, testing discipline, and where each subsystem lives are in **[`CONTRIBUTING.md`](CONTRIBUTING.md)**. The full working agreement (workflow, gates, principles, architecture detail) is [`AGENTS.md`](AGENTS.md).

## Further reading

**[`docs/README.md`](docs/README.md) is the docs index** — the curated map of the per-subsystem "how it works" stories, the architecture and deployment guides, the benchmark plans, the operational runbooks, and the dated run reports. A few high-value entry points:

| Document | What it covers |
|---|---|
| [AGENTS.md](AGENTS.md) | Full working agreement: workflow, testing discipline, benchmarking, gates, principles, architecture detail |
| [docs/configuration.md](docs/configuration.md) | Every environment variable fathom reads, its default, and its safety consequence |
| [docs/migration.md](docs/migration.md) | The built blue/green per-shard schema migration engine |
| [docs/cluster-architecture.md](docs/cluster-architecture.md) | Cluster architecture decision (LB-partition + S3 lease, why not the alternatives) + phase status |
| [docs/deploy-cluster.md](docs/deploy-cluster.md) | Cluster deployment, LB config, chaos rig, phase status |
| [deploy/compose/README.md](deploy/compose/README.md) | The one-command Docker eval stack (fathom + Postgres + MinIO + nginx) |
| [docs/reviews/turso-headtohead-2026-07-10.md](docs/reviews/turso-headtohead-2026-07-10.md) | **"Why not just use Turso?"** — head-to-head against libSQL's own server (`sqld`) on the same box, same wire, same driver, so the only variable is the server implementation |
| [docs/reviews/competitive-oltp-2026-07-10.md](docs/reviews/competitive-oltp-2026-07-10.md) | **"Why not just use Postgres?"** — fathom vs raw SQLite vs Postgres on the same box, both durability modes |
