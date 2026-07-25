# AGENTS.md — Fathom

## Project

Fathom is a multi-tenant sharded data platform built on Phoenix: one SQLite database per shard (eventually millions), served to unchanged libSQL clients (e.g. an unchanged Django app via `django-libsql`) over the network.

**Docs index:** [`docs/README.md`](docs/README.md) maps the per-subsystem how-it-works stories (data path, admission, durability, single-writer, warm-standby, rebalancing, migration, auth, directory), the design plans, the benchmark plans, the runbooks, and the run reports.

**What exists today (the working slice):**

- **Shard data path.** One **connection per Hrana stream** (`Fathom.Shard.Connection`, an `exqlite` connection to the shard file, WAL + busy_timeout), opened at stream start and closed at stream end, so transactions are isolated. A stream also **monitors its coordinator** (Filo's `Executor.owner/1` seam) and tears down if it dies — an orphan writer's WAL frames would be lost by a successor's flush-and-drop (finding #8). `Fathom.Shard` is the per-shard **coordinator** (one GenServer per active shard) owning the file lifecycle: on cold start it **pulls** the file from `Fathom.Shard.Storage`; it tracks checked-out connections (monitoring the stream processes); when idle with zero connections it **checkpoints + flushes** the file back to storage, drops the local copy, and stops — so a flush never races a write, and the local copy is bottomless-backed. All flushes are **write-gated by a `dirty` flag** (set by `ShardExecutor` on a write): a clean read-only/idle shard skips the upload, so durability PUTs track writes, not open-shard count. `Fathom.Shards` is the find-or-start router (`Fathom.ShardRegistry` + `Fathom.ShardSupervisor`); `Fathom.Shards.checkout/1` resolves a `shard_id` to its coordinator, starting it on demand, and returns `{:ok, pid, ref, path}`. **The shard engine is exqlite** (SQLite via `Exqlite.Sqlite3`, wrapped by `Fathom.Shard.Connection`) — decided 2026-07-06: the `Ecto`/`ecto_libsql` path was evaluated and **removed** (the data plane is a SQL proxy over opaque client SQL, which gains nothing from Ecto's schema layer and pays per-query overhead + a Rust NIF for it). `Fathom.Shard.Connection` is the single swap-point if a client ever needs the libSQL *engine* for compatibility. Rationale in `docs/cluster-architecture.md`. **Per-query resource bounds (review #26, all config-gated, off by default)** protect co-located tenants from one runaway query: `:query_timeout_ms` (a watchdog `Sqlite3.interrupt`s the connection at the deadline → 503 `FILO_QUERY_TIMEOUT`), `:query_max_rows` (`Connection.collect/2` errors past the cap instead of materializing an unbounded result → 400 `FILO_RESULT_TOO_LARGE`), and `:max_checkouts_per_shard` (the coordinator refuses a checkout past the cap → 503 `FILO_SHARD_BUSY`, so one tenant can't monopolize a node's streams or wedge the shard un-drainable). **How it works end to end** — the request→stream→connection→coordinator lifecycle, the orphan-writer monitor, and the idle/dirty flush — is `docs/data-path.md`; the durability/RPO contract (WAL + `synchronous=FULL` + the write-gated periodic flush) is `docs/durability.md`.
- **Shard storage.** `Fathom.Shard.Storage` is a `pull/2` + `flush/2` behaviour, backend chosen by `config :fathom, :shard_storage` — `Fathom.Shard.Storage.Local` (a filesystem object store, the default for dev/test) or `Fathom.Shard.Storage.S3` (Req + `aws_sigv4`, no AWS dep; works with S3 / R2 / Tigris / MinIO). A present local file is authoritative on wake (pull only on cold start), so an un-flushed shard is never clobbered.
- **Network protocol.** `Fathom.ShardExecutor` (a `Filo.Executor`) binds each Hrana stream to a shard. The **Filo** library (separate repo, `{:filo, path: "../filo"}`) speaks the libSQL Hrana wire protocol — HTTP v1/v2/v3 (+ cursor) and WebSocket hrana1/2/3 — on its own Bandit listener (`:hrana_port`, default 8080; gated by `:hrana_server`, off in test), separate from the web/dashboard endpoint on 4000. `django-libsql` (WebSocket) and `libsql-experimental`/SDKs (HTTP) both work end to end.
- **Shard selection + admission.** The shard comes from the request's **Host subdomain** (`acme.fathom.example` → `acme`; case-normalized via `Fathom.ShardId.cast` so `ACME`/`acme` are one shard — finding #19). `?db=` / `x-fathom-shard` are **dev-only** fallbacks gated by `:allow_shard_override` (off in prod — finding #4). When nothing resolves, the fallback is `:default_shard` — **unset in prod ⇒ fail closed with a 400**, not commingling into a shared shard (finding #26); dev/test set it to `"demo"`. **Novel-shard admission is double-gated** (finding #14): `:max_open_shards` caps how many shards a node holds open, and `Fathom.Shards.NovelLimiter` (`:novel_shard_rate`, off by default; `NOVEL_SHARD_RATE` in prod) rate-limits how fast *unseen* ids can mint new shards (429 over budget; existing shards — directory row, local file, or running coordinator — are never limited, and the directory check fails open on a Postgres outage). The cap is a **soft cap by default**: at the limit, a new open **evicts the least-recently-used *idle* shard** (flush + drop + release lease — `Fathom.Shards.Lru` + `drain/2`, `[:fathom,:shards,:evicted]` telemetry) rather than 503ing, since a shard's LB home is one node so a refused open means that tenant is down, while an idle shard is bottomless-backed and just cold-re-opens. A **busy** shard (checked-out connections) is never evicted; if the LRU-coldest handful are all busy the node is genuinely saturated and returns 503 (`[:fathom,:shards,:at_capacity]`). Set `:evict_idle_at_capacity` false for a hard cap (503 at the limit). Either way the open count stays bounded by the cap (the fd-cliff protection). **Template capture** (`:template_shard_id`, off in prod by default) records a shard's SQL for fleet-wide replay, so it's a poisoning vector if reachable anonymously: never set a prod template shard without auth on it, and never make `:default_shard` equal it — `Fathom.Application.check_template_default!` refuses that config at prod boot (finding #17). **How it works end to end** — the fail-closed Host routing, the double gate (soft `:max_open_shards` cap + LRU idle-eviction, the novel-shard limiter), and the template-poisoning boot guards — is `docs/admission.md`.
- **Auth.** `Fathom.HranaAuth`, gated by `:hrana_auth` (`:disabled` default; `HRANA_AUTH=required` in prod; unknown values fail closed to required; a boot guard refuses `:required` without a usable secret). A per-shard `Phoenix.Token` (signed with `secret_key_base`, minted via `mix fathom.token <shard>`) is presented as libSQL's `authToken` — an `Authorization: Bearer` header over HTTP, the Hrana `hello`'s `jwt` field over WebSocket (ws clients send no upgrade header, which is why this is Filo's `:authorize` callback seam, not a pre-plug). With auth `:disabled` the trust boundary is the network — the port must be reachable only via the LB (firewall/SG/private subnet; pin the interface with `HRANA_BIND_IP`). See `docs/deploy-cluster.md`; **how it works end to end** — the two modes, the per-shard token on Filo's `:authorize` seam, the no-oracle posture, and the network trust boundary — is `docs/auth.md`. **Token lifecycle (review #24)**: **zero-downtime rotation** — `HranaAuth.rotate/1` raises the shard's `token_version` (stamping `token_version_bumped_at`) and mints a new token while `verify` keeps accepting the previous version for `:hrana_rotation_grace_ms` (default 1h); `revoke/1` (bumped_at cleared) stays immediate. **Read-only scope** — `token_for(id, scope: :ro)` embeds an `"sc"` claim; `authorize/2` stashes the scope and `Fathom.ShardExecutor` rides it in the connection handle, refusing any write on a `ro` token with **403 `FILO_READONLY`** (no Filo change — the scope is carried per-stream). A **boot warning** fires when `:required` runs with an infinite `max_age`. Mint/rotate/revoke are on the `/api/tenants/:id/token` control-plane API (#21).
- **Postgres — `Fathom.Repo`.** The orchestration store and web UI backend, in the supervision tree (Phoenix dashboard on port 4000). It also backs the shard **directory / control plane** below; migrations live in `priv/repo/migrations/` (`shards` directory, `shard_migrations`, Oban jobs).
- **Directory / control plane.** `Fathom.Directory` (+ the `Fathom.Directory.Shard` Ecto schema) is the Postgres record of each shard's `schema_version`, lifecycle `status` (`active`/`migrating`/`retired`/`migration_failed`/`deleted`/`suspended`), `last_active_at`, and `retain_until`. It's the source of truth the rollout/migration machinery reads and flips (`resolve`, `cutover`, `retire`, `laggards`). It's decoupled from the data path: per-checkout accesses are **buffered + batch-flushed off the hot path** by `Fathom.Directory.Recorder` (a coalescing ETS buffer; a Postgres outage drops a flush, never a checkout). It also persists **`last_flushed_at`** (review #28) via a parallel Recorder buffer hooked at each successful upload, so a post-node-loss report survives the node: `Fathom.Directory.flush_lag_report/1` (and `mix fathom.shard loss-report`) list the shards active-since-their-last-flush with per-tenant loss windows (the node-local `FlushWatermark` ETS dies with the node). **How it works end to end** — the recorder's lock-free ETS coalesce + batch-flush + outage-safe re-buffer, and what the directory holds for the migration/rebalancing/warm-standby readers — is `docs/directory.md`.
- **Migration engine.** `Fathom.Migrator` runs blue/green per-shard schema migration: `Migrator.Capture` records template migrations into fleet versions, `Migrator.Copy` + `Migrator.ShardMigration` do the copy+transform and stamp `user_version`, `Migrator.Release` releases a fleet HEAD, and Oban jobs drive it — `ShardMigrationJob` (unique per shard), `ReconcileJob` (hourly cron sweep so the cold tail converges), `RetirementJob` (drop expired retained versions), `RevertJob` (fleet pointer-flip back; a revert backs up the live version first, and a **write-age force-guard** refuses a shard the directory shows active since its `cutover_at` — the job cancels, deterministically — unless `force: true` confirms discarding the post-cutover writes). There is **no `Fathom.ShardExec` or `Fathom.Retirement` module** — that work lives in `Migrator.Copy`/`ShardMigration` and `Migrator.RetirementJob`; don't grep for the old names. **How it works end to end** — the three-place version stamp, the copy-then-flip, cold-tail convergence, the guarded revert, and cross-version tolerance during a mixed vN-1/vN rollout — is `docs/migration.md`.
- **Tenant lifecycle — delete + export (`Fathom.Tenants`, review #15).** The whole-shard operations the migration engine left out of scope: GDPR-erasure/offboarding **delete** and portability **export** (a tenant *is* one SQLite file). `Tenants.delete/1` tombstones the directory row (`deleted` status) and broadcasts the tombstone fleet-wide **synchronously**, then enqueues `Fathom.Tenants.DeleteJob` (queue `:tenants`, unique per shard) for the durable erase: cancel pending per-shard Oban jobs → **force-stop** the home coordinator (`Fathom.Shards.stop/1` terminates it *while its lease is valid* so shutdown flushes/releases cleanly and never self-fences — the fix for a real leak where a graceful drain couldn't stop a busy coordinator, which then quarantined the erased data to a `.fenced.<ts>` file) → `Storage.purge_shard/1` deletes **every** stored object (live `.db`, `.lock`, all `@<version>`, all `@snap-<id>`; exact id-delimiter match so purging `acme` never touches `acme2`) → sweep local files. Purging is safe even if another live node still holds the lease: every coordinator flush is fenced (`If-Match`), so the deleted live object makes that node self-fence on its next flush instead of resurrecting it. The **re-mint guard** is `Fathom.Tenants.Tombstones` — a public ETS set of deleted ids checked O(1) in admission (`start_if_capacity` first branch ⇒ `{:error, :shard_tombstoned}`, off the Postgres hot path), loaded from the directory at boot, pushed on delete over Oban's notifier (which also purges each node's warm-follower copy), and periodically refreshed — so a stray request for a deleted subdomain is refused, never re-minted as an empty shard. **Export** is `Tenants.export/1` (pull the durable object to a temp file) served by `GET /admin/tenants/:id/export` (BasicAuth `send_download`, temp deleted after — never a public URL); `AdminDirectoryLive` gets per-row Delete (with confirm) + Export actions, and `deleted` is excluded from hand-editable statuses. **No feature flag** — inert until an operator invokes it. Cross-node drain of a *busy* remote shard via the `rebalance_commands`/`command_poller` path is a follow-up (single-home + the fenced-flush self-fence cover correctness today). **Provisioning (review #21)** completes the control plane: `Tenants.provision/1` (explicit directory insert + fork-from-template birth when `:fork_from_template` is on + token mint, returning the `libsql://<id>.<base>` URL + `auth_token`) and `FathomWeb.Api.TenantController` — a JSON create/list/get/delete API under `/api`, behind the same admin BasicAuth on `:4000` (separate from the Hrana port), so a platform customer creates/lists/deletes tenants instead of relying on traffic-implied minting. **Suspend/resume (review #20)** is the administrative-offline lever short of deletion: a `suspended` status + `Fathom.Tenants.Suspensions` (the Tombstones ETS gate but reversible — add/remove notification + periodic reconcile). `Tenants.suspend/1` flips the directory, broadcasts so every node denies new streams, and graceful-drains the home coordinator; `resume/1` reverses it. **Both lifecycle denies live in `Fathom.Shards.ensure/1`** (checked on every checkout, O(1) ETS) so a deleted/suspended tenant is refused even with a running coordinator; a suspended open is a distinct **403 `FILO_TENANT_SUSPENDED`** and a deleted one a **410 `FILO_TENANT_DELETED`**. `POST /api/tenants/:id/{suspend,resume}` + admin Suspend/Resume buttons. **Fork + operator tooling (review #14)**: `mix fathom.shard pull|inspect|fork` (`inspect` = a per-shard restore drill: pull + `quick_check` + row counts) and **database forking** — `Storage.fork_shard/2` (one object copy) + `Tenants.fork/2` (clones a live tenant to a new id, registering the dst at the *source* schema version so the laggard sweep won't re-migrate the fork; non-disruptive to src) + `POST /api/tenants/:id/fork`. **How it works end to end** is `docs/tenant-lifecycle.md`.
- **Per-shard load counters (`Fathom.ShardLoad`) — the Phase-2 rebalancing (B) prerequisite.** A public ETS table of per-shard cumulative counters (checkouts, queries, `rows_read`/`rows_written` = query cost), bumped **lock-free from the executing process** (`:ets.update_counter`, `write_concurrency` — the `Directory.Recorder` pattern, no per-query GenServer hop) on `Fathom.Shards.checkout` and `Fathom.ShardExecutor.execute`; a stopped coordinator's row is dropped in `terminate` (`forget/1`). A control plane reads `top(n, by)` / `snapshot/0` to find hot shards (rates = diff two snapshots, churn-safe); the **first reader is `mix fathom.scale --hotspots`** (the §B evidence harness — see Benchmarking). Deliberately **not** a per-shard `Telemetry.Metrics` (a per-shard tag at millions of shards is cardinality death) — the read API is the interface. Gated by `:shard_load`, **off by default** (nothing in the request path consumes it until the rebalancer lands, so the hot path doesn't pay for an unread counter).
- **Dynamic rebalancing — Phase 2 B1 (`Fathom.Rebalancer.*`, built 2026-07-06).** Moves a persistently-hot shard off an overloaded node by layering a per-subdomain **exception table** on the LB's consistent hash. Since there's no BEAM cluster (LB-partition coordinates via S3 for data, Postgres for orchestration), each node **reports**: `Rebalancer.Reporter` (gated `:load_reporter`) diffs two `ShardLoad` snapshots into rates and writes its hot set to `shard_load_samples` (Postgres), tagged with a stable `Rebalancer.node_key/0`. The Oban cron `RebalanceJob` (fleet singleton via Oban's Postgres peer leadership; gated `:rebalancer_enabled`) reads the merged fleet view and runs `Rebalancer.Policy` — hot = **absolute q/s floor or `K×p99`, never `K×median`** (the `--hotspots` finding), with 2-window anti-flap, cooldown, an improvement guard (never relocate a lone hotspot), and least-loaded target — then enqueues a unique-per-shard `HandoffJob`. The handoff is **warm the target → pin the `shard_overrides` row + render the nginx map (`LbMap`) + reload (`LbApply`) → drain the source (release the lease) → target acquires**; a **flip-before-drain** ordering (the `{owner,epoch}` lease blocks any double-write regardless — a healthy node can't be *stolen* from, so the move is a voluntary drain, not the crash-path self-fence). Warm/drain reach a node the orchestrator can't RPC via a `rebalance_commands` Postgres channel + per-node `CommandPoller` (gated `:command_poller`). **Proven live on the chaos rig** (`chaos.sh rebalance`: `acme` @ 143.9 q/s moved fathom1→fathom2, isolation intact). All gates **off by default**; the enable-path runbook (staged gate-by-gate, with an observe-before-arming dry run + rollback) is `docs/runbooks/rebalancer.md`. Hardened per `docs/reviews/expert-review-2026-07-07-013852.md` and re-proven live in `docs/reviews/chaos-run-2026-07-08.md`. See `docs/phase2-scoping.md` §B1 for scoping; **how it works end to end** — detect (per-node reporting) → decide (the p99/floor hotness bar + guards) → execute (the flip-before-drain handoff) — is `docs/rebalancing.md`.
- **Cross-node single-writer (lease + epoch fence + node heartbeat).** `Fathom.Shard.Storage` carries a per-shard **lock** (`{owner, epoch}`, the monotonic `epoch` is the fencing token) and a per-node **heartbeat** (`Fathom.Shard.Heartbeat` renews one `heartbeat/<node>` object every `ttl/3` — liveness is O(nodes), not O(shards), which is the F1 fix). A shard's owner is live iff its heartbeat is fresh, so `acquire_lease` consults the owner's heartbeat to decide held-vs-steal (and fails closed on a heartbeat read error). Coordinators do **no per-shard renewal**; before a flush they fence via `Heartbeat.valid_for_write?/1` (heartbeat valid-with-margin + no lapse since acquire ⇒ write with no per-shard I/O; on a lapse, re-check the lock via `Storage.check_lease/2` and **self-fence** if superseded so a remapped shard never double-writes). If the heartbeat process is down, coordinators degrade to the legacy per-shard renew fence. This is the only cross-node coordination — via S3, not BEAM. **How it works end to end** — the storage behaviour, the lease/epoch/heartbeat primitives, cold-open acquire+pull, the steady-state fence, the crash-steal vs voluntary-drain directions, and the loss contract — is `docs/single-writer.md`.
- **Cluster layer (LB-keyspace-partition).** The L7 load balancer consistent-hashes the `Host` subdomain to one backend node; each node is an independent single-node fathom, and the S3 lease above handles remap safety. `Fathom.HealthPlug` serves `GET /health` (per-node LB probe, `:health_port` default 8081, gated by `:health_server`). `Fathom.Telemetry` runs Telemetry.Metrics over the shard/lease/checkout events + an active-shard poller + a checkout→OpenTelemetry span bridge (traces env-gated on `OTEL_EXPORTER_OTLP_ENDPOINT`, off by default). LB config in `deploy/lb/`; `docs/deploy-cluster.md` + runbook in `docs/runbooks/cluster.md`. The **chaos rig** (`deploy/chaos/` — 3 prod-release nodes behind nginx + MinIO + per-node toxiproxy, driven by `chaos.sh`: `failover`/`pause-fence`/`partition`/`soak`/`warm-home`/`hotspots`/`rebalance`/`density`/`served`/`served-data`/`latency-cost`/`tpc-fleet`) is the failover-time-and-loss-window layer the in-process cluster suite (`test/fathom/cluster/`) can't measure — and (`hotspots`, gated by `SHARD_LOAD=true`) the real-traffic hot-spot layer the in-process `--hotspots` harness can't: it drives Zipf-skewed real Hrana traffic through the LB and reads `Fathom.ShardLoad.top` per node via `bin/fathom rpc` (2026-07-06 run: 17.5k requests, top-20 Zipf-head recall 0.95, hot set spread across all 3 nodes — the per-node-read/merge model confirmed). **`rebalance`** demonstrates the full **B1 handoff live**: drive load on a shard, show the reporter detected it, then pin it + drain the source + prove it moved to the target — the **`lb-reloader` sidecar** (shares nginx's PID namespace, HUPs its master on a map change) applies the flip with no host bridge. Runs: a manual pin (2026-07-06, `acme` fathom1→fathom2) and a fully **autonomous** one (2026-07-07: `Policy.propose` chose `green: fathom2→fathom1` off an engineered imbalance, `RebalanceJob`/`HandoffJob` executed it), both isolation-intact. **`density`** is the fleet counterpart to the single-node `--ramp` ceiling: it mints N novel shards through the LB and reads per node the coordinators held (the keyspace partition) + BEAM/RSS per shard (the cost) — 2026-07-10 run, N=30000 over 3 nodes (~10k/node), spread evenly (1.22×) at ~16 KiB/shard RSS on every node (marginal cost *falls* with scale as fixed overhead amortizes), so capacity is horizontally additive and only the active working set is resident — this is the warm-resident floor (idle, empty shards). **`served`** measures the complementary **fd-bound** ceiling — shards held under a *live connection*: 30k across the fleet (10k/node) at ~220 KiB/shard and **1 fd each**, so served density is a `nofile` knob (compose raises the nodes' soft limit to 65536; the default 1024 caps ~940/node) not a memory or architectural wall. **`served-data`** adds real data per shard (30k shards seeded 256 KiB each, held + scanned): ~640 KiB/shard and **3 fds** (WAL-active `-wal`/`-shm`), and the key result — an *idle* data-bearing shard drops back to ~43 KiB/0 fds (the 256 KiB lives on disk, not RSS), so a *stored* tenant costs disk and only an *active* one costs the connection + page cache. All four regimes (idle floor ~16 KiB, served ~220 KiB, served+data ~640 KiB, idle+data ~43 KiB) measured at 30k across the fleet; served-data was stress-pushed to **105k held / ~150k RAM-bound** (nofile maxed) — the open-rate decay past ~30k/node is a one-process test artifact (production holds one process per stream), not a fathom limit (`docs/reviews/fleet-density-2026-07-10.md`). **`latency-cost`** MEASURES what `latency <ms>` INJECTS — it times the two S3-round-trip-bound paths (cold-open pull, flush upload) per node, baseline vs an injected one-way RTT (2026-07-23 sweep, 10/30/60 ms: cold-open ~26/70/133 ms ≈ **~1 RTT**, matching the in-process `benchmark_s3_sweep.sh` — the `Fathom.Shard.init` lease+pull overlap working on the fleet; flush/drain ~53/143/270 ms ≈ **~2.3× RTT**, down from ~3.5× in the 2026-07-11 sweep after perf review 2026-07-23 #6 removed the drain's redundant lock GET — the lease carries the lock etag from our own PUT responses, so release is one conditional DELETE. The remaining data-PUT→lock-DELETE ordering is irreducible (releasing before the flush lands would let a peer pull stale bytes), so drain now sits within ~0.3 RTT of the design floor — `docs/reviews/latency-cost-2026-07-23.md`; history in `latency-cost-2026-07-11.md`, whose sidecar-upload attribution of the third RTT was wrong — the sidecar is a local file). **`tpc-fleet`** is the throughput analog of `density`: it drives many tenant shards — one single-writer file each — through the LB so they partition across the nodes, then reads back the aggregate txn/s + the per-node load split (2026-07-23 runs, post the LB-502 fix, 6→1024-tenant sweep: **query load follows the shard partition** across all 3 nodes, **zero errors at every step** — 128 tenants (2× the concurrency that used to collapse the rig in a 502 storm) ran 19,200 txns with 0 driver errors and 0 LB upstream failures, and the consistent-hash spread *improves* with key count (1.27× at N=128, 1.06× at N=256) — aggregate **~890 → ~1,066 → ~1,083 → ~1,075 → ~1,082 → ~1,026 txn/s**, a clean CPU plateau on the one rig VM (sagging only ~5% under 256 concurrent held connections — no second ceiling in sight), within ~5% of the p50-implied fair-queueing ceiling at every step, with per-txn p50 6.6 → 11.0 → 20.9 → 57.4 → 113.8 → 239.8 ms growing linearly with concurrency at saturation across independent writers; at N=256 the consistent-hash spread is **1.06×** (85/83/88 — the ring evening out with key count); **1024 concurrent tenants = ~2,661 tps aggregate** (uniform-methodology sweep 2026-07-24: in-network driver containers, 4×256 clients; 153,008 txns, 0.4% transient client reconnects, zero server-side failures) — and aggregate RISES with driver count (~1,140 → 1,625 → 2,661 at 1/2/4 procs), so every single-driver 'plateau' was the DRIVER's ceiling, not the fleet's; the fleet ceiling on this VM is ≥2.7k tps and unfound. High-concurrency driving rules (colima's host forwarder dies ~1k conns — drive in-network; ≤128 clients per driver process — a solo 256-client in-network proc reproducibly collapses, unattributed, see the addendum + tasks/todo.md): `docs/reviews/tpc-fleet-highconc-2026-07-23.md` — but those driving rules are the *Python* driver's, not fathom's: the **Elixir/Filo.Client driver** (`deploy/chaos/tpc_driver.exs` — one lightweight BEAM process per client, no GIL), run as an **in-network container** (`chaos.sh build-driver`; then `TPC_NET=container TPC_DRIVER=elixir ./chaos.sh tpc-fleet`), drove the whole **6→4096-tenant sweep from ONE process, zero errors at every step** — no ≤128-clients/proc cap, no multi-process orchestration (one driver bug en route: a connect+seed thundering herd crashed it at ≥2048, fixed with a per-worker connect stagger + retry; the fleet processed every query at every attempt — ~3.4–4k tps CPU-bound on this VM, spread ≤1.15× out to 4096; a parameter-matched host A/B has the Elixir driver **1.6–2.8× faster** than Python in its clean range and error-free where one Python process collapses at ≥128 clients — `docs/reviews/tpc-fleet-elixir-driver-2026-07-24.md`; the Elixir driver also covers **tpcc** now (all five value-feeding txns + the W-sweep, tpmC parity with Python) plus a **`tpcc-fleet`** multi-tenant sweep (one 1-warehouse TPC-C per tenant, 16→4096 tenants error-free, aggregate tpmC ~25.6k-peak, even per-node partition ≤1.12×), which surfaced a **Filo.Client resilience fix**: a `:closed` connection — e.g. an LB recycling an idle client keep-alive after N requests — is transparently retried with the **same baton** (resume the Hrana stream, exactly-once via the baton seq) instead of abandoning a held stream mid-transaction), still **no intra-shard convoy** (contrast the single-shard TPC-C lock convoy); absolute txn/s is single-host-bound — the horizontal "millions" throughput axis is the even per-node split, not the one-box aggregate — `docs/reviews/tpc-fleet-2026-07-23.md`. **All pre-2026-07-23 tpc-fleet absolutes (e.g. the 07-11 run's ~40 tps) are obsolete**: they were ~95% LB-502 reconnect overhead from an nginx `Connection: close` upstream-keepalive bug, fixed in both LB configs; the old "moderate concurrency by design" rig caveat is retired with it — `docs/reviews/lb-502-fix-2026-07-23.md`). See the § Chaos testing two-layer split in `docs/deploy-cluster.md` and the runs in `docs/reviews/chaos-run-2026-07-05.md`. **The cluster phase (S1–S8) is complete** — LB partition, S3 lease/heartbeat fence, crash/partition contract, observability, directory-off-hot-path, and lease-RPS measurement all shipped (see the Status in `docs/deploy-cluster.md`); what remains cluster-side is Phase 2 (below).
- **Bench + scale harnesses.** `mix fathom.bench` (+ `scripts/benchmark.sh`, `scripts/perf_history.jsonl`, the `scripts/commit_with_bench.sh` regression gate) measures the hot paths; `mix fathom.scale` measures cold-open at real size, node density (`--ramp`), warm density (`--warm-density`), lease-renewal load (`--lease-rps`), and hot-spot detectability (`--hotspots` — the Phase-2 §B rebalancing evidence: Zipf-skewed load read back through `Fathom.ShardLoad`); `mix fathom.rpo` (`Fathom.Rpo`) quantifies the **loss window** — it sweeps `:shard_flush_interval_ms` × write rate and reports lost rows/seconds on node loss (survivor cold-opens the last flushed object) plus a process-crash-loses-nothing check (`synchronous=FULL`), the in-process complement to `deploy/chaos/chaos.sh soak` (see `docs/durability.md`). See Benchmarking.
- **Warm standby (Phase 2, A1 — built).** `Fathom.Shard.WarmFollower` (gated `:warm_follower`, **off by default** and in test) pre-pulls the fleet's recently-active shards this node doesn't own (`Fathom.Directory.active_recent/1`, LRU-capped at `:warm_cache_max`) into a **separate** cache dir, holding the file but **no lease** — it never serves. On failover the coordinator's cold-open **promotes** the warm copy only after a **freshness check** (a warm cache may lag the owner's latest flush; a stale copy is never served): `Fathom.Shard.Storage.pull_if_changed/3`, a conditional `If-None-Match` GET — 304 promote the cache / 200 re-pull fresh bytes / 404 brand-new. The follower records each object's etag in a `<shard>.db.etag` sidecar and **revalidates its whole cached set each poll** so a failover lands on the 304 fast path. A live-dir warm *restart* (this node's own un-flushed writes) still wins untouched — only the follower-cache path is validated. The warm win is the object **body transfer avoided** — ~2.3× at 1 MB / 30 ms / 100 Mbps, marginal for tiny shards on a fat pipe (both paths still pay ~1 S3 RTT). See Benchmarking (failover RTO, warm density); **how it works end to end** — the lease-less read cache, the freshness/etag revalidation, the failover promote path, and the warm-home rule — is `docs/warm-standby.md`.

**Planned (see `docs/migration-plan.md`), NOT in the code yet — don't assume these exist:**

- **Phase 2 remainder**: shard locality/affinity (C) — its **affinity-aware placement** piece is now **built** (folded into B1): a per-node **warm-location signal** (`Fathom.Rebalancer.WarmLocations` + `shard_warm_locations`) lets `Rebalancer.Policy.best_target` prefer a handoff target that already holds the shard warm, within a load band of the least-loaded so balance still improves (`:rebalance_locality_band`, default 0.5). What remains of C is C1 (rendezvous/bounded-load hashing — marginal per `docs/phase2-scoping.md` §C) and C2 (multi-region region-affinity — a separate initiative). Also **live WAL streaming (A2, deferred)**. The warm standby (A1) and **dynamic rebalancing (B1)** are **built** — see the warm-standby and rebalancer bullets above. A `fathom_native` Rust NIF (no `native/`, no Rustler dep yet).

## Execution style

- **Sequenced directives** ("do X then Y", "review then execute") → execute directly. Don't pause to re-confirm the sequence or ask which item to start with. If genuinely ambiguous, name your default and proceed; stop only if the ambiguity risks irreversible harm.
- **"Go ahead" / "continue" / "proceed"** = continue the *most recently scoped* task. Never authorization to escalate review→implementation or jump phases. Asked for a review → deliver only the review.
- **Locating things:** one targeted Read/Grep/Glob, not a fan-out of speculative `find`/`ls`. If the first lookup fails, widen the query.
- **Don't narrate intentions** ("let me check…", "I'll now…"). State results.

## Build

```bash
mix setup          # deps.get + ecto.setup (Postgres) + assets
mix compile        # build (mix precommit uses --warnings-as-errors)
mix test           # creates+migrates the test Postgres DB, then runs tests
mix test test/fathom_web/controllers/page_controller_test.exs:7  # single test
mix test --failed  # rerun last failures
mix format         # format
mix precommit      # the gate: compile --warnings-as-errors, deps.unlock --unused, format, test
iex -S mix phx.server   # start app (dashboard :4000)
```

- **Shell is zsh — stop re-learning this.** These bite every session; internalize them:
  - Backticks and `$(...)` run command substitution even inside double quotes — a backtick or unescaped `$(` in a `git commit -m "..."` body gets executed and silently mangles the message. Don't wrap identifiers in backticks inside `-m`; use `git commit -F <file>` for any non-trivial message.
  - **zsh parameter-expansion patterns glob.** `${var#pat}` / `${var%pat}` treat `(`, `[`, `]`, `#`, `?`, `*` as pattern metacharacters, so `${m#](}` dies with `bad pattern: ](`. Don't hand-strip brackets/parens with `${...#...}` — use `grep -oE`, or just do the text munging in `python3`.
  - Unquoted globs (`*`, `?`, `[...]`) and `{a,b}` brace-expand — quote them when you mean literals.
  - **Don't assume `sed`/`awk`/`dirname`/`head`/`tail` are on PATH** — several aren't in this sandbox (and AGENTS.md already says use Read/Grep/Edit to read/edit files, never those). For a text pipeline, reach for `grep` or a short `python3 - <<'PY'` heredoc, not `sed`/`awk`.
- **When `fathom_native` lands:** the NIF builds via Rustler on `mix compile`; release builds need `MIX_ENV=prod` and are slow (set timeouts). Rust tests: `cd native && cargo test`.

## Workflow

Plan mode for non-trivial tasks (3+ steps or an architectural decision). Stop and re-plan when something goes wrong. Use subagents for research/exploration/parallel work — one task each. When the parent model is Fable, use your own judgement about which model each subagent should run on (match the model to the subtask's difficulty rather than defaulting to the parent's).

**Implementation cycle:** implement → compile → test → (bench if hot path) → `mix precommit` → commit → **push**. Test after every change; fix failures before proceeding. Commit in logical units matching plan phases.

- **ALWAYS `git push` immediately after every commit.** The gate lives at the *commit* — there is no separate gate at push, and push is never something to wait to be asked for. An unpushed commit is unbacked-up work; never batch local commits. (This rule exists because a full night's work was once lost to local-only commits.)
- **NEVER commit** with compiler warnings, build errors, or failing tests. `mix precommit` is the gate (see Gates).
- **Never use `sed`/`awk`/`head`/`tail`/`echo` to read or edit files** — use Read (offset/limit), Grep, Edit/Write. Shell text tools are only for things the standard tools genuinely can't do. Piping command output is fine.
- Track plans in `tasks/todo.md`. Record corrections/lessons in `tasks/lessons.md`.

### Stop-after-2-failures rule

If a script or command (test/build/migration/sweep) fails **twice with a similar error**, stop. Print the exact command, the exact error, and a one-paragraph root-cause hypothesis, then wait. Don't loop on infra failures (missing dep, DB-not-created, port conflict, libSQL file-lock) — diagnose them. Same rule for scope blowups: a refactor producing **>50 compile errors** or running **>60 min past estimate** → stop and report. Never paper over flaky tests or build failures with sleeps, retries, or timeout bumps.

## Testing

Complements the framework **Test guidelines** below (`start_supervised!`, no `Process.sleep`/`Process.alive?`, monitor for DOWN) — those still apply. This section is the *discipline*.

- **Add coverage with every feature:** happy path, error cases, edge cases, backward compatibility. **Don't use TDD/red-green unless explicitly asked** — default to implementation + tests together (or test-after for small changes). If you think red-green fits, suggest it and wait.
- **Two stores, two test modes:**
  - **Postgres directory (`Fathom.Repo`)** → `Fathom.DataCase` with the Ecto SQL sandbox (async-safe, auto-rollback per test).
  - **libSQL shards (`Fathom.Shard` / `Fathom.Shards`)** → no Ecto sandbox; a shard is a real SQLite file. Use a unique `shard_id` per test, drive it through `Fathom.Shards`/`Fathom.ShardExecutor` (the registry + supervisor come up with the app), and `File.rm` the file (`System.tmp_dir!/fathom_shards/<id>.db`) in `on_exit`. Never let two tests share a shard file. See `test/fathom/shard_executor_test.exs`.
- **Save test output to timestamped logs** so results are readable without rerunning, then prune logs older than a day and read the latest instead of rerunning:
  ```bash
  mix test 2>&1 | tee "logs/test-$(date +%Y%m%d-%H%M%S).log"
  find logs/ -name "test-*.log" -mtime +1 -delete 2>/dev/null
  ls -t logs/test-*.log | head -1 | xargs cat   # read latest
  ```
- **Every bug fix ships with a regression test in the same commit.** It must (1) **reproduce deterministically** — fail pre-fix, pass post-fix; if you can't make it fail without the fix you haven't isolated the bug, keep investigating — and (2) **pin the violated invariant**, not just the reproduction steps. Comment the symptom so future readers know why the test exists. Good targets: concurrency/thread-local races (test at the pure-function level), off-by-one/boundary (test at the boundary), classifier/dispatcher mismatches (test the classification), lifecycle ordering (test the sequence).
- **Fathom-specific must-test invariants** (these are the bugs that bite a sharded multi-tenant system):
  - **Shard isolation.** A query for shard A must *never* resolve to or read shard B's data. Any change to routing — `Fathom.ShardExecutor.shard_from_conn` (request → shard), `Fathom.Shards` resolve, shard-path construction in `Fathom.Shard`, or the planned `Fathom.Directory` — ships with a test proving cross-shard isolation.
  - **Migrations are tested both ways.** Every schema migration ships with a test that runs the forward copy+transform on a seeded `vN-1` shard and validates `vN` (row counts / checksums), **and** a test for the revert pointer-flip back to `vN-1`. See the migration gate.
  - **Cross-version tolerance.** During a rollout the fleet is mixed `vN-1`/`vN`; assert the app reads both (`schema_version`-aware branch, or `vN` superset still usable by old code).
- **Hot-path verification.** When you change a hot path (shard cold-open, directory resolve, migration copy, concurrent shard fan-out), add a microbench-style test that asserts an order-of-magnitude floor/ceiling (e.g. `assert open_us < 50_000`), not an exact latency. Tag it (`@tag :bench`) so it's excluded from the default suite. See Benchmarking.

## Benchmarking

**The harness exists** (`docs/benchmark-plan.md`). `mix fathom.bench` measures the hot paths; `scripts/benchmark.sh` runs it prod-compiled against a throwaway `fathom_bench` DB and appends one JSON line per run (commit, branch, dirty, host, metrics) to `scripts/perf_history.jsonl`; `scripts/commit_with_bench.sh` is the bench-then-commit gate — it benches the working tree and **refuses the commit if ANY metric regresses ≥20%** vs the parent's same-host entry (`Fathom.Bench.Gate`; multi-metric because fathom's cost is per-shard open + fan-out; same-`host` comparisons only). Metrics: `cold_open_p50_us`; `cold_open_s3_p50_us`, `warm_s3_shards_per_s`, `failover_cold_s3_p50_us`/`failover_warm_s3_p50_us` (all opt-in S3 — unset `FATHOM_S3_TEST_*` ⇒ `nil`/skipped, so the default gate stays S3-free); `dir_resolve_p50_us`; `copy_rows_per_s`; `fanout_kb_per_shard`; `hrana_rt_us` (null placeholder until remote shards). Hot-path changes also ship `@tag :bench` floor/ceiling guards (`test/fathom/bench_test.exs`). Hold the discipline: don't invent fake numbers; measure or say "unmeasured."

- **Hot paths to watch** (fathom's scaling story is *millions of small shards*, so cost is dominated by per-shard open and fan-out, not single-query throughput):
  - **Shard cold-open latency.** Two paths: *warm* — local file present (node restart, or local-NVMe `Storage.Local`) — is `cold_open_p50_us` (~ms, often page-cache-warm); *cold-from-S3* is `cold_open_s3_p50_us` (opt-in). Realism: MinIO-on-localhost measures the S3 *protocol* + loopback (~6 ms), NOT real S3 latency — either point `FATHOM_S3_TEST_ENDPOINT` at real S3/R2, or use `scripts/benchmark_s3_latency.sh`, which puts **toxiproxy** between the bench and MinIO and injects latency/bandwidth (`S3_FAKE_LATENCY_MS`, `S3_FAKE_RATE_KBPS`). Cold-open is optimized to **~1 S3 round-trip**: the lease acquire is an optimistic conditional create (`PUT If-None-Match:*`, read-then-resolve only on 412) and `Fathom.Shard.init` overlaps it with the pull (independent objects, `.lock` vs `.db`). The pull lands in a temp file promoted only after the lease confirms — a lost lease race never leaves a stale local copy, and we still only *serve* after the lease confirms. Expect ~2× one-way latency + a few ms (`scripts/benchmark_s3_sweep.sh`: one-way 10/30/60/100 ms → ~26/77/137/215 ms); bandwidth helps warming throughput, not single-shard cold-open.
  - **Warming throughput** (`warm_s3_shards_per_s`, opt-in S3) — aggregate shards/s a node can pull from S3 at once (startup/failover). Two design facts keep it high: (1) the lease+pull runs in `handle_continue`, not `init`, so concurrent opens don't serialize at the `DynamicSupervisor` (a `:checkout` queues until the open completes; an open failure stops the coordinator with `{:shutdown, _}` — not restarted — and `Fathom.Shard.checkout/1` maps that exit to `{:error, _}`); (2) `Fathom.Shard.Storage.S3` runs a **dedicated, config-driven Finch pool** (`finch_child_spec/0` supervised in `application.ex`; `config :fathom, Fathom.Shard.Storage.S3, pool_size:` default 200 — the measured knee on the localhost rig — plus `pool_count`, default 1, for real-S3 tuning; `req/0` falls back to Req's default pool when ours isn't started). Measured 2026-06-29 (dev build, MinIO+toxiproxy, 30 ms — a relative lever, not a prod-absolute): ~10 → ~800 shards/s across the two fixes; past ~200 conns the localhost MinIO server saturates, so `pool_count` can only show a win on real S3. Sweep with `FATHOM_S3_TEST_POOL_SIZE=N FATHOM_S3_TEST_POOL_COUNT=C S3_BENCH_ONLY=warm_s3 scripts/benchmark_s3_latency.sh --warm-shards M`.
  - **Directory resolve** — Postgres lookup + resolve cache hit/miss; this is on every request.
  - **Migration copy throughput** — rows/sec per shard for the blue/green copy+transform.
  - **Failover RTO — warm standby vs cold (opt-in S3).** `failover_cold_s3_p50_us` (survivor cold-opens with a full S3 pull) vs `failover_warm_s3_p50_us` (shard is in the warm-follower cache; the freshness check is a conditional **304** GET + a local copy), both at `:warm_size_kb`; `mix fathom.bench --only failover_rto` prints the speedup. **The honest delta:** the warm path is *not* purely local — it still pays one S3 round-trip (the 304, no body). The win is the object **body transfer avoided**, so it scales with shard size × bandwidth-delay and is **marginal for a tiny shard on a fat pipe**. Measured 2026-07-01 (dev build, MinIO+toxiproxy, relative): 1 MB / 30 ms one-way / 100 Mbps → **cold ~162 ms, warm ~72 ms (≈2.3×)**; the warm floor is the lease + freshness round-trips, not ~2 ms. (Those numbers predate the steal-touch takeover machinery, which added round-trips to both paths; don't compare new runs against them directly. 2026-07-23 A/B, same rig, no bandwidth cap: the takeover chain is **~1 RTT cheaper** than pre-iteration — failover cold/warm −10–11% at 30 and 60 ms one-way, the #13 confirm-rotation HEAD now read from the touch's own write response — `docs/reviews/s3-latency-ab-2026-07-23.md`.)
  - **Warm-standby density.** `mix fathom.scale --warm-density [--shards N] [--shard-size-mb S]` pre-pulls N shards into the follower cache and reports per-cached-shard disk, BEAM bookkeeping, and warming rate. **Finding:** a warm-cached shard costs its file on disk plus ~0 process/BEAM/fd (no coordinator, no connection), so warm capacity is **disk-bound** (`disk / shard_size`) — orders of magnitude past the ~196 KiB + 3-fd open-shard ceiling. A standby warms far more than it can serve open.
  - **Concurrent shard fan-out** — how many shards a node can hold open at once. **Two ceilings** (measured 2026-06-29): *connection-per-shard* (every shard actively streaming) is **fd-bound** at ~`kern.maxfilesperproc / 3` (~3 fds per WAL connection; ~82k on a 245760-limit box) — fds bind well before memory. *Idle-open coordinators* (connections are per-stream/transient) go far higher, memory-bound at ~26 KiB each. Per-shard cost is connection + coordinator **overhead** (~180–196 KiB), not the page cache — density is overhead-bound, not data-bound.
  - **Hrana round-trip** — once remote shards land.
- **Run clean and in prod mode.** Compile in `MIX_ENV=prod` (and the Rust NIF in release, slow) for any bench run; start from a clean DB/data state; don't bench a dev build.
- **Tiered regression response:** **<20%** — ignore (noise). **≥20%** — assume real: rerun once to rule out noise; if confirmed, **revert first**, then reproduce minimally on a branch. Never stack fix attempts on top of a known-regressed commit.
- **Phantom-regression rule.** Declaring an observed regression "phantom" (numbers wrong, not the code) requires (a) the same regression measured by a *second* tool/environment, (b) an explicit explanation of why the original measurement was wrong, and (c) a cooling-off before closing. Don't declare phantom-regressions the same day they're observed — broken instrumentation is not evidence of a fixed regression.
- **Scale test (built).** `mix fathom.scale [--shards N] [--shard-size-mb S]` (`Fathom.Scale`) provisions N realistically-sized shards and measures cold-open latency at size + **fan-out node density** (BEAM + RSS per open shard, open throughput). `--ramp [--max N]` opens empty shards until the fd ceiling to find the node-density limit cheaply; `--warm-density` per the bullet above; `--lease-rps` shows per-shard lease renewals collapsed to one node heartbeat (flat regardless of shard count). Raise `ulimit -n` first (~3 fds per live connection). Measured 2026-06-29: 1000 × 4MB → ~196 KiB RSS/shard, warm cold-open ~2.6 ms p50; ramp held 50k linearly → fd ceiling ~82k. (The ramp's per-open slowdown at high N is a one-process test artifact, not fathom — production spreads shards across per-stream processes.)
  - **Hot-spot detectability (`--hotspots`, the Phase-2 §B rebalancing unblock).** `mix fathom.scale --hotspots [--shards N] [--zipf s] [--queries Q] [--workers W] [--stream-len L]` (`Fathom.Scale.hotspots/1`) is the **first reader of `Fathom.ShardLoad`**: it turns on `:shard_load`, drives a Zipf(s)-skewed load over N shards through the real recording path (`Shards.checkout` → `ShardExecutor.execute`), then reads the counters the way a rebalancer would — diff two `snapshot/0`s over a window into per-shard rates. **Load unit is a Hrana stream** (`--stream-len L`): each stream checks out + opens a connection once, bursts L queries on the held connection, then checkins — the real per-stream model. `L=1` (default) is one query per stream (the per-query lower bound, finest detection sampling); raise L to measure realistic throughput, scaling `--queries` so `Q/L` (the stream count) stays ≫ N. Reports the rate distribution (p50/p90/p99/max), two separations (max/median and tail-robust max/p99), **three threshold-family sweeps** (`>Kx median`, `>Kx p99`, and an absolute q/s floor isolating the top-N — each with a Zipf-recall check), a `median_collapsed` flag (shape-based: `>10x-median` flags ≫ `>10x-p99` ⇒ median-relative over-flags), a scale-robust anti-flap signal (top-20-by-rate set overlap across two windows), and whether `ShardLoad.top(20)` recovers the Zipf head. **Detection finding (prod, 10k shards, s=1.1 — synthetic/one-host-relative):** clean (`ShardLoad.top(20)` recall **1.0**, top-20 Jaccard **0.9**, sharp head hot_1 188 q/s → …), but the long cold tail makes **`>Kx-median` over-flag (751/421/216) — key hot-detection on `>Kx-p99` (>20×p99 → 5 shards, recall 1.0) or an absolute q/s floor.** **Throughput finding (prod, 1000 shards, ~20k streams; 2026-07-23):** persistent streams remove the per-query coordinator bottleneck — L=1 **~3.3k q/s** → L=16 **~27k** → L=64 **~54–55k q/s** per node (hottest shard ~600 → ~4.9k → ~9.5k q/s), detection quality flat across all L. So the earlier ~1.2k q/s was the per-query artifact, not a fathom limit. The held-stream numbers are **+13–16% over the pre-2026-07-23-iteration code** (same-day A/B — the per-connection statement cache: at L=64, 63 of 64 queries skip `sqlite3_prepare_v2`), while L=1 is flat — a one-query stream never hits the cache and is per-stream-open-bound, which is the connection-pool question, measure-first pending `hrana_rt_us` (`docs/reviews/hotspots-ab-2026-07-23.md`). **Non-synthetic confirmation — DONE** (`deploy/chaos/chaos.sh hotspots`, 2026-07-06): 17.5k real Hrana requests through the LB over 3 nodes recovered the Zipf head at top-20 recall 0.95, with the hot set spread across nodes (a rebalancer reads `ShardLoad` per node and merges). Enable on a deployed node with `SHARD_LOAD=true` (`config/runtime.exs`).

## Gates

A "gate" is a check that must pass *before* a commit lands — not after.

- **GitHub Actions CI is DISABLED** (repo-wide, `actions/permissions enabled=false`, 2026-07-16; the
  `.github/workflows/ci.yml` file stays but is inert). So there is **no server-side safety net** — the
  *local* gates below are the only enforcement. Never lean on CI to catch a break; run `mix precommit`
  yourself before every commit. Re-enable with `gh api -X PUT repos/cwisecarver/fathom/actions/permissions -F enabled=true`.
- **`mix precommit` is the commit gate** (defined in `mix.exs`): `compile --warnings-as-errors`, `deps.unlock --unused`, `format`, `test`. **Never commit if it fails.** Run it when you're done with all changes and fix everything it surfaces.
- **Migration gate.** A schema migration must not ship without: (a) a forward copy+transform test, (b) a revert-flip test, (c) a cross-version-tolerance check. A migration that can't be reverted by pointer-flip within the retention window, or that the running app can't tolerate mid-rollout, is not done.
- **Shard-isolation gate.** Any change to shard routing (`Fathom.ShardExecutor.shard_from_conn`, `Fathom.Shards`, shard-path construction, or the planned `Fathom.Directory` resolve) must have a test proving shard A never resolves to shard B. Treat a cross-tenant leak as a release blocker, not a finding.
- **NIF-contract guard (when `fathom_native` lands).** Elixir is dynamically typed, so a NIF signature change (arity, **return shape**, param types) silently breaks Elixir callers at *runtime* — the Rust compiler can't catch it. After any NIF signature change, run the integration tests that exercise it and `grep -rn "Fathom.Native.<fn>"` for every caller. Don't rely on the unit suite to catch a contract break.
- **Bench-then-commit gate** (built). Any change touching a hot path (shard routing/open, directory resolve, migration copy, the shard coordinator) goes through `scripts/commit_with_bench.sh -m "<msg>"`: it benches the working tree and refuses the commit on a ≥20% regression in any metric vs the parent's entry in `scripts/perf_history.jsonl` (override `PERF_REGRESS_BLOCK`). Pure docs/test/comment-only changes skip it — `git commit` directly with a `[skip-bench]` token, or `--skip`. See Benchmarking and `docs/benchmark-plan.md`.

## Principles

- **Simplicity first.** Minimal code touched. Find root causes, not workarounds. Prefer elegance for non-trivial changes; skip it for obvious fixes.
- **Fix bugs autonomously** — diagnose from logs/errors/tests, then resolve. No hand-holding.
- **Never hand-roll routing, namespace, or SQL string surgery.** Don't build shard paths / namespace names / directory keys by ad-hoc string concatenation scattered across the codebase — route every shard resolution through `Fathom.Shards` (and request → shard through `Fathom.ShardExecutor.shard_from_conn`; eventually `Fathom.Directory`) so isolation and cutover logic live in one place. Don't hand-roll SQL for the libSQL shards either — always bind with parameterized queries (`Fathom.Shard` passes args through to `exqlite`); never interpolate values into SQL (injection and quoting edge cases bite the same way Postgres's do).
- **Multi-tenant safety is non-negotiable.** Every shard query carries its `shard_id` explicitly through `Fathom.Shards`/`Fathom.Shard` — never an implicit/ambient "current shard," and never the Postgres `Fathom.Repo`. When in doubt about which shard a code path operates on, make it explicit.

## Architecture

### Current data path (built)

```
   libSQL client (django-libsql / ws, libsql-experimental / http)
                          │  Hrana, shard = Host subdomain
                          ▼
   Filo.Plug / Filo.Socket  (the Filo library — Hrana over HTTP + WebSocket)
                          │  Filo.Executor callback
                          ▼
   Fathom.ShardExecutor → Fathom.Shards.checkout/1 (find-or-start → file path)
                          ▼
   Fathom.Shard.Connection (one exqlite conn per stream) → SQLite file
                          ▲ pull on cold start / flush + drop on idle
   Fathom.Shard (coordinator: tracks conns, idle) ── Fathom.Shard.Storage
                                                       (Local | S3 via Req sigv4)
```

Request → shard is still **Host-based** (the shard id comes straight from the
request Host, not a directory lookup). The Postgres directory (`Fathom.Directory`)
now exists and records each access — buffered off the hot path by
`Fathom.Directory.Recorder` — and drives the migration/lifecycle machinery, but it
is not (yet) a routing resolve on the request path. A cross-node lease + epoch
fence (via `Fathom.Shard.Storage`) makes the open single-writer-safe; see the
control-plane / cluster bullets under Project.

### Target design (planned, see `docs/migration-plan.md`)

```
                Postgres  (Fathom.Repo — supervised)
                  directory: shard → namespace, schema_version,
                  live/retired, retain_until, migration_status
                          │  resolve (cached, PubSub-invalidated)
                          ▼
        ┌──────────── per request ────────────┐
        │  resolve shard → shard namespace │
        └──────────────────┬───────────────────┘
                           ▼
   libSQL/Turso shards     one DB per shard · S3 bottomless (planned)
                           ▲
                           │  blue/green migration (docs/migration-plan.md)
        create @vN from template → quiesce → copy+transform → validate
              → flip directory pointer → retire @vN-1 (retain_until)
```

The migration machinery in this diagram is **built** — see the Migration engine bullet under Project. The version stamp lives in **three places**: `django_migrations` in each shard (Django's own migration ledger — the truth), `PRAGMA user_version` (O(1) gate), `shards.schema_version` in Postgres (laggard queries without opening shards). Still aspirational: the **cached, PubSub-invalidated resolve on the request path** (routing is Host-based today).

---

# Framework guidelines (generated by `phx.new` usage-rules)

This is a web application written using the Phoenix web framework.

## Project guidelines

- Use `mix precommit` alias when you are done with all changes and fix any pending issues
- Use the already included and available `:req` (`Req`) library for HTTP requests, **avoid** `:httpoison`, `:tesla`, and `:httpc`. Req is included by default and is the preferred HTTP client for Phoenix apps

### Phoenix v1.8 guidelines

- **Always** begin your LiveView templates with `<Layouts.app flash={@flash} ...>` which wraps all inner content
- The `MyAppWeb.Layouts` module is aliased in the `my_app_web.ex` file, so you can use it without needing to alias it again
- Anytime you run into errors with no `current_scope` assign:
  - You failed to follow the Authenticated Routes guidelines, or you failed to pass `current_scope` to `<Layouts.app>`
  - **Always** fix the `current_scope` error by moving your routes to the proper `live_session` and ensure you pass `current_scope` as needed
- Phoenix v1.8 moved the `<.flash_group>` component to the `Layouts` module. You are **forbidden** from calling `<.flash_group>` outside of the `layouts.ex` module
- Out of the box, `core_components.ex` imports an `<.icon name="hero-x-mark" class="w-5 h-5"/>` component for for hero icons. **Always** use the `<.icon>` component for icons, **never** use `Heroicons` modules or similar
- **Always** use the imported `<.input>` component for form inputs from `core_components.ex` when available. `<.input>` is imported and using it will save steps and prevent errors
- If you override the default input classes (`<.input class="myclass px-2 py-1 rounded-lg">)`) class with your own values, no default classes are inherited, so your
custom classes must fully style the input

### JS and CSS guidelines

- **Use Tailwind CSS classes and custom CSS rules** to create polished, responsive, and visually stunning interfaces.
- Tailwindcss v4 **no longer needs a tailwind.config.js** and uses a new import syntax in `app.css`:

      @import "tailwindcss" source(none);
      @source "../css";
      @source "../js";
      @source "../../lib/my_app_web";

- **Always use and maintain this import syntax** in the app.css file for projects generated with `phx.new`
- **Never** use `@apply` when writing raw css
- **Always** manually write your own tailwind-based components instead of using daisyUI for a unique, world-class design
- Out of the box **only the app.js and app.css bundles are supported**
  - You cannot reference an external vendor'd script `src` or link `href` in the layouts
  - You must import the vendor deps into app.js and app.css to use them
  - **Never write inline <script>custom js</script> tags within templates**

### UI/UX & design guidelines

- **Produce world-class UI designs** with a focus on usability, aesthetics, and modern design principles
- Implement **subtle micro-interactions** (e.g., button hover effects, and smooth transitions)
- Ensure **clean typography, spacing, and layout balance** for a refined, premium look
- Focus on **delightful details** like hover effects, loading states, and smooth page transitions


<!-- usage-rules-start -->

<!-- phoenix:elixir-start -->
## Elixir guidelines

- Elixir lists **do not support index based access via the access syntax**

  **Never do this (invalid)**:

      i = 0
      mylist = ["blue", "green"]
      mylist[i]

  Instead, **always** use `Enum.at`, pattern matching, or `List` for index based list access, ie:

      i = 0
      mylist = ["blue", "green"]
      Enum.at(mylist, i)

- Elixir variables are immutable, but can be rebound, so for block expressions like `if`, `case`, `cond`, etc
  you *must* bind the result of the expression to a variable if you want to use it and you CANNOT rebind the result inside the expression, ie:

      # INVALID: we are rebinding inside the `if` and the result never gets assigned
      if connected?(socket) do
        socket = assign(socket, :val, val)
      end

      # VALID: we rebind the result of the `if` to a new variable
      socket =
        if connected?(socket) do
          assign(socket, :val, val)
        end

- **Never** nest multiple modules in the same file as it can cause cyclic dependencies and compilation errors
- **Never** use map access syntax (`changeset[:field]`) on structs as they do not implement the Access behaviour by default. For regular structs, you **must** access the fields directly, such as `my_struct.field` or use higher level APIs that are available on the struct if they exist, `Ecto.Changeset.get_field/2` for changesets
- Elixir's standard library has everything necessary for date and time manipulation. Familiarize yourself with the common `Time`, `Date`, `DateTime`, and `Calendar` interfaces by accessing their documentation as necessary. **Never** install additional dependencies unless asked or for date/time parsing (which you can use the `date_time_parser` package)
- Don't use `String.to_atom/1` on user input (memory leak risk)
- Predicate function names should not start with `is_` and should end in a question mark. Names like `is_thing` should be reserved for guards
- Elixir's builtin OTP primitives like `DynamicSupervisor` and `Registry`, require names in the child spec, such as `{DynamicSupervisor, name: MyApp.MyDynamicSup}`, then you can use `DynamicSupervisor.start_child(MyApp.MyDynamicSup, child_spec)`
- Use `Task.async_stream(collection, callback, options)` for concurrent enumeration with back-pressure. The majority of times you will want to pass `timeout: :infinity` as option

## Mix guidelines

- Read the docs and options before using tasks (by using `mix help task_name`)
- To debug test failures, run tests in a specific file with `mix test test/my_test.exs` or run all previously failed tests with `mix test --failed`
- `mix deps.clean --all` is **almost never needed**. **Avoid** using it unless you have good reason

## Test guidelines

- **Always use `start_supervised!/1`** to start processes in tests as it guarantees cleanup between tests
- **Avoid** `Process.sleep/1` and `Process.alive?/1` in tests
  - Instead of sleeping to wait for a process to finish, **always** use `Process.monitor/1` and assert on the DOWN message:

      ref = Process.monitor(pid)
      assert_receive {:DOWN, ^ref, :process, ^pid, :normal}

   - Instead of sleeping to synchronize before the next call, **always** use `_ = :sys.get_state/1` to ensure the process has handled prior messages
<!-- phoenix:elixir-end -->

<!-- phoenix:phoenix-start -->
## Phoenix guidelines

- Remember Phoenix router `scope` blocks include an optional alias which is prefixed for all routes within the scope. **Always** be mindful of this when creating routes within a scope to avoid duplicate module prefixes.

- You **never** need to create your own `alias` for route definitions! The `scope` provides the alias, ie:

      scope "/admin", AppWeb.Admin do
        pipe_through :browser

        live "/users", UserLive, :index
      end

  the UserLive route would point to the `AppWeb.Admin.UserLive` module

- `Phoenix.View` no longer is needed or included with Phoenix, don't use it
<!-- phoenix:phoenix-end -->

<!-- phoenix:ecto-start -->
## Ecto Guidelines

- **Always** preload Ecto associations in queries when they'll be accessed in templates, ie a message that needs to reference the `message.user.email`
- Remember `import Ecto.Query` and other supporting modules when you write `seeds.exs`
- `Ecto.Schema` fields always use the `:string` type, even for `:text`, columns, ie: `field :name, :string`
- `Ecto.Changeset.validate_number/2` **DOES NOT SUPPORT the `:allow_nil` option**. By default, Ecto validations only run if a change for the given field exists and the change value is not nil, so such as option is never needed
- You **must** use `Ecto.Changeset.get_field(changeset, :field)` to access changeset fields
- Fields which are set programatically, such as `user_id`, must not be listed in `cast` calls or similar for security purposes. Instead they must be explicitly set when creating the struct
- **Always** invoke `mix ecto.gen.migration migration_name_using_underscores` when generating migration files, so the correct timestamp and conventions are applied
<!-- phoenix:ecto-end -->

<!-- phoenix:html-start -->
## Phoenix HTML guidelines

- Phoenix templates **always** use `~H` or .html.heex files (known as HEEx), **never** use `~E`
- **Always** use the imported `Phoenix.Component.form/1` and `Phoenix.Component.inputs_for/1` function to build forms. **Never** use `Phoenix.HTML.form_for` or `Phoenix.HTML.inputs_for` as they are outdated
- When building forms **always** use the already imported `Phoenix.Component.to_form/2` (`assign(socket, form: to_form(...))` and `<.form for={@form} id="msg-form">`), then access those forms in the template via `@form[:field]`
- **Always** add unique DOM IDs to key elements (like forms, buttons, etc) when writing templates, these IDs can later be used in tests (`<.form for={@form} id="product-form">`)
- For "app wide" template imports, you can import/alias into the `my_app_web.ex`'s `html_helpers` block, so they will be available to all LiveViews, LiveComponent's, and all modules that do `use MyAppWeb, :html` (replace "my_app" by the actual app name)

- Elixir supports `if/else` but **does NOT support `if/else if` or `if/elsif`**. **Never use `else if` or `elseif` in Elixir**, **always** use `cond` or `case` for multiple conditionals.

  **Never do this (invalid)**:

      <%= if condition do %>
        ...
      <% else if other_condition %>
        ...
      <% end %>

  Instead **always** do this:

      <%= cond do %>
        <% condition -> %>
          ...
        <% condition2 -> %>
          ...
        <% true -> %>
          ...
      <% end %>

- HEEx require special tag annotation if you want to insert literal curly's like `{` or `}`. If you want to show a textual code snippet on the page in a `<pre>` or `<code>` block you *must* annotate the parent tag with `phx-no-curly-interpolation`:

      <code phx-no-curly-interpolation>
        let obj = {key: "val"}
      </code>

  Within `phx-no-curly-interpolation` annotated tags, you can use `{` and `}` without escaping them, and dynamic Elixir expressions can still be used with `<%= ... %>` syntax

- HEEx class attrs support lists, but you must **always** use list `[...]` syntax. You can use the class list syntax to conditionally add classes, **always do this for multiple class values**:

      <a class={[
        "px-2 text-white",
        @some_flag && "py-5",
        if(@other_condition, do: "border-red-500", else: "border-blue-100"),
        ...
      ]}>Text</a>

  and **always** wrap `if`'s inside `{...}` expressions with parens, like done above (`if(@other_condition, do: "...", else: "...")`)

  and **never** do this, since it's invalid (note the missing `[` and `]`):

      <a class={
        "px-2 text-white",
        @some_flag && "py-5"
      }> ...
      => Raises compile syntax error on invalid HEEx attr syntax

- **Never** use `<% Enum.each %>` or non-for comprehensions for generating template content, instead **always** use `<%= for item <- @collection do %>`
- HEEx HTML comments use `<%!-- comment --%>`. **Always** use the HEEx HTML comment syntax for template comments (`<%!-- comment --%>`)
- HEEx allows interpolation via `{...}` and `<%= ... %>`, but the `<%= %>` **only** works within tag bodies. **Always** use the `{...}` syntax for interpolation within tag attributes, and for interpolation of values within tag bodies. **Always** interpolate block constructs (if, cond, case, for) within tag bodies using `<%= ... %>`.

  **Always** do this:

      <div id={@id}>
        {@my_assign}
        <%= if @some_block_condition do %>
          {@another_assign}
        <% end %>
      </div>

  and **Never** do this – the program will terminate with a syntax error:

      <%!-- THIS IS INVALID NEVER EVER DO THIS --%>
      <div id="<%= @invalid_interpolation %>">
        {if @invalid_block_construct do}
        {end}
      </div>
<!-- phoenix:html-end -->

<!-- phoenix:liveview-start -->
## Phoenix LiveView guidelines

- **Never** use the deprecated `live_redirect` and `live_patch` functions, instead **always** use the `<.link navigate={href}>` and  `<.link patch={href}>` in templates, and `push_navigate` and `push_patch` functions LiveViews
- **Avoid LiveComponent's** unless you have a strong, specific need for them
- LiveViews should be named like `AppWeb.WeatherLive`, with a `Live` suffix. When you go to add LiveView routes to the router, the default `:browser` scope is **already aliased** with the `AppWeb` module, so you can just do `live "/weather", WeatherLive`

### LiveView streams

- **Always** use LiveView streams for collections for assigning regular lists to avoid memory ballooning and runtime termination with the following operations:
  - basic append of N items - `stream(socket, :messages, [new_msg])`
  - resetting stream with new items - `stream(socket, :messages, [new_msg], reset: true)` (e.g. for filtering items)
  - prepend to stream - `stream(socket, :messages, [new_msg], at: -1)`
  - deleting items - `stream_delete(socket, :messages, msg)`

- When using the `stream/3` interfaces in the LiveView, the LiveView template must 1) always set `phx-update="stream"` on the parent element, with a DOM id on the parent element like `id="messages"` and 2) consume the `@streams.stream_name` collection and use the id as the DOM id for each child. For a call like `stream(socket, :messages, [new_msg])` in the LiveView, the template would be:

      <div id="messages" phx-update="stream">
        <div :for={{id, msg} <- @streams.messages} id={id}>
          {msg.text}
        </div>
      </div>

- LiveView streams are *not* enumerable, so you cannot use `Enum.filter/2` or `Enum.reject/2` on them. Instead, if you want to filter, prune, or refresh a list of items on the UI, you **must refetch the data and re-stream the entire stream collection, passing reset: true**:

      def handle_event("filter", %{"filter" => filter}, socket) do
        # re-fetch the messages based on the filter
        messages = list_messages(filter)

        {:noreply,
         socket
         |> assign(:messages_empty?, messages == [])
         # reset the stream with the new messages
         |> stream(:messages, messages, reset: true)}
      end

- LiveView streams *do not support counting or empty states*. If you need to display a count, you must track it using a separate assign. For empty states, you can use Tailwind classes:

      <div id="tasks" phx-update="stream">
        <div class="hidden only:block">No tasks yet</div>
        <div :for={{id, task} <- @stream.tasks} id={id}>
          {task.name}
        </div>
      </div>

  The above only works if the empty state is the only HTML block alongside the stream for-comprehension.

- When updating an assign that should change content inside any streamed item(s), you MUST re-stream the items
  along with the updated assign:

      def handle_event("edit_message", %{"message_id" => message_id}, socket) do
        message = Chat.get_message!(message_id)
        edit_form = to_form(Chat.change_message(message, %{content: message.content}))

        # re-insert message so @editing_message_id toggle logic takes effect for that stream item
        {:noreply,
         socket
         |> stream_insert(:messages, message)
         |> assign(:editing_message_id, String.to_integer(message_id))
         |> assign(:edit_form, edit_form)}
      end

  And in the template:

      <div id="messages" phx-update="stream">
        <div :for={{id, message} <- @streams.messages} id={id} class="flex group">
          {message.username}
          <%= if @editing_message_id == message.id do %>
            <%!-- Edit mode --%>
            <.form for={@edit_form} id="edit-form-#{message.id}" phx-submit="save_edit">
              ...
            </.form>
          <% end %>
        </div>
      </div>

- **Never** use the deprecated `phx-update="append"` or `phx-update="prepend"` for collections

### LiveView JavaScript interop

- Remember anytime you use `phx-hook="MyHook"` and that JS hook manages its own DOM, you **must** also set the `phx-update="ignore"` attribute
- **Always** provide an unique DOM id alongside `phx-hook` otherwise a compiler error will be raised

LiveView hooks come in two flavors, 1) colocated js hooks for "inline" scripts defined inside HEEx,
and 2) external `phx-hook` annotations where JavaScript object literals are defined and passed to the `LiveSocket` constructor.

#### Inline colocated js hooks

**Never** write raw embedded `<script>` tags in heex as they are incompatible with LiveView.
Instead, **always use a colocated js hook script tag (`:type={Phoenix.LiveView.ColocatedHook}`)
when writing scripts inside the template**:

    <input type="text" name="user[phone_number]" id="user-phone-number" phx-hook=".PhoneNumber" />
    <script :type={Phoenix.LiveView.ColocatedHook} name=".PhoneNumber">
      export default {
        mounted() {
          this.el.addEventListener("input", e => {
            let match = this.el.value.replace(/\D/g, "").match(/^(\d{3})(\d{3})(\d{4})$/)
            if(match) {
              this.el.value = `${match[1]}-${match[2]}-${match[3]}`
            }
          })
        }
      }
    </script>

- colocated hooks are automatically integrated into the app.js bundle
- colocated hooks names **MUST ALWAYS** start with a `.` prefix, i.e. `.PhoneNumber`

#### External phx-hook

External JS hooks (`<div id="myhook" phx-hook="MyHook">`) must be placed in `assets/js/` and passed to the
LiveSocket constructor:

    const MyHook = {
      mounted() { ... }
    }
    let liveSocket = new LiveSocket("/live", Socket, {
      hooks: { MyHook }
    });

#### Pushing events between client and server

Use LiveView's `push_event/3` when you need to push events/data to the client for a phx-hook to handle.
**Always** return or rebind the socket on `push_event/3` when pushing events:

    # re-bind socket so we maintain event state to be pushed
    socket = push_event(socket, "my_event", %{...})

    # or return the modified socket directly:
    def handle_event("some_event", _, socket) do
      {:noreply, push_event(socket, "my_event", %{...})}
    end

Pushed events can then be picked up in a JS hook with `this.handleEvent`:

    mounted() {
      this.handleEvent("my_event", data => console.log("from server:", data));
    }

Clients can also push an event to the server and receive a reply with `this.pushEvent`:

    mounted() {
      this.el.addEventListener("click", e => {
        this.pushEvent("my_event", { one: 1 }, reply => console.log("got reply from server:", reply));
      })
    }

Where the server handled it via:

    def handle_event("my_event", %{"one" => 1}, socket) do
      {:reply, %{two: 2}, socket}
    end

### LiveView tests

- `Phoenix.LiveViewTest` module and `LazyHTML` (included) for making your assertions
- Form tests are driven by `Phoenix.LiveViewTest`'s `render_submit/2` and `render_change/2` functions
- Come up with a step-by-step test plan that splits major test cases into small, isolated files. You may start with simpler tests that verify content exists, gradually add interaction tests
- **Always reference the key element IDs you added in the LiveView templates in your tests** for `Phoenix.LiveViewTest` functions like `element/2`, `has_element/2`, selectors, etc
- **Never** tests again raw HTML, **always** use `element/2`, `has_element/2`, and similar: `assert has_element?(view, "#my-form")`
- Instead of relying on testing text content, which can change, favor testing for the presence of key elements
- Focus on testing outcomes rather than implementation details
- Be aware that `Phoenix.Component` functions like `<.form>` might produce different HTML than expected. Test against the output HTML structure, not your mental model of what you expect it to be
- When facing test failures with element selectors, add debug statements to print the actual HTML, but use `LazyHTML` selectors to limit the output, ie:

      html = render(view)
      document = LazyHTML.from_fragment(html)
      matches = LazyHTML.filter(document, "your-complex-selector")
      IO.inspect(matches, label: "Matches")

### Form handling

#### Creating a form from params

If you want to create a form based on `handle_event` params:

    def handle_event("submitted", params, socket) do
      {:noreply, assign(socket, form: to_form(params))}
    end

When you pass a map to `to_form/1`, it assumes said map contains the form params, which are expected to have string keys.

You can also specify a name to nest the params:

    def handle_event("submitted", %{"user" => user_params}, socket) do
      {:noreply, assign(socket, form: to_form(user_params, as: :user))}
    end

#### Creating a form from changesets

When using changesets, the underlying data, form params, and errors are retrieved from it. The `:as` option is automatically computed too. E.g. if you have a user schema:

    defmodule MyApp.Users.User do
      use Ecto.Schema
      ...
    end

And then you create a changeset that you pass to `to_form`:

    %MyApp.Users.User{}
    |> Ecto.Changeset.change()
    |> to_form()

Once the form is submitted, the params will be available under `%{"user" => user_params}`.

In the template, the form form assign can be passed to the `<.form>` function component:

    <.form for={@form} id="todo-form" phx-change="validate" phx-submit="save">
      <.input field={@form[:field]} type="text" />
    </.form>

Always give the form an explicit, unique DOM ID, like `id="todo-form"`.

#### Avoiding form errors

**Always** use a form assigned via `to_form/2` in the LiveView, and the `<.input>` component in the template. In the template **always access forms this**:

    <%!-- ALWAYS do this (valid) --%>
    <.form for={@form} id="my-form">
      <.input field={@form[:field]} type="text" />
    </.form>

And **never** do this:

    <%!-- NEVER do this (invalid) --%>
    <.form for={@changeset} id="my-form">
      <.input field={@changeset[:field]} type="text" />
    </.form>

- You are FORBIDDEN from accessing the changeset in the template as it will cause errors
- **Never** use `<.form let={f} ...>` in the template, instead **always use `<.form for={@form} ...>`**, then drive all form references from the form assign as in `@form[:field]`. The UI should **always** be driven by a `to_form/2` assigned in the LiveView module that is derived from a changeset
<!-- phoenix:liveview-end -->

<!-- usage-rules-end -->