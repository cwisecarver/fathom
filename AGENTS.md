# AGENTS.md — Fathom

## Project

Fathom is a multi-tenant sharded data platform built on Phoenix: one SQLite database per shard (eventually millions), served to unchanged libSQL clients (e.g. an unchanged Django app via `django-libsql`) over the network.

**Docs index:** [`docs/README.md`](docs/README.md) maps the per-subsystem how-it-works stories (data path, admission, durability, single-writer, warm-standby, rebalancing, migration, auth, directory), the design plans, the benchmark plans, the runbooks, and the run reports.

**What exists today (the working slice):**

- **Shard data path.** One **connection per Hrana stream** (`Fathom.Shard.Connection`, an `exqlite` connection to the shard file, WAL + busy_timeout), opened at stream start and closed at stream end, so transactions are isolated. A stream also **monitors its coordinator** (Filo's `Executor.owner/1` seam) and tears down if it dies — an orphan writer's WAL frames would be lost by a successor's flush-and-drop (finding #8). `Fathom.Shard` is the per-shard **coordinator** (one GenServer per active shard) owning the file lifecycle: on cold start it **pulls** the file from `Fathom.Shard.Storage`; it tracks checked-out connections (monitoring the stream processes); when idle with zero connections it **checkpoints + flushes** the file back to storage, drops the local copy, and stops — so a flush never races a write, and the local copy is bottomless-backed. All flushes are **write-gated by a `dirty` flag** (set by `ShardExecutor` on a write): a clean read-only/idle shard skips the upload, so durability PUTs track writes, not open-shard count. `Fathom.Shards` is the find-or-start router (`Fathom.ShardRegistry` + `Fathom.ShardSupervisor`); `Fathom.Shards.checkout/1` resolves a `shard_id` to its coordinator, starting it on demand, and returns `{:ok, pid, ref, path}`. **The shard engine is exqlite** (SQLite via `Exqlite.Sqlite3`, wrapped by `Fathom.Shard.Connection`) — decided 2026-07-06: the `Ecto`/`ecto_libsql` path was evaluated and **removed** (the data plane is a SQL proxy over opaque client SQL, which gains nothing from Ecto's schema layer and pays per-query overhead + a Rust NIF for it). `Fathom.Shard.Connection` is the single swap-point if a client ever needs the libSQL *engine* for compatibility. Rationale in `docs/cluster-architecture.md`. **Per-query resource bounds (review #26, all config-gated, off by default)** protect co-located tenants from one runaway query: `:query_timeout_ms` (a watchdog `Sqlite3.interrupt`s the connection at the deadline → 503 `FILO_QUERY_TIMEOUT`), `:query_max_rows` (`Connection.collect/2` errors past the cap instead of materializing an unbounded result → 400 `FILO_RESULT_TOO_LARGE`), and `:max_checkouts_per_shard` (the coordinator refuses a checkout past the cap → 503 `FILO_SHARD_BUSY`, so one tenant can't monopolize a node's streams or wedge the shard un-drainable). **How it works end to end** — the request→stream→connection→coordinator lifecycle, the orphan-writer monitor, and the idle/dirty flush — is `docs/data-path.md`; the durability/RPO contract (WAL + `synchronous=FULL` + the write-gated periodic flush) is `docs/durability.md`.
- **Shard storage.** `Fathom.Shard.Storage` is a `pull/2` + `flush/2` behaviour, backend chosen by `config :fathom, :shard_storage` — `Fathom.Shard.Storage.Local` (a filesystem object store, the default for dev/test) or `Fathom.Shard.Storage.S3` (Req + `aws_sigv4`, no AWS dep; works with S3 / R2 / Tigris / MinIO). A present local file is authoritative on wake (pull only on cold start), so an un-flushed shard is never clobbered.
- **Network protocol.** `Fathom.ShardExecutor` (a `Filo.Executor`) binds each Hrana stream to a shard. The **Filo** library (separate repo, `{:filo, path: "../filo"}`) speaks the libSQL Hrana wire protocol — HTTP v1/v2/v3 (+ cursor) and WebSocket hrana1/2/3 — on its own Bandit listener (`:hrana_port`, default 8080; gated by `:hrana_server`, off in test), separate from the web/dashboard endpoint on 4000. `django-libsql` (WebSocket) and `libsql-experimental`/SDKs (HTTP) both work end to end.
- **Shard selection + admission.** The shard comes from the request's **Host subdomain** (`acme.fathom.example` → `acme`; case-normalized via `Fathom.ShardId.cast` so `ACME`/`acme` are one shard — finding #19). `?db=` / `x-fathom-shard` are **dev-only** fallbacks gated by `:allow_shard_override` (off in prod — finding #4). When nothing resolves, the fallback is `:default_shard` — **unset in prod ⇒ fail closed with a 400**, not commingling into a shared shard (finding #26); dev/test set it to `"demo"`. **Novel-shard admission is double-gated** (finding #14): `:max_open_shards` caps how many shards a node holds open, and `Fathom.Shards.NovelLimiter` (`:novel_shard_rate`, off by default; `NOVEL_SHARD_RATE` in prod) rate-limits how fast *unseen* ids can mint new shards (429 over budget; existing shards — directory row, local file, or running coordinator — are never limited, and the directory check fails open on a Postgres outage). The cap is a **soft cap by default**: at the limit, a new open **evicts the least-recently-used *idle* shard** (flush + drop + release lease — `Fathom.Shards.Lru` + `drain/2`, `[:fathom,:shards,:evicted]` telemetry) rather than 503ing, since a shard's LB home is one node so a refused open means that tenant is down, while an idle shard is bottomless-backed and just cold-re-opens. A **busy** shard (checked-out connections) is never evicted; if the LRU-coldest handful are all busy the node is genuinely saturated and returns 503 (`[:fathom,:shards,:at_capacity]`). Set `:evict_idle_at_capacity` false for a hard cap (503 at the limit). Either way the open count stays bounded by the cap (the fd-cliff protection). **Template capture** (`:template_shard_id`, off in prod by default) records a shard's SQL for fleet-wide replay, so it's a poisoning vector if reachable anonymously: never set a prod template shard without auth on it, and never make `:default_shard` equal it — `Fathom.Application.check_template_default!` refuses that config at prod boot (finding #17). **How it works end to end** — the fail-closed Host routing, the double gate (soft `:max_open_shards` cap + LRU idle-eviction, the novel-shard limiter), and the template-poisoning boot guards — is `docs/admission.md`.
- **Auth.** `Fathom.HranaAuth`, gated by `:hrana_auth` (`:disabled` default; `HRANA_AUTH=required` in prod; unknown values fail closed to required; a boot guard refuses `:required` without a usable secret). A per-shard `Phoenix.Token` (signed with `secret_key_base`, minted via `mix fathom.token <shard>`) is presented as libSQL's `authToken` — an `Authorization: Bearer` header over HTTP, the Hrana `hello`'s `jwt` field over WebSocket (ws clients send no upgrade header, which is why this is Filo's `:authorize` callback seam, not a pre-plug). With auth `:disabled` the trust boundary is the network — the port must be reachable only via the LB (firewall/SG/private subnet; pin the interface with `HRANA_BIND_IP`). See `docs/deploy-cluster.md`; **how it works end to end** — the two modes, the per-shard token on Filo's `:authorize` seam, the no-oracle posture, and the network trust boundary — is `docs/auth.md`. **Token lifecycle (review #24)**: **zero-downtime rotation** — `HranaAuth.rotate/1` raises the shard's `token_version` (stamping `token_version_bumped_at`) and mints a new token while `verify` keeps accepting the previous version for `:hrana_rotation_grace_ms` (default 1h); `revoke/1` (bumped_at cleared) stays immediate. **Read-only scope** — `token_for(id, scope: :ro)` embeds an `"sc"` claim; `authorize/2` stashes the scope and `Fathom.ShardExecutor` rides it in the connection handle, refusing any write on a `ro` token with **403 `FILO_READONLY`** (no Filo change — the scope is carried per-stream). A **boot warning** fires when `:required` runs with an infinite `max_age`. Mint/rotate/revoke are on the `/api/tenants/:id/token` control-plane API (#21). **Issuance ledger + fleet-wide revocation (review #37):** `hrana_token_issuances` records one row per mint (shard, `token_version`, scope, actor, `minted_at`) via `Fathom.HranaAuth.Ledger` — **claims only, never the secret**, since a Hrana token is verified by signature rather than lookup. Append-only: a revoke moves the floor, so `outstanding/1` reinterprets history against it while `history/1` keeps the audit trail. Recording is best-effort and **never fails a mint** (`mix fathom.token` has no Repo), so the ledger under-reports rather than over-reports — the safe direction for `HranaAuth.revoke_issued_before(cutoff, opts)`, the time-scoped bulk revoke that bumps the floor on every shard with an outstanding pre-cutoff token, paced per-shard through Oban's `:tokens` queue (`RevokeJob`) and **idempotent** so a re-run is a no-op rather than a second round of disconnects. Before this the only fleet-wide lever was rotating `secret_key_base` — an outage, not a revocation. Also: `:hrana_token_max_age` unset under `:required` now **REFUSES to boot in prod** (was a warning) — rotation is zero-downtime, so immortal tokens no longer buy anything.
- **Postgres — `Fathom.Repo`.** The orchestration store and web UI backend, in the supervision tree (Phoenix dashboard on port 4000). It also backs the shard **directory / control plane** below; migrations live in `priv/repo/migrations/` (`shards` directory, `shard_migrations`, Oban jobs).
- **Directory / control plane.** `Fathom.Directory` (+ the `Fathom.Directory.Shard` Ecto schema) is the Postgres record of each shard's `schema_version`, lifecycle `status` (`active`/`migrating`/`retired`/`migration_failed`/`deleted`/`suspended`), `last_active_at`, and `retain_until`. It's the source of truth the rollout/migration machinery reads and flips (`resolve`, `cutover`, `retire`, `laggards`). It's decoupled from the data path: per-checkout accesses are **buffered + batch-flushed off the hot path** by `Fathom.Directory.Recorder` (a coalescing ETS buffer; a Postgres outage drops a flush, never a checkout). It also persists **`last_flushed_at`** (review #28) via a parallel Recorder buffer hooked at each successful upload, so a post-node-loss report survives the node: `Fathom.Directory.flush_lag_report/1` (and `mix fathom.shard loss-report`) list the shards active-since-their-last-flush with per-tenant loss windows (the node-local `FlushWatermark` ETS dies with the node). **How it works end to end** — the recorder's lock-free ETS coalesce + batch-flush + outage-safe re-buffer, and what the directory holds for the migration/rebalancing/warm-standby readers — is `docs/directory.md`.
- **Migration engine.** `Fathom.Migrator` runs blue/green per-shard schema migration: `Migrator.Capture` records template migrations into fleet versions, `Migrator.Copy` + `Migrator.ShardMigration` do the copy+transform and stamp `user_version`, `Migrator.Release` releases a fleet HEAD, and Oban jobs drive it — `ShardMigrationJob` (unique per shard), `ReconcileJob` (hourly cron sweep so the cold tail converges), `RetirementJob` (drop expired retained versions), `RevertJob` (fleet pointer-flip back; a revert backs up the live version first, and a **write-age force-guard** refuses a shard the directory shows active since its `cutover_at` — the job cancels, deterministically — unless `force: true` confirms discarding the post-cutover writes). There is **no `Fathom.ShardExec` or `Fathom.Retirement` module** — that work lives in `Migrator.Copy`/`ShardMigration` and `Migrator.RetirementJob`; don't grep for the old names. **Capture strips Django's own introspection before recording (2026-08-14, `Capture.pure_read?/1`).** Its schema editor interleaves `SELECT … sqlite_master`, `SELECT QUOTE(?)…` and `PRAGMA foreign_key_check` with the DDL, so a captured version carried "what Django looked at" alongside "what the migration does", and **every tenant re-ran the lot, once per version in the chain**. `foreign_key_check` scans every FK'd table — free on the empty template, not on a tenant: replaying `0002_budget` measured **6 → 13 → 29 → 109 ms at 4 / 19 / 40 / 160 MB with the reads vs 4 → 7 → 17 → 57 ms without**, i.e. the reads are ~33–48% of the step and the gap grows with tenant size, to produce a result `Copy.replay_each/2` discards (so a violation it found was already being ignored). Proven inert before being acted on: replaying the real `django_migrate_capture.json` fixture with them removed yields a byte-identical `sqlite_master`, `user_version` and `django_migrations`. **The allowlist is the safety property, and it fails closed** — `SELECT` is dropped unconditionally (nothing in SQLite mutates through one), PRAGMAs only if the NAME is in `@read_only_pragmas` *and* there is no assignment, so `foreign_keys = OFF`, `legacy_alter_table = ON`, `optimize` and anything unrecognized all survive; `WITH` is deliberately not matched because `WITH … INSERT` is valid SQLite. Two traps worth keeping: the filter runs on the `{sql, args}` **pairs**, never the two parallel lists separately — filtering statements alone would shift the bookkeeping INSERT onto a dropped read's args, silently reviving the NULL-binding class of bug `beff929` fixed; and the assignment check was **dead code in the first draft** (it tested the whole body for `=` before extracting the name, so the allowlist had already rejected `"foreign_keys = OFF"` as a string) — found only because breaking it deliberately changed no test, which is the same "prove it discriminates in BOTH directions" habit the rest of this file records. **How it works end to end** — the three-place version stamp, the copy-then-flip, cold-tail convergence, the guarded revert, and cross-version tolerance during a mixed vN-1/vN rollout — is `docs/migration.md`.
- **Tenant lifecycle — delete + export (`Fathom.Tenants`, review #15).** The whole-shard operations the migration engine left out of scope: GDPR-erasure/offboarding **delete** and portability **export** (a tenant *is* one SQLite file). `Tenants.delete/1` tombstones the directory row (`deleted` status) and broadcasts the tombstone fleet-wide **synchronously**, then enqueues `Fathom.Tenants.DeleteJob` (queue `:tenants`, unique per shard) for the durable erase: cancel pending per-shard Oban jobs → **force-stop** the home coordinator (`Fathom.Shards.stop/1` terminates it *while its lease is valid* so shutdown flushes/releases cleanly and never self-fences — the fix for a real leak where a graceful drain couldn't stop a busy coordinator, which then quarantined the erased data to a `.fenced.<ts>` file) → `Storage.purge_shard/1` deletes **every** stored object (live `.db`, `.lock`, all `@<version>`, all `@snap-<id>`; exact id-delimiter match so purging `acme` never touches `acme2`) → sweep local files. Purging is safe even if another live node still holds the lease: every coordinator flush is fenced (`If-Match`), so the deleted live object makes that node self-fence on its next flush instead of resurrecting it. The **re-mint guard** is `Fathom.Tenants.Tombstones` — a public ETS set of deleted ids checked O(1) in admission (`start_if_capacity` first branch ⇒ `{:error, :shard_tombstoned}`, off the Postgres hot path), loaded from the directory at boot, pushed on delete over Oban's notifier (which also purges each node's warm-follower copy), and periodically refreshed — so a stray request for a deleted subdomain is refused, never re-minted as an empty shard. **Export** is `Tenants.export/1` (pull the durable object to a temp file) served by `GET /admin/tenants/:id/export` (BasicAuth `send_download`, temp deleted after — never a public URL); `AdminDirectoryLive` gets per-row Delete (with confirm) + Export actions, and `deleted` is excluded from hand-editable statuses. **No feature flag** — inert until an operator invokes it. Cross-node drain of a *busy* remote shard via the `rebalance_commands`/`command_poller` path is a follow-up (single-home + the fenced-flush self-fence cover correctness today). **Provisioning (review #21)** completes the control plane: `Tenants.provision/1` (explicit directory insert + fork-from-template birth when `:fork_from_template` is on + token mint, returning the `libsql://<id>.<base>` URL + `auth_token`) and `FathomWeb.Api.TenantController` — a JSON create/list/get/delete API under `/api`, behind the same admin BasicAuth on `:4000` (separate from the Hrana port), so a platform customer creates/lists/deletes tenants instead of relying on traffic-implied minting. **Suspend/resume (review #20)** is the administrative-offline lever short of deletion: a `suspended` status + `Fathom.Tenants.Suspensions` (the Tombstones ETS gate but reversible — add/remove notification + periodic reconcile). `Tenants.suspend/1` flips the directory, broadcasts so every node denies new streams, and graceful-drains the home coordinator; `resume/1` reverses it. **Both lifecycle denies live in `Fathom.Shards.ensure/1`** (checked on every checkout, O(1) ETS) so a deleted/suspended tenant is refused even with a running coordinator; a suspended open is a distinct **403 `FILO_TENANT_SUSPENDED`** and a deleted one a **410 `FILO_TENANT_DELETED`**. `POST /api/tenants/:id/{suspend,resume}` + admin Suspend/Resume buttons. **Fork + operator tooling (review #14)**: `mix fathom.shard pull|inspect|fork` (`inspect` = a per-shard restore drill: pull + `quick_check` + row counts) and **database forking** — `Storage.fork_shard/2` (one object copy) + `Tenants.fork/2` (clones a live tenant to a new id, registering the dst at the *source* schema version so the laggard sweep won't re-migrate the fork; non-disruptive to src) + `POST /api/tenants/:id/fork`. **How it works end to end** is `docs/tenant-lifecycle.md`.
- **Per-shard load counters (`Fathom.ShardLoad`) — the Phase-2 rebalancing (B) prerequisite.** A public ETS table of per-shard cumulative counters (checkouts, queries, `rows_read`/`rows_written` = query cost), bumped **lock-free from the executing process** (`:ets.update_counter`, `write_concurrency` — the `Directory.Recorder` pattern, no per-query GenServer hop) on `Fathom.Shards.checkout` and `Fathom.ShardExecutor.execute`; a stopped coordinator's row is dropped in `terminate` (`forget/1`). A control plane reads `top(n, by)` / `snapshot/0` to find hot shards (rates = diff two snapshots, churn-safe); the **first reader is `mix fathom.scale --hotspots`** (the §B evidence harness — see Benchmarking). Deliberately **not** a per-shard `Telemetry.Metrics` (a per-shard tag at millions of shards is cardinality death) — the read API is the interface. Gated by `:shard_load`, **off by default** (nothing in the request path consumes it until the rebalancer lands, so the hot path doesn't pay for an unread counter).
- **Dynamic rebalancing — Phase 2 B1 (`Fathom.Rebalancer.*`, built 2026-07-06).** Moves a persistently-hot shard off an overloaded node by layering a per-subdomain **exception table** on the LB's consistent hash. Since there's no BEAM cluster (LB-partition coordinates via S3 for data, Postgres for orchestration), each node **reports**: `Rebalancer.Reporter` (gated `:load_reporter`) diffs two `ShardLoad` snapshots into rates and writes its hot set to `shard_load_samples` (Postgres), tagged with a stable `Rebalancer.node_key/0`. The Oban cron `RebalanceJob` (fleet singleton via Oban's Postgres peer leadership; gated `:rebalancer_enabled`) reads the merged fleet view and runs `Rebalancer.Policy` — hot = **absolute q/s floor or `K×p99`, never `K×median`** (the `--hotspots` finding), with 2-window anti-flap, cooldown, an improvement guard (never relocate a lone hotspot), and least-loaded target — then enqueues a unique-per-shard `HandoffJob`. The handoff is **warm the target → pin the `shard_overrides` row + render the nginx map (`LbMap`) + reload (`LbApply`) → drain the source (release the lease) → target acquires**; a **flip-before-drain** ordering (the `{owner,epoch}` lease blocks any double-write regardless — a healthy node can't be *stolen* from, so the move is a voluntary drain, not the crash-path self-fence). Warm/drain reach a node the orchestrator can't RPC via a `rebalance_commands` Postgres channel + per-node `CommandPoller` (gated `:command_poller`). **Proven live on the chaos rig** (`chaos.sh rebalance`: `acme` @ 143.9 q/s moved fathom1→fathom2, isolation intact). All gates **off by default**; the enable-path runbook (staged gate-by-gate, with an observe-before-arming dry run + rollback) is `docs/runbooks/rebalancer.md`. Hardened per `docs/reviews/expert-review-2026-07-07-013852.md` and re-proven live in `docs/reviews/chaos-run-2026-07-08.md`. See `docs/phase2-scoping.md` §B1 for scoping; **how it works end to end** — detect (per-node reporting) → decide (the p99/floor hotness bar + guards) → execute (the flip-before-drain handoff) — is `docs/rebalancing.md`.
- **Cross-node single-writer (lease + epoch fence + node heartbeat).** `Fathom.Shard.Storage` carries a per-shard **lock** (`{owner, epoch}`, the monotonic `epoch` is the fencing token) and a per-node **heartbeat** (`Fathom.Shard.Heartbeat` renews one `heartbeat/<node>` object every `ttl/3` — liveness is O(nodes), not O(shards), which is the F1 fix). A shard's owner is live iff its heartbeat is fresh, so `acquire_lease` consults the owner's heartbeat to decide held-vs-steal (and fails closed on a heartbeat read error). Coordinators do **no per-shard renewal**; before a flush they fence via `Heartbeat.valid_for_write?/1` (heartbeat valid-with-margin + no lapse since acquire ⇒ write with no per-shard I/O; on a lapse, re-check the lock via `Storage.check_lease/2` and **self-fence** if superseded so a remapped shard never double-writes). If the heartbeat process is down, coordinators degrade to the legacy per-shard renew fence. This is the only cross-node coordination — via S3, not BEAM. **How it works end to end** — the storage behaviour, the lease/epoch/heartbeat primitives, cold-open acquire+pull, the steady-state fence, the crash-steal vs voluntary-drain directions, and the loss contract — is `docs/single-writer.md`.
- **Cluster layer (LB-keyspace-partition).** The L7 load balancer consistent-hashes the `Host` subdomain to one backend node; each node is an independent single-node fathom, and the S3 lease above handles remap safety. `Fathom.HealthPlug` serves `GET /health` (per-node LB probe, `:health_port` default 8081, gated by `:health_server`). `Fathom.Telemetry` runs Telemetry.Metrics over the shard/lease/checkout events + an active-shard poller + a checkout→OpenTelemetry span bridge (traces env-gated on `OTEL_EXPORTER_OTLP_ENDPOINT`, off by default). LB config in `deploy/lb/`; `docs/deploy-cluster.md` + runbook in `docs/runbooks/cluster.md`. The **chaos rig** (`deploy/chaos/` — 3 prod-release nodes behind nginx + MinIO + per-node toxiproxy, driven by `chaos.sh`: `failover`/`pause-fence`/`partition`/`soak`/`warm-home`/`hotspots`/`rebalance`/`density`/`served`/`served-data`/`latency-cost`/`tpc-fleet`/`rollout`/`replication`/`rpo`/`quorum-loss`) is the failover-time-and-loss-window layer the in-process cluster suite (`test/fathom/cluster/`) can't measure — and (`hotspots`, gated by `SHARD_LOAD=true`) the real-traffic hot-spot layer the in-process `--hotspots` harness can't: it drives Zipf-skewed real Hrana traffic through the LB and reads `Fathom.ShardLoad.top` per node via `bin/fathom rpc` (2026-07-06 run: 17.5k requests, top-20 Zipf-head recall 0.95, hot set spread across all 3 nodes — the per-node-read/merge model confirmed). **`rebalance`** demonstrates the full **B1 handoff live**: drive load on a shard, show the reporter detected it, then pin it + drain the source + prove it moved to the target — the **`lb-reloader` sidecar** (shares nginx's PID namespace, HUPs its master on a map change) applies the flip with no host bridge. Runs: a manual pin (2026-07-06, `acme` fathom1→fathom2) and a fully **autonomous** one (2026-07-07: `Policy.propose` chose `green: fathom2→fathom1` off an engineered imbalance, `RebalanceJob`/`HandoffJob` executed it), both isolation-intact. **`density`** is the fleet counterpart to the single-node `--ramp` ceiling: it mints N novel shards through the LB and reads per node the coordinators held (the keyspace partition) + BEAM/RSS per shard (the cost) — 2026-07-10 run, N=30000 over 3 nodes (~10k/node), spread evenly (1.22×) at ~16 KiB/shard RSS on every node (marginal cost *falls* with scale as fixed overhead amortizes), so capacity is horizontally additive and only the active working set is resident — this is the warm-resident floor (idle, empty shards). **`served`** measures the complementary **fd-bound** ceiling — shards held under a *live connection*: 30k across the fleet (10k/node) at ~220 KiB/shard and **1 fd each**, so served density is a `nofile` knob (compose raises the nodes' soft limit to 65536; the default 1024 caps ~940/node) not a memory or architectural wall. **`served-data`** adds real data per shard (30k shards seeded 256 KiB each, held + scanned): ~640 KiB/shard and **3 fds** (WAL-active `-wal`/`-shm`), and the key result — an *idle* data-bearing shard drops back to ~43 KiB/0 fds (the 256 KiB lives on disk, not RSS), so a *stored* tenant costs disk and only an *active* one costs the connection + page cache. All four regimes (idle floor ~16 KiB, served ~220 KiB, served+data ~640 KiB, idle+data ~43 KiB) measured at 30k across the fleet; served-data was stress-pushed to **105k held / ~150k RAM-bound** (nofile maxed) — the open-rate decay past ~30k/node is a one-process test artifact (production holds one process per stream), not a fathom limit (`docs/reviews/fleet-density-2026-07-10.md`). **`latency-cost`** MEASURES what `latency <ms>` INJECTS — it times the two S3-round-trip-bound paths (cold-open pull, flush upload) per node, baseline vs an injected one-way RTT (2026-07-23 sweep, 10/30/60 ms: cold-open ~26/70/133 ms ≈ **~1 RTT**, matching the in-process `benchmark_s3_sweep.sh` — the `Fathom.Shard.init` lease+pull overlap working on the fleet; flush/drain ~53/143/270 ms ≈ **~2.3× RTT**, down from ~3.5× in the 2026-07-11 sweep after perf review 2026-07-23 #6 removed the drain's redundant lock GET — the lease carries the lock etag from our own PUT responses, so release is one conditional DELETE. The remaining data-PUT→lock-DELETE ordering is irreducible (releasing before the flush lands would let a peer pull stale bytes), so drain now sits within ~0.3 RTT of the design floor — `docs/reviews/latency-cost-2026-07-23.md`; history in `latency-cost-2026-07-11.md`, whose sidecar-upload attribution of the third RTT was wrong — the sidecar is a local file). **`tpc-fleet`** is the throughput analog of `density`: it drives many tenant shards — one single-writer file each — through the LB so they partition across the nodes, then reads back the aggregate txn/s + the per-node load split (2026-07-23 runs, post the LB-502 fix, 6→1024-tenant sweep: **query load follows the shard partition** across all 3 nodes, **zero errors at every step** — 128 tenants (2× the concurrency that used to collapse the rig in a 502 storm) ran 19,200 txns with 0 driver errors and 0 LB upstream failures, and the consistent-hash spread *improves* with key count (1.27× at N=128, 1.06× at N=256) — aggregate **~890 → ~1,066 → ~1,083 → ~1,075 → ~1,082 → ~1,026 txn/s**, a clean CPU plateau on the one rig VM (sagging only ~5% under 256 concurrent held connections — no second ceiling in sight), within ~5% of the p50-implied fair-queueing ceiling at every step, with per-txn p50 6.6 → 11.0 → 20.9 → 57.4 → 113.8 → 239.8 ms growing linearly with concurrency at saturation across independent writers; at N=256 the consistent-hash spread is **1.06×** (85/83/88 — the ring evening out with key count); **1024 concurrent tenants = ~2,661 tps aggregate** (uniform-methodology sweep 2026-07-24: in-network driver containers, 4×256 clients; 153,008 txns, 0.4% transient client reconnects, zero server-side failures) — and aggregate RISES with driver count (~1,140 → 1,625 → 2,661 at 1/2/4 procs), so every single-driver 'plateau' was the DRIVER's ceiling, not the fleet's; the fleet ceiling on this VM is ≥2.7k tps and unfound. High-concurrency driving rules (colima's host forwarder dies ~1k conns — drive in-network; ≤128 clients per driver process — a solo 256-client in-network proc reproducibly collapses, unattributed, see the addendum + tasks/todo.md): `docs/reviews/tpc-fleet-highconc-2026-07-23.md` — but those driving rules are the *Python* driver's, not fathom's: the **Elixir/Filo.Client driver** (`deploy/chaos/tpc_driver.exs` — one lightweight BEAM process per client, no GIL), run as an **in-network container** (`chaos.sh build-driver`; then `TPC_NET=container TPC_DRIVER=elixir ./chaos.sh tpc-fleet`), drove the whole **6→4096-tenant sweep from ONE process, zero errors at every step** — no ≤128-clients/proc cap, no multi-process orchestration (one driver bug en route: a connect+seed thundering herd crashed it at ≥2048, fixed with a per-worker connect stagger + retry; the fleet processed every query at every attempt — ~3.4–4k tps CPU-bound on this VM, spread ≤1.15× out to 4096; a parameter-matched host A/B has the Elixir driver **1.6–2.8× faster** than Python in its clean range and error-free where one Python process collapses at ≥128 clients — `docs/reviews/tpc-fleet-elixir-driver-2026-07-24.md`; the Elixir driver also covers **tpcc** now (all five value-feeding txns + the W-sweep, tpmC parity with Python) plus a **`tpcc-fleet`** multi-tenant sweep (one 1-warehouse TPC-C per tenant, 16→4096 tenants error-free, aggregate tpmC ~25.6k-peak, even per-node partition ≤1.12×), which surfaced a **Filo.Client resilience fix**: a `:closed` connection — e.g. an LB recycling an idle client keep-alive after N requests — is transparently retried with the **same baton** (resume the Hrana stream, exactly-once via the baton seq) instead of abandoning a held stream mid-transaction), still **no intra-shard convoy** (contrast the single-shard TPC-C lock convoy); absolute txn/s is single-host-bound — the horizontal "millions" throughput axis is the even per-node split, not the one-box aggregate — `docs/reviews/tpc-fleet-2026-07-23.md`. **All pre-2026-07-23 tpc-fleet absolutes (e.g. the 07-11 run's ~40 tps) are obsolete**: they were ~95% LB-502 reconnect overhead from an nginx `Connection: close` upstream-keepalive bug, fixed in both LB configs; the old "moderate concurrency by design" rig caveat is retired with it — `docs/reviews/lb-502-fix-2026-07-23.md`). **EVERY capacity and throughput number above was measured with A2 replication OFF, and as of 2026-08-14 they do NOT hold with it on.** First measurement of the two together (both arms, one rig, one session, `tpc-fleet 16,64,256,1024`): replication off ran every step clean at **2,918 / 4,081 / 4,644 / 4,374 txn/s, 0 errors, spread 1.21×**; replication on cost **~25% throughput where it worked (2,164 / 3,041 / 3,505)** and **did not complete the 1024-tenant step at all** — it OOM-killed a node (`OOMKilled=true`, exit 137) on a 94 GiB VM with no container memory limit, and that step produced no result. One real cause was found and fixed (`e0fda94`: the shippers never full-swept, so forwarded WAL frames accumulated as off-heap refc binaries — **7–18 GiB/node → 83–99 MB**), and it was **not sufficient**: nodes still die at 1024, surviving 6.5 min instead of 2. So treat the numbers above as the **replication-off** ceiling and do not quote them for a replicating fleet. A hypothesis worth NOT re-chasing: the dying nodes emit a flood of `replication rejected` / `quorum IMPOSSIBLE` warnings and Logger queue growth looks obvious, but it was counted — 68,588 lines in 388 s, ~176/sec — far too little to OOM a BEAM. The unexamined suspect is **concurrent seeding**: `Shipper`'s moduledoc says chunking "bounds MEMORY on both sides", which is true *per seed* and says nothing about 1024 at once; `Follower` also lacks `fullsweep_after` and holds its sockets in unlinked Tasks, so the same class may exist on the receiving side (unmeasured). **`rollout`** is the fleet **schema-migration** throughput step (review 2026-08-01 #43's second half): seed N tenants at HEAD, release HEAD+1, run the real `Migrator.rollout/1` sweep, and read back shards/s + the `rate_per_hour`/`eta_seconds` the gate reports + the per-node split from the `[:fathom, :migrator, :shard_migrated]` telemetry event (whose contract — once per shard that MOVED, not once per attempt — the step asserts against the directory). 2026-08-04, 300 tenants over 3 nodes, two runs within 2%: **299 shards in ~23 s = ~12.8–13 shards/s ≈ 46,000 shards/hour**, so `:reconcile_batch_size`'s 100/hour default throttles **~460× below the engine** — the evidence base the finding asked for. Split is **Oban queue-draw, not LB hash** (migration jobs come from one Postgres queue), even at 1.07× for 300 jobs but legitimately lopsided for a handful. The run also found a **reproducible stuck-lease bug** (~1 in 300, different tenant each run): a coordinator lease that outlives its coordinator on a *live* node is never stealable (liveness is the node's heartbeat, and the migrator acquires under a different owner string), so the job snoozes forever in `scheduled` with **no error, no quarantine, `failed: 0`** — that tenant serves normally but can never migrate, and the deploy gate never converges. A client touch on the owning node clears it. **ROOT-CAUSED AND FIXED 2026-08-04, confirmed on the rig (4 runs at 299/300 before, 2 runs at 300/300 after).** The bug was in `release_lease` itself, not in any caller: the release is a conditional `DELETE … If-Match: <the etag we last wrote>` and a **412 was reported as `:ok`**, collapsing two opposite situations — "the lock is someone else's" (correct no-op, finding #22) and "the lock is STILL OURS at a different etag" (a leak reported as success). Our own etag rotates under us: `S3.acquire_existing/4` rewrites the lock on a same-owner reclaim (same epoch, new etag) and `renew_lease/3` rewrites it in legacy mode. The release now re-reads on a 412 and finishes the delete when the lock is still ours; the decision lives in `Fathom.Shard.Storage.resolve_stale_release/4` rather than inside a backend, because every etag-carrying backend must make it identically AND because the default suite's backend is `Fathom.Test.FaultyStorage` — a policy implemented privately per backend would only ever be tested against the double. **Why #9/#11 did not close it:** both fixed CALLERS that released with a stale lease, so any rotation whose result the caller never received still leaked. **Why it was heartbeat-mode-only:** legacy's `Fence.check` does a `renew_lease` PUT on the way into the drop and #9's fix threads that refreshed lease back, so legacy accidentally self-heals a rotation from any source; heartbeat mode does no renew and carries the stale etag straight into the release — which is exactly why the rig hit it every run and the legacy-only suite never could. Three OTHER real lock-leak paths were found and fixed on the way, none of which was this bug (each has a discriminating test; the rig reproduced the straggler again after each): `flush_then_drop/1`'s two "keep the local copy" branches (transient flush error; fence couldn't confirm ownership) kept the **lock** as well as the copy, and an **exception between `acquire_lease` and the built state** skipped the release entirely (the hazard `shard.ex`'s own comments describe — #33 had fixed one instance inside `fork_evidence/2`, but `resolve_fork`/`revalidate_takeover`/`promote_pull` still ran unrescued storage calls in the coordinator). Tests are in `test/fathom/shard_lease_release_test.exs` and assert the **foreign-owner** view — the same-node view cannot see this class at all, since a coordinator silently reclaims its own node's lock, which is why the symptom is so quiet. **The mode gap that hid it is closed too:** `shard_lease_release_test.exs` now generates every mode-agnostic leak scenario for BOTH modes and asserts the mode actually took, which immediately surfaced a second heartbeat-only leak (the ownership-unconfirmed branch). Also still open: the migration snooze is unbounded and silent, so any other cause of a long-held lease stays invisible. `docs/reviews/fleet-rollout-2026-08-04.md`. See the § Chaos testing two-layer split in `docs/deploy-cluster.md` and the runs in `docs/reviews/chaos-run-2026-07-05.md`. **The cluster phase (S1–S8) is complete** — LB partition, S3 lease/heartbeat fence, crash/partition contract, observability, directory-off-hot-path, and lease-RPS measurement all shipped (see the Status in `docs/deploy-cluster.md`); what remains cluster-side is Phase 2 (below).
- **Local-disk observability + warm-cache back-pressure (review 2026-08-01 #36).** Nothing in the metrics layer read the filesystem: `Fathom.Admin.Measurements` had four pollers and none touched disk, and `fathom.storage.bytes` is *S3* usage. `Measurements.disk/0` (on the existing 10 s poller) now emits `fathom.node.disk.{free_bytes,used_ratio}` tagged `dir=data|warm` for the two directories fathom writes to, via `:disksup` — `:os_mon` is in `extra_applications` **solely** for that (there is no pure-BEAM statvfs) and is configured down to it: `start_memsup/start_cpu_sup: false` and `disk_almost_full_threshold: 1.0`, because os_mon alarms **per mount** including mounts fathom has nothing to do with. A missing directory resolves to its nearest existing ancestor, so a freshly booted node — the state where headroom matters most — is not blind until its first shard opens. **Why disk is not an ordinary capacity gauge here:** a full volume fails every cold-open `pull` AND every dirty shard's `VACUUM INTO`, so writes keep being **acked** and can never be made durable (RPO unbounded), and the symptoms that surface are identical to an S3 credential/reachability problem — the diagnostic path pointed the wrong way. The warm-follower cache, the one component sized to fill disk, was bounded only by `:warm_cache_max`, a shard **count** (500 shards = 8 MB or 2 TB); it now also respects `:warm_disk_free_floor_bytes` (default 1 GiB, `WARM_DISK_FREE_FLOOR_BYTES`) and an opt-in `:warm_cache_max_bytes`. Under pressure it **stops warming new shards but keeps and revalidates what it already holds** — evicting would discard failover readiness already paid for without freeing the live data dir any faster — and emits `[:fathom, :warm_follower, :disk_pressure]`. The decision is a pure function (`WarmFollower.headroom?/4`) because ancestor resolution makes the fail-open `:error` branch unreachable from outside, and an untestable safety branch rots. Alerts in `deploy/observability/alert-rules.yml` (`FathomDiskFillingUp` 85% ticket, `FathomDiskCritical` 95% page, `FathomWarmCacheDiskPressure`).
- **Restore drill: the procedure, not just the object (review 2026-08-01 #48).** `verify/2` pulled, `quick_check`ed and compared `user_version` — proving the stored BYTES are readable, but never invoking `Tenants.fork/2`, never cold-opening a coordinator from those bytes, never touching the directory reconcile. So the chain an operator runs under maximum pressure had only ever executed in unit tests. `RestoreDrillJob.run_full_drill/1` (gated `:restore_drill_full_sample`, `RESTORE_DRILL_FULL_SAMPLE`, **off by default** and deliberately a much smaller sample than the read-only drill, because a fork is a full object copy) forks each sampled shard to a scratch tenant, **compares user-table row counts against the source** — a fork that produced an empty database would pass `quick_check` and prove nothing — and drops it. Distinct failure classes (`:fork_failed`, `:restored_mismatch`) because "the recovery path is broken" wants a different response from "this object is corrupt". Scratch tenants are **hard-deleted** (`Directory.hard_delete/1`), never `Tenants.delete/1`: that tombstones, and one tombstone per sample per run would grow the admission-path `Tombstones` ETS set without bound — the routine that proves recovery works would degrade the hot path. **Snapshots** also gained a health signal at all: they are storage objects with no directory row, so `sample_for_drill/1` never saw them and the one class of data a point-in-time recovery reaches for was the one class nothing checked; they are now verified alongside the shard that owns them (`[:fathom, :restore_drill, :snapshot_result]`). Separately, AGENTS.md's own migration-gate item (c) — cross-version tolerance — finally has a test (`test/fathom/cross_version_tolerance_test.exs`), driven over the real Hrana wire and labelled as the tripwire it is rather than a regression test.
- **Data migrations + `atomic = False` — the transform seam (review 2026-08-01 #26).** The engine replays the template's SQL verbatim, so a `RunPython` backfill crossed the wire as literal DML carrying **the template's row values**. `Capture` flagged that and capped HEAD, leaving exactly two options — approve it (replay the template's rows onto every tenant, the corruption the flag prevents) or never advance, with every later migration stacked behind it. `AddField` + `RunPython` is the most common two-step Django migration, so this was certain in month one. **Three parts, per the finding's own order.** (1) **Legibility:** `requires_review` was one boolean, so the status API could say `pending_review: [7]` and nothing more. New `review_reason` / `review_detail` columns + `Migrator.review_block/1` report WHY a version is held and spell out the options with their consequences — and `status/0` exposes them in a NEW `review_blocks` field rather than changing `pending_review`'s type, because `migration_controller_test` failing on `pending_review == [2]` was the published control-plane endpoint pointing out that a field changing type is a consumer break. Releases captured before this re-derive their reason from the statements, so an already-frozen fleet gets the explanation on upgrade. The **dashboard** half followed (2026-08-06): `Fleet.migrations/0` carries `review_blocks` and `/admin/migrations` gets a **Held for review** tile plus a *"Rollout held"* panel above the burndown — reason, the flagged statements, and both options with their runnable commands. **Deliberately read-only**, and not merely a smaller scope: `attach_transform` cannot be a button (module + deploy + allowlist), while `approve_review` is the fleet-wide corruption the flag exists to prevent, so a panel offering only the dangerous half of a two-way decision biases toward it — contrast `AdminDirectoryLive`'s Delete, which is destructive but per-tenant and unambiguous. `admin_live_test.exs` pins the absence of a click handler inside `#review-blocks` so the next person to add one reads the moduledoc first. (2) **The transform seam:** a release may carry a `transform` — a module implementing `Fathom.Migrator.Transform` (`run(conn, shard_id)`) that `Copy.migrate_chain/4` runs **inside the same transaction, after the DDL**, so a failing backfill rolls the DDL back with it rather than leaving a column full of NULLs that every version stamp agrees is fine. **The allowlist (`:migration_transforms`) is the security boundary**: a release row is data written by the capture path, whose template AGENTS.md already calls a fleet-wide poisoning vector, so resolution never calls `String.to_atom`/`to_existing_atom` — being a loaded module is not permission. Transforms are resolved BEFORE any file is copied, so an unregistered one fails before doing work. `attach_transform/2` refuses a release still carrying flagged DML (else the version runs both the literals AND the transform) and refuses a version held for a **gap** (a transform cannot conjure DDL the fleet missed). (3) **`atomic = False`:** capture cannot see it by construction — autocommit, no transaction to hook — so `mix fathom.check_migrations <path>` is the pre-flight CI gate (error on `atomic = False`, warn on `RunPython`/`RunSQL` naming the transform path), with the gap detector kept as the late backstop. Transactionality was verified to discriminate: moving the transform after `COMMIT` fails two tests. `docs/django-migrations.md` carries the operator walkthrough.
- **Scheduled snapshots + GFS retention (review 2026-08-01 #18).** `Fathom.Snapshots.create/2` was a correct, well-fenced primitive that **nothing ever called on a schedule**, and nothing ever expired a `@snap-<id>` object (`RetirementJob` drops `@<version>` only). Since the live durable object is overwritten every `:shard_flush_interval_ms` (default 5 s), the practical recovery capability for **logical** corruption — a bad deploy, a bad backfill, which `docs/durability.md` itself calls the more common incident — was **zero**: "restore tenant acme to 09:00" had no answer, the largest gap against the Postgres-on-RDS baseline fathom replaces. Two fleet-singleton Oban crons close it, **both off by default**: `Fathom.Snapshots.ScheduleJob` (hourly) snapshots up to `:snapshot_schedule_sample` shards chosen by `Directory.sample_for_snapshot/1` — **active, flushed since their last snapshot, least-recently-snapshotted first**, keyed off a new `last_snapshot_at` column — and `Fathom.Snapshots.RetentionJob` (hourly, +30 min offset) applies `:snapshot_retention` (`%{hourly: 24, daily: 7, weekly: 4}`, or `SNAPSHOT_RETENTION="24h,7d,4w"`). **The selection predicate IS the design:** every snapshot is a server-side object COPY, so cost tracks *writes* rather than tenant count and a million cold tenants cost nothing; `last_snapshot_at` is stamped **only on success**, so a failed snapshot stays at the head of the rotation instead of being marked done for a full cycle. **The safety property:** retention only ever deletes what the scheduler created — `ScheduleJob` labels its snapshots `auto` and `Retention.plan/3` refuses anything else, so an operator's `Snapshots.create(id, label: "pre-migration")` is invisible to the automatic policy (automatic creation and automatic deletion are deliberately the same set; expiring a manual snapshot would delete exactly the backup someone took because they were worried). Retention counts **populated** periods, not wall-clock ones (a week of downtime must not expire the history it protects), buckets on the **ISO week year** (2027-01-01 is ISO 2026-W53), and keeps a future-dated snapshot rather than dropping it (clock skew). `plan/3` is a **pure function** with its own exhaustive suite — bucket boundaries are where this goes wrong and none of it needs an object store; `RetentionJob` is a thin shell that lists, plans, deletes, with `dry_run: true` to rehearse. Both gates verified to discriminate (breaking `auto?` fails 3 tests incl. the end-to-end one; removing the flush predicate fails the cost-control test). The **RPO for logical corruption is the snapshot cadence, not the flush interval** — documented beside the node-loss RPO in `docs/durability.md`.
- **Django parity is MEASURED, four ways (2026-08-05).** The fourth is `test/fathom/django_wire_parity_test.exs`: everything else stops at `Fathom.Shard.Connection`, but `django-libsql` reads Hrana's explicit type TAG (`{"type":"integer","value":"12"}`), not the SQL — so a UDF returning INTEGER inside SQLite that went out tagged `text` would reach Django as a string, and `Duration(...) * 2 == timedelta(...)` would quietly stop being true. Invisible at the `Connection` layer, where the value is already right. It drives `Filo.Plug` as a client does and asserts the tag. **Everything OTHER than the UDFs is identical by construction** — fathom runs the same SQLite — which is why the UDF surface was the whole compatibility problem. "Exact compatibility with Django" is a claim that has to be checked, not asserted, and all three suites generate their expectations by RUNNING the reference rather than hand-writing what it is believed to do. (1) `native/fathom_udf/tests/oracle.rs` — 12,501 cases from Django's own `_sqlite_*` functions. (2) `native/fathom_udf/tests/re_oracle.rs` — **3,444 cases from CPython's `re.search`**, because Django's `__regex`/`__iregex` ARE `re.search`. (3) `test/fathom/django_sql_parity_test.exs` — **42 whole queries run against a real Python `sqlite3` connection with Django's UDFs registered exactly as `django/db/backends/sqlite3/base.py` registers them**, requiring identical rows AND identical `typeof()`. The third is the strongest and catches what per-function comparison structurally cannot: result TYPES, which ROWS a `WHERE` returns, GROUP BY/ORDER BY, NULL columns. **`regexp` was rewritten as a result** (`src/pyre.rs`): Rust's `regex` crate rejects lookaround and backreferences (ordinary Python, so a queryset errored where Django returned rows) and — the dangerous half — compiles `$` and returns the OPPOSITE answer, because Python's `$` also matches before a trailing newline. `re.search("^[a-z0-9-]+$", "a-slug-1\n")` is True in Django and was false here: a slug/username validator silently returning the wrong rows on any text that arrived from a form or a file. Fixed with `fancy-regex` plus a small Python→Rust token translator (`$`→`(?=\n?\z)`, `\Z`→`\z`, `(?P=name)`→`\k<name>`) written as a scanner, not a `String::replace`, so `[a$]` and `\$` survive. Both oracles were verified to DISCRIMINATE by re-breaking the `$` rule (10 regex cases and 3 whole queries fail). Engine surface separately probed clean on a **tenant** handle: savepoints (Django's nested `atomic()`) roll back correctly, and `table_info`/`foreign_key_list`/`index_list`/`index_info`/`foreign_key_check`/`table_xinfo`/`sqlite_master`, JSON1, window functions, `LIKE` case-folding and `COLLATE NOCASE` all work. **Exactly two differences remain and both are deliberate**: `STDDEV_POP`/`VAR_POP` over a NULL-containing column and `VAR_SAMP` over one row, where **Django RAISES and fathom answers** (its aggregate step is `list.append`, so `statistics.pstdev` gets a `None`). Not reproduced on purpose — that is Python-library behaviour, not intended Django semantics, and matching it means shipping a crash on any `StdDev` over a nullable column. The direction is what matters and is asserted in the parity test: **every query that works on Django works here**; only queries that ERROR on Django behave differently.
- **Django UDF compatibility — `native/fathom_udf` (review 2026-08-01 #19).** Django's SQLite backend registers **54** functions on every *client* connection; under `django-libsql` the SQL is compiled by **fathom's** SQLite, where they did not exist — so CRUD worked and the first `__year`/`__date`/`Trunc*`/`__regex` lookup raised `OperationalError`, against the flagship "point an unchanged Django app at it" claim. (TPC-B and TPC-C use only builtins, which is why neither harness ever caught it.) All 54 now resolve: 19 from SQLite's `SQLITE_ENABLE_MATH_FUNCTIONS` build, **35 from `fathom_udf`** — a Rust **loadable SQLite extension** (`cdylib`, `rusqlite`'s `loadable_extension`), loaded by `Fathom.Shard.Extension` on every connection. **The finding's own recommendation was impossible**: "register via exqlite's scalar-function registration" needs an API exqlite 0.37.0 does not expose (no `create_function`, no scalar/aggregate registration, anywhere in `Exqlite.Sqlite3`/`Sqlite3NIF`); what it *does* expose is `enable_load_extension/2`. **Rust over C** because six of the 35 take a `tzname` and need a timezone DATABASE server-side — and those are the ones ordinary apps hit first (`USE_TZ` has defaulted to `True` since Django 5.0, the version `django-libsql` pins) — so `chrono-tz` compiles tzdata into the binary and there is no `/usr/share/zoneinfo` dependency; in C that piece is the whole project. **Security posture:** extension loading is arbitrary code execution on a multi-tenant engine, so the sequence is **enable → load ours → disable**, on every handle (`:ro` included), with a failure to re-disable failing the OPEN rather than degrading; `extension_test.exs` asserts the tenant-side `not authorized` rather than trusting the sequence, and was verified to fail when the disable is removed. Loaded BEFORE the `:attach` authorizer, since tenant handles are the ones running Django's SQL. **Fidelity is generated, not asserted by hand:** `native/fathom_udf/tests/oracle.rs` replays **12,501 cases whose expectations come from running Django's own `_sqlite_*` functions** (`tests/oracle/generate_cases.py` vendors the reference with provenance; JSONL, ~69 KB in git). That caught **seven real bugs the hand-written tests missed** — `django_format_dtdelta` returning TEXT where Django returns a number for `*`/`/` (so `WHERE d*2 = 12` would silently stop matching), an i64 `checked_mul` that would have WRAPPED in the release build, the empty string being NULL alone but an ERROR with a timezone (Django's `try` only wraps `typecast_timestamp`, and `""` returns None *without* raising), `date - timedelta` negating the delta's `.days` FIELD rather than the delta, `cast_date` on a bare date erroring because `datetime.date` has no `.date()`, the diff functions erroring rather than NULLing (they have no `try` at all), and a bare-hour tz offset that only parses on some Python versions. Deliberate divergences, both toward safety: aggregates **skip NULLs** (Django's `list.append` step feeds `None` to `statistics.pstdev`, which raises) and a degenerate group returns NULL rather than raising, matching PostgreSQL. **Cost, measured same-tree on/off:** +39 µs on stream open (374 → 413 µs), per-query unchanged (90 → 91 µs) — and ~21 µs of the original +70 was a `File.exists?` stat per open, now cached in `:persistent_term`. Build is `mix compile.fathom_udf`, wired into `compilers:` AFTER `Mix.compilers()` (the task is defined in this project, so `:elixir` must compile it first); it **skips rather than fails when cargo is absent**, printing a line, so a Rust toolchain is not a requirement for every contributor. `SQLITE_EXTENSION=false` disables; the chaos Dockerfile and CI both install Rust so neither silently tests a fathom without it. Operator view: `docs/quickstart-django.md`.
- **Bench + scale harnesses.** `mix fathom.bench` (+ `scripts/benchmark.sh`, `scripts/perf_history.jsonl`, the `scripts/commit_with_bench.sh` regression gate) measures the hot paths; `mix fathom.scale` measures cold-open at real size, node density (`--ramp`), warm density (`--warm-density`), lease-renewal load (`--lease-rps`), and hot-spot detectability (`--hotspots` — the Phase-2 §B rebalancing evidence: Zipf-skewed load read back through `Fathom.ShardLoad`); `mix fathom.rpo` (`Fathom.Rpo`) quantifies the **loss window** — it sweeps `:shard_flush_interval_ms` × write rate and reports lost rows/seconds on node loss (survivor cold-opens the last flushed object) plus a process-crash-loses-nothing check (`synchronous=FULL`), the in-process complement to `deploy/chaos/chaos.sh soak` (see `docs/durability.md`). See Benchmarking.
- **Warm standby (Phase 2, A1 — built).** `Fathom.Shard.WarmFollower` (gated `:warm_follower`, **off by default** and in test) pre-pulls the fleet's recently-active shards this node doesn't own (`Fathom.Directory.active_recent/1`, LRU-capped at `:warm_cache_max`) into a **separate** cache dir, holding the file but **no lease** — it never serves. On failover the coordinator's cold-open **promotes** the warm copy only after a **freshness check** (a warm cache may lag the owner's latest flush; a stale copy is never served): `Fathom.Shard.Storage.pull_if_changed/3`, a conditional `If-None-Match` GET — 304 promote the cache / 200 re-pull fresh bytes / 404 brand-new. The follower records each object's etag in a `<shard>.db.etag` sidecar and **revalidates its whole cached set each poll** so a failover lands on the 304 fast path. A live-dir warm *restart* (this node's own un-flushed writes) still wins untouched — only the follower-cache path is validated. The warm win is the object **body transfer avoided** — ~2.3× at 1 MB / 30 ms / 100 Mbps, marginal for tiny shards on a fat pipe (both paths still pay ~1 S3 RTT). See Benchmarking (failover RTO, warm density); **how it works end to end** — the lease-less read cache, the freshness/etag revalidation, the failover promote path, and the warm-home rule — is `docs/warm-standby.md`.

**Planned (see `docs/migration-plan.md`), NOT in the code yet — don't assume these exist:**

- **Phase 2 remainder**: shard locality/affinity (C) — its **affinity-aware placement** piece is now **built** (folded into B1): a per-node **warm-location signal** (`Fathom.Rebalancer.WarmLocations` + `shard_warm_locations`) lets `Rebalancer.Policy.best_target` prefer a handoff target that already holds the shard warm, within a load band of the least-loaded so balance still improves (`:rebalance_locality_band`, default 0.5). What remains of C is C1 (rendezvous/bounded-load hashing — marginal per `docs/phase2-scoping.md` §C) and C2 (multi-region region-affinity — a separate initiative). **Live WAL streaming (A2) is BUILT and ON `main`** (merged 2026-08-12 from `a2-quorum-replication`), **off by default** — do not describe it as deferred here. Its blocker ("exqlite exposes no WAL-frame API, so there is no seam to ship a frame from") was disproved 2026-08-09 by the same move as the Django UDFs: a **loadable extension** gets a live `sqlite3*`, and `sqlite3_wal_hook` is in the extension pointer table, so exqlite's surface was never the boundary. It needed **no BEAM cluster** — frames go over A2's own socket protocol, so S3 stays the only cross-node *coordination* — and costs **+225 µs per write (4.04×)** on, noise off. Three traps if you touch it: (a) `sqlite3_wal_hook` and `wal_autocheckpoint` are the **same slot**, so a hook that merely observes silently disables checkpointing on every tenant connection and grows the WAL without bound; (b) **shipping and receiving are separate gates** (`REPLICATION_ENABLED` / `REPLICATION_LISTEN`) and listening must be on fleet-wide BEFORE any node ships, or every write 503s `FILO_NO_QUORUM` — the listener was missing from the supervision tree entirely until 2026-08-10; (c) the replication port is **unauthenticated**, so `REPLICATION_BIND_IP` is a security control. Membership is `REPLICATION_MEMBERSHIP=static|roster` with a guarded swap that refuses any set below `quorum+1` — `n` shrinking below `q` raises inside a tenant's commit. **(d) Shipping frames does not by itself close the RPO gap, and this was the shape of the miss worth remembering: every component worked and the guarantee still did not hold, because nothing connected "which node holds a current replica" to "which node the LB failed over to" — the LB hashes the Host subdomain and knows nothing about replication, so the rig measured an acked, quorum-replicated write LOST while three peers held it.** `Fathom.Shard.Replication.Recovery` (built 2026-08-12, `REPLICATION_RECOVER_FROM_PEERS`, off by default) is Waterpark's "ask the followers and adopt the state of the best reader" — ask (two new frames), choose (`Promote.fresher?/2`, unchanged), pull (the seed frames **in reverse**), publish (the promote path, unchanged); the pulled bytes go through `Follower`'s own seed sink so a pulled replica is indistinguishable from a pushed one and needs no second provenance story. It is **safer than Waterpark's** because fathom has the stored object underneath: a peer is adopted only when *provably ahead* of it, and every uncertain case (unreachable fleet, peer one deploy behind, unstamped object) falls back to the object, so it is never worse than off. The new frames were added **at `@version 2` rather than bumping to 3** — additive, and a bump would refuse every frame from a node one deploy behind, taking the commit path down fleet-wide during a rolling upgrade. **PROVEN ON THE RIG 2026-08-12** by the new `chaos.sh rpo` (two runs): it kills with **no flush in between** — which `chaos.sh failover` structurally cannot, since it sleeps one flush interval first — and runs **both arms**, gate-off must LOSE the row and gate-on must KEEP it, so a run that proves nothing says NOT DISCRIMINATING instead of passing. Since every rig node follows every other, the scenario also **erases one follower's replica** before the kill, or the survivor would hold its own copy and promote-on-open alone would pass with survivor selection off. **(e) That first run found a bug that made BOTH A2 recovery features inert in production, and it is the most important thing A2 shipped:** a takeover *touches* the object (server-side self-copy) to rotate its etag, S3's `REPLACE` metadata directive **drops all user metadata**, and the touch re-sent only the integrity md5 — so `x-amz-meta-fathom-pos` was **erased by every takeover**, and an unstamped object is never overridable by design. promote-on-open had therefore **never worked on a real failover** since it shipped, silently, with the shard recovering to its last flush exactly as if A2 were switched off. `Storage.Local` and `Fathom.Test.FaultyStorage` both keep metadata across a touch, so no unit test could reach it — the same double-vs-real-contract gap AGENTS.md already records for lock etags. The fix is `S3.carry_meta/1`, which carries **every** user key rather than a hand-picked one (the failure mode is an omission from a list), and it is public precisely so it can be tested without a store. **(f) The quorum boundary itself is now proven on real containers, both sides** (`chaos.sh quorum-loss`, 2026-08-13). `soak` kills one node at a time on a 25 s timer, so with q=2 of 4 followers the quorum was reachable in every run this rig had ever done — the boundary had never been crossed. Both arms run every time and discriminate each other: losing `n-q` (2) followers **at the same instant** is invisible (44 ms against a 40–41 ms baseline, 5/5 committed — `Quorum.settle/1` returns at Q, so the survivors carry the commit and nothing waits on the corpses), while losing `n-q+1` (3) fails closed in **37–46 ms** against the 5 000 ms ship timeout, with 0 destroyed rows and 0 isolation leaks in two runs. The fast-fail was **checked, not assumed**: raising the ship timeout **6× to 30 000 ms** leaves the same loss returning `FILO_NO_QUORUM` in **~5 ms**, so it is `Quorum`'s `:impossible` branch and not the deadline. Three things it taught. **The first scenario passed while proving less than it claimed** — recovery was gated on a successful write, and with two survivors and q=2 the shard commits happily while both victims are still booting, so arm 2 could have killed one live node and two corpses; it is now gated on the home holding a live connection to *every* follower (`Fleet.shippers/0` + `Shipper.connected?/1`). **`FILO_NO_QUORUM` arrives as HTTP 200** with the error inside the pipeline body, so an operator health check reading only the status scores a refused write as a success — the first discrimination probe made exactly that mistake. And **the in-process fixture could not express node death**: a `Follower` hands every accepted socket to an **unlinked `Task`**, so `stop_supervised!` leaves the connection alive and serving and it answers `:internal` from `handle_push/2`'s rescue — graceful shutdown, never `:disconnected`. Node death needs a peer whose own process owns its sockets (the black-hole listener in `replication_transport_test.exs`), and the same-shape lesson as the `Storage.Local` vs S3 lock-etag gap: a fixture that cannot express the failure exempts every bug in it. `docs/reviews/quorum-simultaneous-loss-2026-08-13.md`. Still untested on this axis: home + follower dying **together** (deliberately out of scope so a failure means quorum rather than quorum-or-failover), loss under sustained load, and roster-mode membership changes under loss. **(g) A2 DOES NOT REACH THE DOCUMENTED TENANT SCALE — it OOM-kills nodes at 1024 tenants, and this is open.** Found 2026-08-14 the first time `tpc-fleet` was run with shipping on (the gap this file had recorded as untested): the same sweep that runs clean with replication off — 2,918 / 4,081 / 4,644 / **4,374 txn/s at 1024, zero errors** — instead killed a node about two minutes in with replication on, `OOMKilled=true`, on a 94 GiB VM with no container limit, and produced no 1024 result. Where the memory was: survivors held 7–18 GiB each, of which `:erlang.memory()[:binary]` was **17,859 MB of 18,034 MB against 71 MB of process memory** — off-heap refc binaries, which is what a WAL frame is. Nothing leaked: a full GC took that node 17,867 MB → 14 MB, and GC-ing **only the five shipper processes** on another freed 8,105 → 3,995 MB. Fixed in `e0fda94` with `spawn_opt: [fullsweep_after: 0]`, which `Fathom.Shard` already carried for the same reason (expert review 2026-07-24 #9) and which the process actually handling frame payloads never got — measured **7–18 GiB → 83–99 MB per node**. **That fix is real and insufficient**: the re-run still OOM-killed two nodes at 1024, each lasting 6.5 min rather than 2. **A second fix — bounding the shipper's mailbox (`:replication_max_queue`, `ca09c1e`) — was then built on a diagnosis that turned out to be WRONG, and the correction is the most useful thing in this entry.** Sampling during the ramp caught a shipper at **25,866 queued messages** with binary memory tracking queue depth at 0.8–1.8 MB/message in 109 of 143 samples, which read as "the mailbox IS the memory". A clean re-run on 2026-08-16 (one sampler, fresh VM) disproved it: still two nodes OOM-killed, peak **44,959 MB against the original 45,409 MB — essentially unchanged** — and a SURVIVING node observed holding **43,005 MB of binary while its deepest queue was 1**. A mailbox of one cannot be 43 GB. **Why the instrument lied, and this generalizes: `Process.info(pid, :memory)` does NOT include an off-heap refc binary's payload, only the reference** — so "top process by memory" is structurally blind to whoever holds these, and the mailbox was visible *precisely because a queued message is the one place the payload gets counted*. Anything hunting this next needs **per-binary attribution**, which no instrument here has. The mailbox bound is KEPT (256 tenants runs 3,689 txn/s / 0 errors with it, against 3,505 without, so it costs nothing and an unbounded mailbox is a hazard on its own terms) but it is a guard, **not the fix**, and it is soft by construction — consulted on dequeue, so the same run reached 12,828 against a cap of 8,192. Do not re-chase the Logger (counted: ~176 lines/sec, nowhere near enough) and do not re-chase the mailbox; the cause is **OPEN**. Untried: **concurrent seeding**, and `Follower`, which lacks `fullsweep_after` and holds sockets in unlinked Tasks. Until this closes, replication on is a **≤256-tenant** configuration on this rig — that boundary is now well measured on both sides. Still not built: per-shard follower sets and zone-aware placement. The warm standby (A1) and **dynamic rebalancing (B1)** are **built** — see the warm-standby and rebalancer bullets above. A `fathom_native` Rust **NIF** is still absent (no Rustler dep), but **`native/` is not empty**: `native/fathom_udf` is a loadable SQLite extension, not a NIF — twice now the answer to "we need a NIF" was the extension already there.

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

- **`--warnings-as-errors` enforces placement, not just correctness.** Three that cost a compile round trip each, every session, and are invisible until you build:
  - A `@module_attribute` must be **defined above every use**. Adding one next to the function that reads it fails when that function sits earlier in the file.
  - **Clauses of the same name/arity must be contiguous.** Inserting a new `handle_info/2` clause after an unrelated function splits the group. A private helper defined *between* two clauses splits it too — put the helper after the whole group.
  - A new clause must go **above the catch-all**, not merely near it. `def handle_info({:DOWN, ref, …}, %{renew_task: …})` placed after the generic `{:DOWN, …}` clause is unreachable, and the compiler says so.
  - When scripting a bulk edit with `python3`, a `str.replace` with no count replaces **every** occurrence — a duplicated function definition is the usual result. Pass a count, then `grep -c` to confirm.
- **Shell is zsh — stop re-learning this.** These bite every session; internalize them:
  - Backticks and `$(...)` run command substitution even inside double quotes — a backtick or unescaped `$(` in a `git commit -m "..."` body gets executed and silently mangles the message. Don't wrap identifiers in backticks inside `-m`; use `git commit -F <file>` for any non-trivial message.
  - **zsh parameter-expansion patterns glob.** `${var#pat}` / `${var%pat}` treat `(`, `[`, `]`, `#`, `?`, `*` as pattern metacharacters, so `${m#](}` dies with `bad pattern: ](`. Don't hand-strip brackets/parens with `${...#...}` — use `grep -oE`, or just do the text munging in `python3`.
  - Unquoted globs (`*`, `?`, `[...]`) and `{a,b}` brace-expand — quote them when you mean literals.
  - **Don't assume `sed`/`awk`/`dirname`/`head`/`tail` are on PATH** — several aren't in this sandbox (and AGENTS.md already says use Read/Grep/Edit to read/edit files, never those). For a text pipeline, reach for `grep` or a short `python3 - <<'PY'` heredoc, not `sed`/`awk`.
- **When `fathom_native` lands:** the NIF builds via Rustler on `mix compile`; release builds need `MIX_ENV=prod` and are slow (set timeouts). Rust tests: `cd native && cargo test`.

## Workflow

Plan mode for non-trivial tasks (3+ steps or an architectural decision). Stop and re-plan when something goes wrong. Use subagents for research/exploration/parallel work — one task each. When the parent model is Fable, use your own judgement about which model each subagent should run on (match the model to the subtask's difficulty rather than defaulting to the parent's).

**Implementation cycle:** implement → compile → test → (bench if hot path) → `mix precommit` → commit → **push once the work is viable**. Test after every change; fix failures before proceeding. Commit in logical units matching plan phases.

- **NEVER `git push` without the user's explicit approval.** Not "when it looks ready", not "when the suite is green", not "when the work seems viable" — **ask, and wait for a yes.** Commit locally as much as you like; pushing is the user's call every time. When you believe a batch is worth pushing, say so and stop.
  - **Why:** the repo went **public** on 2026-07-29. `main` is the project's face, anyone can be watching, and history is no longer safely rewritable once others may have cloned it. That makes "should this be visible now?" a judgment about the project's public posture — the user's to make, not something to infer from a test run.
  - **Commits still don't wait.** Local commits are the checkpoint that makes a bad step cheap to undo, so batching *commits* is still wrong; batching the *push* is the point. Don't sit on a large unpushed pile for days without raising it — a full night's work was once lost to local-only commits, so name the risk and ask.
  - Supersedes the earlier "ALWAYS push immediately after every commit" rule (written while the repo was private) and the interim "push when the work is viable" wording, which still left the decision to the agent.
- **NEVER commit** with compiler warnings, build errors, or failing tests. `mix precommit` is the gate (see Gates).
- **Never use `sed`/`awk`/`head`/`tail`/`echo` to read or edit files** — use Read (offset/limit), Grep, Edit/Write. Shell text tools are only for things the standard tools genuinely can't do. Piping command output is fine.
- Track plans in `tasks/todo.md`. Record corrections/lessons in `tasks/lessons.md`.

### Stop-after-2-failures rule

If a script or command (test/build/migration/sweep) fails **twice with a similar error**, stop. Print the exact command, the exact error, and a one-paragraph root-cause hypothesis, then wait. Don't loop on infra failures (missing dep, DB-not-created, port conflict, libSQL file-lock) — diagnose them. Same rule for scope blowups: a refactor producing **>50 compile errors** or running **>60 min past estimate** → stop and report. Never paper over flaky tests or build failures with sleeps, retries, or timeout bumps.

### A review's recommended fix is a hypothesis, not a spec

A finding from `/expert-audit`, `/review`, or any panel has **two separable claims**: *this is broken*
(usually right — panels verify by reading, sometimes by execution) and *fix it this way* (frequently
wrong, because the recommender did not run it). **Verify the mechanism of the fix before building
it**, with the cheapest experiment that would falsify it.

Measured on the 2026-08-01 panel, where 4 of ~31 recommended fixes were wrong in ways the finding
itself was not:

- "Set the SQLite authorizer in `Connection.open/1`" — would have broken **every durability flush**,
  because `VACUUM INTO` is implemented as an internal ATTACH. A 30-second probe caught it.
- "`quick_check` the snapshot temp" — the temp is the post-`VACUUM INTO` file, and VACUUM *rebuilds
  indexes from table content*, so it repairs exactly the corruption class the gate exists to catch.
  The gate would have shipped and never fired.
- "Delete the lock instead of rolling it back" — makes the next `acquire_lease` a fresh create at
  **epoch 1**, a larger backward jump than the bug being fixed.
- "Reduce `prev_load` to the shards that moved" — an unmoved shard still needs its baseline, or the
  next tick reports a huge spurious rate for an *idle* shard.

So: implement the finding, not the prescription. When the prescription turns out wrong, **record why
in the code comment and the progress file** — the next reader needs to know the obvious-looking fix
was tried and is wrong, or they will "simplify" it back.

### An existing test that blocks your fix may be right

When a fix breaks an existing test, decide **which of the two encodes the intended behaviour** before
touching either. Three outcomes, all seen in one session:

1. **The test pinned the defect** (`flush_gate_test` asserted "unbounded by default"; `lb_apply_test`
   asserted a byte-identical re-render after a *failed* reload returns `:ok`). Update the test, and
   say in the test itself that it previously asserted the opposite and why.
2. **The fixture was unrealistic** — it fabricated a state production cannot produce (a shard `.db`
   with no provenance sidecar; a 1-row table whose index page is mostly free space, so the corruption
   fixture corrupted nothing). Make the fixture realistic, and comment what makes it so.
3. **The test was right and the finding was wrong.** `S3StealTouchRollbackTest` caught that deleting
   the lock reintroduces an unfenced takeover — a hazard a prior review had added that rollback to
   prevent. The test saved the fix.

A test that fails because of a deliberate, documented constraint is case 3. Read the comment above it
before assuming case 1.

## Testing

Complements the framework **Test guidelines** below (`start_supervised!`, no `Process.sleep`/`Process.alive?`, monitor for DOWN) — those still apply. This section is the *discipline*.

- **Add coverage with every feature:** happy path, error cases, edge cases, backward compatibility. **Don't use TDD/red-green unless explicitly asked** — default to implementation + tests together (or test-after for small changes). If you think red-green fits, suggest it and wait.
- **Two stores, two test modes:**
  - **Postgres directory (`Fathom.Repo`)** → `Fathom.DataCase` with the Ecto SQL sandbox (async-safe, auto-rollback per test).
  - **libSQL shards (`Fathom.Shard` / `Fathom.Shards`)** → no Ecto sandbox; a shard is a real SQLite file. Use a unique `shard_id` per test, drive it through `Fathom.Shards`/`Fathom.ShardExecutor` (the registry + supervisor come up with the app), and `File.rm` the file (`System.tmp_dir!/fathom_shards/<id>.db`) in `on_exit`. Never let two tests share a shard file. See `test/fathom/shard_executor_test.exs`.
- **Save test output to timestamped logs** so results are readable without rerunning, then prune logs older than a day and read the latest instead of rerunning:
  ```bash
  mix test 2>&1 | tee "logs/test-$(date +%Y%m%d-%H%M%S).log"
  # NEVER prune test-failures-*.log — see below. `test-*.log` matches it.
  find logs/ -name "test-*.log" ! -name "test-failures-*.log" -mtime +1 -delete 2>/dev/null
  ls -t logs/test-*.log | head -1 | xargs cat   # read latest
  ```
- **On a failure, DON'T re-run first — name it.** A flake you can't name is a flake you can't fix.
  Two intermittent failures (2026-07-25) lost their identity permanently because the run was piped
  through `tail` and the next run overwrote ExUnit's manifest; ~30 later full-suite runs and 25
  seed-swept runs never reproduced either. In order: read the full output you already have, then
  `mix test --failed` (names + reruns only the failures, but the NEXT run overwrites the manifest),
  then `mix test --seed <N>` from the run header — `--seed` fixes test ORDER, which is the only way
  an order-dependent flake reproduces deterministically. **Never pipe a possibly-failing suite run
  through `tail`/`head`** — the failure block is exactly what gets truncated.
  `Fathom.FailureCaptureFormatter` (wired in `test/test_helper.exs`) is the backstop: any failure
  writes `logs/test-failures-<ts>.log` with the seed, the test's location, and a paste-ready rerun
  command. It writes nothing on a green run.
  **Never prune those.** The documented cleanup above used to be `-name "test-*.log"`, which
  **matches `test-failures-*.log`** — so the one artifact that survives an unreproducible flake was
  being deleted a day later by the very command meant to tidy up after it. That is why
  `lb_apply_test:132` is still unattributed on 2026-08-05: the mechanism was in a log the prune had
  already removed, and an intermittent that recurs a week later has no history to compare against.
  A failure log is evidence, not clutter; it costs kilobytes.
- **Every bug fix ships with a regression test in the same commit.** It must (1) **reproduce deterministically** — fail pre-fix, pass post-fix; if you can't make it fail without the fix you haven't isolated the bug, keep investigating — and (2) **pin the violated invariant**, not just the reproduction steps. Comment the symptom so future readers know why the test exists. Good targets: concurrency/thread-local races (test at the pure-function level), off-by-one/boundary (test at the boundary), classifier/dispatcher mismatches (test the classification), lifecycle ordering (test the sequence).
- **ALWAYS run the new test against the unfixed code.** Stash the `lib/` change, run, confirm it fails, restore. Do this every time, not when it feels uncertain — a test that passes both ways is the default outcome of a plausible-looking test, not a rare one. In one session this caught four separate tests that were measuring nothing.
- **A test that races the idle-stop is reproducible on demand: set `:shard_idle_ms` to 1.** The shape is a test that closes its last connection (arming the idle timer) and then asserts on the coordinator — `Shards.flush/1` is the usual one. `Fathom.Shard.terminate/2` deliberately settles a pending flush waiter with `{:error, :coordinator_stopped}` rather than a bare exit (expert review 2026-07-18 #4, so `Shards.flush` cannot mask it as a false `:ok`), so the coordinator is RIGHT and the assertion is wrong. Found in CI 2026-08-14 (`heartbeat_fence_test:309`, OTP 29, seed 80394); at `:shard_idle_ms` 1 it failed 5/5, and 5/5 again after the fix. **The window is narrow and the obvious probe misses it:** a `Process.sleep` before the flush PASSES, because by then the coordinator is fully gone, `Registry.lookup` returns `[]`, and `flush/1` takes its `[] -> :ok` branch — which reads as "cannot reproduce" and gets a real race written off as an unattributable flake. Fix by pinning `:shard_idle_ms` high for that test and restoring after, not by widening a timeout. A sweep of the other 3 close-then-flush sites (`shard_durability_test`, `shard_flush_reconcile_test`, the rest of `heartbeat_fence_test`) at idle=1 found **none** — the class is not systemic, so re-run this probe on a new failure rather than assuming a pattern.
- **When a regression test passes pre-fix, suspect the harness before concluding "unreproducible".** In order:
  1. **The test double can't express the bug.** `Storage.Local` identified a lock by `{owner, epoch}` while the S3 backend fences with `If-Match: lock_etag`, so an entire class of stale-lease bugs was *structurally invisible* to `mix test` — the regression test for one of them passed against the unfixed code. The fix was to teach `Fathom.Test.FaultyStorage` the real contract, which made the bug class visible for good. **A gap between a double and the real backend's contract silently exempts every bug in that contract; closing it is worth more than the one fix that exposed it.**
  2. **The fixture doesn't create the state.** A corruption fixture that scribbles a *nearly-empty* b-tree page corrupts only free space, so `quick_check` still passes and the test proves nothing. Assert the precondition inside the test (`assert {:error, _} = verify_integrity(path), "the fixture did not actually corrupt anything"`).
  3. **The environment already has the property.** `config/test.exs` sets `heartbeat_server: false`, so every coordinator in the suite is already in legacy mode — a setup block "forcing" it was a no-op that made the test look more specific than it was.
- **A coordinator has TWO liveness modes, and the suite defaults to the one production does not use.** `acquire_gen` is fixed at open: non-nil ⇒ **heartbeat** mode (node heartbeat proves liveness, no per-shard renewal), nil ⇒ **legacy** (per-shard renew PUTs). They are different fence, renewal and release paths. `heartbeat_server: false` means a test gets LEGACY unless it starts `Fathom.Shard.Heartbeat` itself, while production and the chaos rig run HEARTBEAT — so a bug that only exists on the heartbeat side is invisible to `mix test` by default. Measured 2026-08-04: the drop path's ownership-unconfirmed lock leak existed in **both** modes and only the legacy one had ever been tested. For anything touching the lease/fence/flush/drop paths, **parameterize the test over both modes** (`for mode <- [:legacy, :heartbeat]`, see `test/fathom/shard_lease_release_test.exs`) and **assert the mode actually took** (`acquire_gen` non-nil/nil) rather than trusting the setup — a scenario that silently ran legacy twice looks like two-mode coverage and is one. Same reasoning as the `Storage.Local` vs S3 lock-etag gap above: closing an environment gap is worth more than the single fix that exposed it.
- **Reaching a specific fence verdict needs the right fixture, and the wrong one passes quietly.** Making `Fence.check` return `:skip` in heartbeat mode by killing the `Heartbeat` process does NOT work — a DOWN heartbeat degrades to the legacy renew fence, which succeeds. The real `:not_valid` state is the process ALIVE with its renewal deadline not comfortably ahead (`now + margin >= deadline`); publish a past `mono_deadline_ms` at the SAME generation (a different generation routes to `:revalidate`). The first fixture written for this passed against unfixed code, which is the only reason it was caught — always assert the intermediate state (`valid_for_write?(gen) == :not_valid`), not just the outcome.
- **If a test genuinely cannot discriminate, say so in its moduledoc.** Some real bugs need a microsecond race or a fault the live backend has no seam for. Keep the test as an invariant guard, but write "these do NOT reproduce the race, and here is what the fix rests on instead" — plainly, in the file. Never let a non-discriminating test read as a regression test; the next person will trust it.
- **Fathom-specific must-test invariants** (these are the bugs that bite a sharded multi-tenant system):
  - **Shard isolation.** A query for shard A must *never* resolve to or read shard B's data. Any change to routing — `Fathom.ShardExecutor.shard_from_conn` (request → shard), `Fathom.Shards` resolve, shard-path construction in `Fathom.Shard`, or the planned `Fathom.Directory` — ships with a test proving cross-shard isolation.
  - **Migrations are tested both ways.** Every schema migration ships with a test that runs the forward copy+transform on a seeded `vN-1` shard and validates `vN` (row counts / checksums), **and** a test for the revert pointer-flip back to `vN-1`. See the migration gate.
  - **Cross-version tolerance.** During a rollout the fleet is mixed `vN-1`/`vN`; assert the app reads both (`schema_version`-aware branch, or `vN` superset still usable by old code).
- **Hot-path verification.** When you change a hot path (shard cold-open, directory resolve, migration copy, concurrent shard fan-out), add a microbench-style test that asserts an order-of-magnitude floor/ceiling (e.g. `assert open_us < 50_000`), not an exact latency. Tag it (`@tag :bench`) so it's excluded from the default suite. See Benchmarking.

## Benchmarking

**The harness exists** (`docs/benchmark-plan.md`). `mix fathom.bench` measures the hot paths; `scripts/benchmark.sh` runs it prod-compiled against a throwaway `fathom_bench` DB and appends one JSON line per run (commit, branch, dirty, host, metrics) to `scripts/perf_history.jsonl`; `scripts/commit_with_bench.sh` is the bench-then-commit gate — it benches the working tree and **refuses the commit if ANY metric regresses ≥20%** vs the parent's same-host entry (`Fathom.Bench.Gate`; multi-metric because fathom's cost is per-shard open + fan-out; same-`host` comparisons only). Metrics: `cold_open_p50_us`; `cold_open_s3_p50_us`, `warm_s3_shards_per_s`, `failover_cold_s3_p50_us`/`failover_warm_s3_p50_us` (all opt-in S3 — unset `FATHOM_S3_TEST_*` ⇒ `nil`/skipped, so the default gate stays S3-free); `dir_resolve_p50_us`; `copy_keystone_rows_per_s` (renamed from `copy_rows_per_s` 2026-07-31 when the fixture became `Fathom.Keystone` — the old series is not comparable); `fanout_kb_per_shard`; and the two **wire** metrics gated from 2026-07-31, `hrana_rt_us` (per-REQUEST loopback round trip) and `wire_rows_per_s` (per-CELL result-set encode over keystone rows, blobs included). Before those two, **no gated metric executed any `Filo` code**, which is how a 200x row-encoding regression stayed invisible; both were verified to discriminate before being gated. Hot-path changes also ship `@tag :bench` floor/ceiling guards (`test/fathom/bench_test.exs`). Hold the discipline: don't invent fake numbers; measure or say "unmeasured."

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
- **A suspiciously GOOD number is a broken measurement until proven otherwise** — and it is the easier one to bank by accident, because the gate says OK and nobody looks. Two from one session: a first-draft `flush_p50_us` read **2 µs** for a full `VACUUM INTO` + upload (the bench's minimal tree has no `WriteCounter`, so `bump/1` rescued to `:ok`, the shard read clean, and `flush_now/1` returned having done nothing); and `fanout_kb_per_shard` reported a **−29% "win"** against a tight 3.77–4.07 historical band on a commit that could not plausibly have caused it (a re-bench of the same HEAD returned 3.77). **Check any metric that moves outside its own historical band in EITHER direction, and re-bench HEAD to leave a corrected baseline** — the real damage from an outlier low is the *next* commit being gated against it, where an ordinary reading becomes a false ≥20% regression and blocks clean work.
- **A new bench metric must assert its own preconditions.** The failure mode is not a wrong number, it's a number for work that never happened. Put the guard inside the harness (`unless dirty?(pid), do: raise "flush bench is measuring nothing"`), so the metric fails loudly instead of reporting a spectacular result.
- **Bench-gate baseline workflow.** `commit_with_bench.sh` compares against the parent's **clean-tree** entry, and committing does not leave one (the pre-commit run is recorded `dirty: true`). So each successive gated commit needs: `git stash push -- lib/ test/` → `scripts/benchmark.sh` → `git stash pop` → gate. Skipping it yields `no baseline for parent <sha>`, and the gate then reaches back to an older, possibly-outlying entry. Same trap the `[skip-bench]` note below describes, reached a different way.
- **`./chaos.sh up` does NOT build.** `cmd_up` is `compose up -d --wait`, so it starts whatever `fathom-chaos:latest` already exists — which can be weeks old. A full rig pass against a stale image is the most expensive way to be wrong, because "it passed" reads as validation. **Before trusting any rig result, prove the image contains the change under test**, in this order:
  1. `./chaos.sh build` first, always, when the rig is validating a code change.
  2. Check the date: `docker image inspect fathom-chaos:latest -f '{{.Created}}'` against `git log -1 --format=%cI`.
  3. Best — assert the fix's **own observable** through the LB before running anything else. A rig validating the ATTACH fix should first confirm `ATTACH DATABASE …` is *refused*; if it succeeds, stop, the binary is old. (Learned the hard way on 2026-08-02: `smoke` and `deploy` both passed against a build that predated every fix under test, and the tell was that ATTACH still worked.)
- **Docker is machine-global, and this machine runs more than one fathom stack.** A sibling checkout (`djathom`) keeps its own compose project up for days. Containers, networks and host ports are namespaced by compose project, so there is no cross-talk — but confirm rather than assume with `docker inspect -f '{{index .Config.Labels "com.docker.compose.project.working_dir"}}' <container>`, which names the checkout that launched it. They do share the colima VM's CPU, so **rig timings are contended even when pass/fail is trustworthy** — treat rig latency as relative, per the same-topology rule.
- **`./chaos.sh down` before benching** — the rig's 8 containers compete with `scripts/benchmark.sh` for the same cores, and it does not shut itself down after a verification run. `docker ps -q | wc -l` should be **0**.
- **When two unrelated metrics move TOGETHER in one run, it is the machine, not the code.** The single best tell, and cheaper than any reasoning about mechanism. Measured 2026-08-05 on a change that added `:os_mon`: one gate run blocked on `dir_resolve_p50_us` +46.9% AND `hrana_open_rt_us` +27.6%, the latter the highest value ever recorded. Two more working-tree runs read 357/359 and 130/132 against a parent of 355/128 — i.e. the pair moved together in exactly one run and nowhere else. A code change that genuinely slowed a Postgres resolve *and* a Hrana stream open by different amounts is far less likely than one contended run. `dir_resolve_p50_us` is also visibly **bimodal** (clusters near ~128 and ~185), so its "regression" is often just which mode the baseline landed in.
- **`cold_open_p99_us` is FAT-TAILED and will false-block you ~1 run in 3.** Measured 2026-08-04 on a change that could not possibly touch it: five gate/bench runs of the same working tree read 5053, 5367, 2076, 2209 against parent runs of 2786, 2039, 2211, 2362 — two isolated samples at ~2.4× and the rest indistinguishable. The p50 never moved. **Diagnose it with a same-harness A/B, not with a mechanism argument.** Bench parent and working tree the same way, back to back; if they agree, the block was a tail sample. Three wrong explanations were talked through first — "the rig was up" (it was down for the second block), "the gate uses a different harness" (`commit_with_bench.sh` just calls `benchmark.sh`), and "the code can't reach it" (true here, but that reasoning had already been wrong twice that day). The number settles it; the story does not. Do not reach for `--skip`: re-establishing a clean parent baseline as the last history line and re-running the gate let it pass on its own terms.
- **Never change the shared bench `setup/1` to serve one metric.** Starting `WriteCounter`/`FlushWatermark` there so the flush metric would work changed what `fanout_kb_per_shard` measures (every open shard gains ETS rows) — the gate correctly blocked at **+46.5%**. That is a *harness topology* change, and per the same-topology rule it invalidates the historical series. Scope new dependencies to the metric that needs them.
- **Scale test (built).** `mix fathom.scale [--shards N] [--shard-size-mb S]` (`Fathom.Scale`) provisions N realistically-sized shards and measures cold-open latency at size + **fan-out node density** (BEAM + RSS per open shard, open throughput). `--ramp [--max N]` opens empty shards until the fd ceiling to find the node-density limit cheaply; `--warm-density` per the bullet above; `--lease-rps` shows per-shard lease renewals collapsed to one node heartbeat (flat regardless of shard count). Raise `ulimit -n` first (~3 fds per live connection). Measured 2026-06-29: 1000 × 4MB → ~196 KiB RSS/shard, warm cold-open ~2.6 ms p50; ramp held 50k linearly → fd ceiling ~82k. (The ramp's per-open slowdown at high N is a one-process test artifact, not fathom — production spreads shards across per-stream processes.)
  - **Hot-spot detectability (`--hotspots`, the Phase-2 §B rebalancing unblock).** `mix fathom.scale --hotspots [--shards N] [--zipf s] [--queries Q] [--workers W] [--stream-len L]` (`Fathom.Scale.hotspots/1`) is the **first reader of `Fathom.ShardLoad`**: it turns on `:shard_load`, drives a Zipf(s)-skewed load over N shards through the real recording path (`Shards.checkout` → `ShardExecutor.execute`), then reads the counters the way a rebalancer would — diff two `snapshot/0`s over a window into per-shard rates. **Load unit is a Hrana stream** (`--stream-len L`): each stream checks out + opens a connection once, bursts L queries on the held connection, then checkins — the real per-stream model. `L=1` (default) is one query per stream (the per-query lower bound, finest detection sampling); raise L to measure realistic throughput, scaling `--queries` so `Q/L` (the stream count) stays ≫ N. Reports the rate distribution (p50/p90/p99/max), two separations (max/median and tail-robust max/p99), **three threshold-family sweeps** (`>Kx median`, `>Kx p99`, and an absolute q/s floor isolating the top-N — each with a Zipf-recall check), a `median_collapsed` flag (shape-based: `>10x-median` flags ≫ `>10x-p99` ⇒ median-relative over-flags), a scale-robust anti-flap signal (top-20-by-rate set overlap across two windows), and whether `ShardLoad.top(20)` recovers the Zipf head. **Detection finding (prod, 10k shards, s=1.1 — synthetic/one-host-relative):** clean (`ShardLoad.top(20)` recall **1.0**, top-20 Jaccard **0.9**, sharp head hot_1 188 q/s → …), but the long cold tail makes **`>Kx-median` over-flag (751/421/216) — key hot-detection on `>Kx-p99` (>20×p99 → 5 shards, recall 1.0) or an absolute q/s floor.** **Throughput finding (prod, 1000 shards, ~20k streams; 2026-07-23):** persistent streams remove the per-query coordinator bottleneck — L=1 **~3.3k q/s** → L=16 **~27k** → L=64 **~54–55k q/s** per node (hottest shard ~600 → ~4.9k → ~9.5k q/s), detection quality flat across all L. So the earlier ~1.2k q/s was the per-query artifact, not a fathom limit. The held-stream numbers are **+13–16% over the pre-2026-07-23-iteration code** (same-day A/B — the per-connection statement cache: at L=64, 63 of 64 queries skip `sqlite3_prepare_v2`), while L=1 is flat — a one-query stream never hits the cache and is per-stream-open-bound, which is the connection-pool question, measure-first pending `hrana_rt_us` (`docs/reviews/hotspots-ab-2026-07-23.md`). **Non-synthetic confirmation — DONE** (`deploy/chaos/chaos.sh hotspots`, 2026-07-06): 17.5k real Hrana requests through the LB over 3 nodes recovered the Zipf head at top-20 recall 0.95, with the hot set spread across nodes (a rebalancer reads `ShardLoad` per node and merges). Enable on a deployed node with `SHARD_LOAD=true` (`config/runtime.exs`).

## Gates

A "gate" is a check that must pass *before* a commit lands — not after.

- **GitHub Actions CI runs again** (2026-07-29). It was off while the repo was private — the
  account is out of Actions minutes for private repos, and a run would fail in ~12 s having
  executed zero steps. Going public restored free minutes. The first real run immediately caught
  a bug the outage had been hiding: the workflow hardcoded a developer's local username as the
  Postgres role, so `config/test.exs` (which resolves `PGUSER || USER || "postgres"`) asked for
  role `runner` on a runner. `PGUSER`/`PGHOST` are now pinned in the job env.
  **CI is the second opinion, not the first** — `mix precommit` is still the gate that has to pass
  before a commit lands. Disable with
  `gh api -X PUT repos/cwisecarver/fathom/actions/permissions -F enabled=false`.
- **`mix precommit` is the commit gate** (defined in `mix.exs`): `compile --warnings-as-errors`, `deps.unlock --unused`, `format`, `dialyzer`, `test`. **Never commit if it fails.** Run it when you're done with all changes and fix everything it surfaces.
- **Typing gate — Dialyzer (added 2026-08-14).** Runs inside `precommit` after `format` and before `test` (deterministic, reuses the fresh beams, ~2 s warm — much cheaper than the suite, so failing fast there is the right order), and in CI across OTP 27/28/29 so a typing difference between VM versions surfaces there rather than on one developer's machine. Manual run is **`MIX_ENV=dev mix dialyzer`**. **The env is the SCOPE, not a detail**: `elixirc_paths/1` compiles `test/support` only in `:test`, so `:dev` analyzes `lib/` alone — which is the plan's stated scope, and which keeps the benchmark drivers' `mint_web_socket` opaque-type cascade (21 findings from one dependency issue, see `mix.exs`) out of the gate. Inside `precommit` it is `cmd env MIX_ENV=dev mix dialyzer`, and the `env` is required — `mix cmd` does not use a shell, so a bare `VAR=value` prefix is taken as the executable and dies with `:enoent`. **First run after `mix deps.get` or a dependency bump pays a partial PLT update (minutes, once); a first-ever build is ~10–20 min.** PLTs live in `priv/plts` (gitignored) so `rm -rf _build` doesn't discard them, and CI caches them keyed on `mix.lock` + OTP version. Suppressions go in `.dialyzer_ignore.exs`, which documents the only two legitimate reasons to be in it and requires a comment per entry; `list_unused_filters: true` fails the run on a filter that stopped matching. **The gate was verified to bite** before being trusted (a deliberately wrong return type on `Shards.migrate_on_touch_mode/0` exits 1 at the dialyzer step without reaching the tests) — see § Typing for the style rules and what it actually caught.
- **Migration gate.** A schema migration must not ship without: (a) a forward copy+transform test, (b) a revert-flip test, (c) a cross-version-tolerance check. A migration that can't be reverted by pointer-flip within the retention window, or that the running app can't tolerate mid-rollout, is not done.
- **Shard-isolation gate.** Any change to shard routing (`Fathom.ShardExecutor.shard_from_conn`, `Fathom.Shards`, shard-path construction, or the planned `Fathom.Directory` resolve) must have a test proving shard A never resolves to shard B. Treat a cross-tenant leak as a release blocker, not a finding.
- **NIF-contract guard (when `fathom_native` lands).** Elixir is dynamically typed, so a NIF signature change (arity, **return shape**, param types) silently breaks Elixir callers at *runtime* — the Rust compiler can't catch it. After any NIF signature change, run the integration tests that exercise it and `grep -rn "Fathom.Native.<fn>"` for every caller. Don't rely on the unit suite to catch a contract break.
- **Bench-then-commit gate** (built). Any change touching a hot path (shard routing/open, directory resolve, migration copy, the shard coordinator) goes through `scripts/commit_with_bench.sh -m "<msg>"`: it benches the working tree and refuses the commit on a ≥20% regression in any metric vs the parent's entry in `scripts/perf_history.jsonl` (override `PERF_REGRESS_BLOCK`). Pure docs/test/comment-only changes skip it — `git commit` directly with a `[skip-bench]` token, or `--skip`. See Benchmarking and `docs/benchmark-plan.md`.

## Typing

Dialyzer-enforced `@spec` coverage. The gate is in § Gates; this is how to write for it.

**The defect it actually finds, over and over.** Fathom had 309 `@spec`s and nothing had ever
verified one. The 2026-08-14 baseline was 115 findings, and the dominant shape was not a wrong
type — it was a **stale** one: someone adds a field or a return case to the code and to every
caller, and never to the declaration. Seven instances, all on paths that matter, all silent:

| stale declaration | what it omitted |
|---|---|
| `Recovery.position` ("the same shape as `FollowerLog.t()`") | `torn` — the field deciding whether a replica may be promoted at all |
| `Storage.lease()` (a CLOSED three-key map) | `lock_etag` — the fencing token release is conditional on |
| `Storage.pull/2`'s `@spec` (its `@callback` was right) | `{:absent, _}` |
| `pull_snapshot/3`'s `@callback` AND `@spec` | `{:absent, _}` |
| `Migrator.status/0` | `review_blocks` — a published control-plane field |
| `Copy.migrate/4` | that statements are `{sql, args}` pairs, not strings |

None broke anything at runtime. Three had a worse consequence than a bad doc: dialyzer concluded
whole paths were **unreachable** — A2 cross-fleet promotion and its mid-flight object re-check read
as dead code because `Recovery.position` lacked one field. **When a type and its callers disagree,
suspect the type**, and prefer aliasing the owning type (`@type position :: FollowerLog.t()`) over
restating its shape, so the drift cannot recur.

**Style rules.**

1. **Skip behaviour callback implementations** — GenServer/LiveView/Plug/Oban/Mix.Task/
   `Filo.Executor`/`Fathom.Shard.Storage` impls. The contract lives once, on the `@callback`.
2. **Spec the client API of GenServers**, not the server callbacks — but know what that buys.
   **A `@spec` on a GenServer client wrapper is DOCUMENTATION ONLY; dialyzer cannot check it.**
   Measured 2026-08-14: `@spec dirty?(pid()) :: :definitely_not_what_it_returns` on
   `Fathom.Shard.dirty?/1` — whose body is one `GenServer.call/3` — passes the gate, because
   `GenServer.call/3` returns `term()` and nothing contradicts it. The discriminating pair is
   `Shards.migrate_on_touch_mode/0`, a pure config read, where the same deliberate break IS caught
   as `invalid_contract`.
   **The rule this generalizes to, and the one worth planning around: a spec is CHECKED only where
   dialyzer can compute a success typing from the body that contradicts it.** Pure functions, data
   transformations and anything whose shape flows between modules are checked. Wrappers over
   `GenServer.call`, `:ets`, dynamic dispatch (`backend().pull(...)`) and NIFs are not. Write them
   anyway for legibility — but do not count them as coverage, and spend effort on the data
   contracts first, since every defect found on 2026-08-14 lived in one.
3. **Reuse owned types**: `Fathom.ShardId.t()`, `Storage.lease()`, `Ecto.Changeset.t()`. Never
   re-inline a shape that has a name — that is how the table above happened.
4. **Ecto schemas get `@type t :: %__MODULE__{}`.** Three were missing it while six specs named it.
5. **A function that always raises is `no_return()`**, not an ignore entry (`mix fathom.token`).
6. **No defensive typing**: no guards added to satisfy a spec, no `term()`/`any()` escape hatches.
   State the bound you can prove — `Migrator.status/0`'s `eta_seconds` is `integer()`, not
   `non_neg_integer()`, because that is what is provable.

**Two things learned the hard way, worth not rediscovering.**

- **Dialyzer uses SUCCESS TYPINGS, not contracts, when analyzing callers.** So a `@spec` on a
  helper — however accurate, including a polymorphic `when result: var` — cannot widen or narrow
  what its callers see. Measured twice on 2026-08-14 (`Bench.with_wire/3`, `HranaClient.await_upgrade/2`).
  An accurate spec on a *public* function still helps: fixing `HranaClient.execute/3` cleared 21
  downstream findings at once.
- **A dependency's `@opaque` type can make a whole subsystem read as dead.** `Mint.WebSocket.t()`
  is opaque, so dialyzer cannot see the `{:ok, conn, t()}` branch of `new/4` and decides the
  handshake never succeeds. Confirm that class in ISOLATION with a probe module that does nothing
  but call the dependency — it separates "our code confuses dialyzer" from "the dependency's
  typings are wrong" in about a minute.

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