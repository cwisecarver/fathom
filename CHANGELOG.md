# Changelog

All notable changes to fathom are recorded here. Format follows
[Keep a Changelog](https://keepachangelog.com/); versions are [SemVer](https://semver.org/).

**fathom ↔ Filo compatibility.** Filo (the Hrana wire server) is a path dependency
(`{:filo, path: "../filo"}`) and is not published yet (expert-review #9), so each fathom release
records the Filo version it was built against. Pin both repos to the listed pair in CI.

| fathom | Filo | Notes |
|---|---|---|
| 0.3.0 | **0.2.0** | Requires Filo 0.2.0 or later: fathom uses `Filo.Client` (the chaos-rig driver) and the `:authorize` seam (`Fathom.HranaAuth`), neither of which exists in Filo 0.1.0. |
| 0.2.0 | 0.1.0 | Filo path dep; HTTP v1/v2/v3 (+cursor) + WebSocket hrana1/2/3. |
| 0.1.0 | 0.1.0 | Initial working slice. |

## [0.3.0] - 2026-07-28

Three expert-review passes (2026-07-19, 2026-07-24, 2026-07-27) plus the Phase-2
rebalancer and a sustained performance iteration. 231 commits: 32 features, 35 fixes,
50 performance changes.

### ⚠️ Behavior change

- **The default flush interval is now 5 s, was 30 s.** This is the node-loss RPO knob,
  so the change tightens the loss window ~6× (measured: 452 rows p50 / 800 p99 lost at
  5 s under 200 writes/s, vs 2,647 / 4,800 at 30 s) at the cost of ~6× the flush rate —
  each flush costing about the same. If you tuned around the old default, set
  `SHARD_FLUSH_INTERVAL_MS=30000` to keep it. Evidence and the cost side:
  [`docs/durability.md`](docs/durability.md), `docs/reviews/rpo-sweep-2026-07-18.md`.

### Added

**Control plane and security**

- **Scoped, revocable, per-identity API keys** for the control plane (`read < manage <
  destroy`), replacing shared-credential-only access. Note the scopes are
  capability-level, not per-tenant.
- **Append-only audit trail** over control-plane and admin actions.
- **Rate limiting** on admin auth and the `/api` control plane.
- **Snapshots and point-in-time restore over the API** (`Fathom.Snapshots`,
  `mix fathom.snapshot`), including a guard against restoring across a
  schema-migration boundary.
- **A durable storage tombstone** so a Postgres directory restore can't resurrect a
  deleted tenant, and a **storage-backed token-revocation floor** so it can't
  un-revoke a token.
- **`mix fathom.directory reconcile`** — the cross-store (Postgres + S3) DR sweep,
  with a runbook.
- **Non-DNS-safe tenant ids are warned/refused at provision and fork**, so no
  un-servable `libsql://<id>.<zone>` URL is ever minted.

**Durability and recovery**

- **Node-level graceful drain** (`Fathom.Shards.drain_all/1` + health draining), so a
  deploy is a drain rather than a crash — every coordinator flushes and releases its
  lease before exit.
- **Automated sampled restore drills**, so a bad cold-tail backup is caught before the
  tenant comes back.
- **Quarantine inventory, recovery-diff, and age-capped retention** — preserved writes
  stay recoverable without leaking disk forever.
- **Export flushes first and integrity-checks**, so a portability export can't silently
  omit or corrupt data.
- **Flush jitter + a node-wide concurrent-flush cap** to bound post-failover storms.
- **A write circuit-breaker** when a node is provably stealable.
- **Optional zlib compression** for stored shard objects (off by default — enable only
  after measuring, it is bandwidth-bound).
- **`mix fathom.rpo`** — the loss-window measurement harness, plus invariant tests.

**Operations**

- **Oban control-plane liveness gauges** — these catch jobs that *don't* run, which
  ordinary job metrics don't.
- **TLS-terminating eval LB config** (Hrana `wss` :8443, control plane `https` :4443).
- **A failover-herd chaos scenario** with live measurement and warm-standby sizing.
- **Migrator**: `requires_review` blocking for flagged data migrations, non-atomic
  migration-gap detection at capture, an `:async` enqueue-on-touch mode, a Django
  migration workflow doc + convergence API, and a revert runbook with a
  template-drift consistency check.

### Fixed

- **Read-only transactions no longer dirty a shard** (plus two latent write-loss holes
  closed alongside it).
- **A dirty shard now flushes on shutdown even with connections still checked out.**
- **`drop_clean` releases the lease** even when no local file exists.
- **`flush_now` surfaces `{:error, :flush_timeout}`** instead of a false `:ok`.
- **A racing fork check can no longer quarantine a good warm copy.**
- **Snapshot restore rejects a caller-supplied id that escapes its prefix** (path
  traversal).
- **Steal decisions use S3's clock, not the reader's**, to judge owner liveness; the
  steal-time touch **preserves the MD5 integrity metadata**; and a cold crash-steal
  **adopts the touched etag instead of re-pulling the whole object**.
- **At-capacity eviction skips busy shards**, so a long-lived stream can't starve
  admission.
- **Oban `Lifeline`** — a node crash can no longer wedge a shard's jobs forever.
- **The LB 502 storm is fixed** — an nginx `Connection: close` defeated upstream
  keepalive. This invalidates every pre-2026-07-23 `tpc-fleet` absolute (see
  `docs/reviews/lb-502-fix-2026-07-23.md`); current numbers are 4,096 concurrent
  tenants at 3,414 txn/s, zero errors.
- **The per-shard size brake moved to write time** and fails loudly at the cliff.
- **The directory reconcile sweep streams** instead of loading the whole table.
- **Recorder touches stamp on the directory's clock**, not the BEAM's.
- **`rel/` is shipped into the release images** (it was silently absent, so every
  `vm.args` flag was too).

### Performance

Measured, with the reports in `docs/reviews/`. Highlights: the drain path dropped
from ~3.5× to ~2.3× S3 RTT (a redundant lock GET removed); a per-connection statement
cache (+13–16% on held streams); inbound WebSocket frames are no longer double-scanned;
large cursor responses no longer spool to disk; idle streams hibernate and the idle
timeout is configurable; the warm follower's steady-state refresh cost is bounded; the
Postgres pool is sized against actual demand; and the admin dashboard stopped counting
the whole shards table on every keystroke.

## [0.2.0] - 2026-07-16

Expert-panel review hardening (`docs/reviews/expert-review-2026-07-14-224105.md`) — the
production-readiness, correctness, tenant-lifecycle, and ops-packaging pass over the working slice.
Highlights (see the review's resolution log for the full per-finding list and commits):

### Added
- **Tenant control plane** — provisioning API (`POST/GET/DELETE /api/tenants`), suspend/resume,
  GDPR delete + export, database forking, and `mix fathom.shard pull|inspect|fork|loss-report`
  operator tooling (`docs/tenant-lifecycle.md`).
- **Token lifecycle** — zero-downtime rotation (grace window), read-only token scope, expiry
  warning, and mint/rotate/revoke API (`docs/auth.md`).
- **Point-in-time snapshots + restore** (`Fathom.Snapshots`, `mix fathom.snapshot`) and S3 bucket
  hardening guidance (`docs/durability.md`).
- **Per-query resource bounds** — statement timeout, result-row cap, per-shard concurrent-stream cap
  (all config-gated, off by default).
- **Per-shard size cap** (`SHARD_MAX_PAGE_COUNT`) enforcing the "limited dataset per shard" premise.
- **Post-node-loss loss accounting** — persisted `last_flushed_at` + `flush_lag_report`.
- **Admin UIs** — directory browse/edit (`/admin/directory`) and a front-door query console
  (`/admin/query`).
- **Ops packaging** — a single-node eval stack (`deploy/compose/`), a full env-var reference
  (`docs/configuration.md`), a Django quickstart (`docs/quickstart-django.md`), non-lease incident
  runbooks (`docs/runbooks/operations.md`), a shipped observability package (`deploy/observability/`
  — Prometheus rules + Grafana + SLOs), and this deploy runbook (`docs/runbooks/deploy.md`).
- **Hrana** `sequence` + `describe` (`executescript()` / prepare introspection).
- **`releases:`** config in `mix.exs`.

### Changed
- Foreign keys enforced by default on every shard connection.
- SQL errors mapped to real Hrana codes (`SQLITE_CONSTRAINT_*` / `BUSY` / `FULL` / …) so Django can
  classify `IntegrityError` vs `OperationalError`.
- `autocommit?/1` reports real transaction state; `… RETURNING` DML reports real rowcounts.
- Observability: exported the page-worthy signals (corrupt-flush, fenced-quarantine, novel-limiter
  429s, idle evictions, heartbeat lapse, Oban failures) that previously emitted telemetry but were
  never scraped.

### Fixed
- `PRAGMA quick_check` before the raw-upload flush; a corrupt local file is quarantined, never
  flushed over the good S3 copy.
- Self-fence quarantines acked-but-unflushed writes (`.fenced.<ts>`) instead of dropping them.
- The capture template is excluded from the laggard/rollout sweep (no self-quarantine of the source).
- A delete that couldn't stop a busy coordinator (which then self-fenced and quarantined the erased
  data) — `Shards.stop/1` force-stops while the lease is valid.

### Verified
- **Clean-shutdown / rolling-deploy**: a graceful (SIGTERM) shutdown flushes every open shard with
  zero committed-write loss at 200 and 500 open dirty shards
  (`docs/reviews/deploy-clean-shutdown-2026-07-16.md`).

## [0.1.0]

The initial working slice: the shard data path (one connection per Hrana stream, per-shard
coordinator, pull/flush storage), the Filo Hrana wire protocol, Host-subdomain shard selection +
admission, the Postgres directory / migration engine, the cross-node S3 lease/epoch/heartbeat fence,
the LB-keyspace-partition cluster layer, warm standby (A1), and dynamic rebalancing (B1). See
`AGENTS.md` and `docs/README.md`.

<!-- Cut a release: bump the version in mix.exs, date the section above, then `git tag vX.Y.Z`. -->
