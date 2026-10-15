# Changelog

All notable changes to fathom are recorded here. Format follows
[Keep a Changelog](https://keepachangelog.com/); versions are [SemVer](https://semver.org/).

**fathom ↔ Filo compatibility.** Filo (the Hrana wire server) is a path dependency
(`{:filo, path: "../filo"}`) and is not published yet (expert-review #9), so each fathom release
records the Filo version it was built against. Pin both repos to the listed pair in CI.

| fathom | Filo | Notes |
|---|---|---|
| unreleased | **0.2.0** (from Hex) | Filo is a Hex dep now, so Mix enforces this pairing instead of the table. |
| 0.3.0 | **0.2.0** (path dep) | Requires Filo 0.2.0 or later: fathom uses `Filo.Client` (the chaos-rig driver) and the `:authorize` seam (`Fathom.HranaAuth`), neither of which exists in Filo 0.1.0. |
| 0.2.0 | 0.1.0 | Filo path dep; HTTP v1/v2/v3 (+cursor) + WebSocket hrana1/2/3. |
| 0.1.0 | 0.1.0 | Initial working slice. |

## [Unreleased]

### Upgrade notes — read before deploying

Three expert-review rounds (2026-10-12 twice, 2026-10-13) landed on main. These change what a
prod node needs to boot or run:

- **Prod refuses to boot on weak admin credentials.** A blank `ADMIN_USER`/`ADMIN_PASS` now means
  "unset" (the dashboard answers 503). A password under 16 bytes, or equal to the user name,
  refuses boot unless `ADMIN_WEAK_CREDENTIALS_OK=true`. See `docs/configuration.md`.
- **Prod refuses to boot when the admin throttles are on and `TRUSTED_PROXIES` is empty** (every
  client would share one lockout bucket). Set `TRUSTED_PROXIES`, or `ADMIN_THROTTLE_SHARED_BUCKET_OK=true`
  if there is no proxy.
- **Prod refuses to boot with a replication frame-auth secret under 32 bytes** while signing or
  requiring frame auth. No opt-out.
- **S3 credentials need two more permissions:** `s3:ListBucketMultipartUploads` and
  `s3:AbortMultipartUpload` (tenant purge aborts the shard's leftover multipart uploads). Also add
  an `AbortIncompleteMultipartUpload` lifecycle rule (1 day) to the shard bucket. See
  `docs/single-writer.md`.
- **Raise `POOL_SIZE`** (default 25). Oban's queues can now hold all 25 connections at peak, which
  leaves none for the control plane; 35 is a reasonable start. See `docs/configuration.md`.
- **Two new Postgres migrations build indexes `CONCURRENTLY`** (`token_version_changed_at`, the
  retention index). Run `mix ecto.migrate` as usual; they do not lock the `shards` table.
- **Client-visible changes:**
  - A failover open that hears from too few replicas now answers a retryable **503
    `FILO_RECOVERING`** for up to 60 s (`REPLICATION_RECOVERY_UNDERQUORUM_WINDOW_MS`) instead of
    opening from an older stored object.
  - `REGEXP` with lookaround or backreferences refuses subjects over 64 KiB; patterns over 64 KiB
    are refused outright.
  - Tenant-facing error messages no longer include internal reasons; details go to the server log.
- **Failover cost:** taking over a shard under 6 MiB now re-uploads it once (a nonce re-PUT that
  guarantees a never-seen etag); larger shards pay nothing extra. Measured on the chaos rig: 100
  tenants of 5 MiB, RTO p50 15.7 s / p99 18.3 s, bound by the lease TTL.
- **The replication protocol gained a reply code** (torn replica). Upgrade all nodes together;
  an old node's replies are still read.

From the 2026-10-14 performance round and the bugs its rig run found:

- **Fixed a failover data-loss bug.** A write acknowledged while a shard's followers were being
  re-seeded (the first write after an idle drop or handoff) left every replica at "unknown" WAL
  ordinal, so failover recovery skipped them and the write was lost if the node died before its
  next push. Seeds now carry the ordinal. This adds a seed frame to the replication protocol behind
  the existing ordinal-wire gate; upgrade all nodes together.
- **Remote flushes work with `NODE_KEY`.** A flush, export, fork or `flush: true` snapshot sent to a
  node that does not hold the shard used to wait 15 s and fail when `NODE_KEY` differed from the
  node name.
- **Idle database handles now stay open 250 ms** (`POOL_DRAIN_GRACE_MS`, 0 restores the old
  close-at-once behaviour), capped at `POOL_IDLE_BUDGET` (8,192 per node). One-off requests are
  about 65% faster (439 → 150 µs); each recently used shard can hold ~170 KB longer.
- **New env knobs:** `RECONCILE_WATERMARK` (1000) and `RECONCILE_ROLLOUT_BUDGET_MS` (10 min) — the
  hourly migration sweep now enqueues up to a backlog watermark instead of a fixed 100.
- **Erasure re-checks decay:** a deleted tenant's storage is probed 6 times over 30 days (1 h, 6 h,
  24 h, 7 d, 29 d) instead of hourly; an object that reappears late is caught at the next probe.
- **Three more Postgres migrations** (`purge_verified_at` column and index; dropping two unused
  `shards` indexes), all `CONCURRENTLY`.
- **Measured on the chaos rig** (1024 tenants, replication on, 30 s flush): 2,400 → 3,340 txn/s,
  p99 1.5 s → 0.6 s, errors 50-116 → 2.

### Changed

- **Filo now comes from Hex** (`{:filo, "~> 0.2.0"}`) instead of a `path: "../filo"`
  dep. A clone of fathom no longer needs filo checked out as a sibling — `mix deps.get`
  is enough. Pinned to the patch range, not `~> 0.2`: filo is pre-1.0, where the minor
  number carries breaking changes.

  Co-developing both repos still works without editing `mix.exs` — set `FILO_PATH`:

      FILO_PATH=../filo mix test

- **CI dropped the sibling-checkout scaffolding.** It used to check fathom out into a
  `fathom/` subdirectory solely so it could clone the private filo repo alongside it
  via a `FILO_ACCESS_TOKEN` PAT. Now a plain root checkout with no second repo and no
  PAT. **That secret can be deleted from the repo settings.**

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

### Notes

- **CI stays disabled** until the repos go public. The account is out of Actions minutes
  for private repos, so a run fails in ~12 s having executed zero steps. `mix precommit`
  is the only enforcement until launch. See `AGENTS.md`.

### Performance

Measured, with the reports in `docs/reviews/`. Highlights: the drain path dropped
from ~3.5× to ~2.3× S3 RTT (a redundant lock GET removed); a per-connection statement
cache (+13–16% on held streams); inbound WebSocket frames are no longer double-scanned;
large cursor responses no longer spool to disk; idle streams hibernate and the idle
timeout is configurable; the warm follower's steady-state refresh cost is bounded; the
Postgres pool is sized against actual demand; and the admin dashboard stopped counting
the whole shards table on every keystroke.

## [0.2.0] - 2026-07-16

Expert-panel review hardening (the 2026-07-14 audit pass; report kept locally, not in the repo) — the
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
