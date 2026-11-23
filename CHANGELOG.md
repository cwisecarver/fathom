# Changelog

All notable changes to fathom are recorded here. Format follows
[Keep a Changelog](https://keepachangelog.com/); versions are [SemVer](https://semver.org/).

**fathom ↔ Filo compatibility.** Filo (the Hrana wire server) has been a Hex dependency since
2026-07-29, pinned to a patch range (`~> 0.4.4`) because Filo is pre-1.0 and its minor number
carries breaking changes. Mix enforces the pin; the table records what each release was built
against. To co-develop both repos, set `FILO_PATH=../filo` (no `mix.exs` edit).

| fathom | Filo | Notes |
|---|---|---|
| unreleased | **0.4.4** (Hex, `~> 0.4.4`) | Same pin as 0.4.0. |
| 0.4.0 | **0.4.4** (Hex, `~> 0.4.4`) | Needs 0.4.x: the coordinator drain calls `Filo.Socket.drain/1` and `Filo.Stream.drain/1` (new in 0.4.0). 0.4.4 bounds protobuf varints to 10 bytes and range-checks int64 on decode, before authorization (expert review 2026-11-07 #7). Mix will not resolve 0.5.0. |
| 0.3.0 | **0.2.0** (path dep) | Requires Filo 0.2.0 or later: fathom uses `Filo.Client` (the chaos-rig driver) and the `:authorize` seam (`Fathom.HranaAuth`), neither of which exists in Filo 0.1.0. |
| 0.2.0 | 0.1.0 | Filo path dep; HTTP v1/v2/v3 (+cursor) + WebSocket hrana1/2/3. |
| 0.1.0 | 0.1.0 | Initial working slice. |

## [Unreleased]

### Performance

- The admin collector's 10 s hot-shard pass no longer re-derives its top-20 heap's size and minimum for every shard, and `ShardLoad` is walked with `:ets.foldl` instead of `tab2list` + `Enum.map`: `observability_tick_us` 14.4 → 11.2 ms at 20k shards (perf audit 2026-11-19 #34/#35; a watch-only, worst-case arm where every one of the 20k rows is active, so a real node gains less). The audit's other micro-optimizations were measured and dropped (each moved its metric by 2% or less); see `docs/component-notes.md`.
- `pool_budget_full` and `close_chain.depth`, emitted inside the shard coordinator on every budget-full checkin and every close, moved from `TelemetryMetricsPrometheus.Core` to `FastCounter`/`FastHistogram`: with 18 processes on one event name, 36.4 and 151.9 µs per event became 1.0 and 0.6 µs (throwaway probe; no gated bench arm reaches this path). The `/metrics` text is unchanged apart from the float `_sum` the Fast reporters already print (perf audit 2026-11-19 #20).
- A statement whose first `;` is followed only by whitespace skips the multi-statement scanner: a ~1 KB SELECT with a trailing `;` went from 32.3-32.7 µs to 3.9-4.0 µs through `ShardExecutor.execute/2`, the same as without the `;` (three rounds each, throwaway probe; no gated bench arm shows it, its statements are 34 bytes). Any other tail still takes the unchanged scanner (perf audit 2026-11-19 #2).

## [0.4.0] - 2026-11-19

Live WAL replication (A2) became the production default, Jepsen joined the chaos rig, and roughly
two dozen expert-review rounds hardened the lease, the fence, the tenant sandbox and the control
plane. 1,497 non-merge commits since 0.3.0: about 115 features, 657 fixes, 181 performance changes.

### Highlights

- **A2 quorum replication is on by default in prod.** A tenant commit is acknowledged only after
  `REPLICATION_QUORUM` (2) followers hold its WAL frames, and a failover open promotes the freshest
  replica, from its own followers or from any peer, over the stored S3 object. Node-loss RPO no
  longer depends on the flush interval. See [`docs/a2-quorum-replication.md`](docs/a2-quorum-replication.md),
  [`docs/reviews/chaos-replication-matrix-2026-08-12.md`](docs/reviews/chaos-replication-matrix-2026-08-12.md)
  and [`docs/reviews/quorum-simultaneous-loss-2026-08-13.md`](docs/reviews/quorum-simultaneous-loss-2026-08-13.md).
- **Jepsen found 11 bugs, all fixed, each with a regression test:** nine that could lose or corrupt
  acknowledged data and two liveness bugs where a shard never took a write again after the faults
  healed ([`jepsen-2026-10-29.md`](docs/reviews/jepsen-2026-10-29.md)). A later run on a follower ring
  lost writes again and was fixed. Since then the Tier 0 push gate (fault-free run plus three
  10-minute hard mixes with power loss, kill, pause, isolate, repl-cut and s3-cut) has been
  Elle-valid and liveness-valid. Reports:
  [`jepsen-2026-10-24`](docs/reviews/jepsen-2026-10-24.md), [`-10-27`](docs/reviews/jepsen-2026-10-27.md),
  [`-10-29`](docs/reviews/jepsen-2026-10-29.md), [`-11-09`](docs/reviews/jepsen-2026-11-09.md).
- **Replicating throughput on the chaos rig:** 1024 tenants with replication on ran 4,343 txn/s
  with 0 errors and p99 385 ms ([`tpc-fleet-repl-on-2026-10-30.md`](docs/reviews/tpc-fleet-repl-on-2026-10-30.md)).
  The rig is CPU-bound at about 4.2-4.3k txn/s ([`tpc-fleet-ceiling-2026-11-11.md`](docs/reviews/tpc-fleet-ceiling-2026-11-11.md)).
  These are five nodes on one VM; quote them as relative numbers.
- **Lock and lineage forensics.** An open whose replica lineage collides with the owner's now
  **refuses** (a retryable 503) instead of serving the older object. A 412'd release counts as
  "ours" only for the same owner, epoch and claim. `SHARD_ACQUIRE_TRACE=true|debug` logs every lock
  acquire, release, marker and rollback, and `jepsen/fathom/forensics/` adds the scanner and
  timeline tools. See [`docs/runbooks/lock-lineage-forensics.md`](docs/runbooks/lock-lineage-forensics.md).
  The Jepsen `hard-1` anomaly that started this work has no known cause yet.
- **Tenant sandbox and control-plane security.** Closed a cross-tenant `ATTACH` that gave any
  authenticated tenant read and write access to every co-resident shard (affects 0.3.0). Also
  fixed several bypasses of the statement and PRAGMA gates, closed a `/api` CSRF hole, and added an
  engine-level PRAGMA authorizer. See **Security**.
- **Density and per-request cost.** Connection pooling is on in prod, statements are cached by
  byte budget, and SQLite's initial page-cache slab shrank to 4 pages (`served_kb_per_shard` -27%,
  [`tuning-audit-2026-11-11.md`](docs/reviews/tuning-audit-2026-11-11.md)). Shard objects are stored
  as zstd, and idle followers absorb their last WAL generation, which took a replica store from
  1,323.6 MB of `-wal` to none (same report).
- **Django compatibility.** A Rust loadable SQLite extension (`native/fathom_udf`) supplies 35 of
  the 54 functions Django's SQLite backend registers client-side. Prod refuses to boot without it.

### Upgrade notes — read before deploying

Most dangerous first. Every boot refusal below is prod-only unless it says otherwise.

- **Cut over the whole fleet at once. A rolling upgrade from 0.3.0 is not supported, and there is
  no downgrade.** fathom is greenfield (`AGENTS.md`), and this release changes three things a
  mixed fleet would share:
  - **Stored objects are zstd by default** (`SHARD_OBJECT_ENCODING=zstd`). A 0.3.0 node refuses
    any marker but zlib (`:unknown_object_encoding`, fail closed). Setting
    `SHARD_OBJECT_ENCODING=none` does not help, because a 0.4.0 steal-touch of a small object
    always re-encodes it as zstd.
  - **The S3 lock body gained fields** (`claim`, `released_by`, `touch_pending`, `prev_owner`,
    `touched_from`/`touched_to`, `dirty`), and objects gained lineage and position metadata
    (`x-amz-meta-fathom-lineage`, `-pos`, `-sha256`, `-enc`). A 0.3.0 node reads a 0.4.0 lock, but
    when it rewrites one it drops the lineage claim the next owner needs.
  - **A 0.3.0 node has no replication listener**, so a 0.4.0 node that ships to it cannot reach a
    quorum, and every write fails with `FILO_NO_QUORUM`.

  Drain and stop every 0.3.0 node (`Fathom.Shards.drain_all/1`, or a graceful SIGTERM), run the
  migrations, then start 0.4.0 everywhere. A later rolling upgrade across a replication
  frame-format change needs the staged wire order in `docs/a2-quorum-replication.md`.
- **Replication is on by default in prod, and an unconfigured node refuses to boot.** Prod defaults
  `REPLICATION_LISTEN`, `REPLICATION_ENABLED`, `REPLICATION_ORDINAL_WIRE`, `REPLICATION_SIGN_FRAMES`
  and `REPLICATION_HMAC_REQUIRED` to on. Each node needs:
  - `REPLICATION_FOLLOWERS`: more entries than `REPLICATION_QUORUM` (default 2, so at least 3).
    Empty, malformed, or `Q >= N` refuses to boot.
  - `REPLICATION_BIND_IP` set to a private interface. Unset or a wildcard refuses to boot, because
    the port (`REPLICATION_LISTEN_PORT`, default 9100) carries raw tenant bytes. Open it between
    fleet nodes only.
  - A frame-auth secret, `REPLICATION_HMAC_SECRET` (falls back to `HRANA_TOKEN_SECRET`) of
    **at least 32 bytes**, the same on every node. A missing or short secret refuses to boot, as
    does requiring HMAC without signing.
  - One of `REPLICATION_RECOVERY_PEERS` (every other node), `REPLICATION_RECOVERY_SYMMETRIC=true`
    (followers already are every other node) or `REPLICATION_MEMBERSHIP=roster`. Without one, the
    node refuses to boot, because recovery over a follower subset lost acked writes in
    [`jepsen-2026-10-27`](docs/reviews/jepsen-2026-10-27.md).
  - `REPLICATION_DIR` outside `SHARD_DATA_DIR`. Equal or nested refuses to boot. Same volume only
    warns, but a separate block device is recommended: the replica store grows from other nodes'
    writes.
  - `SHARD_STORAGE=s3`. A prod node on the (default) Local store refuses to boot while replicating,
    unless `ALLOW_LOCAL_STORAGE=true`.
  - The `fathom_udf` extension built and loadable. With replication on there is no opt-out
    (`ALLOW_NO_UDF` applies only with `REPLICATION_ENABLED=false`).

  To run a single node without replication: `REPLICATION_ENABLED=false` and `REPLICATION_LISTEN=false`.
- **The prod flush interval default is now 300 s** (`SHARD_FLUSH_INTERVAL_MS=300000`; the library,
  dev and test keep 5 s). With replication on, the quorum bounds node-loss RPO, and a short interval
  costs replication throughput. At 512 tenants, 5 s vs 30 s was 22,599 errors vs 4 and +49% throughput
  ([`a2-flush-interval-2026-08-18.md`](docs/reviews/a2-flush-interval-2026-08-18.md)), and prod warns
  below 30 s. **With `REPLICATION_ENABLED=false` the node-loss window is now up to 5 minutes**:
  set the interval explicitly if you relied on 0.3.0's 5 s. `0` or negative warns (idle-only
  durability).
- **More prod boot refusals:**
  - `HRANA_AUTH` must be set explicitly, to `required` or `disabled`.
  - Weak admin credentials: a password under 16 bytes or equal to `ADMIN_USER` is refused unless
    `ADMIN_WEAK_CREDENTIALS_OK=true`. A blank value, or only one of the pair, is refused with no
    opt-out. Both unset boots with the dashboard off (503).
  - The admin/API throttles are on but `TRUSTED_PROXIES` is empty: every client would share one
    lockout bucket. Set it, or `ADMIN_THROTTLE_SHARED_BUCKET_OK=true` when nothing proxies `:4000`.
  - A Repo pool (`POOL_SIZE` × pool count) at or under the sum of Oban's queue limits. The
    `POOL_SIZE` default is now computed as queue sum + 10 (35 today), so leave it unset unless you
    have measured.
  - The S3 boot self-test now also probes conditional DELETE (a stale `If-Match` must get 412), the
    missing-key fenced PUT (412, or 404 with `NoSuchKey`), the multipart touch primitives and
    `s3:DeleteObject` on `*.lock` and `heartbeats/*`. A store that ignores conditional DELETE (MinIO)
    refuses to boot; `SHARD_STORAGE_ALLOW_WEAK_CONDITIONAL_DELETE=true` exists for test rigs only.
  - Stricter env parsing: booleans must be `true`/`false`/`1`/`0`, and integers are trimmed, treat
    empty as unset and refuse negatives. Also refused: `SHARD_LEASE_TTL_MS` outside 5000-600000,
    `MAX_OPEN_SHARDS` ≤ 0, two `LB_BACKENDS` keys that render to the same nginx upstream, an invalid
    `TEMPLATE_SHARD_ID`, and an unknown value for any enum knob (`SHARD_OBJECT_ENCODING`,
    `HRANA_REVOCATION_ON_ERROR`, `MIGRATION_LEDGER_CHECK`, `REPLICATION_PROCESS_PRIORITY`,
    `OTEL_TRACES_SAMPLER_RATIO`, `QUERY_CONSOLE_ENDPOINT`).
- **S3 credentials and bucket:**
  - Grant `s3:DeleteObject` on `<prefix>*.lock`, `<prefix>heartbeats/*` and the keys of erased
    shards.
  - Grant `s3:ListBucketMultipartUploads` and `s3:AbortMultipartUpload` (tenant purge aborts
    leftover uploads).
  - Add an `AbortIncompleteMultipartUpload` lifecycle rule (1 day).

  An `AWS_SESSION_TOKEN` now warns at boot: temporary credentials are cached and never refreshed.
  See `docs/durability.md` (least-privilege node credentials) and `docs/single-writer.md`.
- **Build requirements:**
  - Erlang/OTP 28 or newer: `mix.exs` refuses to build on OTP 27 because zstd needs OTP 28, and the
    images moved to Elixir 1.20.4 / OTP 28.5.
  - A Rust toolchain to build `native/fathom_udf` (both Dockerfiles install it). Without cargo the
    build skips the extension and a prod node then refuses to boot.
  - Filo comes from Hex (`~> 0.4.4`).
- **20 new Postgres migrations.** Run `mix ecto.migrate` before starting 0.4.0. Most build their
  indexes `CONCURRENTLY`. Two do not:
  - `20260805044751_add_last_snapshot_at_to_shards`
  - `20261009044407_add_last_retention_at_to_shards`

  Each builds a partial index on `shards` and blocks writes to that table while it builds, so run
  them in a quiet window if the directory is large. They also add the `hrana_token_issuances`
  table, a unique index on `oban_jobs`, and new `shards` / `releases` / `rebalancer_nodes` columns,
  and drop two unused `shards` indexes.
- **Changed prod defaults worth knowing:**
  - Connection pooling is on, `POOL_DRAIN_GRACE_MS` is 2000 (was 250 when introduced) and
    `POOL_IDLE_BUDGET` is 8,192 handles.
  - `SHARD_IDLE_MS` is 300 s.
  - `QUERY_TIMEOUT_MS` is 30 s (a statement past it is cancelled).
  - `MAX_CHECKOUTS_PER_SHARD` is 256.
  - `SHARD_SOFT_HEAP_LIMIT_BYTES` is 8 GiB node-wide. Lower it on nodes under ~16 GiB.
  - `JOURNAL_SIZE_LIMIT` is 64 MiB.
  - `SHARD_MAX_BYTES` is 4 GiB, a byte-denominated size cap; `SHARD_MAX_PAGE_COUNT` still wins
    when set.
  - `SHARD_LOAD` and `SYSTEM_MONITOR` are on.
  - `MIGRATION_LEDGER_CHECK=enforce`: a shard whose `django_migrations` ledger cannot be reconciled
    is refused and quarantined.
  - `DRAIN_ON_SHUTDOWN` is on.
  - Replication itself uses 32 follower workers per link, compressed pushes and
    `REPLICATION_TIMEOUT_MS` 5 s.
- **Size the stop grace for the drain:** stop grace ≥ `DRAIN_LB_SETTLE_MS` + `DRAIN_ALL_BUDGET_MS` +
  `SHARD_SHUTDOWN_MS` + 90 s, which is 205 s at the defaults. See `docs/runbooks/deploy.md`.
- **Size `nofile` for replication.** Each replicated shard holds `-wal` and `-shm` read fds, and
  each follower worker holds up to `REPLICATION_HELD_WAL_FDS` (16) replica WALs, which comes to
  2,048 fds on a 5-node fleet. The boot log prints the per-shard fd multiplier.
- **Host tuning.** Prod warns when the effective `vm.dirty_background` threshold is large. On the
  rig, `vm.dirty_background_bytes=1GB` took p99 from 598 to 417 ms, while 256 MB and 64 MB were
  worse ([`tpc-fleet-repl-on-2026-10-26.md`](docs/reviews/tpc-fleet-repl-on-2026-10-26.md)).
  Measure on your own hardware. Prod also warns when SQLite's temp dir (`SQLITE_TMPDIR`) shares a
  volume with the shard or replica data.
- **Client-visible changes:**
  - A write that cannot reach its quorum within `REPLICATION_TIMEOUT_MS` answers a retryable **503
    `FILO_NO_QUORUM`**.
  - A failover open that hears from too few replicas answers a retryable **503 `FILO_RECOVERING`**
    for up to 60 s (`REPLICATION_RECOVERY_UNDERQUORUM_WINDOW_MS`) instead of opening from an older
    stored object. A lineage collision refuses the open the same way.
  - A stream open past the per-shard checkout cap answers **503 `FILO_SHARD_BUSY`**.
  - Statements are cancelled at 30 s in prod.
  - `REGEXP` refuses patterns over 64 KiB, subjects over 64 KiB on the backtracking path
    (lookaround, backreferences), and any subject over 256 KiB.
  - Tenant-facing error messages are fixed strings; details go to the server log.
  - State-changing `/api` calls authenticated by BasicAuth (not a Bearer key) must send
    `Content-Type: application/json`, and a `Sec-Fetch-Site: cross-site` request is refused.
  - Filo 0.4 closes a WebSocket that has not said `hello` within 10 s, or that idles inside a
    transaction for 30 s.
  - BLOB cells that happen to be valid UTF-8 now go out as Hrana blobs, not text.
- **Failover cost:** taking over a shard under 6 MiB re-uploads it once (a re-nonced PUT that
  guarantees an etag the key has never had). Larger shards pay nothing extra. With replication on,
  the rig's failover took 17.3 s to the first acked write
  ([`rig-validation-2026-11-07.md`](docs/reviews/rig-validation-2026-11-07.md)).
- **Removed environment variables:** `WARM_FOLLOWER`, `WARM_CACHE_DIR`, `WARM_POLL_MS`,
  `WARM_REFRESH_BYTES_PER_S`, `WARM_MIN_REPULL_MS` and `WARM_HOME_RETENTION_MS` (warm standby is
  gone), `DNS_CLUSTER_QUERY`, and `MAILGUN_API_KEY`/`MAILGUN_DOMAIN`. fathom ignores them now; the
  images no longer create `/data/warm`.
- **CI:** the `FILO_ACCESS_TOKEN` repository secret is no longer used and can be deleted.

### Added

**Replication (A2)**

- **Replicate-before-ack:** the WAL frame seam (via `sqlite3_wal_hook` in the loadable extension),
  the follower listener and shipper, quorum fan-out, streamed seeds that never install partially,
  per-follower catch-up, and promote-on-open with peer recovery (`Fathom.Shard.Replication.*`).
- **A monotone per-lineage WAL ordinal** on the wire, on the object's position stamp and on the
  replica. Freshness ranks on it, not on SQLite's checkpoint counter. A takeover claims a lineage
  above its predecessor and records it in the lock.
- **Frame authentication:** HMAC-signed frames bound to a per-connection nonce, with MACs on seed
  payloads (`REPLICATION_SIGN_FRAMES`, `REPLICATION_HMAC_REQUIRED`, `REPLICATION_CONN_NONCE`). See
  `docs/runbooks/replication-frame-auth.md`.
- **Membership from a roster** (`REPLICATION_MEMBERSHIP=roster`, `REPLICATION_ADVERTISE_HOST`), with
  the static list as the floor. A set smaller than quorum+1 is refused.
- **Quiet absorb** (`REPLICATION_QUIET_ABSORB_MS`, default 2 × `SHARD_IDLE_MS`): a follower folds a
  silent replica's last WAL generation into its `.db`.
- **Back-pressure:** free-space and inode floors on follower seeds (`REPLICATION_DISK_FREE_FLOOR_BYTES`),
  and a node byte budget for pushes.
- **Knobs:** `REPLICATION_PROCESS_PRIORITY` (`normal|hubs|all`), `REPLICATION_DEFERRED_WAL_SYNC`
  (off), and `REPLICATION_REPLY_MARGIN_MS`.
- **Telemetry:** commit ack-latency histogram, lag report, saturation as an alertable signal, and
  under-quorum recovery events. A takeover logs its recovery decision and every peer's answer.

**Shard data path and storage**

- **Connection pooling** with a statement cache that travels with the pooled handle, a drain
  grace, a node idle budget (`POOL_IDLE_BUDGET`) and `PRAGMA shrink_memory` on idle handles
  (`POOL_SHRINK_IDLE_MS`).
- **Local `.db` retention after a clean drop** under a node disk budget (`SHARD_RETAIN_LOCAL_BYTES`,
  off by default; `SHARD_RETAIN_MIN_BYTES`).
- **A node-wide SQLite soft heap limit** (`SHARD_SOFT_HEAP_LIMIT_BYTES`), a per-shard stream cap
  (`MAX_CHECKOUTS_PER_SHARD`), and a WAL size bound (`JOURNAL_SIZE_LIMIT`).
- **A background pre-evictor** below `MAX_OPEN_SHARDS` (`PRE_EVICT_HEADROOM`), and one admission
  per cold shard, shared by concurrent first requests.
- **Scan gates:** a separate `BackgroundScanGate` (`BACKGROUND_SCAN_PERMITS`) for non-flush
  whole-shard scans, and a scan-only flush permit apart from the upload cap.
- **zstd shard objects** (`SHARD_OBJECT_ENCODING=zstd|zlib|none`; any marked object always
  decodes), SHA-256 pull verification with an MD5 fallback, and a boot-probed CRC32 transit checksum
  on compressed PUTs.
- **`mix fathom.lock.repair`** for an undecodable lock (both backends fail closed on one), and
  **`mix fathom.shard pull --version-id`** to recover a bucket version.

**Lease, fencing and forensics**

- **`SHARD_ACQUIRE_TRACE=true|debug`**: `lock-trace` lines for every S3 lock acquire branch,
  release, marker and steal-rollback, the lineage each open chose and each `release_decision`.
  `debug` adds the coordinator timeline and the store's answer to every lock write and delete. Off
  by default; the chaos rig sets `debug`.
- **An always-on `lineage collision context` warning** with the lease, replica and object stamp.
- **`jepsen/fathom/`:** a Jepsen 0.3.15 / Elle test against the chaos rig, with power-loss
  (LazyFS), kill-all and liveness checks. **`jepsen/fathom/forensics/`** adds `lineage_scan.py`,
  `shard_timeline.py`, `dump_lock_versions.sh` and `lineage-hunt.sh`.

**Migration engine**

- **A per-shard data-migration seam** (`Fathom.Migrator.Transform`, allow-listed via
  `:migration_transforms`), legible review blocks and an `atomic=False` preflight.
- **Django ledger checks by name** before and after every replay (`MIGRATION_LEDGER_CHECK`).
- **`mix fathom.check_migrations`**, a CI gate that refuses Django migrations capture cannot see.
- **Visibility:** rollout rate and ETA in `status/0`, a stalled-migration signal
  (`MIGRATION_STALL_AFTER_MS`), three-place stamp-drift reporting, and a warning when a release's
  `template@HEAD` fork source is stale.
- **Fleet revert lands on chain-jumped shards.** The hourly reconcile now enqueues to a backlog
  watermark (`RECONCILE_WATERMARK` 1000, `RECONCILE_ROLLOUT_BUDGET_MS` 10 min) instead of a fixed
  100.

**Tenants, auth and API**

- **A token issuance ledger and fleet-wide time-scoped revocation** (`Fathom.HranaAuth.{Ledger,RevokeJob}`).
  Token expiry is enforced per statement, and revocation reaches already-open connections.
- **`HRANA_REVOCATION_ON_ERROR=fail_open|fail_closed`** and a bounded, single-flight cold-`hello`
  storage-floor read (`HRANA_STORAGE_FLOOR_BUDGET_MS`, default 3000). Under `fail_closed`, a
  storage-floor *error* on a cold shard now refuses instead of reading as floor 0. See `docs/auth.md`.
- **Scheduled snapshots with GFS retention** (`SNAPSHOT_SCHEDULE_SAMPLE`, `SNAPSHOT_RETENTION`,
  `SNAPSHOT_RETENTION_SAMPLE`; off by default; only `auto` snapshots ever expire).
- **A full restore drill** that rehearses fork → open → row-count compare
  (`RESTORE_DRILL_FULL_SAMPLE`) and checks the `django_migrations` ledger against `user_version`.
- **The Django UDF extension:** 35 functions with `re.search` parity for `REGEXP`, loaded per
  connection (enable → load → disable) and registered once per node as an auto-extension.

**Observability and admin**

- **New `/admin` pages:** Cluster (lease health, fence verdicts, lease events), Durability
  (commit-ack latency, replication lag, backup and DR), Rebalancer, per-shard and Audit. Also new:
  Errors, data-plane auth, RPO and Oban panels, a node-health strip, and a freshness chip on every
  panel.
- **Host signals:** dirty-writeback gauges, `:system_monitor` long-schedule and long-GC counters
  (`SYSTEM_MONITOR`), an opt-in fsync probe (`FSYNC_PROBE`), inode gauges, and a memory breakdown
  with allocator carriers and OS RSS.
- **Telemetry:** failure-branch events (query errors, no-quorum, auth rejects, S3 status class),
  route-tagged flush durations, `OTEL_TRACES_SAMPLER_RATIO`, and 30 events that previously reached
  no reporter.

**Rig and tooling**

- **Chaos-rig arms:** `repl-latency` / `REPL_VIA_TOXI` (cross-AZ and cross-region), `quorum-loss`,
  `tpc-sparse`, a gp3-capped disk and the prod auth posture (`docs/runbooks/rig-arms.md`).
- **Bench metrics:** replication-on write round trip, libSQL SDK request shapes,
  Django-sized memory, and failover promote RTO.
- **Typing:** a Dialyzer gate inside `mix precommit` (scoped to `lib/`).

### Changed

- **Filo moved to Hex** (`{:filo, "~> 0.4.4"}`), up from a `path: "../filo"` dep. A clone needs
  only `mix deps.get`, and `FILO_PATH` overrides it for co-development. CI dropped the
  sibling-checkout scaffolding.
- **A coordinator drain asks its Filo holders to close** (`Filo.Stream.drain/1`,
  `Filo.Socket.drain/1`), so an idle WebSocket or HTTP baton stream no longer holds a migration or
  node drain open for its whole window.
- **Remote flushes work with `NODE_KEY`.** A flush, export, fork or `flush: true` snapshot sent to a
  node that does not hold the shard used to wait 15 s and fail when `NODE_KEY` differed from the
  node name.
- **Erasure re-checks decay.** A deleted tenant's storage is re-probed at 1 h, 6 h, 24 h, 7 d and
  29 d (at most six LISTs per deletion) instead of every hour, and a probe that finds leftovers
  keeps it due hourly.
- **The flush snapshot uses `sqlite3_serialize` up to 16 MiB** (`FLUSH_SERIALIZE_MAX_BYTES`)
  instead of `VACUUM INTO`, and a coordinator's first flush tick is drawn over the whole interval
  (`SHARD_FLUSH_JITTER_RATIO`).
- **`docs/reviews/` holds measured run reports only.** Expert-panel audits live in the gitignored
  `audits/` directory, and the committed 2026-07 review files were removed.
- **Internal restructuring, no behaviour change:**
  - The lease and fence envelope stays in `Fathom.Shard`, but `Position`, `Provenance`,
    `Integrity`, `Fork`, `Materializer`, `PromoteOnOpen`, `Snapshot` and `FlushInput` were
    extracted from it.
  - The boot guards moved to `Fathom.BootChecks`.
  - The scan and seed gates share `Fathom.PermitGate`.
  - Companion-file deletes go through `Fathom.FS.rm_db/1`.

### Fixed

**Replication and recovery** (most were found by Jepsen or the chaos rig)

- **A brand-new shard lost quorum-acked writes** when its owner died before the first periodic
  flush.
- **A takeover served the older object over an acked replica.** This happened when replica
  promotion failed on an S3 error, or when the takeover trusted its own warm local copy after
  stealing from a live node.
- **A follower that installed a timed-out seed late** absorbed a reset many WAL generations ahead,
  was promoted, and lost the acked writes in between.
- **A write that failed its quorum stayed readable** on its node until it vanished.
- **A takeover asks every other node** for a newer copy, not only its own followers (a ring lost
  acked writes). Recovery's responder floor no longer counts the writer or a rebooted follower.
- **A write acked while followers were being re-seeded** (the first write after an idle drop or
  handoff) left every replica at an unknown WAL ordinal, so failover skipped them. Seeds now carry
  the ordinal.
- **A checkpoint no longer erases committed frames** the Session has not shipped. A tenant commit
  is never acked on a blind wal-index or an empty follower set.
- **Execution state comes from SQLite:** whether a transaction is open is read from SQLite itself,
  so no write is acked unshipped, and two bypasses of the commit-ack ownership gate are closed.
- **A lost reply no longer leaves a follower "busy" forever**, which had refused writes with every
  follower up. A failed post-lapse ownership re-check is now retried, so a clean shard no longer
  refuses writes (`:ownership_unconfirmed`) for good ([`jepsen-2026-10-29`](docs/reviews/jepsen-2026-10-29.md)).
- **The shipper feedback loop that OOM-killed nodes at 1024 tenants is closed**: the delta is
  capped and the bytes are bounded
  ([`a2-feedback-loop-fixed-2026-08-17.md`](docs/reviews/a2-feedback-loop-fixed-2026-08-17.md)).
- **Torn replicas are sticky and unpromotable** until re-seeded, including across a follower
  restart or reboot. A seed from a lower lineage is refused.

**Lease and fencing**

- **Two nodes acked writes to one shard** after an S3 outage longer than the steal window (a lapse
  revalidation now renews instead of reading). Two owners also appeared when node clocks lagged the
  store's; the steal is now clock-free.
- **A stale release resolves to "ours" only for the same owner, epoch and claim.** An older lease
  can no longer clobber a newer lock on a store that enforces conditional DELETE.
- **An open whose replica lineage collides with the owner's refuses** (retryable) and keeps the
  replica's lineage as a claim, instead of serving the older object.
- **`release_lease` reported a 412 as success**, which left a lease that outlived its coordinator
  and could never be stolen (about 1 in 300 rollout steps,
  [`fleet-rollout-2026-08-04.md`](docs/reviews/fleet-rollout-2026-08-04.md)).
- **Steals:** an ambiguous steal lock-PUT now carries its own marker; a failed steal-touch's
  rollback keeps the steal watch; and every steal-touch reaches an etag the key never had.
- **A deposed owner self-fences** when its lock was deleted, because AWS's conditional-write
  statuses (404 `NoSuchKey`, 412) map correctly. A cold open whose lease acquire failed gives back
  the lock it may have written.
- **An S3 flush with nil lineage erased the shard's lineage.** A non-stamping release now keeps the
  takeover's lineage claim.

**Shard data path and durability**

- **The self-fence and corrupt-local quarantines move `-wal`/`-shm` before the `.db`**, all or
  nothing, and directories are fsynced after renames.
- **Fence bypasses:** a leaked write fence is lifted at open, and a leaked flush-gate slot no longer
  stops all flushing on the node. A hung flush PUT no longer holds a slot.
- **A landed-but-unacked PUT stays this node's own** after many retries, and a sidecar that lags
  its own landed PUT adopts it instead of quarantining.
- **Cold pulls retry transient GET statuses with backoff.** A zero-length body is never promoted
  as an empty database, and an encoded object without a plaintext digest is refused.
- **Leaks:** pooled SQLite handles can no longer leak to a dead coordinator, and a configure failure
  no longer leaks the handle. Abandoned pull temps and shard-sized temps outside the data dir are
  reaped.

**Migration engine**

- **Capture fixes:** capture reads transaction statements the way the executor does, and stops
  recording Django's introspection as migration DDL.
- **No split fleet or resurrection:** attaching a transform to a rolled-out version no longer
  splits the fleet, and migration state writes no longer resurrect a suspended or deleted tenant.
- **Migrate and revert are deferred** while an A2 replica is ahead of the object.
- **No leaked copies:** a discarded retirement job no longer leaks its retained copy, and a
  successful revert drops its restore source.

**Tenants, auth and API**

- **Delete is thorough:** it revokes the tenant's tokens, erases local files and A2 replicas on
  every node, and never installs a seed of a deleted tenant. The token-floor mirror is monotonic.
- **Fork, export and restore guards:** fork and export refuse a deleted source during the erase
  window, fork re-checks both tombstones after the copy, and export lands in a private `0700`
  directory.
- **The throttles:** the "per-IP" control-plane throttles shared one bucket behind a proxy. They
  now count an IPv6 /64 as one source, failed Bearer keys count toward a per-IP lockout, and
  `/admin/metrics` has its own bucket.
- **Audit and console:** audit rows record the resolved client IP and the real operator, and the
  query console audits before executing and connects to the interface Hrana is bound to
  (`QUERY_CONSOLE_ENDPOINT`).
- **Snapshots:** retention could delete every snapshot, manual ones included. A restore now holds
  the lease for the whole copy-back.

**Rig and tooling**

- **The eval stack (`deploy/compose`) boots again**, and the canonical image builds from a plain
  clone.
- **Measurement traps fixed:** the TPC driver's unbounded `Task.async_stream` hung a 1024-tenant
  sweep, `soak` counted recoverable rows as lost, and `quorum-loss` waited for "writable" instead of
  "whole".

### Performance

Measured on the chaos rig (five nodes, one 12-vCPU VM) or by `mix fathom.bench`. The reports are in
`docs/reviews/`.

- **32 follower workers per link (was 8).** Commits that skipped a follower fell from 4.1-6.2% to
  0.5-0.6%, and average p99 from 430 to 411 ms
  ([`tpc-fleet-repl-on-2026-10-26.md`](docs/reviews/tpc-fleet-repl-on-2026-10-26.md)).
- **`POOL_DRAIN_GRACE_MS` 250 → 2000 ms.** With sparse tenants, 250 ms gave a second-round p99 of
  3.4-4.0 s; 1100-5000 ms held about 6 ms ([`rig-arms-2026-10-31.md`](docs/reviews/rig-arms-2026-10-31.md)).
- **SQLite's initial page-cache slab is 4 pages.** `served_kb_per_shard` dropped 27% (201.6 →
  146.6 KB), and the measured throughput change was within noise
  ([`tuning-audit-2026-11-11.md`](docs/reviews/tuning-audit-2026-11-11.md)).
- **Quiet absorb** emptied a follower's 1,323.6 MB of `-wal` within 180 s of the primaries going
  quiet (same report).
- **A cross-region follower set costs one round trip per write and nothing else:** +30 ms RTT gave
  p99 +30.6 ms at 500 txn/s ([`cross-region-quorum-2026-10-30.md`](docs/reviews/cross-region-quorum-2026-10-30.md)).
- **Losing the largest survivable number of followers at once costs nothing measurable, and one
  more fails closed in tens of milliseconds**
  ([`quorum-simultaneous-loss-2026-08-13.md`](docs/reviews/quorum-simultaneous-loss-2026-08-13.md)).
- **Schema rollout runs about 46,000 shards/hour across 3 nodes**
  ([`fleet-rollout-2026-08-04.md`](docs/reviews/fleet-rollout-2026-08-04.md)).
- **Hot-path trimming (unmeasured individually):**
  - The checkout folds the pool take into one coordinator round trip.
  - One `fathom_conn_state()` NIF step per write.
  - Prebuilt column wire forms come from the statement cache.
  - Lock-free histograms, counters and per-shard load counters.
  - Touches are coalesced, and a busy shard's re-touch is written at most every 30 s.
  - WebSocket processes hibernate.
  - File ops bypass `file_server_2`.
  - Followers keep each shard's WAL open between pushes.
  - Group commit for queued same-WAL commits.
  - One object HEAD per replicating cold open.
  - Drain fan-out is 64 coordinators at a time.

### Security

- **Cross-tenant `ATTACH` (affects 0.3.0).** One `ATTACH DATABASE '<data_dir>/<victim>.db'` from any
  authenticated stream gave read and write access to every co-resident shard, bypassing the `:ro`
  scope, the write fence and the lease. Tenant handles now deny attach and detach at the SQLite
  authorizer. Upgrade any deployment that serves untrusted tenants.
- **Statement and PRAGMA gate bypasses closed:**
  - CTE-prefixed DML and scripts slipped past the write fence.
  - Whitespace around the qualifier dot, and a leading BOM, bypassed the gates.
  - `describe` ran no gate at all.
  - A `:ro` handle on a shard with no local file was read-write.
  - `fts3_tokenizer` is now denied.

  An engine-level PRAGMA authorizer backstops the text gate, and a tenant `wal_checkpoint` is
  limited to PASSIVE.
- **Suspend and delete now reach streams that were already open.**
- **`/api` CSRF:** a cross-site form could suspend, rotate or restore a tenant through cached
  BasicAuth credentials. Cross-site tenant export is refused.
- **The replication port** is authenticated (HMAC frames, per-connection nonce) and must bind a
  private interface in prod.
- **A Kelvin-sign Host** no longer resolves to a shard the LB did not route (ASCII-only zone match).
- **Credentials:** creating, forking into or minting a token for the capture template needs the
  `destroy` scope, and console-minted credentials are scoped, attributed and time-boxed.
- **Filo 0.4.4** bounds protobuf varints to 10 bytes, so an oversized unauthenticated body can no
  longer pin decoder CPU before authorization, and it range-checks int64 on decode.
- **The UDF extension** uses checked arithmetic and pre-allocation size caps, and the `REGEXP`
  compile, subject and cache limits bound its CPU and memory.

### Removed

- **`Fathom.Shard.WarmFollower`** (warm standby, A1). A2 replication superseded it on 2026-09-14,
  and a survivor now recovers by promote-on-open. `docs/warm-standby.md` is a tombstone.
- **Unused Phoenix scaffolding:** `Fathom.Mailer` and Swoosh, the DNSCluster wiring (every PubSub
  broadcast is node-local), and the stock home page.
- **The committed expert-review reports** under `docs/reviews/` (now kept in the gitignored
  `audits/`).

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
