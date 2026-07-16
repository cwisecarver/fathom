# Changelog

All notable changes to fathom are recorded here. Format follows
[Keep a Changelog](https://keepachangelog.com/); versions are [SemVer](https://semver.org/).

**fathom ↔ Filo compatibility.** Filo (the Hrana wire server) is a path dependency
(`{:filo, path: "../filo"}`) and is not published yet (expert-review #9), so each fathom release
records the Filo version it was built against. Pin both repos to the listed pair in CI.

| fathom | Filo | Notes |
|---|---|---|
| 0.2.0 | 0.1.0 | Filo path dep; HTTP v1/v2/v3 (+cursor) + WebSocket hrana1/2/3. |
| 0.1.0 | 0.1.0 | Initial working slice. |

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
