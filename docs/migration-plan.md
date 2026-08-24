# Fathom — Shard Schema Migration Plan

> Status: **SUPERSEDED DRAFT — history, not a work list.** This was the design for
> evolving the schema across millions of per-shard libSQL shards. **The engine it
> describes is built**: read [`migration.md`](migration.md) for what actually ships,
> and treat this file as the reasoning that led there.
>
> **Both `DECIDE` markers below are answered** (annotated in place, 2026-08-24). The
> only question left is an operator budget call, not a design one — see the second
> one. Nothing here is waiting on a decision.

## Reality check — this plan vs. the current code

**Written 2026-06-27, and its premise has since expired.** It said the plan
"predates the working spike and assumes an orchestration layer that **does not exist
yet**: no Postgres directory, no `Fathom.Directory` resolve, no `Fathom.Migrator` /
`ShardMigrationJob` / `Fathom.Retirement`, no Oban."

**All of that now exists and ships** (`Fathom.Directory`, `Fathom.Migrator.*`,
Oban-driven rollout with cold-tail reconcile and guarded revert — see the Migration
engine row in AGENTS.md § Project). The one name that never landed is
`Fathom.Retirement`; retirement is `Fathom.Migrator.RetirementJob`, scheduled with the
cutover. Kept rather than deleted because the notes below still explain *why* the built
shape differs from the plan's — which is the part a reader needs.

- **Shards are local SQLite *files*, not Turso *namespaces*.** The spike is
  `Fathom.Shard` (one `exqlite` GenServer per shard, file `<shard_id>.db`) +
  `Fathom.Shards` (find-or-start). So "`template@vN` / `shard@vN` namespace"
  becomes concretely a **versioned file** (e.g. a fresh `<shard_id>@vN.db`), and
  the "directory pointer" is a row mapping `shard_id → current file/version`. The
  namespace framing only returns if real Turso namespaces or a remote-shard path
  are adopted (the `Fathom.ShardRepo` / `ecto_libsql` scaffold for that was removed
  2026-07-06 — data path is exqlite; see `docs/cluster-architecture.md`).
- **The copy runs against *local* shards, not remote Hrana.** Filo (the Hrana
  server, `../filo`) is Fathom's **client-facing** protocol now — it is *not* the
  migration copy mechanism. The copy/transform reads the old shard and writes the
  new one through the local shard layer (`Fathom.Shard` / `Fathom.Shards`), so the
  "ETL over Hrana (remote libSQL)" decision below does not apply to single-node,
  file-backed shards.
- **Naming collision.** The *built* Filo executor is `Fathom.ShardExecutor`. This
  plan's migration-copy helper is named `Fathom.ShardExec` — rename it (e.g.
  `Fathom.Migrator.Copy`) so the two aren't confused.
- **No resolve hook yet.** Current routing is Host subdomain →
  `Fathom.ShardExecutor.shard_from_conn` → `Fathom.Shards`; there is no
  `Directory.resolve` to hang lazy/on-use migration on. The lazy path lands with
  the directory/control-plane.

## Chosen approach: blue/green copy per shard

Migrate one shard at a time by **building a fresh database at the new schema
and copying the data in**, then flipping the directory pointer. Keep the old
database around for a retention window so revert is a pointer flip, and let S3
lifecycle rules age it out.

Per shard, to go from schema `vN-1` → `vN`:

1. **Create the new shard** `shard@vN` on the **same node**, forked from that
   node's `template@vN` namespace (which already has the HEAD schema). Empty,
   correct schema, instant.
2. **Pause** the old shard briefly — mark the shard `migrating` in the
   directory so the app holds/queues writes for the copy window.

   > **DECIDE: consistency — ANSWERED, this is what shipped.**
   > `Directory.mark_migrating/1` sets the row for the copy window and
   > `unmark_migrating/1` clears it, conditionally on the row still being
   > `migrating`. A crash mid-chain used to leave it stuck forever (every
   > laggard/reconcile query filters on status), so `reclaim_stale_migrating/1`
   > reclaims it — see `docs/migration.md`. The serialization that actually
   > protects the copy is not the flag but the **migrator lease**: a per-operation
   > owner plus a linked renewer holds the lock for the whole copy, and a
   > `check_lease` self-fence aborts before the flush/restore clobber points.


3. **Copy + transform** data from `shard@vN-1` → `shard@vN`. The migration
   defines the transform (straight copy, column adds, table splits, type changes —
   anything, because it's a fresh schema).
4. **Validate** — row counts / checksums match expectations before cutover.
5. **Cut over atomically** — update the directory: `shard → shard@vN`,
   `schema_version = N`, `status = active`; invalidate the resolve cache (PubSub).
   New connections land on `vN`.
6. **Retire the old shard** — mark `shard@vN-1` `retired`, set
   `retain_until = now + X days`. Its S3 data ages out via lifecycle policy.

### Revert path (the whole point)

Within **X days**, if `vN` is bad: flip the directory back
(`shard → shard@vN-1`, `schema_version = N-1`, `status = active`),
invalidate cache, delete/abandon the bad `vN` shard. No reverse migration, no data
surgery — just a pointer flip while the old DB still exists on S3.

After X days the old shard is gone (S3 lifecycle) and revert for that migration is
no longer possible — by design.

## Why this fits Fathom specifically

- **Shards are small** (one shard). Full copy costs ~ms–seconds, so the usual
  "never copy a big DB" objection doesn't apply.
- **Restructuring in one step.** Because you copy into a fresh schema, you can do
  renames / splits / type changes in a single migration — no multi-step
  expand/contract dance *within a shard*.
- **Old DB = free backup + revert artifact**, aged out automatically.
- **Atomic per-shard cutover.** A shard is always wholly `vN-1` or wholly `vN`;
  never half-migrated.

## What this does NOT solve (be honest)

- **Cross-shard version skew still exists.** The fleet cuts over gradually, so
  during a rollout some shards are `vN-1` and some `vN`. **The app must tolerate
  both versions during the rollout window** — either version-aware (reads
  `schema_version` and branches) or by keeping `vN` a superset the old code can
  still use. Copy-per-shard removes in-shard half-states and gives easy revert; it
  does not remove app-level cross-version tolerance.
- **Storage doubles for the retention window.** Every migrated shard holds
  `vN-1` + `vN` for X days. At millions of small shards that's ~2× storage for the
  rolling window.

  > **DECIDE: S3 budget — no longer an ARCHITECTURAL decision; it is a knob and an
  > operator call.** The built engine does not keep two live directory rows. It
  > retains the previous **storage object** and schedules its retirement in the same
  > transaction as the cutover (`cutover_with_retirement/3` →
  > `Fathom.Migrator.RetirementJob`), and `shards.retain_until` is a per-shard,
  > operator-editable timestamp (admin directory UI, restricted changeset). So the
  > window is set per shard rather than as a fleet constant, and the 2× is a
  > *ceiling* during a rollout rather than a standing cost.
  >
  > What is genuinely still unanswered is the **number**: nobody has priced a real
  > rollout against a real bucket. That is a measurement, and it belongs with the
  > bare-metal run (`docs/a2-bare-metal-plan.md`), not with this design.

## Reused from the orchestration design (these still apply)

- **Version stamp, three places:** `django_migrations` table inside each shard
  (Django's own migration ledger — the truth/audit) + `PRAGMA user_version` (O(1)
  gate) + `shards.schema_version` in Postgres (laggard queries without opening
  shards).
- **Templates first:** migrate every node's `template@vN` *before* sweeping, so
  newly-created shards are born current and never need migrating.
- **Hybrid rollout:**
  - **Lazy (active set):** on `Directory.resolve`, if `schema_version < N`, enqueue
    a priority migration for that shard. Hot shards migrate themselves on use.
  - **Background sweep (cold tail):** Oban fan-out over
    `WHERE schema_version < HEAD ORDER BY last_active_at DESC`, **rate-limited per
    node** (each migration wakes/creates DBs → memory), off-peak, throttled by node
    load. Self-resuming (the laggard query is the cursor); a cron reconcile sweep
    runs forever so even never-touched shards converge and drift self-heals.
- **Failure isolation + quarantine:** a shard whose copy/validate fails retries
  with backoff; on exhaustion → `status = migration_failed`, surfaced in the
  console. The rollout keeps converging everyone else.
- **Per-shard uniqueness:** Oban `unique` on `shard_id` so the lazy path and
  the sweep never migrate the same shard twice concurrently.

## Decisions (resolved 2026-06-25)

- **Retention window: 7 days.** Old shard kept 7 days after cutover; revert is
  possible only within that window. Tradeoff accepted: ~2× storage on the rolling
  7-day window.
- **Consistency during copy: pause briefly.** Mark the shard `migrating`; the
  app holds/queues writes for the (ms-scale) copy, then resumes on the new shard.
- **Copy mechanism: app-level ETL over Hrana (primary); ATTACH only as an optional,
  risky optimization.** Verified: cross-namespace `ATTACH` in libSQL/Turso is
  **read-only** (directionally fine for copy — read old, write new) but is
  **deprecated** (blocked for new users; self-hosted status unclear), gated behind
  an attach-permission token + same-group, capped at 10 attachments, and
  transaction-only. So Fathom reads the old shard and writes the new one
  (`Fathom.Migrator.Copy`, née `Fathom.ShardExec`). Bytes flow through Fathom —
  fine at shard size. Use ATTACH only if a specific self-hosted `sqld` build is
  pinned and the deprecation risk is accepted. *(Note: "over Hrana / remote
  libSQL" assumes remote shards; the current shards are local files, so the copy
  runs through `Fathom.Shards` locally — see Reality check. The `ecto_libsql` dep
  that scaffolded this was removed 2026-07-06 as unused; a remote path would
  re-add a libSQL driver.)*
- **Retirement: Fathom delete job + S3 lifecycle backstop.** An Oban cron deletes
  namespaces past `retain_until`; an S3 lifecycle rule backstops any orphans the
  job misses.
- **Cutover: hard flip + reconnect.** Flip the directory immediately; existing
  old-shard connections finish or fail and clients reconnect to the new shard
  (clean because writes are already paused).

## Tradeoffs vs. in-place migration

| | Blue/green copy (this plan) | In-place `ALTER` |
|---|---|---|
| Revert | Pointer flip within X days | Reverse migration (hard) |
| Per-shard state | Atomic; never half | Half-applied risk (mitigate w/ txn) |
| Restructuring (rename/split) | One step (transform on copy) | Multi-step expand/contract |
| Storage | ~2× during window | 1× |
| Copy cost | Yes (cheap for small shards) | None |
| Needs app cross-version tolerance during rollout | **Yes** | **Yes** |

## Walkthrough: ship migration N

1. `Fathom.Migrator.release(N, …)` — record migration; HEAD = N.
2. Migrate every node's `template@vN` (new shards born current).
3. Sweep coordinator pages laggards (`schema_version < N`, prioritized by
   `last_active_at`), enqueues per-shard jobs under per-node concurrency caps.
4. Lazy path: `resolve` of a stale shard enqueues a priority migration
   (non-blocking — app is cross-version tolerant).
5. `ShardMigrationJob` (unique per shard): create `@vN` from template →
   drain → copy+transform → validate → flip directory → retire `@vN-1` with
   `retain_until`. Retry/quarantine on failure.
6. Reconcile cron runs until `count(schema_version < HEAD) == 0`. Console shows
   live progress per node via PubSub.
7. **If bad:** flip shards back to `@vN-1` (within X days); investigate; the
   bad `@vN` shards are deleted.

## Components to build (Elixir)

- `Fathom.Migrator` — `release/2`, `rollout/1` (sweep), `reconcile/0` (cron),
  `revert/1` (flip a set back to N-1).
- `Fathom.Migrator.ShardMigrationJob` (Oban) — the per-shard copy+flip+retire,
  unique on `shard_id`, retry → quarantine.
- `Fathom.Migrator.Copy` (renamed from `Fathom.ShardExec` to avoid colliding with
  the built `Fathom.ShardExecutor`) — run the copy/transform + stamp
  `user_version`. For the current local file-backed shards this goes through
  `Fathom.Shards` directly; the "over Hrana (remote libSQL)" form only
  applies if remote shards are adopted (see Reality check).
- `Fathom.Directory` — `schema_version`, `live_namespace`, `retired_namespace`,
  `retain_until`, `migration_status`; atomic cutover + cache invalidation.
- `Fathom.Retirement` (Oban cron) — drop/let-expire `retired` shards past
  `retain_until` (if not using S3 lifecycle).
- Postgres: `schema_migrations(version, up, transform, checksum, inserted_at)`;
  directory columns above.
</content>
