# Fathom — Shard Migration Engine Plan

> Status: **E1–E5 implemented (2026-06-28).** The per-shard blue/green migration
> engine is built end-to-end: capture → version registry → rollout/lazy →
> drain/retain/copy-replay/cutover → retirement → revert, all green under
> `mix precommit`. The remaining gap is validating against a real `django-libsql`
> client (item #1, blocked on Python ≥3.10) and the deferred optimizations noted
> per-phase. The strategy lives in [`migration-plan.md`](migration-plan.md).

## The model (resolved)

The schema is **operator-owned but authored through Django**, and Django's
migration workflow is **not changed**. A migration flows like this:

1. Django runs `migrate` normally against the **reserved template shard** (one
   connection — *not* per tenant; a configured shard id such as `template`,
   addressed like any shard). To Django this is just a database: it reads
   `django_migrations`, emits DDL (and its `INSERT INTO django_migrations`
   bookkeeping), commits.
2. Fathom, serving the template shard, **transparently captures the emitted SQL**
   and records it as the next fleet version N — **auto-detected, no operator
   action**: a committed transaction that wrote `django_migrations` is one
   migration and becomes one version. The template file is left fully migrated, and
   **new tenant shards are forked from it** so they are born at HEAD.
3. An Oban rollout migrates the fleet to N in the background by **replaying the
   captured SQL** against each shard (blue/green into a new version), then cutting
   each shard over independently.

Replaying Django's *exact* emitted SQL — including the `django_migrations`
inserts — makes every shard end up byte-for-byte as if Django had migrated it
directly, so each shard's Django bookkeeping stays consistent for free. There are
no hand-written transforms: **the transform for version N is the captured SQL.**

Applying DDL to a million 4MB shards synchronously at deploy time is a non-starter,
so the rollout is **soak (background, ahead of the app deploy) + lazy
(migrate-then-serve on access)**.

## Guiding principle

Keep the data path decoupled from the control plane. The engine coordinates
through the **lease**, not through new directory reads on the hot path (the one
deliberate exception: the lazy migrate-then-serve check on `checkout`).

## Handling writes during a rollout (the crux, resolved)

Django is **monolithically on one version**: once it is on vN, *every* statement —
reads included — assumes the vN schema (`SELECT new_col` fails on a vN-1 shard
just like an `INSERT` does). So there is no "serve vN-1 and tolerate it" once the
app is vN. That collapses the whole problem to one rule:

> **The version bump flips a contract. From the instant a shard's traffic is vN,
> any access to a shard still at < N migrates it first, then serves the request.**

- **Lazy (migrate-then-serve):** on `checkout`, if `schema_version < HEAD`, migrate
  the shard inline and **block the request** (~tens of ms for 4MB) until it is vN,
  then serve. The write *is* the trigger — there is never a window where a vN write
  lands on a vN-1 shard and is mishandled. Concurrent requests to the same shard
  serialize on the lease (migrator holds it → others wait) and de-dup via Oban
  `unique`.
- **Soak (pre-deploy sweep):** the background sweep migrates the fleet to N
  *before* the app cuts to vN, so few requests pay the first-touch latency and the
  deploy doesn't cause a thundering herd. **Constraint:** during the soak, vN-1 app
  code writes to vN shards, so soak only works for **backward-compatible
  (additive)** migrations — new nullable columns/tables. Destructive changes
  (drop/rename) can't soak (vN-1 code would break on vN); they need expand→contract
  across two releases, or they skip the soak and accept the post-deploy herd via
  the lazy path. This is standard online-migration discipline.

## Design decisions (resolved)

1. **Storage model: single live key + retained versioned copies.** Live stays
   `<shard_id>.db` (data path never reads the directory to find a file); the
   migrator keeps the old version as `<shard_id>@vN-1.db` for the retention window.
2. **Drain via the lease.** The migrator holds the shard's lease across the copy;
   a client `checkout` sees `{:held, "migrator@..."}` and waits/retries. Reuses #2
   fencing; no new hot-path coupling.
3. **Transform = captured Django SQL** (replayed verbatim per shard), not authored
   modules. Stored with the version in the registry.
4. **Copy engine: exqlite.** Replays the SQL through `exqlite`, same as the live
   path. **Resolves exqlite-vs-`ecto_libsql` in favor of exqlite** — the
   `Fathom.ShardRepo`/`ecto_libsql` path was removed 2026-07-06 (see
   `docs/cluster-architecture.md`); `Fathom.Shard.Connection` is the engine swap-point
   if a client ever needs the libSQL engine.
5. **Retention: a scheduled Oban job, no new table.** On cutover, enqueue
   `RetirementJob(shard_id, vN-1)` at `now + 7d`; an S3 lifecycle rule backstops
   orphans. The directory's `retire`/`retain_until` columns stay for the separate
   "retire a whole shard" lifecycle (tenant deletion).
6. **Lazy migration blocks inline; the sweep runs async.** (See "Handling writes".)
7. **Capture: auto-detect only.** Fathom turns each template-shard transaction that
   wrote `django_migrations` into a version — no explicit release step, so a deploy
   stays just `migrate`.
8. **Schema source: a reserved template shard.** A configured shard id Django
   migrates against; capture is enabled there, it is never served as a tenant, and
   new tenant shards are forked from its file at HEAD.

## Storage additions (Local + S3, MinIO-tested like #3)

- `retain(shard_id, version)` — copy live `<shard_id>.db` → `<shard_id>@version.db`
  (server-side `x-amz-copy-source` on S3; `File.cp` on Local).
- `restore(shard_id, version)` — copy `<shard_id>@version.db` → live (for revert).
- `drop_version(shard_id, version)` — delete `<shard_id>@version.db` (retirement).

## Registry change

`shard_migrations` gains a `statements` column (the captured SQL batch for the
version). `Fathom.Migrator.release/3` becomes `(version, name, statements)`; HEAD
is still `max(version)`. Version stamped on each migrated shard as
`PRAGMA user_version = N` (O(1) gate + crash-recovery detection); Django's own
`django_migrations` is maintained by the replay, so no separate `_fathom_migrations`
table is needed.

## The per-shard algorithm (`Fathom.Migrator.ShardMigrationJob`, Oban, unique per `shard_id`)

1. `Directory.mark_migrating(shard_id)`
2. `Fathom.Shards.drain(shard_id)` — refuse new checkouts, let in-flight
   connections check in (bounded by a timeout; on timeout it resumes serving and
   the migration reschedules — no force-close, no torn flush), then flush the
   latest data to the live object, drop local, and stop (releasing the lease). This
   fixes the "coordinator only flushes at conns==0" gap.
3. `Storage.acquire_lease(shard_id, "migrator@<node>", ttl)` + **renew it for the
   copy's duration**; `{:held, _}` → reschedule.
4. `Storage.retain(shard_id, N-1)` — keep the old version for revert.
5. pull live → `old.db`; `new.db` = copy of `old.db` + replay version-N's captured
   SQL (in a transaction); stamp `PRAGMA user_version = N`.
6. validate — **migration-defined** (default "source rows == dest rows" for a
   straight copy; restructuring migrations override, since splits/merges change
   counts).
7. `Storage.flush(shard_id, new.db)` — the live key now holds vN.
8. `Directory.cutover(shard_id, N)` — `schema_version = N`, `status = active`.
9. enqueue `RetirementJob(shard_id, N-1)` at `now + 7d`; release the lease.

**Failure:** retry with backoff; on exhaustion `Directory.mark_failed` + release
the lease. Cutover (8) never ran, so live is still vN-1 and the shard is unharmed.

**Crash recovery (between 7 and 8):** live = vN but directory = vN-1. The re-run is
*not* a no-op — it detects live `user_version == N` and **completes forward** (cutover
+ schedule retirement); it does not re-copy or roll back.

### Revert (N → N-1), within the 7-day window

For each shard at `schema_version = N`: drain → `Storage.restore(shard_id, N-1)`
(`@vN-1` → live) → `Directory.cutover(shard_id, N-1)` → cancel the pending
`RetirementJob`. Only possible while `@vN-1` is still retained.

### Retirement

`RetirementJob(shard_id, version)` (scheduled at `now + 7d`):
`Storage.drop_version(shard_id, version)`. S3 lifecycle backstops orphans.

## Phased build order (each its own commit, TDD, `mix precommit` gate)

- **E1 — Storage versioned ops.** `retain`/`restore`/`drop_version` + Local tests +
  a MinIO `:s3` test (server-side copy). **Done.**
- **E2 — Drain.** `Fathom.Shards.drain/2` (refuse new checkouts, drain in-flight,
  flush + drop + release lease + stop; resume + reschedule on timeout);
  checkout-refused-while-draining. Tests against the real coordinator. **Done.**
- **E3 — Capture + replay. Done.** `shard_migrations.statements` +
  `Fathom.Migrator.Copy` (copy + replay + stamp `user_version`); `Fathom.Migrator.Capture`
  records a version when the template shard's `django_migrations` count rises on
  `COMMIT`, wired through `Fathom.ShardExecutor` (handle tagged with shard_id;
  `:template_shard_id` config). Tests: replay of a real Django-style DDL batch +
  the capture state machine + a migrate-stream integration. *Follow-ups:*
  fork-new-shard-from-template (an optimization — lazy migrate-then-serve already
  brings a v0 shard to HEAD on first use); and validating the exact django-libsql
  transaction framing against a real client (depends on item #1, Python ≥3.10).
- **E4 — The job. Done.** `Fathom.Migrator.ShardMigration.run/2` (Oban-free core:
  drain → migration lease → retain → copy/replay → flush → cutover, with revert +
  crash-forward); Oban added (dep + jobs migration + supervision + config);
  `ShardMigrationJob` (unique per shard, wraps `run/2`, snooze on busy, retry →
  `Directory.mark_failed` quarantine, schedules retirement) + `RetirementJob`
  (drop the retained version). Tests: end-to-end migrate+revert+crash-forward over
  real storage, plus the job's success/retirement/snooze/quarantine paths.
- **E5 — Rollout. Done.** `Fathom.Migrator.rollout/1` (enqueue laggard jobs) +
  `ReconcileJob` cron (re-run the sweep) + fleet `revert/2` (+ `RevertJob`) + lazy
  migrate-then-serve on `checkout` (config-gated `:lazy_migrate`, block inline).
  Tests: rollout/reconcile/revert enqueue, `RevertJob` end-to-end, and lazy
  migrate-then-serve (plus the gated-off negative). *Deferred:*
  fork-new-shard-from-template (new shards born at HEAD) and PubSub
  rollout-progress for a console; retention is the scheduled `RetirementJob` + S3
  lifecycle (no separate cron).

## Risks / accepted tradeoffs

- Draining waits for in-flight connections; on timeout it resumes and the
  migration reschedules (no force-close, no torn flush). A perpetually-busy
  behind-HEAD shard relies on the lazy migrate-then-serve path to make progress.
- ~2× storage during the 7-day retention window → accepted (decided 2026-06-25).
- Soak requires backward-compatible (additive) migrations; destructive changes need
  expand→contract or the post-deploy lazy herd.
- The migration lease owner must be distinguishable from node owners
  (`"migrator@<node>"`) so fencing reads cleanly.

## Known limitations (documented, out of scope)

- **Django data migrations (`RunPython`) are not handled.** They are Python, never
  reach fathom as replayable SQL, and per-shard data transforms aren't captured.
  The operator handles data migrations separately. DDL (and ORM-emitted DML that
  *does* cross the wire) is covered; arbitrary `RunPython` is not.
- **Only transaction-wrapped (atomic) migrations are captured.** The boundary is a
  `BEGIN…COMMIT` whose `django_migrations` count rose. Non-atomic migrations (no
  enclosing transaction) aren't captured; Django defaults migrations to atomic.

## Implementation notes (decisions resolved; care needed)

- **Reserved template shard** (config, e.g. `:template_shard_id`): capture-enabled,
  never served as a tenant, and the fork source for new shards. Runtime tenant
  traffic routes per-subdomain as today; Django's `migrate` config points at the
  template — routing, not a Django-code change.
- **Boundary detection** runs on the template connection: track transaction
  begin/commit and flag commits that wrote `django_migrations`. Edge cases to
  handle: Django wraps each migration in its own transaction by default (→ one
  version each); `migrate --fake` writes `django_migrations` with no DDL (→ a
  no-op/empty version that must be tolerated); squashed migrations; and a
  non-atomic migration still ends with a `django_migrations` write.
</content>
