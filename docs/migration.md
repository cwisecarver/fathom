# Fathom — schema migration (how the built engine works)

> Status: **BUILT.** This describes the migration engine as it exists in code today
> (`lib/fathom/migrator/*`, `Fathom.Directory`, the Oban jobs). The design/decision
> drafts are `docs/migration-plan.md` and `docs/migration-engine-plan.md`; this file is
> the "how it actually works + how it behaves mid-rollout" reference.

## The problem sharding creates

Every tenant is its own SQLite file. A schema change is not one `ALTER TABLE` — it is the *same*
change applied to (eventually) millions of separate databases, **most of which aren't open** at any
moment (they're cold in `Fathom.Shard.Storage`). So the engine has to: never stop the world, never
require opening every shard at once, converge the cold tail on its own, and keep a working undo.

## The version stamp lives in three places

The same schema version `N` is recorded three times, each for a different job:

| Where | What it is | Why |
|---|---|---|
| `_fathom_migrations` table, inside each shard | the migrations that shard has applied | **source of truth** |
| `PRAGMA user_version`, inside each shard | the integer `N` | **O(1) gate** — "is this shard at vN?" without reading a table (`Migrator.ShardMigration` skips a shard whose `user_version` already equals the target — the migration is idempotent) |
| `shards.schema_version`, in Postgres (`Fathom.Directory`) | the version the fleet thinks the shard is at | **find who's behind without opening shards** — laggard queries over millions of tenants are a Postgres index scan, not millions of file opens |

That third stamp is load-bearing: it's what makes "which of my millions of shards still need migrating?" a cheap question (`Fathom.Directory.laggards/2`).

## Blue/green, per shard — a pointer flip, never in-place

The migration copies to a **fresh file** and flips a pointer; it never mutates a live shard in place.

1. **`Migrator.Capture`** records a template's migrations as a fleet **version** (vN);
   **`Migrator.Release`** publishes the fleet **HEAD** (the target version), cached by
   `Migrator.HeadCache`.
2. Per shard, **`Migrator.Copy`** replays vN's captured SQL into a fresh file **in a single
   transaction** and stamps `PRAGMA user_version = N` **after** commit — so a failure rolls the
   whole thing back and leaves the shard untouched at vN-1. **`Migrator.ShardMigration`**
   orchestrates it and is **idempotent** (re-run on an already-migrated shard is a no-op).
3. Validate, then **`Fathom.Directory.cutover/2`** flips the shard's pointer to vN and stamps
   `cutover_at` (the same instant as `last_active_at`, so "written since cutover" is exactly
   `last_active_at > cutover_at`). The old vN-1 version is **retired with a `retain_until`** — kept
   for the revert window, not deleted.

Because it's copy-then-flip on a fresh file, a shard being read *during* its migration never sees a
half-applied schema.

## Oban jobs drive it, and the cold tail converges on its own

- **`ShardMigrationJob`** (unique per shard) migrates one shard.
- **`ReconcileJob`** (hourly cron) is the key to *millions*: it asks the directory for laggards
  (`schema_version < HEAD`) and enqueues them. You never push to millions of shards — a tenant that
  was asleep during the rollout gets migrated the next time reconcile sweeps (or when it next
  wakes). The tail **converges**.
- **`RetirementJob`** drops retained old versions past their `retain_until`.
- **`RevertJob`** flips the fleet pointer back (see below).

Directory lifecycle `status` runs `active → migrating → retired` (or `migration_failed`).

## Cross-version tolerance — the fleet is mixed mid-rollout

During a rollout the fleet is a **mix**: some tenants are still vN-1, some are vN, at the same
wall-clock time. This is the subtle part, because **fathom is a SQL proxy over opaque client SQL** —
it forwards the app's queries to a shard; it does **not** and cannot rewrite them to bridge schema
versions. So the burden of tolerating the mix is split:

**A request hits one tenant at one version, so a query never sees a mixed schema.** The mixing is
*across* tenants, not within a query — tenant A's request runs against vN, tenant B's against vN-1.
What that means for the app: **the same code path must work against both schema versions**, because
it can't control which tenant the next request is for.

**The engine's contract that makes this safe: migrations are backward-compatible (expand-contract /
parallel-change).** A released vN is a **superset** old code still reads. The rollout is:

1. **Expand** — release vN that only *adds* (new tables, nullable/defaulted columns); never rename
   or drop in the same step. Migrate the whole fleet; `ReconcileJob` converges the tail.
2. **Observe** — `Fathom.Directory.laggards(HEAD, _)` tells you exactly when **zero** shards are
   behind, i.e. the whole fleet is at vN. The mixed window is now closed.
3. **Then** deploy app code that assumes vN, and only **later** release a `vN+1` **contract**
   migration that drops the now-unused old columns — safe because no shard and no code needs them.

fathom's job in all this is to keep the mixed window **bounded** (reconcile converges it) and
**observable** (`schema_version` + `laggards` say precisely when it's closed), so you know when each
step above is safe. The discipline is enforced: every migration ships a **cross-version-tolerance
test** proving the app reads both vN-1 and vN. A migration the running app *can't* tolerate
mid-rollout (a rename/drop with no expand-contract dance) is, by definition, **not done**.

## Undo is a guarded pointer flip

**`RevertJob`** flips the fleet pointer back to vN-1 (which is why vN-1 was retained). It **backs up
the live vN first**, and it has a **write-age force-guard**: if the directory shows the shard was
*written since its `cutover_at`* (`last_active_at > cutover_at`), reverting would silently discard
those post-cutover writes, so the job **cancels deterministically** unless `force: true` confirms
you are throwing them away. A revert must be a pointer flip *within the retention window* — past
`retain_until` the old version is gone and there's nothing to flip back to.

## The gate (a migration isn't done without these)

1. **Forward** — seed a vN-1 shard, run the copy+transform, validate vN by row counts / checksums.
2. **Revert** — the pointer-flip back to vN-1 works.
3. **Cross-version tolerance** — the app reads a mixed fleet (both vN-1 and vN).

A migration that can't be reverted by pointer-flip within the retention window, or that the running
app can't tolerate mid-rollout, is not done.

## One-line summary

Capture a version → copy each shard into a fresh file and flip a Postgres-tracked pointer (retiring
the old version for a retain window) → let an hourly reconcile sweep converge the cold tail → keep
migrations backward-compatible so the app tolerates the mixed fleet until `laggards` hits zero →
revert, if needed, is a guarded flip back to the retained version.
