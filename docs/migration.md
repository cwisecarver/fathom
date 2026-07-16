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

### Data migrations are gated for review (`requires_review`)

Django DDL is safe to replay verbatim onto every shard; a **data** migration is not. A RunPython
backfill (`for u in User.objects.all(): u.slug = slugify(u.name); u.save()`) crosses the wire as
template-literal `INSERT/UPDATE/DELETE` carrying the *template's* row values — replayed onto every
tenant it overwrites their data (or, from an empty template, silently backfills nothing) while all
three stamps say "applied" (expert review #1). So capture **lints** each version's buffer: any DML
on a table other than `django_migrations` flags the release `requires_review: true`. The version is
still *recorded* (refusing would fork the template from the fleet — #19), but a flagged version
**caps the fleet HEAD below it** (`Migrator.head/0`), so the rollout never replays that DML until an
operator reviews it. `Migrator.pending_review/0` lists flagged versions; `Migrator.approve_review/1`
clears the flag after the operator confirms it's safe (or supplies a real per-shard data-migration
hook), and HEAD then advances. A flagged version blocks everything above it too (the linear graph
has no skip).

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

### Reverting a bad Django migration — the full loop (the template gotcha)

The fathom-side pointer flip is only half the story (expert review #32). A fleet revert yanks vN and
flips tenants back to vN-1, but the **template** shard still has vN applied — in its schema *and* its
`django_migrations` table. Django's migration graph is **linear**: the next `makemigrations` builds
on vN, so the next captured version emits DDL that assumes schema the fleet reverted away from, and
its fleet-wide replay fails (the #6 fork). So a revert isn't done until the template is walked back
too:

1. **Yank + fleet revert** (fathom side): `Fathom.Migrator.revert(vN, vN-1)` (or the `RevertJob`)
   yanks vN, backs up the live vN, and flips active tenants to vN-1 (within the retention window,
   write-age guard as above). HEAD is now vN-1.
2. **Backwards-migrate the template** (Django side): `manage.py migrate <app> <prev>` against the
   template shard, walking its `django_migrations` back to vN-1. Capture **ignores** a backwards
   migrate for the fleet (it only alarms — fleet undo is the fathom revert above), so this is
   harmless post-yank and is exactly the step that realigns the template with HEAD.
3. **Author the fixed migration**: `makemigrations` now builds on vN-1 (the aligned template), so the
   corrected migration and its captured version assume the schema the fleet actually has.
4. **Re-release** it forward as vN+1 through the normal rollout.

**The consistency check.** `Fathom.Migrator.template_drift/0` catches a skipped step 2: it compares
the template's captured `django_migrations` count (recorded per release) against HEAD's, and reports
`{:drift, _}` when a yanked version above HEAD was captured with a higher count — i.e. the template
is left ahead of the live fleet. `yank/1` runs it automatically and, on drift, emits
`[:fathom, :migrator, :template_drift]` telemetry + a loud `Logger.error` naming the step-2 fix, so a
wedged pipeline is an alert, not a surprise at the next `makemigrations`. (Releases captured before
this feature carry no count and report `:unknown` rather than false-alarming.)

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
