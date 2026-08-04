# Running Django migrations on fathom (operator guide)

Schema change is the most frequent thing a Django shop does, and on fathom it works through the
**template shard**: you migrate one designated shard with Django as usual, fathom **captures** that
migration as a fleet version, and the rollout replays it onto every tenant blue/green. This is the
adopter-facing procedure (expert review #25). The internal mechanics are
[`migration.md`](migration.md); the Django connection setup is [`quickstart-django.md`](quickstart-django.md).

The whole loop: **capture → release → converge → (revert if needed)**.

## Prerequisite: an authenticated template shard

Capture is off by default and is a **poisoning vector** — whatever SQL reaches the template becomes
fleet DDL — so in production the template must be reachable **only by you**:

- Set `:template_shard_id` (e.g. `template`) on the nodes that should capture. Never make it equal
  `:default_shard`, and never leave it anonymously reachable (`Fathom.Application` refuses a prod
  boot where the default shard is the template).
- Turn on `HRANA_AUTH=required` and mint a token for the template: `mix fathom.token template`.
  Point a **dedicated** Django project (your "migrations" project) at it:

  ```python
  DATABASES = {"default": {
      "ENGINE": "libsql.db.backends.sqlite3",
      "NAME": "wss://template.fathom.example",
      "AUTH_TOKEN": os.environ["FATHOM_TEMPLATE_TOKEN"],
      "OPTIONS": {"transaction_mode": "IMMEDIATE"},
  }}
  ```

Tenant projects never point at the template; they point at their own subdomain
([`quickstart-django.md`](quickstart-django.md)).

## 1. Capture — migrate the template

Author the migration in your Django app, then run it against the template project:

```bash
python manage.py makemigrations
python manage.py migrate          # runs against the template shard; fathom captures it
```

Each migration that rises the template's `django_migrations` count inside a transaction is recorded
as the next fleet version (`Fathom.Migrator.list/0`). **Follow expand-contract** — see below; the
engine is safe only for migrations the running app can tolerate mid-rollout.

### Capture-time safety nets (what fathom refuses or flags)

fathom lints each captured migration and **holds the rollout** (`requires_review`, which caps HEAD
below the flagged version) rather than replaying something dangerous fleet-wide:

- **Data migrations** (`RunPython` backfills) cross the wire as template-literal `INSERT/UPDATE/DELETE`
  carrying the *template's* rows — replayed onto every tenant they corrupt or silently skip data.
  Flagged `requires_review` (#1); review and supply a real per-shard data step before approving.
- **Non-atomic migrations** (`atomic = False`) run autocommit and are invisible to capture; the
  *gap* is caught at the next capture and flagged (#6).
- **Backwards migrate** (`manage.py migrate <app> <prev>`) is alarmed (#6) — **fleet undo is a
  fathom revert, never a Django backwards migrate.**
- **Ad-hoc DDL directly on a tenant** is refused with `FILO_DDL_BLOCKED` when `:block_tenant_ddl` is
  on (#7), so no one bypasses the version stamp.

Clear a review hold with `Fathom.Migrator.approve_review(version)` after confirming it's safe;
`Fathom.Migrator.pending_review/0` lists what's held.

### Expand-contract (the rule that makes mid-rollout safe)

During a rollout the fleet is a **mix of vN-1 and vN**, and both must serve. So:

- **Safe, replay verbatim:** add a nullable column, add a table, add an index, backfill via a
  fathom-reviewed data step.
- **NOT safe in one step:** rename or drop a column/table that live code still reads. Split it into
  expand (add the new, dual-write) → migrate readers → contract (drop the old) across releases.
  `docs/migration.md` defines a rename/drop with no expand-contract dance as **"not done."**

> **Why there's no automatic DROP/RENAME lint (yet).** Django's SQLite backend **rebuilds tables**
> for routine `ALTER COLUMN`-class changes (create a new table, copy, `DROP TABLE` old, `RENAME`
> new→old), so a naive DROP/RENAME matcher would false-flag every safe rebuild. Catching a *truly*
> destructive drop needs a before/after **schema diff**, not raw-DDL matching — a deferred follow-up.
> Until then, expand-contract is an authoring discipline, enforced by review, not a capture-time lint.

## 2. Release — publish the fleet HEAD

Once a version is captured (and any review holds cleared), it's the target HEAD and the rollout
sweeps tenants onto it (Oban jobs, hot shards first; the hourly reconcile converges the cold tail).
See `docs/migration.md` for the copy-then-flip mechanics and `MIGRATE_ON_TOUCH`
([`quickstart-django.md`](quickstart-django.md)) for how cold tenants converge on first touch.

## 3. Converge — gate your app deploy on `laggards == 0`

**Do not deploy app code that depends on vN until the fleet has converged to vN.** The gate is a
JSON endpoint your CI/CD can poll (behind the admin BasicAuth, on `:4000`):

```bash
curl -su "$ADMIN_USER:$ADMIN_PASS" https://admin.fathom.example/api/migrations/status
# {"head":7,"laggards":0,"failed":0,"converged":true,"pending_review":[],
#  "rate_per_hour":0,"eta_seconds":0}
```

- `converged: true` (⇔ `laggards == 0`) — every active shard is at HEAD; safe to ship the new app
  version.
- `laggards: N` — N active shards are still behind HEAD; wait (or drive `MIGRATE_ON_TOUCH=async` to
  converge touched tenants faster).
- `pending_review: [v]` — a captured version is **held** for operator review (a data migration or a
  gap); HEAD won't advance to it until you `approve_review`.
- `failed: N` — N shards are **quarantined** (`migration_failed`) — see triage below.
- `rate_per_hour: N` — shards that reached HEAD in the trailing hour (measured from the directory's
  `cutover_at`, so it is fleet-wide, not per-node). A **revert** is not counted as progress.
- `eta_seconds: N` — `laggards / rate` at the current rate. **`null` means the rollout is not
  moving** (rate 0) — a stall, not a long wait. `0` once converged.

A CI gate is just: poll until `converged == true` and `pending_review == []`, then deploy.

### Sizing the rollout

`ReconcileJob` sweeps `:reconcile_batch_size` shards (default 100) on an hourly cron, so the cold
tail converges at ~2,400/day out of the box — months for a deep fleet. `rate_per_hour` is what you
raise `RECONCILE_BATCH_SIZE` *against*: raise it, watch the rate, and stop when the rate stops
following. That plateau tells you the ceiling has moved somewhere else — the `migrations` queue
concurrency, the per-shard S3 round trips, or drain contention with live traffic — and raising the
batch size further only adds latency pressure on live tenants without converging faster.

Per-node throughput is the `[:fathom, :migrator, :shard_migrated]` telemetry event
(`%{count: 1}`, metadata `%{shard_id, from, to}`), emitted once per shard that actually moved.

## 4. Revert — if a migration is bad

`docs/migration.md` → "Reverting a bad Django migration" has the full loop: `Migrator.revert(vN, vN-1)`
(fathom side) → **backwards-migrate the template** → author the fix → re-release. The template
drift check (#32) alarms if you skip the backwards-migrate step.

## Triage: `migration_failed` on one shard of many

A shard whose forward migration exhausts its retries is quarantined (`migration_failed`) so the rest
of the fleet keeps converging — it's excluded from the laggard sweep, not retried forever. For that
one tenant:

- It keeps serving at its **old** version (vN-1) — expand-contract makes that correct, so the tenant
  is up, just behind.
- Inspect the cause (`Fathom.Directory.failed_shards/0`; the job's logs name the shard + error). A
  transient cause (S3 blip, a lock) is common.
- Once the cause is fixed, `Fathom.Migrator.retry_failed/0` un-quarantines failed shards and
  re-enqueues them to HEAD. `failed` in the status endpoint returns to 0 as they converge.

## Client version ceiling

`django-libsql` is an alpha community backend: **Python 3.12, Django 5.0.x** (it breaks on 5.1+ —
`index_together` was removed). Django ≥ 5.1 support means addressing or forking `django-libsql`
(tracked as the out-of-repo companion-package work, review #16). The proven reference integration is
`test/django_validation/`.
