# Quickstart — Django on fathom

Get an unchanged Django app talking to a fathom-served tenant over `django-libsql`. This page covers
the **single-tenant-per-Django-process** pattern — the simplest shape, and the building block for
everything else.

**Multi-tenant per-request routing is built**, just not yet packaged. The `django_fathom` package
(runtime shard-alias registration with an LRU bound, a fail-closed database router, and a
`shard_id=` queryset kwarg) picks a tenant's shard **per request**, and is exercised end to end
against a real fathom. What remains open from review #16 is *distribution*: publishing it as an
installable `fathom-django` rather than a directory each app vendors.

Pairs with the eval stack ([`deploy/compose/README.md`](../deploy/compose/README.md)) and the proven
validation harness (`test/django_validation/`).

## 0. Bring up fathom

Use the eval stack (`cd deploy/compose && docker compose up --build`) or `iex -S mix phx.server` in
dev. You need the Hrana endpoint reachable on `:8080`.

## 1. Provision the tenant's shard

```bash
curl -su admin:admin -X POST http://localhost:4000/api/tenants \
  -H 'content-type: application/json' -d '{"shard_id": "acme"}'
# -> {"shard_id":"acme","url":"libsql://acme.localhost","auth_token":"…","auth_required":false,"warnings":[]}
```

The shard is `acme`, selected by the **Host subdomain** `acme.localhost`. (In dev without the eval
stack, fathom's `:default_shard` is `demo`, so `ws://localhost:8080` routes to `demo` — the harness
default.)

## 2. Point Django at it

The only non-default thing in a stock Django project is `DATABASES`:

```python
# settings.py
DATABASES = {
    "default": {
        "ENGINE": "libsql.db.backends.sqlite3",
        # Host subdomain selects the shard. ws:// = plaintext WebSocket Hrana (eval).
        "NAME": "ws://acme.localhost:8080",
        # With HRANA_AUTH=required, pass the token from step 1:
        # "AUTH_TOKEN": os.environ["FATHOM_AUTH_TOKEN"],
    }
}
```

For TLS (production), use `wss://acme.fathom.example` (the LB terminates TLS; see
[`deploy-cluster.md`](deploy-cluster.md)) and set `AUTH_TOKEN` when `HRANA_AUTH=required`
([`auth.md`](auth.md)).

## 3. Migrate and query

```bash
python manage.py migrate      # applies over Hrana; fathom auto-captures the DDL into a fleet version
python manage.py runserver
```

An unchanged Django app now reads/writes its tenant's SQLite shard over the network. fathom captures
the migrations it serves into fleet versions (`Fathom.Migrator.list/0`) so the same schema can be
rolled fleet-wide — the migration engine story is [`migration.md`](migration.md).

## Schema rollout & the mixed window (`MIGRATE_ON_TOUCH`)

When you ship a new migration, fathom captures it as a fleet version and rolls it onto tenants
blue/green. Between the release and convergence, a **cold** tenant (idle, flushed to S3) is still at
the old version until something migrates it. Django's expand-contract discipline makes serving the
old schema *correct* in that window (the same discipline `docs/migration.md` enforces) — the
question is only *when* each cold tenant converges, and who pays. Three modes (`MIGRATE_ON_TOUCH`):

| Mode | First request to a cold laggard | Convergence | Cost |
|---|---|---|---|
| `off` (default) | served at vN-1, unblocked | the hourly reconcile cron | up to ~1h of stale-schema reads on untouched cold tenants |
| `async` | served at vN-1, unblocked; the migration is **enqueued** | next job cycle after first touch | none inline — a background job per touched laggard |
| `inline` | **blocks** on the full blue/green migration (drain + copy + replay + S3 round-trips) | immediate | multi-second first-request latency per cold laggard after a release |

**Recommendation:** `async` is the middle ground — no multi-second inline tail *and* touched tenants
converge promptly (the untouched cold tail still rides the reconcile cron). Use `inline` only if you
cannot tolerate any old-schema read; `off` if your tenants are rarely cold and the hourly cron is
fine. `async`/`inline` add a per-checkout directory read (the documented hot-path exception), so
leave the default `off` unless you're rolling migrations.

## Multi-tenant routing (the pending piece)

A SaaS serves many tenants from one Django deployment. fathom's model is **one shard per tenant,
selected by Host subdomain** — so multi-tenancy is "point this request at `ws://<tenant>.<zone>:8080`."
The clean way to do that per-request (a Django database router / connection-per-tenant keyed on the
request's tenant) is the `fathom-django` companion package (review #16), which isn't published yet.

Until then, the supported patterns are:
- **One Django process per tenant** (or per tenant group), each with its `NAME` pointed at that
  tenant's subdomain. Simple and fully working today.
- **A hand-rolled router** that swaps the connection's Host per request. Possible with
  `django-libsql`, but you own the routing/pooling — the companion package is what will make this
  turnkey.

Provision/list/suspend/delete tenants through the control-plane API
([`tenant-lifecycle.md`](tenant-lifecycle.md), `/api/tenants`).

## Known constraints (from the validation harness)

`django-libsql` is an alpha community package (github.com/aaronkazah/django-libsql), which pins the
client stack:

- **Python 3.12** — 3.13 deprecated / 3.14 removed `sqlite3.dbapi2.version`, which `libsql-client`
  0.3.1 imports (fails at import on 3.13+).
- **Django 5.0.x** — `django-libsql` 0.1.3's schema editor references `model._meta.index_together`,
  removed in Django 5.1 (breaks on the first `alter_field`).

The end-to-end proof (an unchanged Django project migrating + doing ORM CRUD over fathom, with the
migrations auto-captured) is `test/django_validation/` — run it as the reference integration.

### Django's SQLite UDFs — supplied by `fathom_udf` (expert review #19)

Django's SQLite backend registers **54** Python functions on every **client** connection. Under
`django-libsql` the SQL crosses the wire and is compiled by **fathom's** SQLite, so for a long time
they did not exist there: basic CRUD worked and the first date/regex lookup raised
`OperationalError: no such function: django_date_extract`, an error that reads like a client bug.

**As of 2026-08-05 all 54 resolve.** 19 come from SQLite itself (the 18 math functions, because this
build sets `SQLITE_ENABLE_MATH_FUNCTIONS`, plus `SIGN`). The other **35 are supplied by
`fathom_udf`**, a loadable SQLite extension built from `native/fathom_udf` and loaded onto every
shard connection.

Nothing to configure — an unchanged Django app gets them. `__year`, `__date`, `TruncMonth`,
`__regex`, `DurationField` arithmetic, `StdDev`/`Variance` all work.

| Django code | Function | Supplied by |
|---|---|---|
| `.filter(created__year=…)`, `__month`, `__day`, `__hour`, `__week_day` | `django_date_extract`, `django_datetime_extract` | extension |
| `.filter(created__date=…)`, `__time` | `django_datetime_cast_date`, `django_datetime_cast_time` | extension |
| `TruncMonth/TruncDay/TruncDate(...)` — every time-series chart | `django_date_trunc`, `django_datetime_trunc`, `django_time_trunc` | extension |
| `.filter(field__regex=…)`, `__iregex` | `regexp` | extension |
| `F()` arithmetic on a `DurationField` | `django_format_dtdelta`, `django_timestamp_diff`, `django_time_diff` | extension |
| `LPad`, `RPad`, `Repeat`, `Reverse` | `LPAD`, `RPAD`, `REPEAT`, `REVERSE` | extension |
| `MD5`, `SHA1`…`SHA512` | same names | extension |
| `StdDev`, `Variance` | `STDDEV_POP/SAMP`, `VAR_POP/SAMP` | extension |
| `BitAnd`, `BitOr`, `BitXor`, `ANY_VALUE` | `BIT_AND`, `BIT_OR`, `BIT_XOR`, `ANY_VALUE` | extension |
| `UUIDV4()`, `UUIDV7()`, `Random()` | `UUIDV4`, `UUIDV7`, `RAND` | extension |
| `**` / `Power()`, `Log`, trig | `POWER`, `LOG`, … | SQLite build |

#### Timezones work, including DST

The six `tzname`-taking functions need a timezone **database** server-side, and they are the ones
ordinary apps hit first — `USE_TZ` has defaulted to `True` since Django 5.0, the version
`django-libsql` pins. `fathom_udf` bundles the IANA database (via `chrono-tz`), so there is no
`/usr/share/zoneinfo` dependency and a slim container behaves like a dev box.

Verified against Django's own behaviour: zone conversion happens *before* truncation (so
`TruncDay` on a UTC timestamp near midnight gives the tenant's local day), half-hour and
quarter-hour offsets are exact, the ambiguous hour when DST ends takes the first occurrence
(Python's `fold=0`), and the nonexistent hour when DST starts resolves rather than erroring.

#### How it is loaded, and why that is safe

Loading requires SQLite's extension mechanism, which is **arbitrary code execution** if left
enabled — one `SELECT load_extension('/tmp/evil.so')` from a tenant would be below every guard
fathom has. So `Fathom.Shard.Extension` **enables it, loads exactly this file, and disables it
again** before the connection is used, on every handle including read-only ones. A failure to
re-disable fails the open rather than degrading. `test/fathom/shard/extension_test.exs` asserts the
tenant-side outcome (`not authorized`) rather than trusting the sequence.

Every function in the extension is pure: no filesystem, no network, no process state.

#### Configuration

| `SQLITE_EXTENSION` | Effect |
|---|---|
| unset (default) | load `priv/sqlite_ext/` if the artifact is there |
| `false` | disable — restores the pre-#19 `no such function` behaviour |
| a path | load that file instead |

The artifact is built by `mix compile` (via `mix compile.fathom_udf`) and needs a Rust toolchain.
**Without cargo the build SKIPS rather than fails** — fathom still compiles and runs, Django's UDFs
are just absent. It prints a line saying so; a silent skip is how "works on my machine" starts.

#### Cost

Measured 2026-08-05 (same tree, extension on vs off, back to back): **+39 µs on stream open**
(374 → 413 µs, the `dlopen` plus 35 function registrations) and **no change to per-query latency**
(`hrana_rt_us` 90 → 91 µs, noise). Since a stream serves many queries, the amortized cost is
negligible. Memory per served shard rises ~4%.

#### Fidelity

`native/fathom_udf/tests/oracle.rs` replays **12,501 cases whose expected values were produced by
running Django's own `_sqlite_*` functions** (`tests/oracle/generate_cases.py`, which vendors the
reference implementation with provenance). That found seven real bugs that hand-written
expectations had missed — including `django_format_dtdelta` returning text where Django returns a
number, and `date - timedelta` moving the date a day when Python does not.

Two deliberate divergences, both toward safety and both documented in
`native/fathom_udf/src/aggregates.rs`: aggregates **skip NULLs** (Django's `list.append` step feeds
`None` to `statistics.pstdev`, which raises — so a `STDDEV` over a nullable column would error the
tenant's query), and a **degenerate group returns NULL** rather than raising, matching PostgreSQL.
One known limit: `regexp` uses Rust's `regex` crate, which has no backreferences or lookaround and
rejects such a pattern rather than mismatching.

## Request latency & keeping tenants warm

fathom is bottomless: a tenant that's been idle is flushed to S3 and dropped, so its **first**
request pays a cold-open. Owning this number up front is the difference between "priced" and "slow".

**The two regimes:**

| Request | What it costs | Typical |
|---|---|---|
| **Warm** (tenant already open on the node) | local SQLite over an established stream — no S3, no network beyond the LB | sub-ms to low-ms query time |
| **Cold** (first request to an idle/dropped tenant) | lease acquire + object pull, **≈ 1 S3 round-trip** + body transfer (scales with shard size × bandwidth), done *inline* in that first query | see below |

**The cold-open contract.** Cold-open is optimized to ~1 S3 RTT (the lease acquire and the pull
overlap). Measured on the fleet ([`reviews/latency-cost-2026-07-23.md`](reviews/latency-cost-2026-07-23.md)):
at **10 / 30 / 60 ms** one-way S3 RTT, cold-open ≈ **26 / 70 / 133 ms** — roughly `~2× one-way RTT
+ a few ms`, plus body
transfer for a large shard. So a p99 first-request contract is *your S3 region RTT × ~2 + shard-size
term*; steady-state (warm) requests are not on this curve at all. A 100–300 ms first-request tail is
**expected and bounded**, not pathological — it's the bottomless design, priced.

**Three levers to keep it out of the hot path:**

1. **`CONN_MAX_AGE`.** Django's default `CONN_MAX_AGE=0` opens (and tears down) a fresh Hrana
   stream per request — re-running client init and per-stream `Connection.open` every time. Set
   `CONN_MAX_AGE=None` (persistent connections) so a worker reuses one stream across requests and
   only the *first* request per worker pays stream setup:
   ```python
   DATABASES = {"default": {"ENGINE": "libsql.db.backends.sqlite3", "NAME": "ws://acme.localhost:8080",
                            "CONN_MAX_AGE": None}}
   ```
2. **Idle-flush timing.** A tenant goes cold after `SHARD_IDLE_MS` of no activity (flush + drop).
   Raise it so active tenants stay resident longer (fewer cold-opens) at the cost of more open
   shards per node — trade it against your node density (`docs/configuration.md`,
   `docs/reviews/fleet-density-2026-07-10.md`).
3. **Pre-warm on login (optional).** The warm-standby follower (A1) covers *failover*, not
   *first-touch of a cold tenant*. To hide the cold-open from the user's first real request, open a
   cheap stream (e.g. `SELECT 1`) against the tenant's subdomain when they log in / land — that
   triggers the cold-open ahead of the work. Provisioning already returns the tenant URL, so this is
   a client-side pattern, not a fathom feature.

## Write concurrency — set `transaction_mode: IMMEDIATE` (important)

Each shard is one SQLite database in WAL mode: **one writer at a time.** With more than one Django
worker/thread writing the *same tenant* (the default multi-worker gunicorn/uwsgi deployment),
you'll hit a footgun. Django issues a plain deferred `BEGIN`, so the common
`with transaction.atomic(): obj = Model.objects.get(...); obj.save()` reads first, then upgrades to
a write — and if another writer advanced the WAL in between, SQLite returns `SQLITE_BUSY_SNAPSHOT`
**immediately**. The busy-timeout does *not* wait for this case, so the transaction fails
**non-retryably** with a "database is locked"-class error (fathom surfaces the real `SQLITE_BUSY`
code, review #3).

The fix is client-side and one line — take the write lock up front so the upgrade conflict can't
happen:

```python
DATABASES = {
    "default": {
        "ENGINE": "libsql.db.backends.sqlite3",
        "NAME": "ws://acme.localhost:8080",
        # Django 5.1+: begin every transaction as BEGIN IMMEDIATE (grabs the write lock up front).
        "OPTIONS": {"transaction_mode": "IMMEDIATE"},
    }
}
```

On Django ≤ 5.0 (the `django-libsql` version ceiling above), issue `PRAGMA busy_timeout` /
`BEGIN IMMEDIATE` via an init command or wrap write transactions accordingly. fathom deliberately
does **not** rewrite your SQL server-side — you control the writes, so this stays a one-line client
setting (review #18). Set it whenever a tenant sees concurrent writers.
