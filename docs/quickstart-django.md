# Quickstart — Django on fathom

Get an unchanged Django app talking to a fathom-served tenant over `django-libsql`. The fathom side
is complete today; the **multi-tenant per-request routing helper** (choosing a tenant's shard per
request) is the planned `fathom-django` companion package (review #16) — until it ships, this covers
the single-tenant-per-Django-process pattern, which is the common shape and the building block.

Pairs with the eval stack ([`deploy/compose/README.md`](../deploy/compose/README.md)) and the proven
validation harness (`test/django_validation/`).

## 0. Bring up fathom

Use the eval stack (`cd deploy/compose && docker compose up --build`) or `iex -S mix phx.server` in
dev. You need the Hrana endpoint reachable on `:8080`.

## 1. Provision the tenant's shard

```bash
curl -su admin:admin -X POST http://localhost:4000/api/tenants \
  -H 'content-type: application/json' -d '{"shard_id": "acme"}'
# -> {"shard_id":"acme","url":"libsql://acme.localhost","auth_token":"…","auth_required":false}
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

> **Latency note:** the first request to a *sleeping* tenant pays the shard cold-open inline (one S3
> round-trip; ~2× the region RTT + a few ms). Django's default `CONN_MAX_AGE=0` opens a fresh stream
> per request — set `CONN_MAX_AGE` to reuse connections and amortize stream setup. A fuller
> latency/pooling contract is review #38 (pending).

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
