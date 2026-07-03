# django-libsql ↔ fathom validation harness

The definitive proof of fathom backlog **item #1**: that a **real, unchanged**
Django project, using the `django-libsql` backend, (a) works against fathom over the
Hrana protocol, and (b) emits transactions whose framing fathom's migration
auto-capture recognizes.

This is a stock Django project. The only non-default thing is `DATABASES` in
`validation/settings.py`, which points at fathom's Hrana endpoint:

```python
DATABASES = {"default": {
    "ENGINE": "libsql.db.backends.sqlite3",
    "NAME": "ws://localhost:8080",   # plaintext WebSocket Hrana; no auth token
}}
```

`ws://localhost:8080` routes to fathom's default **`demo`** shard. With
`config :fathom, template_shard_id: "demo"` (set in `config/dev.exs`), fathom
captures the migrations it serves on that shard into fleet versions.

## Result (proven 2026-06-29)

- `manage.py migrate` applied **all 15 migrations** (contenttypes, the full auth
  chain, widgets) cleanly over Hrana — unchanged Django works against fathom.
- CRUD via the ORM round-trips: created/read a `Widget` and a `User`.
- fathom auto-captured **14 fleet versions** (`Fathom.Migrator.list/0`), each named
  `auto-captured`. The widgets version contains the Django DDL **and** the
  `INSERT INTO django_migrations` bookkeeping — the captured framing matches the
  real client. (14 vs 15: the recorder's autocommit `django_migrations` table
  bootstrap is intentionally not captured — a documented engine limitation.)

## Version constraints (important)

- **Python 3.12.** 3.13 deprecated and 3.14 removed `sqlite3.dbapi2.version`, which
  `libsql-client` 0.3.1 imports — it fails at import on 3.13+/3.14.
- **Django 5.0.x.** `django-libsql` 0.1.3's schema editor references
  `model._meta.index_together`, removed in Django 5.1 — 5.1+ breaks on the first
  `alter_field` migration.
- `django-libsql` is an alpha community package (github.com/aaronkazah/django-libsql),
  not an official Turso product. It uses the archived pure-Python `libsql-client`
  over WebSocket Hrana.

## Reproduce

From the repo root (`fathom/`):

```bash
# 1. Python env (once). uv installs 3.12; the harness venv lives at fathom/.venv.
uv venv .venv --python 3.12
uv pip install --python .venv -r test/django_validation/requirements.txt

# 2. Postgres must be running and fathom_dev migrated (Oban + shard_migrations).
mix ecto.migrate

# 3. Start fathom serving Hrana on :8080 (no web server / asset watchers).
#    config/dev.exs already sets template_shard_id: "demo".
mix run --no-halt        # leave running; Hrana listener binds 0.0.0.0:8080

# 4. In another shell: run stock Django migrate + a CRUD smoke against fathom.
cd test/django_validation
../../.venv/bin/python manage.py migrate
../../.venv/bin/python manage.py shell -c "from widgets.models import Widget; \
  w=Widget.objects.create(name='hi'); print(Widget.objects.get(pk=w.pk))"

# 5. Verify capture (stop fathom first to free :8080, then query via the app):
#    pkill -f 'mix run --no-halt'
mix run -e 'IO.inspect Enum.map(Fathom.Migrator.list(), &{&1.version, &1.name, length(&1.statements)})'
```

To re-run from a clean slate, drop the demo shard and the captured versions:

```bash
rm -f "$TMPDIR"/fathom_shards/demo.db* "$TMPDIR"/fathom_remote/demo.db* "$TMPDIR"/fathom_remote/demo.lock
mix run -e 'Fathom.Repo.query!("TRUNCATE shard_migrations RESTART IDENTITY")'
```
