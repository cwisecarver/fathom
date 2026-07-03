"""
Stock Django settings for the fathom validation harness.

The only non-default thing here is DATABASES: an UNCHANGED Django project pointed
at fathom's Hrana endpoint via django-libsql. Migrating + querying this proves a
real libSQL client works against fathom, and (with config :fathom,
template_shard_id: "demo") that fathom auto-captures Django's migrations.
"""
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent.parent

SECRET_KEY = "django-libsql-validation-not-a-secret"
DEBUG = True
ALLOWED_HOSTS = []

INSTALLED_APPS = [
    # Real Django framework migrations (auth depends on contenttypes), plus our
    # own app — so `migrate` exercises stock Django DDL, not just a toy model.
    "django.contrib.contenttypes",
    "django.contrib.auth",
    "widgets",
]

MIDDLEWARE = []
ROOT_URLCONF = "validation.urls"

# The whole point: an unchanged Django project talking to fathom over Hrana.
# ws:// = plaintext WebSocket Hrana; localhost routes to fathom's default "demo"
# shard. No auth token (fathom's Hrana listener requires none).
DATABASES = {
    "default": {
        "ENGINE": "libsql.db.backends.sqlite3",
        "NAME": "ws://localhost:8080",
    }
}

DEFAULT_AUTO_FIELD = "django.db.models.BigAutoField"
USE_TZ = True
