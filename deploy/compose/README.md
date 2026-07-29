# Fathom eval stack — zero to serving a tenant (review #17)

A single `docker compose up` gives you a complete fathom: the shard data plane (Hrana on `:8080`),
the control-plane API + dashboard (`:4000`), MinIO standing in for S3, and Postgres for the
directory. Goal: **a provisioned tenant you can query in under 30 minutes.**

> **Eval only.** Plaintext localhost, placeholder secrets, no durable volumes. Do not expose this
> stack. For a real deployment read [`../../docs/configuration.md`](../../docs/configuration.md)
> (every env knob + its safety consequence), [`../../docs/deploy-cluster.md`](../../docs/deploy-cluster.md)
> (the multi-node topology), and build the image **without** `WEB_INSECURE_LOCAL`.

## Prerequisites

- Docker + Docker Compose.
- The **filo** repo checked out next to fathom (`../filo`) — fathom depends on it as a path dep, and
  the image build needs both trees. The build context is the parent of both.

## 1. Bring it up

```bash
cd deploy/compose
docker compose up --build    # first build compiles a prod release (a few minutes)
```

Wait for the `fathom` service to become healthy (`docker compose ps`). Behind the scenes: Postgres
migrates, MinIO gets the `fathom-shards` bucket, and the node comes up serving `:8080` (Hrana) and
`:4000` (dashboard/API).

Sanity-check the node:

```bash
curl -sf http://localhost:8081/health   # -> ok    (health port, if you expose it)
open http://localhost:4000/admin         # dashboard (BasicAuth admin/admin)
```

## 2. Provision a tenant

Tenants are explicit — create one through the control-plane API (same BasicAuth as the dashboard):

```bash
curl -su admin:admin -X POST http://localhost:4000/api/tenants \
  -H 'content-type: application/json' \
  -d '{"shard_id": "acme"}'
```

Response:

```json
{
  "shard_id": "acme",
  "url": "libsql://acme.localhost",
  "auth_token": "…",
  "auth_required": false,
  "warnings": []
}
```

`auth_required` is `false` because the eval stack ships with `HRANA_AUTH` disabled (the network is
the trust boundary). The `url` is the logical endpoint; on the eval stack the client reaches it
through nginx on **port 8080** — i.e. `libsql://acme.localhost:8080` (or `http://acme.localhost:8080`
for HTTP SDKs). `acme.localhost` resolves to `127.0.0.1` on modern OSes.

List / inspect / delete:

```bash
curl -su admin:admin http://localhost:4000/api/tenants
curl -su admin:admin http://localhost:4000/api/tenants/acme
curl -su admin:admin -X DELETE http://localhost:4000/api/tenants/acme
```

## 3. Query it

Any unchanged libSQL client works — the shard is selected by the Host subdomain. A raw Hrana HTTP
pipeline (no SDK needed) proves the round-trip:

```bash
curl -s -X POST http://localhost:8080/v2/pipeline \
  -H 'Host: acme.localhost' -H 'content-type: application/json' \
  -d '{"requests":[
        {"type":"execute","stmt":{"sql":"CREATE TABLE IF NOT EXISTS hello (msg TEXT)"}},
        {"type":"execute","stmt":{"sql":"INSERT INTO hello VALUES (\"from fathom\")"}},
        {"type":"execute","stmt":{"sql":"SELECT msg FROM hello"}},
        {"type":"close"}
      ]}'
```

The last result carries the row. Isolation check: the same query against `Host: other.localhost` sees
an empty (different) database — that's a different shard, a different SQLite file.

From a libSQL SDK (e.g. `libsql-experimental` for Python):

```python
import libsql_experimental as libsql
conn = libsql.connect("http://acme.localhost:8080")   # HTTP SDK; ws for django-libsql
conn.execute("CREATE TABLE IF NOT EXISTS hello (msg TEXT)")
conn.execute("INSERT INTO hello VALUES ('from fathom')")
print(conn.execute("SELECT msg FROM hello").fetchall())
```

## 4. Django

`django-libsql` connects over WebSocket and selects the shard by Host subdomain. The end-to-end
Django on-ramp — settings, per-request tenant routing, and the migration workflow — is in
[`../../docs/quickstart-django.md`](../../docs/quickstart-django.md).

## Turning it into something real

| Eval default | Production |
|---|---|
| Plaintext localhost (`WEB_INSECURE_LOCAL=1`) | Build without it; terminate TLS at the LB (`deploy/lb/fathom.nginx.conf`, or the eval-TLS variant below) |
| `HRANA_AUTH` disabled (network trust) | `HRANA_AUTH=required` + per-shard tokens, or lock `:8080` to the LB only |
| Placeholder `SECRET_KEY_BASE` / `admin` password | Real secrets from a secret store (`docs/configuration.md`) |
| MinIO, no volume | Real S3/R2/Tigris; bucket hardening (`docs/durability.md`) |
| One node | Multi-node LB partition (`docs/deploy-cluster.md`) |
| No alerting | Wire `deploy/observability/` (Prometheus rules + Grafana + SLOs) |

## TLS on the eval stack

`nginx-tls.conf` is a drop-in TLS-terminating variant of `nginx.conf`: it serves the Hrana
data plane as **`wss` on `:8443`** and the admin/control-plane API as **`https` on `:4443`**
(→ `fathom:4000`), while fathom keeps speaking plain HTTP on `:8080`/`:4000` behind it — same
posture as the production LB. Mount it in place of `nginx.conf` and mount a wildcard cert +
key at `/etc/nginx/certs/{fullchain,privkey}.pem`, then point clients at `wss://<shard>.<zone>:8443`
and the API at `https://<name>.<zone>:4443`. For local eval, generate a throwaway CA + a
`*.<zone>` wildcard at compose-up and make the client trust that CA — that keeps per-shard
tokens off plaintext without needing a real trust anchor.

Tear down: `docker compose down -v`.
