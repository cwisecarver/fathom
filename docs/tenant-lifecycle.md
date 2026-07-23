# Tenant lifecycle — delete & export

*How `Fathom.Tenants` erases and exports a whole tenant. Built for expert-review finding #15.*

A tenant **is** one shard — one SQLite file plus its lease, retained migration versions, and
snapshots. The migration engine deliberately scoped out the two whole-shard lifecycle operations
every multi-tenant platform eventually needs:

- **delete** — GDPR Article 17 erasure / offboarding: erase everything, and make sure it can't
  silently come back.
- **export** — GDPR portability: hand the customer their data. Trivial here, because their data
  *is* a `.db` file.

Neither has a feature flag. They're inert until an operator invokes them.

## Delete

`Fathom.Tenants.delete/1` does the safety-critical work **synchronously**, then hands the slow
physical erase to a durable, retryable job.

```
delete(id):
  Directory.tombstone(id)          # 1. durable: status -> "deleted" (upsert; never resurrected)
  broadcast_deleted(id)            # 2. fleet-wide: ETS re-mint gate + warm-cache purge, now
  enqueue DeleteJob(id)            # 3. the physical erase, off the request path
```

By the time `delete/1` returns, **no new stream can open the shard** anywhere in the fleet — the
re-mint guard is set before a single object is touched.

### The re-mint guard (why a deleted tenant can't resurrect)

The original bug: novel-shard admission mints a shard on first request, and the directory's
"is this known?" check treats *any* row as known (and fails open on a Postgres blip). So a stray
request for a just-deleted subdomain would re-create it as an empty shard — the tenant "comes back".

The guard is `Fathom.Tenants.Tombstones`, modeled on `HranaAuth.Revocations`:

- a **public ETS set** of deleted shard ids, checked O(1) as the *first* branch of
  `Fathom.Shards.start_if_capacity` ⇒ `{:error, :shard_tombstoned}`. No Postgres on the (near-hot)
  open path, so it never touches cold-open latency and holds even during a directory outage.
- **loaded from the directory at boot** (`Directory.deleted_shard_ids/0`),
- **pushed fleet-wide on delete** over Oban's `LISTEN/NOTIFY` (`:fathom_tenant_deleted`) — the same
  notification also drops each node's lease-less **warm-follower copy** of the shard, so an erased
  tenant's cached bytes don't linger until the follower's next poll,
- **refreshed periodically** (`:tenant_tombstone_refresh_ms`, default 5 min) so a node that booted
  during a Postgres outage, or missed a fire-and-forget notification, still converges. The set is
  append-only in memory (a tombstone is permanent).

The directory row is kept tombstoned (not hard-deleted) as the permanent record — it holds no tenant
*data*, just the id + `deleted` status. `resolve`/`record_batch` on-conflict only bump recency, never
status, so a late access can't un-delete it.

### The physical erase (`DeleteJob` → `Tenants.purge/1`)

```
purge(id):
  cancel_pending_jobs(id)   # scheduled/available/retryable migration/revert/retirement/handoff jobs
  Shards.stop(id)           # FORCE-stop the home coordinator (see below)
  Storage.purge_shard(id)   # delete every stored object of the shard
  rm_local(id)              # sweep the local working file + any quarantine copies
```

`Storage.purge_shard/1` deletes the live `.db`, the `.lock`, every retained `@<version>`, and every
`@snap-<id>` in one sweep. Matching is **exact on the id delimiter** — the character after the id must
be `.` (live/lock) or `@` (a version/snapshot). A bare `starts_with(<id>)` match would erase a
different tenant, so purging `acme` provably never touches `acme2` (this is the shard-isolation gate,
pinned by test).

### Why `Shards.stop/1` and not `drain/2` (a real leak, fixed)

The obvious implementation — graceful-drain then delete — has a data-surviving-the-erase bug. A
graceful drain **can't stop a coordinator that's actively serving** (held connections); it returns
`:busy` and leaves it running. If you then delete the storage, the still-live coordinator's next
fenced flush 412s against the now-gone object and **self-fences**, quarantining its un-flushed writes
to a `.fenced.<ts>` file on local disk (finding #5's safety mechanism). That quarantine file is a copy
of the *erased tenant's data*, surviving the erasure.

`Fathom.Shards.stop/1` fixes it: it terminates the coordinator via the supervisor **while its lease is
still valid**, so shutdown flushes/releases cleanly (or is brutal-killed) and never takes the
self-fence path. Only *then* does `purge` delete the storage. `rm_local` additionally sweeps any
stray `.fenced.*` / `.forked.*` / `.corrupt.*` copies (anchored at `<id>.db` so it can't hit a sibling
id). No copy of a deleted tenant is left on disk.

### Cross-node safety

Deletes are usually issued from a node that isn't the shard's LB home. Purging the storage is safe
even if a live node still holds the lease: **every coordinator flush is fenced** (`If-Match`), so once
the live object is gone that node's next flush 412s and it self-fences — dropping its buffered writes
(exactly the erase we want) instead of re-creating the object. This is logged + `[:fathom, :tenants,
:purge_while_held]` telemetry. Promptly draining a *busy* remote coordinator (via the
`rebalance_commands` / `command_poller` path) is a follow-up; single-home routing plus the fenced-flush
self-fence cover correctness today.

## Export

`Fathom.Tenants.export/1` pulls the shard's current **durable** stored object to a temp file and
returns its path + a suggested filename. Because a tenant is one SQLite file, that *is* the export —
no format to build. It reflects the last flush (an active shard may have newer writes buffered on its
coordinator; drain or let it idle for the very latest — the same caveat as a snapshot).

`GET /admin/tenants/:id/export` (BasicAuth, `AdminTenantController`) streams the file via
`send_download` through the `:4000` admin endpoint and **deletes the temp afterward**, so an exported
copy never lingers. It's served through the operator boundary, never a public presigned URL (a
presigned-GET path for very large shards is a possible follow-up). The `:id` is validated by
`ShardId.cast`, so a path-traversal id is a 400, not a file read.

## Provision (the control-plane API)

Tenants used to exist only as a side effect of traffic — novel-shard admission mints one on the
first request for a subdomain. That's a fine data-path fallback but not a product surface. `#21`
adds an explicit control plane so a platform customer can create/list/delete tenants and get a
connection URL + token.

`Fathom.Tenants.provision/1`:

```
provision(id):
  cast + refuse if tombstoned (:tombstoned) or already-exists (:already_exists)
  dns_safety(id)                     # non-DNS-safe id → warn (default) or refuse (:id_not_dns_safe)
  Directory.resolve(id)              # explicit row, status "active"
  maybe fork-from-template @HEAD     # when :fork_from_template is on (#10); else born empty
  mint a bearer token
  -> %{shard_id, url: "libsql://<id>.<base>", auth_token, auth_required, warnings}
```

`auth_required` reflects `:hrana_auth`: with auth on, the client must present `auth_token`; with it
off the token is informational (the trust boundary is the network). `<base>` is `:shard_base_domain`
(prod) or `local` (dev), matching how the Host-subdomain router resolves a shard.

**DNS-safety (#35).** provision/fork is the one place that *knows* the deployment's address, so a
non-DNS-safe shard id — an underscore, `>63` chars, or a leading/trailing hyphen — that no
`*.<zone>` wildcard-TLS cert can serve (RFC 6125) is caught here rather than surfacing days later as
an opaque TLS handshake failure. By default it still provisions but the result carries a `warnings`
advisory (`Fathom.ShardId.dns_safe?/1` decides); set `:wildcard_tls_serving`
(`WILDCARD_TLS_SERVING`) — for a deployment that terminates wildcard TLS — and provision/fork
instead **refuse** with `{:error, :id_not_dns_safe}` (HTTP 422). `ShardId.valid?` stays permissive
(the plaintext path still works); the gate lives only here. Prefer hyphenated ids.

The JSON API (`FathomWeb.Api.TenantController`, `/api`, behind the same admin BasicAuth, on the
`:4000` endpoint — separate from the Hrana data port):

| Method | Path | Body / result |
|--------|------|---------------|
| `POST` | `/api/tenants` | `{"shard_id":"acme"}` → 201 `{shard_id, url, auth_token, auth_required, warnings}` · 409 exists/tombstoned · 422 not DNS-safe (only when wildcard-TLS serving) · 400 invalid |
| `GET` | `/api/tenants` | `?status=&q=&limit=&offset=` → 200 `{tenants:[…], total, limit, offset}` (reuses `Directory.list_page`) |
| `GET` | `/api/tenants/:id` | 200 tenant · 404 |
| `DELETE` | `/api/tenants/:id` | 202 `{status:"deleting"}` (reuses `Tenants.delete/1`) · 400 |

Optionally, with the provisioning API as the front door, an operator can flip prod novel admission
to *known-tenants-only* by turning on `:novel_shard_rate` (the enforcement point already exists) so
an unprovisioned subdomain is rate-limited/refused rather than minted. That's a config choice, not
part of the API.

## Suspend / resume

The administrative-offline lever short of deletion (`#20`) — for a non-paying, abusive, or
legal-hold tenant. It's the reversible sibling of delete's tombstone:

```
suspend(id):
  Directory.suspend(id)             # status -> suspended (refuses a deleted tenant)
  broadcast (ETS gate + notify)     # every node denies NEW streams now
  Shards.drain(id)                  # graceful: in-flight txns finish, coordinator stops

resume(id):
  Directory.resume(id)              # status -> active
  broadcast (ETS gate + notify)     # gate stops denying; next request cold-opens fresh
```

`Fathom.Tenants.Suspensions` is the deny gate — the same ETS + notifier + boot-load shape as
`Tombstones`, but a suspension **comes and goes**: the notification carries an add/remove flag, and
the periodic refresh **reconciles** the set against the directory (insert-then-prune) so a missed
resume converges. It's checked O(1) in `Fathom.Shards.ensure/1`, alongside the tombstone check, on
**every** checkout — so a suspended (or deleted) tenant is refused a new stream even if a coordinator
is still running. A suspended open surfaces a distinct **403 `FILO_TENANT_SUSPENDED`** (a retry won't
help — an operator must resume); a deleted open is **410 `FILO_TENANT_DELETED`**.

Suspend denies new streams fleet-wide immediately and graceful-drains the home coordinator, so
in-flight transactions finish but nothing new opens. It does not delete data — resume brings the
tenant straight back.

## Fork (clone a tenant)

Because a tenant *is* one SQLite object, **forking** it — for a preview environment, per-tenant
staging, test-database forking, or cloning a tenant to debug an incident — is one object copy
(`#14`). `Fathom.Tenants.fork/2`:

```
fork(src, dst):
  refuse if dst is taken/tombstoned
  Storage.fork_shard(src, dst)      # copy src.db -> dst.db (refuse :dst_exists / :no_source)
  register dst @ src's schema version   # so the laggard sweep won't re-migrate the fork
  mint a dst token
  -> %{shard_id: dst, url, auth_token, auth_required, warnings}   # DNS-safety applies to dst too (#35)
```

It reflects `src`'s **last durably-flushed** state and does NOT disrupt `src` (no drain) — snapshot
or drain `src` first for the very latest. Registering the dst directory row at the source's schema
version is load-bearing: a fork left at `v0` while its copied `.db` is at `vN` would be swept as a
laggard and quarantined when the engine replayed a migration onto the already-migrated file.

## Operator tooling (`mix fathom.shard`)

The recover/validate/clone commands over the `Storage` behaviour, so recovery isn't hand-rolled
`aws-cli` + `sqlite3` under incident pressure:

| Command | What |
|---------|------|
| `mix fathom.shard pull <shard> [path]` | download the stored `.db` |
| `mix fathom.shard inspect <shard>` | pull + read-only `quick_check`, `user_version`, per-table row counts — a **per-shard restore drill** (an untested restore path is an unproven backup) |
| `mix fathom.shard fork <src> <dst>` | clone a live tenant to a new id (or `POST /api/tenants/:id/fork`, or `Fathom.Tenants.fork/2` from a node console) |

`pull`/`inspect` only touch stored objects; `fork` needs the directory + token secret (run it from a
node console in a running release). A scheduled **fleet** restore-drill (sample N shards/day) is a
follow-up — `inspect` is the manual per-shard form today. Point-in-time restore is `mix
fathom.snapshot restore` (#12); a fleet schema revert is the migrator.

## Operator runbook

**Delete a tenant.** In the admin dashboard, **Directory** (`/admin/directory`), find the row and
click **Delete** (a confirm dialog states the erase is permanent). The tombstone + fleet-wide re-mint
block take effect immediately; the physical erase runs as a background `DeleteJob` (queue `:tenants`,
watchable in **Migrations**/Oban panels). To delete programmatically: `Fathom.Tenants.delete("acme")`
from a release remote console.

- **Want a safety net first?** Take a snapshot (`mix fathom.snapshot create acme`) before deleting —
  delete is immediate and irreversible by design.
- **A delete looks stuck.** Check the `:tenants` Oban queue for a retrying/failed `DeleteJob`; a
  storage error retries. Re-running `Tenants.delete/1` is safe (idempotent) and re-enqueues.
- **Re-use a deleted id.** The tombstone is permanent. To make the subdomain mintable again, hard-
  delete the directory row and let the tombstone set refresh (or restart the node).

**Export a tenant.** In **Directory**, click **Export** on the row (downloads `<id>.db`), or
`Fathom.Tenants.export/1` for a temp-file path. Open the file with any SQLite client.

**Provision a tenant.** `POST /api/tenants` with `{"shard_id":"acme"}` (BasicAuth) returns the
connection URL + token, or `Fathom.Tenants.provision("acme")` from a remote console. `GET
/api/tenants` lists the fleet; `DELETE /api/tenants/:id` is the API form of a delete.

**Suspend / resume a tenant.** In **Directory**, click **Suspend** on the row (confirm), or `POST
/api/tenants/:id/suspend`. New connections get a 403 fleet-wide until you **Resume** it (`POST
/api/tenants/:id/resume` or `Fathom.Tenants.resume/1`). Use it for non-payment, abuse, or a legal
hold — the data is untouched, so resume brings the tenant straight back.

## Where it lives

- `lib/fathom/tenants.ex` — `delete/1`, `purge/1`, `export/1`, `provision/1`, `suspend/1`,
  `resume/1`, `tombstoned?/1`, `suspended?/1`, `broadcast_deleted/1`, `broadcast_suspension/2`.
- `lib/fathom/tenants/suspensions.ex` — the reversible `suspended` ETS deny gate (#20).
- `lib/fathom_web/controllers/api/tenant_controller.ex` — the `/api/tenants` JSON control-plane.
- `lib/mix/tasks/fathom.shard.ex` — the `pull`/`inspect`/`fork` operator CLI (#14); `Storage.fork_shard/2`
  is the copy primitive and `Tenants.fork/2` the orchestration.
- `lib/fathom/tenants/tombstones.ex` — the ETS re-mint gate + notifier listener + warm-cache purge.
- `lib/fathom/tenants/delete_job.ex` — the Oban worker (queue `:tenants`, unique per shard).
- `lib/fathom/shard/storage.ex` (+ `local.ex` / `s3.ex`) — `purge_shard/1`.
- `lib/fathom/shards.ex` — `stop/1` (force-stop for deletion).
- `lib/fathom/directory.ex` / `directory/shard.ex` — `tombstone/1`, `deleted_shard_ids/0`, the
  `deleted` status.
- `lib/fathom_web/controllers/admin_tenant_controller.ex` + `live/admin_directory_live.ex` — the
  export download and the admin delete/export actions.
