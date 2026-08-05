# Fathom — auth & the trust boundary

> Status: **BUILT.** `Fathom.HranaAuth`, gated `:hrana_auth` (**`:disabled` by default**). How
> fathom authenticates a stream open without changing the libSQL client, why it's a per-shard token
> on Filo's `:authorize` callback seam, and what "the network is the boundary" means when auth is
> off. Deploy guidance is in `docs/deploy-cluster.md`.

## The problem it solves

Fathom serves **unchanged** libSQL clients (django-libsql over WebSocket, SDKs over HTTP). So auth
has to ride the client's *native* credential — libSQL's `authToken` — not a custom header the client
can't send. And it has to be **per-shard**: a token for tenant A must never open tenant B. The
awkward part is the two transports carry the credential differently, and the WebSocket one doesn't
arrive until *after* the HTTP upgrade.

## Two modes

`config :fathom, :hrana_auth`:

- **`:disabled` (default)** — no in-app credential; the **trust boundary is the network** (below).
- **`:required`** — every stream open must present a valid token **for the shard it's opening**. A
  boot guard (`check_config!/0`) refuses `:required` without a usable signing secret, and **any
  other configured value fails closed to `:required`** (a typo can't silently disable auth).

`HRANA_AUTH=required` in prod. Whether auth checks anything is a **runtime** mode, so flipping it
needs no listener restart.

## The credential — a per-shard `Phoenix.Token`

A credential is a `Phoenix.Token` signed with `HRANA_TOKEN_SECRET` (falling back to the endpoint's
`secret_key_base` when unset, with a boot warning — so a leaked web secret and a leaked shard secret
aren't forced to be the same). It's **minted per shard** — `mix fathom.token <shard>` — and carries
that shard id, so it verifies **only** for its own shard. It's presented exactly where libSQL puts
`authToken`:

- **HTTP** — the `Authorization: Bearer <token>` header.
- **WebSocket** — the Hrana `hello` message's `jwt` field (django-libsql's path). The token is
  **not** in the WS upgrade request.

## Why it's a callback, not a plug

Because the WebSocket token arrives in the `hello` message — **after** the HTTP upgrade — a pre-plug
can't see it. So auth is **Filo's `:authorize` callback seam**: Filo calls `HranaAuth.authorize/2`
per stream open, with the resolved shard id and the presented token, on both transports uniformly.
That's the one place that can see the credential on the same footing for HTTP and WS.

## No oracle, and no existence leak

- `authorize(shard_id, token)` returns `:ok` only if the token verifies for that shard. **Bad
  signature, expired, and a foreign shard's token are deliberately indistinguishable** — the failure
  is opaque, so it's not an oracle for guessing valid tokens or probing which shards exist.
- `authorize(nil, _)` (an *unresolved* request) returns `:ok` here — the fail-closed **400** for an
  unresolvable shard is [admission](admission.md)'s job (finding #26), not a **401** that would leak
  whether a shard exists.

## Revocation & expiry

Signing is **per shard**, so rotating a shard's signing element revokes every outstanding token for
**that one shard** without a fleet-wide flush; revocation converges within the verifier's cache TTL.
Tokens **don't expire by default**; set `:hrana_token_max_age` to bound their lifetime — a node running
`:required` with an infinite `max_age` logs a loud boot warning (#24).

## Token lifecycle: rotate, revoke, scope (#24)

A token embeds the shard's **version** (`v`), and `verify` accepts it while `v` clears the shard's
revocation **floor**. That floor is the lever for two distinct operations:

- **`HranaAuth.revoke/1` — immediate.** Raises the floor; every outstanding token below it stops
  verifying at once. The compromise-response path.
- **`HranaAuth.rotate/1` — zero-downtime.** Raises the version and mints a **new** token, but stamps
  `token_version_bumped_at`, and `verify` keeps accepting the **previous** version for a grace window
  (`:hrana_rotation_grace_ms`, default 1h): mint-new → deploy → the old auto-hardens out. This is the
  fix for "rotation is an outage" — at fleet scale, routine credential hygiene no longer means
  thousands of coordinated micro-outages. (A revoke clears `token_version_bumped_at`, so it never
  gets the grace.) The verifier's per-node cache holds `{floor, bumped_at}`, so all of this stays off
  the Postgres hot path.

- **Read-only scope.** `token_for(id, scope: :ro)` mints a token carrying an `"sc": "ro"` claim (a full
  token carries none — absence reads as read-write, so every already-issued token stays full-access).
  On a `ro` stream, `Fathom.ShardExecutor` refuses any write (DML or DDL) with a distinct **403
  `FILO_READONLY`**, so an export/analytics/BI credential can read but never mutate a tenant. The scope
  reaches the executor without a Filo change: `authorize/2` stashes the verified scope and the stream's
  `open` reads it into the connection handle, where it rides across baton-resumes.

Mint, rotate, and revoke are exposed programmatically on the control-plane API: `POST
/api/tenants/:id/token` (mint, `{"scope": "rw"|"ro"}`), `POST /api/tenants/:id/token/rotate`, and
`DELETE /api/tenants/:id/token`. The `/api` control plane authenticates with **scoped API keys**
(an `Authorization: Bearer` key, `read < manage < destroy`, minted via `mix fathom.apikey`),
falling back to the shared admin BasicAuth for backward compatibility (see
[tenant-lifecycle.md](tenant-lifecycle.md)); token routes require the `manage` scope.

## Issuance ledger & fleet-wide revocation (#37)

Everything above covers **one shard**. What was missing was the layer above it, which is the shape
an ordinary month-one incident actually takes: *"a laptop with tokens on it was lost last Tuesday."*
Tokens are stateless `Phoenix.Token`s and only a per-shard `token_version` **floor** was persisted,
so there was no record of which tokens had ever been issued — not who minted one, when, for which
tenant, or with what scope. The only fleet-wide lever was rotating `secret_key_base`, which
invalidates every tenant's token simultaneously: an outage, not a revocation.

**The ledger** (`hrana_token_issuances`, `Fathom.HranaAuth.Ledger`) records one row per mint —
shard, `token_version`, scope, actor, `minted_at`. It stores the **claims, never the secret**:
a Hrana token is verified by signature, not by lookup, so persisting anything derived from the
secret would add a credential to steal while answering no question the claims cannot. (That is
stricter than `api_keys`, which keeps a SHA-256 hash *because* those are looked up.)

Rows are append-only. A revoke does not delete history, it moves the floor — so
`Ledger.outstanding/1` reinterprets the same rows against the current floor, and
`Ledger.history/1` keeps the audit trail intact.

**Recording never fails a mint.** `mix fathom.token` runs with config only and no `Repo`, and a
Postgres blip must not stop an operator issuing a credential. A skipped write logs a warning. The
consequence is that an incomplete ledger **under-reports** what is outstanding — which is the safe
direction, because the bulk revoke below then revokes less than it might, never more.

**Time-scoped bulk revoke:**

```elixir
# Everything minted before the incident window, paced through Oban's :tokens queue.
Fathom.HranaAuth.revoke_issued_before(~U[2026-08-01 00:00:00Z])

# Confirm the blast radius first, or revoke inline on a small fleet.
Fathom.HranaAuth.revoke_issued_before(cutoff, limit: 10)
Fathom.HranaAuth.revoke_issued_before(cutoff, async: false)
```

It bumps the floor on every shard the ledger shows with an **outstanding** token issued before the
cutoff, and is **idempotent** — re-running the same cutoff is a no-op rather than a second round of
client disconnects, which is what makes it safe to retry or run from a cron.

It deliberately depends on the ledger, so a mint that failed to record is invisible to it. The
alternative — revoking every shard regardless — is the `secret_key_base` outage this exists to
avoid. That nuclear option still exists; this is the scalpel, and a scalpel that silently widened
its own incision would be worse than none.

**Tokens now have to expire in prod.** `:hrana_token_max_age` unset with `:hrana_auth` `:required`
used to warn; it now **refuses to boot** in prod, matching `check_template_default!`. The warning
was right while rotation still meant an outage — it no longer does (`rotate/1` keeps the previous
version valid for `:hrana_rotation_grace_ms`), so there is no longer a good reason to run
`:required` with immortal tokens. To keep long-lived tokens deliberately, set a large
`HRANA_TOKEN_MAX_AGE` — an explicit choice in config rather than an unnoticed default. Dev and test
still only warn.

## Control-plane auth throttle (#34)

The shared admin BasicAuth (`ADMIN_USER` / `ADMIN_PASS`) is the one password on the platform, so
the router **locks out a source IP with 429** after `:admin_auth_max_failures` failed attempts
within `:admin_auth_window_ms` — on by default in prod (`ADMIN_AUTH_MAX_FAILURES=20`, 5-min window;
a successful auth clears the count) — and the `/api` surface has its own per-IP rate limit
(`:api_rate_limit`, `API_RATE_LIMIT=120`/min, checked *before* auth). The Hrana *token* path is
HMAC-verified and not brute-forceable, so these guard the password surface, not the data path. The
lockout transition is audited (`admin_auth_locked_out`) and attempts emit
`[:fathom, :admin_auth, :failed|:blocked]` telemetry. Knobs: [configuration.md](configuration.md).

## The trust boundary when auth is disabled

With `:disabled` (the dev default, and a valid prod posture behind a trusted LB), **the network is
the credential**: the Hrana port must be reachable **only via the load balancer** — put it behind a
firewall / security group / private subnet and pin the interface with `HRANA_BIND_IP` so it never
binds a public one. A client that can reach the port can open any shard; the whole security model in
this mode is "only the LB can reach the port."

## `/api` CSRF: state-changing calls must declare JSON

`/api` accepts either a Bearer API key or the shared admin BasicAuth. Browsers cache Basic
credentials per **origin** and re-send them on cross-site form submissions — `SameSite` governs
cookies, not the HTTP auth cache — so an auto-submitted
`<form method=POST action="https://<node>:4000/api/tenants/<victim>/suspend">` was authenticated,
needed no token and no attacker credential (expert review 2026-08-01 #27).

State-changing `/api` requests on the **BasicAuth fallback** must therefore send
`Content-Type: application/json`, and a request carrying `Sec-Fetch-Site: cross-site` is refused
outright. Both return **403**.

This closes form-driven CSRF rather than merely making it harder: an HTML form can only send
`application/x-www-form-urlencoded`, `multipart/form-data` or `text/plain`. Sending JSON
cross-origin requires fetch/XHR, which triggers a CORS preflight this endpoint answers no headers
for.

**Bearer API keys are exempt** — a browser never auto-attaches one, so the whole attack shape does
not exist for them. `GET`/`HEAD`/`OPTIONS` are unaffected.

If you drive `/api` with BasicAuth from curl or a script, add
`-H 'content-type: application/json'`. Using an API key instead is the better fix and needs no
header.

## One-line summary

Auth is a per-shard `Phoenix.Token` presented as libSQL's native `authToken` (Bearer over HTTP,
`hello.jwt` over WS) and checked on Filo's `:authorize` callback — required or disabled via a runtime,
fail-closed `:hrana_auth` mode — so an unchanged client authenticates per tenant with no oracle; when
it's disabled, the trust boundary is LB-only network reachability.
