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
Tokens **don't expire by default**; set `:hrana_token_max_age` to bound their lifetime.

## The trust boundary when auth is disabled

With `:disabled` (the dev default, and a valid prod posture behind a trusted LB), **the network is
the credential**: the Hrana port must be reachable **only via the load balancer** — put it behind a
firewall / security group / private subnet and pin the interface with `HRANA_BIND_IP` so it never
binds a public one. A client that can reach the port can open any shard; the whole security model in
this mode is "only the LB can reach the port."

## One-line summary

Auth is a per-shard `Phoenix.Token` presented as libSQL's native `authToken` (Bearer over HTTP,
`hello.jwt` over WS) and checked on Filo's `:authorize` callback — required or disabled via a runtime,
fail-closed `:hrana_auth` mode — so an unchanged client authenticates per tenant with no oracle; when
it's disabled, the trust boundary is LB-only network reachability.
