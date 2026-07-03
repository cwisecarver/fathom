# Deploying fathom as a multi-node cluster (LB-keyspace-partition)

This is the deploy + rollback guide for the cluster phase. The design rationale and the
review trail live in the office-hours/CEO/eng design doc; this is the operator's view.

## The model in one paragraph

Fathom runs as **N independent single-node instances**. There is **no inter-node BEAM
cluster** — nodes never talk to each other directly; they coordinate only through S3. An
**L7 load balancer is the placement authority**: it consistent-hashes the request `Host`
(e.g. `acme.fathom.example`) to one backend node, so every request and WebSocket for a
subdomain lands on the same node. That node owns and serves the shard via the normal
single-node path. The **S3 lease** (already built into `Fathom.Shard` / `Storage.S3`) is
the safety fence: only one node can hold a shard's lease at a time, so even during a brief
LB remap window there is exactly one writer.

```
        acme.fathom.example                    LB places, lease fences.
        beta.fathom.example          ┌──────────────────────────────────────┐
   client ──TLS──▶  L7 LB  ──hash($host) consistent──▶  fathom node 1 ─┐    │
   (HTTP or WS)     (nginx/HAProxy/Fly)                  fathom node 2 ─┤    │
                       │  health-check failover          fathom node 3 ─┘    │
                       │                                        │  per-shard  │
                       │                                        ▼  S3 lease   │
                       └────────────────────────────────▶   S3 (shards + .lock)
                                                          (the ONLY cross-node
                                                           coordination)
```

## Placement vs safety (why this is correct, not just convenient)

- **LB = placement.** Consistent hashing maps `acme.*` → node deterministically. Adding or
  removing a node remaps only ~1/N of subdomains (ketama), so a scale change does **not**
  stampede S3 cold-opens. (Naive `mod N` hashing would remap nearly everything — do not use
  it.)
- **Lease = safety.** When the LB moves `acme` from a dead/old node to a new node, the new
  node calls `acquire_lease`. If the old node is dead, its lease lapses after the TTL and the
  new node steals it (epoch+1). If the old node is briefly still alive, it self-fences the
  moment its renewal comes back `:superseded` (`Fathom.Shard`, `shard.ex`). Two nodes can
  never both write `acme`. Worst case is a **TTL-bounded** handoff stall, not a double-write.
- **Isolation** (the release-blocker invariant) is the LB partition (one subdomain → one
  node) **plus** the lease (one writer per shard). A query for shard A can never reach shard
  B's node and can never write B's file.

This was validated end-to-end by the S1 spike (2026-06-30, `scratchpad/s1-lb-spike`):
deterministic subdomain→node, **WS routes identically to HTTP**, ~1/N remap on node add,
and in-request failover on node death.

## Trust boundary and authentication

**By default the Hrana data path (port 8080) carries no in-app credential.** A request's
shard is taken from the `Host` subdomain the LB sets — no bearer token, no JWT check, no
per-tenant secret. Isolation is **placement + lease** (above), not identity. In-app
per-shard auth exists and is opt-in — see "Per-tenant credentials" below.

The consequence, and the operator's responsibility: **the Hrana port MUST be reachable only
through the LB.** Anything that reaches a node directly (a leaked node address, SSRF, a
misrouted internal request) can set an arbitrary `Host` subdomain and open the matching
shard. Enforce this with the network — a firewall / security group / private subnet that
admits only the LB to 8080 — and, as defense-in-depth, pin the listener to the private
interface the LB reaches:

    HRANA_BIND_IP=10.0.0.7   # binds Hrana to that interface only (config/runtime.exs)

Unset, the listener binds all interfaces (`0.0.0.0`), which is fine only when the network
already restricts reachability. The health port (8081) and the dashboard (4000) are separate
listeners off the data path.

**The `?db=` / `x-fathom-shard` shard-selection fallbacks are dev-only.** They let a request
with no routable subdomain name a shard directly (curl/testing). They are gated by
`config :fathom, :allow_shard_override` — **off in prod** (on in dev/test) — so in production
a request that reaches a node without a valid subdomain resolves to the default shard, not an
attacker-named one. Real clients address by subdomain and are unaffected.

**Per-tenant credentials (built, opt-in).** When network-only isolation is not enough
(e.g. 8080 must be exposed beyond a trusted LB, or tenants need revocable credentials),
turn on in-app bearer-token auth:

    HRANA_AUTH=required            # every stream open must present a token (401 otherwise)
    HRANA_TOKEN_MAX_AGE=86400      # optional expiry in seconds; unset = non-expiring

`Fathom.HranaAuth` verifies a `Phoenix.Token` (signed with `SECRET_KEY_BASE`) that binds
the caller to exactly one shard. Clients pass it as libSQL's `authToken` and it reaches
the server two different ways: HTTP requests carry `Authorization: Bearer <token>`, but
the WebSocket clients (django-libsql) send it **only as the `jwt` field of the Hrana
`hello` message** — no upgrade header — which is why this is implemented as `Filo.Plug`'s
`:authorize` callback (checked at every stream open / WS hello, before the executor runs)
rather than the pre-plug originally scoped here. Mint a token with `mix fathom.token
<shard>` (dev) or `Fathom.HranaAuth.token_for/1` from a release's remote console; revoke
by rotating `SECRET_KEY_BASE` (or set an expiry). A boot guard refuses `HRANA_AUTH=required`
without a usable secret, and an unrecognized `:hrana_auth` value fails closed to required.
Auth on the template shard also removes the capture-poisoning caveat on setting a prod
`:template_shard_id` (finding #17).

## Running a node

Each node is an ordinary fathom release with the Hrana listener on (`hrana_server: true`,
default `hrana_port: 8080`) and the S3 storage backend configured so every node sees the
same shard bucket:

```elixir
# config/runtime.exs (per node — identical across the fleet)
config :fathom, :shard_storage, Fathom.Shard.Storage.S3
config :fathom, Fathom.Shard.Storage.S3,
  bucket: System.fetch_env!("FATHOM_S3_BUCKET"),
  region: System.fetch_env!("FATHOM_S3_REGION"),
  access_key_id: System.fetch_env!("AWS_ACCESS_KEY_ID"),
  secret_access_key: System.fetch_env!("AWS_SECRET_ACCESS_KEY")
config :fathom, hrana_server: true
config :fathom, health_server: true   # GET /health on :health_port (default 8081)
```

Nodes are stateless beyond their **local scratch** (the pulled shard files under
`:shard_data_dir`) and the **shared S3 bucket**. A node can be added, killed, or replaced at
will; correctness is held by S3 (durability) + the lease (single-writer).

## The load balancer

Start from [`deploy/lb/fathom.nginx.conf`](../deploy/lb/fathom.nginx.conf). The
load-bearing pieces:

- `upstream fathom_hrana { hash $host consistent; server <node>:8080; ... }`
- `proxy_set_header Host $host;` — the Host is BOTH the hash key AND how fathom picks the
  shard (`ShardExecutor.shard_from_conn`). Never rewrite it.
- WebSocket upgrade headers + a long `proxy_read_timeout` (Hrana WS streams are long-lived).
- `proxy_next_upstream` for in-request failover.

HAProxy equivalent: `balance hash` with `hash-type consistent` and `option httpchk`.
Fly.io: `fly-replay` to route by app/instance, or a Fly-side L7 with the same hash.

## Health checks

Fathom exposes a **liveness endpoint**: `GET /health` → `200 "ok"` on each node's health
port (`:health_port`, default **8081**, separate from the `:8080` Hrana port and gated by
`:health_server`, off in test). It is liveness only — it does NOT touch S3, so an LB probe
adds no shard-storage load and a transient S3 blip never flaps a whole node out of rotation
(a node cut off from S3 self-fences its individual shards via the lease, the right
granularity). Implemented in `Fathom.HealthPlug` + a Bandit listener in `Fathom.Application`.

- **Passive health (open-source nginx baseline):** nginx marks a node down after `max_fails`
  connection failures and reroutes; `proxy_next_upstream` retries the same request elsewhere.
  The S1 spike confirmed a killed node fails over with the client seeing no error. OSS nginx
  has no active HTTP health check, so this is the nginx baseline.
- **Active health (recommended, catches a hung-but-listening node):**
  - **HAProxy:** `option httpchk GET /health` + `server node1 10.0.0.11:8080 check port 8081`
  - **nginx Plus:** `health_check uri=/health port=8081;`
  - **Fly.io / k8s:** a TCP or HTTP `GET :8081/health` liveness probe per instance.

  Active health detects a node that is listening but not serving (which passive health only
  catches on the next failed request). Pairs with the S5 observability metrics.

## Scaling and rebalancing

- **Add a node:** add its `server` line to the `upstream` block, `nginx -s reload`. ~1/N of
  subdomains remap; each remapped shard is migrated lazily — on its next request the new node
  cold-opens it from S3 and steals the lease. No coordinated migration, no downtime for
  unaffected subdomains.
- **Remove a node (planned):** drain by removing its `server` line and reloading; its
  subdomains remap to survivors and cold-open on next touch. (Graceful drain of in-flight
  streams is the node's own idle/flush path.)
- **Distribution skew (real caveat, from the S1 spike):** consistent hashing balances load
  only *in expectation*. With few nodes and few subdomains it can be lumpy (the spike saw
  3/8/1 across three nodes for twelve keys), and a single hot tenant always pins to one node
  (inherent to single-writer-per-shard). At fathom's target scale (many subdomains over N
  nodes) it evens out, but **monitor per-node shard count + load** (the S5 placement-balance
  metric) and use the Phase-2 weighted/rebalance lever for persistent hot spots. Do not
  assume even load on a small cluster.

## S3 as the coordination substrate — cost and dependency model

S3 is the *only* cross-node coordination (leases, the node heartbeat, and the shard
data all live there). Two consequences worth owning explicitly rather than discovering.

**Request cost.** Each storage operation is a fixed number of S3 requests (from the
`Storage.S3` backend):

| Operation | Requests | Notes |
|---|---|---|
| `pull` (cold-open body) | 1 GET | shard `.db` bytes |
| `flush` (idle or durability) | 1 PUT | shard `.db` bytes |
| `acquire_lease` — cold (uncontended) | 1 PUT | optimistic `If-None-Match:*` create |
| `acquire_lease` — reclaim own expired | 3 | GET + `If-Match` PUT (+ resolve) |
| `acquire_lease` — steal foreign expired | 4 | GET lock + GET heartbeat + `If-Match` PUT (+ resolve) |
| `renew_lease` (legacy mode only) | 2 | GET + `If-Match` PUT |
| `check_lease` (heartbeat-mode fence) | 1 GET | read-only ownership confirm |
| heartbeat renew | 1 PUT | **per node**, every `ttl/3` (F1) — O(nodes), not O(shards) |

A typical **cold-open** is ~1 PUT (acquire) overlapped with 1 GET (pull) — ~2 requests,
~1 RTT wall-clock. Order-of-magnitude on S3-standard pricing (GET ~$0.40 / M, PUT
~$5.00 / M; R2/Tigris differ, and none of this counts byte transfer, which is free
intra-region and billed on egress):

- **Cold-opens:** ~$5.40 per **million** (1 PUT + 1 GET each). This is the line item that
  scales with churn — a workload that constantly opens cold shards from S3 pays here.
- **Heartbeats:** 1 PUT/node every `ttl/3` (default 10s) ≈ 8.6k PUT/day/node ≈ **$0.04/day/node**. Negligible — that's the whole point of the F1 heartbeat replacing per-shard renewal.
- **Durability flushes:** 1 PUT per *dirty* shard per `:shard_flush_interval_ms`
  (write-gated — clean shards skip it). Cost tracks write volume, not open-shard count.
- **Failover steal-storm:** rerouting a dead node's *S* shards ≈ `S × (4 acquire + 1 pull)`
  requests, one-time, bounded by the dedicated S3 Finch pool (`warm_s3_shards_per_s`).

Levers if cold-open request cost dominates: raise `:shard_idle_ms` (fewer open/close
cycles), warm standby (A1, avoids the pull body on failover), and — for byte transfer —
same-region buckets.

**Liveness dependency (owned SPOF).** Because acquire/renew/steal all go through S3,
**S3 availability is fathom's control-plane availability**: during an S3 regional event no
node can acquire or renew a lease, so shards fail *closed* (a node would rather be
unavailable than risk a split-brain — see `docs/runbooks/cluster.md`, "lease store is
down"). This is correct for safety and it is an accepted, stated outage mode, not a bug:
S3 out-availables almost anything else fathom could coordinate through. Already-open
shards keep serving until their lease lapses (≤ TTL); opens resume when S3 recovers.

## Rollback (to single-node, no redeploy)

The whole point of the LB-partition design: rollback is an LB config change, not a deploy.

1. In the `upstream fathom_hrana` block, comment out **all but one** `server` line.
2. `nginx -s reload`.

All traffic now lands on the one remaining node — the pre-cluster single-node topology. The
S3 lease ensures that node safely takes over any shards the others held (steal-on-lapse).
Reverse the change to re-expand. Because there is no BEAM cluster, no app state, and no
schema change, this is fully reversible (5/5) and takes seconds.

## Failure modes (operator quick reference)

| Event | What happens | Operator action |
|---|---|---|
| A node dies | LB reroutes its subdomains (passive health); survivors cold-open + steal the lease (TTL-bounded) | none (auto); investigate the dead node |
| A node hangs (listening, not serving) | passive health won't catch it until a request fails | enable the active `/health` check (HAProxy `option httpchk` / nginx-Plus, see Health checks) |
| Node ↔ S3 partition | that node can't renew → self-fences; its shards stall ≤ TTL then move | alert on S3 errors (S5); check S3/network |
| LB remap (scale change) | ~1/N subdomains move; lazy cold-open on next touch | none; watch the placement-balance metric |
| Lease store (S3) down | fleet-wide: no shard can acquire/renew → "temporarily unavailable" | page; this is the cluster-wide SPOF |

## Chaos testing

Two layers, deliberately split:

- **Invariants — in-process (`mix test`, `test/fathom/cluster/`).** These run on every commit and
  prove the correctness properties deterministically, simulating "two nodes" through shared
  storage (one owner string + one `.lock` per shard):
  - **cross-shard isolation** (`isolation_test.exs`) — the release-blocker gate: shard A never
    reads/writes shard B, concurrently or across a handoff;
  - **single-writer + flushed-data survival + the RPO boundary** (`lease_handoff_test.exs`,
    `crash_contract_test.exs`) — a steal loses only committed-but-unflushed writes;
  - **fail-closed under a lease-store partition + transient-blip tolerance** (`partition_test.exs`,
    via `Fathom.Test.FaultyStorage`).
- **Failover time + loss window — the real rig (manual / CI).** Time-to-serve and the
  committed-but-unflushed loss window are *measurements*, not pass/fail invariants, and they need
  a real deployment: N nodes behind the LB, with a network fault injector (toxiproxy or iptables)
  to (a) kill a node and time the LB reroute + cold-open on a survivor, and (b) partition a
  node↔S3 and confirm it relinquishes (≤ lease-TTL) rather than serving stale. Record the numbers
  against your S3 region RTT and the failover SLO. The in-process tests pin the *safety*; this rig
  measures the *latency*.

## Status

S1 (LB mechanism) — **DONE/PASS**. S2 (LB config + this doc + `/health`) — **DONE**. S3
(lease-handoff tests), S4 (crash/commit-ack-lost contract), S5 (observability + OTel), S6
(chaos/partition + the cross-node isolation gate) — **DONE**. Next: S7 (directory off the hot
path), S8 (lease-RPS measurement + AGENTS.md reconciliation). See the design doc's
`## ARCHITECTURE PIVOT 2` task list.
