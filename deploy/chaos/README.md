# Fathom chaos rig

The "failover time + loss window" layer of `docs/deploy-cluster.md` § Chaos testing,
as a one-host Docker Compose stack: **5 independent fathom nodes** (prod `mix release`
images) behind **nginx** (`hash $host consistent` — the real LB mechanism), sharing one
**MinIO** bucket (lease store + bottomless backend) through **per-node toxiproxy
proxies** (latency / bandwidth / partition injection), plus one **Postgres**
(directory / control plane). The in-process suite (`test/fathom/cluster/`) pins the
safety invariants deterministically; this rig measures latency and demonstrates the
failure modes end to end.

**Why five nodes.** Phase-2 A2 (`docs/a2-quorum-replication.md`) gives each shard a
replica set of 1 primary + 4 read-only followers, acking at 2-of-4 — so five is the
**minimum** fleet that can hold a single shard's replica set, and a 3-node rig cannot
exercise the target topology at all. The earlier 3-node runs recorded in `docs/reviews/`
were measured on that smaller fleet; per the same-topology rule they are not directly
comparable to runs on this one (the LB partition, per-node shard counts, and Oban queue
draw all change with node count). Start a new series rather than extending the old one.

Docker is sufficient here because fathom nodes coordinate **only through S3** — no
BEAM clustering — and node identity is `node()#<boot-nonce>`, so containers are real
independent nodes. Known limits: all numbers are **relative** (one host, loopback
MinIO — inject latency first); a container kill still RSTs sockets (use the
`failover` command's disconnect-then-kill for silent death); real clock skew can't
be produced (the `steal_margin_ms` guard keeps its unit tests).

## Usage

```bash
cd deploy/chaos
./chaos.sh build          # prod-release image (context = parent dir, for the ../filo path dep)
./chaos.sh up             # full stack, waits for health
./chaos.sh latency 30     # 30 ms each way on every node's S3 path — do this before timing
./chaos.sh smoke          # writes/reads a few tenants through the LB + isolation check

./chaos.sh failover acme      # silent-kill acme's owner; time to first acked write on a survivor
./chaos.sh pause-fence acme   # freeze the owner past TTL, steal on a survivor, prove the zombie self-fences
./chaos.sh partition fathom2  # cut fathom2 off S3; observe heartbeat lapse + recovery
./chaos.sh soak 180           # sustained load + node churn; acked-vs-stored + isolation audit
./chaos.sh warm-home acme     # a shard's home node must NOT warm its own shard (survivors do)

./chaos.sh down
```

Tenant = Host subdomain (`acme.fathom.test`); the driver speaks Hrana v2
`POST /v2/pipeline` through the LB on `localhost:8080`. Per-node direct ports
(18081–5) bypass the LB for forced-steal experiments; per-node health is 18091–5 and
each node's admin dashboard is 4001–5. toxiproxy's API is on `:8474`.

## Rig-specific tuning (docker-compose.yml)

`SHARD_LEASE_TTL_MS=10000` (the failover-stall ceiling), `SHARD_FLUSH_INTERVAL_MS=5000`,
`SHARD_IDLE_MS=20000` — shorter than prod defaults so scenarios iterate in seconds.
All nodes run `WARM_FOLLOWER=true` so a failover can land on the warm-promotion (304)
path. The lock/heartbeat fence probe runs at boot against MinIO — if MinIO ever stops
enforcing conditional writes, nodes refuse to serve (that's the point).

## What each scenario demonstrates

- **failover** — LB passive-health reroute + lease steal after silent node death;
  flushed rows must survive (RPO = committed-but-unflushed only). Logs show whether
  the survivor promoted a warm copy (304) or cold-pulled.
- **pause-fence** — the nastiest case: a frozen-not-dead owner. Also demonstrates the
  documented OSS-nginx limitation (passive health can't see a frozen node; the steal
  is forced via a survivor's direct port, standing in for active health checks /
  operator action). The unpaused zombie's flush must self-fence, never overwrite.
- **partition** — node↔S3 cut: heartbeat lapse, fenced flushes, recovery on heal.
- **soak** — write load with node churn; ends with acked-vs-stored accounting and a
  per-tenant foreign-row isolation audit (must be zero).
- **warm-home** — the warm follower should warm a shard only on the *survivor* nodes,
  never on its LB home (a shard fails over away from its home, so the home warming its
  own is wasted cache budget). Checks placement across all five nodes both while the
  home serves the shard (live-coordinator exclusion) and after it idle-drops (the
  `:warm_home_retention_ms` window — the case the home-set fix targets). Home must read
  `warm=no`, survivors `warm=yes`, at both points.
