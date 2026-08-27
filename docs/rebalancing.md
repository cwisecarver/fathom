# Fathom — dynamic rebalancing (how the built engine works)

> Status: **BUILT** (Phase 2 B1, `Fathom.Rebalancer.*`). All gates are **off by default**. This
> is the "how it actually works" reference; the scoping/decisions are `docs/phase2-scoping.md` §B1,
> the staged enable procedure is `docs/runbooks/rebalancer.md`, and the live proofs are in
> `docs/reviews/chaos-run-2026-07-08.md`.

## The problem it solves

fathom routes by **LB-keyspace-partition**: the load balancer consistent-hashes a tenant's `Host`
subdomain to one backend node, so every shard has a single deterministic **home**. That's fine
until one tenant gets *hot* — a shard is one SQLite file with one writer, so you **cannot spread a
single hot tenant across nodes**, and the consistent hash is static (it knows nothing about load).
Rehashing to relieve a hot node would move *everyone*. What you want instead is a **targeted
exception**: pick up the one persistently-hot shard and set it down on a less-loaded node, leaving
the rest of the hash untouched.

## The hard constraint: no BEAM cluster

Nodes don't talk to each other. The architecture coordinates only through **S3** (shard data + the
`{owner, epoch}` lease) and **Postgres** (orchestration), never a BEAM ring or node-to-node RPC. So
the rebalancer is built entirely on those surfaces: Postgres to decide, S3's lease to hand the shard
over safely, the LB to reroute. Three stages — **detect → decide → execute** — each of which works
without a node ever calling another node directly.

## 1. Detect — every node reports into a Postgres fleet view

Each node keeps `Fathom.ShardLoad`: a public ETS table of per-shard counters (checkouts, queries,
`rows_read`/`rows_written`), bumped **lock-free from the executing process** (`:ets.update_counter`
with `write_concurrency`), so recording load costs nothing on the hot path. `Rebalancer.Reporter`
diffs two `ShardLoad` snapshots into **rates** and writes that node's hot set to `shard_load_samples`
in Postgres, tagged with a stable `Rebalancer.node_key/0`. The fleet load picture assembles itself
in Postgres from independent per-node reports — no cluster required. (`:shard_load` and
`:load_reporter` are both gated **off** by default; nothing in the request path pays for an unread
counter until the rebalancer is armed.)

## 2. Decide — one elected node runs the policy

`Rebalancer.RebalanceJob` is an Oban cron that runs as a **fleet singleton** (Oban's Postgres peer
leadership elects exactly one node to decide), gated `:rebalancer_enabled`. It reads the merged
fleet view and runs `Rebalancer.Policy`. The load-bearing lesson from the `--hotspots` work is in
the "hot" test:

- **Hot = an absolute q/s floor, or `K × fleet-p99` — never `K × median`.** At fleet scale the long
  cold tail pulls the median to ~0, so a median-multiple flags hundreds of false hotspots. The
  **p99-relative** bar self-scales: a uniform fleet has p99 ≈ max, so `K × p99` flags nothing (no
  false move); a sharp Zipf head has a low p99, so the real head clears the bar. The `:fleet_p99`
  is the *true pooled-distribution* p99 supplied by the orchestrator (`Rebalancer.Nodes.fleet_p99/2`),
  **not** computed from the truncated top-N head the reporter publishes (which would systematically
  under-flag). If there's no reliable `:fleet_p99`, the policy makes **no** p99-relative move rather
  than a bad one.

Then the anti-thrash guards:

- **2-window anti-flap** — a shard must read hot in `:rebalance_confirm_windows` (default 2) distinct sample windows (de-duped by `sampled_at`), not a single spike.
- **Cooldown** — a shard pinned within `cooldown_ms` is skipped, so it isn't ping-ponged.
- **Improvement guard** — never relocate a *lone* hotspot (moving it just moves the problem); only
  move when fleet balance genuinely improves.
- **Least-loaded target**, plus **affinity (Phase 2 C, folded in):** among targets whose load is
  within `band × q` of the least-loaded (`:rebalance_locality_band`, default 0.5), prefer one that
  already holds the shard **warm** (`Rebalancer.WarmLocations` / `shard_warm_locations`) — a cheap
  304 handoff instead of a full S3 pull, while balance still improves.

If it decides, it enqueues a unique-per-shard `HandoffJob`.

## 3. Execute — the handoff, and why the ordering is safe

`Rebalancer.HandoffJob` runs a specific sequence: **warm the target → pin + flip the LB → drain the
source → target acquires.**

1. **Warm** the target (pre-pull the shard so the flip lands on a warm copy).
2. **Flip** — write the `shard_overrides` row (Postgres), render the nginx exception map
   (`Rebalancer.LbMap`), reload the LB (`Rebalancer.LbApply`).
3. **Drain** the source — it checkpoints, flushes, and **releases the lease**.
4. The target's next request (already routed there) **acquires the freed lease** and serves from the
   warm cache.

The ordering is **flip-before-drain**, and that is safe rather than a split-brain risk because the
`{owner, epoch}` lease blocks any double-write *regardless* of routing — a healthy node **cannot be
stolen from**. So this is a **voluntary drain** (the source releases), the cooperative direction of
the same lease the crash path uses to *steal* a dead node's shard. Flipping first stops new traffic
hitting the source, which is exactly what makes draining a *hot* shard finish quickly; in the brief
gap, a request landing on the target finds the source still holds the lease (`acquire → {:held}`)
and briefly retries. **Failure handling is careful:** if the flip can't apply, the drain is
**skipped** (don't strand the source); if it still fails on the last attempt, the pin is
**reverted** so traffic returns to the source — the shard is never left pinned-and-unavailable.

## Reaching a node the orchestrator can't call

"Warm the target" and "drain the source" are instructions for *other* nodes, which the orchestrator
can't RPC. They travel over a **`rebalance_commands` Postgres channel** + a per-node
`Rebalancer.CommandPoller` (each node polls Postgres for commands addressed to it, gated
`:command_poller`). The LB reload itself: the app writes the exception map to a shared dir; on the
chaos rig an `lb-reloader` sidecar shares nginx's PID namespace and HUPs the master on a map change
(no host bridge), while a real deploy uses `LB_RELOAD_CMD`. Everything routes through Postgres + a
shared file + the lease — never node-to-node.

## Safety, gating, and proof

Isolation holds throughout (one file, lease-guarded single-writer); no double-write (lease + epoch
fence); anti-flap + cooldown + the improvement guard prevent oscillation; **all gates off by
default**, with a staged, observe-before-arming enable procedure and rollback in
`docs/runbooks/rebalancer.md`. **Proven live** on the rig (`deploy/chaos/chaos.sh rebalance`): a
manual pin (`acme` fathom1→fathom2) and a **fully autonomous** run where `Policy.propose` chose
`green: fathom2→fathom1` off an engineered imbalance and `RebalanceJob`/`HandoffJob` executed it —
isolation intact both times (`docs/reviews/chaos-run-2026-07-08.md`). Hardened per the 2026-07-07
expert-panel pass (18 findings; report kept locally, not in the repo).

## One-line summary

Each node reports its hot shards into Postgres; one elected node decides (p99-relative / absolute-
floor hotness with anti-flap, cooldown, improvement, and warm-affinity guards); a handoff job warms
the target, flips the LB exception map, and drains the source's lease so the target picks it up —
all coordinated through Postgres, S3, and the LB, because there is no BEAM cluster to coordinate
through.
