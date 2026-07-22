# Runbooks — fathom cluster (LB-keyspace-partition)

Operational responses for the multi-node cluster phase. Architecture: `docs/deploy-cluster.md`.
Observability is `Fathom.Telemetry` — `:telemetry` events → `Telemetry.Metrics` (export with a
reporter) and an OpenTelemetry trace span on checkout (OTLP, env-gated).

This file covers **lease / ownership** incidents (stuck-unavailable, split-brain, S3 down). For the
non-lease classes — **Postgres outage, disk full, heartbeat loss, dual-LB double-routing, and the
shard restore drill** — plus the per-dependency **fail-open/fail-closed matrix**, see
[`operations.md`](operations.md). For node upgrade/restart, see [`deploy.md`](deploy.md). Shippable
alert rules + dashboards for these signals live in [`deploy/observability/`](../../deploy/observability/).

## Metrics (from `Fathom.Telemetry.metrics/0`)

| Metric | Type | Meaning |
|---|---|---|
| `fathom.shard.cold_open.duration` | distribution (ms), tag `warm` | Cold-open latency. `warm=false` = pulled from S3; `warm=true` = local file present. |
| `fathom.shard.lease.acquired.count` | counter | Lease acquisitions (first open + steal-on-lapse). |
| `fathom.shard.lease.renewed.count` | counter | Lease renewals — **the per-node S3 lease RPS** (F1: watch this vs active-shard count at scale). |
| `fathom.shard.lease.superseded.count` | counter | Self-fences — a node lost a lease it held. **Sustained > 0 across nodes ⇒ ownership churn / suspected split-brain.** |
| `fathom.shard.lease.held.count` | counter | Starts refused against a live foreign lease (a request hit a non-owner — expected briefly on remap). |
| `fathom.shards.checkout.stop.duration` | distribution (ms), tag `outcome` | Checkout latency by outcome: `ok` / `held` / `unavailable` / `error`. |
| `fathom.shards.active` | last_value | Active shard coordinators on this node. |
| `fathom.rebalancer.move.proposed.count` | counter | Shard moves the policy proposed (handoffs enqueued) — rebalancer activity rate. |
| `fathom.rebalancer.affinity.count` | counter, tag `outcome` | Move target affinity: `hit` = landed on a warm target, `miss` = cold. The #C warm-hit rate. |
| `fathom.rebalancer.handoff.stop.count` | counter, tag `outcome` | Handoff terminal outcome: `completed` / `reverted`. **The core health signal.** |
| `fathom.rebalancer.handoff.retry.count` | counter | Handoff attempts that retried (flip not live / slow drain) — thrash precursor. |
| `fathom.rebalancer.command.stop.count` | counter, tags `command`,`outcome` | Warm/drain command outcomes: `done` / `failed` / `cancelled`. `drain`+`failed` = thrash. |
| `fathom.rebalancer.lb_apply.count` | counter, tag `outcome` | LB-map apply: `applied` / `noop` / `reload_failed` / `config_test_failed` / `write_failed`. **Routing-at-risk.** |
| `fathom.rebalancer.reconcile.unpinned.count` | counter | Pins dropped because their node went dead (#1b) — dead-node reconcile rate. |

**Export.** Metrics: pass `Fathom.Telemetry.metrics/0` to a reporter
(`TelemetryMetricsPrometheus`, `TelemetryMetricsStatsd`, or a `ConsoleReporter` in dev) — none
is started by default; wire the one your backend uses. Traces: set
`OTEL_EXPORTER_OTLP_ENDPOINT` (and optional `OTEL_EXPORTER_OTLP_HEADERS`); off otherwise.

## Alerts (define these in your backend)

- **shard-stuck-unavailable:** `rate(fathom.shards.checkout.stop{outcome="unavailable"|"error"}) > 0` for 2m.
- **suspected-double-write-on-remap:** `rate(fathom.shard.lease.superseded) > 0` sustained for 5m (a steady self-fence rate is churn, not a one-off remap).
- **lease-store-down:** `rate(fathom.shards.checkout.stop{outcome="error"})` spikes fleet-wide AND S3 error logs present.
- **cold-open-latency:** `p99(fathom.shard.cold_open.duration{warm="false"})` above your failover SLO (tune against the S3 region RTT).
- **lease-RPS-ceiling (F1):** `rate(fathom.shard.lease.renewed)` approaching the S3 PUT budget — the node-level-heartbeat lever (S8) is the fix.

### Rebalancer alerts (when B1 is enabled — see `docs/runbooks/rebalancer.md`)

- **handoff-revert-rate:** `rate(fathom.rebalancer.handoff.stop{outcome="reverted"})` sustained > 0 — handoffs are failing to drain and reverting; a shard is wedged (#4). Pair with `handoff.retry` rising.
- **rebalancer-thrash:** `rate(fathom.rebalancer.command.stop{command="drain",outcome="failed"})` sustained — un-drainable hot shards; check whether the cooldown/backoff is holding.
- **lb-routing-at-risk:** `rate(fathom.rebalancer.lb_apply{outcome="reload_failed"|"config_test_failed"|"write_failed"}) > 0` — the LB map couldn't be applied; the running LB may be stale and the next nginx cold start could fail (#3/#11). Investigate immediately.
- **dead-node-reconcile:** `rate(fathom.rebalancer.reconcile.unpinned) > 0` — nodes going dead with pinned shards; correlate with node liveness / deploys (#1b).
- **affinity-hit-rate (informational):** `fathom.rebalancer.affinity{outcome="hit"} / total` — how often handoffs land on a warm target (#C); a persistently low rate means the warm signal isn't helping (warm-follower off, or cap-evicted).

---

## Runbook: a shard is stuck unavailable

**Symptom:** clients for one subdomain get errors; `checkout` outcome `unavailable` or `error`
for that shard; LB health for the owning node may be green (the node is up, the shard isn't).

**Diagnose:**
1. Which node does the LB map the subdomain to? (consistent hash on `Host`.)
2. On that node: is the coordinator alive? Is it cold-opening (pull from S3 slow)? Check
   `cold_open.duration` and S3 latency.
3. Read the `.lock` object in S3 for the shard — who owns it, and is `expires_at_ms` in the past?
   A live foreign owner means the LB and the lease disagree (a node was removed without the LB
   updating). A past expiry means a steal should succeed on the next request.

**Respond:**
- LB ↔ lease disagreement: fix the LB membership (the removed/replaced node must leave the
  upstream). The new owner steals once the old lease lapses (≤ TTL).
- Slow cold-open: transient (S3 latency) — it resolves; if persistent, check S3 health / region.
- Genuinely wedged coordinator: restart that node; its shards re-home via the lease (steal-on-lapse).

## Runbook: suspected double-write / split-brain on remap

**Symptom:** sustained `fathom.shard.lease.superseded` rate; possibly logs of
`lease superseded by another node; self-fencing`.

**What is and isn't happening:** the lease GUARANTEES single-writer — a self-fence means the
system *prevented* a double-write (the loser stopped without flushing). A *one-off* superseded
on a remap is normal. A *sustained* rate means ownership is thrashing: two things keep claiming
the same shards.

**Diagnose:**
1. Is the LB stable? Flapping membership (a node failing health checks intermittently) remaps
   subdomains back and forth → repeated steals. Check LB upstream health history.
2. Are two LBs / two upstream configs pointing at overlapping node sets with different hashes?
3. Clock skew between nodes (the steal decision compares the reader's wall clock to the owner's
   heartbeat expiry). Skew beyond `steal_margin_ms` (default 5s) can make a node think a live
   owner expired and steal it. Check NTP; if skew is real and can't be tightened, raise
   `steal_margin_ms` (at the cost of slower hard-crash failover).

**Respond:**
- Stabilize LB membership / health-check thresholds (raise `fail_timeout` to stop flapping).
- Reconcile to one authoritative LB config / hash.
- Fix clock skew (NTP). **Data is safe throughout** (the fence held); this is an availability /
  churn problem, not a corruption one. Confirm with the chaos/isolation test (S6).

> **NTP also matters for the rebalancer (expert review #15).** Its per-node load samples
> (`shard_load_samples.sampled_at`) are reporter wall clocks, and the cross-node reads that
> pick a shard's "current" node and prune old samples compare them. A few seconds of skew is
> fine (small vs the ~10s window); material skew can mis-attribute a shard's serving node or
> bias load off a slow-clock node. Same fix: keep NTP healthy. Data is never at risk (the S3
> `{owner,epoch}` lease, not the samples, arbitrates writes).
>
> The same applies to the node-liveness beat (`rebalancer_nodes.last_seen_at`, review #9). The
> once-**destructive** risk — a clock-lagged node judged dead and its hot pins deleted — is
> closed: the dead-node reconciler confirms against the S3 lease (`Storage.lease_holder/1`)
> before unpinning (review #1). The residual under skew is benign (a fast-clock dead node's pin
> lingers until its timestamp passes; its shards stay available via the #1a backups + freed lease).

## Runbook: lease store (S3) is down

**Symptom:** fleet-wide — many shards fail to acquire/renew; `checkout` outcome `error`
spikes everywhere; S3 errors in logs. This is the cluster-wide SPOF.

**Diagnose:** confirm it's S3 (not one node): the failure is fleet-wide and correlates with S3
error responses. The lease deliberately **fails closed** — a node that can't reach S3 won't open
or renew (it would rather be unavailable than risk a split-brain).

**Respond:**
- This is an S3/object-store incident — escalate to the storage provider / check the bucket,
  region, credentials, network path.
- Do NOT bypass the fence (no unconditional writes) — that's the split-brain the design avoids.
- Shards already open keep serving until their lease lapses (≤ TTL); new opens wait for S3.
- When S3 recovers, opens resume automatically; no manual shard intervention needed.

## Failover herd + warm-standby sizing

When a node dies, **all** its tenants re-home onto survivors at once. Measured on the rig
([`../reviews/failover-herd-2026-07-16.md`](../reviews/failover-herd-2026-07-16.md), N=300): the herd
re-homes **cleanly** (300/300 served, no S3-throttle collapse, no survivor memory spike — the
single-writer lease and the bounded Finch pool make it safe by construction). What you tune is the
**latency** of the tail:

- **Time-to-served is floored by the lease**, not the herd: `SHARD_LEASE_TTL_MS` + steal margin
  (~15 s in the rig) is how long a *silent-killed* owner's shards stay unstealable while its
  heartbeat lapses. Lower `SHARD_LEASE_TTL_MS` for faster crash failover (at the cost of more false
  steals under clock skew / load). A **graceful** node stop releases leases immediately — no floor
  (see [`deploy.md`](deploy.md)).
- **The concurrent cold-open spread above the floor is small** (~1.3 s for 300 shards) — the pool
  absorbs the herd. It grows with `active_shards_per_node / s3_pool_size × RTT`; size the S3
  `pool_size` for your density.

**When to enable the warm follower (`WARM_FOLLOWER=true`).** The warm win is the object **body
transfer avoided** on failover, so it's marginal for tiny shards (the lease floor + ~1 RTT dominate;
the rig measured ~0.5 s saved for one-row shards) and grows with shard size. Enable it when the
cold-pull herd would move real bytes — roughly when

```
active_shards_per_node × avg_shard_size / S3_bandwidth   ≳   your failover-RTO budget
```

i.e. large shards (MB+) at high density (thousands/node). For KB-scale shards, leave it off — the
follower's disk + revalidation cost buys almost nothing. **Size `:warm_cache_max`** so a survivor can
hold a full dead peer's active set: `:warm_cache_max ≥ active_shards_per_node` (a warm copy costs
its file on disk plus ~0 process/fd — it's disk-bound, so err generous). Watch
`fathom.shard.warm.promoted{result="hit"}` to confirm failovers are landing on the warm path.

## Graceful drain (rolling-deploy pre-stop) — audit #28

A deploy or scale-down should NOT funnel through the crash-adjacent supervisor-shutdown path (burst
flushes, hard-cut live streams, and — on ephemeral disk — real loss). `Fathom.Shards.drain_all/1` is
the node-scope voluntary drain: it marks the node draining so `/health` returns **503** (the LB
deregisters it and stops routing here first), then fans out `request_drain` across every open
coordinator — refuse new checkouts → let in-flight streams finish → flush + release the lease + stop —
with bounded concurrency and a bounded overall budget. It returns `%{drained, busy, timed_out}`.

**Wire it as a release pre-stop, ahead of SIGTERM.** In your deploy orchestration (or a
`rel/env.sh.eex` pre-stop hook), before stopping the node:

```sh
# 1. Stop the LB routing here + drain the open shards within a bounded budget.
bin/fathom rpc 'IO.inspect(Fathom.Shards.drain_all())'
# 2. THEN send SIGTERM / stop the release.
bin/fathom stop
```

Set `:drain_lb_settle_ms` to your LB's health-probe interval so `drain_all` waits for the LB to
notice the 503 before it drains (otherwise a request can still land mid-drain). Tune
`:drain_all_budget_ms` (default 55s, keep under your orchestrator's SIGTERM grace),
`:drain_all_concurrency` (default 16 — the fan-out width, so the drain doesn't fire every shard's
flush at once), and `:drain_all_flush_grace_ms` (default 5s — the tail a busy shard needs to finish
its final flush before the deadline). A clean drain reports `busy: 0, timed_out: 0`; a `timed_out`
shard is left to the supervisor shutdown (raise the budget or lower per-node density). This is the
structural companion to the busy-terminate flush fix (audit #2): together, a clean shutdown's
deploy-time loss window is zero. Validating it against the chaos rig's rolling-deploy scenario and
measuring the window with `mix fathom.rpo` is a follow-up.
