# Runbook — enabling the B1 dynamic rebalancer

How to turn the Phase-2 B1 rebalancer on in a real fleet, **one gate at a time**, verifying at
each stage, with an observe-before-arming step and a clean rollback. All gates are **off by
default**; the cron is scheduled in every environment but stays inert until `REBALANCER_ENABLED`.

Background: `docs/phase2-scoping.md` §B1, the code in `lib/fathom/rebalancer/`, the 2026-07-07
hardening audit (report kept locally, not in the repo), and the live validation
`docs/reviews/chaos-run-2026-07-08.md`. The chaos rig (`deploy/chaos/docker-compose.yml`) is a
known-good full config to copy from.

> Throughout, `rpc '<expr>'` means run the Elixir expression on a node, e.g.
> `bin/fathom rpc '<expr>'` (a release) — the same mechanism `deploy/chaos/chaos.sh` uses.

> **Where the gates live — there is no prod deploy target in this repo.** Every gate below is a
> **deploy-time environment variable** read by `config/runtime.exs`; this repo ships **no**
> production/staging deploy config (no fly.toml / k8s / terraform / env file) — only the chaos rig
> (`deploy/chaos/`) and the LB config (`deploy/lb/`). "Enable a gate" therefore means setting that
> env var in **your fleet's own env-var management** (the platform that deploys the release) and
> redeploying that stage — not editing a file here. The staged procedure was rehearsed end-to-end
> on the rig with all gates off → Stage 1→4 (see `docs/reviews/chaos-run-2026-07-09.md`); the rig
> `docker-compose.yml` is the known-good full env to copy the variable set from.

---

## Preconditions (do NOT skip)

1. **Trust boundary (expert review #17).** The hot decision trusts `q_per_s`, a signal a tenant
   controls from the data path. Enabling the rebalancer presumes the Hrana boundary is enforced:
   either **LB-only network reachability** (the default `HRANA_AUTH=disabled` posture — see
   `docs/deploy-cluster.md`) or `HRANA_AUTH=required`. Without it, an LB-reachable caller can
   drive a shard they don't own over the bar and induce a handoff (a brief drain blip on the
   victim). **Do not enable on a data path open to untrusted callers.**
2. **NTP (expert review #15).** The per-node load samples order/prune on the reporter's wall
   clock. Keep nodes NTP-synced within a few seconds (small vs the report interval). Data is
   never at risk — the S3 `{owner,epoch}` lease arbitrates writes, not the samples — but skew can
   mis-attribute a shard's current node.
3. **LB wiring.** The LB must `include` the map file the rebalancer writes (`LB_MAP_PATH`), and
   there must be a reload path: either `LB_RELOAD_CMD` (the app can reach the LB) **or** an
   out-of-band reloader that reloads on the file changing (the rig's `lb-reloader` sidecar HUPs
   nginx on mtime). The file must be reachable by both the fathom node (write) and the LB (read)
   — a shared volume / synced path. See `deploy/chaos/nginx.conf` for the `map $host
   $fathom_target` + pin-upstream shape.
4. **Single-writer safety is independent of all of this.** A mis-flip is an availability/thrash
   problem, never corruption: the lease + epoch fence blocks a double-write across every
   interleaving. If in doubt, a shard is at worst briefly unavailable, then re-homes.

---

## Enablement stages

Roll each stage to the **whole fleet**, verify, then proceed. `REBALANCER_ENABLED` is a fleet
singleton (Oban Postgres peer leadership), so it's safe to set everywhere — only the leader acts.

### Stage 1 — topology + counters (no behavior change)

Set on every node and deploy:

```
SHARD_LOAD=true
NODE_KEY=<stable per-node key, e.g. fathom1>          # distinct per node
LB_BACKENDS=fathom1=fathom1:8080,fathom2=fathom2:8080,fathom3=fathom3:8080
LB_MAP_PATH=/etc/nginx/lb/exceptions.conf
LB_RELOAD_CMD="nginx -s reload"                        # OR omit + use an out-of-band reloader
# LB_TEST_CMD="nginx -t -c /etc/nginx/test.conf"       # optional #3 config test; {} → candidate
```

Verify counters are recording on each node:

```
rpc 'Fathom.ShardLoad.top(10) |> inspect() |> IO.puts()'
```

Nothing rebalances yet — `SHARD_LOAD` only turns on the counters (cheap, off the request path).

### Stage 2 — reporting (observe-only; still no moves)

```
LOAD_REPORTER=true
LOAD_REPORT_INTERVAL_MS=10000     # default 10s; the rig uses 3000 for a fast demo
```

The reporter now publishes each node's hot samples + a liveness beat + full-distribution p99 to
Postgres. **This is the watch stage.** Confirm the fleet view is healthy:

```
# All nodes beating (dead-node reconciler input, #1b):
rpc 'Fathom.Rebalancer.Nodes.alive(60_000) |> Enum.sort() |> inspect() |> IO.puts()'
#   => ["fathom1", "fathom2", "fathom3"]

# Hot shards being detected:
rpc 'Fathom.Rebalancer.LoadSamples.latest_per_shard(60_000)
     |> Enum.sort_by(&(-&1.q_per_s)) |> Enum.take(10)
     |> Enum.map(fn s -> {s.shard_id, s.node_key, Float.round(s.q_per_s, 1)} end)
     |> inspect() |> IO.puts()'

# Fleet p99 (nil until there is enough load — the min-sample guard, #2):
rpc 'Fathom.Rebalancer.Nodes.fleet_p99(60_000, 50) |> inspect() |> IO.puts()'
```

**Dry-run the policy** — see exactly what it WOULD move, with `REBALANCER_ENABLED` still off:

```
rpc 'samples = Fathom.Rebalancer.LoadSamples.since(120_000) |> Enum.map(&Map.from_struct/1)
     overrides = Fathom.Rebalancer.Overrides.all()
     backends = Fathom.Rebalancer.lb_backends()
     fleet_p99 = Fathom.Rebalancer.Nodes.fleet_p99(60_000, 50)
     Fathom.Rebalancer.Policy.propose(samples, overrides, backends, fleet_p99: fleet_p99)
     |> inspect() |> IO.puts()'
```

Sit here until the proposed moves match your intuition about which shards are genuinely hot.
If it proposes nothing hot enough to move, that's expected on a balanced fleet.

### Stage 3 — command poller (arm the executors, still no decisions)

```
COMMAND_POLLER=true
```

Each node now executes `warm`/`drain` commands addressed to its `NODE_KEY`. **Enable this before
Stage 4** — a handoff issues commands that a node must be able to run; with the poller off,
handoffs would stall and revert. No commands exist yet, so this is inert until Stage 4. Confirm
the poller is up (a node log line or a healthy process); no errors.

### Stage 4 — arm the control loop (moves begin)

Pick the detection bar. **Use the absolute floor** (recommended — `#16`; the p99 path is a
self-scaling fallback whose fleet bar, even after the 2026-07-08 count-weighted-mean refinement,
is only an approximation of the true pooled-distribution p99). Start **conservative** (well above
normal per-shard rates so only a real hotspot trips it):

```
REBALANCE_HOT_QPS_FLOOR=200.0      # tune from your Stage-2 dry-run numbers; unset ⇒ p99 path
REBALANCE_CONFIRM_WINDOWS=2        # windows a shard must stay hot before moving
REBALANCE_COOLDOWN_MS=300000       # 5 min; don't re-move a shard within this
REBALANCER_ENABLED=true
```

Watch the first real handoff:

```
# The control loop is running + completing (not discarding = not crashing):
rpc 'Fathom.Repo.query!("select state, count(*) from oban_jobs where worker=$1 group by 1",
       ["Fathom.Rebalancer.RebalanceJob"]).rows |> inspect() |> IO.puts()'

# Handoffs enqueued/executed:
rpc 'Fathom.Repo.query!("select state, count(*) from oban_jobs where worker=$1 group by 1",
       ["Fathom.Rebalancer.HandoffJob"]).rows |> inspect() |> IO.puts()'

# Current pins + the rendered map:
rpc 'Fathom.Rebalancer.Overrides.all() |> Enum.map(&{&1.shard_id, &1.pinned_node, &1.failed_at}) |> inspect() |> IO.puts()'
rpc 'Fathom.Rebalancer.LbMap.current() |> IO.puts()'
```

After a move, confirm the shard is served by the target and **isolation holds** (a query for the
moved shard reads only its own data). Watch for thrash: a shard whose `failed_at` keeps getting
set is un-drainable and backing off (#4) — investigate the shard, don't lower the cooldown.

**Prefer metrics over RPCs for ongoing operation.** The RPCs above are for the first-handoff
sanity check; in production wire the rebalancer metrics (`fathom.rebalancer.*` — handoff
outcome, retry/thrash, `lb_apply` health, reconcile, affinity hit-rate) and the alerts in
`docs/runbooks/cluster.md`. The **`handoff.stop{outcome="reverted"}`** and
**`lb_apply{outcome="reload_failed"|"config_test_failed"}`** alerts are the two to page on.

### Stage 5 — tune

Lower `REBALANCE_HOT_QPS_FLOOR` toward the real hot-spot threshold from the dry-run data. Watch
`[:fathom,:shards,:evicted]` / `at_capacity` telemetry and the handoff success rate. Keep
`max_moves` at 1 (default) unless the fleet is large and lopsided.

---

## Manual controls (ops)

```
# Force a specific move (what chaos.sh rebalance does): pin, apply, then drain the source.
rpc 'Fathom.Rebalancer.Overrides.pin("acme", "fathom2", reason: "ops"); Fathom.Rebalancer.LbApply.apply!()'
rpc-on-source 'Fathom.Shards.drain("acme", 10_000) |> inspect() |> IO.puts()'

# Undo a pin (return the shard to the pure hash home):
rpc 'Fathom.Rebalancer.Overrides.unpin("acme"); Fathom.Rebalancer.LbApply.apply!()'

# Re-render the map from the DB (idempotent; heals drift — the leader does this each tick):
rpc 'Fathom.Rebalancer.LbApply.apply!() |> inspect() |> IO.puts()'
```

`LbApply.apply!()` returns `{:error, {:reload_failed, _}}` if a configured `LB_RELOAD_CMD`
exits non-zero (the map is still written for the next cold start, but the running LB may not have
picked it up — #11); `{:error, {:config_test_failed, _}}` if `LB_TEST_CMD` rejected the candidate
(the last-good file is kept — #3).

---

## Rollback

Rollback is staged in reverse and each step is safe on its own:

1. **Stop deciding:** set `REBALANCER_ENABLED=false` (or `Oban.pause_queue(queue: :rebalance)` for
   an immediate, no-deploy stop). The cron goes inert; in-flight handoffs finish or revert. No new
   moves.
2. **Return pinned shards to the hash home** (if a pin is causing trouble): `Overrides.unpin(id)`
   + `LbApply.apply!()`. A pinned node's death already fails over to a backup (#1a) and the
   leader unpins a dead node's shards (#1b), so this is rarely needed for availability.
3. **Full stand-down:** turn off `COMMAND_POLLER`, `LOAD_REPORTER`, and `SHARD_LOAD`. All gates
   off = fully inert (verified: the cron is a no-op when disabled and in test). The data path is
   unchanged from a fathom with no rebalancer.

Nothing in a rollback can corrupt data — the lease/epoch fence holds throughout; the worst case
is a shard briefly unavailable while it re-homes.

---

## Config reference

**Gates (env, `true`/`1`):** `SHARD_LOAD`, `LOAD_REPORTER`, `COMMAND_POLLER`,
`REBALANCER_ENABLED`.

**Topology (env):** `NODE_KEY` (stable per node), `LB_BACKENDS` (`key=addr,…`), `LB_MAP_PATH`,
`LB_RELOAD_CMD` (optional), `LB_TEST_CMD` (optional).

**Policy (env):** `REBALANCE_HOT_QPS_FLOOR` (absolute q/s; unset ⇒ p99 path — accepts int or
float, raises at boot on an unusable value), `LOAD_REPORT_INTERVAL_MS` (10000),
`REBALANCE_CONFIRM_WINDOWS` (2), `REBALANCE_COOLDOWN_MS` (300000).

**Advanced tunables (config only — `config :fathom, <key>`; no env var, defaults shown):**
`:rebalance_p99_multiple` (20), `:rebalance_max_moves` (1), `:rebalance_sample_horizon_ms`
(120000), `:rebalance_node_stale_ms` (60000 — a node unseen this long is treated dead by the
reconciler), `:rebalance_min_p99_samples` (50 — fleet sample floor before the p99 bar is
trusted), `:rebalance_command_retention_ms` (3600000), `:rebalance_command_stale_ms` (900000 —
pending commands older than this are expired), `:command_poll_ms` (1000), `:command_drain_ms`
(10000), `:command_poll_concurrency` (8), `:handoff_warm_timeout_ms` (30000),
`:handoff_drain_timeout_ms` (defaults to `command_drain_ms` + 35000 = 45000 — kept **above** the
poller's worst-case drain so a slow-but-succeeding drain isn't mislabeled a timeout, #8).

**Reporter internals (config only):** `:load_report_top_n` (50), `:load_sample_retention_ms`
(600000).
