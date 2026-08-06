# Fathom observability package (review #30)

Shippable alerting + dashboards for a fathom fleet, so an adopter isn't hand-translating prose
into their backend. Everything here reads the in-process Prometheus scrape each node exposes at
**`GET /admin/metrics`** (port 4000, behind the admin BasicAuth) over `Fathom.Telemetry.metrics/0`.

| File | What it is |
|---|---|
| `prometheus.yml` | Scrape config: one target per node's `/admin/metrics`, BasicAuth, relabel to a `node` label, loads the rules. |
| `alert-rules.yml` | 32 Prometheus alerting rules (durability, capacity, ownership, control-plane, rebalancer, disk), each linking its runbook. |
| `fathom-dashboard.json` | Grafana dashboard: node health, RPO exposure, lease churn, capacity/admission, S3 ops. Import and pick your Prometheus datasource. |

## Wire it up

1. **Point Prometheus at your fleet.** Edit `prometheus.yml`'s `static_configs` targets to your
   nodes' private `host:4000` addresses (or swap in a service-discovery SD). Provide `ADMIN_USER` /
   `ADMIN_PASS` (the admin BasicAuth) — from a secret store, not inline.
2. **Validate + load the rules.** `promtool check rules alert-rules.yml && promtool check config
   prometheus.yml`, then load. Point the (commented) `alerting.alertmanagers` block at your
   Alertmanager.
3. **Import the dashboard.** Grafana → Import → `fathom-dashboard.json` → select the Prometheus
   datasource. The `node` template variable is populated from `fathom_shards_active`.
4. **Tune the `(TUNE)` thresholds** in `alert-rules.yml` to your S3 region RTT, node density, and
   the SLOs below.

## Metric naming

Exported names are the underscore form of `Fathom.Telemetry.metrics/0` (the
`telemetry_metrics_prometheus_core` reporter joins the name parts with `_`, **no `_total` suffix**).
Distributions export `<name>_bucket{le}` / `<name>_sum` / `<name>_count`. Examples:
`fathom_shards_active` (gauge), `fathom_shard_lease_superseded_count` (counter),
`fathom_shard_cold_open_duration_bucket{warm="false",le=...}` (histogram).

### Coverage is enforced, in both directions

`test/fathom/telemetry_coverage_test.exs` fails the build when either half of the chain is missing:

* **An emitted event with no metric.** Emitting is the cheap half and nothing fails without the
  other half, so the gap is invisible by construction — the code looks instrumented and the metric
  simply does not exist. A sweep on 2026-08-06 found **30** events in that state, three of them
  written with an explicit intention to alert (`check_template_drift/0`'s own docstring says it
  emits "so a post-revert wedge is alertable"). Adding an event now either exports it or records
  why not.
* **A rule referencing a series nothing produces.** That is not an error anywhere: Prometheus
  evaluates it forever against no data and never fires, which is indistinguishable from the
  condition never happening. The test resolves every `fathom_*` name in `alert-rules.yml` against
  `metrics/0` — a one-character typo fails it.

It also refuses any metric tagged by `shard_id`, per the cardinality rule below.

**Fleet-merge is by the `node` label**, relabeled from the scrape target. fathom is an
LB-keyspace partition — a shard's signal lives on exactly one node — so you `sum`/aggregate across
`node` for a fleet view. There is deliberately **no per-shard tag** (cardinality death at millions
of shards); per-shard hot-set is the `Fathom.ShardLoad` read API (`mix fathom.scale --hotspots` /
`chaos.sh hotspots`), not a metric.

## SLOs

Three explicit SLOs, each with the PromQL to compute it and the alert that fires on breach. Tune the
objectives to your tenants' contract.

### SLO-1 — Checkout success rate ≥ 99.9%

The fraction of shard checkouts that succeed (`outcome="ok"`) vs all outcomes. This is the tenant's
"can I reach my database" number.

```promql
sum(rate(fathom_shards_checkout_stop_duration_count{outcome="ok"}[5m]))
  / clamp_min(sum(rate(fathom_shards_checkout_stop_duration_count[5m])), 1)
```

Breach drivers: `FathomCheckoutUnavailable`, `FathomAtCapacity503`, `FathomLeaseStoreDown`.

### SLO-2 — Cold-open p99 ≤ your failover budget

Cold-open (S3 pull) p99 bounds how fast a tenant's first request after a remap/failover is served.
Expect ~2× the one-way S3 RTT + a few ms (`docs/benchmark-plan.md`); set the objective from your
region RTT (e.g. ≤ 1s for a 30 ms one-way region; the warm-standby follower lowers the tail).

```promql
histogram_quantile(0.99, sum by (le) (rate(fathom_shard_cold_open_duration_bucket{warm="false"}[5m])))
```

Breach driver: `FathomColdOpenSlow`.

### SLO-3 — Unflushed-age ceiling ≤ your RPO target

The oldest un-flushed write on any node bounds the recovery-point objective: if a node is lost, this
is the largest window of acked-but-unflushed writes at risk. Keep it under your RPO contract (a small
multiple of `SHARD_FLUSH_INTERVAL_MS`); a rising value means flushes aren't landing (S3 trouble or a
stuck coordinator) — the closest thing fathom has to a "flush failure rate."

```promql
max(fathom_durability_oldest_age_ms)
```

Breach driver: `FathomUnflushedAgeHigh` (+ `FathomManyDirtyShards` for the fleet-wide view).

## Notes

- The scrape returns an **empty body when the metrics layer is disabled** (`Fathom.Admin.enabled?`
  false — e.g. test). In prod it's on; make sure `ADMIN_USER`/`ADMIN_PASS` are set or `/admin/metrics`
  fails closed (503) and the scrape is empty.
- Counters only appear in the scrape **after their first observation** — a freshly-booted node won't
  export `fathom_shard_corrupt_flush_count` until the first (hopefully never) occurrence. Alerts use
  `increase(...) > 0`, which is correct across the metric's first appearance.
- Rebalancer alerts are inert until B1 is enabled (`docs/runbooks/rebalancer.md`).
