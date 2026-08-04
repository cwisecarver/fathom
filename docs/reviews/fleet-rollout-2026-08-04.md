# Fleet schema-rollout throughput — what `:reconcile_batch_size` is actually throttling

**2026-08-04, Apple M5 Max (18 cores), macOS 27.0.** The 3-node chaos rig (`fathom1/2/3` behind
nginx + MinIO + one Postgres, all in one colima VM, prod-release nodes). Image built at 01:30,
after the HEAD under test (01:25), and the run asserts the change's own observable before measuring
anything.

This is the second half of **expert review 2026-08-01 #43**. The
[first half](expert-review-2026-08-01-000011.md.progress.md#43--telemetry-half-shipped-2026-08-04)
put `rate_per_hour` + `eta_seconds` on `Migrator.status/0` so an operator can *see* a rollout move.
The finding's other half was that `:reconcile_batch_size` — 100 shards on an hourly cron, i.e.
2,400/day, i.e. months for a deep cold tail — is a knob with **no evidence base**: nothing in the
repo measured fleet migration throughput, so raising it was a blind guess whose failure mode is a
fleet-wide latency event.

**The claim under test:** the 100/hour default is a *cron throttle*, not an engine limit, and the
gap between them is large enough that an operator raising the knob is reclaiming real headroom
rather than trading it against tenant latency.

## What it does

`deploy/chaos/chaos.sh rollout [tenants limit timeout_s]`:

0. **Preflight.** Refuses to run if `Migrator.status/0` has no `:rate_per_hour`. `./chaos.sh up`
   does not build, so a stale image would run the whole sweep and report a confident number for
   code that is not the code under test — on 2026-08-02 both `smoke` and `deploy` passed exactly
   that way.
1. **Rig hygiene.** Clears the shard directory. `Migrator.rollout/1` sweeps *every* active shard
   below HEAD, so another step's leftovers (`smoke`'s five tenants, a previous rollout run's
   tenants sitting one version back) get enqueued alongside ours, fail the replay, and quarantine.
2. **Seed N tenants at the current HEAD** through the LB, so each is born on the node the
   consistent hash gives it — the same partition the rollout then works across. Then flush to
   MinIO: the migration pulls each shard's **live object**, not the owner's local file.
3. **Release HEAD+1** and run `Migrator.rollout(limit)` — the production sweep, not a test hook.
4. **Poll** the directory for roll-set progress and `Migrator.status/0` for the fleet's own view.
5. **Read back** throughput, the per-node split from `[:fathom, :migrator, :shard_migrated]`, and
   two independent witnesses of the same quantity.

Versions are computed (`HEAD`, `HEAD+1`) and the added column is version-scoped (`c_v<N>`), so the
step is rerunnable on a rig whose release table is durable. The first draft hardcoded `release(2,
…)` and a second run collided on the unique version, left HEAD where it was, enqueued nothing, and
looked instant.

## Result

Two runs, 300 tenants each, minutes apart.

| | run A | run B |
|---|---|---|
| shards migrated | 299 / 300 | 299 / 300 |
| time to last migration | 23.0 s | 23.4 s |
| **throughput** | **13.0 shards/s** | **12.8 shards/s** |
| extrapolated | ~46,800 /hour | ~45,986 /hour |
| per-node split | 106 / 89 / 104 (1.19×) | 100 / 96 / 103 (**1.07×**) |
| `shard_migrated` vs directory | 299 = 299 ✓ | 299 = 299 ✓ |
| quarantined | 0 | 0 |
| straggler | `roll131` | `roll33` |

**~46,000 shards/hour against a 100/hour default — the cron throttle sits ~460× below what the
engine sustains on this rig.** Both runs land within 2% of each other.

That number is the *engine's* rate under a single burst enqueue on one contended VM: 3 nodes ×
Oban `migrations: 10` = 30 concurrent migrations, each doing its own MinIO pull, replay, fenced
flush and cutover, against one shared Postgres. It is not a production absolute — it is the
evidence that the default is nowhere near the ceiling, which is what the finding asked for.

**The per-node split is a different mechanism from every other fleet step.** `density` and
`tpc-fleet` measure the LB consistent hash. Migration work is drawn from **one Oban queue in
Postgres**, so the split is queue-draw, not hash. It still comes out even at 300 jobs (1.07×), but
at 12 jobs an earlier trial had one node draw zero — correctly, with 30 slots fleet-wide. The step
prints the raw counts rather than a spread ratio when a node draws nothing, because a `0` there is
not a broken partition.

### The rate and ETA track a real rollout

Run B, the fleet's own view while it converged:

```
     t+s   done/300  laggards   failed      rate/h        eta
       3         98       202        0          98      7421s
       6        162       138        0         162      3067s
      13        211        89        0         211      1519s
      20        282        18        0         282       230s
      23        299         1        0         299        13s
```

Both new fields move the way an operator needs them to, on a real fleet, first time.

**Two honest characteristics of the trailing-hour window**, both visible above and neither a bug:

- **Early ETA is pessimistic.** At t+3 the rate is 98/hour because the window is an hour wide and
  the burst is three seconds old, so the ETA reads 7,421 s for work that finished in 23. It
  converges as the window fills. The hour was chosen deliberately — `ReconcileJob` is an hourly
  cron, so a shorter window reads **zero** for most of every hour, which is worse for the cold-tail
  case the field exists to serve. An operator watching the first minute of a burst should watch the
  trend, not the number.
- **A stall reads stale-optimistic for up to an hour.** After the 299th shard the fleet sat at
  `rate=299/h, eta=13s` while nothing moved at all. The arithmetic is right — 1 laggard at the
  measured rate *is* 13 s away — but the rate is backward-looking. It decays to `rate=0,
  eta=null` once the window rolls past the burst, and `null` is the explicit "not moving" signal.
  So a stall is always eventually visible, but not instantly.

## The bug this run found

**Both runs left exactly one shard permanently unmigratable, on different tenants
(`roll131`, `roll33`) with an identical signature.** Reproducible, ~1 in 300.

```
stragglers (shard, directory version, lease holder):
  roll33  v4  {:held, "fathom@b0943aef3f4e#b261800d"}
```

The lock is a **coordinator** lease (`Heartbeat.owner/0` = `node#incarnation`) held by fathom2 —
but fathom2 had no coordinator for that shard and no local file for it. The chain:

1. A coordinator's lease outlives its coordinator. `Shards.drain/2` cannot help: it returns `:ok`
   immediately when the registry lookup is empty, which is exactly this state.
2. `ShardMigration.with_lease/3` acquires as `migrator@node@token` — a **different owner** from the
   coordinator, so there is no same-owner silent reclaim even on the same node.
3. `acquire_lease` judges a coordinator lock by the owner **node's heartbeat**, and fathom2 is
   alive and same-incarnation. The lock is therefore never stealable. **Forever.**
4. `{:retry, {:held, …}}` → `{:snooze, 5}`. A snooze does not burn an attempt, so the job sits in
   `scheduled` at attempt 122 of 127 with an **empty** `errors` array. Nothing is logged above
   `[info]`, `failed` stays 0, and the shard is not quarantined.

**Severity: the tenant serves normally and can never migrate.** A client `SELECT` through the LB
succeeded against `roll131` while it was stuck. That read is also the remedy — the coordinator
reopens, silently reclaims its own same-incarnation lock, and releases it on idle-drop, after which
the next snooze retry lands:

```
05:53:58  shard roll131: migration deferred ({:held, "fathom@b0943aef3f4e#b261800d"})
          <client SELECT through the LB>
05:54:18  shard roll131: migrated v3 -> v4
```

So the failure mode bites **precisely the shards the cold-tail sweep exists for**: a hot tenant
self-heals on its next request, an idle one never does. And it stalls the deploy gate — a CI job
polling for `converged == true` waits forever on a fleet that is otherwise 100% done.

This is **not fixed here.** It is a lease-lifecycle defect, not a rollout-telemetry one, and the
right fix (why does a coordinator lease outlive its coordinator, and should a migrator be able to
reclaim a lock whose coordinator is provably gone?) deserves its own investigation rather than
being folded into a measurement change. What this run did was make it *findable*: the step now
names every straggler with its lease holder and explains the shape, so the next person does not
have to hand-write an Ecto query against `oban_jobs` to discover that a `scheduled` job with no
errors is a held lease.

Related open thread: **the snooze is unbounded and silent.** `max_attempts` climbs with each snooze,
so the job never exhausts, never quarantines, and never surfaces. A shard that cannot acquire its
lease for hours is indistinguishable from one that is about to.

## What this changes

**`:reconcile_batch_size` now has an evidence base.** The measured procedure:

1. Read `rate_per_hour` from `GET /api/migrations/status`.
2. Raise `RECONCILE_BATCH_SIZE`.
3. Watch the rate follow. When it stops following, the ceiling has moved somewhere else — the
   `migrations` queue concurrency, the per-shard S3 round trips, or drain contention with live
   traffic — and raising it further only adds latency pressure on tenants without converging faster.

On this rig the engine sustains ~460× the default batch, so the first several raises are free.
`docs/django-migrations.md` §3 carries the procedure.

## Limits — read before quoting a number

- **One VM, three co-located nodes, loopback MinIO.** Absolute shards/s is CPU- and
  loopback-bound. Per the same-topology rule these numbers compare only to other runs of this step
  on this rig. Run `./chaos.sh latency 30` first if you want an S3-realistic figure; each migration
  pays several round trips (pull, flush, lock), so a real RTT will move this a lot.
- **A sibling `djathom` compose project shares the colima VM.** Timings are contended even when
  pass/fail is trustworthy.
- **Empty tenants.** Each shard is one small table and a `django_migrations` row, so this measures
  the per-shard *orchestration* cost (drain, lease, pull, replay, flush, cutover, retirement
  enqueue) with the copy itself near zero. A fleet of large shards is bounded by the copy instead —
  that is `copy_keystone_rows_per_s`, and the two multiply rather than substitute.
- **A single burst enqueue**, not the hourly cron's steady state. This is the engine's ceiling, and
  deliberately so: the cron's rate is a config value, not a measurement.
- **299, not 300, in both runs.** The headline rate excludes the straggler by construction — it is
  measured to the last shard that actually moved. Do not read it as "300 in 23 s".
