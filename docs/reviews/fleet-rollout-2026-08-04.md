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

### FIXED and confirmed on the rig

| run | before the fix | after |
|---|---|---|
| 1 | 299 / 300 (`roll131`) | **300 / 300** |
| 2 | 299 / 300 (`roll33`) | **300 / 300** |
| 3 | 299 / 300 (`roll8`) | |
| 4 | 299 / 300 (`roll64`) | |

Four runs before, each stranding exactly one tenant on a different shard; two runs after, both
fully converged with `failed=0`. Throughput is unchanged where it is comparable (12.8 shards/s on
the first post-fix run, matching the pre-fix runs) — the fix adds one round trip only on the 412
path, which a healthy release never takes. The second post-fix run reads 8.1 shards/s, which is
back-to-back-run VM contention on the shared colima box (the same-topology rule), not the fix.

### Root cause: the release itself, not any of its callers

`release_lease` is a conditional `DELETE … If-Match: <the etag we last wrote>`, and a `412` was
reported as `:ok`. A 412 is **two** situations:

- the lock is now **someone else's** — a correct no-op (finding #22: an unconditional delete would
  remove a live owner's lock);
- the lock is **still ours at a different etag** — a leak, reported as success.

Our own etag rotates under us. `S3.acquire_existing/4` **rewrites the lock** on a same-owner
reclaim (same owner, same epoch, new etag), and `renew_lease/3` rewrites it in legacy mode. So the
release silently no-op'd and left a lock naming a **live** node with no coordinator behind it —
`owner_live?` reads that node's fresh heartbeat forever, so no peer, no failover and no migrator can
take the shard, while it keeps serving because its own node reclaims at the same incarnation.

**This is why #9 and #11 did not close it.** Both fixed *callers* that released with a stale lease.
Any rotation whose result the caller never received still leaked, because the bug was in the release.

**And this is why it was heartbeat-mode-only.** Simulating the pre-fix policy (412 ⇒ `:ok`) fails
the heartbeat test and *passes* the legacy one: legacy's `Fence.check` performs a `renew_lease` PUT
on the way into the drop, and #9's fix threads that refreshed lease — carrying the current etag —
back to the caller, so legacy accidentally self-heals a rotation from any source. Heartbeat mode does
no renew at all and carries the stale etag straight into the release. The suite ran legacy-only until
the previous commit, so it could not see this; the rig runs heartbeat mode, so it hit it every run.

The fix re-reads the lock on a 412 and finishes the delete when it is still ours. The decision lives
in `Fathom.Shard.Storage.resolve_stale_release/4` rather than inside a backend — both because every
etag-carrying backend must make it identically, and because the default suite's backend is
`Fathom.Test.FaultyStorage`, so a policy implemented privately per backend would only ever be tested
against the double.

### History: two leak paths closed before this, and why they were not it

Read this section before the next one. Two genuine lock-leak paths were found and fixed the same
day, each with a test that fails against the unfixed code — and **neither was this bug**. A rebuilt
rig reproduced the straggler both times (`roll8`, then `roll64`), so the fixes below are real but
not sufficient. Recorded this way deliberately: a fix that is verified by tests and still does not
close the field report is exactly the case that gets written up as "fixed" and quietly isn't.

**What is now known, and what it rules out.** On the last run, with the fleet otherwise fully
converged:

- `roll64.lock` was the **only** lock object left in the entire bucket — 299 shards acquired and
  released cleanly through the same code paths.
- Its owner string is byte-identical to the owning node's **current** `Heartbeat.owner/0`
  (`fathom@<node>#<incarnation>`), so it is a coordinator lock from the live incarnation, not a
  leftover from a previous boot. `stealable_at` tracks the node's heartbeat, so it never expires.
- No coordinator in that node's registry, no local `.db`, no `.forked.*`/`.fenced.*` quarantine.
  The shard's `.db` object in storage is intact.
- **Nothing was logged for that shard except `migration deferred`.** No exception (the new
  post-acquire guard's error log fired zero times fleet-wide), no `self-fencing`, no flush failure.

That rules out all three of the paths that were suspected in turn: the drop path's flush-error and
ownership-unconfirmed branches (fixed anyway — see below), and an exception between `acquire_lease`
and the built state (guard added anyway — it closes a hazard the code's own comments describe).

**The most likely reason it resists reproduction.** An in-process harness that opens and drains 300
shards concurrently does **not** leak a lock. The suite could not express this bug class at all,
because `config/test.exs` sets `heartbeat_server: false` — so every coordinator in every test ran
in **legacy mode**, while the rig runs **heartbeat mode**. Those are different fence, renewal and
liveness paths. Same shape as the lesson already recorded for `Storage.Local` vs the S3 lock-etag
contract: a gap between the test environment and production silently exempts every bug on the other
side of it.

**That gap is now closed** (`test/fathom/shard_lease_release_test.exs`): every mode-agnostic leak
scenario is generated for both modes, each test asserts the mode actually took rather than trusting
the setup, and the mode-specific ones say which and why. It paid for itself immediately — the drop
path's **ownership-unconfirmed leak existed in heartbeat mode too**, and only the legacy version had
ever been tested. Six of the fifteen tests fail against pre-fix code, across both modes.

It did **not** surface the rig straggler. Two things worth knowing for whoever picks this up:

- The first heartbeat fixture for the `:skip` branch was wrong in a way that passed quietly: killing
  the `Heartbeat` process does not produce `:not_valid`, because a DOWN heartbeat **degrades to the
  legacy renew fence** and succeeds. The real state is the process alive with its renewal deadline
  not comfortably ahead. That degrade path is now pinned separately, and labelled as the
  non-regression test it is.
- So heartbeat mode is no longer a blind spot for the *drop* and *open* paths — which means the
  straggler is likely somewhere those tests still do not reach: concurrency between a coordinator
  and the migrator's `Shards.drain` across nodes, or a path only the real S3 backend takes.

### Two leak paths found and fixed on the way (neither is the straggler)

Reproduced deterministically at the unit level rather than diagnosed from the rig, because
`down -v` had already taken the node logs. `flush_then_drop/1` has two branches that keep the local
copy — a **transient flush error** and a **fence that could not confirm ownership** — and both also
kept the **lock**. Keeping the copy is correct: it holds acked-but-unflushed writes. Keeping the
lock was not, and the two are separable.

`Shards.drain/2` cannot compensate, because it returns `:ok` immediately when the registry lookup
is empty — which is exactly this state. And the migrator acquires as `migrator@<node>@<token>`, a
different owner string from the coordinator's `<node>#<incarnation>`, so there is no same-owner
silent reclaim to save it either.

Both branches now release the lease through one shared helper, with a warning log and the alertable
flush-failure telemetry that finding #11 established for the sibling branch. Releasing costs nothing
that was actually being kept: `release_lease` is a conditional `DELETE … If-Match` the etag we last
wrote, so the ownership-unconfirmed case is safe **by construction** — either we still hold the lock
(release, correct) or we do not (no-op on someone else's lock, correct). If a peer then opens the
shard it pulls the last durable object, and our divergent copy is arbitrated on this node's next
open by the provenance sidecar (#1), quarantined recoverably as `.forked.<ts>` — the same path a
node crash with un-flushed writes already takes. Holding the lock never made those writes durable.

A third fix went in alongside them: an **exception between `acquire_lease` and the built state**.
Every failure that path anticipated is an `{:error, _}` tuple, which reaches `open_with_lease/8`'s
else branch and releases; an exception does not, because `handle_continue` never returns, so the
GenServer still holds the pre-open `%{id: shard_id}` state and `terminate/2` falls to the catch-all
clause with no lease to release. The code's own comments describe this hazard, and review
2026-08-01 #33 had fixed exactly one instance of it (a raise inside `fork_evidence/2`) while
`resolve_fork/4`, `revalidate_takeover/5` and `promote_pull/2` still ran storage and File calls
directly in the coordinator. The whole post-acquire region is now guarded: release, then stop with
`{:shutdown, _}` like every other open failure, loudly.

These are the **third through fifth instances of the same class** after review 2026-08-01 #9 and
#11, so the regression tests joined their file (`test/fathom/shard_lease_release_test.exs`). Each
discriminating test asserts the **foreign-owner** view and fails against the unfixed code; the
same-node re-open assertion passes either way — a coordinator silently reclaims its own node's lock
— and is labelled in the file as a value guard rather than a regression test, since that property is
precisely what made the production symptom so quiet.

**None of the three stopped the rig straggler.** The post-acquire guard's error log fired **zero**
times across the whole fleet on the run that still stranded `roll64`, which is what rules the
exception path out rather than merely leaving it unproven.

**Still open, and deliberately separate: the snooze is unbounded and silent.** `max_attempts` climbs
with each snooze, so the job never exhausts, never quarantines, and never surfaces. A shard that
cannot acquire its lease for hours is indistinguishable from one that is about to. The lock leak
that motivated it is fixed, but any *other* cause of a long-held lease would still be invisible.

What the rig contributed beyond the bug itself: the step now names every straggler with its lease
holder and explains the shape, so nobody repeats the hand-written Ecto query against `oban_jobs` it
took to learn that a `scheduled` job with an empty `errors` array means a held lease.

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
