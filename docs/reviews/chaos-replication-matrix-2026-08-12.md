# Chaos rig with A2 replication ON — the six scenarios that had never run with it

**2026-08-12.** Closes the third open item from the A2 merge (`df2d476`): `soak`, `partition`,
`pause-fence`, `rebalance`, `density` and `tpc-fleet` had only ever run with replication **off**.
Only `smoke`, `failover`, `replication` and `rpo` had ever been driven with it on, and those four
are all single-shard, single-event scenarios — nothing had ever run A2 under sustained load, under
node churn, or alongside the rebalancer.

Image built from `46a2952` and **verified before any run** (AGENTS.md's rule after the 2026-08-02
stale-image pass): `function_exported?(Fathom.Shard.Storage, :object_head, 1)` and
`Recovery.recheck/3` both `true` through `bin/fathom rpc`, with
`{replication_enabled, replication_listen} == {true, true}`.

## Result: all six pass with shipping on. Nothing broke.

| scenario | args | verdict |
|---|---|---|
| `pause-fence` | `pf1` | **PASS** — survivor's write present, zombie's dirty write not resurrected |
| `partition` | `fathom2 25` | **PASS** — heartbeat lapses and recovers; no replication-specific error |
| `soak` | `120` | **PASS** — destroyed=0, leaks=0, unreadable=0 (acked 1191) |
| `rebalance` | `rb1 45` | **PASS** — detected at 148 q/s, moved fathom2 → fathom1, isolation intact |
| `density` | `600 20` | **PASS** — 600 shards, spread 1.45×, ~332 shards/s minted |
| `tpc-fleet` | `16,32 25` | **PASS** — **0 errors** at both steps, ~936 / ~964 txn/s |

That is the headline and it is a negative result: turning shipping on does not perturb the
single-writer fence, the S3-partition degrade path, the rebalancer handoff, or fleet throughput.

## The finding worth having: soak measures the RPO claim, and `rpo` cannot

`chaos.sh rpo` kills once, on one shard, with no flush in between. It proves the mechanism. It says
nothing about what a fleet loses across **many** kills under continuous write load — which is the
number an operator actually cares about.

`soak` already reports it, as a line nobody had read with replication on:

```
shipping ON, promote-on-open OFF   acked=1191  stored=1072  DESTROYED=0
  needs-operator-recovery: 119 row(s) live only in .forked.* quarantine copies

shipping ON, promote-on-open ON    acked=1118  stored=1118  DESTROYED=0
  needs-operator-recovery: 0 row(s) live only in .forked.* quarantine copies
```

**119 rows requiring operator recovery → 0.** Every acked row live and served, across 4 node kills
in 120 s.

This is the exact shape of A2 trap (b)/(d) recorded in AGENTS.md, now measured under churn rather
than argued: with shipping on and promote off, the frames ship correctly and **nobody ever reads
them**, so the loss window is identical to A2 being switched off. Shipping is not the feature;
reading is.

### Verified as mechanism, not luck

A suspiciously good number is a broken measurement until proven otherwise, and "0" is the most
suspicious number available. The direct observable settles it — **8 promotions**, 5 distinct
tenants, 3 distinct survivor nodes:

```
fathom1  shard t5: PROMOTED a local replica over the stored object
                   (replica %{epoch: 1, ..., next_offset: 8272} > object %{offset: 0, ...})
fathom1  shard t4: PROMOTED ...
fathom3  shard t3: PROMOTED ...
fathom5  shard t6: PROMOTED ...
   (+4 more, epochs 2 and 3)
```

Also **zero** `promotion_raced` / "abandoning replica promotion" lines: the object re-check added in
`46a2952` did not false-decline a single promotion under real churn, which was the risk of adding
it.

## `density` cannot resolve a replication cost at N=600, and the reason matters

The first A/B looked clean — OFF ~26 KiB/shard, ON ~47 KiB/shard, so ~+81%. A second clean pair
(full `down`/`up` between arms) says otherwise:

| run | replication OFF | replication ON |
|---|---|---|
| pair 1 | ~26 KiB/shard | ~47 KiB/shard |
| pair 2 | ~60 KiB/shard | ~76 KiB/shard |

**The same arm swung 26 → 60 KiB.** Within-arm variance is larger than the between-arm delta, so
this instrument cannot support any per-shard replication cost claim at this scale. Reporting the
first pair alone would have published "+81%" from noise.

The mechanism is the one AGENTS.md already records for the bench's `fanout_kb_per_shard`: the
number is a BEAM memory delta that includes open-path garbage the coordinators have not GC'd, and
at N=600 the fixed overhead has not amortized (the documented ~16 KiB/shard floor comes from a
30 000-shard run). Both pairs do show ON above OFF, so a real cost is plausible — it is simply not
quantifiable here. To measure it: run at 30k where the floor is established, or force a GC before
sampling (`fanout_gc_kb_per_shard` is the bench's answer to exactly this).

**Mint throughput, by contrast, is tight and clean across all four runs** — 332 / 354 / 355 / 361
shards/s, ON and OFF interleaved. Replication costs nothing measurable on the mint path.

## Gap found in the rig itself

`REPLICATION_RECOVER_FROM_PEERS` was **not in `docker-compose.yml`** at all, while every other A2
gate (`REPLICATION_ENABLED`, `REPLICATION_LISTEN`, `REPLICATION_PROMOTE_ON_OPEN`,
`REPLICATION_QUORUM`, `REPLICATION_MEMBERSHIP`) is plumbed there. Survivor selection could only be
reached by `put_env` through `bin/fathom rpc` from inside `cmd_rpo`, which means no other scenario
could turn it on and a reader comparing the compose file against `config/runtime.exs` would
conclude the feature did not exist on the rig. Added.

## What this run did NOT cover

* **Survivor selection (`REPLICATION_RECOVER_FROM_PEERS`) across the six.** Every promotion above
  came from a replica the survivor already held. The cross-fleet pull path is still exercised only
  by `rpo`.
* **`pause-fence` with promote-on-open.** Semantically the loaded combination: the zombie's dirty
  write may have been quorum-acked, and the fence correctly discards it. If a promotion then
  resurrects it from a replica over the survivor's write, that is split-brain. This run had promote
  off during `pause-fence`, so the question is open and is the highest-value next scenario.
* **Quorum under multi-node loss.** `soak` kills one node at a time with `REPLICATION_QUORUM=2` and
  5 nodes, so quorum was always reachable. Killing two at once is the untested edge.
* **`density` and `tpc-fleet` at documented scale** (30 000 shards / 4096 tenants). Both ran small
  to bound rig time, so these are pass/fail evidence, not throughput or density numbers.

Raw logs: the run captured `smoke`, the six scenarios, the promote-on-open soak, the promotion
evidence grep, and both density pairs.
