# Chaos-rig validation of the 2026-11-07 audit fixes

Code: `8c888d75` (96 local commits after `dd5fa084`, the expert review 2026-11-07 fixes: A2 recovery,
lease/fence, executor accounting, open path). Five nodes on one colima VM, replication ON, prod
`mix release` images. Every in-process bench behind those commits ran with replication OFF; this is
the first replication-ON measurement of them. Logs (local, not committed): `logs/rig-20261107*/`.

## Results

| check | config | result |
|---|---|---|
| `tpc-fleet 1024` (fresh `colima restart`) | `SHARD_FLUSH_INTERVAL_MS=30000`, `TPC_NET=container TPC_DRIVER=elixir` | **4,292 txn/s**, 0 errors, 0 shed, p50 220 ms, p99 **389 ms** (2026-10-26 with 1 GB writeback cap: 4,008 txn/s, p99 417 ms) |
| `smoke` (isolation check) | replication ON | OK, `foreign=0` for every tenant |
| `pause-fence acme` | replication ON, flush 5 s | PASS: survivor's seq=400 present, zombie self-fenced (`:fence_skip`), the quorum-acked seq=300 recovered by promote-on-open on the survivor |
| `pause-fence acme` | replication OFF (control), flush 5 s | PASS: seq=400 present, seq=300 resurrected = 0 |
| `failover acme` | replication ON, flush 5 s | **17.3 s** to first acked write; baseline `dd5fa084` on the same rig: 17.35 / 17.31 s, HEAD 17.35 / 17.26 s (no difference). Replication OFF: 10.8 s |
| `soak 180` x3 | replication ON, flush 5 s, variables on the soak command | acked 1603 / 1852 / 2095, DESTROYED 0, leaks 0, unreadable 0, **0** rows only in `.forked.*` copies, every node `replicating? = true` after the churn |
| `partition fathom2` | replication ON | writes fenced `FILO_STALE_LEASE` during the cut, heartbeat resumed after |

`clock lags the store`: 0 lines on every node in every run.

## Two harness traps found (fixed in `deploy/chaos/chaos.sh`)

- **`pause-fence` read the promote gate without its default.** `replication_promote_on_open` defaults
  to ON when the app env is unset, but the check was `get_env(...) == true`, so a default fleet read
  "false", took the replication-OFF branch (seq=300 must be 0) and printed **SPLIT-BRAIN** for a
  correct run: the survivor logged `PROMOTED a local replica over the stored object` and the zombie
  logged `ownership unconfirmed before flush (:fence_skip)`. The check now also requires
  `Fleet.replicating?()`, so the control arm still takes the strict branch. Both verdict branches
  now return non-zero (they exited 0 on SPLIT-BRAIN).
- **`soak` revives killed nodes from its own environment.** Compose interpolates
  `REPLICATION_ENABLED` / `SHARD_FLUSH_INTERVAL_MS` from the shell that runs `soak`, so a soak run
  without the variables that `up` had revived every killed node with shipping OFF. The first such run
  reported 3 acked rows living only in `.forked.*` copies; the same fleet with the variables on the
  soak command reported 0 in three runs. `soak` now refuses a mismatch between the fleet and its
  own `REPLICATION_ENABLED`.

## What this does not cover

Single runs of `pause-fence`, `partition` and `smoke`; the failover A/B is two runs a side;
`tpc-fleet` was one run and is not an A/B (no baseline run on this VM today). The soak A/B against
`dd5fa084` was attempted but its stack did not come up from a git worktree, so soak has no same-day
baseline: the comparison is the 2026-08-12 matrix (0 rows with promote ON). No open-loop run, no
Jepsen, no single-slow-follower or large-DB-wake arm.
