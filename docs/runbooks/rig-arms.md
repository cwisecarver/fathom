# Chaos-rig arms: gp3-capped disk, auth required, sparse tenants

Three opt-in measurement arms for the chaos rig (`deploy/chaos/`). They exist because the rig's
numbers (4,343 txn/s, p99 385 ms closed loop, ~47-125 ms open loop at 3,000 txn/s;
`docs/reviews/tpc-fleet-repl-on-2026-10-30.md`) were taken with an uncapped disk, `HRANA_AUTH=disabled`
and tenants that never go idle, none of which is the production target. Each arm answers one
finding of expert review 2026-10-29 and is built to be **falsified**: the "What would falsify it"
line under each says what result closes the finding. All new behaviour is off unless its variable is
set, so a plain `./chaos.sh up` and every existing arm are unchanged and stay comparable.

None of these arms has been run yet; the first run is the measurement. Nothing below is a result.

## Before any run

- `cd deploy/chaos`. `./chaos.sh build` (the node image) and `./chaos.sh build-driver`: the driver image
  copies `tpc_driver.exs` at build, and `--tokens-file` / `--think-ms` are new, so an old image fails
  or silently ignores them.
- `docker ps -q | wc -l` is 0, `/tmp/fathom_bench.lock` and `/tmp/tidepool_bench.lock` are absent, and the
  one-minute load is low (AGENTS.md § Benchmarking). Fresh VM per run: `colima restart`, then `up`.
- Grep every node's logs for `clock lags the store` after each run (`./chaos.sh logs | grep -c
  'clock lags'`); a stepped clock invalidates the run. `tpc-sparse` counts it per round itself.
- Overlays are chosen when the containers are created. Pass the SAME `RIG_DISK_CAP` / `TPC_AUTH` to every
  later `chaos.sh` command of the session (they extend `COMPOSE_FILE` for `compose exec` and
  `compose run driver`); `./chaos.sh down` clears it.
- The open-loop recipe everywhere below is the one the second-disk A/B used:
  `TPC_NET=container TPC_DRIVER=elixir REPLICATION_ENABLED=true SHARD_FLUSH_INTERVAL_MS=30000`,
  `tpc-tail` with `TPC_TAIL_ARMS=open TPC_OPEN_RATE=<fleet txn/s>` (1024 tenants, 400 txns each, three
  runs; the spread across the runs of one arm is the noise floor, and a difference inside it is not a
  finding).

## 1. gp3-capped disk (review #16)

**Question.** The only mechanism measured to set the tail is an fsync waiting behind other writes on one
device. The rig's virtio disk has no cap; the documented prod target, a gp3 EBS volume, has 3,000 IOPS
and 125 MiB/s (an external AWS figure, not from this repo). Does the cap bind at the rig's load, and does
giving the replicas their own capped device still help under it?

**What the override does.** `docker-compose.gp3.yml` puts a `blkio_config` (read and write bps and
iops) on every node's block device; docker turns it into the container cgroup's `io.max`. The cap is per
container, i.e. per node, which models one gp3 volume per node. It does not model two volumes sharing an
instance's EBS limit (`io.max` cannot span devices), so that case stays unmeasured, as
`docs/deploy-cluster.md` already says.

| `RIG_DISK_CAP=` | compose files | what is capped |
|---|---|---|
| `gp3` | base + `docker-compose.gp3.yml` | the one disk behind `/var/lib/docker`: data, replicas and warm cache share ONE cap |
| `gp3-split` | base + `replicadisk` + `docker-compose.gp3-split.yml` | the data disk AND the replica disk, the same rate on EACH |

Never cap only one side: the uncapped path then absorbs what the capped one sheds and the comparison
is void (the audit's Trap on #16). `RIG_CAP_MIBPS` (default 125) and `RIG_CAP_IOPS` (default 3000) change
both devices together.

**Finding the device.** io.max needs the WHOLE disk (`vdc`, not `vdc1`), and it is the disk behind
`/var/lib/docker`, which is **not** `vda` on this host's colima: on 2026-10-30 `findmnt -T /var/lib/docker`
gave `/dev/vdc1[/docker]` (colima's data disk), the replica disk was `vdb`, and `vda` is the OS disk.
`chaos.sh` resolves both with `colima ssh -- sh -c 'findmnt -no SOURCE -T <path>'` plus `lsblk PKNAME`
and prints them at startup. To override, or if it cannot resolve (a non-colima VM, LVM): find them by hand
with `colima ssh -- lsblk -o NAME,MAJ:MIN,PKNAME,MOUNTPOINT` and
`colima ssh -- findmnt -no SOURCE -T /var/lib/docker`, then export `RIG_DATA_DEV=/dev/<disk>` (and
`RIG_REPLICA_DEV` for `gp3-split`). The cgroup must be v2 with the `io` controller
(`colima ssh -- cat /sys/fs/cgroup/cgroup.controllers`; it was on 2026-10-30).

**Run it.** One-time second-disk setup is in the header of `docker-compose.replicadisk.yml`; before
EVERY `gp3-split` `up`, wipe and recreate the replica directories as that header says.

```bash
cd deploy/chaos
colima restart
# arm = shared | split (| control: no RIG_DISK_CAP, and the replicadisk-only arm from the 2026-10-30 note)
export RIG_DISK_CAP=gp3            # or gp3-split
REPLICATION_ENABLED=true SHARD_FLUSH_INTERVAL_MS=30000 ./chaos.sh up
./chaos.sh disk-cap show           # must end "disk-cap: OK"; else STOP, the arm is not capped
./chaos.sh disk-cap probe          # before load. Expect bandwidth ~125 MiB/s (dd calls it 131 MB/s), iops <= 3000
for rate in 3000 4500; do          # 600 and 900 txn/s per node, the audit's two points
  TPC_NET=container TPC_DRIVER=elixir REPLICATION_ENABLED=true \
    TPC_TAIL_ARMS=open TPC_OPEN_RATE=$rate TPC_JSON_OUT=/tmp/gp3-$RIG_DISK_CAP-$rate \
    ./chaos.sh tpc-tail 1024 400 1000 3
done
./chaos.sh down
```

Alternate the arms across runs (shared, split, control, shared, ...), a fresh `colima restart` before
each, three runs a side, as the 2026-10-30 second-disk re-run did. 4,500 txn/s is above the uncapped
closed-loop ceiling (4,343 txn/s), so the uncapped control may saturate there too; read 3,000 as the
comparable point and 4,500 as the stress point, and run the uncapped control at both.

**Read.** `disk-cap show` (OK) and `disk-cap probe` first: bandwidth near the cap and iops at or below
it prove the cap binds on that device; if the probe reads several times the cap the arm measured
nothing. Then per run the `summary-line open#k p99_excl_ms=...` and `errors` lines of `tpc-tail`, the
per-10 s p99 series, and `jq .err_reasons /tmp/gp3-*.json`. The arithmetic to check against: ~114 KB/txn
on disk with replication (`tpc-fleet-repl-on-2026-10-26.md`) is about 67 MiB/s per node at 600 txn/s
(54% of the bandwidth cap) and about 100 MiB/s at 900 (80%); each commit also costs a WAL write, an
fsync and journal I/O, so IOPS may bind before bandwidth.

**What would falsify it.** The finding says the cap makes fsyncs queue and that queue is the tail.
- The shared-cap arm's `p99_excl` at 3,000 txn/s lies inside the uncapped control's run-to-run spread, with
  the probe having shown the cap binding: the cap is not a lever at this load, and the bytes-per-txn
  levers (`fdatasync`, follower count) are not motivated by it.
- `gp3-split` is not better than `gp3` by more than the spread: a second capped device does not help, so
  the "replicas on their own volume" advice in `docs/deploy-cluster.md` does not survive the cap.
- If `disk-cap show` or the probe fails, the run says nothing either way.

## 2. Auth required (review #18)

**Question.** Every p99 in `docs/reviews/` excludes token verification, the revocation-floor lookup (cold
on a node's first sight of a shard) and the per-statement version/expiry re-check, because the rig runs
`HRANA_AUTH=disabled` and prod refuses to boot without it set explicitly (and intends `required`). Does the
prod posture move the tail, in steady state or across a node restart?

**What it does.** `TPC_AUTH=required` does three things. `./chaos.sh up` stacks
`docker-compose.auth.yml` (nodes get `HRANA_AUTH=required` and `HRANA_TOKEN_MAX_AGE`, default 86400 s,
which prod demands under `required`; the in-network driver gets a read-only `./tpc-auth` mount). Every
elixir-driver run of the `rtt`, `tpcb` and `oneshot-fleet` modes (so `tpc-fleet`, `tpc-tail`, `tpcb`,
`pool-fleet`, `pool-gap`, `tpc-sparse`) first checks the posture (fathom1 reports `hrana_auth=required`
AND an unauthenticated request through the LB answers 401, or it refuses to run), then mints one token per
tenant on fathom1 with the real path, `Fathom.HranaAuth.token_for/1`, into `deploy/chaos/tpc-auth/`
(gitignored, deleted after the run) and passes `--tokens-file` to the driver, which sends each tenant's
token as `Filo.Client`'s `:auth_token` (kept on the client, so a reconnect re-sends it). Not supported:
the python driver and `tpcc` / `tpcc-fleet` (the run is refused, not run unauthenticated), and the
non-tpc scenarios (`smoke`, `failover`, `soak`, ...) send no token, so on an auth-required fleet they
get 401 by design. A refused token
shows in the driver's `err_reasons` as its own `http_401` bucket.

`TPC_RESTART_NODE=<node>` (with `TPC_RESTART_AT_S`, default 60; `TPC_RESTART_GRACE_S`, default 30)
is the restart phase and works with or without auth: during each `tpcb` / `oneshot-fleet` run it
`compose restart`s that node, a graceful SIGTERM-flush-start that reuses the container and its address, and
prints the wall-clock `wall_ms` of the restart and of its return, the same clock as the driver JSON's
`t0_wall_ms`, so the restart lines up with the per-second p99 series. It reverts that node's rpc-pinned
settings (rebalancer off, idle-drop frozen) to boot values, as a real restart does. Use `TPC_TAIL_ARMS=open`
with ONE run, since every matching run restarts the node once more.

**Run it.** Auth is a boot-time node setting, so each posture is its own `up`.

```bash
cd deploy/chaos
for posture in required off; do
  colima restart
  [ $posture = required ] && export TPC_AUTH=required || unset TPC_AUTH
  REPLICATION_ENABLED=true SHARD_FLUSH_INTERVAL_MS=30000 ./chaos.sh up
  # steady state, three runs at 3,000 txn/s (the audit's falsifier)
  TPC_NET=container TPC_DRIVER=elixir REPLICATION_ENABLED=true TPC_TAIL_ARMS=open TPC_OPEN_RATE=3000 \
    TPC_JSON_OUT=/tmp/auth-$posture-steady ./chaos.sh tpc-tail 1024 400 1000 3
  # across a node restart, one run: the node restarts ~70 s in (the 137 s run is past its warm-up)
  TPC_NET=container TPC_DRIVER=elixir REPLICATION_ENABLED=true TPC_TAIL_ARMS=open TPC_OPEN_RATE=3000 \
    TPC_RESTART_NODE=fathom3 TPC_RESTART_AT_S=70 TPC_JSON_OUT=/tmp/auth-$posture-restart \
    ./chaos.sh tpc-tail 1024 400 1000 1
  ./chaos.sh down
done
```

The expiry cliff is its own optional arm: `HRANA_TOKEN_MAX_AGE=90 TPC_AUTH=required ... ./chaos.sh up`,
then any run longer than 90 s. Tokens minted at the start stop verifying at +90 s, so `http_401` should
appear from then on; that is the expected behaviour of a client that does not refresh, not a defect, and it
shows what the per-statement expiry check does to a long-lived client.

**Read.** Per posture: the steady runs' `p99_excl_ms` against the off-posture runs' spread; the restart
run's p99-by-window series around the logged `wall_ms` (find the window with the driver's `t0_wall_ms`)
and `jq .err_reasons` on its JSON. The auth-specific signal is the DIFFERENCE between the `required` and
`off` restart runs, because the restart itself costs errors either way (fathom3's tenants see
connection errors in both). A nonzero `http_401` in the steady or post-restart windows is a finding in
itself: every token comes from the same secret and the same directory, so none should be refused.

**What would falsify it.** Review #18 calls this a validity gap, not a lever, and says steady-state auth
is cheap (`Phoenix.Token.verify` 2.73 us warm). It is confirmed as a non-event if the `required` steady
and restart runs both land inside the `off` runs' spread and `http_401` stays 0: record that and drop the
gap. It is a finding if the post-restart windows (cold revocation floor, review #20) or the steady state
are outside that spread.

## 3. Sparse tenants, replication on (review #2)

**Question.** After 250 ms idle the pool drains and the last close runs SQLite's close-time checkpoint (WAL
fsync, backfill, DB fsync, unlink `-wal` and `-shm`); the next write recreates them, dirsyncs the directory
under FULL, and under A2 starts a new WAL generation. Every other arm drives tenants back to back, so none
has idle gaps over 250 ms, yet the real tenant population is sparse. Is that path a p99 cost on Linux/ext4
with replication on?

**What it does.** `tpc_driver.exs oneshot-fleet --think-ms T` is new: each worker issues one
`[UPDATE, SELECT, close]` one-shot write (a stream open and close) per T ms, OPEN LOOP (op k is intended
at `start + phase + (k-1)*T`, the phase random per worker; response time runs from the intended instant),
so every gap is about T. `./chaos.sh tpc-sparse [tenants ops think_ms graces_csv]` runs it in rounds
that alternate the pool's `drain_grace_ms` over `graces_csv`. It sets the grace with `put_env` on every
node before the round, which equals booting with `POOL_DRAIN_GRACE_MS` because a coordinator reads the
pool options at init and each round uses its own shard namespace. It prints per round the ops/s against
the offered rate, p50/p95/p99/max, errors, the fleet pool_take `hit%`, the `clock lags` count and the A2
commit histogram groups. It pins the rebalancer off and idle-stop at 900 s for the run and restores both
(and the grace) at the end. The grace default was 250 ms until 2026-10-31 and is 2000 ms since (results:
`docs/reviews/rig-arms-2026-10-31.md`); this arm always sets the grace explicitly per round.

**The intended A/B.** Two axes:

- `POOL_DRAIN_GRACE_MS` 250 (the pre-2026-10-31 default: every gap over 250 ms drains) vs 5000 (a handle
  survives a 1 s gap, so no close checkpoint): one `up` carries both, as ABAB rounds.
- Replicas on the same disk vs a second disk (`docker-compose.replicadisk.yml`, with or without
  `RIG_DISK_CAP`, section 1): a different `up`, so a fresh `colima restart` each.

```bash
cd deploy/chaos
for layout in same second; do
  colima restart
  if [ $layout = second ]; then
    # replica-dir wipe from docker-compose.replicadisk.yml's header, then:
    export COMPOSE_FILE=docker-compose.yml:docker-compose.replicadisk.yml
  else
    unset COMPOSE_FILE
  fi
  REPLICATION_ENABLED=true SHARD_FLUSH_INTERVAL_MS=30000 ./chaos.sh up
  TPC_NET=container TPC_DRIVER=elixir REPLICATION_ENABLED=true \
    ./chaos.sh tpc-sparse 512 120 1000 250,5000,250,5000     # then 1024 tenants
  ./chaos.sh down
done
```

Each round is `ops x think_ms` (2 minutes at the defaults) plus the seed; 512 tenants at 1 op/s is about
512 ops/s fleet-wide, each one a pool reopen. `think_ms` must stay well above 250 (the arm warns under
500) or the 250 arm does not drain every gap. `TPC_AUTH=required` composes with it, which is the
realistic prod posture for sparse tenants.

**Read.** `hit%` first: about 0 at grace 250 and near 100 at 5000 with a 1 s gap, or the grace did not
take and the rounds are not comparable. Then p99 for the two 250 rounds and the two 5000 rounds; the
spread within a grace is the noise floor. `ops/s` below `offered` means the fleet or the driver fell behind
the schedule and the p99 then includes that queueing. With the finding right, the 250 rounds should also
show `fathom_replication_commit_stage` groups with `reset="true"` (the first commit after a checkpoint)
at about one per op, and the 5000 rounds almost none (one new WAL generation per gap versus none). A `clock lags` count above 0 invalidates a round.

**What would falsify it.** The review's own: if p99 does not move between 250 and 5000 (outside the spread of
the paired rounds), the close checkpoint is not the cost and the finding drops. Second axis: if the
second-disk layout does not lower the grace-250 p99 relative to the same-disk layout, the idle-gap cost
is not fsync-behind-replica-writes. This arm does not touch memory: a longer grace holds about 156 KB plus
cache per idle handle, which is the trade a prod default change would also have to weigh.
