# tpc-fleet, replication on, 1024 tenants (2026-10-30)

The capacity check from [`tpc-fleet-repl-on-2026-10-26.md`](tpc-fleet-repl-on-2026-10-26.md), re-run
on current code. Two things changed since that run: the rig ships to three followers per node, not
four (2026-10-27), and fathom moved to filo 0.4.3 (`7468ed98`). This run also measures that filo
change by running the commit before it (`ae4d4d41`, filo 0.4.2) the same way.

## Setup

```bash
cd deploy/chaos
./chaos.sh build && colima restart
REPLICATION_ENABLED=true SHARD_FLUSH_INTERVAL_MS=30000 ./chaos.sh up
TPC_NET=container TPC_DRIVER=elixir REPLICATION_ENABLED=true ./chaos.sh tpc-fleet 1024
```

Five nodes on one 12-vCPU colima VM, 1024 tenants (one shard each), 400 TPC-B transactions per
tenant, 1000 accounts per tenant. Each arm: a fresh image build, `colima restart`, then the run. The
nodes reported the filo version they ran (`Application.spec(:filo, :vsn)`) and
`REPLICATION_ENABLED=true`, `SHARD_FLUSH_INTERVAL_MS=30000`. No node logged `clock lags the store`
in either arm. Both arms used the same driver image. Scripts: `logs/ds/tpc043.sh`,
`logs/ds/tpc042.sh` (not committed).

## Results

| arm | txn/s | errors | shed | p50 | p95 | p99 (CI95) |
|---|---|---|---|---|---|---|
| **`7468ed98`, filo 0.4.3** | **4,343** | 0 | 0 | 217 ms | 332 ms | **385 ms** (384-385) |
| `ae4d4d41`, filo 0.4.2 | 4,419 | 0 | 0 | 214 ms | 325 ms | 381 ms (381-382) |
| 2026-10-26, four followers (`a39b1dcb`) | 4,317 | 0 | 0 | 215 ms | 304 ms | 598 ms (565-631) |

**filo 0.4.3 does not move the rig.** −1.7% throughput and +1% p99 from one run a side is inside
the noise band. Its gain is in encoding result rows (`wire_server_rows_per_s` +15.6% in
`mix fathom.bench`), and TPC-B transactions return almost nothing, so encoding is not what bounds
this workload.

**Against 2026-10-26, p99 fell from 598 to 385 ms at the same throughput.** That run had four
followers per node; this one has three, so this is a different topology and the change is not
attributable to code alone. The 2026-10-26 report traced its p99 to 30 s page-cache writeback
spikes (1,167 and 1,191 ms windows); `chaos.sh up` has set `vm.dirty_background_bytes` since, and
the 10 s windows here have no spike:

```
0.4.3: 0s=386 10s=398 20s=420 30s=384 40s=365 50s=382 60s=372 70s=389 80s=362 90s=248
0.4.2: 0s=356 10s=382 20s=397 30s=369 40s=358 50s=376 60s=401 70s=404 80s=361 90s=172
```

The number to quote for the current code is the 0.4.3 row: **4,343 txn/s, 0 errors, p99 385 ms**,
replication on, on the one-VM rig.

## Replicas on a second disk, re-run

The second-disk comparison from `tpc-fleet-repl-on-2026-10-26.md` ("Replicas on a second block
device"), repeated on `5d9d23e7` with the same method: open loop at 3,000 txn/s, 1024 tenants, three
followers, `docker-compose.replicadisk.yml` for the `disk` arm, a fresh `colima restart` and wiped
replica directories before every run, three runs a side alternated. `df` in fathom1 showed
`/data/replicas` on `/dev/vdb1` in every `disk` run and on the overlay root in every `base` run.
0 errors, 0 `clock lags`, 2,998.6-2,999.2 txn/s achieved. Script: `logs/ds/secdisk.sh`.

| replicas on | p99 per run (excl. first 10 s) | p99 avg | p50 |
|---|---|---|---|
| the root disk (`base`) | 115.8 / 131.0 / 129.0 ms | 125 ms | 17.1-19.0 ms |
| a second virtio disk (`disk`) | 49.6 / 48.6 / 43.1 ms | 47 ms | 15.2-18.3 ms |

Every `disk` run beat every `base` run, and the averages match 2026-10-27 (131 and 49 ms). Same
limit as then: both virtio disks are files on one host SSD.
