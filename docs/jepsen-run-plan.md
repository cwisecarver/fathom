# Jepsen run plan for fathom

Drafted 2026-10-28, after the first LazyFS power-loss runs. Built from three inputs:

- a read of the official Jepsen analyses of transactional and SQL systems: MariaDB Galera 12.1.2, Amazon RDS for PostgreSQL 17.4, TigerBeetle 0.16.11, MySQL 8.0.34, PostgreSQL 12.3, TiDB 2.1.7, YugabyteDB 1.1.9 and 1.3.1, FaunaDB 2.5.4, CockroachDB beta-20160829, VoltDB 6.3, Dgraph 1.1.1 and NATS JetStream 2.12.1;
- the Jepsen toolkit itself: `jepsen.nemesis.combined`, `jepsen.lazyfs` and Elle;
- a map of `jepsen/fathom/` and `deploy/chaos/` as they stand at 4ffe3975.

Numbers quoted from the analyses come from summaries of the reports; check the report before citing one.

## What the analyses teach

1. **Engine anomalies show up without faults, and fast.** Postgres 12.3 G2-item, MySQL Repeatable Read, the RDS long fork and the Galera stale reads all reproduced on a healthy cluster within 30 to 120 s at high contention. The cheapest run is a fault-free, high-rate one.
2. **Durability and recovery bugs need faults, often two or more at once.**
   - Galera lost writes only with crash plus partition, "once every few hours".
   - YugabyteDB lost inserts only when partitions overlapped a membership change, "a handful in tens of hours".
   - FaunaDB needed a crash just after a partial sync, then a restart.
   - Our own bug 9 needed power loss, a pause and an S3 cut together.
3. **Rare bugs are found by total fault-hours, not by a single long run.** Each report sums many runs. Long single runs matter only for slow-onset problems: the YugabyteDB process leak (~1,200 s), TigerBeetle's ~1,000 s latency tail, and memory growth over hours.
4. **The operation the system does rarely is where the bugs are.** All 5 new Dgraph bugs came from tablet moves. TiDB's came from region shuffles, and FaunaDB's from topology changes. For fathom the equivalent is the **rebalancer move, migration flip, fork, suspend/delete and roster change**, and none of them run under Jepsen today.
5. **Corruption is a fault class of its own.** TigerBeetle (bitflip, misdirected write, lost write) and NATS (`.blk` corruption lost 679k acknowledged writes) both found their worst bugs there.
6. **Retried requests with lost responses duplicate writes.** This was the CockroachDB set bug. fathom's client classifies timeouts as `:info`, but nothing exercises a client that *retries*.
7. **Every run heals, waits, then reads back every acknowledged write.** We already do this (`FinalReads`, `--quiet-period`).

## Where fathom stands

| Area | Today | Gap |
|---|---|---|
| Workload | Elle list-append per shard, strict-serializable | No bank (read skew), no interactive multi-request txns, no retrying client |
| Process faults | kill, pause, kill-majority, rolling-restart, power-loss (one node) | No **kill-all** (coordinated crash, the Galera finding) |
| Network | whole-node isolate, flap, per-node S3 and inbound replication cuts | No **node-to-node partitions** (majority/minority, ring, bridge) while both sides reach S3; the containers have no iptables |
| Clock | global VM step (jump/back) | No **per-node skew or strobe** (shared VM kernel) |
| Storage | LazyFS drop of un-fsynced writes | No **bitflip / truncate** of shard `.db`, WAL, replica files or the S3 object; no **disk full** |
| Topology | fixed 5 nodes, static followers | No **roster membership** change, no **rebalancer move**, no **migration**, no **fork / suspend / delete** under faults |
| Client path | Hrana v2 HTTP pipeline, one batch per txn | No WebSocket, no baton/stream resume, no `:ro` tokens, no auth |
| Liveness | only implicit | No checker for "every shard writable again within X s of the heal" (A05 had 6 shards with zero ok txns and passed as `:unknown`) |
| Object store | MinIO with weak conditional delete | Real S3 never run |

## Harness work, in priority order

Each item names the bug class it buys.

**P1, cheap and high yield**

1. **Liveness checker.** After the final heal, every shard must commit within N seconds (start at 60), and a shard with zero ok txns is a failure, not `:unknown`. This turns A05-style runs into real verdicts.
2. **`kill-all` fault.** SIGKILL all 5 nodes together, then restart them. Under fathom's claims a process crash loses nothing (the page cache survives), so any loss is a bug. This is the Galera crash-all case.
3. **Bank workload.** A per-shard set of accounts with conserved totals, read in full. It catches read skew and lost updates that list-append may miss on predicate reads, and it costs about 100 lines (the client already speaks SQL).
4. **Rebalancer-move nemesis.** Drive `chaos.sh rebalance`-style moves (warm, flip the LB map, drain the lease) as a fault, mixed with kill/pause/s3-cut. This is the Dgraph lesson, and the highest-value new fault.
5. **Node-to-node partitions.** Add `NET_ADMIN` and iptables to the node image, then partition shapes from `nemesis.combined`: one, minority-third, majority, majorities-ring, plus a "primary cut from its followers but not from S3" shape. This tests the quorum and lease logic under real splits.

**P2, more work**

6. **File corruption nemesis.** Bitflip and truncate on a random shard `.db`, `-wal`, replica file or the stored S3 object, on a minority of nodes. fathom has integrity gates; this proves they fire.
7. **Interactive transactions and a retrying client.** Hold a stream across requests with a baton, and retry on timeout the way `django-libsql` does. This is the CockroachDB duplicate-write class.
8. **Disk-full fault.** Fill the replica volume (back-pressure refuses seeds) and the data volume.
9. **Roster membership change** under faults (`REPLICATION_MEMBERSHIP=roster`): add and remove a follower mid-run.
10. **Per-node clock skew and strobe** via libfaketime in the node container. First, prove the BEAM honours it, since the BEAM reads time through the vDSO.

**P3**

11. Migration flip, fork, suspend/resume and delete as nemesis ops.
12. A WebSocket client path, and `:ro` tokens with auth on.
13. One run against real S3 (conditional DELETE fence).

## The run tiers

Every run uses a fresh rig, an image built from HEAD (`run.sh` checks), the clock-lag check, `caffeinate -s`, and a `colima restart` every 3rd run. Teardown aborts FUSE connections first (see `logs/ds/lz-a01.sh`).

### Tier 0: smoke (before any push touching lease, flush or replication)

- 1 x 2 min fault-free, `--rate 1000`, 4 keys per shard: engine anomalies.
- 3 x 10 min hard mix, `--max-concurrent-faults 3`, faults `power-loss,kill,pause,isolate,repl-cut,s3-cut`.
- Plus the standard `./chaos.sh rpo`.
- About 40 min. It replaces "one green 30-min run" as the push gate.

### Tier 1: the matrix (weekly, and after any new fault or workload)

10 min per cell, one fault family per cell, then pairs. Workloads: append, plus bank once it exists.

| Cell | Faults |
|---|---|
| healthy | none, high rate |
| crash | kill, kill-all, rolling-restart |
| power | power-loss |
| stall | pause, minio-pause, postgres-pause |
| network | isolate, flap, then partitions once P1-5 lands |
| storage-link | s3 family |
| replication-link | repl family |
| clock | clock-jump, clock-back, then skew/strobe once P2-10 lands |
| topology | rebalancer move (P1-4), then roster (P2-9) |
| corruption | bitflip/truncate (P2-6) |
| pair mixes | power+pause+s3-cut, kill+partition, move+kill, move+power |

That is 11 cells x 2 workloads x 10 min, about 4 hours with setup. Run it as one unattended batch.

### Tier 2: soak (overnight, for the rare bugs)

- About 40 x 10 min hard runs with `--max-concurrent-faults 3`, each drawing a random fault subset from everything available, with the topology ops weighted up.
- That gives about 6 to 7 fault-hours per night. Galera's rare loss showed "every few hours" and YugabyteDB's in "tens of hours", so a single night is a sample, not a proof. Run it weekly.
- Log the fault set and seed per run, so a failing combination can be re-run directly.

### Tier 3: long runs (monthly)

- 2 x 90 min with a mild fault mix: one fault, a 60 s interval.
- Add a resource checker: per-node RSS, open fds, replica-dir and data-dir bytes, and WAL sizes, sampled every 30 s. Fail on unbounded growth.
- This targets leaks, slow recovery tails, and WAL or replica growth, the YugabyteDB and TigerBeetle classes.

## When a bug counts as fixed

- A regression test in `mix test`, probed against the unfixed code (the existing rule).
- 3 clean 10-min runs of the exact fault mix that found the bug, on an image built after the fix.
- The next Tier 1 run passes.

## Order of work

1. P1-1 liveness checker and P1-2 `kill-all` (about a day), then Tier 0 becomes the push gate.
2. P1-3 bank and P1-4 rebalancer move, then the first full Tier 1.
3. P1-5 partitions, then the first Tier 2 night.
4. P2 items as they land, each added to Tier 1 the week it lands.
