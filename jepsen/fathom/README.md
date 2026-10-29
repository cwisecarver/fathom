# Jepsen tests for fathom

A [Jepsen](https://jepsen.io) test that runs against the `deploy/chaos` rig: 5 fathom nodes
behind the nginx LB, MinIO for S3, and toxiproxy in front of S3.

## What it checks

fathom promises one writer per shard (one SQLite file). So each shard's history should be
**strict-serializable**. The test checks that with Elle's list-append workload:

- 8 shards at once, 2 client threads per shard, 4 keys per shard at a time.
- Each transaction stays inside one shard. It is one Hrana batch: `BEGIN IMMEDIATE`, the
  reads and appends, then `COMMIT` only if every step before it succeeded, and `ROLLBACK`
  only if `COMMIT` did not.
- Elle checks each shard on its own and reports lost writes, stale reads, G1 (dirty or
  aborted reads) and G2 (cycles).
- A liveness check (`jepsen.fathom.liveness`) fails the run when a shard takes no ok write
  after the faults are healed. Elle judges only what completed, so a shard stuck refusing
  writes used to pass, or come back `:unknown`. `--recovery-deadline SECS` also fails a
  shard that took longer than that to write again.

## Faults

By default there is one fault at a time, on one random node, lasting `--nemesis-interval` seconds
(default 20, longer than the lease TTL plus the steal margin, 15 s, so a steal can really happen).
`--hard` instead mixes faults in shifting combinations, after Jepsen's Datomic and Bufstream
analyses: start one, maybe pile on more 0-10 s later (up to `--max-concurrent-faults`, default 2),
hold 3-30 s, heal everything, rest 0-15 s. The last `--quiet-period` seconds (default 30) have no
faults. Then every key each shard touched is read back.

| fault | what it does |
|---|---|
| `kill` | disconnect from the network, then SIGKILL; revived with `docker start` |
| `pause` | `docker pause`: the zombie owner that wakes up and tries to flush |
| `isolate` | disconnect from the network: no LB, peers, S3 or Postgres |
| `flap` | cut and restore one node's network every 1-3 s |
| `s3-cut` / `s3-blackhole` / `s3-slow` | one node's S3 path refuses / hangs / adds 1-3 s |
| `s3-reset` / `s3-truncate` | one node's S3 connections are reset / closed after 4-64 KB |
| `repl-cut` / `repl-blackhole` / `repl-slow` | the link into one node's follower port refuses / hangs / adds latency (needs `REPL_VIA_TOXI=1`) |
| `kill-majority` | kill 3 of 5 nodes |
| `kill-all` | kill all 5 nodes at once, then start them all (a coordinated crash; must lose nothing) |
| `rolling-restart` | kill and revive every node in turn |
| `minio-pause` / `postgres-pause` | SIGSTOP the object store / the control plane |
| `lb-restart` | restart nginx |
| `clock-jump` / `clock-back` | step the VM clock 20-60 s forward / back: every node and MinIO at once |
| `power-loss` | SIGKILL a node whose `/data` is on LazyFS, losing every write it had not fsynced; one node at a time (needs the LazyFS overlay, below) |

`--faults all` selects every fault except `power-loss`. Add it by name: `--faults all,power-loss`.
The default is `kill,pause,isolate,s3-cut`.

### Power loss (LazyFS)

A plain `kill` keeps the page cache, because the colima kernel outlives the container. To lose
un-fsynced writes the way a power failure does, bring the rig up with `/data` on
[LazyFS](https://github.com/dsrhaslab/lazyfs), the filesystem Jepsen's own analyses use for this:

```sh
cd ../../deploy/chaos
COMPOSE_FILE=docker-compose.yml:docker-compose.lazyfs.yml REPLICATION_ENABLED=true REPL_VIA_TOXI=1 ./chaos.sh up
```

LazyFS holds every write in its own cache until it is fsynced, so SIGKILLing the container drops
whatever was not. On this rig `kill` first writes the cache through (`lazyfs::cache-checkpoint`),
so it stays a process crash. `power-loss` hits one node at a time: followers ack from RAM by design
(`:replication_fsync` off) and take durability from the replica count, so a quorum losing power
together is outside what fathom claims to survive. The test refuses `power-loss` unless every
node's `/data` is a LazyFS mount.

## Targets

- `--target lb` (default): every request goes through nginx, like a real client.
- `--target direct`: each client talks straight to one node, so non-owners get requests too.
  This tests the lease refusal and steal paths.
- `--target mixed`: half the clients do each, so both kinds of traffic hit a shard at once.

## Running

You need Java and Leiningen (`brew install openjdk leiningen`). Bring the rig up **with
replication on**, which is prod's default. With replication off, a killed owner loses its
unflushed acked writes by design, and Elle reports them as lost.

```bash
cd deploy/chaos
./chaos.sh build                         # `up` does not build
REPLICATION_ENABLED=true REPL_VIA_TOXI=1 ./chaos.sh up
cd ../../jepsen/fathom

./run.sh --time-limit 60 --faults none   # smoke test: should be valid
./run.sh --time-limit 300               # 5 min, through the LB, all faults
./run.sh --target direct --faults pause,kill
./run.sh --time-limit 1800 --hard --target mixed --max-concurrent-faults 3 --faults all --shards 16 --rate 120
./run.sh serve                           # results on http://localhost:8080 (stop the rig LB or pass -p)
```

`run.sh` refuses to start while a bench lock is held, checks that replication is on and the
image is not older than HEAD, and marks the run **invalid** (exit 2) if any node logged
`clock lags the store` (a colima clock step), unless the run injects clock faults on purpose. With the
Mac's lid closed, macOS Maintenance Sleep freezes the VM for most of every minute; run under
`caffeinate -s`, not just `caffeinate -dims`.

Findings so far: [`docs/reviews/jepsen-2026-10-24.md`](../../docs/reviews/jepsen-2026-10-24.md).

Results go in `store/<test>/<time>/`. Look at `results.edn` first, then `elle/` under each
shard's `independent/<n>/` folder for anomaly explanations and cycle graphs.

Unit tests for the client's batch layout and outcome classification: `lein test`.
