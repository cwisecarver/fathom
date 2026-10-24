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

## Faults

There is one fault at a time, on one random node. Each fault lasts `--nemesis-interval`
seconds (default 20), which is longer than the lease TTL plus the steal margin (15 s), so a
steal can really happen. The last `--quiet-period` seconds (default 30) have no faults, so
reads can see what survived.

| fault     | what it does                                                      |
|-----------|-------------------------------------------------------------------|
| `kill`    | disconnect from the network, then SIGKILL; revived with `docker start` |
| `pause`   | `docker pause`: the zombie owner that wakes up and tries to flush |
| `isolate` | disconnect from the network: no LB, peers, S3 or Postgres         |
| `s3-cut`  | disable the node's toxiproxy S3 proxy: lease renewals fail        |

## Targets

- `--target lb` (default): every request goes through nginx, like a real client.
- `--target direct`: each client talks straight to one node, so non-owners get requests too.
  This tests the lease refusal and steal paths.

## Running

You need Java and Leiningen (`brew install openjdk leiningen`). Bring the rig up **with
replication on**, which is prod's default. With replication off, a killed owner loses its
unflushed acked writes by design, and Elle reports them as lost.

```bash
cd deploy/chaos
./chaos.sh build                         # `up` does not build
REPLICATION_ENABLED=true ./chaos.sh up
cd ../../jepsen/fathom

./run.sh --time-limit 60 --faults none   # smoke test: should be valid
./run.sh --time-limit 300               # 5 min, through the LB, all faults
./run.sh --target direct --faults pause,kill
./run.sh serve                           # results on http://localhost:8080 (stop the rig LB or pass -p)
```

`run.sh` refuses to start while a bench lock is held, checks that replication is on and the
image is not older than HEAD, and marks the run **invalid** (exit 2) if any node logged
`clock lags the store` (a colima clock step).

Results go in `store/<test>/<time>/`. Look at `results.edn` first, then `elle/` under each
shard's `independent/<n>/` folder for anomaly explanations and cycle graphs.

Unit tests for the client's batch layout and outcome classification: `lein test`.
