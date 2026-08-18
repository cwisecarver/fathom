# Measuring A2 on three real machines — plan (not yet run)

**The question this answers, and it is the only one that matters right now: was the ≤256-tenant
replication ceiling a property of fathom, or of running five nodes on one 12-vCPU VM?**

Every A2 number ever recorded comes from `deploy/chaos`, where five fathom nodes, nginx, MinIO,
Postgres and the load driver share one machine and talk over loopback. That rig proved the failure
modes (`docs/reviews/a2-feedback-loop-fixed-2026-08-17.md`) but it structurally cannot separate
"fathom saturates" from "the box saturates". Three separate machines can.

Status: **planned, not executed.** Nothing below has been measured.

## The measured constant everything rests on

**A TPC-B commit produces ~17.3 KB of WAL** (mean over 200 commits; p50 16,480 B = exactly 4 pages
at 4 KB, p95 28.8 KB, max 37 KB). Measured 2026-08-17 on a real `Fathom.Shard.Connection`.

Two traps in measuring it, both hit on the first attempt:

* `wal_autocheckpoint` recycles the WAL in place, so a file-size delta reads **zero growth**. Turn it
  off for the probe.
* `wal_checkpoint(RESTART)` only rewinds the file — SQLite then reuses the allocated bytes and the
  size still does not move. Use `TRUNCATE`. This is the same "file size is not the WAL's identity"
  trap `Fathom.Shard.Replication.Wal`'s moduledoc describes, arrived at from the other direction.

From that constant, with `F` followers, per node:

    egress bytes/s  =  txn/s  x  17.3 KB  x  F
    ingress bytes/s =  (sum of peers' txn/s) x 17.3 KB

## Prerequisite: wire them. Do not run this over Wi-Fi

Wi-Fi is a **shared** medium and every replicated byte crosses the air twice (node → AP → node), so
the whole cluster shares one budget. Ethernet is switched: each host gets a dedicated full-duplex
link.

At 17.3 KB/commit with F=2:

| medium | usable | cluster txn/s (ceiling) |
|---|---|---|
| Wi-Fi 6 (~1 Gbps aggregate, shared) | 125 MB/s total | **~1,800** |
| Wi-Fi 7 (~2.5 Gbps aggregate, optimistic) | 312 MB/s total | ~4,500 |
| **1 GbE switched** | 125 MB/s **per host** | **~10,800** |
| 2.5 GbE switched | 312 MB/s per host | ~27,000 |

The existing one-VM rig already does 3,418 txn/s. **On Wi-Fi the radio would bind before the CPUs
did, and the run would answer nothing** — it could even read as a regression. A gigabit switch and
three cables (~$50) is the difference between an experiment and a wasted weekend. 2.5 GbE USB
adapters (~$25/host) put it firmly back on CPU.

Wi-Fi is worth keeping for a *different* run — see "Later: the jitter run".

## Topology

Three fathom nodes, one per host. Shared services have to live somewhere, so put them where there is
headroom and record the handicap rather than pretending it is absent.

| host | fathom node | also runs |
|---|---|---|
| Ryzen 9, 128 GB | `fathom3` | **the load driver** (most headroom) |
| M1 Mac Mini | `fathom1` | nginx LB, MinIO |
| laptop | `fathom2` | Postgres |

**Detecting the handicap:** the sweep prints a per-node shard/query split. If the driver's host node
is consistently the low one beyond the consistent-hash spread (~1.2x is normal), that node is
CPU-starved by the driver and its numbers are a floor, not a measurement.

**MinIO on one host is a real change from the rig**, where object storage was loopback. Every other
node's cold-open pulls and durability flushes now cross the network. Raise
`SHARD_FLUSH_INTERVAL_MS` (rig uses an artificially fast 5000) to 30000+ so flush traffic does not
contaminate the throughput reading; the rig's 5 s exists to make failover scenarios observable, not
because it is realistic.

### Quorum with three nodes

Each node has **2 peers**, and `Quorum.new/2` refuses `q >= n` — so **q=1 is forced**. That is still
replicate-before-ack, but weaker than the q=2 a production fleet would run: losing the primary and
the one follower that acked loses the write.

If you specifically want to measure q=2 behaviour, run **two fathom nodes per host** (6 nodes, 5
peers each, q=2). That is a legitimate *throughput* test and a dishonest *failure-domain* test — two
nodes on one host die together. Do not mix the two purposes in one run.

## Per-host configuration

Run native releases (`MIX_ENV=prod mix release`) rather than Docker. Each host builds for its own
architecture, which sidesteps the aarch64 (M1, laptop) / x86_64 (Ryzen) split entirely, and avoids
container NAT in the middle of a network measurement. Each host needs a Rust toolchain —
`native/fathom_udf` is a loadable SQLite extension and `mix compile.fathom_udf` **skips with a
printed line if cargo is absent**, so a silent build without it is possible. Verify per host:

    bin/fathom rpc 'IO.inspect(Fathom.Shard.Extension.available?())'

### Shared by every node

    DATABASE_URL=ecto://postgres:postgres@<laptop-ip>/fathom_bare
    SECRET_KEY_BASE=<mix phx.gen.secret>
    RELEASE_COOKIE=<shared secret>
    PHX_HOST=fathom.test
    SHARD_BASE_DOMAIN=fathom.test

    SHARD_STORAGE=s3
    S3_BUCKET=fathom-shards
    S3_ENDPOINT=http://<macmini-ip>:9000
    S3_PATH_STYLE=true
    AWS_ACCESS_KEY_ID=fathom
    AWS_SECRET_ACCESS_KEY=<secret>

    SHARD_DATA_DIR=/var/lib/fathom/shards
    SHARD_FLUSH_INTERVAL_MS=30000     # NOT the rig's 5000 — see above
    SHARD_IDLE_MS=900000              # keep shards open for the duration of a sweep
    SHARD_LEASE_TTL_MS=10000
    SHARD_LOAD=true                   # the per-node split readback depends on this

    REPLICATION_LISTEN=true
    REPLICATION_LISTEN_PORT=9100
    REPLICATION_DIR=/var/lib/fathom/replicas
    REPLICATION_QUORUM=1              # forced by n=2; see "Quorum with three nodes"
    REPLICATION_BIND_IP=<this host's LAN IP>   # SECURITY: the port is unauthenticated

`REPLICATION_BIND_IP` is not tuning. The replication port has **no authentication**, and unlike the
compose network there is now a real interface to hide from. Bind it to the LAN address and firewall
it to the other two hosts.

### Per node

    # fathom1 (Mac Mini)
    NODE_KEY=fathom1
    REPLICATION_ADVERTISE_HOST=<macmini-ip>
    REPLICATION_FOLLOWERS=fathom2@<laptop-ip>:9100,fathom3@<ryzen-ip>:9100

    # fathom2 (laptop)
    NODE_KEY=fathom2
    REPLICATION_ADVERTISE_HOST=<laptop-ip>
    REPLICATION_FOLLOWERS=fathom1@<macmini-ip>:9100,fathom3@<ryzen-ip>:9100

    # fathom3 (Ryzen)
    NODE_KEY=fathom3
    REPLICATION_ADVERTISE_HOST=<ryzen-ip>
    REPLICATION_FOLLOWERS=fathom1@<macmini-ip>:9100,fathom2@<laptop-ip>:9100

**`REPLICATION_ENABLED` is deliberately absent above.** Listening must be on fleet-wide *before* any
node ships, or every tenant write 503s `FILO_NO_QUORUM`. Boot all three with listening only, confirm
it, then enable shipping. That ordering is the documented rollout and the rig demonstrates it for a
reason.

### Ports between hosts

| port | direction | purpose |
|---|---|---|
| 8080 | LB → each node | Hrana |
| 8081 | LB → each node | health probe |
| 9100 | node ↔ node | replication (unauthenticated — firewall it) |
| 5432 | nodes → laptop | Postgres |
| 9000 | nodes → Mac Mini | MinIO |
| 4000 | you → any node | admin dashboard (optional) |

### LB

Copy `deploy/lb/fathom.nginx.conf` and replace the upstream with the three real IPs. The
`hash $host consistent;` line is the keyspace partition and must not change. `keepalive 512` is
sized against a 1024–4096 concurrent-tenant target; leave it.

    upstream fathom_hrana {
      hash $host consistent;
      server <macmini-ip>:8080 max_fails=2 fail_timeout=10s;
      server <laptop-ip>:8080  max_fails=2 fail_timeout=10s;
      server <ryzen-ip>:8080   max_fails=2 fail_timeout=10s;
      keepalive 512;
    }

Clients route by `Host: <shard>.fathom.test`, so point that wildcard at the LB in
`/etc/hosts` on the driver host, or use a resolver that wildcards.

## Running it

The driver is `deploy/chaos/tpc_driver.exs` and takes a plain URL, so it works against real hosts
with no rig involvement:

    elixir deploy/chaos/tpc_driver.exs rtt  --lb http://<lb-ip>:8080 --domain fathom.test \
      --shard rttprobe --samples 200

    elixir deploy/chaos/tpc_driver.exs tpcb --lb http://<lb-ip>:8080 --domain fathom.test \
      --shard tfleet256 --txns 102400 --clients 256 --accounts 1000

`--clients N` is the tenant count; `--txns` should be `400 x N` to match the recorded sweeps. It
prints one JSON object to stdout.

**Order of runs. Do not skip the first two — a result you cannot compare to anything is not a
result.**

1. **Baseline, replication OFF**, 256 / 512 / 1024. Establishes what these three machines do at all,
   and is the control for everything after.
2. **Turn shipping on** (`REPLICATION_ENABLED=true`, all three), re-verify with a single write.
3. **Replication ON**, the same 256 / 512 / 1024.
4. If 1024 is clean, keep doubling until it is not. **That number is the answer.**

Between runs: restart the nodes. Back-to-back heavy runs degrade a host, and the contamination
signature is run 1 healthy with runs 2–3 collapsing — which does not look like a code regression and
must not be read as one.

## What to measure, and the pass conditions

Read these per node via `bin/fathom rpc`, using the tools built for the OOM investigation:

    deploy/chaos/bin_holders.sh   # who holds the binaries (sums Process.info(pid, :binary))
    deploy/chaos/bin_sizes.sh     # what they hold: a size histogram

Both take a container name and will need a small edit to shell out to a remote host instead
(`ssh <host> bin/fathom rpc ...`). The probe strings inside them are the valuable part.

| signal | pass |
|---|---|
| **largest payload** | exactly **1,048,576 B** and never above — the cap binding |
| **mean payload**, two samples ~40 s apart | flat or falling |
| node `:erlang.memory()[:binary]` | bounded, sawtooth, **falling** between samples |
| errors | 0 at the tenant count you are claiming |
| per-node split | ~1.2x or better, and the driver's host not an outlier |

**Watch the mean and largest payload, never the queue depth.** Queue depth was flat (8,265 → 8,195)
across a doubling of held memory (6,893 → 15,798 MB) and would have reported success. That is the
single most expensive lesson A2 has produced.

`:overloaded` rejects mean the per-node byte budget fired — expected only under genuine saturation.
`:already_in_flight` is the known weak point and is **pre-existing, not caused by the catch-up
loop** (the 512 A/B clears it: 21,442 uncapped vs 8,781 capped). If it drops sharply on real
hardware, that is evidence the straggler problem is variance-driven and worth attacking.

## What this cannot tell you

* **Cross-AZ latency and cost.** All three hosts are one LAN hop apart. Production A2 across AZs
  pays real RTT on every quorum wait, and AWS bills every replicated byte `F` times in each
  direction — at F=4 and sustained load that transfer bill plausibly exceeds the instance bill.
  Decide placement before a cloud scale test, not after.
* **Fleet size.** Three nodes says nothing about fifty. Each node ships to a fixed small follower
  set, so it should not be O(N^2) — but per-shard follower sets and zone-aware placement are **not
  built**, so the follower set does not follow a shard when the rebalancer moves it.
* **q=2 behaviour**, unless you run the 6-node variant above.

## Later: the jitter run

Once the wired numbers exist, **redo it on Wi-Fi on purpose.** High, variable latency with
occasional multi-hundred-millisecond spikes is a decent stand-in for a bad cross-AZ link, and every
A2 resilience number to date is loopback. Specifically watch whether `send_timeout` (5 s, tears down
the socket and fails every waiter on it) starts firing on ordinary jitter, and whether
`:already_in_flight` climbs. That is a real gap in A2's evidence and this hardware is well suited to
filling it.
