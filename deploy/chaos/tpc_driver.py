#!/usr/bin/env python3
"""Fathom chaos-rig remote TPC driver (Phase 4, docs/tpc-benchmark-plan.md).

A dep-free (stdlib only) libSQL/Hrana **remote client** that drives the TPC-B and TPC-C
workloads over the real network through the LB — the realism layer the in-process loopback WS
gate (`mix fathom.wire_bench` / `mix fathom.tpcc`) cannot reach. It speaks Hrana v2 over HTTP
`/v2/pipeline` with batons (a persistent, keep-alive TCP connection per client stream), routing
each stream to its shard by the `Host: <shard>.<domain>` header exactly as django-libsql /
libsql-experimental do, so the subdomain hashes to a node as in prod.

Every transaction is sent **statement-by-statement on a held stream** (chatty, non-batching —
the real client shape), so per-txn latency includes each statement's network round-trip. Timing
is `time.perf_counter()` in one long-lived process (no per-statement subprocess spawn), so the
numbers are the real client-visible latency, not shell-tool overhead.

Recorded-only: TPC latencies are host/fsync-sensitive, so this is a peer-comparability + true-
network-path characterization, never a gate. Invoked by `chaos.sh tpcb` / `chaos.sh tpcc`.

Modes:
    tpc_driver.py rtt   --lb URL --domain D --shard S [--samples N]
    tpc_driver.py tpcb  --lb URL --domain D --shard S [--txns N --clients C --accounts A]
    tpc_driver.py tpcc  --lb URL --domain D [--max-w W --threads T --txns N --scale F]

All modes print a human summary to stderr and a single JSON result object to stdout (so
chaos.sh can tee it into a docs/reviews report).
"""

import argparse
import http.client
import json
import random
import statistics
import sys
import threading
import time
from urllib.parse import urlparse


# --- Hrana v2 HTTP client (baton-threaded, one persistent stream) ---------------------------

class HranaError(RuntimeError):
    pass


class Stream:
    """One Hrana stream to one shard over a persistent HTTP connection through the LB.

    `execute` sends a single statement carrying the current baton and returns decoded rows; the
    stream stays open (the returned baton threads to the next call), so a transaction is a burst
    of executes on the held connection — the real per-statement round-trip model.
    """

    def __init__(self, lb_url, domain, shard, timeout=15.0):
        u = urlparse(lb_url)
        self.host, self.port = u.hostname, (u.port or 80)
        self.authority = f"{shard}.{domain}"
        self.timeout = timeout
        self.conn = http.client.HTTPConnection(self.host, self.port, timeout=timeout)
        self.baton = None

    def reconnect(self):
        """Drop the stream and open a fresh connection (baton reset). The server rolls back any
        uncommitted transaction on the abandoned stream. Used to recover from a transient error
        (e.g. a 502 while the rebalancer flips a hot shard between nodes) without losing the
        worker — a real client reconnects and retries the same way."""
        try:
            self.conn.close()
        except Exception:
            pass
        self.conn = http.client.HTTPConnection(self.host, self.port, timeout=self.timeout)
        self.baton = None

    def _post(self, requests):
        body = json.dumps({"baton": self.baton, "requests": requests})
        # Override the Host header so the LB routes by subdomain (as a real client does).
        self.conn.request("POST", "/v2/pipeline", body,
                          {"Host": self.authority, "Content-Type": "application/json"})
        resp = self.conn.getresponse()
        raw = resp.read()
        if resp.status != 200:
            raise HranaError(f"HTTP {resp.status}: {raw[:200]!r}")
        doc = json.loads(raw)
        self.baton = doc.get("baton")
        return doc["results"]

    def execute(self, sql, args=()):
        results = self._post([{"type": "execute",
                               "stmt": {"sql": sql, "args": [_enc(a) for a in args]}}])
        r = results[0]
        if r["type"] != "ok":
            raise HranaError(f"{sql[:60]}...: {r.get('error')}")
        res = r["response"]["result"]
        return [[_dec(v) for v in row] for row in res["rows"]]

    def close(self):
        try:
            self._post([{"type": "close"}])
        except Exception:
            pass
        self.conn.close()


def _enc(v):
    if v is None:
        return {"type": "null"}
    if isinstance(v, bool):
        return {"type": "integer", "value": str(int(v))}
    if isinstance(v, int):
        return {"type": "integer", "value": str(v)}
    if isinstance(v, float):
        return {"type": "float", "value": v}
    return {"type": "text", "value": str(v)}


def _dec(v):
    t = v["type"]
    if t == "null":
        return None
    if t == "integer":
        return int(v["value"])
    if t == "float":
        return v["value"]
    if t == "text":
        return v["value"]
    return v.get("value")


def pctls(samples):
    """p50/p95/p99/max in µs (samples are seconds); None if empty."""
    if not samples:
        return {"p50_us": None, "p95_us": None, "p99_us": None, "max_us": None}
    xs = sorted(samples)

    def q(p):
        if len(xs) == 1:
            return xs[0]
        rank = p * (len(xs) - 1)
        lo = int(rank)
        hi = min(lo + 1, len(xs) - 1)
        frac = rank - lo
        return xs[lo] * (1 - frac) + xs[hi] * frac

    return {
        "p50_us": round(q(0.50) * 1e6, 1),
        "p95_us": round(q(0.95) * 1e6, 1),
        "p99_us": round(q(0.99) * 1e6, 1),
        "max_us": round(max(xs) * 1e6, 1),
    }


# --- RTT probe ------------------------------------------------------------------------------

def mode_rtt(args):
    s = Stream(args.lb, args.domain, args.shard)
    s.execute("SELECT 1")  # warm the stream (open + cold-open), untimed
    lat = []
    for _ in range(args.samples):
        t0 = time.perf_counter()
        s.execute("SELECT 1")
        lat.append(time.perf_counter() - t0)
    s.close()
    p = pctls(lat)
    print(f"  rtt over {args.samples} warm SELECT 1 round-trips through the LB: "
          f"p50={p['p50_us']}µs p95={p['p95_us']}µs p99={p['p99_us']}µs", file=sys.stderr)
    return {"mode": "rtt", "shard": args.shard, "samples": args.samples,
            "rtt_us": p["p50_us"], **{f"rtt_{k}": v for k, v in p.items()}}


# --- TPC-B (pgbench bank txn: 7 statements, no value-feeding) --------------------------------

TPCB_SCHEMA = [
    "CREATE TABLE IF NOT EXISTS branches (bid INTEGER PRIMARY KEY, bbalance INTEGER)",
    "CREATE TABLE IF NOT EXISTS tellers (tid INTEGER PRIMARY KEY, bid INTEGER, tbalance INTEGER)",
    "CREATE TABLE IF NOT EXISTS accounts (aid INTEGER PRIMARY KEY, bid INTEGER, abalance INTEGER)",
    "CREATE TABLE IF NOT EXISTS history (tid INTEGER, bid INTEGER, aid INTEGER, delta INTEGER, mtime TEXT)",
]
TPCB_TELLERS = 10


def tpcb_seed(s, accounts):
    for ddl in TPCB_SCHEMA:
        s.execute(ddl)
    s.execute("INSERT OR IGNORE INTO branches (bid, bbalance) VALUES (1, 0)")
    s.execute(f"WITH RECURSIVE seq(i) AS (SELECT 1 UNION ALL SELECT i+1 FROM seq WHERE i < {TPCB_TELLERS}) "
              "INSERT OR IGNORE INTO tellers (tid,bid,tbalance) SELECT i,1,0 FROM seq")
    s.execute(f"WITH RECURSIVE seq(i) AS (SELECT 1 UNION ALL SELECT i+1 FROM seq WHERE i < {accounts}) "
              "INSERT OR IGNORE INTO accounts (aid,bid,abalance) SELECT i,1,0 FROM seq")


def tpcb_txn(s, accounts):
    aid = random.randint(1, accounts)
    tid = random.randint(1, TPCB_TELLERS)
    delta = random.randint(-5000, 5000)
    s.execute("BEGIN IMMEDIATE")
    s.execute("UPDATE accounts SET abalance=abalance+? WHERE aid=?", (delta, aid))
    s.execute("SELECT abalance FROM accounts WHERE aid=?", (aid,))
    s.execute("UPDATE tellers SET tbalance=tbalance+? WHERE tid=?", (delta, tid))
    s.execute("UPDATE branches SET bbalance=bbalance+? WHERE bid=1", (delta,))
    s.execute("INSERT INTO history (tid,bid,aid,delta,mtime) VALUES (?,1,?,?,?)",
              (tid, aid, delta, str(int(time.time()))))
    s.execute("COMMIT")


def mode_tpcb(args):
    # One client per TENANT shard (fathom's real model: each tenant is its own single-writer
    # file), so there is no intra-shard lock convoy — the per-txn latency is clean and the
    # aggregate is the realistic multi-tenant node throughput. `--clients C` = C tenant shards.
    per_client = max(1, args.txns // args.clients)
    lat_lock = threading.Lock()
    lat = []
    spans = []
    errs = [0]

    def worker(cid):
        random.seed((cid + 1) * 7919)
        shard = f"{args.shard}_{cid}" if args.clients > 1 else args.shard
        s = Stream(args.lb, args.domain, shard)
        tpcb_seed(s, args.accounts)  # seed this tenant's own shard (untimed)
        local, e = [], 0
        t0 = time.perf_counter()
        for _ in range(per_client):
            a = time.perf_counter()
            try:
                tpcb_txn(s, args.accounts)
                local.append(time.perf_counter() - a)
            except HranaError:
                e += 1
                s.reconnect()  # transient (e.g. rebalancer flip) — reconnect, skip this txn
        t1 = time.perf_counter()
        s.close()
        with lat_lock:
            lat.extend(local)
            spans.append((t0, t1))
            errs[0] += e

    run_threads([threading.Thread(target=worker, args=(c,)) for c in range(args.clients)])

    window = max(e for _, e in spans) - min(a for a, _ in spans)
    tps = round(len(lat) / window, 1) if window > 0 else 0.0
    p = pctls(lat)
    print(f"  tpcb: {len(lat)} txns across {args.clients} tenant shard(s), {tps} txn/s through "
          f"the LB ({errs[0]} transient errs) — p50={p['p50_us']}µs p95={p['p95_us']}µs "
          f"p99={p['p99_us']}µs", file=sys.stderr)
    return {"mode": "tpcb", "shard": args.shard, "tenant_shards": args.clients, "txns": len(lat),
            "errors": errs[0], "tpcb_tps": tps, **{f"tpcb_{k}": v for k, v in p.items()}}


# --- helpers --------------------------------------------------------------------------------

def run_threads(threads):
    for t in threads:
        t.start()
    for t in threads:
        t.join()


def main():
    ap = argparse.ArgumentParser(description="Fathom remote TPC driver (rig / Phase 4)")
    ap.add_argument("mode", choices=["rtt", "tpcb", "tpcc"])
    ap.add_argument("--lb", default="http://localhost:8080")
    ap.add_argument("--domain", default="fathom.test")
    ap.add_argument("--shard", default="tpc")
    ap.add_argument("--samples", type=int, default=200)
    ap.add_argument("--txns", type=int, default=2000)
    ap.add_argument("--clients", type=int, default=8)
    ap.add_argument("--accounts", type=int, default=100000)
    ap.add_argument("--max-w", type=int, default=5)
    ap.add_argument("--threads", type=int, default=8)
    ap.add_argument("--scale", type=float, default=1.0)
    args = ap.parse_args()

    if args.mode == "rtt":
        out = mode_rtt(args)
    elif args.mode == "tpcb":
        out = mode_tpcb(args)
    else:
        from tpcc_deck import mode_tpcc
        out = mode_tpcc(args, Stream, pctls, run_threads)

    print(json.dumps(out))


if __name__ == "__main__":
    main()
