#!/usr/bin/env bash
# Multi-tenant head-to-head — fathom's N shards vs libsql-server's N namespaces.
#
# This is the comparison that stresses fathom's DESIGN POINT: many isolated single-writer tenant
# databases, fanned out. Both route by the same Host-subdomain mechanism, so tpc_driver.py drives
# both — against fathom each tenant is its own shard file; against sqld each is its own namespace
# (pre-created here via the admin API). Aggregate TPC-B throughput + per-tenant latency across a
# sweep of N tenants.
#
# Load model: N INDEPENDENT PROCESSES, one per tenant (not threads — the Python driver is GIL-bound
# under many threads and would measure the client, not the servers' fan-out). Aggregate tps = sum
# of the per-tenant process tps; latency = the median per-tenant p50/p99.
#
#   ./turso_multitenant.sh [ "N1 N2 N3" ]      # default: 8 32 64
#
# Prereqs: the fathom rig up (./chaos.sh up) and Docker. sqld (namespaces) is started fresh.
# NOTE: single host, steady-state, relative — a feel, not a cert. Durability differs (fathom rig
# image / S3-backed; sqld libSQL defaults / local disk). This is fan-out THROUGHPUT; tenant DENSITY
# (millions of shards vs sqld's --max-active-namespaces) is a separate axis.
set -u -o pipefail
cd "$(dirname "$0")"

if command -v docker >/dev/null 2>&1; then
  DOCKER=docker
else
  DOCKER=$(ls /opt/homebrew/Cellar/docker/*/bin/docker 2>/dev/null | head -1)
  [ -n "$DOCKER" ] || { echo "docker CLI not found" >&2; exit 1; }
  export DOCKER_HOST=${DOCKER_HOST:-unix://$HOME/.colima/default/docker.sock}
fi

SQLD_IMG=${SQLD_IMG:-ghcr.io/tursodatabase/libsql-server:latest}
SQLD_PORT=${SQLD_PORT:-8090}
SQLD_ADMIN=${SQLD_ADMIN:-8091}
SQLD_URL=http://localhost:$SQLD_PORT
FATHOM_URL=${FATHOM_URL:-http://localhost:18081}
NLIST=${1:-"8 32 64"}
PER=${PER:-300}   # TPC-B txns per tenant process
BASE=mt
OUT=$(mktemp -d)

reachable() { curl -sS --max-time 5 "$1/v2/pipeline" -X POST -H "Host: x.fathom.test" \
  -H "Content-Type: application/json" \
  -d '{"requests":[{"type":"execute","stmt":{"sql":"SELECT 1"}},{"type":"close"}]}' >/dev/null 2>&1; }

echo "starting a fresh sqld with namespaces (data :$SQLD_PORT, admin :$SQLD_ADMIN) ..."
"$DOCKER" rm -f libsql-ns >/dev/null 2>&1
"$DOCKER" run -d --name libsql-ns -p "$SQLD_PORT:8080" -p "$SQLD_ADMIN:8081" "$SQLD_IMG" \
  /bin/sqld --enable-namespaces --admin-listen-addr 0.0.0.0:8081 >/dev/null
for _ in $(seq 1 25); do curl -sS --max-time 3 "http://localhost:$SQLD_ADMIN/v1/namespaces/x/config" >/dev/null 2>&1 && break; sleep 1; done
reachable "$SQLD_URL" || { echo "sqld data plane not reachable" >&2; exit 1; }
reachable "$FATHOM_URL" || { echo "fathom not reachable at $FATHOM_URL — run ./chaos.sh up first" >&2; exit 1; }

# Pre-create the sqld namespaces the driver will address (fathom creates shards on demand).
maxn=0; for n in $NLIST; do [ "$n" -gt "$maxn" ] && maxn=$n; done
echo "creating $maxn sqld namespaces (${BASE}_0..${BASE}_$((maxn-1))) ..."
for i in $(seq 0 $((maxn-1))); do
  curl -sS --max-time 8 -X POST "http://localhost:$SQLD_ADMIN/v1/namespaces/${BASE}_${i}/create" \
    -H "Content-Type: application/json" -d '{}' >/dev/null 2>&1
done

run_batch() {  # run_batch <url> <N> <pref>: launch N one-tenant processes, one per shard/namespace
  local url=$1 n=$2 pref=$3 i pids=()
  for i in $(seq 0 $((n - 1))); do
    python3 tpc_driver.py tpcb --lb "$url" --domain fathom.test --shard "${BASE}_${i}" \
      --txns "$PER" --clients 1 --accounts 10000 2>/dev/null > "$OUT/${pref}_${i}.json" &
    pids+=($!)
  done
  wait "${pids[@]}" 2>/dev/null || true
}

for N in $NLIST; do
  echo "=== N=$N tenants × ${PER} txns each (independent processes) ==="
  run_batch "$FATHOM_URL" "$N" "f$N"
  run_batch "$SQLD_URL" "$N" "s$N"
done

python3 - "$OUT" "$NLIST" <<'PY'
import json, sys, os, glob, statistics
d, nlist = sys.argv[1], sys.argv[2].split()
def agg(pref):
    tps, p50, p99 = [], [], []
    for f in glob.glob(os.path.join(d, f"{pref}_*.json")):
        try: o = json.load(open(f))
        except Exception: continue
        for key, acc in (("tpcb_tps", tps), ("tpcb_p50_us", p50), ("tpcb_p99_us", p99)):
            if isinstance(o.get(key), (int, float)): acc.append(o[key])
    return (sum(tps), statistics.median(p50), statistics.median(p99)) if tps else None
print(f"\n  {'N':>4} | {'fathom agg tps':>14} {'sqld agg tps':>12} | "
      f"{'fathom p50':>11} {'sqld p50':>9} | {'fathom p99':>11} {'sqld p99':>9}   (µs, per-tenant median)")
print(f"  {'-'*4} | {'-'*14} {'-'*12} | {'-'*11} {'-'*9} | {'-'*11} {'-'*9}")
for n in nlist:
    def row(a): return (f"{a[0]:.0f}", f"{a[1]:.0f}", f"{a[2]:.0f}") if a else ("n/a", "n/a", "n/a")
    ft, fp, fq = row(agg(f"f{n}")); st, sp, sq = row(agg(f"s{n}"))
    print(f"  {n:>4} | {ft:>14} {st:>12} | {fp:>11} {sp:>9} | {fq:>11} {sq:>9}")
print("\n  aggregate TPC-B tps summed across N independent tenant DBs (one process/writer each);")
print("  p50/p99 are the median per-tenant latency. Relative, single host, steady-state.")
PY
rm -rf "$OUT"
