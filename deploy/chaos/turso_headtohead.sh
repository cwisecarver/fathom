#!/usr/bin/env bash
# Turso / libSQL head-to-head — fathom vs the reference libsql-server (sqld).
#
# Both serve SQLite over the SAME Hrana protocol, so tpc_driver.py drives both UNCHANGED (just a
# different URL). That isolates the one variable: the server implementation — fathom (Elixir /
# BEAM / Filo / Bandit, S3-backed, multi-tenant) vs sqld (Rust, local-disk, single-DB). Same
# engine, same protocol, same workload, same box → a fair per-DB comparison.
#
#   ./turso_headtohead.sh [rtt_samples tpcb_txns tpcc_txns]
#
# Prereqs: the fathom rig up (./chaos.sh up) and Docker. sqld is started here if not running.
# Fathom is hit on a node's DIRECT port (LB-bypass), so both are single-server Hrana over HTTP.
# NOTE: single host, single DB, single client, steady-state — relative numbers, a feel not a cert.
# Durability differs (fathom synchronous=FULL + S3 flush; sqld libSQL defaults, local disk).
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
SQLD_URL=http://localhost:$SQLD_PORT
FATHOM_URL=${FATHOM_URL:-http://localhost:18081}   # fathom1 direct (LB-bypass)
OUT=$(mktemp -d)

RTT_SAMPLES=${1:-200}
TPCB_TXNS=${2:-500}
TPCC_TXNS=${3:-300}

pipeline='{"requests":[{"type":"execute","stmt":{"sql":"SELECT 1"}},{"type":"close"}]}'
reachable() { curl -sS --max-time 5 "$1/v2/pipeline" -X POST -H "Host: x.fathom.test" \
  -H "Content-Type: application/json" -d "$pipeline" >/dev/null 2>&1; }

ensure_sqld() {
  # Always start a FRESH sqld (clean single DB). sqld is single-database, so state accumulated
  # across runs (or a wedged connection) would otherwise skew or hang a later pass; a fresh
  # container is deterministic. fathom stays robust on a fixed shard (idempotent seed +
  # busy_timeout + coordinator), so only sqld needs the reset.
  echo "starting a fresh sqld ($SQLD_IMG) on :$SQLD_PORT ..."
  "$DOCKER" rm -f libsql-h2h >/dev/null 2>&1
  "$DOCKER" run -d --name libsql-h2h -p "$SQLD_PORT:8080" "$SQLD_IMG" >/dev/null
  for _ in $(seq 1 20); do reachable "$SQLD_URL" && break; sleep 1; done
  reachable "$SQLD_URL" || { echo "sqld not reachable at $SQLD_URL" >&2; exit 1; }
}

drive() {  # drive <label> <url> <mode> <extra args...>
  local label=$1 url=$2 mode=$3; shift 3
  python3 tpc_driver.py "$mode" --lb "$url" --domain fathom.test "$@" 2>/dev/null \
    > "$OUT/${label}_${mode}.json"
}

ensure_sqld
reachable "$FATHOM_URL" || { echo "fathom not reachable at $FATHOM_URL — run ./chaos.sh up first" >&2; exit 1; }

echo "head-to-head: fathom ($FATHOM_URL) vs libsql-server ($SQLD_URL), same box, same Hrana wire"
for pair in "fathom $FATHOM_URL" "sqld $SQLD_URL"; do
  set -- $pair; label=$1; url=$2
  drive "$label" "$url" rtt  --shard h2h --samples "$RTT_SAMPLES"
  drive "$label" "$url" tpcb --shard h2h --txns "$TPCB_TXNS" --clients 1 --accounts 10000
  drive "$label" "$url" tpcc --max-w 1 --threads 1 --txns "$TPCC_TXNS" --scale 0.02
done

python3 - "$OUT" <<'PY'
import json, sys, os
d = sys.argv[1]
def load(label, mode):
    p = os.path.join(d, f"{label}_{mode}.json")
    try:
        return json.load(open(p))
    except Exception:
        return {}
def g(o, k):
    v = o.get(k); return f"{v:.0f}" if isinstance(v, (int, float)) else "n/a"
f_rtt, s_rtt = load("fathom","rtt"), load("sqld","rtt")
f_b, s_b     = load("fathom","tpcb"), load("sqld","tpcb")
f_c = (load("fathom","tpcc").get("results") or [{}])[0]
s_c = (load("sqld","tpcc").get("results") or [{}])[0]
rows = [
  ("RTT p50 (µs)",            g(f_rtt,"rtt_p50_us"),        g(s_rtt,"rtt_p50_us")),
  ("RTT p99 (µs)",            g(f_rtt,"rtt_p99_us"),        g(s_rtt,"rtt_p99_us")),
  ("TPC-B p50 (µs)",          g(f_b,"tpcb_p50_us"),         g(s_b,"tpcb_p50_us")),
  ("TPC-B p99 (µs)",          g(f_b,"tpcb_p99_us"),         g(s_b,"tpcb_p99_us")),
  ("TPC-B tps (1 client)",    g(f_b,"tpcb_tps"),            g(s_b,"tpcb_tps")),
  ("TPC-C tpmC (1 thread)",   g(f_c,"tpcc_tpmc"),           g(s_c,"tpcc_tpmc")),
  ("TPC-C New-Order p50 (µs)",g(f_c,"tpcc_neworder_p50_us"),g(s_c,"tpcc_neworder_p50_us")),
  ("TPC-C Payment p50 (µs)",  g(f_c,"tpcc_payment_p50_us"), g(s_c,"tpcc_payment_p50_us")),
]
w = max(len(r[0]) for r in rows)
print(f"\n  {'metric':<{w}}   {'fathom':>10}   {'libsql-server':>13}")
print(f"  {'-'*w}   {'-'*10}   {'-'*13}")
for name, fv, sv in rows:
    print(f"  {name:<{w}}   {fv:>10}   {sv:>13}")
print("\n  (relative, single host / single DB / single client, steady-state — a feel, not a cert)")
PY
rm -rf "$OUT"
