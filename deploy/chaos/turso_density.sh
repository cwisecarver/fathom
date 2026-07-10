#!/usr/bin/env bash
# Density axis — how many tenant DBs can one server hold, and at what per-tenant cost.
#
# This measures the SQLD side: create namespaces in a sweep and watch the process RSS, disk, and
# creation rate. Compare to fathom's shard density from `mix fathom.scale --ramp` (open working-set
# ceiling) and the architecture (a shard is just a file, cold-opened on demand, so a NODE holds a
# working set while STORAGE holds millions — memory tracks the working set, not total tenants).
#
# The structural difference this surfaces: sqld keeps per-namespace state resident in ONE process
# (RSS grows with TOTAL namespaces) and its creation rate degrades as the count grows; fathom's
# per-node memory tracks only the open working set, shard creation is O(1) (first write cold-opens
# a file), and tenants distribute across the LB-partitioned fleet.
#
#   ./turso_density.sh [ "N1 N2 N3 N4" ]      # cumulative namespace counts; default 500 1000 2000 4000
#
# NOTE: single host, one container, relative — a feel, not a cert. This does not run to millions
# (infeasible); it measures the per-namespace cost + the creation-rate slope to characterize the
# ceiling.
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
NLIST=${1:-"500 1000 2000 4000"}
now() { /usr/bin/python3 -c 'import time;print(time.time())'; }

echo "starting a fresh sqld with namespaces (admin :$SQLD_ADMIN) ..."
"$DOCKER" rm -f libsql-ns >/dev/null 2>&1
"$DOCKER" run -d --name libsql-ns -p "$SQLD_PORT:8080" -p "$SQLD_ADMIN:8081" "$SQLD_IMG" \
  /bin/sqld --enable-namespaces --admin-listen-addr 0.0.0.0:8081 >/dev/null
for _ in $(seq 1 25); do curl -sS --max-time 3 "http://localhost:$SQLD_ADMIN/v1/namespaces/x/config" >/dev/null 2>&1 && break; sleep 1; done
echo "  baseline RSS (0 namespaces): $("$DOCKER" stats --no-stream --format '{{.MemUsage}}' libsql-ns)"

echo ""
printf "  %-10s %-16s %-14s %s\n" "namespaces" "RSS" "disk" "create rate (this batch)"
printf "  %-10s %-16s %-14s %s\n" "----------" "----------------" "--------------" "------------------------"
created=0
for target in $NLIST; do
  batch=$((target - created)); t0=$(now)
  while [ "$created" -lt "$target" ]; do
    curl -sS --max-time 8 -X POST "http://localhost:$SQLD_ADMIN/v1/namespaces/dens_${created}/create" \
      -H "Content-Type: application/json" -d '{}' >/dev/null 2>&1
    created=$((created + 1))
  done
  t1=$(now)
  rate=$(/usr/bin/python3 -c "print(f'{$batch/($t1-$t0):.0f}')" 2>/dev/null)
  rss=$("$DOCKER" stats --no-stream --format '{{.MemUsage}}' libsql-ns | awk '{print $1}')
  disk=$("$DOCKER" exec libsql-ns sh -c 'du -sh iku.db 2>/dev/null | cut -f1' 2>/dev/null)
  printf "  %-10s %-16s %-14s ~%s/s\n" "$target" "$rss" "${disk:-?}" "$rate"
done
echo ""
echo "  RSS grows with TOTAL namespaces (per-namespace state resident in one process); the create"
echo "  rate degrades as the count grows. Compare: mix fathom.scale --ramp (fathom open-shard density)."
