#!/usr/bin/env bash
# Run the fathom Jepsen test against the deploy/chaos rig.
#
#   ./run.sh --time-limit 300                  # 5 min, through the LB, all faults
#   ./run.sh --time-limit 60 --faults none     # smoke: no faults, should be valid
#   ./run.sh --target direct --faults pause    # zombie owner, clients hit every node
#   ./run.sh serve                             # browse results on http://localhost:8080
#
# The rig must already be up WITH REPLICATION ON (prod's default; without it a
# killed owner loses its unflushed acked writes by design and Elle reports them):
#
#   cd ../../deploy/chaos && ./chaos.sh build && REPLICATION_ENABLED=true ./chaos.sh up
#
# Results land in store/<test-name>/<timestamp>/ (store/latest is a symlink).
set -u -o pipefail
cd "$(dirname "$0")"

export JAVA_HOME=${JAVA_HOME:-/opt/homebrew/opt/openjdk}

if command -v docker >/dev/null 2>&1; then
  DOCKER=$(command -v docker)
else
  DOCKER=$(ls /opt/homebrew/Cellar/docker/*/bin/docker 2>/dev/null | head -1)
  [ -n "$DOCKER" ] || { echo "docker CLI not found" >&2; exit 1; }
  export DOCKER_HOST=${DOCKER_HOST:-unix://$HOME/.colima/default/docker.sock}
fi
export DOCKER

if [ "${1:-}" = "serve" ]; then
  shift
  exec lein run serve "$@"
fi

# One heavy run per host (AGENTS.md § Benchmarking).
for lock in /tmp/fathom_bench.lock /tmp/tidepool_bench.lock; do
  [ -e "$lock" ] && { echo "refusing: $lock is held (another heavy run is going)" >&2; exit 1; }
done

running=$("$DOCKER" ps --filter name=fathom-chaos-fathom --format '{{.Names}}' | wc -l | tr -d ' ')
[ "$running" = "5" ] || {
  echo "the rig is not up ($running of 5 fathom nodes running):" >&2
  echo "  cd ../../deploy/chaos && REPLICATION_ENABLED=true ./chaos.sh up" >&2
  exit 1
}

# A rig whose `up` half-failed still has 5 "running" nodes when one is a leftover from the last
# run, and every op then fails with connection refused, which reads as a fathom liveness failure
# (2026-10-28, the S3 run). Each node must be healthy and the LB must answer as nginx.
for _ in $(seq 1 30); do
  unhealthy=$("$DOCKER" ps --filter name=fathom-chaos-fathom --format '{{.Names}} {{.Status}}' | grep -v '(healthy)')
  [ -z "$unhealthy" ] && break
  sleep 2
done
[ -z "$unhealthy" ] || { echo "refusing: nodes not healthy:" >&2; echo "$unhealthy" >&2; exit 1; }
lb_server=$(curl -s -o /dev/null -D - "http://localhost:${LB_PORT:-8080}/" 2>/dev/null | tr -d '\r' | awk -F': ' 'tolower($1)=="server"{print $2}')
case "$lb_server" in
  nginx*) ;;
  *) echo "refusing: the LB on port ${LB_PORT:-8080} is not answering as nginx (Server: '${lb_server}')" >&2; exit 1 ;;
esac

# Ask the node, not this shell: the setting is baked in at container boot.
repl=$("$DOCKER" exec fathom-chaos-fathom1-1 /app/bin/fathom rpc \
  'IO.puts(Fathom.Shard.Replication.Fleet.replicating?())' 2>/dev/null | tr -d '\r\n ')
if [ "$repl" != "true" ]; then
  echo "WARNING: replication is OFF on fathom1 (got '$repl'). A killed owner will lose acked"
  echo "         writes BY DESIGN, and Elle will report them as lost. To test prod's config:"
  echo "         cd ../../deploy/chaos && REPLICATION_ENABLED=true ./chaos.sh up"
fi

# `chaos.sh up` does not build: a pass against a weeks-old image validates nothing.
img=$("$DOCKER" image inspect fathom-chaos:latest -f '{{.Created}}' 2>/dev/null)
head=$(git log -1 --format=%cI)
echo "image built $img, HEAD committed $head"
if [[ "$img" < "$head" ]]; then
  echo "WARNING: the image is older than HEAD. Rebuild: cd ../../deploy/chaos && ./chaos.sh build"
fi

# A colima clock step invalidates a run (AGENTS.md Loud warnings); check after.
start=$(date -u +%Y-%m-%dT%H:%M:%SZ)
lein run test "$@"
rc=$?

# Keep every node's log with the run. `chaos.sh down` deletes the containers and their logs with
# them, which is how the evidence for one anomaly was lost on 2026-10-24.
store=$(ls -td store/*/20* 2>/dev/null | head -1)
if [ -n "$store" ]; then
  mkdir -p "$store/node-logs"
  for c in fathom1 fathom2 fathom3 fathom4 fathom5 postgres minio; do
    "$DOCKER" logs -t "fathom-chaos-$c-1" >"$store/node-logs/$c.log" 2>&1
  done
  echo "node logs saved to $store/node-logs"
fi
steps=0
for n in 1 2 3 4 5; do
  c=$("$DOCKER" logs --since "$start" "fathom-chaos-fathom$n-1" 2>&1 | grep -c 'clock lags the store')
  steps=$((steps + c))
done
if [ "$steps" -gt 0 ] && [[ " $* " == *clock-* || " $* " == *"faults all"* ]]; then
  echo "($steps 'clock lags the store' lines: expected, this run jumps the clock on purpose)"
elif [ "$steps" -gt 0 ]; then
  echo "*** RUN INVALID: $steps 'clock lags the store' lines in node logs — colima clock stepped."
  exit 2
fi
exit $rc
