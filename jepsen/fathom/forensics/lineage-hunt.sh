#!/bin/zsh
# Lineage-collision hunt harness (Jepsen 2026-11-14 hard-1). See docs/runbooks/lock-lineage-forensics.md.
# Usage: [SOAK=1] [HARD_RUNS=12] zsh jepsen/fathom/forensics/lineage-hunt.sh   (outputs land in logs/ds/)
# Hard-1 hunt harness: lock-trace + release_decision scan, MinIO bucket versioning (every lock body kept), mc admin trace (requester IP), lock-version dump on any flag/invalid run. HARD_RUNS (default 6), SOAK=1 skips nofault/rpo. Was: Tier 0 push gate for the 2026-11-13 audit batch (#23 #25 #28 #30 #31 #32 #33 #35 #40, HEAD at run time) - was 87136f97 (docs/jepsen-run-plan.md): 1 x 2 min fault-free, then
# 3 x 10 min hard mix. rpo runs last.
set -u
R=$(cd "$(dirname "$0")/../../.." && pwd)
mkdir -p $R/logs/ds
D=$(ls -d /opt/homebrew/Cellar/docker/*/bin | tail -1); export PATH="$D:$PATH"
export COMPOSE_FILE=docker-compose.yml:docker-compose.lazyfs.yml REPLICATION_ENABLED=true REPL_VIA_TOXI=1
unset SHARD_FLUSH_INTERVAL_MS
S=$R/logs/ds/lineage-hunt-summary.txt
: >| $S
(cd $R/deploy/chaos && ./chaos.sh down >/dev/null 2>&1 && ./chaos.sh build >| $R/logs/ds/lineage-hunt-build.log 2>&1) || { echo BUILD_FAILED >> $S; echo DONE >> $S; exit 1; }
echo "image $(docker image inspect fathom-chaos:latest -f '{{.Created}}') head $(git -C $R log -1 --format=%h)" >> $S

n=0
run() {
  local name=$1 secs=$2; shift 2
  n=$((n + 1))
  cd $R/deploy/chaos
  ./chaos.sh down >/dev/null 2>&1
  if [ $((n % 3)) = 1 ]; then colima restart >/dev/null 2>&1; sleep 20; fi
  ./chaos.sh up >| $R/logs/ds/lineage-hunt-up-$name.log 2>&1 || { echo "$name UP_FAILED" >> $S; return; }
  sleep 15
  docker exec -e MC_CONFIG_DIR=/tmp/.mc fathom-chaos-minio-1 bash -c 'mc alias set local http://localhost:9000 fathom fathom-secret >/dev/null; mc version enable local/fathom-shards' >> $R/logs/ds/lineage-hunt-version-$name.log 2>&1
  TR=$R/logs/ds/lock-trace-$name.log
  ( docker exec -e MC_CONFIG_DIR=/tmp/.mc fathom-chaos-minio-1 bash -c 'mc alias set local http://localhost:9000 fathom fathom-secret >/dev/null; mc admin trace local --call s3 --path "*.lock"' >| $TR 2>&1 & echo $! >| /tmp/tracepid_h )
  cd $R/jepsen/fathom
  caffeinate -s ./run.sh --time-limit $secs --target mixed --shards 16 "$@" >| $R/logs/ds/lineage-hunt-$name.log 2>&1
  local rc=$?
  kill $(cat /tmp/tracepid_h) 2>/dev/null
  local st=$(ls -td store/*/20* | head -1)
  local live=$(grep -A6 ':liveness' $st/results.edn 2>/dev/null | grep -oE ':valid\? [a-z]+|:never-recovered \[[^]]*\]|:max-recovery-s [0-9.]+' | tr '\n' ' ')
  echo "$name exit=$rc store=$st append=$(grep -m1 -oE '^\{:append \{:valid\? [a-z]+' $st/results.edn | awk '{print $NF}') liveness=$live docker-warn=$(grep -c 'WARN.*docker' $st/jepsen.log) clocklag=$(grep -l 'clock lags the store' $st/node-logs/* 2>/dev/null | wc -l | tr -d ' ') refused=$(cat $st/node-logs/*.log | grep -c "not promoting") scan=$(python3 $R/jepsen/fathom/forensics/lineage_scan.py $st | head -1)" >> $S
  if tail -1 $S | grep -qE "append=false|flags=[1-9]|refused=[1-9]|unverified_deletes=[1-9]"; then $R/jepsen/fathom/forensics/dump_lock_versions.sh $R/logs/ds/lock-versions-$name.jsonl >> $S 2>&1; cp -r $st $R/logs/ds/flagged-$name-store 2>/dev/null; fi
}
[ -n "${SOAK:-}" ] || run nofault 120 --rate 1000 --faults none
for i in $(seq 1 ${HARD_RUNS:-6}); do
  run hard-$i 600 --rate 120 --quiet-period 120 --hard --max-concurrent-faults 3 --faults power-loss,kill,pause,isolate,repl-cut,s3-cut
done
cd $R/deploy/chaos; ./chaos.sh down >/dev/null 2>&1
if [ -z "${SOAK:-}" ]; then
unset COMPOSE_FILE REPL_VIA_TOXI
colima restart >/dev/null 2>&1; sleep 20
SHARD_FLUSH_INTERVAL_MS=5000 ./chaos.sh up >| $R/logs/ds/lineage-hunt-up-rpo.log 2>&1 || echo "rpo UP_FAILED" >> $S
sleep 15
caffeinate -s ./chaos.sh rpo >| $R/logs/ds/lineage-hunt-rpo.log 2>&1
echo "rpo exit=$? $(grep -E '^\s+(PASS|FAIL)' $R/logs/ds/lineage-hunt-rpo.log | head -1) clocklag=$(for n in 1 2 3 4 5; do docker logs fathom-chaos-fathom$n-1 2>&1 | grep -c 'clock lags the store'; done | paste -sd' ' -)" >> $S
./chaos.sh down >/dev/null 2>&1
fi
echo DONE >> $S
