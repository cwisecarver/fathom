#!/usr/bin/env bash
# Fathom chaos-rig driver — the "failover time + loss window" layer of
# docs/deploy-cluster.md "Chaos testing" (the in-process suite pins safety;
# this rig measures latency and demonstrates the failure modes end to end).
#
#   ./chaos.sh build              build the fathom release image
#   ./chaos.sh up | down | logs [svc]
#   ./chaos.sh smoke              write+read a few tenants through the LB; check isolation
#   ./chaos.sh owner <shard>      which node holds the shard file
#   ./chaos.sh latency <ms>|clear inject S3 latency on every node (toxiproxy)
#   ./chaos.sh failover <shard>   silent-kill the owner; time LB reroute + survivor steal
#   ./chaos.sh pause-fence <shard>  freeze the owner past TTL; steal on a survivor;
#                                   unpause the zombie and prove it self-fences
#   ./chaos.sh partition <node> [secs]  cut one node off S3; observe lapse + recovery
#   ./chaos.sh soak <secs>        sustained load + node churn; end-to-end integrity check
#   ./chaos.sh warm-home <shard>  a shard's home node must NOT warm its own shard
#                                 (survivors do) — while served AND after idle-drop
#
# All timings are RELATIVE (one host, loopback MinIO): run `latency 30` first so
# failover numbers reflect a real S3 RTT rather than loopback.

set -u -o pipefail
cd "$(dirname "$0")"

LB=${LB:-http://localhost:8080}
TOXI=${TOXI:-http://localhost:8474}
DOMAIN=fathom.test
TTL_MS=10000
# A steal is allowed once the owner's heartbeat age exceeds TTL + steal_margin
# (the clock-skew guard, Fathom.Shard.Storage.steal_margin_ms, default 5000 and
# not overridden in the rig). Scenarios that force a steal must wait past both.
STEAL_MARGIN_MS=5000
NODES=(fathom1 fathom2 fathom3)
# direct (LB-bypassing) Hrana port per node, for forced-steal experiments.
# A function (not an associative array) so the script runs on macOS's bash 3.2.
direct_port() {
  case $1 in
    fathom1) echo 18081 ;;
    fathom2) echo 18082 ;;
    fathom3) echo 18083 ;;
    *) echo "unknown node: $1" >&2; return 1 ;;
  esac
}

# -- docker CLI resolution (colima setups often have the CLI unlinked) ---------
if command -v docker >/dev/null 2>&1; then
  DOCKER=docker
else
  DOCKER=$(ls /opt/homebrew/Cellar/docker/*/bin/docker 2>/dev/null | head -1)
  [ -n "$DOCKER" ] || { echo "docker CLI not found" >&2; exit 1; }
  export DOCKER_HOST=${DOCKER_HOST:-unix://$HOME/.colima/default/docker.sock}
fi
compose() { "$DOCKER" compose "$@"; }
cname() { echo "fathom-chaos-$1-1"; }
netname() { echo "fathom-chaos_default"; }

now_ms() { /usr/bin/python3 -c 'import time; print(int(time.time()*1000))'; }

# -- Hrana v2 pipeline over HTTP ------------------------------------------------
# hrana <base-url> <shard> <sql>  → full pipeline response JSON on stdout.
# rc 0 ONLY on a genuine success: HTTP 2xx AND a well-formed pipeline whose
# execute result is type "ok". A refused stream open (lease held / node down)
# comes back as a non-2xx or an error-shaped body with NO .results — those must
# read as failure, or a failover experiment would time a false success (the
# body without .results was silently passing the old per-statement-only check).
hrana() {
  local base=$1 shard=$2 sql=$3 raw code out
  raw=$(curl -sS --max-time 8 -w $'\n%{http_code}' -X POST "$base/v2/pipeline" \
    -H "Host: $shard.$DOMAIN" -H "Content-Type: application/json" \
    -d "{\"requests\":[{\"type\":\"execute\",\"stmt\":{\"sql\":$(jq -Rn --arg s "$sql" '$s')}},{\"type\":\"close\"}]}") || return 1
  code=${raw##*$'\n'}   # trailing status line
  out=${raw%$'\n'*}     # body
  echo "$out"
  [ "$code" = "200" ] || return 1
  # The execute (first request) must have produced an ok result with a response.
  [ "$(echo "$out" | jq -r 'try (.results[0].type) catch "none"')" = "ok" ] || return 1
  [ "$(echo "$out" | jq -r '[.results[]? | select(.type=="error")] | length')" = "0" ]
}

sql() { hrana "$LB" "$@"; }                      # through the LB
sql_direct() { local node=$1; shift; hrana "http://localhost:$(direct_port "$node")" "$@"; }
val() { jq -r '.results[0].response.result.rows[0][0].value'; } # first row/col

seed() { # seed <shard>: kv table exists
  sql "$1" "CREATE TABLE IF NOT EXISTS kv (id INTEGER PRIMARY KEY AUTOINCREMENT, tenant TEXT, seq INTEGER)" >/dev/null
}

# -- commands -------------------------------------------------------------------
cmd_build() { compose build migrate; }
cmd_up()    { compose up -d --wait --wait-timeout 180; compose ps; }
cmd_down()  { compose down -v --remove-orphans; }
cmd_logs()  { compose logs -f "${1:-}"; }

cmd_owner() {
  local shard=$1 n
  for n in "${NODES[@]}"; do
    if compose exec -T "$n" test -f "/data/shards/$shard.db" 2>/dev/null; then echo "$n"; return 0; fi
  done
  return 1
}

cmd_latency() {
  local p
  if [ "${1:-}" = "clear" ]; then
    for p in s3-fathom1 s3-fathom2 s3-fathom3; do
      curl -sS -X DELETE "$TOXI/proxies/$p/toxics/lat_down" >/dev/null 2>&1
      curl -sS -X DELETE "$TOXI/proxies/$p/toxics/lat_up" >/dev/null 2>&1
    done
    echo "latency cleared"
  else
    local ms=${1:?usage: latency <ms>|clear}
    for p in s3-fathom1 s3-fathom2 s3-fathom3; do
      curl -sS -X POST "$TOXI/proxies/$p/toxics" -d \
        "{\"name\":\"lat_down\",\"type\":\"latency\",\"stream\":\"downstream\",\"attributes\":{\"latency\":$ms}}" >/dev/null
      curl -sS -X POST "$TOXI/proxies/$p/toxics" -d \
        "{\"name\":\"lat_up\",\"type\":\"latency\",\"stream\":\"upstream\",\"attributes\":{\"latency\":$ms}}" >/dev/null
    done
    echo "S3 latency: ${ms}ms each way on all nodes"
  fi
}

cmd_smoke() {
  local t rc=0
  for t in alpha bravo charlie delta echo1; do
    seed "$t" || { echo "SMOKE FAIL: seed $t"; rc=1; continue; }
    sql "$t" "INSERT INTO kv (tenant, seq) VALUES ('$t', 1)" >/dev/null || { echo "SMOKE FAIL: write $t"; rc=1; continue; }
    local cnt foreign
    cnt=$(sql "$t" "SELECT count(*) FROM kv" | val)
    foreign=$(sql "$t" "SELECT count(*) FROM kv WHERE tenant <> '$t'" | val)
    printf "  %-8s rows=%-4s foreign=%-2s owner=%s\n" "$t" "$cnt" "$foreign" "$(cmd_owner "$t" || echo '?')"
    [ "$foreign" = "0" ] || { echo "SMOKE FAIL: ISOLATION LEAK in $t"; rc=1; }
  done
  [ $rc -eq 0 ] && echo "smoke: OK" || echo "smoke: FAILED"
  return $rc
}

silent_kill() { # network-disconnect first so peers see a hang, not a clean RST
  local node=$1
  "$DOCKER" network disconnect "$(netname)" "$(cname "$node")" 2>/dev/null
  "$DOCKER" kill "$(cname "$node")" >/dev/null
}

revive() {
  local node=$1
  "$DOCKER" network connect "$(netname)" "$(cname "$node")" 2>/dev/null
  compose up -d "$node" >/dev/null 2>&1
}

cmd_failover() {
  local shard=${1:?usage: failover <shard>}
  seed "$shard"
  sql "$shard" "INSERT INTO kv (tenant, seq) VALUES ('$shard', 100)" >/dev/null
  echo "waiting one flush interval so the write is durable in S3..."
  sleep 6
  local owner; owner=$(cmd_owner "$shard") || { echo "no owner found"; return 1; }
  local pre; pre=$(sql "$shard" "SELECT count(*) FROM kv" | val)
  echo "shard=$shard owner=$owner rows(pre)=$pre — silent-killing $owner"

  local t0 t1; t0=$(now_ms)
  silent_kill "$owner"
  local tries=0
  until sql "$shard" "INSERT INTO kv (tenant, seq) VALUES ('$shard', 200)" >/dev/null 2>&1; do
    tries=$((tries + 1))
    [ $tries -gt 600 ] && { echo "FAIL: no recovery after 600 attempts"; revive "$owner"; return 1; }
    sleep 0.1
  done
  t1=$(now_ms)

  local post newowner
  post=$(sql "$shard" "SELECT count(*) FROM kv" | val)
  newowner=$(cmd_owner "$shard" || echo '?')
  echo "RESULT failover: $((t1 - t0)) ms to first acked write on a survivor"
  echo "  new owner=$newowner rows pre=$pre post=$post (seq=100 flushed pre-kill must survive)"
  case $post in
    ''|*[!0-9]*) echo "  *** post-read did not return a count ($post) — investigate" ;;
    *) [ "$post" -ge "$pre" ] || echo "  *** DATA LOSS BEYOND RPO — investigate" ;;
  esac
  grep_promote "$newowner"
  revive "$owner"
}

grep_promote() { # did the survivor open warm (304-promote) or cold?
  compose logs --since 2m "$1" 2>/dev/null |
    grep -Ei "warm|promote" | tail -3 | sed 's/^/  log: /'
}

cmd_pause_fence() {
  local shard=${1:?usage: pause-fence <shard>}
  seed "$shard"
  # The schema must be DURABLE in S3 before the freeze, or the survivor steals a
  # working lease but pulls an empty shard (the flush interval hasn't elapsed) and
  # its write fails on a missing table — a false "couldn't steal".
  echo "waiting one flush interval so $shard's schema is durable in S3..."
  sleep 6
  local owner; owner=$(cmd_owner "$shard") || { echo "no owner"; return 1; }
  # pick a survivor for the forced steal
  local survivor; for survivor in "${NODES[@]}"; do [ "$survivor" != "$owner" ] && break; done

  # a dirty, unflushed write right before the freeze — the zombie's flush payload.
  # seq=300 is NOT in S3 (frozen before the next flush); losing it is within RPO.
  # The invariant is that the zombie must not RESURRECT it over the survivor's write.
  sql "$shard" "INSERT INTO kv (tenant, seq) VALUES ('$shard', 300)" >/dev/null
  echo "owner=$owner survivor=$survivor — pausing owner with a dirty (unflushed) write"
  "$DOCKER" pause "$(cname "$owner")" >/dev/null
  local wait_s=$(( (TTL_MS + STEAL_MARGIN_MS) / 1000 + 6 ))  # + cushion past the steal threshold
  echo "frozen; waiting ${wait_s}s (past TTL ${TTL_MS}ms + steal margin ${STEAL_MARGIN_MS}ms)..."
  sleep "$wait_s"

  echo "forcing the steal: writing to $shard directly on $survivor (LB passive health can't see a frozen node — that hang is the documented OSS-nginx limitation)"
  if sql_direct "$survivor" "$shard" "INSERT INTO kv (tenant, seq) VALUES ('$shard', 400)" >/dev/null; then
    echo "  steal OK: $survivor serves $shard"
  else
    echo "  FAIL: survivor could not steal"; "$DOCKER" unpause "$(cname "$owner")"; return 1
  fi

  echo "unpausing the zombie; its next flush must self-fence, not overwrite"
  "$DOCKER" unpause "$(cname "$owner")" >/dev/null
  sleep 8   # > flush interval: let the zombie attempt its fenced flush
  echo "zombie ($owner) fence log (self-fence = heartbeat lapse + flush refused):"
  compose logs --since 2m "$owner" 2>/dev/null |
    grep -Ei "lapse|unconfirmed|ownership|flush skipped|fenc|supersed|quarantin|stale|epoch" |
    tail -6 | sed 's/^/  /'
  local live; live=$(sql_direct "$survivor" "$shard" "SELECT count(*) FROM kv WHERE seq = 400" | val)
  local zombie; zombie=$(sql_direct "$survivor" "$shard" "SELECT count(*) FROM kv WHERE seq = 300" | val)
  echo "RESULT pause-fence:"
  echo "  survivor's post-steal write (seq=400) present = $live   (MUST be 1)"
  echo "  zombie's dirty write (seq=300) resurrected     = $zombie   (MUST be 0 — self-fence held)"
  [ "$live" = "1" ] && [ "$zombie" = "0" ] && echo "  PASS: single-writer held across the zombie's flush" \
    || echo "  *** SPLIT-BRAIN — the zombie overwrote the survivor"
}

cmd_partition() {
  local node=${1:?usage: partition <node> [secs]} secs=${2:-25}
  local proxy="s3-${node}"
  echo "cutting $node off S3 for ${secs}s (toxiproxy $proxy disabled)"
  curl -sS -X POST "$TOXI/proxies/$proxy" -d '{"enabled": false}' >/dev/null
  sleep "$secs"
  echo "$node lapse/fence log during the partition:"
  compose logs --since "${secs}s" "$node" 2>/dev/null |
    grep -Ei "heartbeat|lapse|unconfirmed|ownership|flush skipped|fenc|error" | tail -10 | sed 's/^/  /'
  curl -sS -X POST "$TOXI/proxies/$proxy" -d '{"enabled": true}' >/dev/null
  echo "restored; node should resume renewing (watch: ./chaos.sh logs $node)"
}

cmd_soak() {
  local secs=${1:-120} tenants=(t1 t2 t3 t4 t5 t6) t
  local ack_dir; ack_dir=$(mktemp -d)
  echo "soak: ${secs}s of writes across ${#tenants[@]} tenants with node churn (acks in $ack_dir)"
  for t in "${tenants[@]}"; do seed "$t"; : > "$ack_dir/$t"; done

  local stop_at=$(( $(now_ms) / 1000 + secs )) seq=0 churn_at=$(( $(now_ms) / 1000 + 20 ))
  while [ "$(( $(now_ms) / 1000 ))" -lt "$stop_at" ]; do
    seq=$((seq + 1))
    t=${tenants[$(( seq % ${#tenants[@]} ))]}
    if sql "$t" "INSERT INTO kv (tenant, seq) VALUES ('$t', $seq)" >/dev/null 2>&1; then
      echo "$seq" >> "$ack_dir/$t"
    fi
    if [ "$(( $(now_ms) / 1000 ))" -ge "$churn_at" ]; then
      local victim=${NODES[$((RANDOM % ${#NODES[@]}))]}
      echo "  churn: killing $victim (t=$seq)"
      "$DOCKER" kill "$(cname "$victim")" >/dev/null 2>&1
      ( sleep 8; compose up -d "$victim" >/dev/null 2>&1 ) &
      churn_at=$(( $(now_ms) / 1000 + 25 ))
    fi
  done
  wait
  echo "soak load done; letting flushes settle..."
  sleep 10

  local total_acked=0 total_stored=0 leaks=0
  for t in "${tenants[@]}"; do
    local acked stored foreign
    acked=$(wc -l < "$ack_dir/$t" | tr -d ' ')
    stored=$(sql "$t" "SELECT count(*) FROM kv WHERE seq > 0" | val)
    foreign=$(sql "$t" "SELECT count(*) FROM kv WHERE tenant <> '$t'" | val)
    total_acked=$((total_acked + acked)); total_stored=$((total_stored + stored))
    [ "$foreign" != "0" ] && { leaks=$((leaks + 1)); echo "  *** ISOLATION LEAK in $t: $foreign foreign rows"; }
    printf "  %-4s acked=%-6s stored=%-6s foreign=%s\n" "$t" "$acked" "$stored" "$foreign"
  done
  echo "RESULT soak: acked=$total_acked stored=$total_stored lost=$((total_acked - total_stored)) (RPO: committed-but-unflushed at each kill), leaks=$leaks (MUST be 0)"
  [ $leaks -eq 0 ]
}

# A shard's HOME node (the LB-hash target that serves it) must NOT warm its own
# shard — only the survivors warm it, so a failover away from the home lands warm
# while the home never wastes cache budget on a shard that routes back to it. The
# regression is a home node re-warming a shard it just idle-dropped (lease released,
# no live "home" signal); the fix remembers recently-owned shards for
# :warm_home_retention_ms. This checks BOTH while-served and after-idle-drop.
check_warm_placement() {
  local shard=$1 home=$2 rc=0 n present
  for n in "${NODES[@]}"; do
    if compose exec -T "$n" test -f "/data/warm/$shard.db" 2>/dev/null; then present=yes; else present=no; fi
    if [ "$n" = "$home" ]; then
      printf "  %-8s warm=%-3s (home — MUST be no)\n" "$n" "$present"
      [ "$present" = "no" ] || rc=1
    else
      printf "  %-8s warm=%-3s (survivor — should be yes)\n" "$n" "$present"
    fi
  done
  [ $rc -eq 0 ] && echo "  PASS: home node does not warm its own shard" \
    || echo "  *** FAIL: home node warmed its own shard"
  return $rc
}

cmd_warm_home() {
  local shard=${1:?usage: warm-home <shard>}
  seed "$shard"
  local home; home=$(cmd_owner "$shard") || { echo "no home found"; return 1; }
  echo "home=$home — waiting ~8s for warm propagation (shard still served, < idle)"
  sleep 8
  echo "--- while served (live coordinator on home) ---"
  check_warm_placement "$shard" "$home" || true
  # Past SHARD_IDLE_MS (rig 20s) so home flushes+drops+releases, but within the
  # home-retention window (default 60s) — the regression window.
  echo "waiting past idle-drop, staying within the home-retention window..."
  sleep 18
  echo "--- after idle-drop, within home-retention (the fix's window) ---"
  check_warm_placement "$shard" "$home"
}

case "${1:-}" in
  build)       cmd_build ;;
  up)          cmd_up ;;
  down)        cmd_down ;;
  logs)        shift; cmd_logs "$@" ;;
  smoke)       cmd_smoke ;;
  owner)       shift; cmd_owner "$@" ;;
  latency)     shift; cmd_latency "$@" ;;
  failover)    shift; cmd_failover "$@" ;;
  pause-fence) shift; cmd_pause_fence "$@" ;;
  partition)   shift; cmd_partition "$@" ;;
  soak)        shift; cmd_soak "$@" ;;
  warm-home)   shift; cmd_warm_home "$@" ;;
  *) sed -n '2,20p' "$0"; exit 64 ;;
esac
