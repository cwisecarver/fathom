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
#   ./chaos.sh hotspots [shards secs zipf workers]  drive Zipf-skewed REAL Hrana
#                                 traffic through the LB, then read Fathom.ShardLoad.top
#                                 per node — the non-synthetic Phase-2 §B hot-spot run
#   ./chaos.sh rebalance [shard secs]  the Phase-2 B1 handoff live: detect a hot shard,
#                                 pin it + reload the LB, drain the source, prove it moved
#   ./chaos.sh tpcb [shards txns accounts]  remote TPC-B: RTT probe + N tenant shards driven
#                                 through the LB by a real libSQL client (the Phase-4 realism run)
#   ./chaos.sh tpcc [max_w threads txns scale]  remote TPC-C: W=1..max_w sweep through the LB
#                                 by a real libSQL client — per-txn-type latency + tpmC
#   ./chaos.sh density [shards workers]  mint N novel shards through the LB; read per node
#                                 the coordinators held (the partition) + BEAM/RSS per shard
#                                 (the cost) — the fathom counterpart to turso_density.sh
#   ./chaos.sh served [per]       hold `per` shards under a LIVE connection on each node at once
#                                 (the fd-bound served ceiling, ~220 KiB/shard); needs container
#                                 nofile raised (compose sets soft 65536; default 1024 caps ~940)
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

# rpc <node> <elixir-expr>: evaluate on a running node via the release, return stdout.
rpc() { local node=$1; shift; compose exec -T "$node" /app/bin/fathom rpc "$*" 2>/dev/null | tr -d '\r'; }
# other_node <node>: any NODES element that isn't <node>.
other_node() { local not=$1 n; for n in "${NODES[@]}"; do [ "$n" != "$not" ] && { echo "$n"; return 0; }; done; }
# reload_lb: apply the rendered exception map (nginx re-reads the included file).
reload_lb() { compose exec -T lb nginx -s reload; }
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

# -- hotspots: real-traffic hot-spot detection (Phase-2 §B) --------------------
# Drive Zipf-skewed REAL Hrana traffic (through the LB, so the subdomain hashes to a
# node exactly as in prod), then read Fathom.ShardLoad.top per node via the release
# RPC. This is the non-synthetic confirmation of `mix fathom.scale --hotspots`: does
# the shipped read API recover the hot head under real wire traffic? Needs SHARD_LOAD
# on (set in docker-compose's fathom-env).
cmd_hotspots() {
  local shards=${1:-200} secs=${2:-60} zexp=${3:-1.1} workers=${4:-24}
  echo "hotspots: ${secs}s of Zipf(s=$zexp) REAL Hrana traffic over $shards shards via $workers workers (through the LB)"

  # Reset the per-node counters so this run's rates aren't polluted by earlier traffic.
  local n
  for n in "${NODES[@]}"; do
    compose exec -T "$n" /app/bin/fathom rpc 'Fathom.ShardLoad.reset()' >/dev/null 2>&1
  done

  # A pool of Zipf-drawn shard ids (hot_1 hottest). python3 is already a dep (now_ms).
  local targets; targets=$(mktemp)
  /usr/bin/python3 - "$shards" "$zexp" > "$targets" <<'PY'
import sys, random, bisect
n=int(sys.argv[1]); s=float(sys.argv[2])
w=[1.0/(k**s) for k in range(1,n+1)]; tot=sum(w)
cum=[]; c=0.0
for x in w: c+=x/tot; cum.append(c)
for _ in range(300000):
    print(f"hot_{bisect.bisect_left(cum, random.random())+1}")
PY

  # `workers` background loops, each firing sql on every workers-th target line until
  # the deadline. Each sql is a real open->execute(SELECT 1)->close Hrana stream.
  local deadline=$(( $(now_ms) + secs * 1000 ))
  local ackdir; ackdir=$(mktemp -d)
  local w
  for w in $(seq 1 "$workers"); do
    (
      awk -v off="$w" -v step="$workers" 'NR % step == (off % step)' "$targets" | while read -r shard; do
        [ "$(now_ms)" -lt "$deadline" ] || break
        sql "$shard" "SELECT 1" >/dev/null 2>&1 && echo 1 >> "$ackdir/$w"
      done
    ) &
  done
  wait

  local total=0
  for w in $(seq 1 "$workers"); do
    [ -f "$ackdir/$w" ] && total=$(( total + $(wc -l < "$ackdir/$w") ))
  done
  local elapsed=$(( secs ))
  echo "drove ~$total successful requests (~$(( total / (elapsed>0?elapsed:1) )) req/s through the LB)"

  # Read the top shards each node saw, merge, and score recovery of the Zipf head.
  local dump; dump=$(mktemp)
  for n in "${NODES[@]}"; do
    echo "== $n =="
    compose exec -T "$n" /app/bin/fathom rpc \
      'Fathom.ShardLoad.top(20, :queries) |> Enum.map(fn m -> {m.shard_id, m.queries} end) |> inspect(limit: :infinity) |> IO.puts' \
      | tee -a "$dump"
  done

  echo "--- merged hot set + Zipf-head recovery ---"
  /usr/bin/python3 - "$dump" <<'PY'
import sys, re
pairs=[]
for line in open(sys.argv[1]):
    for sid, q in re.findall(r'\{"(hot_\d+)",\s*(\d+)\}', line):
        pairs.append((sid, int(q)))
pairs.sort(key=lambda p: -p[1])
top=pairs[:15]
print("  rank  shard      queries")
for i,(sid,q) in enumerate(top,1):
    print(f"  {i:>4}  {sid:<9}  {q}")
# Recall of the true Zipf head: of the observed top-K, how many are hot_1..hot_K.
for K in (5,10,20):
    obs={s for s,_ in pairs[:K]}
    true={f"hot_{k}" for k in range(1,K+1)}
    hit=len(obs & true)
    print(f"  top-{K} Zipf-head recall: {hit}/{K} = {hit/K:.2f}")
PY
  rm -rf "$targets" "$ackdir" "$dump"
}

# -- rebalance: the Phase-2 B1 handoff, live end to end ------------------------
# Drives load on a shard, shows the reporter detected it, then executes the safe handoff
# to another node — pin + render the exception map, RELOAD the LB (flip), drain the source
# (release the lease), and prove the shard is now served by the target. The steps are
# host-orchestrated here because a fathom container can't reach the nginx container to
# reload; in a deployment where it can (`LB_RELOAD_CMD`), the HandoffJob runs the same
# sequence itself. Ordering is the safety: flip BEFORE drain, so the source doesn't
# re-acquire (the {owner,epoch} lease blocks a double-write regardless).
cmd_rebalance() {
  local shard=${1:-acme} secs=${2:-18}
  seed "$shard" >/dev/null
  echo "rebalance: driving ${secs}s of load on '$shard', then handing it off + reloading the LB"

  local deadline=$(( $(now_ms) + secs * 1000 )) w
  for w in 1 2 3 4 5 6; do
    ( while [ "$(now_ms)" -lt "$deadline" ]; do sql "$shard" "SELECT 1" >/dev/null 2>&1; done ) &
  done
  wait

  echo "--- detection: Fathom.Rebalancer.LoadSamples (published to Postgres) ---"
  rpc fathom1 "Fathom.Rebalancer.LoadSamples.latest_per_shard(60_000) |> Enum.filter(&(&1.q_per_s > 3)) |> Enum.sort_by(&(-&1.q_per_s)) |> Enum.take(5) |> Enum.map(fn s -> {s.shard_id, s.node_key, Float.round(s.q_per_s, 1)} end) |> inspect() |> IO.puts()"

  local from; from=$(cmd_owner "$shard" || echo "?")
  local to; to=$(other_node "$from")
  echo "  '$shard' is on $from → handing off to $to"

  # 1. Pin + render the exception map (the app writes it to the shared lb-runtime dir).
  rpc fathom1 "Fathom.Rebalancer.Overrides.pin(\"$shard\", \"$to\", reason: \"demo\"); Fathom.Rebalancer.LbApply.apply!()" >/dev/null
  echo "  pinned $shard → $to; exception map rendered:"
  rpc fathom1 "Fathom.Rebalancer.LbMap.current() |> IO.puts()" | grep -E "$shard|fathom_pin_$to" | sed 's/^/    /'

  # 2. Flip: the lb-reloader sidecar reloads nginx on the map change (no host bridge);
  # give it a moment to apply before draining, so the source stops taking new traffic.
  echo "  waiting for the lb-reloader sidecar to apply the flip ..."
  sleep 3

  # 3. Drain the source so it releases the lease (flip-first means it finishes fast).
  echo "  draining $shard on $from ..."
  rpc "$from" "Fathom.Shards.drain(\"$shard\", 10_000) |> inspect() |> IO.puts()" | sed 's/^/    drain: /'

  # 4. A request now routes to the target, which acquires the freed lease and serves.
  sql "$shard" "SELECT 1" >/dev/null 2>&1
  sleep 1
  local now_owner; now_owner=$(cmd_owner "$shard" || echo "?")
  # Isolation must still hold after the move.
  local foreign; foreign=$(sql "$shard" "SELECT count(*) FROM kv WHERE tenant <> '$shard'" | val 2>/dev/null)

  echo "  '$shard' now served by: $now_owner (was $from, pinned to $to)"
  if [ "$now_owner" = "$to" ] && [ "${foreign:-0}" = "0" ]; then
    echo "rebalance: OK — $shard moved $from → $to, isolation intact"
  else
    echo "rebalance: owner=$now_owner (expected $to), foreign=$foreign"
    return 1
  fi
}

# -- tpcb / tpcc: remote-client TPC over the real network through the LB --------
# The realism layer for the TPC benchmarks (docs/tpc-benchmark-plan.md Phase 4): a real
# libSQL/Hrana client (tpc_driver.py, dep-free stdlib) drives the workload statement-by-statement
# on held streams through the LB — the true remote-client path the in-process loopback gate
# (mix fathom.wire_bench / mix fathom.tpcc) cannot reach. Recorded-only; results go to
# docs/reviews/. NOTE: the rig is single-host, so client→LB is loopback — the realism is the real
# nginx LB hop + prod-release node (Bandit/Filo) + S3(MinIO)-backed storage, not a WAN RTT.
cmd_tpcb() {
  local shards=${1:-8} txns=${2:-4000} accounts=${3:-10000}
  echo "tpcb: RTT probe + $shards tenant shards × TPC-B writes through the LB (remote client)" >&2
  python3 tpc_driver.py rtt --lb "$LB" --domain "$DOMAIN" --shard tpcb_rtt --samples 200
  python3 tpc_driver.py tpcb --lb "$LB" --domain "$DOMAIN" --shard tpcb \
    --txns "$txns" --clients "$shards" --accounts "$accounts"
}

cmd_tpcc() {
  local max_w=${1:-5} threads=${2:-8} txns=${3:-2000} scale=${4:-0.02}
  echo "tpcc: W=1..$max_w sweep, $threads threads, $txns txns/W, scale $scale — remote client through the LB" >&2
  python3 tpc_driver.py tpcc --lb "$LB" --domain "$DOMAIN" \
    --max-w "$max_w" --threads "$threads" --txns "$txns" --scale "$scale"
}

# -- density: multi-node fleet shard density (the fathom counterpart to turso_density.sh) --
# sqld holds every namespace resident in ONE process (~17 KiB/ns, degrading create rate);
# fathom spreads shards across the fleet via the LB keyspace-partition, so each node pays
# only for its ACTIVE working set (idle shards flush to MinIO, 0 resident) and capacity is
# horizontally additive. This mints N novel shards through the LB — the consistent hash
# spreads d_1..d_N across the nodes — then reads per node the coordinators held (the
# partition) and BEAM/RSS memory (the per-shard cost), showing the fleet holds ~N total,
# ~N/nodes each, at the single-node per-shard cost with no per-node degradation.
cmd_density() {
  local shards=${1:-6000} workers=${2:-24}
  local ncount=${#NODES[@]}
  echo "density: minting $shards novel shards through the LB across $ncount nodes ($workers workers) ..."

  local n i
  # New coordinators must outlive the read: the rig idle-drop is 20s and a coordinator
  # freezes idle_ms at init (Fathom.Shard init reads it once), so raise it BEFORE minting.
  # Quiet the rebalancer so no handoff moves a shard off its hash-home node — that would blur
  # the partition-evenness we're measuring. And lift the production `max_open_shards` soft cap
  # (else a node near the cap starts LRU-evicting idle shards and we'd measure eviction churn,
  # not raw density) — set it to N so no partition skew can hit it. All restored at the end.
  for n in "${NODES[@]}"; do
    rpc "$n" "Application.put_env(:fathom, :shard_idle_ms, 900_000); Application.put_env(:fathom, :rebalancer_enabled, false); Application.put_env(:fathom, :max_open_shards, $shards)" >/dev/null
  done

  # One round-trip per node -> "coordinators beam_bytes rss_kb". GC every process first so
  # :erlang.memory(:total) reflects LIVE memory, not un-collected garbage (which otherwise
  # makes the per-shard BEAM cost lumpy across nodes by GC timing, not real variance).
  _dsample() {
    rpc "$1" 'Enum.each(Process.list(), fn p -> :erlang.garbage_collect(p) end); rss = (case File.read("/proc/self/status") do {:ok, s} -> (Regex.run(~r/VmRSS:\s+(\d+)/, s, capture: :all_but_first) || ["0"]) |> hd(); _ -> "0" end); IO.puts("#{Registry.count(Fathom.ShardRegistry)} #{:erlang.memory(:total)} #{rss}")'
  }

  local -a bc bm br   # baseline per node: coordinators, BEAM bytes, RSS kB
  i=0
  for n in "${NODES[@]}"; do
    read -r bc[$i] bm[$i] br[$i] <<< "$(_dsample "$n")"
    i=$((i + 1))
  done

  # Mint: SELECT 1 needs no table; each is a real cold-open (create the file + acquire the
  # lease in MinIO) on the node the LB hashes d_<i>.<domain> to. Workers stride the id space.
  local start_ms; start_ms=$(now_ms)
  local w
  for w in $(seq 1 "$workers"); do
    ( local j=$w; while [ "$j" -le "$shards" ]; do sql "d_$j" "SELECT 1" >/dev/null 2>&1; j=$((j + workers)); done ) &
  done
  wait
  local mint_ms=$(( $(now_ms) - start_ms )); [ "$mint_ms" -gt 0 ] || mint_ms=1
  echo "minted in ${mint_ms}ms (~$(( shards * 1000 / mint_ms )) shards/s through the LB)"
  echo ""

  # Read back: the partition (coordinators held) + the cost (BEAM/RSS delta) per node.
  printf "  %-9s %8s %11s %10s %11s\n" node held "BEAM MB" "KiB/shd" "RSS MB"
  local th=0 tbeam=0 trss=0 minh=-1 maxh=-1
  i=0
  for n in "${NODES[@]}"; do
    local ac am ar held dbeam drss perkib
    read -r ac am ar <<< "$(_dsample "$n")"
    held=$(( ac - bc[i] )); dbeam=$(( am - bm[i] )); drss=$(( ar - br[i] ))
    if [ "$held" -gt 0 ]; then perkib=$(( dbeam / 1024 / held )); else perkib=0; fi
    printf "  %-9s %8d %11s %10d %11s\n" "$n" "$held" \
      "$(awk -v x="$dbeam" 'BEGIN{printf "%.1f", x/1048576}')" "$perkib" \
      "$(awk -v x="$drss" 'BEGIN{printf "%.1f", x/1024}')"
    th=$(( th + held )); tbeam=$(( tbeam + dbeam )); trss=$(( trss + drss ))
    if [ "$minh" -lt 0 ] || [ "$held" -lt "$minh" ]; then minh=$held; fi
    if [ "$maxh" -lt 0 ] || [ "$held" -gt "$maxh" ]; then maxh=$held; fi
    i=$((i + 1))
  done

  echo ""
  local ideal=$(( shards / ncount )) avgkib=0
  [ "$th" -gt 0 ] && avgkib=$(( tbeam / 1024 / th ))
  echo "  partition:  $th shards held across $ncount nodes (minted $shards); ideal ~$ideal/node,"
  echo "              observed min $minh / max $maxh (spread $(awk -v a="$maxh" -v b="$minh" 'BEGIN{printf "%.2f", (b>0)?a/b:0}')x, 1.00 = perfect)"
  echo "  cost:       ~$avgkib KiB/shard resident (BEAM Δ); fleet RSS Δ $(awk -v x="$trss" 'BEGIN{printf "%.0f", x/1024}') MB"
  echo "  fleet:      capacity = nodes × per-node working set; idle shards flush to MinIO (0 resident)."
  echo "              vs sqld (every namespace resident in one process): fathom's resident cost tracks"
  echo "              the ACTIVE set and scales out by adding nodes, not by growing one process."

  # Restore rig defaults (already-minted coordinators keep their frozen idle and drop on
  # their own; this un-freezes future opens, re-arms the rebalancer, re-instates the cap).
  for n in "${NODES[@]}"; do
    rpc "$n" 'Application.put_env(:fathom, :shard_idle_ms, 20_000); Application.put_env(:fathom, :rebalancer_enabled, true); Application.put_env(:fathom, :max_open_shards, 10_000)' >/dev/null
  done
  unset -f _dsample
}

# -- served: the fd-bound SERVED ceiling — how many shards a node holds under a LIVE connection
# `density` above measures the warm-resident *floor* (idle coordinators, ~16 KiB, memory-bound).
# This measures the *served* ceiling: a held exqlite connection per shard (~1 fd, ~220 KiB — an
# order of magnitude over the idle floor), driven under a query pass. It is **fd-bound**, so the
# container nofile must be raised (docker-compose sets soft 65536; the default 1024 caps a node at
# ~940). Opens node-scoped ids *locally* per node (not via the LB — the served ceiling is a per-node
# property; the LB partition is what `density` measures). Each node holds `per` connections at once,
# concurrently, so the fleet holds `nodes × per` live connections simultaneously.
cmd_served() {
  local per=${1:-10000}
  local ncount=${#NODES[@]}
  echo "served: holding $per shards under a LIVE connection on each of $ncount nodes (fleet $(( per * ncount ))) ..."
  echo "  fd-bound: ~1 fd + ~220 KiB per held connection. Needs container nofile raised — compose sets"
  echo "  soft 65536; the default 1024 caps a node at ~940. (Idle-floor density is \`chaos.sh density\`.)"
  local n
  # Lift the cap + idle + rebalancer for the run (the held set is all busy, so the soft cap can't
  # evict to make room — it would 503; raise it above `per`). Restored at the end.
  for n in "${NODES[@]}"; do
    rpc "$n" "Application.put_env(:fathom, :max_open_shards, $(( per + 5000 ))); Application.put_env(:fathom, :shard_idle_ms, 900_000); Application.put_env(:fathom, :rebalancer_enabled, false)" >/dev/null
  done

  # Per node: open `per` node-scoped shards each holding a live connection until (op or the fd wall),
  # GC, sample RSS/fds, run one query pass over all held connections (throughput), then release. The
  # id prefix is the sanitised node name so the three nodes never contend for the same lease.
  local expr
expr=$(cat <<'ELIXIR'
base_rss = (File.read!("/proc/self/status") |> then(fn s -> [x] = Regex.run(~r/VmRSS:\s+(\d+)/, s, capture: :all_but_first); String.to_integer(x) end)); base_fds = length(File.ls!("/proc/self/fd")); pref = "srv" <> String.replace(Atom.to_string(node()), ~r/[^a-z0-9]/, "") <> "_"; {open_us, {op, hs}} = :timer.tc(fn -> Enum.reduce_while(1..__PER__, {0, []}, fn i, {c, a} -> id = pref <> Integer.to_string(i); r = (try do (with {:ok, p, rf, pa} <- Fathom.Shards.checkout(id), {:ok, cn} <- Fathom.Shard.Connection.open(pa), {:ok, _} <- Fathom.Shard.Connection.query(cn, "SELECT 1", []), do: {:ok, {p, rf, cn}}) rescue e -> {:error, e} catch :exit, x -> {:error, x} end); case r do {:ok, h} -> {:cont, {c + 1, [h | a]}}; _ -> {:halt, {c, a}} end end) end); open_rate = if open_us > 0, do: round(op * 1_000_000 / open_us), else: 0; :erlang.garbage_collect(); rss = (File.read!("/proc/self/status") |> then(fn s -> [x] = Regex.run(~r/VmRSS:\s+(\d+)/, s, capture: :all_but_first); String.to_integer(x) end)); fds = length(File.ls!("/proc/self/fd")); {qus, _} = :timer.tc(fn -> Enum.each(hs, fn {_, _, cn} -> Fathom.Shard.Connection.query(cn, "SELECT 1", []) end) end); Enum.each(hs, fn {p, rf, cn} -> Fathom.Shard.Connection.close(cn); Fathom.Shard.checkin(p, rf) end); qps = if qus > 0, do: round(op * 1_000_000 / qus), else: 0; IO.puts("opened=#{op} open_rate=#{open_rate} rss_per_shard_kb=#{div(rss - base_rss, max(op, 1))} fds_per_shard=#{Float.round((fds - base_fds) / max(op, 1), 2)} qps=#{qps}")
ELIXIR
)
  expr=${expr//__PER__/$per}

  local tmp; tmp=$(mktemp -d)
  for n in "${NODES[@]}"; do ( rpc "$n" "$expr" > "$tmp/$n" 2>&1 ) & done
  wait

  echo ""
  printf "  %-9s %8s %10s %9s %11s\n" node held "KiB/shd" "fds/shd" "q/s pass"
  local total=0 tqps=0
  for n in "${NODES[@]}"; do
    local line held perkib fdsp qps
    line=$(cat "$tmp/$n")
    held=$(printf '%s' "$line" | grep -o 'opened=[0-9]*' | cut -d= -f2); held=${held:-0}
    perkib=$(printf '%s' "$line" | grep -o 'rss_per_shard_kb=[0-9]*' | cut -d= -f2)
    fdsp=$(printf '%s' "$line" | grep -o 'fds_per_shard=[0-9.]*' | cut -d= -f2)
    qps=$(printf '%s' "$line" | grep -o 'qps=[0-9]*' | cut -d= -f2); qps=${qps:-0}
    if [ "$held" = "0" ]; then
      printf "  %-9s %8s  %s\n" "$n" "ERR" "$line"
    else
      printf "  %-9s %8d %10s %9s %11s\n" "$n" "$held" "${perkib:-?}" "${fdsp:-?}" "${qps:-?}"
    fi
    total=$(( total + held )); tqps=$(( tqps + qps ))
  done
  echo ""
  echo "  fleet:  $total shards held under a live connection at once, ~$tqps q/s aggregate on a query pass."
  echo "          Served cost ~220 KiB/shard (vs the ~16 KiB idle floor) at 1 fd/shard — memory-cheap,"
  echo "          fd-bound: raise container nofile to lift the per-node ceiling, add nodes to multiply."

  for n in "${NODES[@]}"; do
    rpc "$n" 'Application.put_env(:fathom, :max_open_shards, 10_000); Application.put_env(:fathom, :shard_idle_ms, 20_000); Application.put_env(:fathom, :rebalancer_enabled, true)' >/dev/null
  done
  rm -rf "$tmp"
}

case "${1:-}" in
  build)       cmd_build ;;
  tpcb)        shift; cmd_tpcb "$@" ;;
  tpcc)        shift; cmd_tpcc "$@" ;;
  density)     shift; cmd_density "$@" ;;
  served)      shift; cmd_served "$@" ;;
  rebalance)   shift; cmd_rebalance "$@" ;;
  hotspots)    shift; cmd_hotspots "$@" ;;
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
