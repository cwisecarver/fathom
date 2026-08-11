#!/usr/bin/env bash
# Fathom chaos-rig driver — the "failover time + loss window" layer of
# docs/deploy-cluster.md "Chaos testing" (the in-process suite pins safety;
# this rig measures latency and demonstrates the failure modes end to end).
#
#   ./chaos.sh build              build the fathom release image
#   ./chaos.sh build-driver       build the in-network elixir tpc-driver image (for TPC_NET=container)
#   ./chaos.sh up | down | logs [svc]
#   ./chaos.sh smoke              write+read a few tenants through the LB; check isolation
#   ./chaos.sh owner <shard>      which node holds the shard file
#   ./chaos.sh latency <ms>|clear inject S3 latency on every node (toxiproxy)
#   ./chaos.sh failover <shard>   silent-kill the owner; time LB reroute + survivor steal
#   ./chaos.sh failover-herd [shards warm]  kill a node holding N shards; measure time-to-served
#                                 across ALL its tenants (p50/p90/p99/max) — the herd, warm=on|off
#   ./chaos.sh deploy [shards node]  clean-shutdown (rolling-deploy) proof: hold N open+dirty shards
#                                 on a node, SIGTERM it, verify the graceful terminate flushed EVERY
#                                 shard (zero loss) — the herd analog of failover for a node upgrade
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
#   ./chaos.sh replication [shard]  Phase-2 A2: prove every node's follower listener accepts
#                                 from its peers (the half that was never started outside tests).
#                                 Add REPLICATION_ENABLED=true — to `up` AND to this — to also
#                                 ship a write and check followers hold a replica.
#   ./chaos.sh tpcb [shards txns accounts]  remote TPC-B: RTT probe + N tenant shards driven
#                                 through the LB by a real libSQL client (the Phase-4 realism run)
#   ./chaos.sh tpcc [max_w threads txns scale]  remote TPC-C: W=1..max_w sweep through the LB
#                                 by a real libSQL client — per-txn-type latency + tpmC
#   ./chaos.sh tpc-fleet [tenants_csv per_client accounts]  multi-tenant TPC-B THROUGHPUT across the
#                                 fleet: sweep the tenant count (one single-writer shard each), read
#                                 aggregate txn/s + the per-node load split — the throughput-across-nodes run
#   TPC_NET=container ./chaos.sh tpcc-fleet [tenants_csv per_client scale]  the TPC-C analog: sweep the
#                                 tenant count with a 1-warehouse TPC-C per tenant; aggregate tpmC +
#                                 per-node split (elixir driver only)
#   TPC_DRIVER=elixir ./chaos.sh tpcb|tpc-fleet  drive with the Filo.Client BEAM driver
#                                 (tpc_driver.exs): one process per client instead of python's
#                                 thread-per-client, so one box models far more concurrent clients.
#                                 Covers rtt/tpcb/tpcc.
#   TPC_NET=container TPC_DRIVER=elixir ./chaos.sh tpcb|tpc-fleet  run that driver IN-NETWORK
#                                 (compose run driver → LB as http://lb), past the host forwarder's
#                                 ~1k-conn ceiling. Build it once: `./chaos.sh build-driver`.
#   ./chaos.sh rollout [tenants limit timeout_s]  fleet SCHEMA-MIGRATION throughput: seed N tenants
#                                 at v1, release v2, run the sweep, measure shards/s + the rate/ETA
#                                 status/0 reports + the per-node split from shard_migrated telemetry
#                                 — the evidence base for :reconcile_batch_size (review 2026-08-01 #43)
#   ./chaos.sh density [shards workers]  mint N novel shards through the LB; read per node
#                                 the coordinators held (the partition) + BEAM/RSS per shard
#                                 (the cost) — the fathom counterpart to turso_density.sh
#   ./chaos.sh served [per]       hold `per` shards under a LIVE connection on each node at once
#                                 (the fd-bound served ceiling, ~220 KiB/shard); needs container
#                                 nofile raised (compose sets soft 65536; default 1024 caps ~940)
#   ./chaos.sh served-data [per rows blob]  same, but each shard seeded with rows×blob bytes and
#                                 scanned — the data-bearing served cost (page cache + 3 WAL fds/shard)
#   ./chaos.sh latency-cost [ms samples rows blob]  MEASURE cold-open (pull) + flush (upload) cost
#                                 per node, baseline vs an injected S3 RTT — the TPC Phase-4 follow-on
#                                 (what `latency <ms>` INJECTS, this quantifies). Cold-open ≈ ~2× one-way
#
# All timings are RELATIVE (one host, loopback MinIO): run `latency 30` first so
# failover numbers reflect a real S3 RTT rather than loopback.

set -u -o pipefail
cd "$(dirname "$0")"

LB_PORT=${LB_PORT:-8080}
LB=${LB:-http://localhost:$LB_PORT}
TOXI=${TOXI:-http://localhost:8474}
DOMAIN=fathom.test
TTL_MS=10000
# A steal is allowed once the owner's heartbeat age exceeds TTL + steal_margin
# (the clock-skew guard, Fathom.Shard.Storage.steal_margin_ms, default 5000 and
# not overridden in the rig). Scenarios that force a steal must wait past both.
STEAL_MARGIN_MS=5000
# FIVE nodes since Phase-2 A2 (docs/a2-quorum-replication.md): a shard's replica set is
# 1 primary + 4 read-only followers acking at 2-of-4, so five is the MINIMUM fleet that can
# hold one. Every scenario below reads this array rather than naming nodes, so growing the
# fleet is this one line plus the port map — the exceptions are noted where they exist
# (`deploy`/`failover-herd` target fathom1 by default, and the control-plane rpc calls in
# `rebalance`/`rollout` go to fathom1 because Postgres is fleet-shared, not per-node).
NODES=(fathom1 fathom2 fathom3 fathom4 fathom5)
# direct (LB-bypassing) Hrana port per node, for forced-steal experiments.
# A function (not an associative array) so the script runs on macOS's bash 3.2.
direct_port() {
  case $1 in
    fathom1) echo 18081 ;;
    fathom2) echo 18082 ;;
    fathom3) echo 18083 ;;
    fathom4) echo 18084 ;;
    fathom5) echo 18085 ;;
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
vals() { jq -r '.results[0].response.result.rows[]?[0].value'; } # every row, first col

# recoverable_seqs <tenant>: every `seq` an operator could still recover on ANY node, one per
# line — across all three places an acked row legitimately survives a kill.
#
# A shard whose owner is SIGKILLed mid-write keeps its un-flushed rows in that node's local file.
# From there the row can end up in any of:
#
#   <shard>.db           the node's plain local copy, when it has NOT re-opened the shard since
#                        restarting — nothing has run the fork check yet, so the rows just sit
#                        there. Easy to forget because this file is invisible through the LB,
#                        which serves whichever node currently owns the lease.
#   <shard>.db.forked.*  quarantined on re-open, once the node sees a peer advanced the lineage.
#   <shard>.db.fenced.*  quarantined when the lease was superseded with un-flushed writes
#                        ("acked writes preserved for recovery" — a DIFFERENT suffix, and the
#                        first version of this helper globbed only `.forked.*` and missed it).
#
# Scanning fewer than all three under-counts, and soak then reports recoverable rows as DESTROYED.
recoverable_seqs() {
  local t=$1 n
  for n in "${NODES[@]}"; do
    rpc "$n" "
      ([\"/data/shards/$t.db\"] ++ Path.wildcard(\"/data/shards/$t.db.forked.*\") ++
         Path.wildcard(\"/data/shards/$t.db.fenced.*\"))
      |> Enum.filter(&File.exists?/1)
      |> Enum.each(fn f ->
        case Fathom.Shard.Connection.open(f) do
          {:ok, c} ->
            case Fathom.Shard.Connection.query(c, \"SELECT seq FROM kv\", []) do
              {:ok, %{rows: rows}} -> Enum.each(rows, fn [s] -> IO.puts(s) end)
              _ -> :ok
            end
            Fathom.Shard.Connection.close(c)
          _ -> :ok
        end
      end)"
  done
}
# `val` yields the literal "null" on an error/empty response. Guard every arithmetic use:
# bash arithmetic treats a bare word as a variable name, so `$(( n + null ))` aborts under set -u.
is_num() { case "${1:-}" in ''|*[!0-9]*) return 1 ;; *) return 0 ;; esac; }

# seed <shard>: kv table exists AND is EMPTY.
#
# Every run starts fresh. This used to be `CREATE TABLE IF NOT EXISTS` alone, so a shard kept
# whatever earlier runs left in it — and since the rig reuses shard files and the object store
# across runs, that state is durable and silently carried forward.
#
# It is not cosmetic: `soak` compares rows it ACKED this run against rows STORED in the shard,
# so a previous run's survivors counted as stored but never as acked and the headline came out
# as NEGATIVE loss (`acked=1233 stored=1808 lost=-575`). Diagnosed by grouping on `seq`: every
# value appeared exactly twice and never three times — one copy per run. `smoke` drifted the
# same way, reporting rows=2, then 3, then 5 across successive runs.
#
# Truncating here rather than in each command means every caller (smoke, failover, pause-fence,
# soak, warm-home, rebalance) gets a clean baseline by construction, and a command added later
# cannot forget to. Every current caller seeds once at setup, never between steps, so this
# cannot clear data a run is still relying on.
seed() {
  sql "$1" "CREATE TABLE IF NOT EXISTS kv (id INTEGER PRIMARY KEY AUTOINCREMENT, tenant TEXT, seq INTEGER)" >/dev/null
  sql "$1" "DELETE FROM kv" >/dev/null 2>&1 || true

  # Quarantine copies are durable too, and truncating only the LIVE table left them behind.
  # `soak` now reconciles acks against `.forked.*` (see forked_seqs), so a previous run's
  # quarantine files counted as this run's recoverable rows: the first run with the new verdict
  # printed DESTROYED=-656, negative because `recoverable` (1125, carrying seqs up to 1809 from
  # the prior run) exceeded `acked` (469). Exactly the shape of the `lost=-575` contamination
  # this function's DELETE was written for — a second surface of the same bug.
  #
  # `.fenced.*` and `.corrupt.*` are swept for the same reason: they are per-shard forensic
  # copies a later run must not inherit.
  local n
  for n in "${NODES[@]}"; do
    compose exec -T "$n" sh -lc \
      "rm -f /data/shards/$1.db.forked.* /data/shards/$1.db.fenced.* /data/shards/$1.db.corrupt.*" \
      >/dev/null 2>&1 || true
  done

  # Make the empty state DURABLE before the run starts. The DELETE above only lands in the
  # owner's local file; if a churn kill hits before the next periodic flush, a survivor
  # cold-opens the last STORED object — which still holds the previous run's rows. That is how
  # a 90s soak came back with `t6 acked=49 stored=152`: 152 was mostly last run's data restored
  # from S3, and it dragged `needs-operator-recovery` negative. Same "every run starts fresh"
  # contract as the DELETE; it just has to reach storage to hold across a kill.
  for n in "${NODES[@]}"; do
    rpc "$n" "Fathom.Shards.flush(\"$1\")" >/dev/null 2>&1 || true
  done
}

# -- commands -------------------------------------------------------------------
cmd_build() { compose build migrate; }
# The in-network elixir driver image (deploy/chaos/Dockerfile.driver), used by TPC_NET=container.
cmd_build_driver() { compose build driver; }
cmd_up()    { compose up -d --wait --wait-timeout 180; compose ps; check_lb_port; }

# Is the LB port actually OURS? Docker does not fail when the host port is already taken — it
# publishes anyway and the earlier listener keeps winning, so every request through $LB reaches
# the squatter. Cost 2026-08-10: an `mlx_lm server` had held :8080 for six days, `smoke` failed
# every write, the reply was a bare "Not Found", and nginx logged NOTHING — which reads exactly
# like a fathom 404. The tell is the Server header: nginx sets one, squatters generally do not.
#
# A warning, not a failure: the rig is still fine over the compose network, and a scenario that
# refused to run because of an unrelated host process would be worse than one that says why.
check_lb_port() {
  local server
  server=$(curl -s -m 3 -D- -o /dev/null "$LB/" -H "Host: probe.$DOMAIN" 2>/dev/null \
             | tr -d '\r' | grep -i '^server:' | head -1 | cut -d' ' -f2-)

  case "$server" in
    *nginx*) return 0 ;;
  esac

  echo
  echo "WARNING: $LB is not answering as nginx (Server: ${server:-none})."
  echo "  Something else holds host port $LB_PORT, and Docker published over it silently."
  echo "  Every LB-routed scenario will fail with confusing 404s. Find it with:"
  echo "      lsof -nP -iTCP:$LB_PORT -sTCP:LISTEN"
  echo "  Then free the port, or re-run on another one:  LB_PORT=18080 ./chaos.sh up"
}
cmd_down()  { compose down -v --remove-orphans; }
cmd_logs()  { compose logs -f "${1:-}"; }

cmd_owner() {
  local shard=$1 n
  for n in "${NODES[@]}"; do
    if compose exec -T "$n" test -f "/data/shards/$shard.db" 2>/dev/null; then echo "$n"; return 0; fi
  done
  return 1
}

# One toxiproxy per node's S3 path, named `s3-<node>` (see toxiproxy.json) — the same
# convention `partition <node>` uses. Derived from NODES rather than spelled out, so a
# fleet-size change can't leave a node's S3 path un-injected: a scenario would then time
# a "latency" run in which one node was silently still on the loopback floor.
cmd_latency() {
  local p n
  if [ "${1:-}" = "clear" ]; then
    for n in "${NODES[@]}"; do
      p="s3-$n"
      curl -sS -X DELETE "$TOXI/proxies/$p/toxics/lat_down" >/dev/null 2>&1
      curl -sS -X DELETE "$TOXI/proxies/$p/toxics/lat_up" >/dev/null 2>&1
    done
    echo "latency cleared"
  else
    local ms=${1:?usage: latency <ms>|clear}
    for n in "${NODES[@]}"; do
      p="s3-$n"
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
  # seed/1 truncates, so acked (this run) and stored are directly comparable. See seed/1.
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

  local total_acked=0 total_stored=0 leaks=0 unreadable=0 total_recovered=0
  for t in "${tenants[@]}"; do
    local acked stored foreign recovered
    # `val` emits the literal string "null" when the response carries no row — which happens
    # here whenever a tenant's shard is mid-failover at verdict time (the churn loop kills a
    # node 8s before it restarts it, and the last kill can land right before this readback).
    # `$(( x + null ))` then treats `null` as a variable NAME, so `set -u` killed the whole
    # summary before it printed the verdict — the isolation check the soak actually asserts.
    # Retry once, then report the tenant as UNREADABLE rather than silently counting it as 0:
    # "could not read t6" is a result, not a zero.
    stored=$(sql "$t" "SELECT count(*) FROM kv WHERE seq > 0" | val)
    is_num "$stored" || { sleep 5; stored=$(sql "$t" "SELECT count(*) FROM kv WHERE seq > 0" | val); }
    foreign=$(sql "$t" "SELECT count(*) FROM kv WHERE tenant <> '$t'" | val)

    # RECOVERABLE, not lost: seqs present in the live shard OR anywhere recoverable_seqs looks.
    # A tenant unreadable live still counts those rows, which is the whole point — t6 in the
    # first 2026-08-03 run was unreadable with all 68 of its acks sitting in quarantine.
    #
    # SET intersection, not a count difference. The first version subtracted cardinalities and
    # went NEGATIVE (`DESTROYED=-8`), because a shard can hold rows that were never acked: the
    # write committed server-side and the ACK was lost when churn killed the node mid-response.
    # That is a normal distributed outcome, not a surplus — so "destroyed" has to mean
    # "acked seqs present NOWHERE", which only a set difference answers.
    local all_seqs acks_sorted
    all_seqs=$(mktemp); acks_sorted=$(mktemp)
    { sql "$t" "SELECT seq FROM kv" | vals; recoverable_seqs "$t"; } \
      | grep -E '^[0-9]+$' | sort -u > "$all_seqs"
    sort -u "$ack_dir/$t" > "$acks_sorted"
    acked=$(wc -l < "$acks_sorted" | tr -d ' ')
    recovered=$(comm -12 "$acks_sorted" "$all_seqs" | wc -l | tr -d ' ')
    rm -f "$all_seqs" "$acks_sorted"
    total_acked=$((total_acked + acked)); total_recovered=$((total_recovered + recovered))

    if is_num "$stored" && is_num "$foreign"; then
      total_stored=$((total_stored + stored))
      [ "$foreign" != "0" ] && { leaks=$((leaks + 1)); echo "  *** ISOLATION LEAK in $t: $foreign foreign rows"; }
      printf "  %-4s acked=%-6s stored=%-6s recoverable=%-6s destroyed=%-4s foreign=%s\n" \
        "$t" "$acked" "$stored" "$recovered" "$((acked - recovered))" "$foreign"
    else
      unreadable=$((unreadable + 1))
      printf "  %-4s acked=%-6s stored=%-6s recoverable=%-6s destroyed=%-4s foreign=%-6s  *** UNREADABLE live\n" \
        "$t" "$acked" "${stored:-?}" "$recovered" "$((acked - recovered))" "${foreign:-?}"
    fi
  done

  # DESTROYED is the number that matters, and it is NOT `acked - stored`.
  #
  # The old headline was `lost=acked-stored`, counting only the LIVE shard — so it reported every
  # quarantined row as lost. The 2026-08-03 run printed `lost=498` and looked like a serious
  # regression; reconciling the acks against the `.forked.*` copies showed all 1349 acked rows
  # present, 0 destroyed. The system was doing exactly what single-writer promises and the metric
  # was lying about it, in the alarming direction (#24 was the same lesson pointing the other way:
  # a metric that reads healthy while data is gone).
  #
  # `stored` is kept because the gap IS the operator's recovery workload — rows that need a
  # `.forked.*` merge rather than being served — but it is no longer the pass/fail number.
  local destroyed=$((total_acked - total_recovered))
  echo "RESULT soak: acked=$total_acked stored=$total_stored recoverable=$total_recovered DESTROYED=$destroyed (MUST be 0), leaks=$leaks (MUST be 0), unreadable=$unreadable"
  echo "  needs-operator-recovery: $((total_acked - total_stored)) row(s) live only in .forked.* quarantine copies (RPO: committed-but-unflushed at a kill)"
  [ "$unreadable" -gt 0 ] && echo "  NOTE: $unreadable tenant(s) unreadable live — their quarantined rows ARE counted in recoverable"
  [ $leaks -eq 0 ] && [ $destroyed -eq 0 ]
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

# -- replication: A2 quorum replication, multi-node for the first time ---------
# Two halves, and the FIRST one is the point.
#
# Until 2026-08-10 `Replication.Follower` — the listener that accepts frames — was started only by
# the test suite, never by the application. So `REPLICATION_ENABLED` on a real node shipped every
# commit into a closed port and 503'd every tenant write. The rig had no replication env at all, so
# nothing here could have caught it, despite fathom4/fathom5 having been added specifically to hold
# a full replica set.
#
# AGENTS.md's rig rule is to assert the fix's OWN observable before trusting anything else, so
# `listeners` runs first and independently: if the listeners are not up, no later number means
# anything. It needs no shipping, so it is also the check that the documented rollout order
# (listen fleet-wide, THEN ship) is actually satisfiable.
#
#   ./chaos.sh replication                              # listeners only
#   REPLICATION_ENABLED=true ./chaos.sh up && \
#     REPLICATION_ENABLED=true ./chaos.sh replication acme   # + the shipping half
cmd_replication() {
  local shard=${1:-acme}
  local n peer probe ok=0 fail=0

  echo "=== 1. every node is LISTENING (the fix's own observable) ==="
  for n in "${NODES[@]}"; do
    peer=$(other_node "$n")
    # Dial peer:9100 FROM another node, so this proves reachability over the same network path a
    # shipper uses — not just that a socket exists inside one container.
    probe=$(rpc "$n" 'IO.puts(case :gen_tcp.connect(~c"'"$peer"'", 9100, [:binary], 2000) do {:ok, s} -> :gen_tcp.close(s); "OPEN"; {:error, e} -> "FAIL #{inspect(e)}" end)')
    case "$probe" in
      *OPEN*) echo "  $n -> $peer:9100  OPEN"; ok=$((ok + 1)) ;;
      *)      echo "  $n -> $peer:9100  $probe"; fail=$((fail + 1)) ;;
    esac
  done

  if [ "$fail" -ne 0 ]; then
    echo "FAIL: $fail/$((ok + fail)) probes could not reach a follower listener."
    echo "  The image may predate the listener (./chaos.sh build), or REPLICATION_LISTEN is unset."
    return 1
  fi
  echo "  all $ok probes OPEN"

  echo
  echo "=== 2. the replica directory is reported to the disk gauge ==="
  for n in "${NODES[@]}"; do
    printf '  %s replica dir: %s\n' "$n" \
      "$(rpc "$n" 'IO.puts(Fathom.Shard.Replication.Follower.default_dir())')"
  done

  if [ "${REPLICATION_ENABLED:-}" != "true" ]; then
    echo
    echo "shipping is OFF (REPLICATION_ENABLED unset) — listeners proven, stopping here."
    echo "for the shipping half:  REPLICATION_ENABLED=true ./chaos.sh up && REPLICATION_ENABLED=true ./chaos.sh replication $shard"
    return 0
  fi

  echo
  echo "=== 3. a write reaches followers (shard=$shard) ==="
  seed "$shard"
  local home; home=$(cmd_owner "$shard") || { echo "no home found"; return 1; }
  echo "  home=$home"

  sql "$shard" "INSERT INTO kv (tenant, seq) VALUES ('$shard', 1)" >/dev/null
  sql "$shard" "INSERT INTO kv (tenant, seq) VALUES ('$shard', 2)" >/dev/null
  sleep 3

  local holders=0
  for n in "${NODES[@]}"; do
    [ "$n" = "$home" ] && continue
    probe=$(rpc "$n" 'IO.puts(if File.exists?(Fathom.Shard.Replication.Follower.db_path(Fathom.Shard.Replication.Follower, "'"$shard"'")), do: "HOLDS", else: "-")')
    case "$probe" in
      *HOLDS*) echo "  $n  HOLDS a replica of $shard"; holders=$((holders + 1)) ;;
      *)       echo "  $n  no replica" ;;
    esac
  done

  echo
  # Q=2 is the rig default, so at least two non-home nodes must hold bytes for the commit that
  # returned to have been legitimately acked.
  if [ "$holders" -ge 2 ]; then
    echo "PASS: $holders followers hold a replica (quorum ${REPLICATION_QUORUM:-2})"
  else
    echo "FAIL: only $holders followers hold a replica — below the quorum the commit claimed"
    return 1
  fi
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
# libSQL/Hrana client (tpc_driver.py stdlib, or the Filo.Client tpc_driver.exs via TPC_DRIVER=elixir)
# drives the workload statement-by-statement
# on held streams through the LB — the true remote-client path the in-process loopback gate
# (mix fathom.wire_bench / mix fathom.tpcc) cannot reach. Recorded-only; results go to
# docs/reviews/. NOTE: the rig is single-host, so client→LB is loopback — the realism is the real
# nginx LB hop + prod-release node (Bandit/Filo) + S3(MinIO)-backed storage, not a WAN RTT.
# Driver selection: `python` (default; dep-free stdlib) or `elixir` (the Filo.Client driver — one
# lightweight BEAM process per client instead of one python OS thread, so one box models far more
# concurrent clients). Both cover rtt / tpcb / tpcc.
TPC_DRIVER=${TPC_DRIVER:-python}

# Where the elixir driver runs: `host` (default) or `container` — in-network via `compose run
# driver`, so one driver holds far more concurrent streams than the host port-forwarder allows
# (colima's forwarder dies ~1k conns). Build the image first: `chaos.sh build-driver`. In-network
# the LB is a service name, so the container drives $LB_NET (http://lb), not the host's $LB.
TPC_NET=${TPC_NET:-host}
LB_NET=${LB_NET:-http://lb}

# Compile the elixir driver's Mix.install deps once, before any captured run (Mix.install prints
# compile logs to stdout on a cold cache — warming here keeps the captures below clean). The compile
# happens even if the LB is down, since it precedes the connect. Host-elixir only — the container
# image is prewarmed at build.
tpc_prewarm() {
  { [ "$TPC_DRIVER" = elixir ] && [ "$TPC_NET" != container ]; } || return 0
  echo "  compiling the elixir tpc driver (Mix.install, first run only) ..." >&2
  elixir tpc_driver.exs rtt --lb "$LB" --domain "$DOMAIN" --shard prewarm --samples 1 >/dev/null 2>&1 || true
}

# Dispatch one driver run. tpc_run OWNS the --lb (the host $LB, or the in-network service name for a
# containerized driver), so callers pass only the mode + workload flags. For elixir, keep only the
# final JSON line — insurance against Mix.install / compose chatter on a cold cache.
tpc_run() { # tpc_run <mode> [flags...]
  local mode=$1
  shift
  if [ "$TPC_DRIVER" = elixir ]; then
    if [ "$TPC_NET" = container ]; then
      compose run --rm -T driver "$mode" --lb "$LB_NET" "$@" | grep -E '^\{.*\}$' | tail -1
    else
      elixir tpc_driver.exs "$mode" --lb "$LB" "$@" | grep -E '^\{.*\}$' | tail -1
    fi
  else
    python3 tpc_driver.py "$mode" --lb "$LB" "$@"
  fi
}

cmd_tpcb() {
  local shards=${1:-8} txns=${2:-4000} accounts=${3:-10000}
  echo "tpcb: RTT probe + $shards tenant shards × TPC-B writes through the LB (remote client, $TPC_DRIVER driver)" >&2
  tpc_prewarm
  tpc_run rtt --domain "$DOMAIN" --shard tpcb_rtt --samples 200
  tpc_run tpcb --domain "$DOMAIN" --shard tpcb \
    --txns "$txns" --clients "$shards" --accounts "$accounts"
}

cmd_tpcc() {
  local max_w=${1:-5} threads=${2:-8} txns=${3:-2000} scale=${4:-0.02}
  echo "tpcc: W=1..$max_w sweep, $threads threads, $txns txns/W, scale $scale — remote client through the LB ($TPC_DRIVER driver)" >&2
  tpc_prewarm
  tpc_run tpcc --domain "$DOMAIN" \
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
  # id prefix is the sanitised node name so no two nodes ever contend for the same lease.
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

# -- served-data: the served ceiling with REAL DATA per shard (page cache + WAL cost)
# `served` holds *empty* shards under a live connection (~220 KiB, 1 fd — a read-only handle). This
# seeds each shard with `rows` × `blob` bytes and holds it under a live connection while scanning the
# data, so the cost also carries SQLite's per-connection page cache (the pages read) and the WAL's
# `-wal`/`-shm` fds materialise (a WAL-active connection is 3 fds, not 1). It measures the one axis the
# empty runs don't: a shard actively serving real tenant data. Args: per (10000), rows (256), blob (1024).
cmd_served_data() {
  local per=${1:-10000} rows=${2:-256} blob=${3:-1024}
  local ncount=${#NODES[@]}
  local kib=$(( rows * blob / 1024 ))
  echo "served-data: $per shards/node (fleet $(( per * ncount ))), each seeded ${rows}×${blob}B (~${kib} KiB) + held under a live connection scanning the rows ..."
  echo "  (data-bearing: WAL-active = 3 fds/shard, so the fd ceiling is ~nofile/3; RSS carries the page cache)"
  local n
  for n in "${NODES[@]}"; do
    rpc "$n" "Application.put_env(:fathom, :max_open_shards, $(( per + 5000 ))); Application.put_env(:fathom, :shard_idle_ms, 900_000); Application.put_env(:fathom, :rebalancer_enabled, false)" >/dev/null
  done

  # Per node: open `per` node-scoped shards, DROP+CREATE a table, seed `rows` blobs, scan (warms the
  # page cache), hold the connection; then sample RSS/fds, run a scan pass (throughput on real data),
  # release. Node-scoped id prefix so no two nodes ever contend for the same lease.
  local expr
expr=$(cat <<'ELIXIR'
base_rss = (File.read!("/proc/self/status") |> then(fn s -> [x] = Regex.run(~r/VmRSS:\s+(\d+)/, s, capture: :all_but_first); String.to_integer(x) end)); base_fds = length(File.ls!("/proc/self/fd")); pref = "srvd" <> String.replace(Atom.to_string(node()), ~r/[^a-z0-9]/, "") <> "_"; seed_ddl = "WITH RECURSIVE c(x) AS (SELECT 1 UNION ALL SELECT x + 1 FROM c WHERE x < __ROWS__) INSERT INTO t (b) SELECT randomblob(__BLOB__) FROM c"; scan = "SELECT count(*), sum(length(b)) FROM t"; {open_us, {op, hs}} = :timer.tc(fn -> Enum.reduce_while(1..__PER__, {0, []}, fn i, {c, a} -> id = pref <> Integer.to_string(i); r = (try do (with {:ok, p, rf, pa} <- Fathom.Shards.checkout(id), {:ok, cn} <- Fathom.Shard.Connection.open(pa), :ok <- Fathom.Shard.Connection.exec(cn, "DROP TABLE IF EXISTS t"), :ok <- Fathom.Shard.Connection.exec(cn, "CREATE TABLE t (id INTEGER PRIMARY KEY, b BLOB)"), :ok <- Fathom.Shard.Connection.exec(cn, seed_ddl), {:ok, _} <- Fathom.Shard.Connection.query(cn, scan, []), do: {:ok, {p, rf, cn}}) rescue e -> {:error, e} catch :exit, x -> {:error, x} end); case r do {:ok, h} -> {:cont, {c + 1, [h | a]}}; _ -> {:halt, {c, a}} end end) end); open_rate = if open_us > 0, do: round(op * 1_000_000 / open_us), else: 0; :erlang.garbage_collect(); rss = (File.read!("/proc/self/status") |> then(fn s -> [x] = Regex.run(~r/VmRSS:\s+(\d+)/, s, capture: :all_but_first); String.to_integer(x) end)); fds = length(File.ls!("/proc/self/fd")); {qus, _} = :timer.tc(fn -> Enum.each(hs, fn {_, _, cn} -> Fathom.Shard.Connection.query(cn, scan, []) end) end); Enum.each(hs, fn {p, rf, cn} -> Fathom.Shard.Connection.close(cn); Fathom.Shard.checkin(p, rf) end); qps = if qus > 0, do: round(op * 1_000_000 / qus), else: 0; IO.puts("opened=#{op} open_rate=#{open_rate} rss_per_shard_kb=#{div(rss - base_rss, max(op, 1))} fds_per_shard=#{Float.round((fds - base_fds) / max(op, 1), 2)} qps=#{qps}")
ELIXIR
)
  expr=${expr//__PER__/$per}; expr=${expr//__ROWS__/$rows}; expr=${expr//__BLOB__/$blob}

  local tmp; tmp=$(mktemp -d)
  for n in "${NODES[@]}"; do ( rpc "$n" "$expr" > "$tmp/$n" 2>&1 ) & done
  wait

  echo ""
  printf "  %-9s %8s %10s %9s %11s\n" node held "KiB/shd" "fds/shd" "q/s scan"
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
  echo "  fleet:  $total data-bearing shards (~${kib} KiB each) held under a live connection at once,"
  echo "          ~$tqps q/s aggregate scanning real rows. Data-bearing served cost = the connection"
  echo "          (~220 KiB) + SQLite page cache for the pages read + the WAL fds — so both RSS and"
  echo "          fds/shard rise vs empty served; still the ACTIVE working set, not total tenants."

  for n in "${NODES[@]}"; do
    rpc "$n" 'Application.put_env(:fathom, :max_open_shards, 10_000); Application.put_env(:fathom, :shard_idle_ms, 20_000); Application.put_env(:fathom, :rebalancer_enabled, true)' >/dev/null
  done
  rm -rf "$tmp"
}

# -- latency-cost: what an injected S3 RTT costs the two round-trip-bound paths ----------------
# `latency <ms>` INJECTS S3 latency (a toxiproxy knob the other scenarios lean on). This MEASURES
# what that latency costs the two paths whose wall-clock is S3-round-trip-bound (docs/benchmark-plan
# "Hot paths"): a **cold-open** (coordinator start → lease acquire + pull the .db from MinIO, ~1 RTT
# overlapped) and a **flush** (checkpoint → upload the .db → release, the durability write path via
# drain/2). It times both per node with NO latency (the loopback-MinIO floor) and again under an
# injected one-way RTT, so the delta isolates the S3 cost from the local work. This is the TPC Phase-4
# follow-on carried in docs/reviews/fleet-density-2026-07-10.md's Remaining Work.
#
# Method per node (node-scoped ids, so no two nodes ever contend for one lease; driven via rpc
# LOCALLY, bypassing the LB — cold-open/flush are per-node properties, not partition ones):
#   setup  : seed `samples` small shards (rows×blob) and drain each → bytes live in MinIO, local dropped.
#   measure: for each, TIME checkout (a genuine cold pull, blocks until the open completes), then a
#            dirty write + WriteCounter.bump (so the flush is write-gated ON — a clean shard skips the
#            upload), then TIME drain (flush upload + drop + release; re-arms the next cold pull).
# Small shards by default (~64 KiB) so the number reflects the RTT, not bulk transfer — raise rows/blob
# to fold in transfer cost. Cold-open should land near ~2× one-way + a few ms (scripts/benchmark_s3_sweep.sh).
cmd_latency_cost() {
  local ms=${1:-30} samples=${2:-12} rows=${3:-64} blob=${4:-1024}
  local ncount=${#NODES[@]}
  local kib=$(( rows * blob / 1024 ))
  echo "latency-cost: cold-open (pull) + flush (upload) cost per node — baseline vs ${ms}ms injected S3 RTT"
  echo "  ($samples shards/node, ~${kib} KiB each; cold-open ≈ ~2× one-way + a few ms — scripts/benchmark_s3_sweep.sh)"

  # Freeze idle-drop + quiet the rebalancer for the run so nothing drops or moves a shard mid-measure
  # (we drain explicitly; this is belt-and-suspenders). Restored at the end. Sample count is tiny, so
  # the production max_open_shards cap is never in play — left untouched.
  local n
  for n in "${NODES[@]}"; do
    rpc "$n" 'Application.put_env(:fathom, :shard_idle_ms, 900_000); Application.put_env(:fathom, :rebalancer_enabled, false)' >/dev/null
  done

  # SETUP (untimed, no latency): create the MinIO object each measure round will pull.
  local setup_expr
setup_expr=$(cat <<'ELIXIR'
pref = "lat" <> String.replace(Atom.to_string(node()), ~r/[^a-z0-9]/, "") <> "_"; seed = "WITH RECURSIVE c(x) AS (SELECT 1 UNION ALL SELECT x + 1 FROM c WHERE x < __ROWS__) INSERT INTO t (b) SELECT randomblob(__BLOB__) FROM c"; Enum.each(1..__N__, fn i -> id = pref <> Integer.to_string(i); {:ok, p, rf, pa} = Fathom.Shards.checkout(id); {:ok, cn} = Fathom.Shard.Connection.open(pa); Fathom.Shard.Connection.exec(cn, "DROP TABLE IF EXISTS t"); Fathom.Shard.Connection.exec(cn, "CREATE TABLE t (id INTEGER PRIMARY KEY, b BLOB)"); Fathom.Shard.Connection.exec(cn, seed); Fathom.Shard.Connection.close(cn); Fathom.Shard.WriteCounter.bump(id); Fathom.Shard.checkin(p, rf); Fathom.Shards.drain(id, 15_000) end); IO.puts("setup_ok=__N__")
ELIXIR
)
  setup_expr=${setup_expr//__N__/$samples}; setup_expr=${setup_expr//__ROWS__/$rows}; setup_expr=${setup_expr//__BLOB__/$blob}

  # MEASURE (timed): cold pull, then a bump-dirtied write, then drain — μs; median/min/max per node.
  local measure_expr
measure_expr=$(cat <<'ELIXIR'
pref = "lat" <> String.replace(Atom.to_string(node()), ~r/[^a-z0-9]/, "") <> "_"; {opens, flushes} = Enum.reduce(1..__N__, {[], []}, fn i, {os, fs} -> id = pref <> Integer.to_string(i); {ou, res} = :timer.tc(fn -> Fathom.Shards.checkout(id) end); case res do {:ok, p, rf, pa} -> {:ok, cn} = Fathom.Shard.Connection.open(pa); Fathom.Shard.Connection.exec(cn, "INSERT INTO t (b) VALUES (randomblob(__BLOB__))"); Fathom.Shard.Connection.close(cn); Fathom.Shard.WriteCounter.bump(id); Fathom.Shard.checkin(p, rf); {fu, _} = :timer.tc(fn -> Fathom.Shards.drain(id, 15_000) end); {[ou | os], [fu | fs]}; _ -> {os, fs} end end); p50 = fn l -> case Enum.sort(l) do [] -> 0; s -> Enum.at(s, div(length(s), 2)) end end; lo = fn l -> if l == [], do: 0, else: Enum.min(l) end; hi = fn l -> if l == [], do: 0, else: Enum.max(l) end; IO.puts("samples=#{length(opens)} open_p50=#{p50.(opens)} open_min=#{lo.(opens)} open_max=#{hi.(opens)} flush_p50=#{p50.(flushes)} flush_min=#{lo.(flushes)} flush_max=#{hi.(flushes)}")
ELIXIR
)
  measure_expr=${measure_expr//__N__/$samples}; measure_expr=${measure_expr//__BLOB__/$blob}

  echo ""
  echo "  setup: seeding $samples shards/node into MinIO (untimed) ..."
  local tmp; tmp=$(mktemp -d)
  for n in "${NODES[@]}"; do ( rpc "$n" "$setup_expr" > "$tmp/setup-$n" 2>&1 ) & done
  wait

  # baseline: no injected latency (loopback-MinIO floor)
  cmd_latency clear >/dev/null
  for n in "${NODES[@]}"; do ( rpc "$n" "$measure_expr" > "$tmp/base-$n" 2>&1 ) & done
  wait

  # injected: the requested one-way S3 RTT
  cmd_latency "$ms" >/dev/null
  for n in "${NODES[@]}"; do ( rpc "$n" "$measure_expr" > "$tmp/inj-$n" 2>&1 ) & done
  wait
  cmd_latency clear >/dev/null

  _lc_field() { printf '%s' "$1" | grep -o "$2=[0-9]*" | cut -d= -f2; }
  _lc_ms()    { awk -v x="${1:-0}" 'BEGIN{printf "%.1f", x/1000}'; }
  _lc_row() { # <label-file-prefix> -> print one table row per node, echo fleet sums via globals
    local phase=$1 n line held op omin omax fp fmin fmax
    for n in "${NODES[@]}"; do
      line=$(cat "$tmp/$phase-$n")
      held=$(_lc_field "$line" samples); held=${held:-0}
      if [ "$held" = "0" ]; then printf "  %-9s %8s  %s\n" "$n" "ERR" "$line"; continue; fi
      op=$(_lc_field "$line" open_p50); omin=$(_lc_field "$line" open_min); omax=$(_lc_field "$line" open_max)
      fp=$(_lc_field "$line" flush_p50); fmin=$(_lc_field "$line" flush_min); fmax=$(_lc_field "$line" flush_max)
      printf "  %-9s %8d %10s %14s %10s %14s\n" "$n" "$held" \
        "$(_lc_ms "$op")" "$(_lc_ms "$omin")–$(_lc_ms "$omax")" \
        "$(_lc_ms "$fp")" "$(_lc_ms "$fmin")–$(_lc_ms "$fmax")"
      _LC_OP_SUM=$(( _LC_OP_SUM + op )); _LC_FP_SUM=$(( _LC_FP_SUM + fp )); _LC_CNT=$(( _LC_CNT + 1 ))
    done
  }

  echo ""
  echo "  --- baseline (no injected latency — the loopback-MinIO floor) ---"
  printf "  %-9s %8s %10s %14s %10s %14s\n" node samples "open p50" "open rng(ms)" "flush p50" "flush rng(ms)"
  _LC_OP_SUM=0 _LC_FP_SUM=0 _LC_CNT=0; _lc_row base
  local base_open=0 base_flush=0
  [ "$_LC_CNT" -gt 0 ] && { base_open=$(( _LC_OP_SUM / _LC_CNT )); base_flush=$(( _LC_FP_SUM / _LC_CNT )); }

  echo ""
  echo "  --- injected (${ms}ms each way on every node's S3 path) ---"
  printf "  %-9s %8s %10s %14s %10s %14s\n" node samples "open p50" "open rng(ms)" "flush p50" "flush rng(ms)"
  _LC_OP_SUM=0 _LC_FP_SUM=0 _LC_CNT=0; _lc_row inj
  local inj_open=0 inj_flush=0
  [ "$_LC_CNT" -gt 0 ] && { inj_open=$(( _LC_OP_SUM / _LC_CNT )); inj_flush=$(( _LC_FP_SUM / _LC_CNT )); }

  echo ""
  local dopen=$(( inj_open - base_open )) dflush=$(( inj_flush - base_flush ))
  echo "  fleet (avg of per-node p50):"
  echo "    cold-open:  baseline $(_lc_ms "$base_open") ms → injected $(_lc_ms "$inj_open") ms  (Δ $(_lc_ms "$dopen") ms ≈ $(awk -v d="$dopen" -v m="$ms" 'BEGIN{printf "%.1f", (m>0)?d/1000.0/m:0}')× the ${ms}ms one-way)"
  echo "    flush:      baseline $(_lc_ms "$base_flush") ms → injected $(_lc_ms "$inj_flush") ms  (Δ $(_lc_ms "$dflush") ms ≈ $(awk -v d="$dflush" -v m="$ms" 'BEGIN{printf "%.1f", (m>0)?d/1000.0/m:0}')× the ${ms}ms one-way)"
  echo "    both paths are round-trip-bound: a cold-open overlaps lease+pull to ~1 RTT (≈2× one-way);"
  echo "    a flush uploads the .db then releases the lease. Injected latency adds RTTs, not local work."

  unset -f _lc_field _lc_ms _lc_row
  for n in "${NODES[@]}"; do
    rpc "$n" 'Application.put_env(:fathom, :shard_idle_ms, 20_000); Application.put_env(:fathom, :rebalancer_enabled, true)' >/dev/null
  done
  rm -rf "$tmp"
}

# -- tpc-fleet: multi-tenant TPC-B THROUGHPUT across the fleet — the throughput-across-nodes angle ----
# The 2026-07-10 tpc-run measured the real-stack *latency shape* (per-txn cost, the single-shard write
# convoy) and flagged that *throughput* isn't a single-shard number. This is the complement: drive many
# tenant shards — **one single-writer file each** (fathom's real model, no intra-shard convoy) — through
# the real LB so they partition across the nodes, sweep the tenant count, and read back BOTH the aggregate
# txn/s (throughput vs concurrency) AND the per-node distribution (the keyspace-partition carrying the
# load). It is to throughput what `chaos.sh density` is to capacity: density proved the fleet holds ~N
# shards ~N/nodes each; this proves the *work* partitions the same way. Honest ceiling: all five nodes
# share ONE 12-vCPU colima VM, so the absolute txn/s is CPU-bound by one box — the horizontal-scaling
# claim is carried by the even per-node split (on N real machines the work is ~N× additive), not by the
# single-host aggregate. Needs SHARD_LOAD on (docker-compose sets it). Each step uses its own shard
# namespace (`tfleet<c>_*`) so no shard is re-seeded; the distribution reads the whole `tfleet` set.
cmd_tpc_fleet() {
  local clients_csv=${1:-"8,16,32,64"} per_client=${2:-400} accounts=${3:-1000}
  local ncount=${#NODES[@]}
  echo "tpc-fleet: multi-tenant TPC-B throughput across $ncount nodes (one single-writer shard per tenant)" >&2
  echo "  sweep tenants=$clients_csv, $per_client txns/tenant, $accounts accounts/tenant — aggregate txn/s + per-node split" >&2

  # Quiet the rebalancer (a mid-run handoff would move a shard off its hash-home and blur the partition we
  # read back) and freeze idle-drop (a dropped coordinator calls ShardLoad.forget, dropping its row from
  # the distribution). Reset the counters so the per-node read is this run's. All restored at the end.
  local n
  for n in "${NODES[@]}"; do
    rpc "$n" 'Application.put_env(:fathom, :rebalancer_enabled, false); Application.put_env(:fathom, :shard_idle_ms, 900_000); Fathom.ShardLoad.reset()' >/dev/null
  done

  tpc_prewarm

  # The cross-LB warm-read floor every statement of every txn pays (context for the per-txn numbers).
  local rtt; rtt=$(tpc_run rtt --domain "$DOMAIN" --shard rttprobe --samples 200 2>/dev/null | jq -r '.rtt_p50_us // 0')
  echo "" >&2
  echo "  cross-LB warm SELECT 1 RTT p50: $(awk -v x="$rtt" 'BEGIN{printf "%.0f", x}')µs (the per-statement network floor)"
  echo ""
  echo "  --- throughput vs tenant concurrency (one writer per tenant, no intra-shard convoy) ---"
  printf "  %-9s %9s %11s %9s %9s %9s %6s\n" tenants txns "txn/s" "p50 ms" "p95 ms" "p99 ms" errs
  local c txns json tps p50 p95 p99 errs
  local IFS=,
  set -f; set -- $clients_csv; set +f   # split the CSV on commas (bash 3.2 safe)
  unset IFS
  for c in "$@"; do
    txns=$(( per_client * c ))
    json=$(tpc_run tpcb --domain "$DOMAIN" --shard "tfleet$c" \
      --txns "$txns" --clients "$c" --accounts "$accounts" 2>/dev/null)
    tps=$(printf '%s' "$json" | jq -r '.tpcb_tps // 0')
    p50=$(printf '%s' "$json" | jq -r '.tpcb_p50_us // 0')
    p95=$(printf '%s' "$json" | jq -r '.tpcb_p95_us // 0')
    p99=$(printf '%s' "$json" | jq -r '.tpcb_p99_us // 0')
    errs=$(printf '%s' "$json" | jq -r '.errors // 0')
    printf "  %-9s %9s %11s %9s %9s %9s %6s\n" "$c" "$txns" "$tps" \
      "$(awk -v x="$p50" 'BEGIN{printf "%.2f", x/1000}')" \
      "$(awk -v x="$p95" 'BEGIN{printf "%.2f", x/1000}')" \
      "$(awk -v x="$p99" 'BEGIN{printf "%.2f", x/1000}')" "$errs"
  done

  # Per-node distribution: how many tenant shards each node held + total query load it absorbed.
  echo ""
  echo "  --- per-node distribution (the LB keyspace-partition carrying the throughput) ---"
  printf "  %-9s %10s %12s\n" node "shards" "queries"
  local dist; dist=$(mktemp)
  local total_shards=0 total_q=0 sh q
  for n in "${NODES[@]}"; do
    read -r sh q <<< "$(rpc "$n" 'ms = Enum.filter(Fathom.ShardLoad.snapshot(), fn m -> String.starts_with?(m.shard_id, "tfleet") end); IO.puts("#{length(ms)} #{Enum.reduce(ms, 0, fn m, a -> a + m.queries end)}")')"
    sh=${sh:-0}; q=${q:-0}
    printf "  %-9s %10s %12s\n" "$n" "$sh" "$q"
    total_shards=$(( total_shards + sh )); total_q=$(( total_q + q ))
    echo "$sh" >> "$dist"
  done
  echo ""
  local minh maxh; minh=$(sort -n "$dist" | head -1); maxh=$(sort -n "$dist" | tail -1)
  echo "  fleet:  $total_shards tenant shards over $ncount nodes ($total_q queries recorded), spread $(awk -v a="$maxh" -v b="$minh" 'BEGIN{printf "%.2f", (b>0)?a/b:0}')x (1.00 = perfect)."
  echo "          Each tenant is one single-writer file on the node the LB hashed it to, so throughput"
  echo "          partitions across the fleet with no cross-tenant contention (contrast the single-shard"
  echo "          convoy in docs/reviews/tpc-run-2026-07-10.md). Absolute txn/s is CPU-bound by this one"
  echo "          12-vCPU VM; on N separate machines the partitioned work is ~N× (the 'millions' angle)."
  rm -f "$dist"

  for n in "${NODES[@]}"; do
    rpc "$n" 'Application.put_env(:fathom, :rebalancer_enabled, true); Application.put_env(:fathom, :shard_idle_ms, 20_000)' >/dev/null
  done
}

# tpcc-fleet: the TPC-C analog of tpc-fleet — N concurrent TENANTS, each its own shard running a
# 1-warehouse TPC-C mix (heavier: value-fed, multi-statement, held streams), swept by tenant count.
# Reads aggregate tpmC + txn/s and the per-node split, like tpc-fleet. This is the multi-tenant path
# (partition across the fleet), NOT the single-shard convoy that `tpcc` (the W-sweep) exercises.
# ELIXIR-ONLY (the python driver has no tpcc-fleet mode), so it forces TPC_DRIVER=elixir; combine
# with TPC_NET=container to drive in-network. Small default --scale keeps each tenant's seed light
# at thousands of shards.
cmd_tpcc_fleet() {
  local clients_csv=${1:-"8,16,32,64"} per_client=${2:-100} scale=${3:-0.005}
  local ncount=${#NODES[@]}
  local TPC_DRIVER=elixir
  echo "tpcc-fleet: multi-tenant TPC-C across $ncount nodes (one 1-warehouse TPC-C shard per tenant, $TPC_NET driver)" >&2
  echo "  sweep tenants=$clients_csv, $per_client txns/tenant, scale $scale — aggregate tpmC + per-node split" >&2

  local n
  for n in "${NODES[@]}"; do
    rpc "$n" 'Application.put_env(:fathom, :rebalancer_enabled, false); Application.put_env(:fathom, :shard_idle_ms, 900_000); Fathom.ShardLoad.reset()' >/dev/null
  done

  tpc_prewarm

  local rtt; rtt=$(tpc_run rtt --domain "$DOMAIN" --shard rttprobe --samples 200 2>/dev/null | jq -r '.rtt_p50_us // 0')
  echo "" >&2
  echo "  cross-LB warm SELECT 1 RTT p50: $(awk -v x="$rtt" 'BEGIN{printf "%.0f", x}')µs (the per-statement network floor)"
  echo ""
  echo "  --- TPC-C throughput vs tenant concurrency (one 1-wh TPC-C per tenant, no cross-tenant contention) ---"
  printf "  %-9s %9s %10s %10s %9s %9s %6s\n" tenants txns "tpmC" "txn/s" "p50 ms" "p99 ms" errs
  local c txns json tpmc tps p50 p99 errs
  local IFS=,
  set -f; set -- $clients_csv; set +f
  unset IFS
  for c in "$@"; do
    txns=$(( per_client * c ))
    json=$(tpc_run tpcc-fleet --domain "$DOMAIN" --shard "tcfleet$c" \
      --txns "$txns" --clients "$c" --scale "$scale" 2>/dev/null)
    tpmc=$(printf '%s' "$json" | jq -r '.tpcc_fleet_tpmc // 0')
    tps=$(printf '%s' "$json" | jq -r '.tpcc_fleet_tps // 0')
    p50=$(printf '%s' "$json" | jq -r '.tpcc_fleet_p50_us // 0')
    p99=$(printf '%s' "$json" | jq -r '.tpcc_fleet_p99_us // 0')
    errs=$(printf '%s' "$json" | jq -r '.errors // 0')
    printf "  %-9s %9s %10s %10s %9s %9s %6s\n" "$c" "$txns" "$tpmc" "$tps" \
      "$(awk -v x="$p50" 'BEGIN{printf "%.2f", x/1000}')" \
      "$(awk -v x="$p99" 'BEGIN{printf "%.2f", x/1000}')" "$errs"
  done

  echo ""
  echo "  --- per-node distribution (the LB keyspace-partition carrying the TPC-C load) ---"
  printf "  %-9s %10s %12s\n" node "shards" "queries"
  local dist; dist=$(mktemp)
  local total_shards=0 total_q=0 sh q
  for n in "${NODES[@]}"; do
    read -r sh q <<< "$(rpc "$n" 'ms = Enum.filter(Fathom.ShardLoad.snapshot(), fn m -> String.starts_with?(m.shard_id, "tcfleet") end); IO.puts("#{length(ms)} #{Enum.reduce(ms, 0, fn m, a -> a + m.queries end)}")')"
    sh=${sh:-0}; q=${q:-0}
    printf "  %-9s %10s %12s\n" "$n" "$sh" "$q"
    total_shards=$(( total_shards + sh )); total_q=$(( total_q + q ))
    echo "$sh" >> "$dist"
  done
  echo ""
  local minh maxh; minh=$(sort -n "$dist" | head -1); maxh=$(sort -n "$dist" | tail -1)
  echo "  fleet:  $total_shards TPC-C tenant shards over $ncount nodes ($total_q queries recorded), spread $(awk -v a="$maxh" -v b="$minh" 'BEGIN{printf "%.2f", (b>0)?a/b:0}')x (1.00 = perfect)."
  echo "          Each tenant is its own 1-warehouse TPC-C file on the node the LB hashed it to, so the"
  echo "          value-fed multi-statement load partitions across the fleet — the multi-tenant path, not"
  echo "          the single-shard convoy that \`tpcc\` (the W-sweep) exercises. Absolute is CPU-bound by"
  echo "          this one 12-vCPU VM; on N machines the partitioned work is ~N×."
  rm -f "$dist"

  for n in "${NODES[@]}"; do
    rpc "$n" 'Application.put_env(:fathom, :rebalancer_enabled, true); Application.put_env(:fathom, :shard_idle_ms, 20_000)' >/dev/null
  done
}

# -- rollout: fleet SCHEMA-MIGRATION throughput — the second half of expert review 2026-08-01 #43.
#
# The first half put `rate_per_hour` + `eta_seconds` on `Migrator.status/0` so an operator can SEE
# the rollout move. This is the half that gives `:reconcile_batch_size` an evidence base: seed N
# tenants at v1, release v2, run the sweep, and measure what the engine actually converges at —
# shards/s wall-clock, the rate the gate reports, and which node did the work.
#
# Why this cannot be an in-process benchmark: `copy_keystone_rows_per_s` measures ONE shard's copy
# loop. Fleet rollout throughput is a different quantity — Oban's `migrations: 10` per node × 5
# nodes contending on one Postgres, each job doing its own S3 pull + replay + fenced flush + cutover.
# Only the rig has all of that at once.
#
# The per-node split is read from `[:fathom, :migrator, :shard_migrated]`, this run's other subject:
# the rig is the event's first real consumer, and summing it against the shards that actually moved
# is what proves it fires once per migration rather than once per ATTEMPT (a retry-inflated counter
# would read as throughput the fleet never had — the exact failure the telemetry half exists to
# avoid). Cross-checked against the directory, which is the independent witness.
cmd_rollout() {
  local tenants=${1:-200} limit=${2:-0} timeout_s=${3:-600}
  [ "$limit" -gt 0 ] 2>/dev/null || limit=$tenants
  local ncount=${#NODES[@]} n i t

  echo "rollout: fleet schema-migration throughput — $tenants tenants across $ncount nodes"
  echo ""

  # -- 0. Preflight: prove THIS image carries the code under test ---------------------------------
  # `./chaos.sh up` does NOT build (cmd_up is `compose up -d`), so the rig will happily run a
  # weeks-old image and report a confident number for code that is not the code under test. On
  # 2026-08-02 both `smoke` and `deploy` passed that way. Assert the change's own observable before
  # anything else: no rate_per_hour ⇒ the image predates the first half and every number below
  # would be meaningless.
  local probe
  probe=$(rpc fathom1 'IO.puts(if Map.has_key?(Fathom.Migrator.status(), :rate_per_hour), do: "yes", else: "no")' | head -1)
  if [ "$probe" != "yes" ]; then
    echo "ABORT: Migrator.status/0 has no :rate_per_hour on this image."
    echo "       That means the image predates the change under test. 'up' does not build:"
    echo "         ./chaos.sh build && ./chaos.sh up"
    return 1
  fi
  echo "preflight: status/0 carries rate_per_hour + eta_seconds on this image ✓"

  # -- 1. Quiet everything that would migrate a shard the sweep did not enqueue -------------------
  # migrate-on-touch defaults :off, but the rig is a shared box and an env var can turn it on;
  # forcing it makes this run's numbers attributable to the sweep alone. The rebalancer would move
  # a shard mid-migration (safe — the lease fences it — but it blurs the per-node split we read).
  # Idle-drop stays at rig default: a migration drains the coordinator anyway.
  for n in "${NODES[@]}"; do
    rpc "$n" 'Application.put_env(:fathom, :migrate_on_touch, :off); Application.put_env(:fathom, :rebalancer_enabled, false)' >/dev/null
  done

  # -- 1b. Clear OTHER steps' shards out of the directory -----------------------------------------
  # `Migrator.rollout/1` sweeps every active shard below HEAD, so `smoke`'s five tenants (and any
  # other step's leftovers) get enqueued alongside ours — and they have no `app_thing` table, so
  # they fail the replay and quarantine. Five bogus `migration_failed` rows in the output of a
  # throughput run is noise that reads like a real failure.
  #
  # They also never converge, which puts a permanent floor under `status/0`: the first trial run of
  # this step sat at `laggards=5` and `eta=1500s` forever, and its wall-clock "throughput" was the
  # timeout rather than the work. That is `status/0` reporting correctly on a fleet that genuinely
  # has five shards nothing will ever migrate; it is the RIG that is wrong, not the gate.
  #
  # Deleting the row is rig hygiene, the same contract as `seed()` truncating `kv`: every run starts
  # fresh. It is safe here and ONLY here — a directory row is re-created at v0 by `resolve/1` on the
  # next touch, and the shard FILE is untouched. Do not copy this into production tooling; the
  # supported way to remove a tenant is `Fathom.Tenants.delete/1`, which tombstones.
  #
  # ALL rows, including this step's own from a previous run. The first version excluded `roll%` to
  # keep "our" tenants, which was backwards: a prior run's tenants sit at ITS target, one version
  # below the new HEAD, so they are laggards too — the run started at `laggards=24` for 12 tenants
  # and the progress query could have counted them. Seeding re-registers every tenant we need.
  local cleared
  cleared=$(rpc fathom1 'IO.puts(elem(Fathom.Repo.delete_all(Fathom.Directory.Shard), 0))' | head -1)
  is_num "$cleared" || cleared=0
  echo "rig hygiene: cleared $cleared directory rows (leftovers are laggards, get swept in, and quarantine)"

  # -- 2. Per-node counter on the event under test ------------------------------------------------
  # :counters in :persistent_term, not ETS: the handler outlives the `rpc` process that attached it
  # (an ETS table owned by that process would vanish the moment rpc returns, and the run would
  # report zero migrations on every node).
  for n in "${NODES[@]}"; do
    rpc "$n" '
      :telemetry.detach("rig-rollout")
      ref = :counters.new(1, [:write_concurrency])
      :persistent_term.put(:rig_rollout_ctr, ref)
      :telemetry.attach("rig-rollout", [:fathom, :migrator, :shard_migrated],
        fn _e, _m, _meta, _c -> :counters.add(ref, 1, 1) end, nil)' >/dev/null
  done

  # -- 3. Seed N tenants at the CURRENT head ------------------------------------------------------
  # Version numbers are computed, never hardcoded: the release table is durable across rig runs, so
  # a second `release(2, ...)` collides on the unique version, HEAD stays put, and the sweep enqueues
  # nothing — a run that looks instant and measured nothing. Seed at HEAD, release HEAD+1.
  local base; base=$(rpc fathom1 'IO.puts(Fathom.Migrator.head())' | head -1)
  is_num "$base" || base=0
  local target=$(( base + 1 ))

  # Through the LB, so each tenant is born on the node the consistent hash gives it — the same
  # partition the rollout then has to work across. `PRAGMA user_version` is the shard file's own
  # stamp (one of the three the migration engine keeps in agreement); the directory's copy is set
  # by cutover below. Ids carry no underscore: `_` is a single-character wildcard in SQL LIKE, and
  # the progress query below matches on `roll%`.
  echo "seeding $tenants tenants at v$base through the LB (target v$target) ..."
  local seed_start; seed_start=$(now_ms)
  local workers=12 w
  for w in $(seq 1 "$workers"); do
    ( local j=$w
      while [ "$j" -le "$tenants" ]; do
        t="roll$j"
        sql "$t" "CREATE TABLE IF NOT EXISTS app_thing (id INTEGER PRIMARY KEY, name TEXT)" >/dev/null 2>&1
        sql "$t" "CREATE TABLE IF NOT EXISTS django_migrations (id INTEGER PRIMARY KEY, app TEXT, name TEXT, applied TEXT)" >/dev/null 2>&1
        sql "$t" "INSERT INTO django_migrations (app, name, applied) VALUES ('app', '0001', 'now')" >/dev/null 2>&1
        sql "$t" "PRAGMA user_version = $base" >/dev/null 2>&1
        j=$(( j + workers ))
      done ) &
  done
  wait
  echo "  seeded in $(( ($(now_ms) - seed_start) / 1000 ))s"

  # The migration pulls each shard's LIVE OBJECT from MinIO, not the owner's local file, so the
  # seeded bytes have to reach storage before the sweep starts. Flush on every node: the non-owners
  # no-op. Without this the copy replays onto a stale file and every shard quarantines.
  echo "flushing v$base to storage ..."
  for n in "${NODES[@]}"; do
    rpc "$n" "Enum.each(1..$tenants, fn i -> Fathom.Shards.flush(\"roll#{i}\") end)" >/dev/null 2>&1 || true
  done

  # Directory to match the file. cutover/2 is the same call the engine uses.
  rpc fathom1 "Enum.each(1..$tenants, fn i -> Fathom.Directory.cutover(\"roll#{i}\", $base) end)" >/dev/null

  # -- 4. Release HEAD+1 and take the pre-rollout reading -----------------------------------------
  # The added column is version-scoped: rerunning the step on the same rig replays a DIFFERENT
  # ALTER, so a second run cannot die on "duplicate column name".
  rpc fathom1 "Fathom.Migrator.release($target, \"rig_v$target\", [\"ALTER TABLE app_thing ADD COLUMN c_v$target TEXT\", \"INSERT INTO django_migrations (app, name, applied) VALUES ('app', 'rig_$target', 'now')\"])" >/dev/null

  # Roll-set progress, the run's own clock. Separate from status/0's fleet-wide laggards on purpose:
  # this counts only the tenants THIS run seeded, so the throughput number is the work we asked for.
  _rdone() {
    rpc fathom1 "
      import Ecto.Query
      IO.puts(Fathom.Repo.aggregate(from(s in Fathom.Directory.Shard,
        where: s.status == \"active\" and s.schema_version == $target and like(s.shard_id, \"roll%\")), :count))" | head -1
  }
  echo ""
  _rstatus() {
    rpc fathom1 's = Fathom.Migrator.status(); IO.puts("#{s.head} #{s.laggards} #{s.failed} #{s.rate_per_hour} #{s.eta_seconds || -1}")' | head -1
  }
  local h0 l0 f0 r0 e0
  read -r h0 l0 f0 r0 e0 <<< "$(_rstatus)"
  echo "  pre-rollout:  head=$h0 laggards=$l0 failed=$f0 rate=$r0/h eta=$( [ "$e0" = "-1" ] && echo null || echo "${e0}s" )"

  # -- 5. Sweep ------------------------------------------------------------------------------------
  local start_ms; start_ms=$(now_ms)
  local enq; enq=$(rpc fathom1 "{:ok, c} = Fathom.Migrator.rollout($limit); IO.puts(c)" | head -1)
  is_num "$enq" || enq=0
  echo "  enqueued $enq migration jobs (Oban migrations queue, 10/node × $ncount nodes)"
  echo ""
  printf "  %8s %10s %9s %8s %11s %10s\n" "t+s" "done/$tenants" laggards failed "rate/h" "eta"

  # `work_ms` is the clock that matters: time from the sweep to the LAST shard that actually moved,
  # not the loop's total runtime. Dividing by the loop instead turned a real 13 shards/s into a
  # reported 0.3 — one straggler (a shard with an orphaned lock, see the straggler dump below) held
  # the loop to its 900s timeout while 299 of 300 had finished in 23s. A number that bad is a broken
  # measurement just as surely as a number that good.
  local elapsed_ms=0 h l f r e d converged=0 deadline_ms=$(( timeout_s * 1000 ))
  local last_d=0 work_ms=1
  while [ "$elapsed_ms" -lt "$deadline_ms" ]; do
    sleep 3
    elapsed_ms=$(( $(now_ms) - start_ms ))
    d=$(_rdone); is_num "$d" || continue
    read -r h l f r e <<< "$(_rstatus)"
    is_num "$l" || l=0
    printf "  %8s %10s %9s %8s %11s %10s\n" "$(( elapsed_ms / 1000 ))" "$d" "$l" "$f" "$r" \
      "$( [ "$e" = "-1" ] && echo "null" || echo "${e}s" )"
    if [ "$d" -gt "$last_d" ]; then last_d=$d; work_ms=$elapsed_ms; fi
    if [ "$d" -ge "$tenants" ]; then converged=1; break; fi
  done
  local total_ms=$(( $(now_ms) - start_ms )); [ "$total_ms" -gt 0 ] || total_ms=1
  [ "$work_ms" -gt 0 ] || work_ms=1

  # -- 6. Read back: throughput, the per-node split, and the two independent witnesses -------------
  echo ""
  local migrated; migrated=$(_rdone)
  is_num "$migrated" || migrated=0

  printf "  %-9s %12s\n" node "shard_migrated"
  local dist; dist=$(mktemp)
  local evt_total=0 c
  for n in "${NODES[@]}"; do
    c=$(rpc "$n" 'IO.puts(:counters.get(:persistent_term.get(:rig_rollout_ctr), 1))' | head -1)
    is_num "$c" || c=0
    printf "  %-9s %12s\n" "$n" "$c"
    evt_total=$(( evt_total + c ))
    echo "$c" >> "$dist"
  done
  local minc maxc; minc=$(sort -n "$dist" | head -1); maxc=$(sort -n "$dist" | tail -1)
  rm -f "$dist"

  echo ""
  echo "  throughput: $migrated shards migrated in $(awk -v x="$work_ms" 'BEGIN{printf "%.1f", x/1000}')s of work"
  echo "              = $(awk -v s="$migrated" -v m="$work_ms" 'BEGIN{printf "%.1f", s*1000/m}') shards/s"
  echo "              = ~$(awk -v s="$migrated" -v m="$work_ms" 'BEGIN{printf "%.0f", s*3600000/m}') shards/hour sustained"
  echo "              (vs :reconcile_batch_size default 100/hour — the cron throttle, not the engine)"
  [ "$work_ms" -lt "$total_ms" ] && \
    echo "              [loop ran $(awk -v x="$total_ms" 'BEGIN{printf "%.0f", x/1000}')s total; the tail was idle — see stragglers]"
  # Spread only reads as a number when every node did some work. An idle node is not a broken
  # partition here: migration work is drawn from ONE Oban queue in Postgres, not routed by the LB
  # hash, so with few jobs and 10 slots per node one node can legitimately take them all. That is a
  # different distribution mechanism from `density`/`tpc-fleet`, where the LB hash IS the split.
  if [ "$minc" -gt 0 ]; then
    echo "  split:      $evt_total events across $ncount nodes, min $minc / max $maxc (spread $(awk -v a="$maxc" -v b="$minc" 'BEGIN{printf "%.2f", a/b}')x, 1.00 = perfect)"
  else
    echo "  split:      $evt_total events across $ncount nodes, min $minc / max $maxc — a node drew no jobs"
    echo "              (Oban queue draw, not LB hash: $(( 10 * ncount )) slots fleet-wide, so a small batch can land on one node)"
  fi

  # The event's contract, checked rather than assumed: one per shard that MOVED. A retry-inflated
  # counter reads as throughput the fleet never had, so evt > migrated is a real failure here.
  if [ "$evt_total" -eq "$migrated" ] && [ "$migrated" -gt 0 ]; then
    echo "  telemetry:  PASS — shard_migrated fired exactly once per migrated shard ($evt_total = $migrated)"
  else
    echo "  telemetry:  MISMATCH — $evt_total events vs $migrated shards at v2 (expected equal)"
  fi

  # -- 7. Prove the schema actually changed. The whole run is worthless if the shards did not move;
  # a directory that says v2 over a file that never got the column is precisely the silent
  # corruption the three-place version stamp exists to prevent.
  local ok=0 bad=0
  for i in 1 3 7; do
    [ "$i" -le "$tenants" ] || continue
    if sql "roll$i" "SELECT c_v$target FROM app_thing" >/dev/null 2>&1 &&
       [ "$(sql "roll$i" "PRAGMA user_version" 2>/dev/null | val)" = "$target" ]; then
      ok=$(( ok + 1 ))
    else
      bad=$(( bad + 1 ))
    fi
  done
  echo "  schema:     $ok/$(( ok + bad )) sampled shards serve the v$target column at user_version=$target"

  if [ "$converged" -eq 1 ]; then
    echo "  verdict:    CONVERGED — all $tenants tenants reached v$target, failed=${f:-0}"
  else
    echo "  verdict:    TIMEOUT after ${timeout_s}s — $migrated/$tenants at v$target, failed=${f:-?}"
    echo ""
    # Name the stragglers AND their lock holder. Without this the operator sees "1 laggard, 0
    # failed" and has nothing to go on: the job is `scheduled`, not `retryable`, because a held
    # lease returns {:retry, _} -> {:snooze, 5}, which does not burn an attempt and never surfaces
    # as an error. The 2026-08-04 run needed a hand-written Ecto query to learn that one shard's
    # lease was held by a coordinator that no longer existed — that diagnosis belongs in the tool.
    echo "  stragglers (shard, directory version, lease holder):"
    rpc fathom1 "
      import Ecto.Query
      from(s in Fathom.Directory.Shard,
        where: s.status == \"active\" and s.schema_version < $target and like(s.shard_id, \"roll%\"),
        select: {s.shard_id, s.schema_version}, limit: 10)
      |> Fathom.Repo.all()
      |> Enum.each(fn {id, v} ->
        IO.puts(\"    #{id}  v#{v}  #{inspect(Fathom.Shard.Storage.lease_holder(id))}\")
      end)"
    echo "  note: a lease held by a LIVE node's coordinator never expires (liveness is the node's"
    echo "        heartbeat), so if that coordinator is already gone the migration snoozes forever."
    echo "        A client touch on the owning node reclaims + releases it and the next retry lands."
  fi

  # -- 8. Restore rig defaults --------------------------------------------------------------------
  for n in "${NODES[@]}"; do
    rpc "$n" ':telemetry.detach("rig-rollout"); Application.put_env(:fathom, :rebalancer_enabled, true)' >/dev/null
  done
  unset -f _rstatus _rdone
  [ "$converged" -eq 1 ]
}

# cmd_deploy — the clean-shutdown (rolling-deploy) verification: the herd analog of `failover`.
# `failover` SIGKILLs a node and measures the loss window; `deploy` SIGTERMs a node holding a HIGH
# open-shard count and proves the graceful path flushes EVERY open shard (zero loss) — the property
# a node-by-node upgrade relies on (docs/runbooks/deploy.md). We hold N shards open AND dirty by
# disabling idle-drop + the periodic flush on the target, so the ONLY thing that can persist their
# writes is the graceful-terminate flush. Then `docker stop` (SIGTERM -> BEAM init:stop -> supervised
# terminate -> each coordinator flushes within :shard_shutdown_ms) and verify every committed row
# survived via a survivor's cold-open from S3 (clean shutdown also RELEASES the leases, so a survivor
# acquires immediately — no TTL wait).
cmd_deploy() {
  local shards=${1:-200} node=${2:-fathom1}
  echo "clean-shutdown (deploy) verification: hold $shards open+dirty shards on $node, then SIGTERM"

  # Keep every seeded shard open AND dirty through the run: only the graceful terminate may flush.
  rpc "$node" 'Application.put_env(:fathom, :shard_idle_ms, 3_600_000); Application.put_env(:fathom, :shard_flush_interval_ms, 3_600_000); Application.put_env(:fathom, :max_open_shards, 100_000)' >/dev/null

  echo "seeding $shards shards directly on $node (each a committed seq=999) ..."
  local i s
  for i in $(seq 1 "$shards"); do
    s="deploy_${i}"
    sql_direct "$node" "$s" "CREATE TABLE IF NOT EXISTS kv (id INTEGER PRIMARY KEY AUTOINCREMENT, seq INTEGER)" >/dev/null 2>&1
    sql_direct "$node" "$s" "INSERT INTO kv (seq) VALUES (999)" >/dev/null 2>&1
  done

  local held; held=$(rpc "$node" 'IO.puts(Registry.count(Fathom.ShardRegistry))' | tr -dc 0-9)
  echo "  $node holds $held open coordinators (idle-drop + periodic flush disabled ⇒ all dirty)"

  local t0 t1
  t0=$(now_ms)
  "$DOCKER" stop -t 120 "$(cname "$node")" >/dev/null   # -t 120 so docker never premature-SIGKILLs
  t1=$(now_ms)
  echo "RESULT clean-shutdown: $((t1 - t0)) ms to graceful stop with $held open dirty shards ($(( (t1 - t0) / (held > 0 ? held : 1) )) ms/shard)"

  echo "verifying survival of all $shards shards via a survivor cold-open from S3 ..."
  local survivor lost=0 ok=0 got
  survivor=$(other_node "$node")
  for i in $(seq 1 "$shards"); do
    s="deploy_${i}"
    got=$(sql_direct "$survivor" "$s" "SELECT seq FROM kv LIMIT 1" 2>/dev/null | val 2>/dev/null)
    if [ "$got" = "999" ]; then ok=$((ok + 1)); else lost=$((lost + 1)); fi
  done
  echo "  survived: $ok / $shards   lost: $lost"
  if [ "$lost" -eq 0 ]; then
    echo "PASS: clean shutdown flushed every open dirty shard — zero loss (contrast: failover/kill loses unflushed)"
  else
    echo "FAIL: $lost shard(s) lost committed writes on a CLEAN shutdown — investigate :shard_shutdown_ms vs flush time"
  fi

  revive "$node"
}

# cmd_failover_herd — the failover HERD at density (expert review #39). `failover` measures ONE
# shard's re-home time; a real node death re-homes ALL its tenants onto survivors at once. This seeds
# N shards on a node, kills it, then fires all N requests through the LB CONCURRENTLY and reports the
# time-to-served distribution (p50/p90/p99/max + served/N) — the herd. `warm=off` disables the
# survivors' warm follower + clears their cache so every re-home cold-opens (full pull); `warm=on`
# (default) lets them 304-promote the pre-warmed copy. The floor is the lease TTL + steal margin
# (~15s here): a silent-killed owner's shards are unstealable until its heartbeat lapses.
cmd_failover_herd() {
  local shards=${1:-300} warm=${2:-on} node=fathom1
  echo "failover-herd: $shards shards on $node, warm=$warm — kill it, measure time-to-served across the herd"

  rpc "$node" 'Application.put_env(:fathom, :shard_idle_ms, 600_000); Application.put_env(:fathom, :max_open_shards, 100_000)' >/dev/null

  echo "seeding $shards shards on $node ..."
  local i s
  for i in $(seq 1 "$shards"); do
    s="herd_${i}"
    sql_direct "$node" "$s" "CREATE TABLE IF NOT EXISTS kv (id INTEGER PRIMARY KEY AUTOINCREMENT, seq INTEGER)" >/dev/null 2>&1
    sql_direct "$node" "$s" "INSERT INTO kv (seq) VALUES (1)" >/dev/null 2>&1
  done
  echo "waiting for a durable flush (a kill must lose nothing) ..."
  sleep 8

  local sv
  if [ "$warm" = "on" ]; then
    echo "warm=on: waiting so survivors pre-warm $node's shards (2 poll cycles) ..."
    sleep 6
  else
    echo "warm=off: disabling the follower + clearing warm caches on survivors (force cold-open) ..."
    for sv in "${NODES[@]}"; do
      [ "$sv" = "$node" ] && continue
      rpc "$sv" 'Application.put_env(:fathom, :warm_follower, false)' >/dev/null
      compose exec -T "$sv" sh -c 'rm -rf /data/warm/* 2>/dev/null' || true
    done
  fi

  local tmp; tmp=$(mktemp -d)
  echo "killing $node; firing $shards concurrent requests through the LB ..."
  local t0; t0=$(now_ms)
  silent_kill "$node"

  for i in $(seq 1 "$shards"); do
    (
      s="herd_${i}"
      until sql "$s" "SELECT seq FROM kv LIMIT 1" >/dev/null 2>&1; do sleep 0.05; done
      echo $(($(now_ms) - t0)) >"$tmp/$i"
    ) &
    # Bound the fork fan-out in waves of 150 so the driver host isn't the bottleneck.
    if [ $((i % 150)) -eq 0 ]; then wait; fi
  done
  wait

  /usr/bin/python3 - "$tmp" "$shards" <<'PY'
import sys, os, glob
tmp, n = sys.argv[1], int(sys.argv[2])
vals = sorted(int(open(f).read().strip()) for f in glob.glob(os.path.join(tmp, '*')) if open(f).read().strip())
def pct(p):
    return vals[min(len(vals) - 1, int(p / 100 * len(vals)))] if vals else 0
print(f"RESULT failover-herd: served {len(vals)}/{n}  p50={pct(50)}ms  p90={pct(90)}ms  p99={pct(99)}ms  max={max(vals) if vals else 0}ms")
print("  (floor ~= lease TTL + steal margin; the spread above it is the concurrent cold-open/pool cost)")
PY
  rm -rf "$tmp"

  revive "$node"
}

case "${1:-}" in
  build)       cmd_build ;;
  build-driver) cmd_build_driver ;;
  failover-herd) shift; cmd_failover_herd "$@" ;;
  deploy)      shift; cmd_deploy "$@" ;;
  tpcb)        shift; cmd_tpcb "$@" ;;
  tpcc)        shift; cmd_tpcc "$@" ;;
  rollout)     shift; cmd_rollout "$@" ;;
  density)     shift; cmd_density "$@" ;;
  served)      shift; cmd_served "$@" ;;
  served-data) shift; cmd_served_data "$@" ;;
  latency-cost) shift; cmd_latency_cost "$@" ;;
  tpc-fleet)   shift; cmd_tpc_fleet "$@" ;;
  tpcc-fleet)  shift; cmd_tpcc_fleet "$@" ;;
  rebalance)   shift; cmd_rebalance "$@" ;;
  hotspots)    shift; cmd_hotspots "$@" ;;
  replication) shift; cmd_replication "$@" ;;
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
