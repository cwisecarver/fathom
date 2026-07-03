#!/usr/bin/env bash
# Sweep the S3 cold-open bench across several injected latencies, to chart how
# cold-open scales with S3 network latency — without an AWS account. Companion to
# scripts/benchmark_s3_latency.sh; stands up MinIO + toxiproxy ONCE and just
# reconfigures the latency toxic between points (fast), then prints a table.
#
#   scripts/benchmark_s3_sweep.sh
#   S3_SWEEP_LATENCIES="0 20 50 100 150" scripts/benchmark_s3_sweep.sh
#
# Env:
#   S3_SWEEP_LATENCIES  one-way latencies in ms to test (default "0 10 30 60 100").
#                       Each request RTT ≈ 2x this (latency is added both directions).
#   S3_SWEEP_SAMPLES    cold_open_s3 samples per point (default 12)
#
# Reading it: cold-open is ~1 S3 round-trip after the lease/pull optimizations, so
# the result is ~linear in latency (≈ 2x one-way + a few ms). A real region adds
# S3's own server-side TTFB on top of this network model.
set -uo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

if ! command -v docker >/dev/null 2>&1; then
  for d in /opt/homebrew/Cellar/docker/*/bin; do [ -d "$d" ] && PATH="$d:$PATH"; done
  export PATH
fi

NET=fathom-bench-net
MINIO=fathom-minio-sweep
TOXI=fathom-toxiproxy-sweep
BUCKET=fathom-shards-test
AK=fathomtest
SK=fathomtest123
MP=9100
PP=9110
API=8474
LATENCIES="${S3_SWEEP_LATENCIES:-0 10 30 60 100}"
SAMPLES="${S3_SWEEP_SAMPLES:-12}"

cleanup() {
  docker rm -f "$MINIO" "$TOXI" >/dev/null 2>&1 || true
  docker network rm "$NET" >/dev/null 2>&1 || true
}
trap cleanup EXIT
cleanup

docker network create "$NET" >/dev/null
docker run -d --name "$MINIO" --network "$NET" -p "$MP:9000" \
  -e MINIO_ROOT_USER="$AK" -e MINIO_ROOT_PASSWORD="$SK" \
  minio/minio:latest server /data >/dev/null
docker run -d --name "$TOXI" --network "$NET" -p "$PP:$PP" -p "$API:8474" \
  ghcr.io/shopify/toxiproxy:latest >/dev/null

echo "waiting for MinIO + toxiproxy ..."
for _ in $(seq 1 40); do curl -fsS "http://localhost:$MP/minio/health/live" >/dev/null 2>&1 && break; sleep 0.5; done
for _ in $(seq 1 40); do curl -fsS "http://localhost:$API/version" >/dev/null 2>&1 && break; sleep 0.5; done

ACF="$(mktemp)"; printf '[default]\ns3 =\n    addressing_style = path\n' > "$ACF"
AWS_CONFIG_FILE="$ACF" AWS_ACCESS_KEY_ID="$AK" AWS_SECRET_ACCESS_KEY="$SK" AWS_DEFAULT_REGION=us-east-1 \
  aws --endpoint-url "http://localhost:$MP" s3 mb "s3://$BUCKET" >/dev/null 2>&1 || true
rm -f "$ACF"

curl -fsS -X POST "http://localhost:$API/proxies" \
  -d "{\"name\":\"minio\",\"listen\":\"0.0.0.0:$PP\",\"upstream\":\"$MINIO:9000\",\"enabled\":true}" >/dev/null

printf '\n%-14s %s\n' "one_way_ms" "cold_open_s3_ms"
for L in $LATENCIES; do
  curl -fsS -X DELETE "http://localhost:$API/proxies/minio/toxics/lat_down" >/dev/null 2>&1 || true
  curl -fsS -X DELETE "http://localhost:$API/proxies/minio/toxics/lat_up" >/dev/null 2>&1 || true
  if [ "$L" != 0 ]; then
    for stream in downstream upstream; do
      curl -fsS -X POST "http://localhost:$API/proxies/minio/toxics" \
        -d "{\"name\":\"lat_${stream}\",\"type\":\"latency\",\"stream\":\"${stream}\",\"attributes\":{\"latency\":$L,\"jitter\":2}}" >/dev/null
    done
  fi

  MS=$(FATHOM_S3_TEST_ENDPOINT="http://localhost:$PP" FATHOM_S3_TEST_BUCKET="$BUCKET" \
       FATHOM_S3_TEST_ACCESS_KEY="$AK" FATHOM_S3_TEST_SECRET_KEY="$SK" \
       mix fathom.bench --only cold_open_s3 --cold-open-s3-samples "$SAMPLES" 2>/dev/null \
       | grep -E '^\{' | tail -1 \
       | python3 -c "import sys,json; print(round(json.load(sys.stdin)['cold_open_s3_p50_us']/1000,1))" 2>/dev/null || echo "err")
  printf '%-14s %s\n' "$L" "$MS"
done
