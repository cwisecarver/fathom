#!/usr/bin/env bash
# Run the S3 cold-open bench against MinIO with INJECTED network latency/bandwidth,
# so cold_open_s3 reflects realistic S3 conditions without a real AWS account.
#
# How: a toxiproxy container sits between the bench and MinIO and adds a latency
# toxic (and optional bandwidth cap) to the real S3 byte stream. This shapes the
# actual S3 protocol path — TTFB + RTT + transfer — not a fake constant. It is NOT
# real S3 (no AWS), but it's a far better proxy for it than loopback MinIO.
#
#   scripts/benchmark_s3_latency.sh                 # default ~30ms latency
#   S3_FAKE_LATENCY_MS=60 scripts/benchmark_s3_latency.sh
#   S3_FAKE_LATENCY_MS=25 S3_FAKE_RATE_KBPS=100000 scripts/benchmark_s3_latency.sh
#
# Env:
#   S3_FAKE_LATENCY_MS   one-way downstream delay added to the S3 byte stream (default 30;
#                        models S3 first-byte/RTT — the dominant cold-open cost)
#   S3_FAKE_JITTER_MS    jitter on the latency (default 5)
#   S3_FAKE_RATE_KBPS    downstream bandwidth cap in KB/s (default 0 = unlimited; set
#                        e.g. 100000 ≈ 800 Mbps single-stream, or higher for 10GbE+)
#   S3_BENCH_SAMPLES     cold_open_s3 samples (default 15)
set -uo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

# docker may live in the Homebrew Cellar rather than on PATH (per minio_test.sh).
if ! command -v docker >/dev/null 2>&1; then
  for d in /opt/homebrew/Cellar/docker/*/bin; do [ -d "$d" ] && PATH="$d:$PATH"; done
  export PATH
fi

NET=fathom-bench-net
MINIO=fathom-minio-lat
TOXI=fathom-toxiproxy
BUCKET="${FATHOM_S3_TEST_BUCKET:-fathom-shards-test}"
AK="${FATHOM_S3_TEST_ACCESS_KEY:-fathomtest}"
SK="${FATHOM_S3_TEST_SECRET_KEY:-fathomtest123}"
MINIO_HOST_PORT=9100      # published for bucket creation (no latency)
PROXY_HOST_PORT=9110      # the bench connects here (latency-shaped)
TOXI_API_PORT=8474

LATENCY_MS="${S3_FAKE_LATENCY_MS:-30}"
JITTER_MS="${S3_FAKE_JITTER_MS:-5}"
RATE_KBPS="${S3_FAKE_RATE_KBPS:-0}"
SAMPLES="${S3_BENCH_SAMPLES:-15}"

cleanup() {
  docker rm -f "$MINIO" "$TOXI" >/dev/null 2>&1 || true
  docker network rm "$NET" >/dev/null 2>&1 || true
}
trap cleanup EXIT
cleanup

docker network create "$NET" >/dev/null

echo "=== starting MinIO + toxiproxy ==="
docker run -d --name "$MINIO" --network "$NET" -p "${MINIO_HOST_PORT}:9000" \
  -e MINIO_ROOT_USER="$AK" -e MINIO_ROOT_PASSWORD="$SK" \
  minio/minio:latest server /data >/dev/null

docker run -d --name "$TOXI" --network "$NET" \
  -p "${PROXY_HOST_PORT}:${PROXY_HOST_PORT}" -p "${TOXI_API_PORT}:8474" \
  ghcr.io/shopify/toxiproxy:latest >/dev/null

echo "    waiting for MinIO ..."
for _ in $(seq 1 40); do
  curl -fsS "http://localhost:${MINIO_HOST_PORT}/minio/health/live" >/dev/null 2>&1 && break
  sleep 0.5
done
echo "    waiting for toxiproxy API ..."
for _ in $(seq 1 40); do
  curl -fsS "http://localhost:${TOXI_API_PORT}/version" >/dev/null 2>&1 && break
  sleep 0.5
done

# Create the bucket directly against MinIO (no latency on this control step).
ACF="$(mktemp)"; printf '[default]\ns3 =\n    addressing_style = path\n' > "$ACF"
AWS_CONFIG_FILE="$ACF" AWS_ACCESS_KEY_ID="$AK" AWS_SECRET_ACCESS_KEY="$SK" AWS_DEFAULT_REGION=us-east-1 \
  aws --endpoint-url "http://localhost:${MINIO_HOST_PORT}" s3 mb "s3://${BUCKET}" 2>&1 | tail -1
rm -f "$ACF"

echo "=== configuring toxiproxy: ${LATENCY_MS}ms latency (jitter ${JITTER_MS}ms)$([ "$RATE_KBPS" != 0 ] && echo ", ${RATE_KBPS} KB/s cap") ==="
# Proxy localhost:9110 -> minio:9000, on the docker network.
curl -fsS -X POST "http://localhost:${TOXI_API_PORT}/proxies" -d "{
  \"name\":\"minio\",\"listen\":\"0.0.0.0:${PROXY_HOST_PORT}\",\"upstream\":\"${MINIO}:9000\",\"enabled\":true
}" >/dev/null
# Latency on both streams models request RTT + response TTFB.
curl -fsS -X POST "http://localhost:${TOXI_API_PORT}/proxies/minio/toxics" -d "{
  \"name\":\"lat_down\",\"type\":\"latency\",\"stream\":\"downstream\",\"attributes\":{\"latency\":${LATENCY_MS},\"jitter\":${JITTER_MS}}
}" >/dev/null
curl -fsS -X POST "http://localhost:${TOXI_API_PORT}/proxies/minio/toxics" -d "{
  \"name\":\"lat_up\",\"type\":\"latency\",\"stream\":\"upstream\",\"attributes\":{\"latency\":${LATENCY_MS},\"jitter\":${JITTER_MS}}
}" >/dev/null
if [ "$RATE_KBPS" != 0 ]; then
  curl -fsS -X POST "http://localhost:${TOXI_API_PORT}/proxies/minio/toxics" -d "{
    \"name\":\"bw_down\",\"type\":\"bandwidth\",\"stream\":\"downstream\",\"attributes\":{\"rate\":${RATE_KBPS}}
  }" >/dev/null
fi

# S3_BENCH_ONLY picks which S3 metric(s) to run (default cold_open_s3; e.g. warm_s3).
# Extra args ($@) pass through to mix fathom.bench (e.g. --warm-shards 500).
echo "=== running ${S3_BENCH_ONLY:-cold_open_s3} through the latency proxy ==="
FATHOM_S3_TEST_ENDPOINT="http://localhost:${PROXY_HOST_PORT}" \
FATHOM_S3_TEST_BUCKET="$BUCKET" \
FATHOM_S3_TEST_ACCESS_KEY="$AK" FATHOM_S3_TEST_SECRET_KEY="$SK" \
  mix fathom.bench --only "${S3_BENCH_ONLY:-cold_open_s3}" --cold-open-s3-samples "$SAMPLES" "$@" 2>&1 \
  | grep -vE "\[debug\]|Compiling|Generated" | grep -E "cold_open_s3|warm_s3|skipped|\{" | tail -2

echo ""
echo "NOTE: injected latency models S3 conditions; it is NOT real S3. Each cold-open"
echo "does several S3 round-trips (lease GET + conditional PUT + object GET), so the"
echo "number is roughly that many x the injected one-way latency."
