#!/usr/bin/env bash
# Stand up an ephemeral MinIO and run the :s3 integration tests against it.
#
#   scripts/minio_test.sh          # start MinIO, create bucket, run tests, tear down
#   scripts/minio_test.sh --keep   # leave MinIO running afterward (for reruns)
#
# MinIO listens on FATHOM_S3_TEST_PORT (default 9100; console on +1). Override the
# bucket/creds with the FATHOM_S3_TEST_* env vars the test reads.
set -euo pipefail

# docker may live in the Homebrew Cellar rather than on PATH.
if ! command -v docker >/dev/null 2>&1; then
  for d in /opt/homebrew/Cellar/docker/*/bin; do [ -d "$d" ] && PATH="$d:$PATH"; done
  export PATH
fi

NAME=fathom-minio-test
API_PORT="${FATHOM_S3_TEST_PORT:-9100}"
CONSOLE_PORT=$((API_PORT + 1))
ENDPOINT="http://localhost:${API_PORT}"
BUCKET="${FATHOM_S3_TEST_BUCKET:-fathom-shards-test}"
ACCESS_KEY="${FATHOM_S3_TEST_ACCESS_KEY:-fathomtest}"
SECRET_KEY="${FATHOM_S3_TEST_SECRET_KEY:-fathomtest123}"

KEEP=0
[ "${1:-}" = "--keep" ] && KEEP=1

cleanup() { [ "$KEEP" = "1" ] || docker rm -f "$NAME" >/dev/null 2>&1 || true; }
trap cleanup EXIT

docker rm -f "$NAME" >/dev/null 2>&1 || true
docker run -d --name "$NAME" \
  -p "${API_PORT}:9000" -p "${CONSOLE_PORT}:9001" \
  -e MINIO_ROOT_USER="$ACCESS_KEY" \
  -e MINIO_ROOT_PASSWORD="$SECRET_KEY" \
  minio/minio:latest server /data --console-address ":9001" >/dev/null

echo "Waiting for MinIO at ${ENDPOINT} ..."
for _ in $(seq 1 40); do
  curl -fsS "${ENDPOINT}/minio/health/live" >/dev/null 2>&1 && break
  sleep 0.5
done

# Create the bucket with path-style addressing, isolated from the user's ~/.aws.
AWS_CONFIG_FILE="$(mktemp)"
printf '[default]\ns3 =\n    addressing_style = path\n' > "$AWS_CONFIG_FILE"
export AWS_CONFIG_FILE
AWS_ACCESS_KEY_ID="$ACCESS_KEY" AWS_SECRET_ACCESS_KEY="$SECRET_KEY" AWS_DEFAULT_REGION=us-east-1 \
  aws --endpoint-url "$ENDPOINT" s3 mb "s3://${BUCKET}" 2>/dev/null || true
rm -f "$AWS_CONFIG_FILE"

export FATHOM_S3_TEST_ENDPOINT="$ENDPOINT"
export FATHOM_S3_TEST_BUCKET="$BUCKET"
export FATHOM_S3_TEST_ACCESS_KEY="$ACCESS_KEY"
export FATHOM_S3_TEST_SECRET_KEY="$SECRET_KEY"

mix test --include s3 test/fathom/shard_storage_s3_test.exs
