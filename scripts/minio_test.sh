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

# WHERE MinIO COMES FROM (rewritten 2026-09-29). Upstream MinIO stopped publishing its community
# Docker images in late 2025: `minio/minio` on Docker Hub now answers "repository does not exist",
# and `quay.io/minio/minio` answers 401 — from a laptop as well as from GitHub runners, so this is
# not a rate limit and no amount of registry juggling brings them back. (This step was red from
# 2026-09-27 for exactly that reason; an earlier comment here blamed Docker Hub rate limits, which
# was the right diagnosis once and stopped being the whole story.)
#
# The replacement is Chainguard's MinIO, rebuilt from source and multi-arch (amd64 + arm64). The
# full :s3 suite (26 tests, including the conditional-write lease fence) passes against it. It is
# MIRRORED into this project's own registry so an upstream policy change cannot break CI again:
#   primary:  ghcr.io/cwisecarver/minio:latest   (mirror; also tagged 2026-09-29)
#   fallback: cgr.dev/chainguard/minio:latest    (the source of the mirror; anonymous pulls)
# Refresh the mirror with:
#   docker buildx imagetools create --tag ghcr.io/cwisecarver/minio:latest \
#     --tag ghcr.io/cwisecarver/minio:$(date +%F) cgr.dev/chainguard/minio:latest
# FATHOM_MINIO_IMAGE / FATHOM_MINIO_IMAGE_FALLBACK override either.
MINIO_IMAGE="${FATHOM_MINIO_IMAGE:-ghcr.io/cwisecarver/minio:latest}"
MINIO_IMAGE_FALLBACK="${FATHOM_MINIO_IMAGE_FALLBACK:-cgr.dev/chainguard/minio:latest}"

KEEP=0
[ "${1:-}" = "--keep" ] && KEEP=1

cleanup() { [ "$KEEP" = "1" ] || docker rm -f "$NAME" >/dev/null 2>&1 || true; }
trap cleanup EXIT

docker rm -f "$NAME" >/dev/null 2>&1 || true

# Resolve the image up front: a registry refusal is a clear message HERE, not a cryptic `docker run`
# exit 125 mid-step (the failure that had this step red on 2026-09-27..29). Try the primary, fall
# back to the other source, fail the step only if BOTH refuse.
if ! docker pull "$MINIO_IMAGE" >/dev/null 2>&1; then
  echo "MinIO pull failed for ${MINIO_IMAGE}; falling back to ${MINIO_IMAGE_FALLBACK}" >&2
  if docker pull "$MINIO_IMAGE_FALLBACK" >/dev/null 2>&1; then
    MINIO_IMAGE="$MINIO_IMAGE_FALLBACK"
  else
    echo "both MinIO registries refused the pull (${MINIO_IMAGE} and ${MINIO_IMAGE_FALLBACK})." >&2
    echo "The :s3 suite cannot run without the image. Add registry auth (docker/login-action) or" >&2
    echo "point FATHOM_MINIO_IMAGE at a mirror the runner can read." >&2
    exit 1
  fi
fi

docker run -d --name "$NAME" \
  -p "${API_PORT}:9000" -p "${CONSOLE_PORT}:9001" \
  -e MINIO_ROOT_USER="$ACCESS_KEY" \
  -e MINIO_ROOT_PASSWORD="$SECRET_KEY" \
  "$MINIO_IMAGE" server /data --console-address ":9001" >/dev/null

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
