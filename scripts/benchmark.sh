#!/usr/bin/env bash
# Run fathom's hot-path benchmarks prod-compiled, append one line to
# scripts/perf_history.jsonl, and tee the human-readable run to logs/.
#
#   scripts/benchmark.sh                 # bench all four metrics, append history
#   scripts/benchmark.sh --only cold_open,copy,fanout   # skip the directory bench
#   scripts/benchmark.sh --trials 3 --copy-rows 200000  # pass-through to the task
#
# This is the orchestration half of the harness (the measurement half is
# `Fathom.Bench` + `mix fathom.bench`; see docs/benchmark-plan.md). It runs in
# MIX_ENV=prod against a throwaway `fathom_bench` Postgres DB so the numbers are
# prod-representative and the directory bench has a real `shards` table to resolve
# against. `Fathom.Bench` cleans its own scratch dirs each run, so "clean state"
# is intrinsic — no data dir to wipe here.
#
# Env:
#   FATHOM_BENCH_DATABASE_URL  full ecto:// URL for the bench DB
#                              (default: ecto://$USER@localhost/fathom_bench)
#   FATHOM_BENCH_PGUSER        Postgres user when building the default URL ($USER)
#   SECRET_KEY_BASE            honored if already set; otherwise a throwaway is used
#
# Why the secrets: fathom's config/runtime.exs (config_env() == :prod) raises
# without DATABASE_URL and SECRET_KEY_BASE, and that runs under `mix app.config`
# too — even though the bench never starts the web endpoint. So we provide both;
# SECRET_KEY_BASE is unused (no endpoint) but must be present.
set -uo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

export MIX_ENV=prod
export DATABASE_URL="${FATHOM_BENCH_DATABASE_URL:-ecto://${FATHOM_BENCH_PGUSER:-$USER}@localhost/fathom_bench}"
export SECRET_KEY_BASE="${SECRET_KEY_BASE:-benchonlynotasecret0000000000000000000000000000000000000000000000}"

HISTORY="scripts/perf_history.jsonl"
mkdir -p logs
LOG="logs/bench-$(date +%Y%m%d-%H%M%S).log"

echo "=== fathom benchmark (MIX_ENV=prod) ==="
echo "    db:      $DATABASE_URL"
echo "    history: $HISTORY"
echo "    log:     $LOG"
echo ""

# Prod-compile up front so a compile error fails fast, not mid-bench.
if ! mix compile >/dev/null 2>&1; then
    echo "ERROR: prod compile failed — re-running to show output:" >&2
    mix compile
    exit 1
fi

# Ensure the bench DB exists + is migrated (the directory bench needs `shards`).
if ! mix ecto.create --quiet; then
    echo "ERROR: ecto.create failed for the bench DB. Is Postgres up and \$USER trusted?" >&2
    exit 1
fi

if ! mix ecto.migrate --quiet >/dev/null 2>&1; then
    echo "ERROR: ecto.migrate failed — re-running to show output:" >&2
    mix ecto.migrate
    exit 1
fi

# Run the bench. The task writes the JSON line to stdout and appends it to
# HISTORY; its human table goes to stderr, which we tee to the log.
mix fathom.bench --append "$HISTORY" --log "$LOG" "$@" 2> >(tee "$LOG" >&2)
bench_rc=${PIPESTATUS[0]}

if (( bench_rc != 0 )); then
    echo "ERROR: mix fathom.bench failed (rc=$bench_rc)." >&2
    exit "$bench_rc"
fi

echo ""
echo "appended to $HISTORY:"
tail -n 1 "$HISTORY"
