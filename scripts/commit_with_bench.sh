#!/usr/bin/env bash
# Bench-then-commit gate. Runs the hot-path benchmark against the working tree,
# compares every metric to the parent commit's entry in scripts/perf_history.jsonl,
# and creates the commit only if no metric regressed past the threshold.
#
# The gate is MULTI-METRIC: a change can leave shard cold-open flat while
# doubling per-shard memory (halving node density), so the gate refuses on a
# >=20% regression in ANY hot-path metric, not one TPS scalar.
# See docs/benchmark-plan.md.
#
# Usage:
#   scripts/commit_with_bench.sh -m "message"          # bench + commit
#   scripts/commit_with_bench.sh --check-only          # bench, don't commit
#   scripts/commit_with_bench.sh -m "message" --skip   # commit without bench
#                                                       (also: [skip-bench] in -m)
#
# Pure docs/test/comment-only changes don't touch a hot path — commit those with
# `git commit` directly and a `[skip-bench]` token, or pass --skip here.
#
# Env:
#   PERF_REGRESS_BLOCK   percent regression that refuses the commit (default 20)
#   PERF_REGRESS_WARN    percent regression that prompts (default 20; equal to
#                        block by default, so there is no warn band — set lower
#                        for a heads-up prompt beneath the refuse threshold)
#   (benchmark.sh env passes through: FATHOM_BENCH_DATABASE_URL, SECRET_KEY_BASE)
set -uo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

HISTORY="scripts/perf_history.jsonl"
BLOCK="${PERF_REGRESS_BLOCK:-20}"
WARN="${PERF_REGRESS_WARN:-20}"

MESSAGE=""
CHECK_ONLY=0
SKIP_BENCH=0
GIT_COMMIT_ARGS=()

usage() {
    grep -E '^# ' "$0" | head -30 | sed 's/^# //; s/^#$//'
    exit "${1:-0}"
}

while (( $# > 0 )); do
    case "$1" in
        -m|--message) MESSAGE="$2"; shift 2 ;;
        -m*) MESSAGE="${1#-m}"; shift ;;
        --check-only) CHECK_ONLY=1; shift ;;
        --skip|--skip-bench) SKIP_BENCH=1; shift ;;
        -h|--help) usage 0 ;;
        --) shift; GIT_COMMIT_ARGS+=("$@"); break ;;
        *) GIT_COMMIT_ARGS+=("$1"); shift ;;
    esac
done

# Allow a [skip-bench] token in the commit message.
if [[ "$MESSAGE" == *"[skip-bench]"* ]]; then
    SKIP_BENCH=1
fi

if (( CHECK_ONLY == 0 )) && [[ -z "$MESSAGE" ]]; then
    echo "ERROR: -m \"<message>\" is required (or --check-only to bench without committing)" >&2
    usage 1
fi

# Parent to compare against: current HEAD (the commit we're branching off).
# COMMIT_BENCH_PARENT overrides it — handy to re-compare against an older baseline.
PARENT="${COMMIT_BENCH_PARENT:-$(git rev-parse HEAD 2>/dev/null || echo unknown)}"
PARENT_SHORT="$(git rev-parse --short "$PARENT" 2>/dev/null || echo "$PARENT")"

# Verify there's something to commit (unless --check-only).
if (( CHECK_ONLY == 0 )); then
    if git diff --quiet --cached && git diff --quiet; then
        echo "ERROR: nothing to commit (no staged or unstaged changes)" >&2
        exit 1
    fi
fi

if (( SKIP_BENCH == 1 )); then
    echo "[skip-bench] Skipping benchmark gate."
    if (( CHECK_ONLY == 0 )); then
        if (( ${#GIT_COMMIT_ARGS[@]} > 0 )); then
            git commit -m "$MESSAGE" "${GIT_COMMIT_ARGS[@]}"
        else
            git commit -m "$MESSAGE"
        fi
    fi
    exit $?
fi

echo "=== commit_with_bench: parent=$PARENT_SHORT ==="
echo "    refuse on >=${BLOCK}% regression in any metric, warn on >${WARN}%"

# Snapshot the history BEFORE benching: the working-tree bench records under the
# current HEAD (= the parent), so the new line and the parent baseline share a
# SHA. The snapshot lets the check tell them apart (parent = last same-SHA line
# in the snapshot; new = last line of the live history).
SNAP="$(mktemp -t fathom_perf_history.XXXXXX)"
cp "$HISTORY" "$SNAP" 2>/dev/null || : > "$SNAP"

echo ""
echo "=== Running benchmark.sh on the working tree ==="
scripts/benchmark.sh
bench_rc=$?

if (( bench_rc != 0 )); then
    echo "ERROR: benchmark.sh failed (rc=$bench_rc) — refusing to commit." >&2
    rm -f "$SNAP"
    exit "$bench_rc"
fi

echo ""
echo "=== Gate ==="
mix fathom.bench.check \
    --parent "$PARENT" \
    --parent-history "$SNAP" \
    --new-history "$HISTORY" \
    --block "$BLOCK" \
    --warn "$WARN"
check_rc=$?
rm -f "$SNAP"

case "$check_rc" in
    0)
        echo "  verdict: OK"
        ;;
    2)
        echo ""
        echo "No parent baseline — cannot detect a regression."
        echo "  → Bench the parent first (scripts/benchmark.sh on a clean checkout of"
        echo "    $PARENT_SHORT), or commit with --skip / [skip-bench] if this is the"
        echo "    genuinely-first bench."
        exit 2
        ;;
    4)
        echo ""
        echo "BLOCKED: a metric regressed >= ${BLOCK}%. Refusing to commit."
        echo "  → Revert the change, reproduce minimally, then fix and re-run — or commit"
        echo "    with --skip / [skip-bench] if the regression is intentional (rare:"
        echo "    usually a correctness fix that trades throughput for safety)."
        exit 4
        ;;
    3)
        echo ""
        echo "WARN: a metric regressed in (${WARN}%, ${BLOCK}%)."
        if (( CHECK_ONLY == 1 )); then
            exit 0
        fi
        # A backgrounded run (no TTY) would hang on `read`. Abort cleanly instead.
        if [[ ! -t 0 ]]; then
            echo "ABORTED: WARN-band regression and stdin is not a TTY."
            echo "  → Re-run attached to a terminal, or commit with [skip-bench] after"
            echo "    documenting the verdict in the message."
            exit 5
        fi
        echo -n "  → Continue and create the commit anyway? [y/N] "
        read -r answer
        case "$answer" in
            y|Y|yes|YES) ;;
            *) echo "Aborted."; exit 5 ;;
        esac
        ;;
    *)
        echo "ERROR: gate check failed (rc=$check_rc) — refusing to commit." >&2
        exit "$check_rc"
        ;;
esac

if (( CHECK_ONLY == 1 )); then
    echo ""
    echo "(--check-only: not committing)"
    exit 0
fi

echo ""
echo "=== Creating commit ==="
if (( ${#GIT_COMMIT_ARGS[@]} > 0 )); then
    git commit -m "$MESSAGE" "${GIT_COMMIT_ARGS[@]}"
else
    git commit -m "$MESSAGE"
fi
