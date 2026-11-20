#!/usr/bin/env bash
# Launch omp with every model role on Anthropic's newest models.
#
#   Opus 5.5   — thinking AND doing: the main driver, task subagents, vision, advisor, planning,
#                slow/deep reasoning, code review
#   Sonnet 5.5 — fast + cheap, at high effort: smol jobs, commit messages, scouts, quick lookups
#                (and the advisor's fallback if Opus fails)
#
# Your ~/.omp/agent/config.yml is not changed; this overlay applies to this run only.
# Extra args pass straight through:  ./omp-anthropic.sh -c   ./omp-anthropic.sh "fix the flake"
set -euo pipefail

OPUS="anthropic/claude-opus-5-5"
SONNET="anthropic/claude-sonnet-5-5"

overlay="$(mktemp -t omp-anthropic).yml"
trap 'rm -f "$overlay"' EXIT

cat >"$overlay" <<YAML
modelRoles:
  default: ${OPUS}:high
  plan: ${OPUS}:high
  slow: ${OPUS}:xhigh
  advisor: ${OPUS}:high
  vision: ${OPUS}:medium
  smol: ${SONNET}:high
  commit: "@smol"
  worker_task: ${OPUS}:high
  worker_review: ${OPUS}:high
  worker_sonic: ${SONNET}:high
  worker_scout: ${SONNET}:high
retry:
  fallbackChains:
    advisor:
      - ${SONNET}:high
advisor:
  enabled: true
YAML

omp --config "$overlay" "$@"
