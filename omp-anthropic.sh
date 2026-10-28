#!/usr/bin/env bash
# Launch omp with every model role on Anthropic's newest models.
#
#   Opus 5.5   — thinking: advisor, planning, slow/deep reasoning, code review
#   Sonnet 5.5 — doing: the main driver, task subagents, vision
#   Haiku 5.5  — fast + cheap: smol jobs, commit messages, scouts, quick lookups
#
# Your ~/.omp/agent/config.yml is not changed; this overlay applies to this run only.
# Extra args pass straight through:  ./omp-anthropic.sh -c   ./omp-anthropic.sh "fix the flake"
set -euo pipefail

OPUS="anthropic/claude-opus-5-5"
SONNET="anthropic/claude-sonnet-5-5"
HAIKU="anthropic/claude-haiku-5-5"

overlay="$(mktemp -t omp-anthropic).yml"
trap 'rm -f "$overlay"' EXIT

cat >"$overlay" <<YAML
modelRoles:
  default: ${SONNET}:high
  plan: ${OPUS}:high
  slow: ${OPUS}:xhigh
  advisor: ${OPUS}:high
  vision: ${SONNET}:medium
  smol: ${HAIKU}:low
  commit: "@smol"
  worker_task: ${SONNET}:high
  worker_review: ${OPUS}:high
  worker_sonic: ${HAIKU}:low
  worker_scout: ${HAIKU}:low
retry:
  fallbackChains:
    advisor:
      - ${SONNET}:high
advisor:
  enabled: true
YAML

omp --config "$overlay" "$@"
