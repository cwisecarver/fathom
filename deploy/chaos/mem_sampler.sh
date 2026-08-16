#!/usr/bin/env bash
# Sample every node's memory breakdown while a scenario runs.
#
# WHY THIS EXISTS: the 2026-08-14 A2 OOM was diagnosed twice from the SURVIVORS, after the fact —
# which is exactly why the second cause is still unnamed. A node that dies takes its own evidence
# with it, so the only way to see what a dying node was holding is to have been sampling it.
#
# Emits one CSV row per node per tick: the BEAM's own memory split plus the top process by memory
# and by message queue. Deliberately reads `:erlang.memory/0` rather than container RSS — the
# container tells you a node grew, this tells you WHICH ARENA grew, and binary-vs-processes was
# the whole answer last time.
#
#   ./mem_sampler.sh out.csv [interval_secs] [max_secs]
#
# IT STOPS BY ITSELF, and that is not a convenience. The first version was `while true` with no
# exit, and one instance per rig run accumulated: by 2026-08-16 there were FIVE running, the oldest
# for ~24 hours. Each one execs into all five nodes every interval, so they were applying load to
# the cluster under measurement — and the count GREW between runs, which silently broke the
# same-topology rule every run-to-run comparison depends on. The 256-tenant throughput comparison
# of 2026-08-15/16 is contaminated by exactly this and has to be re-run with one sampler.
#
# So: a default deadline, and a refusal to start when another instance is already sampling.
set -euo pipefail

OUT=${1:-mem-samples.csv}
INTERVAL=${2:-5}
# 45 min comfortably outlasts a 256,1024 sweep and is far short of "still running tomorrow".
MAX_SECS=${3:-2700}
DOCKER=${DOCKER:-$(ls /opt/homebrew/Cellar/docker/*/bin/docker 2>/dev/null | head -1)}
NODES=(fathom1 fathom2 fathom3 fathom4 fathom5)

# Refuse to be the second sampler, via an atomic lock dir rather than a process scan.
#
# `pgrep -f mem_sampler` was tried first and is WRONG here: the harness runs this through a shell
# whose own command line contains the script path, so a single real instance counts as two and the
# guard refused every time. `mkdir` is atomic, and the PID inside lets a genuinely stale lock (the
# failure mode that made me avoid a pidfile) be detected by liveness rather than assumed.
LOCK=${MEM_SAMPLER_LOCK:-/tmp/fathom-mem-sampler.lock}

claim_lock() {
  echo $$ > "$LOCK/pid"
  # Release on every exit path, including the deadline and ctrl-c, or the next run inherits a lock
  # that outlives its owner — which is the same class of bug as the sampler that outlived its run.
  trap 'rm -rf "$LOCK"' EXIT INT TERM
}

if mkdir "$LOCK" 2>/dev/null; then
  claim_lock
else
  holder=$(cat "$LOCK/pid" 2>/dev/null || echo "")
  if [ -n "$holder" ] && kill -0 "$holder" 2>/dev/null; then
    echo "REFUSING: mem_sampler.sh is already running as pid $holder." >&2
    echo "A second sampler execs into all five nodes on its own interval, adding load to the very" >&2
    echo "cluster being measured — that is how the 2026-08-16 comparison got contaminated." >&2
    exit 1
  fi
  echo "note: clearing a stale lock (pid ${holder:-unknown} is gone)" >&2
  claim_lock
fi

echo "ts,node,total_mb,processes_mb,binary_mb,ets_mb,top_proc,top_proc_mb,top_q_proc,top_q_len" > "$OUT"

# One eval, kept to a single line: `bin/fathom rpc` takes an expression, and multi-line pipelines
# through `docker exec` have already cost a debugging round here.
PROBE='m = :erlang.memory(); ps = :erlang.processes() |> Enum.map(fn p -> {p, elem(Process.info(p, :memory) || {:x, 0}, 1), elem(Process.info(p, :message_queue_len) || {:x, 0}, 1), Process.info(p, :registered_name)} end); {bp, bm, _, bn} = Enum.max_by(ps, fn {_, mm, _, _} -> mm end); {qp, _, ql, qn} = Enum.max_by(ps, fn {_, _, q, _} -> q end); name = fn n, p -> case n do {:registered_name, a} when is_atom(a) -> Atom.to_string(a); _ -> inspect(p) end end; IO.puts(Enum.join([div(m[:total], 1048576), div(m[:processes], 1048576), div(m[:binary], 1048576), div(m[:ets], 1048576), name.(bn, bp), div(bm, 1048576), name.(qn, qp), ql], ","))'

echo "sampling every ${INTERVAL}s for up to ${MAX_SECS}s -> $OUT  (ctrl-c to stop early)"
started=$(date +%s)

while true; do
  ts=$(date +%s)

  if [ $((ts - started)) -ge "$MAX_SECS" ]; then
    echo "reached the ${MAX_SECS}s deadline; stopping. $(($(wc -l < "$OUT") - 1)) samples in $OUT"
    exit 0
  fi

  for n in "${NODES[@]}"; do
    # A dead node must leave a row saying so, not a gap: an absent row is indistinguishable from a
    # slow sample, and "when did it stop reporting" is the timestamp that matters most.
    row=$("$DOCKER" exec -i "fathom-chaos-${n}-1" /app/bin/fathom rpc "$PROBE" 2>/dev/null | tr -d '\r' | tail -1 || true)
    case "$row" in
      [0-9]*) echo "$ts,$n,$row" >> "$OUT" ;;
      *)      echo "$ts,$n,DEAD,,,,,,," >> "$OUT" ;;
    esac
  done
  sleep "$INTERVAL"
done
