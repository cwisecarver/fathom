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
#   ./mem_sampler.sh out.csv [interval_secs]
set -euo pipefail

OUT=${1:-mem-samples.csv}
INTERVAL=${2:-5}
DOCKER=${DOCKER:-$(ls /opt/homebrew/Cellar/docker/*/bin/docker 2>/dev/null | head -1)}
NODES=(fathom1 fathom2 fathom3 fathom4 fathom5)

echo "ts,node,total_mb,processes_mb,binary_mb,ets_mb,top_proc,top_proc_mb,top_q_proc,top_q_len" > "$OUT"

# One eval, kept to a single line: `bin/fathom rpc` takes an expression, and multi-line pipelines
# through `docker exec` have already cost a debugging round here.
PROBE='m = :erlang.memory(); ps = :erlang.processes() |> Enum.map(fn p -> {p, elem(Process.info(p, :memory) || {:x, 0}, 1), elem(Process.info(p, :message_queue_len) || {:x, 0}, 1), Process.info(p, :registered_name)} end); {bp, bm, _, bn} = Enum.max_by(ps, fn {_, mm, _, _} -> mm end); {qp, _, ql, qn} = Enum.max_by(ps, fn {_, _, q, _} -> q end); name = fn n, p -> case n do {:registered_name, a} when is_atom(a) -> Atom.to_string(a); _ -> inspect(p) end end; IO.puts(Enum.join([div(m[:total], 1048576), div(m[:processes], 1048576), div(m[:binary], 1048576), div(m[:ets], 1048576), name.(bn, bp), div(bm, 1048576), name.(qn, qp), ql], ","))'

echo "sampling every ${INTERVAL}s -> $OUT  (ctrl-c to stop)"
while true; do
  ts=$(date +%s)
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
