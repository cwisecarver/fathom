#!/usr/bin/env bash
# Attribute a node's OFF-HEAP BINARY memory to the processes holding it.
#
# WHY THIS EXISTS: `mem_sampler.sh` ranks processes by `Process.info(pid, :memory)`, and that call
# does NOT include an off-heap refc binary's payload — only the small reference. So on a node with
# 43 GB of binary and 54 MB of process memory it reports a 9 MB process as "the biggest" and is
# structurally incapable of naming the holder. That blindness produced a wrong diagnosis of the
# 2026-08-14 A2 OOM (the mailbox was blamed because a QUEUED message is the one place a payload is
# counted, so it was the only place the instrument could see). Do not diagnose binary growth
# without this.
#
#   ./bin_holders.sh <node> [top_n] [mode]
#
# MODES, and the difference matters:
#
#   refs (default) — NON-DESTRUCTIVE. Sums `Process.info(pid, :binary)` per process: every refc
#                    binary reachable from that process, live or garbage. Answers "who is holding
#                    it RIGHT NOW". Caveat: a binary shared by N processes is counted N times, so
#                    the total can exceed `:erlang.memory(:binary)` — read the ranking, not the sum.
#
#   leak           — DESTRUCTIVE, and it is the recon `bin_leak` idea: garbage-collect each process
#                    in turn and attribute the drop in `:erlang.memory(:binary)` to it. Answers
#                    "who is sitting on RELEASABLE garbage", which is the sharper question when a
#                    full GC is known to free everything (as it did here: 17,867 MB -> 14 MB).
#                    It FREES the memory as a side effect, so the node is changed by measuring it.
#                    One shot, at the moment of interest.
set -euo pipefail

NODE=${1:-fathom1}
TOP=${2:-10}
MODE=${3:-refs}
DOCKER=${DOCKER:-$(ls /opt/homebrew/Cellar/docker/*/bin/docker 2>/dev/null | head -1)}

# `Process.info(pid, :binary)` can be genuinely expensive on a process referencing very many
# binaries, and this walks every process — acceptable for a diagnostic, not for a poll loop. That
# is why this is a separate on-demand probe rather than a column in the sampler.
case "$MODE" in
  refs)
    PROBE='rank = :erlang.processes() |> Enum.map(fn p -> bytes = case Process.info(p, :binary) do {:binary, bins} -> Enum.reduce(bins, 0, fn b, acc -> acc + elem(b, 1) end); _ -> 0 end; name = case Process.info(p, :registered_name) do {:registered_name, a} when is_atom(a) -> Atom.to_string(a); _ -> inspect(p) end; q = elem(Process.info(p, :message_queue_len) || {:x, 0}, 1); {bytes, name, q} end) |> Enum.sort_by(fn {b, _, _} -> -b end) |> Enum.take('"$TOP"'); IO.puts("node binary total: " <> to_string(div(:erlang.memory()[:binary], 1048576)) <> " MB"); IO.puts("held_MB  msgq  process"); for {b, n, q} <- rank, do: IO.puts(String.pad_leading(to_string(div(b, 1048576)), 7) <> "  " <> String.pad_leading(to_string(q), 4) <> "  " <> n)'
    ;;
  leak)
    PROBE='before = :erlang.memory()[:binary]; rank = :erlang.processes() |> Enum.map(fn p -> b0 = :erlang.memory()[:binary]; :erlang.garbage_collect(p); freed = b0 - :erlang.memory()[:binary]; name = case Process.info(p, :registered_name) do {:registered_name, a} when is_atom(a) -> Atom.to_string(a); _ -> inspect(p) end; {freed, name} end) |> Enum.sort_by(fn {f, _} -> -f end) |> Enum.take('"$TOP"'); after_ = :erlang.memory()[:binary]; IO.puts("binary " <> to_string(div(before, 1048576)) <> " MB -> " <> to_string(div(after_, 1048576)) <> " MB after GCing every process"); IO.puts("freed_MB  process"); for {f, n} <- rank, do: IO.puts(String.pad_leading(to_string(div(f, 1048576)), 8) <> "  " <> n)'
    ;;
  *)
    echo "unknown mode '$MODE' (want: refs | leak)" >&2
    exit 1
    ;;
esac

"$DOCKER" exec -i "fathom-chaos-${NODE}-1" /app/bin/fathom rpc "$PROBE" 2>&1 | tr -d '\r'
