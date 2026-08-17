#!/usr/bin/env bash
# Size HISTOGRAM of the binaries one process is holding.
#
# `bin_holders.sh` answers WHO holds the memory. This answers WHAT they are holding, which is the
# next question and needs a different read: a shipper's queue full of 16 KB seed chunks and one
# full of 1 MB WAL deltas look identical by total bytes and mean completely different things.
#
# Reads sizes only. `Process.info(pid, :binary)` returns {id, size, refcount} triples, so this
# never copies a payload — unlike `Process.info(pid, :messages)`, which would copy the entire
# mailbox (gigabytes here) into the calling process and is not safe to run on a node under study.
#
#   ./bin_sizes.sh <node> <registered_process_name>
set -euo pipefail

NODE=${1:?node}
PROC=${2:?registered process name, e.g. Elixir.Fathom.Shard.Replication.Shipper.N_ZmF0aG9tNA}
DOCKER=${DOCKER:-$(ls /opt/homebrew/Cellar/docker/*/bin/docker 2>/dev/null | head -1)}

PROBE='p = Process.whereis(:"'"$PROC"'"); if is_nil(p) do IO.puts("no such process") else bins = case Process.info(p, :binary) do {:binary, b} -> b; _ -> [] end; sizes = Enum.map(bins, fn t -> elem(t, 1) end); total = Enum.sum(sizes); n = length(sizes); q = elem(Process.info(p, :message_queue_len) || {:x, 0}, 1); IO.puts("binaries=" <> to_string(n) <> " total=" <> to_string(div(total, 1048576)) <> "MB msgq=" <> to_string(q) <> " mean=" <> to_string(if n > 0, do: div(total, n), else: 0) <> "B"); buckets = Enum.reduce(sizes, %{}, fn s, acc -> k = cond do s < 16*1024 -> "<16KB"; s < 64*1024 -> "16-64KB"; s < 256*1024 -> "64-256KB"; s < 1024*1024 -> "256KB-1MB"; s < 4*1024*1024 -> "1-4MB"; true -> ">4MB" end; Map.update(acc, k, 1, &(&1 + 1)) end); for k <- ["<16KB", "16-64KB", "64-256KB", "256KB-1MB", "1-4MB", ">4MB"], c = Map.get(buckets, k), c != nil, do: IO.puts("  " <> String.pad_trailing(k, 10) <> " " <> to_string(c)); sorted = Enum.sort(sizes, :desc) |> Enum.take(5); IO.puts("largest: " <> inspect(Enum.map(sorted, &div(&1, 1024))) <> " KB") end'

"$DOCKER" exec -i "fathom-chaos-${NODE}-1" /app/bin/fathom rpc "$PROBE" 2>&1 | tr -d '\r'
