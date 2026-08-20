#!/usr/bin/env bash
# WHO holds binary memory, classified by ROLE — shipper (send side) vs follower serve-task
# (receive side) vs everything else.
#
# `bin_holders.sh` names the top holders but not what they ARE, and the two replication roles look
# identical in that list (a shipper is a named process, a follower's per-connection handler is a
# bare pid from `Task.start`). This exists to answer one question the shipper investigation left
# open: does the RECEIVE side retain frames the way the send side did (7-18 GiB before
# `fullsweep_after: 0`)?
#
#   ./follower_mem.sh <node>
set -euo pipefail
NODE=${1:?node}
DOCKER=${DOCKER:-$(ls /opt/homebrew/Cellar/docker/*/bin/docker 2>/dev/null | head -1)}

PROBE='
bin = fn p -> case Process.info(p, :binary) do {:binary, b} -> Enum.sum(Enum.map(b, &elem(&1, 1))); _ -> 0 end end
role = fn p ->
  ic = case Process.info(p, :dictionary) do
    {:dictionary, d} -> Keyword.get(d, :"$initial_call")
    _ -> nil
  end
  cf = case Process.info(p, :current_function) do {:current_function, f} -> f; _ -> nil end
  name = case Process.info(p, :registered_name) do {:registered_name, n} when is_atom(n) -> n; _ -> nil end
  cond do
    name != nil and String.contains?(to_string(name), "Replication.Shipper") -> :shipper
    match?({Fathom.Shard.Replication.Follower, _, _}, ic) -> :follower_task
    match?({Fathom.Shard.Replication.Follower, _, _}, cf) -> :follower_task
    true -> :other
  end
end
acc = Enum.reduce(Process.list(), %{}, fn p, a ->
  b = bin.(p)
  if b > 0 do
    r = role.(p)
    {n, tot, mx} = Map.get(a, r, {0, 0, 0})
    Map.put(a, r, {n + 1, tot + b, max(mx, b)})
  else
    a
  end
end)
total = div(:erlang.memory()[:binary], 1048576)
IO.puts("node binary total: " <> to_string(total) <> " MB")
for r <- [:shipper, :follower_task, :other] do
  case Map.get(acc, r) do
    nil -> IO.puts("  " <> String.pad_trailing(to_string(r), 15) <> " none")
    {n, tot, mx} ->
      IO.puts("  " <> String.pad_trailing(to_string(r), 15) <>
        String.pad_leading(to_string(div(tot, 1048576)), 6) <> " MB across " <>
        String.pad_leading(to_string(n), 5) <> " procs, largest " <>
        to_string(div(mx, 1048576)) <> " MB")
  end
end
'
"$DOCKER" exec -i "fathom-chaos-${NODE}-1" /app/bin/fathom rpc "$PROBE" 2>&1 | tr -d '\r'
