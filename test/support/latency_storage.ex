defmodule Fathom.Test.LatencyStorage do
  @moduledoc """
  Test storage backend that makes every call on ONE shard cost a fixed round trip, and records when
  each call started and finished. It delegates every callback to `Fathom.Shard.Storage.Local`.

  Counting serial storage round trips on the open path is the only way to see "the HEAD waited for
  the pull" or "the warm check and the lineage read are two HEADs": with `Storage.Local` every call
  returns in microseconds, so the structure of the open (what overlaps what) is invisible, and a
  wall-clock assertion has nothing to measure (perf review 2026-10-14 #3).

      Application.put_env(:fathom, :shard_storage, Fathom.Test.LatencyStorage)
      Application.put_env(:fathom, :latency_storage,
        shard: id, delay_ms: 100, sink: self(), extra: %{pull: 100})

  Each call on `shard` sleeps `delay_ms` (plus `extra[op]`), runs, and sends
  `{:storage_call, op, started_ms, finished_ms}` to `sink`, in monotonic milliseconds. Calls whose
  first argument is not `shard` (heartbeat ops, other tests' shards) pass straight through, so a
  coordinator left over from an earlier test never pollutes the record.

  `extra: %{pull: n}` models a pull that costs more than a lock PUT: a GET that transfers a body.
  A real shard pull is that; a bare `delay_ms` for everything is the lower bound.
  """

  alias Fathom.Shard.Storage
  alias Fathom.Shard.Storage.Local

  Code.ensure_compiled!(Local)

  for {name, arity} <- Storage.behaviour_info(:callbacks),
      function_exported?(Local, name, arity) do
    args = Macro.generate_arguments(arity, __MODULE__)

    def unquote(name)(unquote_splicing(args)) do
      call(unquote(name), unquote(args), fn -> apply(Local, unquote(name), unquote(args)) end)
    end
  end

  defp call(op, [shard_id | _], fun) do
    cfg = Application.get_env(:fathom, :latency_storage, [])

    if is_binary(shard_id) and Keyword.get(cfg, :shard) == shard_id do
      started = System.monotonic_time(:millisecond)
      extra = cfg |> Keyword.get(:extra, %{}) |> Map.get(op, 0)
      # Lets a test monitor the process making this call (e.g. an overlapped HEAD task).
      send(Keyword.fetch!(cfg, :sink), {:"storage_#{op}_started", self()})
      Process.sleep(Keyword.fetch!(cfg, :delay_ms) + extra)

      result =
        case cfg |> Keyword.get(:fail, %{}) |> Map.fetch(op) do
          {:ok, reason} -> {:error, reason}
          :error -> fun.()
        end

      finished = System.monotonic_time(:millisecond)
      send(Keyword.fetch!(cfg, :sink), {:storage_call, op, started, finished})
      result
    else
      fun.()
    end
  end

  defp call(_op, _args, fun), do: fun.()
end
