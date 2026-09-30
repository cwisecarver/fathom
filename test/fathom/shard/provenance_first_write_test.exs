defmodule Fathom.Shard.ProvenanceFirstWriteTest do
  @moduledoc """
  The provenance sidecar is made DURABLE on the shard's first write, not at open (expert review
  2026-09-29 #14, option B as decided 2026-09-29).

  The failure is an OS crash (power loss, kernel panic) while the open path's plain-written sidecar
  is still in the page cache: it reads back empty/torn, `Fork.resolve` quarantines the live `.db` —
  which by then holds acked, fsynced writes — and the older object is served. Fsyncing at open was
  measured at +24–28% cold_open_p50, so the fsync moved to the first `:became_dirty`.

  A test cannot crash the OS, so this pins the MECHANISM with a call trace on
  `Storage.atomic_write/2` (fsync-before-rename) for this shard's sidecar:

    * a read-only open makes NO durable sidecar write (the open path stays as cheap as before);
    * the first write — statement or script — makes exactly one, before any flush;
    * later writes in the same coordinator lifetime make no more.

  Pre-fix the first write made none, so the sidecar stayed non-durable until the first flush.
  """
  use ExUnit.Case, async: false

  alias Fathom.Shard.Storage
  alias Fathom.{ShardExecutor, Shards}
  alias Filo.Stmt

  setup do
    id = "provfw_#{System.unique_integer([:positive])}"
    prev_idle = Application.get_env(:fathom, :shard_idle_ms)
    prev_flush = Application.get_env(:fathom, :shard_flush_interval_ms)
    # No flush and no idle drop inside the test window — only the first-write sync can write it.
    Application.put_env(:fathom, :shard_idle_ms, 60_000)
    Application.put_env(:fathom, :shard_flush_interval_ms, 600_000)

    on_exit(fn ->
      :erlang.trace(:all, false, [:call])
      :erlang.trace_pattern({Storage, :atomic_write, 2}, false, [:local])
      restore(:shard_idle_ms, prev_idle)
      restore(:shard_flush_interval_ms, prev_flush)
      Shards.stop(id)

      for dir <- [Fathom.Shard.data_dir(), Storage.Local.dir()],
          path <- Path.wildcard(Path.join(dir, "#{id}*")),
          do: File.rm(path)
    end)

    %{id: id}
  end

  defp restore(k, nil), do: Application.delete_env(:fathom, k)
  defp restore(k, v), do: Application.put_env(:fathom, k, v)

  defp trace_atomic_writes! do
    :erlang.trace_pattern({Storage, :atomic_write, 2}, true, [:local])
    :erlang.trace(:all, true, [:call, {:tracer, self()}])
  end

  # Durable writes of THIS shard's sidecar received so far (drains the trace messages).
  defp durable_sidecar_writes(id) do
    sidecar = Fathom.Shard.db_path(id) <> ".etag"

    Stream.repeatedly(fn ->
      receive do
        {:trace, _pid, :call, {Storage, :atomic_write, [^sidecar, _]}} -> 1
        {:trace, _pid, :call, _} -> 0
      after
        0 -> :done
      end
    end)
    |> Enum.take_while(&(&1 != :done))
    |> Enum.sum()
  end

  defp sync(conn), do: _ = :sys.get_state(elem(conn, 0))

  test "read-only: none; first statement write: exactly one; later writes: none", %{id: id} do
    trace_atomic_writes!()

    {:ok, c} = ShardExecutor.open(id)
    {:ok, _} = ShardExecutor.execute(c, %Stmt{sql: "SELECT 1", args: []})
    sync(c)
    assert durable_sidecar_writes(id) == 0, "a read-only open paid the sidecar fsync"

    {:ok, _} = ShardExecutor.execute(c, %Stmt{sql: "CREATE TABLE t (a)", args: []})
    sync(c)
    assert durable_sidecar_writes(id) == 1, "the first write did not make the sidecar durable"
    :ok = ShardExecutor.close(c)

    # A second stream writing to the same (still open) coordinator does not pay again.
    {:ok, c2} = ShardExecutor.open(id)
    {:ok, _} = ShardExecutor.execute(c2, %Stmt{sql: "INSERT INTO t VALUES (1)", args: []})
    sync(c2)
    :ok = ShardExecutor.close(c2)
    assert durable_sidecar_writes(id) == 0
  end

  test "a script-only writer also makes the sidecar durable", %{id: id} do
    trace_atomic_writes!()

    {:ok, c} = ShardExecutor.open(id)
    :ok = ShardExecutor.execute_sequence(c, "CREATE TABLE t (a); INSERT INTO t VALUES (1);")
    sync(c)
    assert durable_sidecar_writes(id) == 1, "a script write never reached :became_dirty"
    :ok = ShardExecutor.close(c)
  end
end
