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

  Expert review 2026-10-12 #24 (below): the first stamp must also be IN PLACE, not temp+rename.
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
      :erlang.trace_pattern({:file, :open, 2}, false, [:local])
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
    # Since expert review 2026-10-12 #24 the sidecar is born as a fixed-width record, so the first
    # durable stamp is an in-place pwrite (`:file.open` of the sidecar + pwrite + fsync) rather than
    # a temp+rename `atomic_write` (`pwrite_sync` opens `[:read, :write, :raw, :binary]`; the open path's
    # plain `File.write` opens differently, so it is not counted). Either is a durable sidecar write; count both.
    :erlang.trace_pattern({:file, :open, 2}, true, [:local])
    :erlang.trace(:all, true, [:call, {:tracer, self()}])
  end

  # Durable writes of THIS shard's sidecar received so far (drains the trace messages).
  defp durable_sidecar_writes(id) do
    sidecar = Fathom.Shard.db_path(id) <> ".etag"

    Stream.repeatedly(fn ->
      receive do
        {:trace, _pid, :call, {Storage, :atomic_write, [^sidecar, _]}} -> 1
        {:trace, _pid, :call, {:file, :open, [^sidecar, [:read, :write, :raw, :binary]]}} -> 1
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

  # Expert review 2026-10-12 #24. `Provenance.write/2` (the open path) wrote a bare etag, so the
  # first durable stamp found a non-record file and converted it with temp+rename: the sidecar-loses-
  # a-rename window the fixed-width in-place format exists to close, reopened once per coordinator
  # life. Invariant: the sidecar is born as the fixed-width record and the first stamp is a pwrite —
  # same size before and after, and no temp file, ever.
  test "the sidecar is a fixed-width record from birth; the first stamp leaves no temp", %{id: id} do
    {:ok, c} = ShardExecutor.open(id)
    sidecar = Fathom.Shard.db_path(id) <> ".etag"

    assert %{size: 177} = File.stat!(sidecar), "write/2 did not create the fixed-width record"

    {:ok, _} = ShardExecutor.execute(c, %Stmt{sql: "CREATE TABLE t (a)", args: []})
    sync(c)
    assert %{size: 177} = File.stat!(sidecar)
    :ok = ShardExecutor.close(c)

    leftovers =
      Fathom.Shard.data_dir()
      |> File.ls!()
      |> Enum.filter(&(String.starts_with?(&1, id) and String.contains?(&1, ".etag.")))

    assert leftovers == []
  end
end
