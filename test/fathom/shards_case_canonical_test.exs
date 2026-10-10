defmodule Fathom.ShardsCaseCanonicalTest do
  @moduledoc """
  `Shards.flush/drain/stop/ensure` canonicalize the shard id (expert review 2026-10-10 #9).

  The Registry, the ETS lifecycle gates and `db_path` are all lowercase-keyed. These entry points
  used to take the raw id, so `flush("ACME")` missed the live `acme` coordinator and the `.db`,
  answered `:ok` without flushing, and `ensure("ACME")` skipped the tombstone/suspension gates.
  Shard-isolation gate: every spelling resolves to exactly the one canonical shard, and never to a
  different one.
  """
  use ExUnit.Case, async: false

  alias Fathom.{ShardExecutor, Shards}
  alias Filo.Stmt

  setup do
    n = System.unique_integer([:positive])
    a = "casea_#{n}"
    b = "caseb_#{n}"
    prev = Application.get_env(:fathom, :shard_idle_ms)
    Application.put_env(:fathom, :shard_idle_ms, 60_000)

    on_exit(fn ->
      if is_nil(prev),
        do: Application.delete_env(:fathom, :shard_idle_ms),
        else: Application.put_env(:fathom, :shard_idle_ms, prev)

      for id <- [a, b] do
        Shards.stop(id)

        for dir <- [Fathom.Shard.data_dir(), Fathom.Shard.Storage.Local.dir()],
            path <- Path.wildcard(Path.join(dir, "#{id}*")),
            do: File.rm(path)
      end
    end)

    %{a: a, b: b}
  end

  defp stmt(sql), do: %Stmt{sql: sql, args: []}

  test "ensure/1 on an uppercase id returns the canonical coordinator, never another shard", %{
    a: a,
    b: b
  } do
    {:ok, pa} = Shards.ensure(a)
    {:ok, pb} = Shards.ensure(b)
    assert pa != pb

    assert {:ok, ^pa} = Shards.ensure(String.upcase(a))
    assert {:ok, ^pb} = Shards.ensure(String.upcase(b))
    assert {:error, :invalid_shard_id} = Shards.ensure("bad id")
  end

  test "flush/1 on an uppercase id flushes the live lowercase coordinator", %{a: a} do
    {:ok, h} = ShardExecutor.open(a)
    {:ok, _} = ShardExecutor.execute(h, stmt("CREATE TABLE t (v TEXT)"))
    {:ok, _} = ShardExecutor.execute(h, stmt("INSERT INTO t VALUES ('x')"))
    [{pid, _}] = Registry.lookup(Fathom.ShardRegistry, a)
    assert Fathom.Shard.dirty?(pid), "precondition: the write must leave the shard dirty"

    # Pre-fix this returned :ok via the "no coordinator, no file" branch WITHOUT flushing.
    assert :ok = Shards.flush(String.upcase(a))
    refute Fathom.Shard.dirty?(pid)
    :ok = ShardExecutor.close(h)

    assert {:error, :invalid_shard_id} = Shards.flush("bad id")
  end

  test "drain/2 and stop/1 on an uppercase id act on the canonical coordinator only", %{
    a: a,
    b: b
  } do
    {:ok, pa} = Shards.ensure(a)
    {:ok, pb} = Shards.ensure(b)
    ref = Process.monitor(pa)

    assert :ok = Shards.stop(String.upcase(a))
    assert_receive {:DOWN, ^ref, :process, ^pa, _}, 10_000
    assert {:ok, ^pb} = Shards.ensure(b)
    assert {:error, :invalid_shard_id} = Shards.drain("bad id", 100)
  end
end
