defmodule Fathom.Tenants.EraseLocalBatchAttributionTest do
  @moduledoc """
  Expert review 2026-11-07 #66 (tenant-lifecycle#6). `erase_local_batch/1` zipped an
  `ordered: false` `Task.async_stream` with its input list. A timed-out stop yields `{:exit, :timeout}`
  with no id, so it is attributed by POSITION — but with `ordered: false` results arrive in COMPLETION
  order, so the hung id in the middle was blamed on the LAST one: the hung shard had no entry (its
  result read `{id, nil}`, neither `:ok` nor an error), an unrelated shard was recorded as failed with
  its own `:ok` overwritten, and `LocalEraser.erase_now/1` could return `nil`. Two deleted tenants'
  `<id>.db*` then stayed on disk.

  One stop is made to hang through the `:erase_local_stop` seam (and the 120 s timeout shortened
  through `:erase_local_stop_timeout_ms`): no sleeps, the hung stop simply never answers. The id in
  the MIDDLE hangs, because with three ids that is the only position a positional zip gets wrong while
  the others complete first.
  """
  use ExUnit.Case, async: false

  alias Fathom.Shard
  alias Fathom.Tenants
  alias Fathom.Tenants.Tombstones

  setup do
    n = System.unique_integer([:positive])
    ids = for tag <- ~w(a b c), do: "erasebatch#{tag}_#{n}"

    prev =
      for k <- [:erase_local_stop, :erase_local_stop_timeout_ms],
          do: {k, Application.fetch_env(:fathom, k)}

    on_exit(fn ->
      for {k, v} <- prev do
        case v do
          {:ok, val} -> Application.put_env(:fathom, k, val)
          :error -> Application.delete_env(:fathom, k)
        end
      end

      for id <- ids do
        :ets.delete(Tombstones, id)
        for f <- Path.wildcard(Path.join(Shard.data_dir(), id <> ".db*")), do: File.rm(f)
      end
    end)

    %{ids: ids}
  end

  defp local_files(id), do: Path.wildcard(Path.join(Shard.data_dir(), id <> ".db*"))

  test "a hung stop is attributed to ITS id; the other tombstoned ids still erase", %{ids: ids} do
    [a, b, c] = ids

    for id <- ids do
      # Tombstoned directly in the public set: `Tombstones.put/1` would also trigger the async local
      # eraser this test is exercising by hand.
      :ets.insert(Tombstones, {id})
      File.mkdir_p!(Shard.data_dir())
      File.write!(Path.join(Shard.data_dir(), id <> ".db"), "tenant data")
      assert [_] = local_files(id), "fixture: #{id} must have a local file"
    end

    Application.put_env(:fathom, :erase_local_stop_timeout_ms, 300)

    Application.put_env(:fathom, :erase_local_stop, fn
      ^b ->
        # A stop that never answers (a coordinator wedged in terminate).
        receive do
          :never -> :ok
        end

      _id ->
        :ok
    end)

    results = Tenants.erase_local_batch(ids)

    assert [{^a, :ok}, {^b, {:error, {:local_erase, {:stop, :timeout}}}}, {^c, :ok}] = results,
           "a hung stop was blamed on the wrong shard (or on none): #{inspect(results)}"

    assert local_files(a) == []
    assert local_files(c) == []
    assert [_] = local_files(b), "the hung shard's local copy must stay for the next sweep"
  end

  test "with no stop hanging every id erases, in input order", %{ids: ids} do
    for id <- ids do
      :ets.insert(Tombstones, {id})
      File.mkdir_p!(Shard.data_dir())
      File.write!(Path.join(Shard.data_dir(), id <> ".db"), "tenant data")
    end

    Application.put_env(:fathom, :erase_local_stop, fn _id -> :ok end)

    assert Enum.map(Tenants.erase_local_batch(ids), &elem(&1, 1)) == [:ok, :ok, :ok]
    for id <- ids, do: assert(local_files(id) == [])
  end
end
