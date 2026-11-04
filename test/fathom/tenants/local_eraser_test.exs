defmodule Fathom.Tenants.LocalEraserTest do
  @moduledoc """
  Tombstone local erases are coalesced into batches (expert review 2026-11-04 #6).

  Symptom: every tombstone listed the whole fleet-sized data dir on its own, concurrently, so a delete
  burst of 200 ids against 100k files held +2.8 GB and stalled unrelated shards (see
  `tombstone_burst_bench_test.exs`, which measures it). Invariants pinned here:

    * a burst of N tombstones costs O(1) data-dir listings, not N (fails on the per-id form);
    * `erase_local/1` (the synchronous seam) still returns only after the files are gone, and does not
      wait out the batching window;
    * a sibling id sharing a prefix with a victim is never touched, in a batch with several victims;
    * a listing failure is an error for every id in the batch, never a `:local_erased`.
  """
  use ExUnit.Case, async: false

  alias Fathom.Shard
  alias Fathom.Tenants.Tombstones

  setup do
    prev = Application.get_env(:fathom, :shard_data_dir)
    dir = Path.join(System.tmp_dir!(), "fathom_eraser_test_#{System.unique_integer([:positive])}")
    File.mkdir_p!(dir)
    Application.put_env(:fathom, :shard_data_dir, dir)

    on_exit(fn ->
      if is_nil(prev),
        do: Application.delete_env(:fathom, :shard_data_dir),
        else: Application.put_env(:fathom, :shard_data_dir, prev)

      File.rm_rf(dir)
    end)

    %{dir: dir}
  end

  defp seed!(dir, id),
    do:
      for(s <- [".db", ".db-wal", ".db.fenced.1"], do: File.write!(Path.join(dir, id <> s), "x"))

  defp files(dir, id), do: Path.wildcard(Path.join(dir, id <> ".db*"))

  test "a burst of tombstones costs a bounded number of data-dir listings", %{dir: dir} do
    ids = for i <- 1..60, do: "burst#{System.unique_integer([:positive])}_#{i}"
    for id <- ids, do: seed!(dir, id)
    File.write!(Path.join(dir, "bystander.db"), "x")
    on_exit(fn -> for id <- ids, do: :ets.delete(Tombstones, id) end)

    me = self()
    ref = make_ref()

    :telemetry.attach(
      "eraser-burst-#{inspect(ref)}",
      [:fathom, :tenants, :local_erased],
      fn _, _, %{shard_id: id}, pid -> send(pid, {:erased, id}) end,
      me
    )

    on_exit(fn -> :telemetry.detach("eraser-burst-#{inspect(ref)}") end)

    :erlang.trace_pattern({Fathom.FS, :list_dir_isolated, 2}, true, [:local])
    :erlang.trace(:all, true, [:call, {:tracer, me}])

    try do
      for id <- ids, do: Tombstones.put(id)
      for _ <- ids, do: assert_receive({:erased, _}, 10_000)
    after
      :erlang.trace(:all, false, [:call])
      :erlang.trace_pattern({Fathom.FS, :list_dir_isolated, 2}, false, [:local])
    end

    listings = count_listings(0)

    assert listings <= 3,
           "#{length(ids)} tombstones did #{listings} data-dir listings; expected a coalesced batch"

    for id <- ids, do: assert(files(dir, id) == [])
    assert File.exists?(Path.join(dir, "bystander.db"))
  end

  defp count_listings(n) do
    receive do
      {:trace, _, :call, {Fathom.FS, :list_dir_isolated, _}} -> count_listings(n + 1)
    after
      0 -> n
    end
  end

  test "erase_local/1 is synchronous and does not wait out the batching window", %{dir: dir} do
    id = "syncer#{System.unique_integer([:positive])}"
    seed!(dir, id)
    :ets.insert(Tombstones, {id})
    on_exit(fn -> :ets.delete(Tombstones, id) end)

    {us, :ok} = :timer.tc(fn -> Fathom.Tenants.erase_local(id) end)

    assert files(dir, id) == [], "erase_local returned before the files were gone"
    assert us < 400_000, "erase_local waited #{div(us, 1000)} ms (the window is 500 ms)"
  end

  test "a batch erases every tombstoned id and never a prefix-sharing sibling", %{dir: dir} do
    base = "pfx#{System.unique_integer([:positive])}"
    [a, b] = [base <> "a", base <> "b"]
    live = [base <> "a9", base, base <> "-x"]
    for id <- [a, b] ++ live, do: seed!(dir, id)
    for id <- [a, b], do: :ets.insert(Tombstones, {id})
    on_exit(fn -> for id <- [a, b], do: :ets.delete(Tombstones, id) end)

    assert [{^a, :ok}, {^b, :ok}] = Fathom.Tenants.erase_local_batch([a, b])

    for id <- [a, b], do: assert(files(dir, id) == [])
    for id <- live, do: assert(length(files(dir, id)) == 3, "erase reached sibling #{id}")
  end

  test "a data-dir listing failure fails every id in the batch and emits no :local_erased",
       %{dir: dir} do
    ids = for i <- 1..3, do: "lf#{System.unique_integer([:positive])}_#{i}"
    for id <- ids, do: :ets.insert(Tombstones, {id})
    on_exit(fn -> for id <- ids, do: :ets.delete(Tombstones, id) end)

    not_a_dir = Path.join(dir, "notadir")
    File.write!(not_a_dir, "")
    Application.put_env(:fathom, :shard_data_dir, not_a_dir)

    me = self()

    :telemetry.attach_many(
      "eraser-lf",
      [[:fathom, :tenants, :local_erased], [:fathom, :tenants, :local_erase_failed]],
      fn ev, _, %{shard_id: id}, pid -> send(pid, {:ev, List.last(ev), id}) end,
      me
    )

    on_exit(fn -> :telemetry.detach("eraser-lf") end)

    results = Fathom.Tenants.erase_local_batch(ids)

    for {_id, r} <- results, do: assert({:error, {:local_erase, _}} = r)
    for id <- ids, do: assert_received({:ev, :local_erase_failed, ^id})
    refute_received {:ev, :local_erased, _}
    _ = Shard
  end
end
