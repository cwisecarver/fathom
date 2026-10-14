defmodule Fathom.ShardCoordinatorHeapTest do
  @moduledoc """
  perf review 2026-10-14 #24 — a held coordinator's heap after open.

  The early `:erlang.garbage_collect/0` in `open_coordinator` shrinks the init high-water mark, but
  the lease-acquire / pull / state-build that FOLLOWS it re-grows the heap (measured 8,960 B per
  coordinator against 4,049 B after one more GC). `open_after_acquire` therefore collects once more
  when the open succeeds. This pins that a freshly opened, idle coordinator holds no more than a
  ceiling that the un-collected heap exceeds (`@tag :bench`: order-of-magnitude floor, excluded from
  the default suite).
  """
  use ExUnit.Case, async: false

  alias Fathom.Shards

  @n 40

  @tag :bench
  test "an opened idle coordinator's heap is post-GC sized" do
    ids = for i <- 1..@n, do: "coordheap_#{System.unique_integer([:positive])}_#{i}"

    handles =
      for id <- ids do
        {:ok, pid, ref, _path} = Shards.checkout(id)
        {id, pid, ref}
      end

    on_exit(fn -> for id <- ids, do: Shards.drain(id, 2_000) end)

    sizes = for {_id, pid, _ref} <- handles, do: elem(:erlang.process_info(pid, :memory), 1)
    median = sizes |> Enum.sort() |> Enum.at(div(@n, 2))

    IO.puts(
      "coordinator_heap_bytes median=#{median} min=#{Enum.min(sizes)} max=#{Enum.max(sizes)}"
    )

    # Un-collected measured ~8,960 B; collected ~4,050 B. 6,500 sits between the two.
    assert median < 6_500
  end
end
