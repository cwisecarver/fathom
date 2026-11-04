defmodule Fathom.Shard.TempReaperMemoryBenchTest do
  @moduledoc """
  Watch-only measurement for expert review 2026-11-04 #16: after a sweep of a ~400k-entry data dir
  the `TempReaper` process must not retain the whole name list. Prints `Process.info(reaper, :memory)`
  right after the sweep; asserts a generous ceiling (10 MB, the review's drop threshold) so a
  regression to retaining the listing fails. Run: `mix test --include bench <this file>`.
  """
  use ExUnit.Case, async: false
  @moduletag :bench
  @moduletag timeout: 600_000

  alias Fathom.Shard.TempReaper

  @stubs 400_000

  test "reaper memory after sweeping #{@stubs} entries" do
    prev = Application.get_env(:fathom, :shard_data_dir)
    dir = Path.join(System.tmp_dir!(), "fathom_reaper_mem_#{System.unique_integer([:positive])}")
    File.mkdir_p!(dir)
    Application.put_env(:fathom, :shard_data_dir, dir)

    on_exit(fn ->
      if is_nil(prev),
        do: Application.delete_env(:fathom, :shard_data_dir),
        else: Application.put_env(:fathom, :shard_data_dir, prev)

      File.rm_rf(dir)
    end)

    start_supervised!(TempReaper)
    _ = TempReaper.sweep()

    for i <- 1..@stubs, do: :ok = Fathom.FS.write(Path.join(dir, "stub#{i}.db"), "")
    # One real orphan so the sweep has something to reap.
    pid = Process.whereis(TempReaper)
    before = Process.info(pid, :memory) |> elem(1)

    {us, _} = :timer.tc(fn -> TempReaper.sweep() end)
    {:memory, after_sweep} = Process.info(pid, :memory)
    {:total_heap_size, heap_words} = Process.info(pid, :total_heap_size)

    IO.puts("""

    REAPER_MEM_BENCH stubs=#{@stubs}
      sweep_ms=#{div(us, 1000)}
      before_mb=#{Float.round(before / 1_048_576, 2)}
      after_sweep_mb=#{Float.round(after_sweep / 1_048_576, 2)} (heap words #{heap_words})
    """)

    assert after_sweep < 10 * 1_048_576
  end
end
