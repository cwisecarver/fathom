defmodule Fathom.Admin.MemoryMeasurementsTest do
  # Expert review 2026-10-17 #33: the poller called :erlang.memory twice per tick (node_memory/0 and
  # memory_breakdown/0). One call now feeds both events; the node-total gauge must still be emitted
  # and the poller must not list a second memory measurement.
  #
  # Expert review 2026-10-21 r7 #3: `:erlang.memory/0` sums allocated BLOCKS, but SQLite's page
  # caches pin driver_alloc CARRIERS, which go back to the OS only when completely empty. After a
  # connection churn the blocks figure falls while RSS stays at its peak, so every density number
  # was misreadable. memory_breakdown/0 now also reports carriers, driver_alloc carriers/blocks,
  # their ratio and (Linux) the OS RSS, and these tests pin that the gauges exist and that they see
  # SQLite allocations.
  # Not async: the SQLite-allocation test reads a node-wide allocator delta that a concurrent
  # test freeing driver memory could mask.
  use ExUnit.Case, async: false

  alias Fathom.Admin.Measurements

  defp capture_breakdown do
    ref = make_ref()
    test = self()

    :telemetry.attach_many(
      {__MODULE__, ref},
      [[:fathom, :node, :memory], [:fathom, :node, :memory_breakdown]],
      fn event, m, _meta, _ -> send(test, {ref, event, m}) end,
      nil
    )

    on_exit(fn -> :telemetry.detach({__MODULE__, ref}) end)
    Measurements.memory_breakdown()
    ref
  end

  test "memory_breakdown/0 emits both the breakdown and the node total from one call" do
    ref = capture_breakdown()

    assert_receive {^ref, [:fathom, :node, :memory], %{total: total}} when total > 0
    assert_receive {^ref, [:fathom, :node, :memory_breakdown], %{processes: _, ets: _, system: _}}
  end

  test "memory_breakdown/0 reports carriers next to blocks, so churn RSS cannot be misread" do
    ref = capture_breakdown()
    assert_receive {^ref, [:fathom, :node, :memory_breakdown], m}

    # A carrier is at least as large as the blocks living in it, for every slice.
    assert m.allocated_blocks > 0
    assert m.allocated_carriers >= m.allocated_blocks
    assert m.driver_carriers >= m.driver_blocks

    if m.driver_blocks > 0 do
      assert m.driver_fragmentation >= 1.0
    end

    case :os.type() do
      {:unix, :linux} -> assert m.rss > 0
      _ -> :ok
    end
  end

  test "driver_alloc gauges see SQLite allocations (where the page caches live)" do
    ref = capture_breakdown()
    assert_receive {^ref, [:fathom, :node, :memory_breakdown], before}

    {:ok, conn} = Exqlite.Sqlite3.open(":memory:")
    :ok = Exqlite.Sqlite3.execute(conn, "CREATE TABLE t(v BLOB)")

    # 16 x 512 KiB rows held in an in-memory database: SQLite allocates its pages through the
    # driver allocator, so the gauge must grow by most of the 8 MiB.
    for _ <- 1..16 do
      :ok = Exqlite.Sqlite3.execute(conn, "INSERT INTO t VALUES (randomblob(524288))")
    end

    Measurements.memory_breakdown()
    assert_receive {^ref, [:fathom, :node, :memory_breakdown], after_alloc}

    assert after_alloc.driver_blocks - before.driver_blocks > 4 * 1024 * 1024
    assert after_alloc.driver_carriers >= after_alloc.driver_blocks

    Exqlite.Sqlite3.close(conn)
  end

  test "sum_allocator_sizes/1 sums mbcs, sbcs and pooled carriers and blocks" do
    instances = [
      {:instance, 0,
       [
         mbcs: [
           {:blocks, [driver_alloc: [{:size, 100, 100, 200}]]},
           {:carriers_size, 1000, 1000, 1000}
         ],
         mbcs_pool: [blocks: [driver_alloc: [size: 10]], carriers_size: 50],
         sbcs: [{:blocks, [driver_alloc: [{:size, 7, 7, 7}]]}, {:carriers_size, 64, 64, 64}]
       ]},
      {:instance, 1,
       [mbcs: [{:blocks, []}, {:carriers_size, 500, 500, 500}], sbcs: [{:blocks, []}]]},
      # Instances/areas that are not size maps are ignored, not crashes.
      {:versions, ~c"0.9", ~c"3.0"},
      {:instance, 2, [{:options, [e: true]}]}
    ]

    assert Measurements.sum_allocator_sizes(instances) == {1000 + 50 + 64 + 500, 100 + 10 + 7}
  end

  test "rss_bytes/1 parses VmRSS from a /proc status body" do
    status = "Name:\tbeam.smp\nVmHWM:\t  999999 kB\nVmRSS:\t  123456 kB\nThreads:\t40\n"

    assert Measurements.rss_bytes(status) == 123_456 * 1024
    # The high-water mark line must not be mistaken for the resident set.
    assert Measurements.rss_bytes("VmHWM:\t5 kB\n") == nil
    assert Measurements.rss_bytes("") == nil
  end
end
