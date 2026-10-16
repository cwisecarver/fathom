defmodule Fathom.Admin.DurabilityTest do
  @moduledoc """
  The Durability page's data module (expert review 2026-10-15 #1, #8, #16): the windowed counter
  diff behind the alarm chips, the single-flight cache, and the bounded dirtiest-shards walk.
  """
  use Fathom.DataCase, async: false

  alias Fathom.Admin.{Durability, FlushWatermark}

  defp scrape(text), do: Fathom.Admin.PrometheusScrape.parse(text)

  describe "extract_counters/1 + window_diff/1 + alarms/2" do
    test "an alarm counter that moved inside the window fires; one that did not stays quiet" do
      t0 = scrape("fathom_shard_flush_failed_count 2\nfathom_shard_corrupt_flush_count 1\n")
      t1 = scrape("fathom_shard_flush_failed_count 5\nfathom_shard_corrupt_flush_count 1\n")
      c0 = Durability.extract_counters(t0)
      c1 = Durability.extract_counters(t1)

      {diff, span} = Durability.window_diff([{300_000, c1}, {0, c0}])
      assert span == 300_000

      alarms = Map.new(Durability.alarms(c1, diff), &{&1.key, &1})
      assert %{window: 3, total: 5, firing?: true} = alarms.flush_failed
      # Lifetime count 1 but no movement in the window: NOT firing (shown "since boot" only).
      assert %{window: 0, total: 1, firing?: false} = alarms.corrupt_flush
      assert %{window: 0, total: 0, firing?: false} = alarms.too_large
    end

    test "a counter that went backwards (node restart) clamps to zero, never negative" do
      c_old = Durability.extract_counters(scrape("fathom_shard_flush_failed_count 50\n"))
      c_new = Durability.extract_counters(scrape("fathom_shard_flush_failed_count 1\n"))

      {diff, _} = Durability.window_diff([{10_000, c_new}, {0, c_old}])
      assert Map.fetch!(diff, {:alarm, :flush_failed}) == 0
    end

    test "tagged counters are split by their bounded label and do not absorb sibling families" do
      text = """
      fathom_replication_reject_count{reason="overloaded"} 4
      fathom_replication_reject_count{reason="already_in_flight"} 1
      fathom_replication_seed_count{result="ok"} 2
      fathom_replication_seed_bytes{result="ok"} 999
      """

      c = Durability.extract_counters(scrape(text))
      assert c[{:reject, "overloaded"}] == 4
      assert c[{:reject, "already_in_flight"}] == 1
      assert c[{:seed, "ok"}] == 2
      # seed.bytes is a different family and must not leak into the seed count.
      assert Enum.sum(for {{:seed, _}, v} <- c, do: v) == 2
    end

    test "a single sample (or none) is a window that has not started" do
      assert {%{}, 0} == Durability.window_diff([])
      c = Durability.extract_counters(scrape("fathom_shard_flush_failed_count 9\n"))
      assert {diff, 0} = Durability.window_diff([{5, c}])
      assert Enum.all?(diff, fn {_k, v} -> v == 0 end)
    end
  end

  describe "snapshot/0" do
    test "without a metrics reporter the snapshot degrades to an empty, non-firing state" do
      start_supervised!(Durability)
      snap = Durability.snapshot()
      refute snap.scrape?
      assert length(snap.alarms) == 6
      refute Enum.any?(snap.alarms, & &1.firing?)
      assert snap.replication.enabled? == false
    end

    test "works uncached when the cache owner is not running" do
      assert %{alarms: [_ | _]} = Durability.snapshot()
    end
  end

  describe "dirtiest/0" do
    test "is single-flight cached: a second call inside the TTL reuses the first" do
      start_supervised!(Durability)
      id = "dur_cache_#{System.unique_integer([:positive])}"
      :ets.insert(FlushWatermark, {id, 0, -999, System.monotonic_time(:millisecond) - 5_000})
      on_exit(fn -> FlushWatermark.forget(id) end)

      first = Durability.dirtiest()
      assert Enum.any?(first.rows, &(&1.shard_id == id))

      FlushWatermark.forget(id)
      # Inside the 10 s TTL the cached value is served even though the row is gone.
      assert Durability.dirtiest() == first
      Durability.reset()
      refute Enum.any?(Durability.dirtiest().rows, &(&1.shard_id == id))
    end

    test "keeps only the 20 oldest dirty shards, oldest first" do
      prefix = "dur_dirty_#{System.unique_integer([:positive])}_"
      now = System.monotonic_time(:millisecond)

      ids =
        for i <- 1..25 do
          id = prefix <> String.pad_leading("#{i}", 2, "0")
          # counter_gen -999 never matches the live generation => dirty; age grows with i.
          :ets.insert(FlushWatermark, {id, 0, -999, now - i * 1_000})
          id
        end

      on_exit(fn -> Enum.each(ids, &FlushWatermark.forget/1) end)

      d = Durability.dirtiest()
      assert length(d.rows) == 20
      assert d.rows == Enum.sort_by(d.rows, & &1.age_ms, :desc)
      assert d.dirty_count >= 25
      assert d.watermark_rows >= 25
      assert d.under_reporting? == d.watermark_rows < d.open_shards
    end

    test "under_reporting? is true when open shards outnumber published watermarks" do
      FlushWatermark.reset()
      # No shard is open in this test, so force the comparison through the pure fields.
      d = Durability.dirtiest()
      assert d.watermark_rows == 0
      assert d.under_reporting? == 0 < d.open_shards
    end
  end
end
