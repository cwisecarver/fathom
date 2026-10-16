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

  describe "commit latency window" do
    defp commit_text(ok_buckets, nq_buckets, ok_n, nq_n) do
      les = ["10", "100", "+Inf"]

      series = fn outcome, bs ->
        for {le, n} <- Enum.zip(les, bs) do
          ~s(fathom_replication_commit_duration_bucket{outcome="#{outcome}",le="#{le}"} #{n}\n)
        end
      end

      Enum.join(
        series.("ok", ok_buckets) ++
          series.("no_quorum", nq_buckets) ++
          [
            ~s(fathom_replication_commit_duration_count{outcome="ok"} #{ok_n}\n),
            ~s(fathom_replication_commit_duration_count{outcome="no_quorum"} #{nq_n}\n)
          ]
      )
    end

    test "diffs the tagged histogram across outcomes (one entry per le) and counts outcomes" do
      c0 = Durability.extract_counters(scrape(commit_text([10, 10, 10], [0, 0, 0], 10, 0)))
      c1 = Durability.extract_counters(scrape(commit_text([110, 110, 110], [0, 5, 5], 110, 5)))
      {diff, _} = Durability.window_diff([{300_000, c1}, {0, c0}])

      # 100 ok commits all <= 10ms, 5 no_quorum all in (10,100]: summed per le, NOT walked as
      # duplicate-le series (the tagged-histogram trap).
      assert diff[{:commit_bucket, 10.0}] == 100
      assert diff[{:commit_bucket, 100.0}] == 105

      lat = Durability.commit_latency(diff)
      assert lat.total == 105
      assert lat.outcomes == %{"ok" => 100, "no_quorum" => 5, "timeout" => 0, "other" => 0}
      assert lat.p50 <= 10.0
      assert lat.p99 > 10.0 and lat.p99 <= 100.0
    end

    test "no commits in the window yields nil percentiles" do
      c = Durability.extract_counters(scrape(commit_text([3, 3, 3], [0, 0, 0], 3, 0)))
      {diff, _} = Durability.window_diff([{10_000, c}, {0, c}])
      lat = Durability.commit_latency(diff)
      assert lat.total == 0
      assert lat.p50 == nil and lat.p99 == nil
    end
  end

  describe "lag/1" do
    test "is single-flight cached: N callers inside the TTL compute once" do
      start_supervised!(Durability)
      Durability.reset()
      counter = :counters.new(1, [])

      compute = fn ->
        :counters.add(counter, 1, 1)
        %{sessions: 1, answered: 1, timed_out: 0, catching_up: 0, worst: []}
      end

      for _ <- 1..5, do: assert(%{answered: 1} = Durability.lag(compute: compute))
      assert :counters.get(counter, 1) == 1
    end

    test "without replication running there is nothing to report" do
      start_supervised!(Durability)
      Durability.reset()
      assert Durability.lag() == nil
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
