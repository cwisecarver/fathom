defmodule Fathom.Shard.FlushJitterTest do
  @moduledoc """
  The durability-flush jitter knobs (expert review 2026-10-22 r8 #11).

  `SHARD_FLUSH_JITTER_RATIO` is the rig's A/B lever for flush-timer phase lock after a mass
  (re)open, so it must reach `:shard_flush_jitter_ratio` from the release env without a rebuild,
  and a malformed value must fail the boot rather than silently keep the default. Not async — it
  mutates the process environment.
  """
  use ExUnit.Case, async: false

  defp fathom_config(env_overlay) do
    prev = Map.new(env_overlay, fn {k, _} -> {k, System.get_env(k)} end)

    Enum.each(env_overlay, fn
      {k, nil} -> System.delete_env(k)
      {k, v} -> System.put_env(k, v)
    end)

    try do
      "config/runtime.exs"
      |> Config.Reader.read!(env: :test)
      |> Keyword.get(:fathom, [])
    after
      Enum.each(prev, fn
        {k, nil} -> System.delete_env(k)
        {k, v} -> System.put_env(k, v)
      end)
    end
  end

  describe "flush_delay/3 (pure)" do
    # The re-arm walk is base ± ratio·base, never below 1.
    test ":rearm stays within ±ratio of the base" do
      draws = for _ <- 1..2_000, do: Fathom.Shard.flush_delay(:rearm, 10_000, 0.25)
      assert Enum.min(draws) >= 7_500 and Enum.max(draws) <= 12_500
      assert Enum.min(draws) < 8_000 and Enum.max(draws) > 12_000, "the walk must use its window"
    end

    # THE FIRST ARM SPANS A WHOLE INTERVAL (expert review 2026-10-22 r8 #11). Symptom it pins:
    # co-opened coordinators drew their first tick from the same ±25% window and flushed in
    # lockstep for ~4 intervals. The draw must cover (0, base], not [0.75, 1.25]·base.
    test ":first is uniform over (0, base] whenever jitter is on" do
      draws = for _ <- 1..4_000, do: Fathom.Shard.flush_delay(:first, 10_000, 0.25)
      assert Enum.min(draws) >= 1 and Enum.max(draws) <= 10_000
      assert Enum.min(draws) < 500, "the first arm must reach the start of the interval"
      assert Enum.max(draws) > 9_500, "the first arm must reach the end of the interval"

      # Each quarter of the interval gets about a quarter of the draws (uniform phase).
      for q <- 0..3 do
        n = Enum.count(draws, &(&1 > q * 2_500 and &1 <= (q + 1) * 2_500))
        assert n in 800..1_200, "quarter #{q} got #{n} of 4000 draws"
      end
    end

    test "ratio 0 (deterministic tests) disables both draws" do
      assert Fathom.Shard.flush_delay(:first, 10_000, 0.0) == 10_000
      assert Fathom.Shard.flush_delay(:rearm, 10_000, 0.0) == 10_000
      assert Fathom.Shard.flush_delay(:first, 10_000, 0) == 10_000
    end
  end

  describe "a coordinator's armed timers" do
    alias Fathom.{ShardExecutor, Shards}
    alias Filo.Stmt

    setup do
      prev =
        for k <- [:shard_flush_jitter_ratio, :shard_flush_interval_ms, :shard_idle_ms],
            do: {k, Application.fetch_env(:fathom, k)}

      Application.put_env(:fathom, :shard_flush_jitter_ratio, 0.25)
      Application.put_env(:fathom, :shard_flush_interval_ms, 100_000)
      Application.put_env(:fathom, :shard_idle_ms, 600_000)
      ids = for i <- 1..12, do: "fjit_#{System.unique_integer([:positive])}_#{i}"

      on_exit(fn ->
        for {k, v} <- prev do
          case v do
            {:ok, val} -> Application.put_env(:fathom, k, val)
            :error -> Application.delete_env(:fathom, k)
          end
        end

        for id <- ids, do: _ = Shards.drain(id)

        for id <- ids,
            dir <- [Fathom.Shard.data_dir(), Fathom.Shard.Storage.Local.dir()],
            suffix <- [".db", ".db-wal", ".db-shm", ".db.etag", ".lock"],
            do: File.rm(Path.join(dir, id <> suffix))
      end)

      %{ids: ids}
    end

    # Discriminating: under the old ±25% first arm every remaining delay is >= 75 s minus the few
    # ms since the arm; under the whole-interval draw P(all 12 >= 75 s) = 0.25^12 ≈ 6e-8.
    test "the first arm of a coordinator's life lands anywhere in the interval", %{ids: ids} do
      conns = for id <- ids, do: elem(ShardExecutor.open(id), 1)

      remaining =
        for id <- ids do
          {:ok, pid} = Shards.ensure(id)
          state = :sys.get_state(pid)
          assert state.flush_first_armed?, "the checkout grant armed the first tick"
          Process.read_timer(state.flush_timer)
        end

      for c <- conns, do: ShardExecutor.close(c)

      assert Enum.all?(remaining, &(is_integer(&1) and &1 > 0 and &1 <= 100_000))

      assert Enum.min(remaining) < 70_000,
             "no first arm fell below the old ±25% floor: #{inspect(Enum.sort(remaining))}"
    end

    # The whole-interval draw is for the FIRST arm only: after a flush, a dirty-writing shard's
    # re-arm walks ±ratio again (so steady-state cadence and the RPO bound are unchanged).
    test "the arm after a flush is the ±ratio re-arm, not another first draw", %{ids: [id | _]} do
      {:ok, conn} = ShardExecutor.open(id)
      {:ok, _} = ShardExecutor.execute(conn, %Stmt{sql: "CREATE TABLE t (v INTEGER)"})
      {:ok, pid} = Shards.ensure(id)
      :ok = Shards.flush(id)
      state = :sys.get_state(pid)
      left = Process.read_timer(state.flush_timer)
      ShardExecutor.close(conn)

      refute state.flush_timer_slow?, "a shard that wrote this checkout stays at full rate"

      assert is_integer(left) and left > 70_000 and left <= 125_000,
             "the post-flush re-arm must be the ±25% walk, got #{inspect(left)} ms"
    end
  end

  describe "SHARD_FLUSH_JITTER_RATIO" do
    test "a number in [0, 1] is written as a float" do
      for {raw, want} <- [{"0.25", 0.25}, {"1.0", 1.0}, {"1", 1.0}, {"0", 0.0}, {" 0.5 ", 0.5}] do
        assert Keyword.fetch!(
                 fathom_config(%{"SHARD_FLUSH_JITTER_RATIO" => raw}),
                 :shard_flush_jitter_ratio
               ) == want,
               "SHARD_FLUSH_JITTER_RATIO=#{inspect(raw)}"
      end
    end

    test "unset leaves the key unwritten (the compiled/test default stands)" do
      refute Keyword.has_key?(
               fathom_config(%{"SHARD_FLUSH_JITTER_RATIO" => nil}),
               :shard_flush_jitter_ratio
             )
    end

    test "a malformed or out-of-range value is a boot error naming the variable" do
      for bad <- ["abc", "-0.1", "1.5", "0.25x", ""] do
        err =
          assert_raise RuntimeError, fn ->
            fathom_config(%{"SHARD_FLUSH_JITTER_RATIO" => bad})
          end

        assert err.message =~ "SHARD_FLUSH_JITTER_RATIO"
      end
    end
  end
end
