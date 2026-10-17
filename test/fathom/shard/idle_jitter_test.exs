defmodule Fathom.Shard.IdleJitterTest do
  @moduledoc """
  The idle-stop deadline is spread ±10% per coordinator (expert review 2026-10-17 #28).

  Without it a cohort of coordinators opened together (a mass re-home) or idled together (a traffic
  stop on a tenant fleet) idle-stops in lockstep: N drop flushes and lease releases in the same
  instant. The deadline is decided once at init (`state.idle_ms`), which is the only value the
  `:idle_timeout` handler and `schedule_idle/1` read.

  Pinned at the pure-function level (AGENTS.md: a decision about timing is tested as a function, not
  by waiting out a timer), plus one coordinator-level check that the spread reaches `state`.

  Deliberate non-spread: BELOW 1 s the deadline is exact. Tests (`idle_ms: 1` and friends) pin tiny
  values and must not become flaky; `config/test.exs` also sets the ratio to 0 for the longer ones.
  """
  use ExUnit.Case, async: false

  alias Fathom.{ShardExecutor, Shards}

  describe "jitter_idle_ms/2" do
    test "stays within ±ratio of the base and actually varies" do
      samples = for _ <- 1..500, do: Fathom.Shard.jitter_idle_ms(60_000, 0.1)

      assert Enum.all?(samples, &(&1 >= 54_000 and &1 <= 66_000)),
             "a sample left the ±10% band: #{inspect(Enum.min_max(samples))}"

      # Both sides of the base, and not a constant: a stuck spread would re-create the lockstep.
      assert Enum.min(samples) < 60_000 and Enum.max(samples) > 60_000
      assert length(Enum.uniq(samples)) > 50
    end

    test "is exact below the floor, for a zero ratio, and caps an absurd ratio at ±50%" do
      for base <- [1, 50, 999], do: assert(Fathom.Shard.jitter_idle_ms(base, 0.1) == base)
      assert Fathom.Shard.jitter_idle_ms(60_000, 0) == 60_000
      assert Fathom.Shard.jitter_idle_ms(60_000, 0.0) == 60_000
      assert Fathom.Shard.jitter_idle_ms(60_000, -1) == 60_000

      capped = for _ <- 1..200, do: Fathom.Shard.jitter_idle_ms(60_000, 9.0)
      assert Enum.all?(capped, &(&1 >= 30_000 and &1 <= 90_000))
    end
  end

  test "a coordinator's idle deadline carries the spread; a pinned tiny one stays exact" do
    prev_idle = Application.get_env(:fathom, :shard_idle_ms)
    prev_ratio = Application.get_env(:fathom, :shard_idle_jitter_ratio)
    Application.put_env(:fathom, :shard_idle_jitter_ratio, 0.1)

    on_exit(fn ->
      restore(:shard_idle_ms, prev_idle)
      restore(:shard_idle_jitter_ratio, prev_ratio)
    end)

    open = fn id ->
      on_exit(fn -> Shards.stop(id) end)
      {:ok, conn} = ShardExecutor.open(id)
      {:ok, coordinator} = Shards.ensure(id)
      on_exit(fn -> ShardExecutor.close(conn) end)
      :sys.get_state(coordinator).idle_ms
    end

    Application.put_env(:fathom, :shard_idle_ms, 60_000)
    wide = for _ <- 1..12, do: open.("idlejit_#{System.unique_integer([:positive])}")
    assert Enum.all?(wide, &(&1 >= 54_000 and &1 <= 66_000))
    assert length(Enum.uniq(wide)) > 1, "twelve coordinators drew the same idle deadline"

    Application.put_env(:fathom, :shard_idle_ms, 1)
    assert open.("idlejit_#{System.unique_integer([:positive])}") == 1
  end

  defp restore(key, nil), do: Application.delete_env(:fathom, key)
  defp restore(key, value), do: Application.put_env(:fathom, key, value)
end
