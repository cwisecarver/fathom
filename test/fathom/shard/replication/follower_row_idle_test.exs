defmodule Fathom.Shard.Replication.FollowerRowIdleTest do
  # expert review 2026-11-06 #1. The replica row sweep derived its cutoff from a stale 60_000
  # literal while Fathom.Shard's idle default moved to 300 s (2026-11-04), so prod swept rows at
  # 600 s instead of the intended 10 x 300 s = 3000 s. Pins the derivation to the one public source.
  use ExUnit.Case, async: false

  alias Fathom.Shard
  alias Fathom.Shard.Replication.Follower

  setup do
    keys = [:shard_idle_ms, :replication_row_idle_ms]
    saved = for k <- keys, do: {k, Application.fetch_env(:fathom, k)}
    Enum.each(keys, &Application.delete_env(:fathom, &1))

    on_exit(fn ->
      for {k, v} <- saved do
        case v do
          {:ok, val} -> Application.put_env(:fathom, k, val)
          :error -> Application.delete_env(:fathom, k)
        end
      end
    end)
  end

  test "with nothing set, the row sweep is 10 x the Shard idle default (3000 s)" do
    assert Shard.idle_ms() == 300_000
    assert Follower.row_idle_ms() == 10 * Shard.idle_ms()
    assert Follower.row_idle_ms() == 3_000_000
  end

  test "an explicit :shard_idle_ms is honored" do
    Application.put_env(:fathom, :shard_idle_ms, 7_000)
    assert Follower.row_idle_ms() == 70_000
  end

  test "an explicit :replication_row_idle_ms wins" do
    Application.put_env(:fathom, :replication_row_idle_ms, 1_234)
    assert Follower.row_idle_ms() == 1_234
  end
end
