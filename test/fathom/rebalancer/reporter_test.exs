defmodule Fathom.Rebalancer.ReporterTest do
  @moduledoc """
  The per-node reporter: diff two `Fathom.ShardLoad` snapshots into rates and publish the
  hottest to `shard_load_samples`. Not async — `Fathom.ShardLoad` is a shared named ETS
  table; the reporter also writes Postgres from its own process (sandbox-allowed).
  """
  use Fathom.DataCase, async: false

  alias Fathom.Rebalancer
  alias Fathom.Rebalancer.{LoadSamples, Reporter}
  alias Fathom.ShardLoad

  setup do
    prev_load = Application.get_env(:fathom, :shard_load)
    Application.put_env(:fathom, :shard_load, true)
    ShardLoad.reset()

    on_exit(fn ->
      ShardLoad.reset()

      if is_nil(prev_load),
        do: Application.delete_env(:fathom, :shard_load),
        else: Application.put_env(:fathom, :shard_load, prev_load)
    end)

    :ok
  end

  test "publishes a window: per-shard rates for the hottest shards, tagged with this node" do
    # Reporter takes its empty baseline in init (no Repo), so the next report is a real diff.
    pid = start_supervised!(Reporter)
    Ecto.Adapters.SQL.Sandbox.allow(Fathom.Repo, self(), pid)

    # Skewed load: hot_1 gets far more queries than hot_2.
    for _ <- 1..50, do: ShardLoad.record_query("rep_hot_1", 8, 0)
    for _ <- 1..5, do: ShardLoad.record_query("rep_hot_2", 8, 0)

    :ok = Reporter.report_now()

    latest = LoadSamples.latest_per_shard(60_000) |> Map.new(&{&1.shard_id, &1})

    assert Map.has_key?(latest, "rep_hot_1")
    assert Map.has_key?(latest, "rep_hot_2")
    # Both positive rates, and the skew is preserved (hot_1 hotter than hot_2).
    assert latest["rep_hot_1"].q_per_s > 0.0
    assert latest["rep_hot_1"].q_per_s > latest["rep_hot_2"].q_per_s
    assert latest["rep_hot_1"].rows_read_per_s > 0.0
    # Tagged with the reporting node's stable key (= the shard's current serving node).
    assert latest["rep_hot_1"].node_key == Rebalancer.node_key()
  end

  test "a counter reset between windows reports curr/window, not a spurious 0 (#6)" do
    # Regression for #6: an idle shard evicted + cold-re-opened between snapshots resets its
    # ShardLoad row to 0, so curr < prev. The old max(curr-prev,0) clamped that to 0 and the
    # shard was dropped from the window (and its confirm streak reset) — a systematic bias
    # against churny shards, which is the routine case here (LRU evict + cold re-open).
    pid = start_supervised!(Reporter)
    Ecto.Adapters.SQL.Sandbox.allow(Fathom.Repo, self(), pid)

    # Window 1: shard accrues load; reporter's prev baseline becomes {reset_shard: 50}.
    for _ <- 1..50, do: ShardLoad.record_query("reset_shard", 8, 0)
    :ok = Reporter.report_now()

    # Clear window-1's rows so the assertion below can ONLY see what window 2 publishes —
    # otherwise window-1's positive sample masks a window-2 drop-to-0 (within the 60s read).
    LoadSamples.prune(0)

    # Simulate an evict + cold re-open: drop the row, then it re-accrues from 0 (curr < prev).
    ShardLoad.forget("reset_shard")
    for _ <- 1..5, do: ShardLoad.record_query("reset_shard", 8, 0)

    :ok = Reporter.report_now()

    latest = LoadSamples.latest_per_shard(60_000) |> Map.new(&{&1.shard_id, &1})

    assert Map.has_key?(latest, "reset_shard"),
           "reset shard published in window 2 (not dropped as a spurious 0)"

    assert latest["reset_shard"].q_per_s > 0.0
  end

  test "a Postgres outage drops the window without crashing the reporter (#13)" do
    import ExUnit.CaptureLog

    pid = start_supervised!(Reporter)
    for _ <- 1..5, do: ShardLoad.record_query("outage_shard", 8, 0)

    # The outage: cut every non-owner process (incl. the reporter) off from Postgres, so its
    # publish/prune fail. do_report must drop the window (rescue + catch :exit) and the
    # reporter must survive — "a Postgres outage drops a window, never crashes the node".
    Ecto.Adapters.SQL.Sandbox.mode(Fathom.Repo, :manual)

    log = capture_log(fn -> assert Reporter.report_now() == :ok end)
    assert log =~ "load reporter window dropped"
    assert Process.alive?(pid)
  end

  test "top-N cap: only the hottest shards are published" do
    Application.put_env(:fathom, :load_report_top_n, 3)
    on_exit(fn -> Application.delete_env(:fathom, :load_report_top_n) end)

    pid = start_supervised!(Reporter)
    Ecto.Adapters.SQL.Sandbox.allow(Fathom.Repo, self(), pid)

    # 6 shards with descending load; only the top 3 should be published.
    for {shard, n} <- [
          {"c_1", 60},
          {"c_2", 50},
          {"c_3", 40},
          {"c_4", 30},
          {"c_5", 20},
          {"c_6", 10}
        ] do
      for _ <- 1..n, do: ShardLoad.record_query(shard, 1, 0)
    end

    :ok = Reporter.report_now()

    published = LoadSamples.latest_per_shard(60_000) |> Enum.map(& &1.shard_id) |> MapSet.new()
    assert MapSet.size(published) == 3
    assert MapSet.subset?(MapSet.new(["c_1", "c_2", "c_3"]), published)
    refute MapSet.member?(published, "c_6")
  end
end
