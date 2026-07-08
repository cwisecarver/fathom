defmodule Fathom.Rebalancer.RebalanceJobTest do
  @moduledoc "The cron control loop: read merged load, decide moves, enqueue handoffs."
  use Fathom.DataCase, async: false
  use Oban.Testing, repo: Fathom.Repo

  alias Fathom.Rebalancer.{Command, Commands, HandoffJob, LoadSample, RebalanceJob}
  alias Fathom.Repo

  import Ecto.Query, only: [from: 2]

  setup do
    prev = for k <- config_keys(), into: %{}, do: {k, Application.get_env(:fathom, k)}

    Application.put_env(:fathom, :lb_backends, %{
      "n1" => "n1:8080",
      "n2" => "n2:8080",
      "n3" => "n3:8080"
    })

    on_exit(fn ->
      Enum.each(prev, fn
        {k, nil} -> Application.delete_env(:fathom, k)
        {k, v} -> Application.put_env(:fathom, k, v)
      end)
    end)

    :ok
  end

  defp config_keys,
    do: [:rebalancer_enabled, :lb_backends, :rebalance_hot_qps_floor, :rebalance_confirm_windows]

  defp sample(node, shard, q, age_ms) do
    at = DateTime.add(DateTime.utc_now(), -age_ms, :millisecond)

    Repo.insert!(%LoadSample{
      node_key: node,
      shard_id: shard,
      q_per_s: q / 1,
      rows_read_per_s: q * 8 / 1,
      checkouts_per_s: q / 4,
      window_s: 10.0,
      sampled_at: at
    })
  end

  test "gate off (default): reads nothing, enqueues nothing" do
    sample("n1", "hot_1", 900, 0)
    assert :ok = perform_job(RebalanceJob, %{})
    refute_enqueued(worker: HandoffJob)
  end

  test "gate on: a confirmed hot shard on an overloaded node enqueues a handoff" do
    Application.put_env(:fathom, :rebalancer_enabled, true)
    Application.put_env(:fathom, :rebalance_hot_qps_floor, 500.0)
    Application.put_env(:fathom, :rebalance_confirm_windows, 2)

    # n1 overloaded (hot_1 900 + filler 300, two windows of hot_1); n2/n3 cold.
    sample("n1", "hot_1", 900, 10_000)
    sample("n1", "hot_1", 900, 0)
    sample("n1", "filler", 300, 0)
    sample("n2", "warm", 40, 0)
    sample("n3", "cool", 10, 0)

    assert :ok = perform_job(RebalanceJob, %{})

    assert_enqueued(worker: HandoffJob, args: %{"shard_id" => "hot_1", "from_node" => "n1"})
    [job] = all_enqueued(worker: HandoffJob)
    assert job.args["to_node"] == "n3", "least-loaded target"
  end

  test "an enabled tick sweeps the command channel: prunes terminal + expires stale (#12)" do
    Application.put_env(:fathom, :rebalancer_enabled, true)
    Application.put_env(:fathom, :rebalance_hot_qps_floor, 500.0)

    {:ok, old_done} = Commands.issue("s1", "n1", "drain")
    {:ok, _} = Commands.complete(old_done, "done", "drained")
    {:ok, old_pending} = Commands.issue("s2", "n1", "warm")

    # Age both ~2h so they clear the default retention (1h) / stale (15min) windows.
    at = DateTime.add(DateTime.utc_now(), -7_200_000, :millisecond)

    Repo.update_all(from(c in Command, where: c.id in ^[old_done.id, old_pending.id]),
      set: [inserted_at: at, updated_at: at]
    )

    assert :ok = perform_job(RebalanceJob, %{})

    assert Commands.get(old_done.id) == nil, "old terminal pruned"
    assert Commands.get(old_pending.id).status == "failed", "old pending expired"
  end

  test "gate on but nothing hot: no handoffs" do
    Application.put_env(:fathom, :rebalancer_enabled, true)
    Application.put_env(:fathom, :rebalance_hot_qps_floor, 500.0)

    sample("n1", "a", 50, 0)
    sample("n2", "b", 40, 0)

    assert :ok = perform_job(RebalanceJob, %{})
    refute_enqueued(worker: HandoffJob)
  end
end
