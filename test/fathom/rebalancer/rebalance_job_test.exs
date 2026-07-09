defmodule Fathom.Rebalancer.RebalanceJobTest do
  @moduledoc "The cron control loop: read merged load, decide moves, enqueue handoffs."
  use Fathom.DataCase, async: false
  use Oban.Testing, repo: Fathom.Repo

  alias Fathom.Rebalancer.{
    Command,
    Commands,
    HandoffJob,
    LoadSample,
    Nodes,
    Overrides,
    RebalanceJob,
    WarmLocations
  }

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

  test "an enabled tick re-renders the LB map from the override table (#10)" do
    Application.put_env(:fathom, :rebalancer_enabled, true)
    Application.put_env(:fathom, :rebalance_hot_qps_floor, 500.0)

    map_path =
      Path.join(System.tmp_dir!(), "fathom_reblb_#{System.unique_integer([:positive])}.conf")

    prev_map = Application.get_env(:fathom, :lb_map_path)
    Application.put_env(:fathom, :lb_map_path, map_path)

    on_exit(fn ->
      if is_nil(prev_map),
        do: Application.delete_env(:fathom, :lb_map_path),
        else: Application.put_env(:fathom, :lb_map_path, prev_map)

      File.rm(map_path)
      for f <- Path.wildcard(map_path <> ".tmp.*"), do: File.rm(f)
    end)

    {:ok, _} = Fathom.Rebalancer.Overrides.pin("hot_1", "n2", reason: "test")
    # Drift: the on-disk map is missing the pin (a raced/failed prior apply).
    File.write!(map_path, "# stale\n")

    assert :ok = perform_job(RebalanceJob, %{})
    # The periodic re-render healed the drift from the current override table.
    assert File.read!(map_path) =~ "hot_1."
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

  test "reconcile unpins a pin whose node is dead so the shard re-homes (#1b)" do
    Application.put_env(:fathom, :rebalancer_enabled, true)
    Application.put_env(:fathom, :rebalance_hot_qps_floor, 500.0)

    # n1 is beating; dead_node is not. hot_1 is pinned to dead_node.
    :ok = Nodes.beat("n1")
    {:ok, _} = Overrides.pin("hot_1", "dead_node", reason: "test")

    assert :ok = perform_job(RebalanceJob, %{})
    assert Overrides.for_shard("hot_1") == nil, "dead-node pin unpinned (re-homed to hash)"
  end

  test "reconcile keeps a pin whose node stopped beating but still HOLDS the S3 lease (#1)" do
    Application.put_env(:fathom, :rebalancer_enabled, true)
    Application.put_env(:fathom, :rebalance_hot_qps_floor, 500.0)

    shard = "recon_held_#{System.unique_integer([:positive])}"
    lock = Path.join([System.tmp_dir!(), "fathom_remote_test", shard <> ".lock"])
    on_exit(fn -> File.rm(lock) end)

    # n1 beats; "ghost" does NOT beat but HOLDS the shard's S3 lease (its data plane is alive).
    :ok = Nodes.beat("n1")
    {:ok, _} = Fathom.Shard.Storage.acquire_lease(shard, "ghost", 30_000)
    {:ok, _} = Overrides.pin(shard, "ghost", reason: "test")

    assert :ok = perform_job(RebalanceJob, %{})

    # The reporter beat says ghost is dead, but the S3 lease is held → KEEP the pin (fail-safe):
    # unpinning a live-owned shard would route it to a node that can't steal the held lease.
    refute is_nil(Overrides.for_shard(shard)), "held-lease pin kept despite the stale beat"
    assert is_nil(Overrides.for_shard(shard).failed_at)
  end

  test "reconcile fails open: no beats ⇒ no unpins (#1b)" do
    Application.put_env(:fathom, :rebalancer_enabled, true)
    Application.put_env(:fathom, :rebalance_hot_qps_floor, 500.0)

    {:ok, _} = Overrides.pin("hot_1", "n2", reason: "test")
    # Nothing has beaten — the alive set is empty, so the reconciler must NOT yank pins
    # (the beat mechanism could just be starting / down).
    assert :ok = perform_job(RebalanceJob, %{})
    assert Overrides.for_shard("hot_1") != nil, "no unpin when nothing is beating"
  end

  test "affinity: an enabled tick routes the handoff to a warm target (#C)" do
    Application.put_env(:fathom, :rebalancer_enabled, true)
    Application.put_env(:fathom, :rebalance_hot_qps_floor, 500.0)
    Application.put_env(:fathom, :rebalance_confirm_windows, 2)

    # n1 overloaded; n2 coldest (10), n3 warm for hot_1 and within the band (100).
    sample("n1", "hot_1", 900, 10_000)
    sample("n1", "hot_1", 900, 0)
    sample("n1", "filler", 300, 0)
    sample("n2", "x", 10, 0)
    sample("n3", "y", 100, 0)
    :ok = WarmLocations.publish("n3", ["hot_1"])

    assert :ok = perform_job(RebalanceJob, %{})

    [job] = all_enqueued(worker: HandoffJob)
    assert job.args["shard_id"] == "hot_1"

    assert job.args["to_node"] == "n3",
           "warm-location signal steered the handoff to n3, not the cold n2"
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
