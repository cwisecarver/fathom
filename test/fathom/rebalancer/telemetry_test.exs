defmodule Fathom.Rebalancer.TelemetryTest do
  @moduledoc """
  The rebalancer emits the :telemetry events `Fathom.Telemetry` turns into operability metrics
  (handoff outcome, thrash, LB-apply health, reconcile, affinity hit-rate). Verifies each fires
  with the expected measurements/metadata — no exporter needed.
  """
  use Fathom.DataCase, async: false
  use Oban.Testing, repo: Fathom.Repo

  alias Fathom.Rebalancer.{CommandPoller, Commands, HandoffJob, LbApply, Overrides}
  alias Fathom.Rebalancer.{LoadSample, Nodes, RebalanceJob, WarmLocations}

  # Named handler (telemetry warns on anonymous-fun handlers).
  def forward(event, measurements, meta, %{pid: pid}) do
    send(pid, {:telemetry, event, measurements, meta})
  end

  defp attach(events) do
    id = "rebal-tel-#{System.unique_integer([:positive])}"
    :telemetry.attach_many(id, events, &__MODULE__.forward/4, %{pid: self()})
    on_exit(fn -> :telemetry.detach(id) end)
  end

  defp enable_rebalancer do
    for {k, v} <- [
          rebalancer_enabled: true,
          rebalance_hot_qps_floor: 500.0,
          rebalance_confirm_windows: 2,
          lb_backends: %{"n1" => "n1:8080", "n2" => "n2:8080", "n3" => "n3:8080"}
        ] do
      prev = Application.get_env(:fathom, k)
      Application.put_env(:fathom, k, v)
      on_exit(fn -> restore(k, prev) end)
    end
  end

  defp restore(k, nil), do: Application.delete_env(:fathom, k)
  defp restore(k, v), do: Application.put_env(:fathom, k, v)

  defp sample(node, shard, q, age_ms) do
    Repo.insert!(%LoadSample{
      node_key: node,
      shard_id: shard,
      q_per_s: q / 1,
      rows_read_per_s: 0.0,
      checkouts_per_s: 0.0,
      window_s: 10.0,
      sampled_at: DateTime.add(DateTime.utc_now(), -age_ms, :millisecond)
    })
  end

  test "a proposed move emits move.proposed + affinity (hit when the target is warm)" do
    enable_rebalancer()
    attach([[:fathom, :rebalancer, :move, :proposed], [:fathom, :rebalancer, :affinity]])

    # n1 overloaded; n3 warm for hot_1 within the band → affinity hit.
    sample("n1", "hot_1", 900, 10_000)
    sample("n1", "hot_1", 900, 0)
    sample("n1", "filler", 300, 0)
    sample("n2", "x", 10, 0)
    sample("n3", "y", 100, 0)
    :ok = WarmLocations.publish("n3", ["hot_1"])

    assert :ok = perform_job(RebalanceJob, %{})

    assert_receive {:telemetry, [:fathom, :rebalancer, :move, :proposed], %{count: 1},
                    %{shard_id: "hot_1", to_node: "n3"}}

    assert_receive {:telemetry, [:fathom, :rebalancer, :affinity], %{count: 1},
                    %{outcome: :hit, to_node: "n3"}}
  end

  test "reconcile.unpinned fires when a dead-node pin is dropped" do
    enable_rebalancer()
    attach([[:fathom, :rebalancer, :reconcile, :unpinned]])

    :ok = Nodes.beat("n1")
    {:ok, _} = Overrides.pin("hot_1", "dead_node", reason: "test")

    assert :ok = perform_job(RebalanceJob, %{})

    assert_receive {:telemetry, [:fathom, :rebalancer, :reconcile, :unpinned], %{count: 1},
                    %{shard_id: "hot_1", node: "dead_node"}}
  end

  test "handoff.retry then handoff.stop(:reverted) fire when the drain never completes" do
    shard = "tel_ho_#{System.unique_integer([:positive])}"
    node = Fathom.Rebalancer.node_key()
    prev_w = Application.get_env(:fathom, :handoff_warm_timeout_ms)
    prev_d = Application.get_env(:fathom, :handoff_drain_timeout_ms)
    Application.put_env(:fathom, :handoff_warm_timeout_ms, 50)
    Application.put_env(:fathom, :handoff_drain_timeout_ms, 50)
    on_exit(fn -> restore(:handoff_warm_timeout_ms, prev_w) end)
    on_exit(fn -> restore(:handoff_drain_timeout_ms, prev_d) end)

    attach([[:fathom, :rebalancer, :handoff, :retry], [:fathom, :rebalancer, :handoff, :stop]])
    args = %{"shard_id" => shard, "from_node" => node, "to_node" => node, "q_per_s" => 500.0}

    # No CommandPoller ⇒ the drain command never completes. Attempt 1 retries...
    assert {:error, _} = perform_job(HandoffJob, args, attempt: 1)
    assert_receive {:telemetry, [:fathom, :rebalancer, :handoff, :retry], %{count: 1}, _}

    # ...the final attempt reverts.
    assert {:cancel, _} = perform_job(HandoffJob, args, attempt: 3)

    assert_receive {:telemetry, [:fathom, :rebalancer, :handoff, :stop], %{count: 1},
                    %{outcome: :reverted}}
  end

  test "lb_apply emits :applied then :noop on an unchanged re-render" do
    map_path =
      Path.join(System.tmp_dir!(), "tel_lbapply_#{System.unique_integer([:positive])}.conf")

    prev_map = Application.get_env(:fathom, :lb_map_path)
    prev_backends = Application.get_env(:fathom, :lb_backends)
    Application.put_env(:fathom, :lb_map_path, map_path)
    Application.put_env(:fathom, :lb_backends, %{"n1" => "n1:8080"})
    Application.delete_env(:fathom, :lb_reload_cmd)

    on_exit(fn ->
      restore(:lb_map_path, prev_map)
      restore(:lb_backends, prev_backends)
      File.rm(map_path)
      for f <- Path.wildcard(map_path <> ".tmp.*"), do: File.rm(f)
    end)

    attach([[:fathom, :rebalancer, :lb_apply]])
    {:ok, _} = Overrides.pin("hot_1", "n1", reason: "test")

    assert :ok = LbApply.apply!()

    assert_receive {:telemetry, [:fathom, :rebalancer, :lb_apply], %{count: 1},
                    %{outcome: :applied}}

    assert :ok = LbApply.apply!()
    assert_receive {:telemetry, [:fathom, :rebalancer, :lb_apply], %{count: 1}, %{outcome: :noop}}
  end

  test "command.stop fires with the command + outcome" do
    shard = "tel_cmd_#{System.unique_integer([:positive])}"
    node = Fathom.Rebalancer.node_key()

    on_exit(fn ->
      for suffix <- ["", "-wal", "-shm"] do
        File.rm(Path.join([System.tmp_dir!(), "fathom_shards", "#{shard}.db"]) <> suffix)
      end
    end)

    attach([[:fathom, :rebalancer, :command, :stop]])
    {:ok, _} = Commands.issue(shard, node, "warm")
    start_supervised!(CommandPoller)
    assert CommandPoller.poll_now() == 1

    assert_receive {:telemetry, [:fathom, :rebalancer, :command, :stop], %{count: 1},
                    %{command: "warm", outcome: :done}}
  end
end
