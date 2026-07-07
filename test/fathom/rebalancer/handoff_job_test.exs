defmodule Fathom.Rebalancer.HandoffJobTest do
  @moduledoc """
  The handoff orchestration end to end on a single node: warm → pin + flip LB → drain,
  driven through the real command channel + poller. from == to == this node so both
  commands land on this node's poller (there's only one node in the test). Not async —
  real coordinators + the shared sandbox.
  """
  use Fathom.DataCase, async: false
  use Oban.Testing, repo: Fathom.Repo

  alias Fathom.Rebalancer
  alias Fathom.Rebalancer.{CommandPoller, HandoffJob, Overrides}
  alias Fathom.Shards

  setup do
    shard = "handoff_#{System.unique_integer([:positive])}"

    map_path =
      Path.join(System.tmp_dir!(), "fathom_lbmap_#{System.unique_integer([:positive])}.conf")

    node = Rebalancer.node_key()

    prev_idle = Application.get_env(:fathom, :shard_idle_ms)
    prev_poll = Application.get_env(:fathom, :command_poll_ms)
    prev_map = Application.get_env(:fathom, :lb_map_path)
    prev_backends = Application.get_env(:fathom, :lb_backends)

    # Keep the coordinator alive through the test; poll fast; render the map to a temp file.
    Application.put_env(:fathom, :shard_idle_ms, 600_000)
    Application.put_env(:fathom, :command_poll_ms, 50)
    Application.put_env(:fathom, :lb_map_path, map_path)
    Application.put_env(:fathom, :lb_backends, %{node => "#{node}:8080"})

    on_exit(fn ->
      restore(:shard_idle_ms, prev_idle)
      restore(:command_poll_ms, prev_poll)
      restore(:lb_map_path, prev_map)
      restore(:lb_backends, prev_backends)
      File.rm(map_path)

      for dir <- ["fathom_shards", "fathom_remote_test"], suffix <- ["", "-wal", "-shm"] do
        File.rm(Path.join([System.tmp_dir!(), dir, "#{shard}.db"]) <> suffix)
      end
    end)

    %{shard: shard, node: node, map_path: map_path}
  end

  defp restore(k, nil), do: Application.delete_env(:fathom, k)
  defp restore(k, v), do: Application.put_env(:fathom, k, v)

  test "runs warm → pin + write LB map → drain, and completes", %{
    shard: shard,
    node: node,
    map_path: map_path
  } do
    # Open the shard here so drain has a coordinator to release; free the connection so
    # drain isn't blocked by an in-flight checkout.
    {:ok, pid, ref, _path} = Shards.checkout(shard)
    Fathom.Shard.checkin(pid, ref)
    down = Process.monitor(pid)

    start_supervised!(CommandPoller)

    args = %{"shard_id" => shard, "from_node" => node, "to_node" => node, "q_per_s" => 500.0}
    assert :ok = perform_job(HandoffJob, args)

    # Pinned to the target, LB map rendered with the pinned host, source coordinator drained.
    assert Overrides.for_shard(shard).pinned_node == node
    assert File.read!(map_path) =~ "#{shard}."
    assert_receive {:DOWN, ^down, :process, ^pid, _}, 5_000
  end

  test "period: :infinity — a second handoff for the same shard is deduped past 60s", %{
    shard: shard,
    node: node
  } do
    # Regression for #5: a handoff routinely outlives Oban's default 60s unique window
    # (warm + drain awaits + retry backoff). With period: 60, once the first job's
    # inserted_at ages past 60s a second enqueue for the same shard is NOT deduped — two
    # handoffs then pin + drain the same shard. period: :infinity keeps "one per shard
    # until terminal".
    args = %{"shard_id" => shard, "from_node" => node, "to_node" => node, "q_per_s" => 500.0}

    {:ok, job1} = args |> HandoffJob.new() |> Oban.insert()

    # Simulate a slow handoff: age the in-flight job past the old 60s window.
    from(j in Oban.Job, where: j.id == ^job1.id)
    |> Repo.update_all(set: [inserted_at: DateTime.add(DateTime.utc_now(), -120, :second)])

    {:ok, job2} = args |> HandoffJob.new() |> Oban.insert()

    assert job2.id == job1.id, "deduped to the still-in-flight handoff"
    assert length(all_enqueued(worker: HandoffJob, args: %{"shard_id" => shard})) == 1
  end
end
