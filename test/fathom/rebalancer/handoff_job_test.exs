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
  alias Fathom.Rebalancer.{CommandPoller, Commands, HandoffJob, Overrides}
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

  test "drain failure on the last attempt reverts to a retained cooldown record (#4)", %{
    shard: shard,
    node: node
  } do
    # Regression for #4: with NO CommandPoller running, warm + drain commands never
    # complete, so on the final attempt the handoff reverts. It must RETAIN the override
    # (stamped failed_at) as a cooldown record — deleting it (the old unpin) left the still
    # hot shard with no cooldown → re-proposed every tick (thrash).
    prev_warm = Application.get_env(:fathom, :handoff_warm_timeout_ms)
    prev_drain = Application.get_env(:fathom, :handoff_drain_timeout_ms)
    Application.put_env(:fathom, :handoff_warm_timeout_ms, 50)
    Application.put_env(:fathom, :handoff_drain_timeout_ms, 50)

    on_exit(fn ->
      restore(:handoff_warm_timeout_ms, prev_warm)
      restore(:handoff_drain_timeout_ms, prev_drain)
    end)

    args = %{"shard_id" => shard, "from_node" => node, "to_node" => node, "q_per_s" => 500.0}
    # attempt == max_attempts (3) so drain failure takes the revert branch, not a retry.
    assert {:cancel, _} = perform_job(HandoffJob, args, attempt: 3)

    o = Overrides.for_shard(shard)
    assert o != nil, "override retained as a cooldown record after revert"
    assert o.failed_at != nil

    # The pending drain (its await timed out) was cancelled on revert (#7), so the source
    # poller can't fire it later against the restored source.
    assert Commands.cancel_pending_drains(shard) == 0, "no pending drain left after revert"
  end

  test "drain await is ordered above the poller's worst-case drain (#8)" do
    # The poller drains via Shards.drain(id, command_drain_ms), whose safety net adds +30s
    # (coordinator shutdown). If the handoff's await were shorter, a slow-but-succeeding
    # drain would be mislabeled a timeout → premature retry (duplicate drain) / spurious
    # revert. The default must exceed command_drain_ms + 30s.
    prev = Application.get_env(:fathom, :command_drain_ms)
    prev_h = Application.get_env(:fathom, :handoff_drain_timeout_ms)
    Application.put_env(:fathom, :command_drain_ms, 10_000)
    Application.delete_env(:fathom, :handoff_drain_timeout_ms)

    on_exit(fn ->
      restore(:command_drain_ms, prev)
      restore(:handoff_drain_timeout_ms, prev_h)
    end)

    assert HandoffJob.drain_timeout() >= 10_000 + 30_000

    # An explicit override wins (operator owns the ordering).
    Application.put_env(:fathom, :handoff_drain_timeout_ms, 12_345)
    assert HandoffJob.drain_timeout() == 12_345
  end

  test "an invalid shard_id doesn't crash the handoff (#14)", %{node: node} do
    # Regression for #14: pin_and_flip hard-matched {:ok, _}, so a rejected pin (invalid
    # shard_id) would MatchError-crash the job. It must handle the error and revert instead.
    prev_warm = Application.get_env(:fathom, :handoff_warm_timeout_ms)
    Application.put_env(:fathom, :handoff_warm_timeout_ms, 50)
    on_exit(fn -> restore(:handoff_warm_timeout_ms, prev_warm) end)

    args = %{
      "shard_id" => "evil; } server { #",
      "from_node" => node,
      "to_node" => node,
      "q_per_s" => 500.0
    }

    # No raise; a permanent invalid pin reverts (cancels) rather than crashing.
    assert {:cancel, _} = perform_job(HandoffJob, args, attempt: 3)
  end

  test "a flip that can't be applied skips the drain (source not stranded) (#11)", %{
    shard: shard,
    node: node
  } do
    # Regression for #11: if the LB flip isn't live (reload command fails), draining the
    # source while traffic still routes to it would strand the shard. pin_and_flip surfaces
    # the failure, so drain is SKIPPED — no drain command is issued and the source stays up.
    prev_reload = Application.get_env(:fathom, :lb_reload_cmd)
    prev_warm = Application.get_env(:fathom, :handoff_warm_timeout_ms)
    Application.put_env(:fathom, :lb_reload_cmd, "false")
    Application.put_env(:fathom, :handoff_warm_timeout_ms, 50)

    on_exit(fn ->
      restore(:lb_reload_cmd, prev_reload)
      restore(:handoff_warm_timeout_ms, prev_warm)
    end)

    {:ok, pid, ref, _path} = Shards.checkout(shard)
    Fathom.Shard.checkin(pid, ref)
    down = Process.monitor(pid)

    args = %{"shard_id" => shard, "from_node" => node, "to_node" => node, "q_per_s" => 500.0}
    assert {:cancel, _} = perform_job(HandoffJob, args, attempt: 3)

    # No drain command was ever issued (drain gated behind the failed flip).
    assert Commands.pending_for(node) |> Enum.filter(&(&1.command == "drain")) == []
    # Source coordinator was NOT drained.
    refute_receive {:DOWN, ^down, :process, ^pid, _}, 300
    assert Process.alive?(pid)
    # Reverted to a cooldown record.
    assert Overrides.for_shard(shard).failed_at != nil
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
