defmodule Fathom.ShardCheckoutTimeoutTest do
  # Finding #10: the checkout GenServer.call used the default 5s timeout, but the open path
  # (handle_continue: lease acquire + pull) can legitimately run up to @pull_timeout (60s) for
  # a large cross-region cold open. A slow-but-normal open therefore failed the FIRST checkout
  # with {:error, :timeout} on the headline path. The timeout is now configurable and defaults
  # above the coordinator's own open budget. Not async: shards + storage config are global.
  use Fathom.ClusterShardCase

  alias Fathom.Test.FaultyStorage

  setup do
    prev_storage = Application.get_env(:fathom, :shard_storage)
    Application.put_env(:fathom, :shard_storage, FaultyStorage)
    # Make every cold-open pull slow, so the checkout call timeout is what decides.
    Application.put_env(:fathom, :storage_pull_delay_ms, 400)

    on_exit(fn ->
      Application.delete_env(:fathom, :storage_pull_delay_ms)
      Application.delete_env(:fathom, :shard_checkout_timeout_ms)

      if prev_storage,
        do: Application.put_env(:fathom, :shard_storage, prev_storage),
        else: Application.delete_env(:fathom, :shard_storage)
    end)

    :ok
  end

  test "checkout honors a configurable timeout instead of the hardcoded default",
       %{shard: shard} do
    # A 100ms budget below the 400ms pull must time out. Pre-fix the code ignored this config
    # entirely and used the 5s default, so a 400ms pull SUCCEEDED — this assertion failed.
    Application.put_env(:fathom, :shard_checkout_timeout_ms, 100)

    capture_log(fn ->
      assert {:error, :timeout} = Shards.checkout(shard)
    end)

    # The coordinator opens behind our back (the phantom-checkout path); stop it so it doesn't
    # linger touching files during on_exit.
    stop_coordinator(shard)
  end

  # Expert review #26: the timed-out call's :checkout message was still dequeued later —
  # the coordinator monitored the long-gone caller and registered a phantom connection.
  # The late reply is dropped by the call alias so no checkin ever arrives, and the
  # monitor only fires if the CALLER process dies (a Filo stream or Oban runner can live
  # for hours) — the shard was pinned open indefinitely: idle never armed, no
  # flush-and-drop, lease held, drains aborting :busy. The invariant: a timed-out
  # checkout leaves the coordinator with zero connections, so it idles out normally.
  test "a timed-out checkout does not pin the shard open (phantom conn abandoned)",
       %{shard: shard} do
    Application.put_env(:fathom, :shard_checkout_timeout_ms, 100)
    prev_idle = Application.get_env(:fathom, :shard_idle_ms)
    Application.put_env(:fathom, :shard_idle_ms, 50)

    on_exit(fn ->
      if is_nil(prev_idle),
        do: Application.delete_env(:fathom, :shard_idle_ms),
        else: Application.put_env(:fathom, :shard_idle_ms, prev_idle)
    end)

    capture_log(fn ->
      assert {:error, :timeout} = Shards.checkout(shard)

      # The coordinator finishes its slow open behind our back, processes the stale
      # :checkout (the phantom grant), then our abandon cast — and with zero conns
      # left it must idle-stop on its own. Pre-fix the phantom pinned it open forever.
      [{pid, _}] = Registry.lookup(Fathom.ShardRegistry, shard)
      ref = Process.monitor(pid)
      assert_receive {:DOWN, ^ref, :process, ^pid, :normal}, 5_000
    end)
  end

  test "a slow cold open under the timeout budget still yields a connection", %{shard: shard} do
    # The same 400ms pull, but an ample budget: the checkout waits it out and gets a connection
    # rather than failing — the actual fix, letting a normal slow open through.
    Application.put_env(:fathom, :shard_checkout_timeout_ms, 60_000)

    assert {:ok, pid, ref, _path} = Shards.checkout(shard)
    Fathom.Shard.checkin(pid, ref)
    stop_coordinator(shard)
  end

  # Stop the coordinator via its supervisor without starting a fresh one (Registry lookup, not
  # ensure/2), so a lingering phantom-checkout coordinator never touches files during on_exit.
  defp stop_coordinator(shard) do
    case Registry.lookup(Fathom.ShardRegistry, shard) do
      [{pid, _}] ->
        ref = Process.monitor(pid)
        _ = DynamicSupervisor.terminate_child(Fathom.ShardSupervisor, pid)

        receive do
          {:DOWN, ^ref, :process, ^pid, _} -> :ok
        after
          3_000 -> :ok
        end

      [] ->
        :ok
    end
  end
end
