defmodule Fathom.ShardsCheckoutRetryCounterTest do
  @moduledoc """
  Expert review 2026-11-04 #10 (watch-only counter, NO behaviour change). A checkout queued behind a
  coordinator's idle-stop waits out the stop's whole drop flush, exits `:normal`, and cold-reopens
  after a 1 ms retry; nothing counted how often. `[:fathom, :shards, :checkout, :retry]` now fires
  per retry with a bounded `reason`, to be read on the `tpc-churn` arm before any fix is built.

  The race is driven for real: the coordinator is suspended, an `:idle_timeout` is queued ahead of
  a caller's checkout, and resuming lets the coordinator stop with the checkout still pending.

  Not async: shards and app env are global.
  """
  use ExUnit.Case, async: false

  @moduletag :capture_log

  alias Fathom.{Shard, Shards}
  alias Fathom.Shard.Storage

  setup do
    shard = "retrycount_#{System.unique_integer([:positive])}"
    prev_idle = Application.get_env(:fathom, :shard_idle_ms)
    Application.put_env(:fathom, :shard_idle_ms, 1)

    test_pid = self()
    handler = "retrycount-#{shard}"

    :telemetry.attach(
      handler,
      [:fathom, :shards, :checkout, :retry],
      fn _e, m, meta, _ -> send(test_pid, {:retry, m, meta}) end,
      nil
    )

    on_exit(fn ->
      :telemetry.detach(handler)

      if prev_idle,
        do: Application.put_env(:fathom, :shard_idle_ms, prev_idle),
        else: Application.delete_env(:fathom, :shard_idle_ms)

      Shards.drain(shard, 2_000)

      for dir <- [Shard.data_dir(), Storage.Local.dir()],
          suffix <- [".db", ".db-wal", ".db-shm", ".db.etag", ".lock"],
          do: File.rm(Path.join(dir, shard <> suffix))
    end)

    %{shard: shard}
  end

  test "a checkout that lands while the coordinator is idle-stopping is counted", %{shard: shard} do
    {:ok, pid} = Shards.ensure(shard)
    ref = Process.monitor(pid)
    :ok = :sys.suspend(pid)

    # Ahead of the checkout in the mailbox, so the stop wins the race.
    send(pid, :idle_timeout)
    task = Task.async(fn -> Shards.checkout(shard) end)

    # The caller's `:checkout` call is queued behind the idle-timeout once the mailbox holds both.
    wait_queued(pid)
    :ok = :sys.resume(pid)

    assert_receive {:DOWN, ^ref, :process, ^pid, :normal}, 5_000
    assert_receive {:retry, %{count: 1}, %{reason: :normal}}, 5_000

    # No behaviour change: the retry re-resolves and the checkout still succeeds.
    assert {:ok, new_pid, _ref, _path} = Task.await(task, 10_000)
    refute new_pid == pid
  end

  test "an ordinary checkout emits nothing", %{shard: shard} do
    Application.put_env(:fathom, :shard_idle_ms, 60_000)
    assert {:ok, _pid, _ref, _path} = Shards.checkout(shard)
    refute_receive {:retry, _, _}, 100
  end

  # Wait until the caller's `GenServer.call` itself (not just any message) is in the mailbox.
  defp wait_queued(pid, tries \\ 200) do
    {:messages, msgs} = Process.info(pid, :messages)

    cond do
      Enum.any?(msgs, &match?({:"$gen_call", _, _}, &1)) ->
        :ok

      tries == 0 ->
        flunk("the checkout never queued behind the idle-timeout")

      true ->
        receive do
        after
          10 -> wait_queued(pid, tries - 1)
        end
    end
  end
end
