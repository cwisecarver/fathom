defmodule Fathom.Shard.ReopenGapTest do
  @moduledoc """
  The idle-drop → reopen gap (expert review 2026-10-01 #10): the measurement `:shard_idle_ms`
  has to be set from. A new measurement, not a bug fix — these pin what it counts (idle drops
  followed by a cold open, nothing else) and that its table stays bounded.
  """
  use ExUnit.Case, async: false

  alias Fathom.Shard.ReopenGap
  alias Fathom.ShardExecutor

  setup do
    shard = "reopen_gap_#{System.unique_integer([:positive])}"
    prev_idle = Application.get_env(:fathom, :shard_idle_ms)

    test = self()
    handler = "reopen-gap-#{shard}"

    :telemetry.attach(
      handler,
      [:fathom, :shard, :reopen],
      fn _event, measurements, %{shard_id: id}, _ ->
        if id == shard, do: send(test, {:reopen, measurements.gap_ms})
      end,
      nil
    )

    on_exit(fn ->
      :telemetry.detach(handler)

      if prev_idle,
        do: Application.put_env(:fathom, :shard_idle_ms, prev_idle),
        else: Application.delete_env(:fathom, :shard_idle_ms)

      for base <- [
            Path.join([Fathom.Shard.data_dir(), "#{shard}.db"]),
            Path.join([Fathom.Shard.Storage.Local.dir(), "#{shard}.db"])
          ],
          suffix <- ["", "-wal", "-shm"],
          do: File.rm(base <> suffix)
    end)

    %{shard: shard}
  end

  defp coordinator(shard) do
    [{pid, _}] = Registry.lookup(Fathom.ShardRegistry, shard)
    pid
  end

  defp open_and_close(shard) do
    {:ok, h} = ShardExecutor.open(shard)
    pid = coordinator(shard)
    :ok = ShardExecutor.close(h)
    pid
  end

  test "a cold open after an idle drop reports the gap; the first open does not", %{shard: shard} do
    Application.put_env(:fathom, :shard_idle_ms, 20)

    pid = open_and_close(shard)
    refute_received {:reopen, _}, "a first-ever open has no previous drop to measure from"

    ref = Process.monitor(pid)
    assert_receive {:DOWN, ^ref, :process, ^pid, :normal}, 5_000

    Process.sleep(30)
    Application.put_env(:fathom, :shard_idle_ms, 60_000)
    {:ok, h} = ShardExecutor.open(shard)

    assert_receive {:reopen, gap_ms}, 1_000
    assert gap_ms >= 30, "gap #{gap_ms} ms is shorter than the time the shard was closed"

    # Taken, not read: a second open of the SAME incarnation does not report it again.
    :ok = ShardExecutor.close(h)
    {:ok, h} = ShardExecutor.open(shard)
    refute_received {:reopen, _}
    :ok = ShardExecutor.close(h)
  end

  test "a stop that is not an idle drop is not recorded", %{shard: shard} do
    Application.put_env(:fathom, :shard_idle_ms, 60_000)
    pid = open_and_close(shard)

    ref = Process.monitor(pid)
    :ok = Fathom.Shards.stop(shard)
    assert_receive {:DOWN, ^ref, :process, ^pid, _}, 5_000

    {:ok, h} = ShardExecutor.open(shard)
    refute_received {:reopen, _}, "an explicit stop was counted as an idle drop"
    :ok = ShardExecutor.close(h)
  end

  test "the sweep drops rows past the horizon and keeps recent ones" do
    old = "reopen_gap_old_#{System.unique_integer([:positive])}"
    new = "reopen_gap_new_#{System.unique_integer([:positive])}"
    now = ReopenGap.now_ms()

    :ets.insert(ReopenGap, {old, now - 10_000})
    :ets.insert(ReopenGap, {new, now})

    assert ReopenGap.sweep(now - 5_000) >= 1
    assert :ets.lookup(ReopenGap, old) == []
    assert [{^new, _}] = :ets.lookup(ReopenGap, new)
    :ets.delete(ReopenGap, new)
  end
end
