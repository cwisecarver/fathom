defmodule Fathom.ConnectionLiveGaugeTest do
  @moduledoc """
  perf review 2026-10-14 #16 (gauge half) and #29 (boot warning).

  The live-connection gauge must equal opens minus closes, with `close/1` idempotent (the stream
  and the coordinator may both close one handle, shard_executor.ex checkin), and it must reach the
  telemetry poller's event. `soft_heap_warnings/3` is the pure core of the soft-heap boot warning.
  """
  use ExUnit.Case, async: false

  alias Fathom.Application, as: App
  alias Fathom.Shard.Connection

  setup do
    dir = Path.join(System.tmp_dir!(), "live_gauge_#{System.unique_integer([:positive])}")
    File.mkdir_p!(dir)
    on_exit(fn -> File.rm_rf(dir) end)
    %{dir: dir}
  end

  test "the gauge equals live opens, and a double close counts once", %{dir: dir} do
    base = Connection.live_count()
    {:ok, a} = Connection.open(Path.join(dir, "a.db"))
    {:ok, b} = Connection.open(Path.join(dir, "b.db"), scope: :rw)
    {:ok, c} = Connection.open(Path.join(dir, "c.db"), tenant?: true)
    assert Connection.live_count() == base + 3

    :ok = Connection.close(a)
    assert Connection.live_count() == base + 2
    # Idempotent: a second close of the same handle must not decrement again.
    :ok = Connection.close(a)
    assert Connection.live_count() == base + 2

    :ok = Connection.close(b)
    :ok = Connection.close(c)
    assert Connection.live_count() == base
  end

  test "a failed open leaves the gauge untouched", %{dir: dir} do
    base = Connection.live_count()
    # A directory where the db file should be: the open fails.
    File.mkdir_p!(Path.join(dir, "d.db"))
    assert {:error, _} = Connection.open(Path.join(dir, "d.db"))
    assert Connection.live_count() == base
  end

  test "the poller measurement reports the gauge", %{dir: dir} do
    {:ok, a} = Connection.open(Path.join(dir, "e.db"))
    id = "live-gauge-#{System.unique_integer([:positive])}"
    test_pid = self()

    :telemetry.attach(
      id,
      [:fathom, :node, :connections],
      fn _, m, _, _ -> send(test_pid, {:live, m.live}) end,
      nil
    )

    Fathom.Admin.Measurements.connections()
    assert_receive {:live, n}
    assert n == Connection.live_count() and n >= 1
    :telemetry.detach(id)
    Connection.close(a)
  end

  test "soft_heap_warnings: cap x 190 KB vs the limit and vs container RAM" do
    gib = 1024 * 1024 * 1024
    # 50k shards x 190 KB = ~9.1 GiB: over an 8 GiB limit and a 4 GiB container.
    assert [w1, w2] = App.soft_heap_warnings(50_000, 8 * gib, 4 * gib) |> Enum.take(2)
    assert w1 =~ "above :shard_soft_heap_limit_bytes"
    assert w2 =~ "container memory limit"
    # Limit at/above RAM warns even when the cap is small.
    assert [w3] = App.soft_heap_warnings(1_000, 8 * gib, 8 * gib)
    assert w3 =~ "never sheds"
    # Healthy and unknown-RAM configs are silent.
    assert [] = App.soft_heap_warnings(1_000, 8 * gib, 64 * gib)
    assert [] = App.soft_heap_warnings(:infinity, nil, nil)
    assert [] = App.soft_heap_warnings(1_000, 8 * gib, nil)
  end
end
