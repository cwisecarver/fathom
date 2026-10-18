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

  # Symptom (expert review 2026-10-17 #2): the live table stored the exqlite resource term itself,
  # and an ETS row holds a reference, so the NIF resource destructor (which closes the sqlite
  # handle) could never run for a handle whose owner died without `close/1`: the fd, ~166 KB and
  # the WAL reader slot leaked until restart. Invariant: the table must not retain the resource.
  test "a handle orphaned by a killed owner is GC-closed (the table must not retain it)", %{
    dir: dir
  } do
    path = Path.join(dir, "orphan.db")
    test_pid = self()

    {pid, ref} =
      spawn_monitor(fn ->
        {:ok, _conn} = Connection.open(path, tenant?: true)
        send(test_pid, :opened)
        Process.sleep(:infinity)
      end)

    assert_receive :opened, 5_000
    assert fd_open?(path), "precondition: the handle holds the db file open"

    Process.exit(pid, :kill)
    assert_receive {:DOWN, ^ref, :process, ^pid, :killed}

    released? =
      Enum.reduce_while(1..40, false, fn _, _ ->
        for p <- Process.list(), do: :erlang.garbage_collect(p)

        if fd_open?(path) do
          Process.sleep(50)
          {:cont, false}
        else
          {:halt, true}
        end
      end)

    assert released?, "orphaned handle was never GC-closed: the live table retains the resource"
  end

  defp fd_open?(path) do
    {out, _} = System.cmd("lsof", ["-Fn", "-p", System.pid()], stderr_to_stdout: true)
    String.contains?(out, Path.basename(path))
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
