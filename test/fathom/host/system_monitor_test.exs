defmodule Fathom.Host.SystemMonitorTest do
  # Expert review 2026-10-29 #17d: `:erlang.system_monitor` long_schedule / long_gc counters.
  #
  # NOT async: `:erlang.system_monitor/2` is VM-global (one owner per node), and the app does not
  # start `Fathom.Host.SystemMonitor` in test (`:system_monitor` is unset), so each test owns it for
  # its own duration and `on_exit` releases it.
  #
  # long_schedule is exercised with a synthetic report: the BEAM only emits it for a process that
  # holds a scheduler without yielding, which no portable test fixture can force. The REAL report
  # path is covered for long_gc, where a forced collection of a large heap crosses a 1 ms threshold.
  use ExUnit.Case, async: false

  import ExUnit.CaptureLog

  alias Fathom.Host.SystemMonitor

  setup do
    assert :erlang.system_monitor() == :undefined,
           "another owner holds :erlang.system_monitor; this test needs it free"

    on_exit(fn -> :erlang.system_monitor(:undefined) end)
  end

  defp capture(events) do
    ref = make_ref()
    test = self()

    :telemetry.attach_many(
      {__MODULE__, ref},
      events,
      fn event, m, meta, _ -> send(test, {ref, event, m, meta}) end,
      nil
    )

    on_exit(fn -> :telemetry.detach({__MODULE__, ref}) end)
    ref
  end

  test "installs itself as the monitor with the 20 ms / 50 ms thresholds" do
    pid = start_supervised!(SystemMonitor)

    assert {^pid, opts} = :erlang.system_monitor()
    assert Enum.sort(opts) == [long_gc: 50, long_schedule: 20]
  end

  test "a long_schedule report becomes a counter event, tagged process or port" do
    ref = capture([[:fathom, :node, :long_schedule]])
    pid = start_supervised!(SystemMonitor)

    send(pid, {:monitor, self(), :long_schedule, timeout: 25, in: :foo, out: :bar})

    assert_receive {^ref, [:fathom, :node, :long_schedule], %{duration_ms: 25}, %{kind: :process}}

    port = Port.open({:spawn, "cat"}, [])
    send(pid, {:monitor, port, :long_schedule, timeout: 31, port_op: :timeout})

    assert_receive {^ref, [:fathom, :node, :long_schedule], %{duration_ms: 31}, %{kind: :port}}
  end

  test "a long_gc report becomes a counter event" do
    ref = capture([[:fathom, :node, :long_gc]])
    pid = start_supervised!(SystemMonitor)

    send(pid, {:monitor, self(), :long_gc, timeout: 77, heap_size: 1})

    assert_receive {^ref, [:fathom, :node, :long_gc], %{duration_ms: 77}, %{}}
  end

  test "a real long garbage collection reaches the counter" do
    ref = capture([[:fathom, :node, :long_gc]])
    start_supervised!({SystemMonitor, long_gc_ms: 1})

    parent = self()

    {pid, mon} =
      spawn_monitor(fn ->
        _big = Enum.to_list(1..3_000_000)
        send(parent, :heap_built)

        receive do
          :done -> :ok
        end
      end)

    assert_receive :heap_built, 10_000
    :erlang.garbage_collect(pid)

    assert_receive {^ref, [:fathom, :node, :long_gc], %{duration_ms: ms}, _}, 10_000
    assert ms >= 1

    send(pid, :done)
    assert_receive {:DOWN, ^mon, :process, ^pid, _}
  end

  test "unrelated messages are dropped, not crashed on" do
    pid = start_supervised!(SystemMonitor)
    ref = Process.monitor(pid)

    send(pid, :noise)
    send(pid, {:monitor, self(), :large_heap, []})

    # A system message is ordered after the sends, so both are handled by the time it answers.
    assert :sys.get_state(pid) == nil
    refute_received {:DOWN, ^ref, _, _, _}
  end

  test "never takes the monitor from another owner" do
    owner = spawn_link(fn -> receive do: (:stop -> :ok) end)
    :erlang.system_monitor(owner, long_gc: 999)

    log =
      capture_log(fn ->
        assert SystemMonitor.start_link() == :ignore
      end)

    assert log =~ "already owned by #{inspect(owner)}"
    assert {^owner, [long_gc: 999]} = :erlang.system_monitor()
  end
end
