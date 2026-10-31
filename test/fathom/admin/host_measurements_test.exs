defmodule Fathom.Admin.HostMeasurementsTest do
  # Expert review 2026-10-29 #1 / #17b: the poller gauges for Dirty / Writeback / PSI / the effective
  # dirty-background threshold, and the prod-only boot preflight built on the same reads. Every read
  # goes through an injected reader, so the tests need neither /proc nor Linux.
  #
  # Not async: the boot-check tests flip `:env`, which other tests read.
  use ExUnit.Case, async: false

  import ExUnit.CaptureLog

  alias Fathom.Admin.Measurements

  @gib 1024 * 1024 * 1024

  @meminfo """
  MemTotal:       98566144 kB
  MemAvailable:   96468992 kB
  Dirty:              2048 kB
  Writeback:            64 kB
  """

  @pressure """
  some avg10=3.50 avg60=1.00 avg300=0.20 total=999
  full avg10=1.50 avg60=0.50 avg300=0.10 total=444
  """

  # A host with the default ratio, the rig's shape.
  @default_host %{
    "/proc/meminfo" => @meminfo,
    "/proc/sys/vm/dirty_background_bytes" => "0\n",
    "/proc/sys/vm/dirty_background_ratio" => "10\n",
    "/proc/pressure/io" => @pressure
  }

  defp reader(files) do
    fn path ->
      case Map.fetch(files, path) do
        {:ok, body} -> {:ok, body}
        :error -> {:error, :enoent}
      end
    end
  end

  defp capture(events) do
    ref = make_ref()
    test = self()

    :telemetry.attach_many(
      {__MODULE__, ref},
      events,
      fn event, m, _meta, _ -> send(test, {ref, event, m}) end,
      nil
    )

    on_exit(fn -> :telemetry.detach({__MODULE__, ref}) end)
    ref
  end

  @events [
    [:fathom, :node, :host_memory],
    [:fathom, :node, :vm_dirty],
    [:fathom, :node, :io_pressure]
  ]

  describe "host/1" do
    test "emits Dirty, Writeback, the effective threshold and PSI" do
      ref = capture(@events)
      assert :ok = Measurements.host(reader(@default_host))

      assert_receive {^ref, [:fathom, :node, :host_memory], %{dirty: dirty, writeback: wb}}
      assert dirty == 2048 * 1024
      assert wb == 64 * 1024

      assert_receive {^ref, [:fathom, :node, :vm_dirty], %{background_bytes: bytes}}
      assert bytes == div(96_468_992 * 1024 * 10, 100)

      assert_receive {^ref, [:fathom, :node, :io_pressure], %{some_avg10: 3.5, full_avg10: 1.5}}
    end

    test "dirty_background_bytes wins when set" do
      ref = capture(@events)
      files = Map.put(@default_host, "/proc/sys/vm/dirty_background_bytes", "#{@gib}\n")
      Measurements.host(reader(files))

      assert_receive {^ref, [:fathom, :node, :vm_dirty], %{background_bytes: @gib}}
    end

    test "absent files are skipped silently, event by event" do
      ref = capture(@events)

      # No PSI (older kernel / compiled out): everything else still reports.
      assert :ok = Measurements.host(reader(Map.delete(@default_host, "/proc/pressure/io")))
      assert_receive {^ref, [:fathom, :node, :host_memory], _}
      assert_receive {^ref, [:fathom, :node, :vm_dirty], _}
      refute_receive {^ref, [:fathom, :node, :io_pressure], _}, 0
    end

    test "off Linux (nothing readable) emits nothing and does not raise" do
      ref = capture(@events)
      assert :ok = Measurements.host(reader(%{}))

      refute_receive {^ref, _, _}, 0
    end

    test "the poller runs it as a zero-arity measurement" do
      assert function_exported?(Measurements, :host, 0)
      assert {Measurements, :host, []} in poller_measurements()
    end
  end

  describe "check_vm_dirty_background!/1 (boot preflight)" do
    setup do
      prev = Application.get_env(:fathom, :env)
      Application.put_env(:fathom, :env, :prod)

      on_exit(fn ->
        if is_nil(prev),
          do: Application.delete_env(:fathom, :env),
          else: Application.put_env(:fathom, :env, prev)
      end)
    end

    test "a default large-RAM host warns once, with the rig evidence and 'measure', not 'lower'" do
      log =
        capture_log(fn ->
          assert Fathom.Application.check_vm_dirty_background!(reader(@default_host)) == nil
        end)

      assert log =~ "effective vm.dirty_background threshold is 9.2 GiB"
      assert log =~ "p99 598 ms"
      assert log =~ "p99 417 ms"
      assert log =~ "MEASURE"
      assert log =~ "256 MB (p99 485 ms) and 64 MB (p99 762 ms) were worse"
      assert length(Regex.scan(~r/config warning/, log)) == 1
    end

    test "a 1 GB cap does not warn" do
      files = Map.put(@default_host, "/proc/sys/vm/dirty_background_bytes", "#{@gib}\n")

      assert capture_log(fn -> Fathom.Application.check_vm_dirty_background!(reader(files)) end) ==
               ""
    end

    test "exactly 2 GiB does not warn, one byte over does" do
      at_line = Map.put(@default_host, "/proc/sys/vm/dirty_background_bytes", "#{2 * @gib}\n")
      over = Map.put(@default_host, "/proc/sys/vm/dirty_background_bytes", "#{2 * @gib + 1}\n")

      assert capture_log(fn -> Fathom.Application.check_vm_dirty_background!(reader(at_line)) end) ==
               ""

      assert capture_log(fn -> Fathom.Application.check_vm_dirty_background!(reader(over)) end) =~
               "config warning"
    end

    test "missing files (non-Linux) are a silent no-op that never raises" do
      assert capture_log(fn ->
               assert Fathom.Application.check_vm_dirty_background!(reader(%{})) == nil
             end) == ""
    end

    test "prod-only, like every sibling guard" do
      Application.put_env(:fathom, :env, :dev)

      assert capture_log(fn ->
               Fathom.Application.check_vm_dirty_background!(reader(@default_host))
             end) == ""
    end
  end

  defp poller_measurements do
    # The measurement list is private to the supervisor's init; read it from its child spec.
    {:ok, {_flags, children}} = Fathom.Telemetry.init([])

    Enum.find_value(children, fn
      %{start: {:telemetry_poller, _, [opts]}} -> Keyword.fetch!(opts, :measurements)
      _ -> nil
    end)
  end
end
