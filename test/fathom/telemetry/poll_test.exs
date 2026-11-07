defmodule Fathom.Telemetry.PollTest do
  @moduledoc """
  Expert review 2026-11-07 #77: `:telemetry_poller` drops a measurement for the life of the poller
  after its FIRST raise or exit (it keeps only the ones that did not misbehave on a tick), so one
  `:noproc` from `Shards.open_count/0` during a data-plane restart removed `fathom.shards.active`
  until the node rebooted. Every measurement is now registered through `Fathom.Telemetry.Poll`.
  """
  use ExUnit.Case, async: false

  import ExUnit.CaptureLog

  alias Fathom.Telemetry.Poll
  alias Fathom.Test.Await

  # Measurements the pollers below call: bump a counter, then misbehave on the first call only.
  def flaky(counter) do
    n = :counters.get(counter, 1) + 1
    :counters.put(counter, 1, n)
    if n == 1, do: exit(:noproc)
    :ok
  end

  def raising, do: raise("boom")
  def throwing, do: throw(:ball)

  setup do
    Fathom.LogLimiter.reset(:poller_measurement_failed)
    on_exit(fn -> Fathom.LogLimiter.reset(:poller_measurement_failed) end)
    :ok
  end

  def tick(counter), do: :counters.add(counter, 1, 1)

  describe "the stock poller (the premise)" do
    test "drops a measurement after its first exit" do
      counter = :counters.new(1, [])
      healthy = :counters.new(1, [])

      # The poller logs the failure itself (as an error); that line is not under test.
      capture_log(fn ->
        start_supervised!(
          {:telemetry_poller,
           measurements: [{__MODULE__, :flaky, [counter]}, {__MODULE__, :tick, [healthy]}],
           period: 30,
           name: :"poll_premise_#{System.unique_integer([:positive])}"}
        )

        # The healthy sibling proves the poller kept ticking while `flaky` stayed at ONE call.
        Await.until(fn -> :counters.get(healthy, 1) >= 3 end, "the poller never ticked again")
      end)

      assert :counters.get(counter, 1) == 1,
             "the stock poller no longer drops a failed measurement"
    end
  end

  describe "Poll.wrap/1" do
    test "a measurement that exits on its first tick is called again on the next" do
      counter = :counters.new(1, [])

      log =
        capture_log(fn ->
          start_supervised!(
            {:telemetry_poller,
             measurements: Poll.wrap([{__MODULE__, :flaky, [counter]}]),
             period: 30,
             name: :"poll_wrapped_#{System.unique_integer([:positive])}"}
          )

          # Pre-fix (the stock poller) the count stops at 1.
          Await.until(fn -> :counters.get(counter, 1) >= 3 end, "the measurement was dropped")
        end)

      assert log =~ "poller measurement Fathom.Telemetry.PollTest.flaky failed"
    end

    test "run/3 turns raise, throw and exit into :ok, counts each and logs once per window" do
      handler = "poll-failed-#{System.unique_integer([:positive])}"
      parent = self()

      :telemetry.attach(
        handler,
        [:fathom, :poller, :measurement_failed],
        fn _e, meas, meta, _ -> send(parent, {:failed, meas, meta}) end,
        nil
      )

      on_exit(fn -> :telemetry.detach(handler) end)

      log =
        capture_log(fn ->
          assert :ok = Poll.run(__MODULE__, :raising, [])
          assert :ok = Poll.run(__MODULE__, :throwing, [])
          assert :ok = Poll.run(__MODULE__, :flaky, [:counters.new(1, [])])
        end)

      assert_receive {:failed, %{count: 1}, %{measurement: "Fathom.Telemetry.PollTest.raising"}}
      assert_receive {:failed, %{count: 1}, %{measurement: "Fathom.Telemetry.PollTest.throwing"}}
      assert_receive {:failed, %{count: 1}, %{measurement: "Fathom.Telemetry.PollTest.flaky"}}

      assert log =~ "poller measurement Fathom.Telemetry.PollTest.raising failed"
      # One line per minute window, not one per failure.
      assert length(String.split(log, "telemetry poller measurement")) == 2
    end

    test "a healthy measurement is untouched" do
      assert :ok = Poll.run(Kernel, :+, [1, 2])
    end
  end

  describe "Shards.open_count/0 while the supervisor is down" do
    test "the fallback answers 0 instead of exiting :noproc" do
      # PRECONDITION: the bare call really exits, which is what raised into the poller.
      assert catch_exit(DynamicSupervisor.count_children(:no_such_supervisor_for_this_test))
      assert Fathom.Shards.count_children_active(:no_such_supervisor_for_this_test) == 0
    end
  end

  describe "the node's pollers" do
    # A measurement added to either poller without the wrapper would be dropped on its first raise.
    test "every registered measurement goes through Poll.run/3" do
      for name <- [Fathom.ShardPoller, Fathom.ObanPoller], pid = Process.whereis(name) do
        %{measurements: measurements} = :sys.get_state(pid)
        assert measurements != []

        for m <- measurements do
          assert {Poll, :run, [_module, _fun, _args]} = m,
                 "#{inspect(name)} runs #{inspect(m)} outside Fathom.Telemetry.Poll"
        end
      end

      # PRECONDITION: the shard poller is up in the suite, so the loop above checked something.
      assert Process.whereis(Fathom.ShardPoller)
    end
  end
end
