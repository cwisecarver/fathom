defmodule Fathom.Admin.DiskPollBlockedTest do
  @moduledoc """
  Expert review 2026-11-07 **#76**: `Measurements.disk/0` ran a bare `System.cmd("df")` on the ONE
  telemetry-poller process that runs every gauge serially. A `statfs` that hangs (an EBS / NFS
  brown-out — the event that also hangs SQLite's fsyncs) therefore stopped every gauge on the node,
  including the dirty-IO run queue the stall is diagnosed with. A timeout alone is not enough: the
  OS `df` sits in an uninterruptible `statfs` that nothing ends, so a timeout that kills the task
  and retries leaves one more stuck `df` per poll for as long as the stall lasts.

  The fixture is `Fathom.Test.FakeDf`: a fake `df` first on `PATH` that records each invocation and
  blocks until the test releases it, so what is asserted is what the OS sees: the poll returns, and
  a second poll does not start a second `df`. Only invocations naming THIS test's data directory
  are counted, and the live poller is suspended (see the helper's moduledoc: the first version of
  this file failed once in a full run with "the fake df was never started", never reproduced).
  """
  use ExUnit.Case, async: false

  import ExUnit.CaptureLog

  alias Fathom.Admin.Measurements
  alias Fathom.Test.{Await, FakeDf}

  @runner Fathom.Admin.Measurements.DfVolumes

  setup do
    # The directory `Measurements.disk/0` reads, so the fake `df` is invoked with its path.
    data = Fathom.Shard.data_dir()
    File.mkdir_p!(data)

    # Let the blocked fake `df` (and so its runner) finish before the next test, so none finds the
    # single-flight name taken.
    fake =
      FakeDf.install!(
        settle: fn ->
          Await.until(
            fn -> Process.whereis(@runner) == nil end,
            "the df runner never exited",
            10_000
          )
        end
      )

    Fathom.LogLimiter.reset(:disk_read_blocked)
    on_exit(fn -> Fathom.LogLimiter.reset(:disk_read_blocked) end)

    %{fake: fake, data: data}
  end

  defp calls(fake, data), do: length(FakeDf.calls(fake, data))

  # The fake `df` has started `n` times for this test's directory. The failure message names
  # everything the fake recorded, so a stray caller shows up in the report.
  defp await_calls!(fake, data, n) do
    assert Await.eventually?(fn -> calls(fake, data) == n end, 5_000),
           "expected #{n} fake df call(s) on #{data}; recorded: #{inspect(FakeDf.calls(fake))}"
  end

  # Pre-fix the first poll never returns (the fake df never does), so bound the test itself.
  @tag timeout: 15_000
  test "a hung df does not hold the poller, and a second poll does not start a second df", %{
    fake: fake,
    data: data
  } do
    # PRECONDITION: no runner left over from another test, so this poll is the one that spawns.
    assert Process.whereis(@runner) == nil

    {{us, result}, log} = with_log(fn -> :timer.tc(fn -> Measurements.disk() end) end)
    assert log =~ "the volumes read"

    # Pre-fix this call did not return until the fake df did (it never does, here).
    assert result == :ok
    assert us < 2_000_000, "the poll took #{div(us, 1000)} ms against a hung df"

    # The fake df was started, and it is STILL running: the runner is alive and holds the name.
    await_calls!(fake, data, 1)
    assert is_pid(Process.whereis(@runner)), "the blocked df's runner is gone"

    # A second poll (the next 10 s tick) while that df is still stuck must not add another.
    {us2, _log} = :timer.tc(fn -> capture_log(fn -> assert :ok = Measurements.disk() end) end)

    assert us2 < 1_000_000, "the second poll took #{div(us2, 1000)} ms"
    assert calls(fake, data) == 1, "a second df was started while the first was still blocked"
  end

  @tag timeout: 15_000
  test "once the stuck df returns, the next poll reads again", %{fake: fake, data: data} do
    assert capture_log(fn -> assert :ok = Measurements.disk() end) =~ "did not answer"
    await_calls!(fake, data, 1)

    # Unblock it: the runner finishes (the fake exits 1) and releases the single-flight name.
    FakeDf.release!(fake)
    Await.until(fn -> Process.whereis(@runner) == nil end, "the runner never exited")

    before = calls(fake, data)
    assert :ok = Measurements.disk()
    assert calls(fake, data) > before, "the next poll did not run df again"
  end
end
