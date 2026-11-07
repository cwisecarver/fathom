defmodule Fathom.Admin.DiskPollBlockedTest do
  @moduledoc """
  Expert review 2026-11-07 **#76**: `Measurements.disk/0` ran a bare `System.cmd("df")` on the ONE
  telemetry-poller process that runs every gauge serially. A `statfs` that hangs (an EBS / NFS
  brown-out — the event that also hangs SQLite's fsyncs) therefore stopped every gauge on the node,
  including the dirty-IO run queue the stall is diagnosed with. A timeout alone is not enough: the
  OS `df` sits in an uninterruptible `statfs` that nothing ends, so a timeout that kills the task
  and retries leaves one more stuck `df` per poll for as long as the stall lasts.

  The fixture is a fake `df` first on `PATH` that records each invocation and blocks until the test
  releases it, so what is asserted is what the OS sees: the poll returns, and a second poll does not
  start a second `df`.
  """
  use ExUnit.Case, async: false

  import ExUnit.CaptureLog

  alias Fathom.Admin.Measurements
  alias Fathom.Test.Await

  @runner Fathom.Admin.Measurements.DfVolumes

  setup do
    dir = Path.join(System.tmp_dir!(), "fathom_fake_df_#{System.unique_integer([:positive])}")
    File.mkdir_p!(dir)
    counter = Path.join(dir, "calls")
    release = Path.join(dir, "release")

    # Blocks until `release` exists, then fails (`volume_readings` answers "unreadable").
    File.write!(Path.join(dir, "df"), """
    #!/bin/sh
    echo x >> '#{counter}'
    while [ ! -f '#{release}' ]; do sleep 0.02; done
    exit 1
    """)

    File.chmod!(Path.join(dir, "df"), 0o755)

    old_path = System.get_env("PATH")
    System.put_env("PATH", dir <> ":" <> old_path)
    Application.put_env(:fathom, :disk_poll_timeout_ms, 300)
    Fathom.LogLimiter.reset(:disk_read_blocked)

    on_exit(fn ->
      # Let the blocked fake `df` (and so its runner) finish BEFORE the PATH comes back, then wait
      # for the runner to be gone so no later test finds the single-flight name taken.
      File.write!(release, "")
      Await.until(fn -> Process.whereis(@runner) == nil end, "the df runner never exited", 10_000)
      System.put_env("PATH", old_path)
      Application.delete_env(:fathom, :disk_poll_timeout_ms)
      Fathom.LogLimiter.reset(:disk_read_blocked)
      File.rm_rf!(dir)
    end)

    %{counter: counter, release: release}
  end

  defp calls(counter) do
    case File.read(counter) do
      {:ok, body} -> body |> String.split("\n", trim: true) |> length()
      {:error, _} -> 0
    end
  end

  # Pre-fix the first poll never returns (the fake df never does), so bound the test itself.
  @tag timeout: 15_000
  test "a hung df does not hold the poller, and a second poll does not start a second df", %{
    counter: counter
  } do
    # PRECONDITION: no runner left over from another test, so this poll is the one that spawns.
    assert Process.whereis(@runner) == nil

    {{us, result}, log} = with_log(fn -> :timer.tc(fn -> Measurements.disk() end) end)
    assert log =~ "the volumes read"

    # Pre-fix this call did not return until the fake df did (it never does, here).
    assert result == :ok
    assert us < 2_000_000, "the poll took #{div(us, 1000)} ms against a hung df"

    # The fake df was started, and it is STILL running: the runner is alive and holds the name.
    Await.until(fn -> calls(counter) == 1 end, "the fake df was never started")
    assert is_pid(Process.whereis(@runner)), "the blocked df's runner is gone"

    # A second poll (the next 10 s tick) while that df is still stuck must not add another.
    {us2, _log} = :timer.tc(fn -> capture_log(fn -> assert :ok = Measurements.disk() end) end)

    assert us2 < 1_000_000, "the second poll took #{div(us2, 1000)} ms"
    assert calls(counter) == 1, "a second df was started while the first was still blocked"
  end

  @tag timeout: 15_000
  test "once the stuck df returns, the next poll reads again", %{
    counter: counter,
    release: release
  } do
    assert capture_log(fn -> assert :ok = Measurements.disk() end) =~ "did not answer"
    Await.until(fn -> calls(counter) == 1 end, "the fake df was never started")

    # Unblock it: the runner finishes (the fake exits 1) and releases the single-flight name.
    File.write!(release, "")
    Await.until(fn -> Process.whereis(@runner) == nil end, "the runner never exited")

    before = calls(counter)
    assert :ok = Measurements.disk()
    assert calls(counter) > before, "the next poll did not run df again"
  end
end
