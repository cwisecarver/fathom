defmodule Fathom.Migrator.CaptureGuardTest do
  @moduledoc """
  Expert review 2026-07-18 #6: `Migrator.Capture` holds captures that failed to record (a Postgres
  outage) in an in-memory retry buffer. A restart during the outage — realistically a rolling
  deploy — wipes the buffer, silently forking the template schema from the fleet. The guard exposes
  the pending count and makes a shutdown-while-pending LOUD (a deploy gate can poll the count).

  `async: true` puts DataCase in MANUAL sandbox mode, so the Capture GenServer we start here is NOT
  allowed on the test's connection — a faithful simulation of the outage: `record/3`'s Repo calls
  raise, so the committed-on-template capture is stashed pending instead of recorded.
  """
  use Fathom.DataCase, async: true

  import ExUnit.CaptureLog

  alias Fathom.Migrator.Capture

  # Start an isolated Capture (own name) and drive one migration commit that can't reach Postgres,
  # so exactly one capture is left buffered pending.
  defp capture_with_one_pending! do
    name = :"cap_guard_#{System.unique_integer([:positive])}"
    cap = start_supervised!(Supervisor.child_spec({Capture, name: name}, id: name))
    assert Capture.pending_count(cap) == 0

    conn = make_ref()
    Capture.begin(conn, 0, cap)
    Capture.append(conn, "CREATE TABLE t (v TEXT)", [], cap)

    Capture.append(
      conn,
      "INSERT INTO django_migrations (app, name) VALUES ('app', '0001')",
      [],
      cap
    )

    # record/3 raises (no Repo access) → the capture is stashed pending, NOT lost.
    assert {:error, _} = Capture.commit(conn, 1, cap)
    assert Capture.pending_count(cap) == 1

    {name, cap}
  end

  test "a shutdown while captures are pending alarms loudly (telemetry + error)" do
    {name, _cap} = capture_with_one_pending!()

    test_pid = self()
    handler = "cap-pending-#{System.unique_integer([:positive])}"

    :telemetry.attach(
      handler,
      [:fathom, :migrator, :capture_pending_on_shutdown],
      fn _e, meas, _meta, _cfg -> send(test_pid, {:pending_on_shutdown, meas}) end,
      nil
    )

    on_exit(fn -> :telemetry.detach(handler) end)

    # A supervisor shutdown (the deploy / SIGTERM path — terminate runs only because we trap exits).
    log = capture_log(fn -> :ok = stop_supervised(name) end)

    assert_receive {:pending_on_shutdown, %{count: 1}}
    assert log =~ "NOT yet recorded to Postgres"
  end

  test "a clean shutdown with an empty buffer is silent" do
    name = :"cap_clean_#{System.unique_integer([:positive])}"
    cap = start_supervised!(Supervisor.child_spec({Capture, name: name}, id: name))
    assert Capture.pending_count(cap) == 0

    test_pid = self()
    handler = "cap-clean-#{System.unique_integer([:positive])}"

    :telemetry.attach(
      handler,
      [:fathom, :migrator, :capture_pending_on_shutdown],
      fn _e, meas, _meta, _cfg -> send(test_pid, {:pending_on_shutdown, meas}) end,
      nil
    )

    on_exit(fn -> :telemetry.detach(handler) end)

    log = capture_log(fn -> :ok = stop_supervised(name) end)

    refute_receive {:pending_on_shutdown, _}, 100
    refute log =~ "NOT yet recorded"
  end
end
