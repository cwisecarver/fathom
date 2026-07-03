defmodule Fathom.Directory.RecorderTest do
  # Exercises the app-global recorder + Postgres, so not async (shared sandbox lets
  # the recorder process write through the test's connection).
  use Fathom.DataCase, async: false

  alias Fathom.Directory
  alias Fathom.Directory.Recorder

  setup do
    # Drain any buffer leftover from a prior test so flush counts are deterministic.
    Recorder.flush()
    :ok
  end

  defp uniq, do: "rec_#{System.unique_integer([:positive])}"

  test "coalesces repeated accesses into one row per shard and batch-flushes" do
    a = uniq()
    b = uniq()

    assert :ok = Recorder.record(a)
    assert :ok = Recorder.record(a)
    assert :ok = Recorder.record(b)

    # Two distinct shards, despite three records (a was coalesced).
    assert Recorder.flush() == 2

    assert {:ok, %{shard_id: ^a, schema_version: 0, status: "active"}} = Directory.get(a)
    assert {:ok, %{shard_id: ^b, schema_version: 0, status: "active"}} = Directory.get(b)
  end

  test "flushing an empty buffer is a no-op" do
    assert Recorder.flush() == 0
  end

  test "re-recording bumps recency without resetting version or status" do
    a = uniq()

    assert :ok = Recorder.record(a)
    assert Recorder.flush() == 1
    {:ok, first} = Directory.get(a)

    # Advance the shard's lifecycle the way a migration would.
    {:ok, _} = Directory.cutover(a, 5)

    assert :ok = Recorder.record(a)
    assert Recorder.flush() == 1
    {:ok, second} = Directory.get(a)

    # The on-conflict path only touches recency.
    assert second.schema_version == 5
    assert second.status == "active"
    assert DateTime.compare(second.last_active_at, first.last_active_at) in [:gt, :eq]
  end

  # Expert review #11: do_flush drained the ETS buffer BEFORE the Postgres write, and a
  # failed batch (outage) just logged — the drained touches were gone. Those touches feed
  # last_active_at, the sole input to the revert write-age force-guard, and a Postgres
  # outage is exactly when operators revert things. The invariant: a failed flush
  # re-buffers what it drained, so the touches land once Postgres recovers.
  test "a failed flush re-buffers the drained touches instead of dropping them" do
    import ExUnit.CaptureLog

    a = uniq()
    assert :ok = Recorder.record(a)

    # The outage: cut the recorder process off from Postgres, so record_batch raises
    # an ownership error inside the flush.
    Ecto.Adapters.SQL.Sandbox.mode(Fathom.Repo, :manual)

    log = capture_log(fn -> assert Recorder.flush() == 0 end)
    assert log =~ "Recorder flush"

    # Postgres recovers (a fresh shared owner): the touch must still be buffered
    # and flush through.
    owner = Ecto.Adapters.SQL.Sandbox.start_owner!(Fathom.Repo, shared: true)
    on_exit(fn -> Ecto.Adapters.SQL.Sandbox.stop_owner(owner) end)

    assert Recorder.flush() == 1
    assert {:ok, %{shard_id: ^a}} = Directory.get(a)
  end

  # Expert review #30: terminate/2 documents "don't lose the last window of touches on
  # a graceful stop" and flushes the buffer — but the Recorder never trapped exits, so a
  # supervisor :shutdown killed it outright and terminate NEVER ran: the documented
  # guarantee was dead code on every deploy, and the dropped touches feed
  # last_active_at, the revert write-age guard's sole input. The invariant: a graceful
  # stop flushes the buffered window.
  test "a supervisor shutdown flushes the buffered touches via terminate/2" do
    a = uniq()
    assert :ok = Recorder.record(a)

    pid = Process.whereis(Recorder)
    ref = Process.monitor(pid)
    :ok = Supervisor.terminate_child(Fathom.ControlPlane.Supervisor, Recorder)
    assert_receive {:DOWN, ^ref, :process, ^pid, :shutdown}, 1_000

    on_exit(fn -> Supervisor.restart_child(Fathom.ControlPlane.Supervisor, Recorder) end)

    assert {:ok, %{shard_id: ^a}} = Directory.get(a),
           "the buffered touch must be flushed by the graceful stop"

    {:ok, _} = Supervisor.restart_child(Fathom.ControlPlane.Supervisor, Recorder)
  end

  @tag :bench
  test "record/1 stays off the Postgres hot path (sub-50µs ETS write)" do
    id = uniq()
    # Warm the path, then measure a steady-state buffer write.
    Recorder.record(id)
    {us, :ok} = :timer.tc(fn -> Recorder.record(id) end)

    # An ETS insert is single-digit µs; a synchronous Postgres upsert (~100µs+,
    # see dir_resolve_p50_us) could never land under this ceiling.
    assert us < 50
  end
end
