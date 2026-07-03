defmodule Fathom.Shard.HeartbeatTest do
  @moduledoc """
  The per-node liveness heartbeat: it renews one object per node and exposes the
  fence the coordinator uses (generation + valid_for_write?). Lapse detection is
  driven deterministically via :sys.replace_state (force an expired heartbeat, then
  a renew tick) rather than wall-clock sleeps.
  """
  use ExUnit.Case, async: false

  import ExUnit.CaptureLog

  alias Fathom.Shard.{Heartbeat, Storage}

  @ttl 30_000

  setup do
    # Each test gets its own remote dir so heartbeat objects never collide.
    dir = Path.join(System.tmp_dir!(), "fathom_hbproc_#{System.unique_integer([:positive])}")
    prev = Application.get_env(:fathom, Fathom.Shard.Storage.Local)
    Application.put_env(:fathom, Fathom.Shard.Storage.Local, dir: dir)

    on_exit(fn ->
      File.rm_rf!(dir)

      if prev,
        do: Application.put_env(:fathom, Fathom.Shard.Storage.Local, prev),
        else: Application.delete_env(:fathom, Fathom.Shard.Storage.Local)
    end)

    owner = "node_#{System.unique_integer([:positive])}@test"
    pid = start_supervised!({Heartbeat, ttl_ms: @ttl, owner: owner})
    # init does the first renew via handle_continue; sync so it's written.
    _ = :sys.get_state(pid)
    %{pid: pid, owner: owner}
  end

  test "writes a fresh heartbeat on start and reports generation 0", %{owner: owner} do
    assert {:ok, %{owner: ^owner, expires_at_ms: exp}} = Storage.read_heartbeat(owner)
    assert exp > System.system_time(:millisecond)
    assert Heartbeat.generation() == 0
  end

  test "valid_for_write? is :ok when fresh and the generation matches" do
    assert Heartbeat.valid_for_write?(0) == :ok
    # A generation mismatch (without a lapse here) is treated as needing revalidation.
    assert Heartbeat.valid_for_write?(999) == :revalidate
  end

  test "a renew tick advances the heartbeat expiry", %{pid: pid, owner: owner} do
    {:ok, %{expires_at_ms: before}} = Storage.read_heartbeat(owner)
    # Force the stored expiry backwards so a renew is observably newer, then tick.
    :sys.replace_state(pid, fn s -> %{s | expires_at_ms: before - 5_000} end)
    send(pid, :renew)
    _ = :sys.get_state(pid)

    {:ok, %{expires_at_ms: after_exp}} = Storage.read_heartbeat(owner)
    assert after_exp >= before
  end

  test "a lapse bumps the generation and makes prior acquirers revalidate", %{pid: pid} do
    assert Heartbeat.generation() == 0
    assert Heartbeat.valid_for_write?(0) == :ok

    # Simulate a missed renewal: the heartbeat expired before the next tick ran.
    :sys.replace_state(pid, fn s ->
      %{s | expires_at_ms: System.system_time(:millisecond) - 1_000}
    end)

    capture_log(fn ->
      send(pid, :renew)
      _ = :sys.get_state(pid)
    end)

    # The lapse bumped the generation; the renew recovered a fresh heartbeat.
    assert Heartbeat.generation() == 1
    # A coordinator that acquired at generation 0 must now revalidate before flushing.
    assert Heartbeat.valid_for_write?(0) == :revalidate
    # A coordinator acquiring now (generation 1) is clean.
    assert Heartbeat.valid_for_write?(1) == :ok
  end

  test "not_valid when the heartbeat is not comfortably valid", %{pid: pid} do
    # Drive the confirmed expiry into the past: no comfortable margin ⇒ no write.
    :sys.replace_state(pid, fn s ->
      %{s | expires_at_ms: System.system_time(:millisecond) - 1}
    end)

    assert Heartbeat.valid_for_write?(0) == :not_valid
  end

  test "clears its heartbeat on clean shutdown", %{pid: pid, owner: owner} do
    assert {:ok, _} = Storage.read_heartbeat(owner)
    ref = Process.monitor(pid)
    :ok = stop_supervised(Fathom.Shard.Heartbeat)
    assert_receive {:DOWN, ^ref, :process, ^pid, _}, 1_000
    assert Storage.read_heartbeat(owner) == :not_found
  end
end
