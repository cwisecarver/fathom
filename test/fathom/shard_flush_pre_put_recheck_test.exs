defmodule Fathom.ShardFlushPrePutRecheckTest do
  @moduledoc """
  Expert review 2026-10-19 #1. The round-1 #28 heartbeat recheck (`recheck_before_put/1`) ran BEFORE
  `Storage.flush/6`, whose S3 backend then compresses + hashes the snapshot (~5.6 ms/MB) and only
  then PUTs. On a large shard the codec time sat unfenced between the recheck and the PUT, so a
  heartbeat that lapsed during the codec still let the PUT go out under a lease a peer may hold.
  The fix runs the same check inside the `:pre_put` callback, which the backend invokes after the
  codec and immediately before the PUT.

  ## The seam

  `Fathom.Test.FaultyStorage.flush/6` runs `:faulty_before {:flush, fun}` first thing INSIDE the
  backend call — i.e. after the coordinator's early recheck and before the backend's `:pre_put`.
  That is exactly the window the codec occupies in production, so the hook publishes a past
  heartbeat deadline there, standing in for "the codec took longer than the margin".

  ## Both modes

  The check is HEARTBEAT-ONLY by design (`recheck_before_put/1`: legacy's equivalent is a renew
  PUT, a round trip per flush). So the legacy case is the control: it asserts the mode took
  (`acquire_gen` nil) and that the same hook does NOT veto the flush — nothing is fenced there
  and nothing should change.

  Not async: shards and the heartbeat are global.
  """
  use ExUnit.Case, async: false

  import ExUnit.CaptureLog

  alias Fathom.{Shard, ShardExecutor, Shards}
  alias Fathom.Shard.{Heartbeat, Storage}
  alias Filo.Stmt

  setup do
    shard = "prepput_#{System.unique_integer([:positive])}"
    prev_storage = Application.get_env(:fathom, :shard_storage)
    Application.put_env(:fathom, :shard_storage, Fathom.Test.FaultyStorage)

    on_exit(fn ->
      Application.delete_env(:fathom, :faulty_before)

      if prev_storage,
        do: Application.put_env(:fathom, :shard_storage, prev_storage),
        else: Application.delete_env(:fathom, :shard_storage)

      Shards.drain(shard, 2_000)

      for dir <- [Shard.data_dir(), Storage.Local.dir()],
          suffix <- [".db", ".db-wal", ".db-shm", ".db.etag", ".lock"],
          do: File.rm(Path.join(dir, shard <> suffix))

      for f <- Path.wildcard(Path.join(Shard.data_dir(), "#{shard}.db.*")), do: File.rm(f)
    end)

    %{shard: shard}
  end

  defp seed!(shard) do
    {:ok, conn} = ShardExecutor.open(shard)
    {:ok, _} = ShardExecutor.execute(conn, %Stmt{sql: "CREATE TABLE kv (v TEXT)", args: []})
    {:ok, _} = ShardExecutor.execute(conn, %Stmt{sql: "INSERT INTO kv VALUES ('x')", args: []})
    :ok = ShardExecutor.close(conn)
  end

  defp remote_db(shard), do: Path.join(Storage.Local.dir(), "#{shard}.db")

  # Publish a past deadline at the SAME generation: the process stays alive, so this is the real
  # `:not_valid` (a different generation would route to `:revalidate`).
  defp lapse_heartbeat! do
    state = :sys.get_state(Process.whereis(Heartbeat))
    Heartbeat.publish_status(%{state | mono_deadline_ms: System.monotonic_time(:millisecond) - 1})
  end

  # Fire `fun` inside the backend call for THIS shard only (the env is global and other
  # coordinators may still be flushing).
  defp on_backend_flush!(shard, fun) do
    Application.put_env(
      :fathom,
      :faulty_before,
      {:flush, fn id -> if id == shard, do: fun.() end}
    )
  end

  describe "heartbeat mode" do
    setup do
      hb = start_supervised!({Heartbeat, ttl_ms: 30_000})
      _ = :sys.get_state(hb)
      :ok
    end

    test "a heartbeat that lapses AFTER the early recheck (during the codec) vetoes the PUT",
         %{shard: shard} do
      seed!(shard)
      {:ok, coordinator} = Shards.ensure(shard)
      gen = :sys.get_state(coordinator).acquire_gen
      assert is_integer(gen), "expected heartbeat mode (non-nil acquire_gen)"
      assert Shard.dirty?(coordinator)

      test_pid = self()

      on_backend_flush!(shard, fn ->
        lapse_heartbeat!()
        send(test_pid, :lapsed_inside_backend)
      end)

      log =
        capture_log(fn ->
          # The early recheck passes (heartbeat healthy when the flush starts); only the in-backend
          # lapse can fail this flush.
          assert Heartbeat.valid_for_write?(gen) == :ok
          assert {:error, :ownership_unconfirmed_before_put} = Shards.flush(shard)
        end)

      assert_received :lapsed_inside_backend,
                      "the hook never ran, so the window was not exercised"

      assert Heartbeat.valid_for_write?(gen) == :not_valid

      # Symptom: the PUT went out under a lease whose liveness was unprovable.
      refute File.exists?(remote_db(shard)), "a PUT was issued after the heartbeat lapsed"
      assert log =~ "ownership_unconfirmed_before_put"
      assert Shard.dirty?(coordinator), "the shard must stay dirty to retry"
    end
  end

  describe "legacy mode (control)" do
    test "the in-backend hook does not veto: the check is heartbeat-only", %{shard: shard} do
      refute Heartbeat.running?()
      seed!(shard)
      {:ok, coordinator} = Shards.ensure(shard)
      assert is_nil(:sys.get_state(coordinator).acquire_gen), "expected legacy mode"

      test_pid = self()
      on_backend_flush!(shard, fn -> send(test_pid, :hook_ran) end)

      assert :ok = Shards.flush(shard)
      assert_received :hook_ran
      assert File.exists?(remote_db(shard))
    end
  end
end
