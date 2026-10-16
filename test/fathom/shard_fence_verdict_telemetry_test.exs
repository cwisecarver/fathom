defmodule Fathom.ShardFenceVerdictTelemetryTest do
  @moduledoc """
  The flush fence's verdict is counted at the call site in `Fathom.Shard` (expert review 2026-10-15
  #9): `[:fathom, :shard, :fence, :verdict]` with bounded `verdict` x `mode` tags. Before this, a
  node whose fence started answering `:skip` (ownership unconfirmed) was visible only as a Logger
  line.

  Parameterized over BOTH liveness modes and asserts each mode took (acquire_gen nil / non-nil),
  per AGENTS.md. The heartbeat `:skip` fixture is the documented one: the process ALIVE with a past
  deadline published at the SAME generation (killing it degrades to the legacy fence instead).
  """
  use ExUnit.Case, async: false

  import ExUnit.CaptureLog

  alias Fathom.{Shard, ShardExecutor, Shards}
  alias Fathom.Shard.{Heartbeat, Storage}
  alias Filo.Stmt

  @modes [:legacy, :heartbeat]

  setup do
    shard = "fenceverdict_#{System.unique_integer([:positive])}"
    prev_storage = Application.get_env(:fathom, :shard_storage)
    Application.put_env(:fathom, :shard_storage, Fathom.Test.FaultyStorage)

    on_exit(fn ->
      Application.delete_env(:fathom, :storage_fault)

      if prev_storage,
        do: Application.put_env(:fathom, :shard_storage, prev_storage),
        else: Application.delete_env(:fathom, :shard_storage)

      Shards.drain(shard, 2_000)

      for dir <- [Shard.data_dir(), Storage.Local.dir()],
          suffix <- [".db", ".db-wal", ".db-shm", ".db.etag", ".lock"],
          do: File.rm(Path.join(dir, shard <> suffix))

      for f <- Path.wildcard(Path.join(Shard.data_dir(), "#{shard}.db.*")), do: File.rm(f)
    end)

    me = self()
    id = {__MODULE__, shard}

    :telemetry.attach(
      id,
      [:fathom, :shard, :fence, :verdict],
      fn _e, _m, meta, _ -> send(me, {:verdict, meta}) end,
      nil
    )

    on_exit(fn -> :telemetry.detach(id) end)
    %{shard: shard}
  end

  defp set_mode!(:legacy) do
    refute Heartbeat.running?()
    :ok
  end

  defp set_mode!(:heartbeat) do
    hb = start_supervised!({Heartbeat, ttl_ms: 30_000})
    _ = :sys.get_state(hb)
    assert Heartbeat.running?()
    :ok
  end

  defp assert_mode!(coordinator, :heartbeat),
    do: assert(is_integer(:sys.get_state(coordinator).acquire_gen), "heartbeat mode did not take")

  defp assert_mode!(coordinator, :legacy),
    do: assert(is_nil(:sys.get_state(coordinator).acquire_gen), "legacy mode did not take")

  defp seed!(shard) do
    {:ok, conn} = ShardExecutor.open(shard)
    {:ok, _} = ShardExecutor.execute(conn, %Stmt{sql: "CREATE TABLE kv (v TEXT)", args: []})
    {:ok, _} = ShardExecutor.execute(conn, %Stmt{sql: "INSERT INTO kv VALUES ('x')", args: []})
    :ok = ShardExecutor.close(conn)
  end

  defp drain!(shard, coordinator) do
    ref = Process.monitor(coordinator)

    capture_log(fn ->
      Shards.drain(shard, 5_000)
      assert_receive {:DOWN, ^ref, :process, ^coordinator, _}, 5_000
    end)
  end

  for mode <- @modes do
    describe "#{mode} mode" do
      setup do: set_mode!(unquote(mode))

      test "a confirmed fence on the drop flush is counted :ok", %{shard: shard} do
        seed!(shard)
        {:ok, coordinator} = Shards.ensure(shard)
        assert_mode!(coordinator, unquote(mode))
        assert Shard.dirty?(coordinator), "the fixture must be dirty to reach the fence"

        drain!(shard, coordinator)

        assert_receive {:verdict, %{shard_id: ^shard, verdict: :ok, mode: unquote(mode)}}, 2_000
      end

      test "an unconfirmed fence is counted :skip (regression: it was a log line only)", %{
        shard: shard
      } do
        seed!(shard)
        {:ok, coordinator} = Shards.ensure(shard)
        assert_mode!(coordinator, unquote(mode))

        if unquote(mode) == :legacy do
          Application.put_env(:fathom, :storage_fault, :renew)
        else
          state = :sys.get_state(Process.whereis(Heartbeat))

          Heartbeat.publish_status(%{
            state
            | mono_deadline_ms: System.monotonic_time(:millisecond) - 1
          })

          assert Heartbeat.valid_for_write?(:sys.get_state(coordinator).acquire_gen) ==
                   :not_valid,
                 "the fixture did not produce :not_valid"
        end

        drain!(shard, coordinator)

        assert_receive {:verdict, %{shard_id: ^shard, verdict: :skip, mode: unquote(mode)}}, 2_000
      end
    end
  end
end
