defmodule Fathom.Shard.LivenessModeLatchTest do
  @moduledoc """
  A node CONFIGURED for the heartbeat never opens a coordinator in legacy mode by accident (expert
  review 2026-11-07 #59).

  `acquire_gen` is fixed at open: non-nil is heartbeat mode, nil is legacy (per-shard renew PUTs, no
  lapse subscription, Session ack ungated). The open sampled it as
  `if Heartbeat.running?(), do: Fence.generation(), else: nil`, and `Fence.generation/1` maps ANY
  exit to nil. So an open that overlapped a Heartbeat (re)start — the name is registered before
  `init/1` does its storage I/O, or between a crash and the supervisor's restart — latched LEGACY for
  the coordinator's whole life: it never learned of a later lapse and, with the renewal failing, never
  started the write-fence clock.

  ## The shape chosen, and why

  Refuse the open (a retryable 503, nothing acquired) instead of upgrading the mode later. An upgrade
  at the first revalidate has no sound trigger (a legacy coordinator receives no lapse broadcast) and
  would have to prove no steal happened while it was legacy, which is exactly what the generation
  baseline taken BEFORE the lease exists to prove. Refusing costs a client retry for the length of a
  Heartbeat restart; legacy mode stays reachable when the config says so (`heartbeat_server: false`:
  tests, dev).

  ## Also pinned: the legacy renew-failure clock lives in `write_fence_timer_test.exs`

  Not here: it needs that module's fixture.

  Not async: Heartbeat, the node-global config and storage.
  """
  use ExUnit.Case, async: false

  alias Fathom.Shard.{Heartbeat, Storage}
  alias Fathom.Test.{Await, Env}
  alias Fathom.{ShardExecutor, Shards}

  setup do
    shard = "latch_#{System.unique_integer([:positive])}"
    Env.put!(:shard_idle_ms, 600_000)

    on_exit(fn ->
      _ = Shards.stop(shard)
      for f <- Path.wildcard(Path.join(Fathom.Shard.data_dir(), "#{shard}.db*")), do: File.rm(f)
    end)

    %{shard: shard}
  end

  test "configured for the heartbeat but it is not running: the open is refused, nothing acquired",
       %{shard: shard} do
    Env.put!(:heartbeat_server, true)
    refute Heartbeat.running?(), "fixture: no Heartbeat"

    assert {:error, {:heartbeat_unavailable, :down}} = Shards.checkout(shard)

    assert Storage.lease_holder(shard) == :free, "a refused open must hold no lease"
    assert Registry.lookup(Fathom.ShardRegistry, shard) == []
  end

  test "the Hrana open answers a RETRYABLE 503 for it", %{shard: shard} do
    Env.put!(:heartbeat_server, true)
    refute Heartbeat.running?()

    assert {:error, %{status: 503, code: "FILO_UNAVAILABLE"}} = ShardExecutor.open(shard)
  end

  test "the Heartbeat is registered but cannot answer (booting / crashing): refused, not legacy",
       %{shard: shard} do
    Env.put!(:heartbeat_server, true)

    # Registered under the Heartbeat's name with no status table: `generation/0` falls to a
    # `GenServer.call`, which this stub answers by dying — what a Heartbeat does when `init/1` is
    # still doing storage I/O past the call's timeout, or crashes under the call.
    stub =
      spawn(fn ->
        Process.register(self(), Heartbeat)

        receive do
          {:"$gen_call", _from, _msg} -> exit(:boom)
        end
      end)

    on_exit(fn -> Process.exit(stub, :kill) end)
    Await.until(&Heartbeat.running?/0, "fixture: the stub never registered")

    assert {:error, {:heartbeat_unavailable, :booting}} = Shards.checkout(shard)
    assert Storage.lease_holder(shard) == :free
  end

  test "configured for the heartbeat and it is running: the open is heartbeat mode", %{
    shard: shard
  } do
    Env.put!(:heartbeat_server, true)
    hb = start_supervised!({Heartbeat, ttl_ms: 30_000})
    _ = :sys.get_state(hb)

    assert {:ok, pid} = Shards.ensure(shard)
    assert is_integer(:sys.get_state(pid).acquire_gen)
  end

  test "configured OFF (tests, dev): legacy mode is still the answer", %{shard: shard} do
    Env.put!(:heartbeat_server, false)
    refute Heartbeat.running?()

    assert {:ok, pid} = Shards.ensure(shard)
    assert is_nil(:sys.get_state(pid).acquire_gen)
  end
end
