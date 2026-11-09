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
    #
    # ONLY this test's coordinator may kill it (CI 2026-11-08: "the stub never registered", a flake
    # no local run reproduced). While `heartbeat_server` is true and no real Heartbeat is up, ANY
    # process that asks the Heartbeat for a generation or a fence verdict (a shard or session an
    # earlier test left running) lands a `GenServer.call` on whatever holds the name, and a stub
    # that died for every caller was killed by it before the open under test ever called. A
    # stray's call goes unanswered (it times out, which the fence callers already read as "down"),
    # and the registration is announced by message rather than polled for, so a stub that lived
    # for a moment is not mistaken for one that never registered.
    test_pid = self()

    stub =
      spawn(fn ->
        try do
          Process.register(self(), Heartbeat)
        rescue
          e in ArgumentError ->
            holder = Process.whereis(Heartbeat)

            send(
              test_pid,
              {:stub_not_registered, Exception.message(e),
               holder && Process.info(holder, [:initial_call, :current_function, :status])}
            )

            exit(:normal)
        end

        send(test_pid, :stub_registered)
        stub_loop(shard)
      end)

    on_exit(fn -> Process.exit(stub, :kill) end)

    receive do
      :stub_registered ->
        :ok

      {:stub_not_registered, message, holder} ->
        flunk(
          "fixture: the stub could not register (#{message}); name held by #{inspect(holder)}"
        )
    after
      5_000 -> flunk("fixture: the stub never registered")
    end

    # PRECONDITION of the assertions below: a call from a process that is not this shard's
    # coordinator is consumed (the stub's mailbox drains) and does NOT take the stub down. Before
    # the stub told callers apart, this was the CI flake.
    spawn(fn ->
      send(Heartbeat, {:"$gen_call", {self(), make_ref()}, :generation})
      send(test_pid, :stray_sent)
    end)

    assert_receive :stray_sent, 5_000

    Await.until(
      fn -> Process.info(stub, :message_queue_len) == {:message_queue_len, 0} end,
      "fixture: the stub never consumed the stray call (or a stray caller took it down)"
    )

    assert Process.whereis(Heartbeat) == stub, "a stray caller took the stub down"

    assert {:error, {:heartbeat_unavailable, :booting}} = Shards.checkout(shard)
    assert Storage.lease_holder(shard) == :free
  end

  # The stub's life: dies for the coordinator of `shard` (the open under test), ignores every
  # other caller and message.
  defp stub_loop(shard) do
    receive do
      {:"$gen_call", {caller, _tag}, _msg} ->
        if Registry.keys(Fathom.ShardRegistry, caller) == [shard] do
          exit(:boom)
        else
          stub_loop(shard)
        end

      _other ->
        stub_loop(shard)
    end
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
