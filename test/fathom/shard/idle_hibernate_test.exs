defmodule Fathom.Shard.IdleHibernateTest do
  @moduledoc """
  Expert review 2026-10-22 r8 #12. SYMPTOM: `hibernate_after` was floored above the jittered flush
  tick (#32), which at prod's 300 s interval is 376 s, while an idle clean coordinator idle-stops at
  60 s (rig 20 s) — so no idle coordinator ever hibernated (-2.6 KB per idle coordinator lost).

  INVARIANT: when the flush interval is longer than the idle stop, a coordinator hibernates after
  half the idle stop; otherwise #32's floor stands. Pinned on the pure derivation and on a live
  coordinator in BOTH lease modes (the mode asserted to have taken), with a prod-shaped 300 s
  interval: it must reach the hibernated loop well before its idle stop.

  Not async: app env and real shard files.
  """
  use ExUnit.Case, async: false

  alias Fathom.{Shard, ShardExecutor, Shards}
  alias Fathom.Shard.{Heartbeat, Storage}

  describe "hibernate_after_ms/3 (pure)" do
    test "interval longer than the idle stop: half the idle stop" do
      assert Shard.hibernate_after_ms(300_000, 0.25, 60_000) == 30_000
      assert Shard.hibernate_after_ms(300_000, 0.25, 20_000) == 10_000
    end

    test "never below 1 s, never above #32's floor" do
      assert Shard.hibernate_after_ms(300_000, 0.25, 50) == 1_000
      assert Shard.hibernate_after_ms(40_000, 0.25, 39_000) == 19_500

      assert Shard.hibernate_after_ms(300_000, 0.25, 1_000_000) ==
               Shard.hibernate_after_ms(300_000, 0.25)
    end

    test "interval at or below the idle stop keeps #32's floor (the dirty-quiet tick race)" do
      assert Shard.hibernate_after_ms(30_000, 0.25, 60_000) ==
               Shard.hibernate_after_ms(30_000, 0.25)

      assert Shard.hibernate_after_ms(30_000, 0.25, 60_000) > round(30_000 * 1.25)
      assert Shard.hibernate_after_ms(5_000, 0.25, 60_000) == 30_000

      assert Shard.hibernate_after_ms(60_000, 0.25, 60_000) ==
               Shard.hibernate_after_ms(60_000, 0.25)
    end

    test "flush disabled or an unusable idle value falls back to the 2-arity floor" do
      assert Shard.hibernate_after_ms(0, 0.25, 60_000) == 30_000
      assert Shard.hibernate_after_ms(300_000, 0.25, 0) == Shard.hibernate_after_ms(300_000, 0.25)

      assert Shard.hibernate_after_ms(300_000, 0.25, nil) ==
               Shard.hibernate_after_ms(300_000, 0.25)
    end
  end

  describe "a live coordinator" do
    setup do
      id = "idlehib_#{System.unique_integer([:positive])}"
      keys = [:shard_flush_interval_ms, :shard_idle_ms]
      prev = for k <- keys, do: {k, Application.fetch_env(:fathom, k)}
      # Prod-shaped: interval far above the idle stop. Idle 6 s => hibernate after 3 s.
      Application.put_env(:fathom, :shard_flush_interval_ms, 300_000)
      Application.put_env(:fathom, :shard_idle_ms, 6_000)

      on_exit(fn ->
        for {k, v} <- prev do
          case v do
            {:ok, val} -> Application.put_env(:fathom, k, val)
            :error -> Application.delete_env(:fathom, k)
          end
        end

        Shards.stop(id)

        for dir <- [Shard.data_dir(), Storage.Local.dir()],
            s <- [".db", ".db-wal", ".db-shm", ".db.etag", ".lock"],
            do: File.rm(Path.join(dir, id <> s))
      end)

      %{id: id}
    end

    # Hibernated = parked in `erlang:hibernate/3` (how gen_server hibernates). Read with
    # Process.info, which sends the coordinator no message (any message would reset the timeout).
    defp hibernated?(pid) do
      Process.info(pid, :current_function) in [
        {:current_function, {:erlang, :hibernate, 3}},
        {:current_function, {:gen_server, :loop_hibernate, 4}}
      ]
    end

    defp await_hibernated(pid, deadline_ms) do
      cond do
        hibernated?(pid) ->
          :ok

        System.monotonic_time(:millisecond) > deadline_ms ->
          flunk("coordinator never hibernated: #{inspect(Process.info(pid, :current_function))}")

        true ->
          receive do
          after
            50 -> await_hibernated(pid, deadline_ms)
          end
      end
    end

    for mode <- [:legacy, :heartbeat] do
      test "[#{mode}] an idle clean coordinator hibernates before its idle stop", %{id: id} do
        if unquote(mode) == :heartbeat do
          hb = start_supervised!({Heartbeat, ttl_ms: 30_000})
          _ = :sys.get_state(hb)
          assert Heartbeat.running?()
        else
          refute Heartbeat.running?()
        end

        {:ok, conn} = ShardExecutor.open(id)
        {:ok, _} = ShardExecutor.execute(conn, %Filo.Stmt{sql: "SELECT 1", args: []})
        ShardExecutor.close(conn)
        {:ok, pid} = Shards.ensure(id)

        state = :sys.get_state(pid)

        if unquote(mode) == :heartbeat,
          do: assert(is_integer(state.acquire_gen), "heartbeat mode did not take"),
          else: assert(is_nil(state.acquire_gen), "legacy mode did not take")

        refute Shard.dirty?(pid), "fixture: an idle CLEAN coordinator"
        started = System.monotonic_time(:millisecond)

        # Hibernate after 3 s; the idle stop is at 6 s. Pre-fix hibernate_after was 301 s.
        await_hibernated(pid, started + 5_000)
      end
    end
  end
end
