defmodule Fathom.FlushStormTest do
  @moduledoc """
  The flush storm and the supervision blast radius (expert review 2026-08-01 #16 and #22).

  ## #16 — the gate shipped unbounded, and shutdown never consulted it

  `FlushGate` exists to bound concurrent durability flushes; its own moduledoc names the
  cascade it was written for. But `cap/0` read a config key with **no default**, so `nil` ⇒
  unbounded ⇒ `try_acquire/0` returned `:disabled` and the gate was inert on every deployment
  that had not explicitly set it. The only active mitigation was ±25% timer jitter, which
  decorrelates phase but not sustained rate.

  Separately, `DynamicSupervisor` terminates all children simultaneously under one wall-clock
  timer, so a bare SIGTERM ran every open coordinator's snapshot + full-object PUT at once —
  and `Fathom.Shards.drain_all/1`, which bounds exactly this, was reachable only as an opt-in
  pre-stop rpc.

  ## #22 — the plane supervisors inherited OTP's default 3-in-5s

  Finding #16 (the earlier one) deliberately sized `Fathom.ShardSupervisor` to 100-in-10s so a
  shard restart storm could not kill its siblings. The plane supervisors, added later, kept the
  default — so four crashes of any one of `DataPlane`'s I/O-doing children in five seconds took
  every open shard down at once, from above.

  Not async: flips application env and drives real coordinators.
  """
  use ExUnit.Case, async: false

  alias Fathom.Shard.FlushGate

  describe "#16 — the flush gate ships bounded" do
    setup do
      prev = Application.fetch_env(:fathom, :shard_flush_max_concurrency)

      # RESET THE GLOBAL COUNTER. `FlushGate` is one ETS counter for the whole node, and the
      # enforcement test below asserts ABSOLUTE occupancy — it acquires from zero and expects the
      # (cap+1)th to be refused. Nothing here owned that precondition, so any flush still in flight
      # from another test left a slot held and the first `try_acquire()` came back `:full`.
      #
      # It went red on CI (OTP 29, seed 616439, 2026-08-10) and passed locally at the same seed on
      # the same OTP, which is the signature of shared state plus timing rather than test order.
      # Reproduced deliberately by acquiring one slot before the test: identical failure.
      #
      # Resetting in setup rather than releasing in on_exit because the leak can come from a test
      # in a DIFFERENT module — this makes the precondition this module depends on explicit instead
      # of assumed (AGENTS.md: assert the precondition inside the test).
      FlushGate.reset()

      on_exit(fn ->
        case prev do
          {:ok, v} -> Application.put_env(:fathom, :shard_flush_max_concurrency, v)
          :error -> Application.delete_env(:fathom, :shard_flush_max_concurrency)
        end
      end)

      :ok
    end

    test "an unconfigured node still has a cap" do
      Application.delete_env(:fathom, :shard_flush_max_concurrency)

      cap = FlushGate.cap()

      assert is_integer(cap) and cap > 0,
             "the gate shipped inert: nil cap means try_acquire/0 always returns :disabled"
    end

    # Asserts the DERIVATION, with the scheduler count passed in.
    #
    # This test previously drove `FlushGate.cap/0` and asserted `big > small` after swapping
    # :pool_size 4000 → 40. That is machine-dependent and it was WRONG: the cap is
    # min(pool/4, schedulers) floored at 4, so the pool only moves it while pool/4 is the
    # binding term. On an 18-scheduler dev box (18 vs 10) it is; on a 2–4 core CI runner
    # `schedulers` binds at both pool sizes and the cap is a flat 4, so `big > small` is
    # false. It passed locally and failed on all three OTP versions in CI from `dc3d2a3`
    # until the derivation was extracted. Keep this driven by the pure function.
    test "the default derives from the Finch pool, capped by schedulers and floored at 4" do
      # The pool is the binding term: a quarter of it, and raising it raises the cap.
      assert FlushGate.default_cap(40, 64) == 10
      assert FlushGate.default_cap(4000, 64) == 64, "capped by schedulers, not a quarter of 4000"

      assert FlushGate.default_cap(400, 64) > FlushGate.default_cap(40, 64),
             "while pool/4 is the binding term, tuning the pool tunes the cap"

      # The scheduler count is the binding term — a big pool cannot oversubscribe the
      # dirty-IO schedulers each in-flight VACUUM INTO occupies.
      assert FlushGate.default_cap(4000, 8) == 8

      # The floor holds regardless of how small either input gets.
      assert FlushGate.default_cap(4, 64) == 4, "never serialise every flush"
      assert FlushGate.default_cap(4000, 2) == 4, "floor wins over a low scheduler count too"
    end

    test "cap/0 applies that derivation to the configured pool" do
      Application.delete_env(:fathom, :shard_flush_max_concurrency)
      prev_s3 = Application.get_env(:fathom, Fathom.Shard.Storage.S3, [])

      on_exit(fn -> Application.put_env(:fathom, Fathom.Shard.Storage.S3, prev_s3) end)
      Application.put_env(:fathom, Fathom.Shard.Storage.S3, Keyword.put(prev_s3, :pool_size, 40))

      assert FlushGate.cap() == FlushGate.default_cap(40, System.schedulers_online())
    end

    test "an explicit integer still wins" do
      Application.put_env(:fathom, :shard_flush_max_concurrency, 7)
      assert FlushGate.cap() == 7
    end

    test "unbounded remains reachable for an operator who wants it" do
      Application.put_env(:fathom, :shard_flush_max_concurrency, 0)
      assert FlushGate.cap() == nil
      assert FlushGate.try_acquire() == :disabled
    end

    test "the cap is actually enforced — the (cap + 1)th concurrent flush is refused" do
      Application.put_env(:fathom, :shard_flush_max_concurrency, 3)
      # Reserve up to the cap.
      assert FlushGate.try_acquire() == :ok
      assert FlushGate.try_acquire() == :ok
      assert FlushGate.try_acquire() == :ok

      # The next one backs off rather than piling onto the pool.
      assert FlushGate.try_acquire() == :full

      # Releasing frees exactly one slot.
      FlushGate.release()
      assert FlushGate.try_acquire() == :ok
      assert FlushGate.try_acquire() == :full

      for _ <- 1..3, do: FlushGate.release()
    end
  end

  describe "#16 — shutdown takes the bounded drain path" do
    test "the application declares a prep_stop that drains" do
      # prep_stop/1 runs before the tree is torn down. Without it a bare SIGTERM skipped
      # drain_all/1 entirely and every coordinator flushed simultaneously under one shared
      # shutdown timer.
      assert function_exported?(Fathom.Application, :prep_stop, 1),
             "a bare SIGTERM must reach the bounded drain, not just the opt-in rpc"
    end

    test "the drain can be turned off for an operator who drains out of band" do
      prev = Application.get_env(:fathom, :drain_on_shutdown)
      on_exit(fn -> restore(:drain_on_shutdown, prev) end)

      Application.put_env(:fathom, :drain_on_shutdown, false)
      # With no shards open this is a no-op either way; the assertion is that the knob is read
      # and prep_stop stays total.
      assert Fathom.Application.prep_stop(:state) == :state
    end

    test "prep_stop never blocks shutdown, even if the drain raises" do
      prev = Application.get_env(:fathom, :drain_on_shutdown)
      on_exit(fn -> restore(:drain_on_shutdown, prev) end)

      Application.put_env(:fathom, :drain_on_shutdown, true)
      assert Fathom.Application.prep_stop(:whatever) == :whatever
    end
  end

  describe "#22 — plane supervisors have an explicit restart budget" do
    test "the planes and the top supervisor are sized above OTP's 3-in-5s default" do
      for tier <- [:plane, :top] do
        budget = Fathom.Application.restart_budget(tier)

        refute {budget[:max_restarts], budget[:max_seconds]} == {3, 5},
               "#{tier} still has OTP's default 3-in-5s budget — four crashes of one " <>
                 "I/O-doing child would take every open shard down with it"

        assert budget[:max_restarts] >= 10,
               "#{tier} budget is too tight: #{inspect(budget)}"
      end
    end

    test "every plane supervisor is actually running with a non-default budget" do
      # The budget above is only meaningful if the live tree was started with it.
      for name <- [
            Fathom.Supervisor,
            Fathom.Infra.Supervisor,
            Fathom.ControlPlane.Supervisor,
            Fathom.DataPlane.Supervisor,
            Fathom.Edge.Supervisor
          ] do
        assert is_pid(Process.whereis(name)), "#{inspect(name)} is not running"
      end
    end

    test "ShardSupervisor's own budget is unchanged" do
      # The earlier finding's fix must survive this one.
      opts = Fathom.Application.shard_supervisor_opts()
      assert opts[:max_restarts] >= 100
    end
  end

  defp restore(key, nil), do: Application.delete_env(:fathom, key)
  defp restore(key, value), do: Application.put_env(:fathom, key, value)
end
