defmodule Fathom.Shard.ReplicationBudgetTest do
  @moduledoc """
  The per-node queued-byte ceiling — the safety net under `Primary.plan/3`'s per-push cap.
  See `docs/reviews/a2-shipper-feedback-loop-2026-08-16.md`.

  Everything here is about a quantity three previous fixes measured wrong. `Shipper`'s
  `:replication_max_queue` counts MESSAGES per shipper, and the run that killed a node had a queue
  of 6,263 against a cap of 8,192 — under it, so it never fired — while the node held 45 GB. This
  module counts BYTES and sums them across the node, which is the arithmetic that was never done.

  Two properties get the most attention because they are the ones a plausible implementation gets
  wrong:

    * **reserve and release must be symmetric.** A release without its reserve drives the counter
      negative and manufactures budget out of nothing, which would make the ceiling silently rise
      every time a push was refused for some other reason.
    * **a shipper's counter must not outlive the shipper.** A process killed with a full mailbox
      never runs the releases its queued messages owed. With one long-lived counter those bytes are
      charged forever and replication eventually refuses everything — a fix that converts an OOM
      into a permanent outage is not a fix.
  """
  use ExUnit.Case, async: false

  alias Fathom.Shard.Replication.Budget
  alias Fathom.Shard.Replication.Fleet

  @cap 10_000

  setup do
    prev_cap = Application.get_env(:fathom, :replication_max_queue_bytes)
    Application.put_env(:fathom, :replication_max_queue_bytes, @cap)

    # `Budget.queued/0` sums over the shippers Fleet publishes, so the fleet view IS part of the
    # unit under test — a name with a counter that Fleet does not publish must not count.
    names = for i <- 1..3, do: :"budget_test_shipper_#{System.unique_integer([:positive])}_#{i}"
    for n <- names, do: Budget.install(n)
    published = Enum.map(names, fn n -> {to_string(n), "127.0.0.1", 9100, n} end)
    Fleet.publish(published)

    on_exit(fn ->
      for n <- names, do: Budget.forget(n)
      Fleet.publish([])

      if prev_cap do
        Application.put_env(:fathom, :replication_max_queue_bytes, prev_cap)
      else
        Application.delete_env(:fathom, :replication_max_queue_bytes)
      end
    end)

    %{names: names, a: Enum.at(names, 0), b: Enum.at(names, 1), c: Enum.at(names, 2)}
  end

  describe "reserve/2 and release/2" do
    test "a reservation is visible per shipper and in the node total", %{a: a, b: b} do
      assert Budget.queued() == 0

      assert {:ok, 100} = Budget.reserve(a, 100)
      assert {:ok, 250} = Budget.reserve(b, 250)

      assert Budget.queued(a) == 100
      assert Budget.queued(b) == 250
      assert Budget.queued() == 350
    end

    test "releasing exactly what was reserved returns to zero", %{a: a} do
      assert {:ok, 400} = Budget.reserve(a, 400)
      assert :ok = Budget.release(a, 400)
      assert Budget.queued(a) == 0
      assert Budget.queued() == 0
    end

    test "the reported total never goes negative", %{a: a} do
      # A release without a reserve is a bug, but reporting a NEGATIVE budget would be worse than
      # the bug: it hands the node free headroom above the cap, so the ceiling silently rises.
      assert :ok = Budget.release(a, 500)
      assert Budget.queued(a) == 0
      assert Budget.queued() == 0
    end
  end

  describe "the ceiling is per NODE, not per shipper" do
    test "shippers each under the cap are refused once their SUM crosses it", ctx do
      %{a: a, b: b, c: c} = ctx

      # This is the exact arithmetic `:replication_max_queue` never did. Each of these is well
      # under the cap on its own; together they are not.
      assert {:ok, 4000} = Budget.reserve(a, 4000)
      assert {:ok, 4000} = Budget.reserve(b, 4000)
      assert Budget.queued() == 8000

      assert :rejected = Budget.reserve(c, 4000)

      # A rejected reservation must leave NOTHING behind, or a saturated link would ratchet the
      # total up on every refusal and never recover.
      assert Budget.queued(c) == 0
      assert Budget.queued() == 8000
    end

    test "refusing frees up again once the queue drains", %{a: a, b: b} do
      assert {:ok, 9000} = Budget.reserve(a, 9000)
      assert :rejected = Budget.reserve(b, 2000)

      assert :ok = Budget.release(a, 9000)
      assert {:ok, 2000} = Budget.reserve(b, 2000)
    end

    test "a reservation landing exactly ON the cap is allowed", %{a: a} do
      assert {:ok, @cap} = Budget.reserve(a, @cap)
      assert Budget.queued() == @cap
      assert :rejected = Budget.reserve(a, 1)
    end
  end

  describe "the bound can be switched off" do
    test "cap 0 disables it and reserves nothing", %{a: a} do
      Application.put_env(:fathom, :replication_max_queue_bytes, 0)

      assert {:ok, 0} = Budget.reserve(a, 5_000_000)
      assert Budget.queued(a) == 0
      assert Budget.queued() == 0
    end

    test "a shipper with no counter is unbounded rather than refused" do
      # The test-only shape: an unnamed shipper. Refusing over it would break every fixture that
      # starts one without a `:name`, which is a worse trade than leaving the net off for a process
      # production never creates.
      assert {:ok, 0} = Budget.reserve(:budget_test_never_installed, 5_000_000)
      assert Budget.queued(:budget_test_never_installed) == 0
    end
  end

  describe "counters do not outlive their shipper" do
    test "install/1 issues a FRESH counter, so a stranded count cannot be inherited", %{a: a} do
      # Simulates the case that makes this design necessary: a shipper killed while holding queued
      # messages, whose releases therefore never run. With one long-lived counter those bytes stay
      # charged to the node forever and replication eventually refuses every write.
      assert {:ok, 9_500} = Budget.reserve(a, 9_500)
      assert Budget.queued(a) == 9_500

      Budget.install(a)

      assert Budget.queued(a) == 0
      assert Budget.queued() == 0
      assert {:ok, 9_500} = Budget.reserve(a, 9_500)
    end

    test "a forgotten shipper stops counting", %{a: a, b: b} do
      assert {:ok, 3_000} = Budget.reserve(a, 3_000)
      assert {:ok, 3_000} = Budget.reserve(b, 3_000)
      assert Budget.queued() == 6_000

      Budget.forget(a)
      assert Budget.queued() == 3_000
    end

    test "a counter Fleet does not publish is not part of the node total", %{a: a} do
      orphan = :"budget_test_orphan_#{System.unique_integer([:positive])}"
      Budget.install(orphan)
      on_exit(fn -> Budget.forget(orphan) end)

      assert {:ok, 5_000} = Budget.reserve(orphan, 5_000)

      # It has bytes, but it is not in `Fleet.shippers/0` — a follower removed by a membership swap
      # must stop counting against the node the moment it stops being shipped to.
      assert Budget.queued(orphan) == 5_000
      assert Budget.queued() == 0

      # And therefore does not consume headroom from the shippers that ARE live.
      assert {:ok, @cap} = Budget.reserve(a, @cap)
    end
  end

  # WHERE THE PER-INCARNATION REFS LIVE (expert review 2026-08-20 #36).
  #
  # `install/1` was a `:persistent_term.put` and `forget/1` an `erase` -- ONCE PER SHIPPER
  # INCARNATION. Each schedules a literal-area cleanup that must scan every process on the node,
  # which at the 10k-30k shard processes fathom targets is not free, and it fired hardest during a
  # shipper restart STORM: the documented `shipper connection lost: :timeout` under saturation,
  # where `send_timeout_close: true` tears sockets down. The cost landed exactly where there was no
  # headroom.
  #
  # The per-incarnation ref is the RIGHT design and did not change (a single node-wide counter
  # would drift: a shipper killed with a full mailbox never runs the releases its queued messages
  # owed). Only the publication mechanism moved, to an ETS table `Fleet`'s supervisor owns.
  describe "publication does not churn the literal area (#36)" do
    test "a shipper's counter costs no persistent term, on install OR on forget" do
      names = for i <- 1..20, do: :"pt_budget_#{System.unique_integer([:positive])}_#{i}"

      before = :persistent_term.info()[:count]

      for n <- names, do: Budget.install(n)

      assert :persistent_term.info()[:count] == before,
             "installing shipper counters wrote persistent terms. Each one schedules a global " <>
               "literal-area cleanup that scans every process on the node, and this fires once " <>
               "per shipper INCARNATION -- i.e. hardest during a restart storm on an already " <>
               "overloaded node."

      # PRECONDITION: the counters really were installed. A no-op install would satisfy the
      # assertion above while proving nothing.
      Enum.each(names, fn n ->
        assert {:ok, 10} = Budget.reserve(n, 10)
        assert Budget.queued(n) == 10
      end)

      for n <- names, do: Budget.forget(n)

      assert :persistent_term.info()[:count] == before,
             "forgetting a departed shipper's counter erased a persistent term"

      # And forgetting really forgot: no ref, so no budget, which is the documented answer for a
      # shipper with no counter.
      Enum.each(names, fn n -> assert Budget.queued(n) == 0 end)
    end

    test "a fresh install still supersedes the previous incarnation's counter" do
      n = :"pt_supersede_#{System.unique_integer([:positive])}"
      Budget.install(n)
      on_exit(fn -> Budget.forget(n) end)

      {:ok, 500} = Budget.reserve(n, 500)
      assert Budget.queued(n) == 500

      # The whole reason for a ref per incarnation: a shipper killed with a full mailbox never runs
      # the release/2 calls its queued messages owed, and those bytes must die with it rather than
      # be charged to the node forever.
      Budget.install(n)
      assert Budget.queued(n) == 0
    end
  end
end
