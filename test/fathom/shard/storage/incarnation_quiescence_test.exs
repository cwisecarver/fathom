defmodule Fathom.Shard.Storage.IncarnationQuiescenceTest do
  @moduledoc """
  Proving a predecessor is really gone before stealing its locks (expert review 2026-08-20 #10).

  `mark_incarnation_dead/1` records that a predecessor's **heartbeat object** stopped being
  renewed. That is not a proof that the process stopped. A node whose `Heartbeat` GenServer died
  but which keeps SERVING produces exactly that reading — and it is the case finding #11's
  `:not_found` branch exists to protect, because such a node degrades to the legacy per-shard renew
  fence and goes on renewing every lock it holds while its heartbeat sits frozen. Taking the fast
  steal against it is a double-serve window with no partition involved.

  ## The finding's own fix is rejected, and that is the point of this file

  It proposed gating the fast path on `lock_expires_at_ms <= now`. `judge_previous/1` marks a
  predecessor dead the moment its HEARTBEAT is stale past `steal_margin_ms` — and at that instant a
  genuinely crashed predecessor's LOCKS are still in the **future**, because they were renewed
  shortly before the crash. The condition is false in exactly the case round-2 #34 optimizes, so it
  would silently revert #34 and reintroduce a per-shard stall on every fast restart. There is a
  test below that pins that case specifically.

  ## Why the state machine is tested as a pure function

  The real discriminator is "are the locks still being RENEWED", which needs two observations
  separated by more than the renewal cadence. Driving that through a live store would mean either a
  ten-second sleep or a fake clock threaded through two backends. `judge_quiescence/5` takes the
  state, the observation and the clock as arguments, so every branch is reachable in microseconds —
  and it is shared by both backends, so neither can drift from the other.
  """
  use ExUnit.Case, async: false

  alias Fathom.Shard.Storage

  @probe 10_000
  @owner "nonode@nohost#deadbeef"

  defp judge(state, shard, expiry, now),
    do: Storage.judge_quiescence(state, shard, expiry, now, @probe)

  describe "the two-read state machine" do
    test "a first observation never permits the steal — it only starts the clock" do
      assert {false, {:sample, "acme", 5_000, 100}} = judge(nil, "acme", 5_000, 100)
    end

    test "an unchanged expiry, a full probe window later, means nobody is renewing" do
      state = {:sample, "acme", 5_000, 100}
      assert {true, :quiescent} = judge(state, "acme", 5_000, 100 + @probe)
    end

    test "not enough time yet is NOT a proof of anything" do
      state = {:sample, "acme", 5_000, 100}

      assert {false, :keep} = judge(state, "acme", 5_000, 100 + @probe - 1),
             "silence shorter than the renewal cadence catches a live renewer BETWEEN renewals " <>
               "and calls it dead"
    end

    test "an expiry that MOVED is a live process, and the verdict is sticky" do
      state = {:sample, "acme", 5_000, 100}
      assert {false, :renewing} = judge(state, "acme", 9_000, 100 + @probe)

      # Sticky: a predecessor observed renewing is a LIVE process. Nothing short of a fresh death
      # proof (a new owner string, which gets its own entry) should make it stealable again.
      assert {false, :keep} = judge(:renewing, "acme", 9_000, 999_999)
    end

    test "a DIFFERENT shard does not disturb the sample" do
      state = {:sample, "acme", 5_000, 100}

      # Its expiry says nothing about the sampled lock, and replacing the sample would restart the
      # clock forever on a node opening many shards — the sample has to be allowed to age.
      assert {false, :keep} = judge(state, "beta", 77, 100 + @probe)
      assert {true, :quiescent} = judge(state, "acme", 5_000, 100 + @probe)
    end

    test "once quiescent, every shard of that incarnation is stealable with no further probing" do
      assert {true, :keep} = judge(:quiescent, "anything", 12_345, 1)
    end
  end

  describe "the composed check" do
    setup do
      Storage.reset_quiescence()
      on_exit(&Storage.reset_quiescence/0)
      :ok
    end

    test "an owner that was never proven dead is never fast-stealable, however quiet" do
      # The heartbeat proof is still REQUIRED, not replaced. A foreign owner whose liveness we
      # cannot know must always fall back to the lock TTL.
      refute Storage.fast_steal_ok?("some-other-node", "acme", 5_000, 100)
      refute Storage.fast_steal_ok?("some-other-node", "acme", 5_000, 100 + 10_000_000)
    end

    test "a proven-dead incarnation still needs the renewal probe to settle" do
      Storage.mark_incarnation_dead(@owner)

      refute Storage.fast_steal_ok?(@owner, "acme", 5_000, 100),
             "the heartbeat proof alone allowed the steal. A node whose Heartbeat process died " <>
               "but which is still SERVING reads exactly like this, and it keeps renewing every " <>
               "lock it holds — stealing from it is a double-serve window with no partition."

      # Same lock, unchanged, a full probe window later.
      assert Storage.fast_steal_ok?(@owner, "acme", 5_000, 100 + 10_000_000)

      # And the verdict now applies to the whole incarnation, not just the sampled shard.
      assert Storage.fast_steal_ok?(@owner, "some-other-shard", 999, 100 + 10_000_000)
    end

    test "a predecessor still renewing is never fast-stolen from" do
      Storage.mark_incarnation_dead(@owner)

      refute Storage.fast_steal_ok?(@owner, "acme", 5_000, 100)

      # Its lock moved forward: something is renewing it. That is a live process.
      refute Storage.fast_steal_ok?(@owner, "acme", 40_000, 100 + 10_000_000)

      # Sticky across every shard, so one live observation protects all of them.
      refute Storage.fast_steal_ok?(@owner, "beta", 1, 100 + 99_000_000)
    end

    # THE CASE THAT KILLS THE FINDING'S OWN PROPOSED FIX.
    #
    # It suggested gating on `lock_expires_at_ms <= now`. A genuinely crashed predecessor renewed
    # its locks shortly before dying, so at the moment its heartbeat is judged stale those locks
    # are still in the FUTURE. Under the proposed condition this steal would be refused — which is
    # precisely the per-shard stall round-2 #34 exists to erase.
    test "a crashed predecessor whose locks are still in the FUTURE is stealable once quiescent" do
      Storage.mark_incarnation_dead(@owner)

      now = 1_000_000
      future_expiry = now + 25_000

      refute Storage.fast_steal_ok?(@owner, "acme", future_expiry, now)

      assert Storage.fast_steal_ok?(@owner, "acme", future_expiry, now + 10_000_000),
             "a proven-dead predecessor's locks are ALWAYS in the future at the moment of the " <>
               "proof — gating on expiry would refuse every fast restart, which is exactly what " <>
               "round-2 #34 removed"
    end
  end
end
