defmodule Fathom.Shard.FenceTest do
  # The fence is the single "may I write this shard back to storage?" decision — the
  # double-write-avoidance core. These exercise every branch with injected fakes, so
  # the safety logic is pinned without a running heartbeat or real store. Invariant
  # that must never break: a lost/unconfirmed lease NEVER yields {:ok, _}.
  use ExUnit.Case, async: true

  alias Fathom.Shard.Fence

  @lease %{owner: "node_a", epoch: 3, expires_at_ms: 1_000_000}

  defp ctx(overrides \\ %{}) do
    Map.merge(%{id: "shard_a", lease: @lease, ttl_ms: 30_000, acquire_gen: 5}, overrides)
  end

  # Each dep defaults to a fun that fails the test if the fence calls it — so each
  # test asserts *exactly* which heartbeat/storage calls the decision made (e.g. the
  # heartbeat-:ok path must do NO per-shard I/O).
  defp deps(overrides) do
    Map.merge(
      %{
        valid_for_write: fn _gen -> flunk("valid_for_write? should not be called") end,
        generation: fn -> flunk("generation should not be called") end,
        check_lease: fn _id, _lease -> flunk("check_lease should not be called") end,
        renew_lease: fn _id, _lease, _ttl -> flunk("renew_lease should not be called") end
      },
      overrides
    )
  end

  describe "heartbeat mode" do
    test ":ok => proceed, lease/gen unchanged, no per-shard I/O" do
      d = deps(%{valid_for_write: fn 5 -> :ok end})
      assert {:ok, %{lease: @lease, acquire_gen: 5}} = Fence.check(ctx(), d)
    end

    test ":not_valid => :skip, no I/O" do
      d = deps(%{valid_for_write: fn _ -> :not_valid end})
      assert :skip = Fence.check(ctx(), d)
    end

    # THE REVALIDATION RENEWS THE LOCK, A WRITE (Jepsen 2026-10-24, `RevalidateCasTest`). These
    # :revalidate tests used to stub `check_lease` and assert the read-only check, which is what
    # let a thief that read the lock during the lapse win its If-Match steal after the owner had
    # resumed: two writers on one shard. A renew moves the lock etag, so one of the two wins.
    # `check_lease` is left to fail the test if the fence ever goes back to reading.
    test ":revalidate + still owned => renews the lock and proceeds with a refreshed generation" do
      renewed = %{@lease | expires_at_ms: 2_000_000}

      d =
        deps(%{
          valid_for_write: fn _ -> :revalidate end,
          renew_lease: fn "shard_a", @lease, 30_000 -> {:ok, renewed} end,
          generation: fn -> 9 end
        })

      assert {:ok, %{lease: ^renewed, acquire_gen: 9}} = Fence.check(ctx(), d)
    end

    # A HEARTBEAT-PROCESS EXIT DURING REVALIDATE MUST NOT NULL THE GENERATION
    # (expert review 2026-08-20 #30).
    #
    # `generation/1` maps the exit to nil, and every caller merges this result straight into
    # coordinator state. A nil `acquire_gen` permanently routes that coordinator to `legacy/2` --
    # the correct DEGRADATION, except the per-shard renewal timer is armed only ONCE, at open.
    # `schedule_renew/1` is otherwise reachable only from a timer already running, so a
    # coordinator downgraded mid-life renews its lock only when `check/2` runs -- and `check/2`
    # runs only on a FLUSH, which only happens while the shard is DIRTY.
    #
    # A read-only or lightly-written shard in that state never renews. Its `expires_at_ms` was set
    # once at acquire, so after ttl + steal_margin a peer legitimately steals the lease while this
    # node is still serving: a double-serve window with no partition and no heartbeat failure.
    test ":revalidate with the heartbeat process GONE keeps the old generation" do
      d =
        deps(%{
          valid_for_write: fn _ -> :revalidate end,
          renew_lease: fn "shard_a", lease, 30_000 -> {:ok, lease} end,
          generation: fn -> exit(:noproc) end
        })

      assert {:ok, %{lease: @lease, acquire_gen: 5}} = Fence.check(ctx(), d),
             "a momentary heartbeat exit nulled acquire_gen, permanently downgrading this " <>
               "coordinator to legacy mode WITHOUT arming its renewal timer -- so a quiet shard " <>
               "stops renewing its lock and is legitimately stolen while still serving"
    end

    # The contrast that shows the nil above was unintended rather than a decision: `check/2`'s own
    # `:legacy` branch has always preserved `ctx.acquire_gen`.
    test ":legacy preserves the generation, so the heartbeat is used again when it returns" do
      d =
        deps(%{
          valid_for_write: fn _ -> exit(:noproc) end,
          renew_lease: fn "shard_a", lease, 30_000 -> {:ok, %{lease | epoch: 3}} end
        })

      assert {:ok, %{acquire_gen: 5}} = Fence.check(ctx(), d)
    end

    test ":revalidate + superseded => :superseded (self-fence, no {:ok,_})" do
      # generation is sampled before the renew (finding #5), so it's read even on this path.
      d =
        deps(%{
          valid_for_write: fn _ -> :revalidate end,
          generation: fn -> 9 end,
          renew_lease: fn _id, _lease, _ttl -> {:error, :superseded} end
        })

      assert :superseded = Fence.check(ctx(), d)
    end

    test ":revalidate + transient store error => :skip (don't write)" do
      d =
        deps(%{
          valid_for_write: fn _ -> :revalidate end,
          generation: fn -> 9 end,
          renew_lease: fn _id, _lease, _ttl -> {:error, :timeout} end
        })

      assert :skip = Fence.check(ctx(), d)
    end

    # Finding #5: the revalidate baseline must be sampled BEFORE the ownership re-check. If
    # generation is read after the renew returns :ok, a steal landing in that gap is folded
    # into the refreshed baseline and hidden, so every later flush passes unconditionally.
    test ":revalidate samples the generation before the renew, not after" do
      calls = start_supervised!({Agent, fn -> [] end})
      record = fn tag -> Agent.update(calls, &[tag | &1]) end

      d =
        deps(%{
          valid_for_write: fn _ -> :revalidate end,
          generation: fn -> record.(:generation) && 9 end,
          renew_lease: fn "shard_a", lease, _ttl -> record.(:renew_lease) && {:ok, lease} end
        })

      assert {:ok, %{acquire_gen: 9}} = Fence.check(ctx(), d)
      assert Enum.reverse(Agent.get(calls, & &1)) == [:generation, :renew_lease]
    end

    test "heartbeat process down => falls back to the legacy renew fence" do
      new_lease = %{@lease | expires_at_ms: 2_000_000}

      d =
        deps(%{
          valid_for_write: fn _ -> exit(:noproc) end,
          renew_lease: fn "shard_a", _lease, 30_000 -> {:ok, new_lease} end
        })

      assert {:ok, %{lease: ^new_lease, acquire_gen: 5}} = Fence.check(ctx(), d)
    end
  end

  describe "legacy mode (acquire_gen nil)" do
    test "renew ok => proceed with the refreshed lease" do
      new_lease = %{@lease | expires_at_ms: 2_000_000}
      d = deps(%{renew_lease: fn "shard_a", _lease, 30_000 -> {:ok, new_lease} end})

      assert {:ok, %{lease: ^new_lease, acquire_gen: nil}} =
               Fence.check(ctx(%{acquire_gen: nil}), d)
    end

    test "renew superseded => :superseded" do
      d = deps(%{renew_lease: fn _id, _lease, _ttl -> {:error, :superseded} end})
      assert :superseded = Fence.check(ctx(%{acquire_gen: nil}), d)
    end

    test "renew transient error => :skip" do
      d = deps(%{renew_lease: fn _id, _lease, _ttl -> {:error, :timeout} end})
      assert :skip = Fence.check(ctx(%{acquire_gen: nil}), d)
    end
  end

  describe "generation/1" do
    test "returns the heartbeat generation" do
      assert 7 = Fence.generation(%{generation: fn -> 7 end})
    end

    test "nil when the heartbeat process is down" do
      assert nil == Fence.generation(%{generation: fn -> exit(:noproc) end})
    end
  end
end
