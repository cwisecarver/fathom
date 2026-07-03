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

    test ":revalidate + still owned => proceed with a refreshed generation" do
      d =
        deps(%{
          valid_for_write: fn _ -> :revalidate end,
          check_lease: fn "shard_a", _lease -> :ok end,
          generation: fn -> 9 end
        })

      assert {:ok, %{lease: @lease, acquire_gen: 9}} = Fence.check(ctx(), d)
    end

    test ":revalidate + superseded => :superseded (self-fence, no {:ok,_})" do
      # generation is sampled before check_lease (finding #5), so it's read even on this path.
      d =
        deps(%{
          valid_for_write: fn _ -> :revalidate end,
          generation: fn -> 9 end,
          check_lease: fn _id, _lease -> {:error, :superseded} end
        })

      assert :superseded = Fence.check(ctx(), d)
    end

    test ":revalidate + transient store error => :skip (don't write)" do
      d =
        deps(%{
          valid_for_write: fn _ -> :revalidate end,
          generation: fn -> 9 end,
          check_lease: fn _id, _lease -> {:error, :timeout} end
        })

      assert :skip = Fence.check(ctx(), d)
    end

    # Finding #5: the revalidate baseline must be sampled BEFORE the ownership re-check. If
    # generation is read after check_lease returns :ok, a steal landing in that gap is folded
    # into the refreshed baseline and hidden, so every later flush passes unconditionally.
    test ":revalidate samples the generation before check_lease, not after" do
      calls = start_supervised!({Agent, fn -> [] end})
      record = fn tag -> Agent.update(calls, &[tag | &1]) end

      d =
        deps(%{
          valid_for_write: fn _ -> :revalidate end,
          generation: fn -> record.(:generation) && 9 end,
          check_lease: fn "shard_a", _lease -> record.(:check_lease) && :ok end
        })

      assert {:ok, %{acquire_gen: 9}} = Fence.check(ctx(), d)
      assert Enum.reverse(Agent.get(calls, & &1)) == [:generation, :check_lease]
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
