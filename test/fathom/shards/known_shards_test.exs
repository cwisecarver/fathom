defmodule Fathom.Shards.KnownShardsTest do
  @moduledoc """
  The novel-shard rate gate's node-local "known" set (expert review 2026-10-01 #18).

  With `NOVEL_SHARD_RATE` on, every cold open asked Postgres whether the shard exists, and a shard
  waking from an idle drop has no local file to short-circuit it — so every reopen of a known shard
  read the directory. These pin the decision in `Shards.novelty/4`: the set answers for the RATE
  gate only, never for fork-from-template, and the real check runs at most once and only when a
  gate needs it.
  """
  use ExUnit.Case, async: false

  alias Fathom.Shards
  alias Fathom.Shards.KnownShards

  # A `fresh` that records each call, so "the directory was not read" is observable.
  defp fresh(answer) do
    test = self()

    fn ->
      send(test, :fresh_called)
      answer
    end
  end

  describe "novelty/4" do
    test "a KNOWN shard skips the directory read when only the rate gate is on" do
      assert {false, false} = Shards.novelty(true, false, true, fresh(true))
      refute_received :fresh_called, "a known shard still paid the synchronous directory read"
    end

    test "an unknown shard is checked once and gated by its real novelty" do
      assert {true, false} = Shards.novelty(true, false, false, fresh(true))
      assert_received :fresh_called
      refute_received :fresh_called

      assert {false, false} = Shards.novelty(true, false, false, fresh(false))
    end

    test "fork-from-template NEVER trusts the cache" do
      # A deleted-and-recreated tenant is cached as known; it must still be forked if it is
      # genuinely novel, or it is born empty instead of at HEAD.
      assert {false, true} = Shards.novelty(true, true, true, fresh(true))
      assert_received :fresh_called
    end

    test "both gates on and unknown: one fresh check serves both" do
      assert {true, true} = Shards.novelty(true, true, false, fresh(true))
      assert_received :fresh_called
      refute_received :fresh_called, "novelty was computed twice (review 2026-07-23 #28)"
    end

    test "no gate on: nothing is read" do
      assert {false, false} = Shards.novelty(false, false, false, fresh(true))
      refute_received :fresh_called
    end
  end

  test "a successful open records the shard as known" do
    id = "known_#{System.unique_integer([:positive])}"

    on_exit(fn ->
      for e <- ["", "-wal", "-shm"] do
        File.rm(Fathom.Shard.db_path(id) <> e)
        File.rm(Path.join(Fathom.Shard.Storage.Local.dir(), "#{id}.db") <> e)
      end
    end)

    refute KnownShards.known?(id)
    {:ok, h} = Fathom.ShardExecutor.open(id)
    assert KnownShards.known?(id)
    :ok = Fathom.ShardExecutor.close(h)
  end

  # Expert review 2026-10-17 #13: the set used to be cleared wholesale at @max_entries, so every known
  # shard became unknown at once and a fleet at the cap paid a directory read per shard in one burst.
  # It is now two generations (current + previous), and a lookup checks both. These pin the two
  # properties that matter: an id survives one rotation, and the set stays near two generations.
  describe "two generations (#13)" do
    # The tables are shared with every other test in the file, so start each one from empty. Direct
    # ETS, not a new API, so the module's public surface is unchanged.
    setup do
      :ets.delete_all_objects(KnownShards)
      :ets.delete_all_objects(KnownShards.Prev)
      :ok
    end

    defp fill(n, prefix) do
      for i <- 1..n, do: KnownShards.put("#{prefix}#{i}")
    end

    defp total_size,
      do: :ets.info(KnownShards, :size) + :ets.info(KnownShards.Prev, :size)

    test "an id from before one rotation is still known; after two it is forgotten" do
      cap = KnownShards.max_entries()

      KnownShards.put("old")
      assert KnownShards.known?("old")

      # Fill the current generation to the cap. The next put is the one that rotates.
      fill(cap - 1, "g1_")
      KnownShards.put("after_one")

      assert KnownShards.known?("old"),
             "an id from before one rotation was forgotten, which is the wholesale clear back"

      assert KnownShards.known?("after_one")

      # PRECONDITION: a rotation really happened. The previous generation holds the full first
      # generation and the current one holds the id put after the rotation. A clear-all would leave 1.
      assert total_size() == cap + 1, "the fixture never rotated, so the test proved nothing"

      fill(cap - 1, "g2_")
      KnownShards.put("after_two")

      refute KnownShards.known?("old"), "an id survived two rotations"
      assert KnownShards.known?("after_one"), "the generation one rotation back was dropped early"
      assert KnownShards.known?("after_two")
    end

    test "the set never holds more than about two generations" do
      cap = KnownShards.max_entries()

      fill(3 * cap, "bulk_")

      total = total_size()

      assert total <= 2 * cap, "the set grew past two generations (#{total} > #{2 * cap})"

      # PRECONDITION: it did rotate, so the bound is the two-generation bound and not a trivially
      # small table. A clear-all would sit at or below one generation.
      assert total > cap, "the set never reached two generations, so the bound proved nothing"
    end
  end
end
