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
end
