defmodule Fathom.Shard.ReplicationFollowerNamesTest do
  @moduledoc """
  Expert review 2026-10-10 #P4. `Follower.table/1` and `locks/1` are called on the per-frame path;
  the default name now resolves at compile time instead of `Module.concat/2` per call. The names
  must be exactly what the concat produced, or every ETS lookup misses.
  """
  use ExUnit.Case, async: true

  alias Fathom.Shard.Replication.Follower

  test "table/1 and locks/1 keep their derived names, default and custom" do
    assert Follower.table() == Fathom.Shard.Replication.Follower.Shards
    assert Follower.locks() == Fathom.Shard.Replication.Follower.ShardLocks
    assert Follower.table(Follower) == Follower.table()
    assert Follower.table(:custom_name) == Module.concat(:custom_name, Shards)
    assert Follower.locks(:custom_name) == Module.concat(:custom_name, ShardLocks)
  end
end
