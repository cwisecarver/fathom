defmodule Fathom.Shard.ReplicationDepartedFollowerTest do
  @moduledoc """
  Audit 2026-11-22 #16. A Session keys its per-follower position records by shipper name, which
  comes from the endpoint, and nothing deleted from them: after a roster change the departed
  member's last position kept counting (toward Q-coverage, toward the idle-close `laggards`, and as
  "behind" in `lag_info`) for the life of the Session. The reads now consider only the CURRENT
  shipper set.

  The departed record is injected with `:sys.replace_state/2` (the real state shape, a name no longer
  in `Fleet.shippers/0`), because swapping a live follower's endpoint under a running shard is
  the production path and is too slow and racy for a unit test to reach on demand.
  """
  use ExUnit.Case, async: false

  import Fathom.Test.ReplicationCase

  alias Fathom.Shard.Replication.{Fleet, Session}
  alias Fathom.{ShardExecutor, Shards}
  alias Fathom.Test.Await
  alias Filo.Stmt

  @registry Fathom.Shard.Replication.SessionRegistry

  test "a follower no longer in the shipper set is not counted as behind" do
    id = "departed_#{System.unique_integer([:positive])}"
    root = Path.join(System.tmp_dir!(), "departed_#{System.unique_integer([:positive])}")

    on_exit(fn ->
      _ = Shards.stop(id)
      Session.stop(id)
      File.rm_rf(root)
      for s <- ["", "-wal", "-shm"], do: File.rm(Fathom.Shard.db_path(id) <> s)
    end)

    followers = start_followers!(root, 3, "departed")
    enable_replication!(followers, quorum: 2, connected: :all)

    {:ok, h} = ShardExecutor.open(id)
    {:ok, _} = ShardExecutor.execute(h, %Stmt{sql: "CREATE TABLE t (a INTEGER)", args: []})
    {:ok, _} = ShardExecutor.execute(h, %Stmt{sql: "INSERT INTO t VALUES (1)", args: []})

    # Q = 2 of 3 acked, and the third catches up: nobody is behind.
    Await.until(
      fn -> match?(%{behind_followers: 0}, Session.lag_info(id)) end,
      "precondition: a healthy three-follower fleet reported a follower behind"
    )

    [{session, _}] = Registry.lookup(@registry, id)
    refute :departed_shipper in Fleet.shippers()

    :sys.replace_state(session, fn st ->
      stale = %{wal_gen: 0, salt1: 0, offset: 0}
      %{st | followers: Map.put(st.followers, :departed_shipper, stale)}
    end)

    assert %{behind_followers: 0} = Session.lag_info(id),
           "a follower that left the roster is still reported (and gated on) as behind"

    :ok = ShardExecutor.close(h)
  end
end
