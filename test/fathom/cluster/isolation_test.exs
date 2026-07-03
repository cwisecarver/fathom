defmodule Fathom.Cluster.IsolationTest do
  # Cluster phase (S6): the RELEASE-BLOCKER cross-shard isolation gate. A query for shard A must
  # NEVER read or write shard B — on any node, before / during / after an ownership handoff.
  # AGENTS.md requires this for any routing change, and the LB-keyspace-partition phase changes
  # routing (shard id from the Host subdomain → one node → that shard's own file). A cross-tenant
  # leak is a release blocker, not a finding. Helpers + setup from Fathom.ClusterShardCase.
  use Fathom.ClusterShardCase

  defp seed(shard, value) do
    serve(shard, fn conn ->
      {:ok, _} = ShardExecutor.execute(conn, stmt("CREATE TABLE kv (v TEXT)"))
      {:ok, _} = ShardExecutor.execute(conn, stmt("INSERT INTO kv VALUES (?)", [value]))
    end)
  end

  test "two shards keep strictly separate data — neither bleeds into the other", %{shard: a} do
    Application.put_env(:fathom, :shard_idle_ms, 50)
    b = unique_shard()

    seed(a, "a-data")
    seed(b, "b-data")

    # Re-open each (cold-pull from its own object) and confirm each serves ONLY its own data.
    assert serve(a, &select_v/1) == ["a-data"]
    assert serve(b, &select_v/1) == ["b-data"]
  end

  test "concurrently open shards are isolated — interleaved writes never cross", %{shard: a} do
    Application.put_env(:fathom, :shard_idle_ms, 50)
    b = unique_shard()

    {:ok, ca} = ShardExecutor.open(a)
    {:ok, cb} = ShardExecutor.open(b)

    {:ok, _} = ShardExecutor.execute(ca, stmt("CREATE TABLE kv (v TEXT)"))
    {:ok, _} = ShardExecutor.execute(cb, stmt("CREATE TABLE kv (v TEXT)"))
    {:ok, _} = ShardExecutor.execute(ca, stmt("INSERT INTO kv VALUES ('A1')"))
    {:ok, _} = ShardExecutor.execute(cb, stmt("INSERT INTO kv VALUES ('B1')"))
    {:ok, _} = ShardExecutor.execute(ca, stmt("INSERT INTO kv VALUES ('A2')"))

    assert select_v(ca) == ["A1", "A2"], "shard A sees only its own writes"
    assert select_v(cb) == ["B1"], "shard B sees only its own writes"

    close_and_stop(a, ca)
    close_and_stop(b, cb)
  end

  test "a handoff on one shard does not touch another shard's data", %{shard: a} do
    Application.put_env(:fathom, :shard_idle_ms, 50)
    b = unique_shard()

    seed(a, "a-data")
    seed(b, "b-data")

    # Hand shard A off to a "new node": a crashed prior owner left an expired lock, so the next
    # checkout of A steals it and cold-opens A's object. B is entirely uninvolved.
    # Expired past the steal margin so the no-heartbeat owner is genuinely dead (finding #11).
    put_raw_lock(a, "dead@node", 5, now_ms() - 60_000)

    assert serve(a, &select_v/1) == ["a-data"], "A's new owner serves A's data"
    assert serve(b, &select_v/1) == ["b-data"], "B is untouched by A's handoff"
  end

  test "an invalid shard id is refused (it can never resolve to a real shard's data)", %{
    shard: a
  } do
    Application.put_env(:fathom, :shard_idle_ms, 50)
    seed(a, "a-data")

    # A malformed/path-traversal id must not open (and so can never reach another shard's file).
    assert {:error, _} = Shards.checkout("../#{a}")
    assert {:error, _} = Shards.checkout("a/b")
    # The real shard is unaffected.
    assert serve(a, &select_v/1) == ["a-data"]
  end
end
