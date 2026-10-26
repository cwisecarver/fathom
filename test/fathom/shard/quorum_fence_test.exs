defmodule Fathom.Shard.QuorumFenceTest do
  @moduledoc """
  Jepsen finding 7 (2026-10-25, `docs/reviews/jepsen-2026-10-24.md`): a read could see a write that
  failed its follower quorum.

  SYMPTOM: `[:append 214 6]` got `FILO_NO_QUORUM` (503), but SQLite had committed it locally, so a
  read on the same node 7.5 s later returned `[6]`. After a takeover by a node that never received
  the frames the key restarted at `[24]`: a value a client had seen was gone.

  INVARIANT pinned here: after a commit fails its quorum, the node serves nothing from that shard,
  reads included, until a later round has put the frames on a quorum. Pre-fix the read below
  returns the unconfirmed row.
  """
  use ExUnit.Case, async: false

  alias Fathom.Shard.Replication.{Fleet, Follower, QuorumFence, Session, Shipper}
  alias Fathom.ShardExecutor
  alias Fathom.Test.PausablePeer
  alias Filo.{Error, Stmt, StmtResult}

  @keys [
    :replication_enabled,
    :replication_followers,
    :replication_quorum,
    :replication_timeout_ms
  ]

  setup do
    id = "repl_qfence_#{System.unique_integer([:positive])}"
    root = Path.join(System.tmp_dir!(), "replqfence_#{System.unique_integer([:positive])}")
    File.mkdir_p!(root)
    prev = Map.new(@keys, &{&1, Application.get_env(:fathom, &1)})

    on_exit(fn ->
      Session.stop(id)

      Enum.each(prev, fn
        {k, nil} -> Application.delete_env(:fathom, k)
        {k, v} -> Application.put_env(:fathom, k, v)
      end)

      File.rm_rf(root)
      for s <- ["", "-wal", "-shm"], do: File.rm(Fathom.Shard.db_path(id) <> s)
    end)

    %{id: id, root: root}
  end

  defp stmt(sql, args \\ []), do: %Stmt{sql: sql, args: args}

  defp await!(fun, what, timeout \\ 10_000) do
    deadline = System.monotonic_time(:millisecond) + timeout

    Stream.repeatedly(fn ->
      cond do
        fun.() -> true
        System.monotonic_time(:millisecond) > deadline -> false
        true -> Process.sleep(20) && nil
      end
    end)
    |> Enum.find(&is_boolean/1)
    |> assert("never: #{what}")
  end

  test "a read after an under-quorum write is refused until the write reaches a quorum",
       %{id: id, root: root} do
    followers =
      for i <- 1..3 do
        name = :"qfence_f#{i}_#{System.unique_integer([:positive])}"

        pid =
          start_supervised!(
            {Follower, name: name, port: 0, dir: Path.join(root, to_string(name))},
            id: name
          )

        {:ok, port} = Follower.port(pid)
        {name, port}
      end

    [{_, pa}, {_, pb}, {_, pc}] = followers
    # Two of three behind peers whose replies can be held: with both held, Q=2 cannot form.
    peer_b = start_supervised!({PausablePeer, upstream_port: pb}, id: :qfence_peer_b)
    peer_c = start_supervised!({PausablePeer, upstream_port: pc}, id: :qfence_peer_c)

    Application.put_env(:fathom, :replication_enabled, true)
    Application.put_env(:fathom, :replication_quorum, 2)
    Application.put_env(:fathom, :replication_timeout_ms, 2_000)

    Application.put_env(
      :fathom,
      :replication_followers,
      for(p <- [pa, PausablePeer.port(peer_b), PausablePeer.port(peer_c)], do: {~c"127.0.0.1", p})
    )

    start_supervised!(Fleet)
    await!(fn -> Enum.all?(Fleet.shippers(), &Shipper.connected?/1) end, "shippers connect")

    {:ok, h} = ShardExecutor.open(id)
    on_exit(fn -> ShardExecutor.close(h) end)
    coordinator = elem(h, 0)
    {:ok, epoch} = Fathom.Shard.epoch(coordinator)
    lineage = Fathom.Shard.lineage(coordinator)
    for {n, _} <- followers, do: Follower.seed(n, id, epoch, 0, 0, 0, lineage)

    assert {:ok, _} = ShardExecutor.execute(h, stmt("CREATE TABLE kv (v INTEGER)"))
    assert {:ok, _} = ShardExecutor.execute(h, stmt("INSERT INTO kv VALUES (1)"))
    assert QuorumFence.marked(id) == nil

    # A second client is mid-transaction when the failure lands.
    {:ok, h2} = ShardExecutor.open(id)
    on_exit(fn -> ShardExecutor.close(h2) end)
    assert {:ok, _} = ShardExecutor.execute(h2, stmt("BEGIN"))
    assert {:ok, _} = ShardExecutor.execute(h2, stmt("SELECT count(*) FROM kv"))

    # Both held: the next write commits locally and misses its quorum.
    Application.put_env(:fathom, :replication_timeout_ms, 300)
    :ok = PausablePeer.pause(peer_b)
    :ok = PausablePeer.pause(peer_c)

    assert {:error, %Error{code: "FILO_NO_QUORUM", status: 503}} =
             ShardExecutor.execute(h, stmt("INSERT INTO kv VALUES (6)"))

    assert {:error, %Error{code: "FILO_NO_QUORUM", status: 503}} =
             ShardExecutor.execute(h, stmt("SELECT v FROM kv ORDER BY v")),
           "a read returned a write that failed its quorum and may vanish on a takeover"

    assert QuorumFence.marked(id) != nil, "the failed commit did not mark the shard"

    # ROLLBACK reads nothing, and refusing it would leave that client stuck in its transaction.
    assert {:ok, _} = ShardExecutor.execute(h2, stmt("ROLLBACK")),
           "the fence refused a ROLLBACK"

    # The quorum comes back: the next statement's round confirms the frames and the fence lifts.
    {:ok, _} = PausablePeer.release(peer_b)
    {:ok, _} = PausablePeer.release(peer_c)

    await!(
      fn ->
        match?(
          {:ok, %StmtResult{rows: [[1], [6]]}},
          ShardExecutor.execute(h, stmt("SELECT v FROM kv ORDER BY v"))
        )
      end,
      "the read serves again once the quorum holds"
    )

    assert QuorumFence.marked(id) == nil
  end

  describe "generations" do
    test "a clear for an older mark leaves a newer failure fenced" do
      id = "qfence_gen_#{System.unique_integer([:positive])}"
      on_exit(fn -> QuorumFence.clear(id, QuorumFence.marked(id) || 0) end)

      :ok = QuorumFence.mark(id)
      old = QuorumFence.marked(id)
      # A second failure lands while a confirming round that read `old` is finishing.
      :ok = QuorumFence.mark(id)
      :ok = QuorumFence.clear(id, old)
      assert QuorumFence.marked(id) == old + 1, "a stale confirm cleared a newer failure"

      :ok = QuorumFence.clear(id, old + 1)
      assert QuorumFence.marked(id) == nil
    end
  end
end
