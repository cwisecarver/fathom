defmodule Fathom.ShardExecutorFailedWriteShipTest do
  @moduledoc """
  Expert review 2026-11-07 #5, the A2 half. A failed script (or a failed capped `RETURNING` DML) may
  have COMMITTED, and `QuorumFence.mark` alone only makes the NEXT statement run a quorum round: if
  no next statement arrives the frames never ship, so a follower quorum is behind the primary's
  locally visible state until the next write. The error branches therefore ship (`maybe_replicate`)
  themselves; these scenarios assert a follower quorum holds the primary's WAL byte for byte right
  after the failed request, with no later statement.

  Same harness as `ShardExecutorShipTest`.
  """
  use ExUnit.Case, async: false

  alias Fathom.{Shard, ShardExecutor, Shards}
  alias Fathom.Shard.Replication.{Fleet, Follower, Session}
  alias Fathom.Test.Await
  alias Filo.Stmt

  setup do
    id = "failship_#{System.unique_integer([:positive])}"
    root = Path.join(System.tmp_dir!(), "failship_#{System.unique_integer([:positive])}")
    File.mkdir_p!(root)

    keys = [:replication_enabled, :replication_followers, :replication_quorum, :query_max_rows]
    prev = for k <- keys, do: {k, Application.get_env(:fathom, k)}

    on_exit(fn ->
      Session.stop(id)

      for {k, v} <- prev do
        if is_nil(v),
          do: Application.delete_env(:fathom, k),
          else: Application.put_env(:fathom, k, v)
      end

      File.rm_rf(root)
      for s <- ["", "-wal", "-shm"], do: File.rm(Shard.db_path(id) <> s)
    end)

    followers =
      for i <- 1..3 do
        name = :"failship_f#{i}_#{System.unique_integer([:positive])}"

        pid =
          start_supervised!({Follower, name: name, port: 0, dir: Path.join(root, "f#{i}")},
            id: name
          )

        {:ok, port} = Follower.port(pid)
        {name, port}
      end

    Application.put_env(:fathom, :replication_enabled, true)
    Application.put_env(:fathom, :replication_quorum, 2)

    Application.put_env(
      :fathom,
      :replication_followers,
      for({_n, port} <- followers, do: {~c"127.0.0.1", port})
    )

    start_supervised!(Fleet)
    %{id: id, followers: followers}
  end

  defp run(conn, sql), do: ShardExecutor.execute(conn, %Stmt{sql: sql, args: []})

  defp quorum_holds?(followers, id) do
    primary = File.read!(Shard.db_path(id) <> "-wal")
    assert byte_size(primary) > 0, "the primary wrote no WAL -- this test measured nothing"

    Enum.count(followers, fn {n, _} -> File.read(Follower.wal_path(n, id)) == {:ok, primary} end) >=
      2
  end

  defp open!(id, followers) do
    {:ok, conn} = ShardExecutor.open(id)
    {:ok, _coordinator} = Shards.ensure(id)
    for {name, _} <- followers, do: Follower.seed(name, id, 0, 0, 0, 0)
    {:ok, _} = run(conn, "CREATE TABLE t (k INTEGER PRIMARY KEY, v TEXT)")
    on_exit(fn -> ShardExecutor.close(conn) end)

    # Precondition: the table's frames are on the quorum, so only a LATER write can make it differ.
    Await.until(fn -> quorum_holds?(followers, id) end, "fixture: CREATE TABLE never shipped")
    conn
  end

  test "a failed script ships the statements that committed before its error", %{
    id: id,
    followers: f
  } do
    conn = open!(id, f)

    assert {:error, %Filo.Error{}} =
             ShardExecutor.execute_sequence(
               conn,
               "INSERT INTO t VALUES (1, 'a'); INSERT INTO t VALUES (1, 'dup');"
             )

    assert {:ok, %{rows: [[1]]}} = run(conn, "SELECT count(*) FROM t"),
           "fixture: the first statement did not commit"

    # No statement follows, so nothing but the error branch itself could have shipped the INSERT.
    Await.until(
      fn -> quorum_holds?(f, id) end,
      "the failed script's commit never reached a quorum"
    )
  end

  test "a capped INSERT … RETURNING ships the rows SQLite committed", %{id: id, followers: f} do
    conn = open!(id, f)
    Application.put_env(:fathom, :query_max_rows, 2)

    assert {:error, %Filo.Error{code: "FILO_RESULT_TOO_LARGE"}} =
             run(
               conn,
               "INSERT INTO t(v) VALUES ('a'), ('b'), ('c'), ('d') RETURNING k"
             )

    assert {:ok, %{rows: [[4]]}} = run(conn, "SELECT count(*) FROM t"),
           "fixture: the capped statement did not commit"

    Await.until(fn -> quorum_holds?(f, id) end, "the capped DML's commit never reached a quorum")
  end
end
