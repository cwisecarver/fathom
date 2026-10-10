defmodule Fathom.ShardExecutorShipTest do
  @moduledoc """
  Expert review 2026-10-10 #H2. The executor decided "is this write inside a transaction?" from a
  text-parsed flag updated only by statements that SUCCEED. SQLite ending the transaction itself
  (here: `INSERT OR ROLLBACK` on a conflict) or an outermost SAVEPOINT/RELEASE with no BEGIN left
  the flag stuck on "in a transaction", so every LATER autocommit write skipped the A2 quorum ship
  and was ACKED while no follower held its frames. The decision now reads
  `Connection.autocommit?/1`. Each scenario ends with an autocommit INSERT and asserts a follower
  quorum holds the primary's WAL byte-for-byte.
  """
  use ExUnit.Case, async: false

  alias Fathom.{Shard, ShardExecutor, Shards}
  alias Fathom.Shard.Connection
  alias Fathom.Shard.Replication.{Fleet, Follower, Session}
  alias Filo.Stmt

  setup do
    id = "ship_#{System.unique_integer([:positive])}"
    root = Path.join(System.tmp_dir!(), "shipt_#{System.unique_integer([:positive])}")
    File.mkdir_p!(root)

    keys = [:replication_enabled, :replication_followers, :replication_quorum]
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
        name = :"ship_f#{i}_#{System.unique_integer([:positive])}"

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

  # A commit returns at the Q-th ack, so poll briefly for the quorum's bytes.
  defp await_quorum(followers, id, tries \\ 100) do
    cond do
      quorum_holds?(followers, id) ->
        true

      tries == 0 ->
        false

      true ->
        Process.sleep(20)
        await_quorum(followers, id, tries - 1)
    end
  end

  defp open!(id, followers) do
    {:ok, conn} = ShardExecutor.open(id)
    {:ok, _coordinator} = Shards.ensure(id)
    for {name, _} <- followers, do: Follower.seed(name, id, 0, 0, 0, 0)
    {:ok, _} = run(conn, "CREATE TABLE t (k INTEGER PRIMARY KEY, v TEXT)")
    on_exit(fn -> ShardExecutor.close(conn) end)
    conn
  end

  test "an autocommit write ships after SQLite auto-rolled the transaction back", %{
    id: id,
    followers: f
  } do
    conn = open!(id, f)
    {:ok, _} = run(conn, "INSERT INTO t VALUES (1, 'a')")

    {:ok, _} = run(conn, "BEGIN")
    {:ok, _} = run(conn, "INSERT INTO t VALUES (2, 'b')")
    # ON CONFLICT ROLLBACK: SQLite itself ends the transaction; the statement errors.
    assert {:error, _} = run(conn, "INSERT OR ROLLBACK INTO t VALUES (1, 'dup')")
    assert Connection.autocommit?(elem(conn, 2)), "fixture: SQLite did not roll back"

    {:ok, _} = run(conn, "INSERT INTO t VALUES (3, 'c')")
    assert await_quorum(f, id), "autocommit write after an auto-rollback was acked unshipped"
  end

  test "an autocommit write ships after savepoint-only usage", %{id: id, followers: f} do
    conn = open!(id, f)
    {:ok, _} = run(conn, "SAVEPOINT a")
    {:ok, _} = run(conn, "INSERT INTO t VALUES (1, 'a')")
    {:ok, _} = run(conn, "RELEASE a")
    assert Connection.autocommit?(elem(conn, 2)), "fixture: still in a transaction"

    {:ok, _} = run(conn, "INSERT INTO t VALUES (2, 'b')")
    assert await_quorum(f, id), "autocommit write after SAVEPOINT/RELEASE was acked unshipped"
  end

  # Expert review 2026-10-10 #P1: one Session per replicated shard, mostly idle -- it must sweep
  # fully and hibernate like Shard/Shipper.
  test "a replication Session runs with fullsweep_after 0", %{
    id: id,
    followers: f
  } do
    conn = open!(id, f)
    {:ok, _} = run(conn, "INSERT INTO t VALUES (1, 'a')")
    [{pid, _}] = Registry.lookup(Fathom.Shard.Replication.SessionRegistry, id)

    {:garbage_collection, gc} = Process.info(pid, :garbage_collection)
    assert gc[:fullsweep_after] == 0
  end
end
