defmodule Fathom.Shard.Replication.PromoteStageFenceTest do
  @moduledoc """
  Expert review 2026-10-08 #13: `Promote.stage/4` copies a replica's `.db` and `-wal` as ONE pair,
  and the deposed owner can no longer write into the replica once it has been staged.

  `stage/3` copied the two files with no coordination with the follower worker that owns the shard,
  and the deposed owner's lineage stayed accepted until `Follower.forget/2` ran after the publish.
  So a zombie primary's reset push (absorb into the `.db`, truncate the `-wal`) could land between
  the two copies and stage a torn pair, and any push it landed after the copy was ACKED for a write
  the promoted database does not hold.

  The in-flight mutation here is a seed on a real follower worker, because a seed holds the shard
  from `seed_begin` to `seed_end` across frames — the one replica mutation a test can pause at an
  exact point without a hook in the code under test.
  """
  use ExUnit.Case, async: false

  alias Fathom.Shard.Connection
  alias Fathom.Shard.Replication.{Follower, Promote, Protocol}

  setup do
    name = :"stagefence_#{System.unique_integer([:positive])}"
    dir = Path.join(System.tmp_dir!(), to_string(name))
    start_supervised!({Follower, name: name, port: 0, dir: dir}, id: name)
    on_exit(fn -> File.rm_rf(dir) end)

    id = "stagefence_#{System.unique_integer([:positive])}"

    # The replica: a real database holding row 1, its WAL already folded in, at lineage 3.
    File.write!(Follower.db_path(name, id), database_with(dir, [1]))
    File.write!(Follower.wal_path(name, id), "")
    :ok = Follower.seed(name, id, 1, 0, 0, 0, 3)

    %{name: name, dir: dir, id: id, temp: Path.join(dir, "staged.db")}
  end

  defp database_with(dir, rows) do
    path = Path.join(dir, "build_#{System.unique_integer([:positive])}.db")
    {:ok, conn} = Connection.open(path)
    {:ok, _} = Connection.query(conn, "CREATE TABLE t (a INTEGER PRIMARY KEY)", [])
    for r <- rows, do: {:ok, _} = Connection.query(conn, "INSERT INTO t VALUES (?1)", [r])
    :ok = Connection.close(conn)
    bytes = File.read!(path)
    for s <- ["", "-wal", "-shm"], do: File.rm(path <> s)
    bytes
  end

  defp rows_in(path) do
    {:ok, conn} = Connection.open(path)

    try do
      {:ok, %{rows: rows}} = Connection.query(conn, "SELECT a FROM t ORDER BY a", [])
      Enum.map(rows, fn [a] -> a end)
    after
      Connection.close(conn)
    end
  end

  defp connection(name) do
    {:ok, l} = :gen_tcp.listen(0, [:binary, packet: 4, active: false])
    {:ok, port} = :inet.port(l)
    {:ok, client} = :gen_tcp.connect(~c"127.0.0.1", port, [:binary, packet: 4, active: false])
    {:ok, server} = :gen_tcp.accept(l)
    :gen_tcp.close(l)

    worker = Follower.spawn_worker(server, name, nil, self(), :atomics.new(1, []))
    Process.unlink(worker)
    on_exit(fn -> Process.exit(worker, :kill) end)
    %{worker: worker, client: client}
  end

  defp frame(%{worker: worker}, frame) do
    send(worker, {:frame, frame, 7})
    assert_receive {:frame_done, 7}, 5_000
    :ok
  end

  defp reply(%{client: client}) do
    {:ok, bytes} = :gen_tcp.recv(client, 0, 5_000)
    {:ok, decoded} = Protocol.decode(bytes)
    decoded
  end

  defp put_env(key, value) do
    prev = Application.get_env(:fathom, key)
    Application.put_env(:fathom, key, value)

    on_exit(fn ->
      if is_nil(prev),
        do: Application.delete_env(:fathom, key),
        else: Application.put_env(:fathom, key, prev)
    end)
  end

  test "staging waits out an in-flight replica mutation and copies the pair it leaves", ctx do
    %{name: name, dir: dir, id: id, temp: temp} = ctx
    put_env(:replication_promote_lock_wait_ms, 5_000)

    # A connection is mid-seed of a NEWER replica (rows 1 and 2): every byte streamed, not ended.
    seeded = database_with(dir, [1, 2])
    a = connection(name)

    :ok =
      frame(a, %Protocol.SeedBegin{
        shard_id: id,
        epoch: 1,
        wal_gen: 0,
        salt1: 0,
        wal_offset: 0,
        db_size: byte_size(seeded),
        wal_size: 0,
        lineage: 3
      })

    :ok = frame(a, {:seed_chunk, id, :db, 0, seeded})

    stage = Task.async(fn -> Promote.stage(name, id, temp, lineage: 5) end)

    assert Task.yield(stage, 300) == nil,
           "staging copied the replica while another process was in the middle of mutating it"

    :ok = frame(a, {:seed_end, id})
    assert {:ack, ^id, 0} = reply(a)

    assert Task.await(stage, 10_000) == :ok
    assert rows_in(temp) == [1, 2], "the staged copy is not the pair the mutation left behind"
  end

  test "once staged, the deposed owner's pushes and seeds are refused", ctx do
    %{name: name, id: id, temp: temp} = ctx
    assert Promote.stage(name, id, temp, lineage: 5) == :ok

    zombie = connection(name)

    :ok =
      frame(zombie, %Protocol.Push{
        shard_id: id,
        epoch: 1,
        wal_gen: 0,
        salt1: 0,
        offset: 0,
        payload: "a write the promoted database will never hold",
        lineage: 3
      })

    assert {:reject, ^id, :stale_epoch, 0} = reply(zombie),
           "the deposed owner's push was acked after the replica was staged for promotion"

    assert File.read!(Follower.wal_path(name, id)) == "",
           "the deposed owner's push wrote into a replica being promoted"

    # And a fenced replica is never ranked or offered again: its lineage is the new owner's, its
    # bytes the old owner's.
    assert %{torn: true, lineage: 5} = Follower.state_of(name, id)
    assert Follower.offerable(name, id) == nil
  end

  test "a replica that moved since it was ranked is left untouched and not staged", ctx do
    # A GUARD for the new `:expect` check (no pre-fix equivalent): the caller ranked lineage 2, the
    # replica is now lineage 3, so the copy would not be the replica that decision was about.
    %{name: name, id: id, temp: temp} = ctx
    before = Follower.state_of(name, id)

    assert Promote.stage(name, id, temp, lineage: 5, expect: %{before | lineage: 2}) ==
             {:error, :replica_changed}

    assert Follower.state_of(name, id) == before
  end

  test "a promotion that cannot get the replica declines instead of copying under a writer",
       ctx do
    %{name: name, id: id, temp: temp} = ctx
    put_env(:replication_promote_lock_wait_ms, 50)

    holder =
      spawn(fn ->
        {:ok, _} = Follower.lock_shard(name, id, 0)

        receive do
          :release -> :ok
        end
      end)

    on_exit(fn -> send(holder, :release) end)
    wait_until(fn -> :ets.lookup(Follower.locks(name), id) != [] end)

    assert Promote.stage(name, id, temp, lineage: 5) == {:error, :replica_busy}
    refute File.exists?(temp)
    assert %{torn: false, lineage: 3} = Follower.state_of(name, id)
  end

  defp wait_until(fun, tries \\ 200) do
    cond do
      fun.() -> :ok
      tries == 0 -> flunk("condition never held")
      true -> wait_until_after(fun, tries)
    end
  end

  defp wait_until_after(fun, tries) do
    receive do
    after
      5 -> wait_until(fun, tries - 1)
    end
  end
end
