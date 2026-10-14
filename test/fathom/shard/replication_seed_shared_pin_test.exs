defmodule Fathom.Shard.ReplicationSeedSharedPinTest do
  @moduledoc """
  A reopened shard re-seeds EVERY follower at once, and the seed tasks share ONE pinned snapshot
  (perf review 2026-10-14 #12(a)).

  Before: `start_seeds/3` ran a task per follower and each opened its own `:ro` `Connection` and
  pinned its own snapshot of the same `.db` (~361 us per open) — N followers, N identical pins.
  After: the batch shares one `SeedPin`; the pin is still held before any task reads the WAL and
  until the last stream ends, so the consistency argument above `Session.do_seed/6` is unchanged.

  The count is deterministic: every `Connection.open(<the shard's .db>, scope: :ro)` is traced, which
  counts opens identically before and after the change (on the unpatched code it is one per
  follower).
  """
  use ExUnit.Case, async: false

  alias Fathom.Shard.Connection
  alias Fathom.Shard.Replication.Fleet
  alias Fathom.Shard.Replication.Follower
  alias Fathom.Shard.Replication.SeedPin
  alias Fathom.Shard.Replication.Session
  alias Fathom.Shard.Replication.Shipper
  alias Fathom.Shards

  @gates [:replication_enabled, :replication_followers, :replication_quorum]
  @followers 4

  setup do
    id = "repl_pin_#{System.unique_integer([:positive])}"
    root = Path.join(System.tmp_dir!(), "replpin_#{System.unique_integer([:positive])}")
    File.mkdir_p!(root)
    prev = Map.new(@gates, &{&1, Application.get_env(:fathom, &1)})

    on_exit(fn ->
      Session.stop(id)

      for {k, v} <- prev do
        if is_nil(v),
          do: Application.delete_env(:fathom, k),
          else: Application.put_env(:fathom, k, v)
      end

      Shards.drain(id, 5_000)
      for e <- ["", "-wal", "-shm", ".etag"], do: File.rm(Fathom.Shard.db_path(id) <> e)
      File.rm_rf(root)
    end)

    %{id: id, root: root}
  end

  defp start_followers!(root) do
    for i <- 1..@followers do
      name = :"pin_f_#{System.unique_integer([:positive])}"
      dir = Path.join(root, "f#{i}")
      pid = start_supervised!({Follower, name: name, port: 0, dir: dir}, id: name)
      {:ok, port} = Follower.port(pid)
      {name, port}
    end
  end

  defp await!(fun, msg) do
    deadline = System.monotonic_time(:millisecond) + 10_000

    Stream.repeatedly(fn -> fun.() || Process.sleep(20) end)
    |> Enum.find(fn
      v when v in [nil, false, :ok] -> System.monotonic_time(:millisecond) > deadline
      _ -> true
    end)
    |> case do
      v when v in [nil, false, :ok] -> flunk(msg)
      v -> v
    end
  end

  test "an N-follower seed batch opens ONE snapshot, and every follower still gets seeded",
       %{id: id, root: root} do
    followers = start_followers!(root)
    Application.put_env(:fathom, :replication_enabled, true)
    Application.put_env(:fathom, :replication_quorum, 1)

    Application.put_env(
      :fathom,
      :replication_followers,
      for({_n, port} <- followers, do: {~c"127.0.0.1", port})
    )

    start_supervised!(Fleet)

    await!(
      fn -> Enum.all?(Fleet.shippers(), &Shipper.connected?/1) end,
      "shippers never connected"
    )

    {:ok, coordinator, ref, path} = Shards.checkout(id)
    {:ok, conn} = Connection.open(path)
    {:ok, _} = Connection.query(conn, "CREATE TABLE t (a BLOB)", [])
    {:ok, _} = Connection.query(conn, "INSERT INTO t VALUES (randomblob(20000))", [])

    # Count every :ro open of THIS shard's .db from here on (all processes: the seed tasks, or the
    # SeedPin process, are not known in advance).
    :erlang.trace_pattern({Connection, :open, 2}, true, [:local])
    :erlang.trace(:all, true, [:call, {:tracer, self()}])

    try do
      assert :ok = Session.commit(id, path <> "-wal", coordinator)

      await!(
        fn ->
          Enum.all?(followers, fn {name, _} ->
            match?(%{next_offset: o} when o > 0, Follower.state_of(name, id))
          end)
        end,
        "not every follower was seeded"
      )
    after
      :erlang.trace(:all, false, [:call])
      :erlang.trace_pattern({Connection, :open, 2}, false, [:local])
    end

    ro_opens = ro_opens_of(path)

    assert ro_opens == 1,
           "#{ro_opens} snapshot opens for #{@followers} followers — each seed task pinned its own"

    for {name, _} <- followers do
      refute Follower.state_of(name, id).torn
    end

    Session.stop(id)
    Connection.close(conn)
    Fathom.Shard.checkin(coordinator, ref)
  end

  defp ro_opens_of(db) do
    receive_all([])
    |> Enum.count(fn
      {:trace, _pid, :call, {Connection, :open, [^db, opts]}} -> opts[:scope] == :ro
      _ -> false
    end)
  end

  defp receive_all(acc) do
    receive do
      {:trace, _, _, _} = msg -> receive_all([msg | acc])
    after
      100 -> acc
    end
  end

  describe "SeedPin lifetime" do
    setup do
      db = Path.join(System.tmp_dir!(), "pin_#{System.unique_integer([:positive])}.db")
      {:ok, c} = Connection.open(db)
      :ok = Connection.exec(c, "CREATE TABLE t (a INTEGER)")
      Connection.close(c)
      on_exit(fn -> for s <- ["", "-wal", "-shm"], do: File.rm(db <> s) end)
      %{db: db}
    end

    test "a missing .db fails the attach instead of materializing an empty one" do
      db = Path.join(System.tmp_dir!(), "pin_missing_#{System.unique_integer([:positive])}.db")
      {:ok, pin} = SeedPin.start(self(), db)
      assert {:error, :enoent} = SeedPin.attach(pin)
      refute File.exists?(db), "a seed of a shard with no .db would ship an empty database"
    end

    test "the pin is held until the LAST attached task detaches, then released", %{db: db} do
      {:ok, pin} = SeedPin.start(self(), db)
      parent = self()

      tasks =
        for _ <- 1..2 do
          spawn(fn ->
            :ok = SeedPin.attach(pin)
            send(parent, {:attached, self()})

            receive do
              :go -> SeedPin.detach(pin)
            end

            send(parent, {:detached, self()})
          end)
        end

      SeedPin.expect(pin, tasks)
      for t <- tasks, do: assert_receive({:attached, ^t})

      # While any task holds the shared snapshot a TRUNCATE checkpoint cannot finish: the WAL
      # frames committed after the reader's mark stay in the WAL.
      {:ok, w} = Connection.open(db)
      :ok = Connection.exec(w, "PRAGMA wal_autocheckpoint=0")
      :ok = Connection.exec(w, "INSERT INTO t VALUES (1)")
      [t1, t2] = tasks
      send(t1, :go)
      assert_receive {:detached, ^t1}

      assert {:ok, %{rows: [[_busy, log, ckpt]]}} =
               Connection.query(w, "PRAGMA wal_checkpoint(TRUNCATE)", [])

      assert ckpt < log, "the snapshot was released while a task still held it"

      ref = Process.monitor(pin)
      send(t2, :go)
      assert_receive {:detached, ^t2}
      # All expected tasks finished: the pin process ends and the snapshot is released.
      assert_receive {:DOWN, ^ref, :process, _, :normal}

      assert {:ok, %{rows: [[0, log2, log2]]}} =
               Connection.query(w, "PRAGMA wal_checkpoint(TRUNCATE)", [])

      assert log2 >= 0
      Connection.close(w)
    end

    test "a killed task releases its claim without running any after block", %{db: db} do
      {:ok, pin} = SeedPin.start(self(), db)
      parent = self()

      task =
        spawn(fn ->
          :ok = SeedPin.attach(pin)
          send(parent, :attached)
          Process.sleep(:infinity)
        end)

      SeedPin.expect(pin, [task])
      assert_receive :attached
      ref = Process.monitor(pin)
      Process.exit(task, :kill)
      assert_receive {:DOWN, ^ref, :process, _, :normal}
    end

    test "the pin dies with its Session", %{db: db} do
      session = spawn(fn -> Process.sleep(:infinity) end)
      {:ok, pin} = SeedPin.start(session, db)
      ref = Process.monitor(pin)
      Process.exit(session, :kill)
      assert_receive {:DOWN, ^ref, :process, _, :normal}
    end
  end
end
