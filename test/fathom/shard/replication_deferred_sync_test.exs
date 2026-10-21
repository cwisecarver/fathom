defmodule Fathom.Shard.ReplicationDeferredSyncTest do
  @moduledoc """
  Expert review 2026-10-21 #7: the deferred WAL sync (`Fathom.Shard.Replication.WalSync`), OFF by
  default.

  With the flag on and replication on, a tenant `:rw` handle runs `synchronous=NORMAL`, and the A2
  Session issues one `fdatasync` of the `-wal` per commit round. The invariant every test here pins:

  > **No tenant write is acked before an fdatasync that started after its COMMIT has succeeded.**

  A failing sync must fail the commit, and a failed fdatasync poisons the Session, because a later
  successful sync cannot vouch for pages the kernel already dropped (`WalSync` moduledoc). The
  sync is driven through the `:replication_wal_sync_fun` seam, so the tests can hold it, fail it,
  or count it deterministically.

  What these tests CANNOT show is power-loss durability itself: a BEAM or process kill loses nothing
  under NORMAL either, because the frames are already in the kernel page cache. That needs the rig
  crash test named in the commit that added this, which drops the page cache of unsynced data.
  """
  use ExUnit.Case, async: false

  alias Fathom.{Shard, ShardExecutor, Shards}
  alias Fathom.Shard.Connection
  alias Fathom.Shard.Replication.{Fleet, Follower, Session, WalSync}
  alias Filo.Stmt

  @keys [
    :replication_enabled,
    :replication_followers,
    :replication_quorum,
    :replication_deferred_wal_sync,
    :replication_wal_sync_fun,
    :replication_timeout_ms
  ]

  setup do
    id = "dsync_#{System.unique_integer([:positive])}"
    root = Path.join(System.tmp_dir!(), "dsync_#{System.unique_integer([:positive])}")
    File.mkdir_p!(root)
    prev = for k <- @keys, do: {k, Application.get_env(:fathom, k)}

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

    %{id: id, root: root}
  end

  # Three loopback followers, quorum two, replication on. Returns the follower names.
  defp replicate!(root) do
    followers =
      for i <- 1..3 do
        name = :"dsync_f#{i}_#{System.unique_integer([:positive])}"

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
    for {name, _} <- followers, do: name
  end

  # The seam. `:ok`/`{:error, _}` answers at once; `:hold` reports in and waits for `{:release, r}`.
  # Every call reports `{:wal_sync_called, pid, path}` to the test, so a test can also count them.
  defp seam!(mode) do
    test = self()

    Application.put_env(:fathom, :replication_wal_sync_fun, fn path ->
      send(test, {:wal_sync_called, self(), path})

      case mode do
        :hold ->
          receive do
            {:release, result} -> result
          end

        :real ->
          Application.delete_env(:fathom, :replication_wal_sync_fun)
          result = WalSync.sync(path)
          send(test, {:wal_sync_result, result})
          result

        result ->
          result
      end
    end)
  end

  defp sync_mode(conn) do
    {:ok, %{rows: [[n]]}} = Connection.query(conn, "PRAGMA synchronous", [])
    n
  end

  # A real coordinator, a deferred-mode tenant handle on it, a table, and every follower seeded.
  defp primary!(id, followers) do
    {:ok, coordinator, ref, path} = Shards.checkout(id)
    on_exit(fn -> Shard.checkin(coordinator, ref) end)

    {:ok, conn} = Connection.open(path, tenant?: true, deferred_sync?: true)
    on_exit(fn -> Connection.close(conn) end)
    assert sync_mode(conn) == 1, "fixture: the tenant handle is not synchronous=NORMAL"

    {:ok, _} = Connection.query(conn, "CREATE TABLE t (k INTEGER PRIMARY KEY, v TEXT)", [])
    for name <- followers, do: Follower.seed(name, id, 0, 0, 0, 0)
    {coordinator, conn, path <> "-wal"}
  end

  defp insert!(conn, k),
    do: {:ok, _} = Connection.query(conn, "INSERT INTO t VALUES (?1, 'v')", [k])

  describe "the handle's sync mode" do
    test "NORMAL only on a deferred tenant :rw handle; FULL everywhere else", %{id: id} do
      path = Shard.db_path(id)
      File.mkdir_p!(Path.dirname(path))

      open = fn opts ->
        {:ok, c} = Connection.open(path, opts)
        n = sync_mode(c)
        Connection.close(c)
        n
      end

      assert open.(tenant?: true, deferred_sync?: true) == 1
      assert open.(tenant?: true) == 2

      # Fathom's own connections (snapshot, checkpoint, migration) never relax, whatever is passed.
      assert open.(deferred_sync?: true) == 2
    end

    test "a pooled handle takes the current mode on reuse, or is refused by its own guard",
         %{id: id} do
      assert Fathom.Shard.Extension.available?(), "the extension is not built"
      path = Shard.db_path(id)
      File.mkdir_p!(Path.dirname(path))

      {:ok, deferred} = Connection.open(path, tenant?: true, deferred_sync?: true)
      on_exit(fn -> Connection.close(deferred) end)
      assert :ok = Connection.reset_for_reuse(deferred, :rw, deferred_sync?: true)
      assert sync_mode(deferred) == 1

      # The guard pinned NORMAL at open, so a FULL reuse is refused (the caller then discards the
      # handle and opens a fresh FULL one), and vice versa: a FULL handle never becomes NORMAL.
      assert {:error, _} = Connection.reset_for_reuse(deferred, :rw)

      {:ok, full} = Connection.open(path, tenant?: true)
      on_exit(fn -> Connection.close(full) end)
      assert {:error, _} = Connection.reset_for_reuse(full, :rw, deferred_sync?: true)
      assert sync_mode(full) == 2
    end

    test "a tenant cannot relax a FULL handle to NORMAL through the engine guard", %{id: id} do
      assert Fathom.Shard.Extension.available?(), "the extension is not built"
      path = Shard.db_path(id)
      File.mkdir_p!(Path.dirname(path))
      {:ok, c} = Connection.open(path, tenant?: true)
      on_exit(fn -> Connection.close(c) end)

      assert {:error, _} = Connection.query(c, "PRAGMA synchronous=NORMAL", [])
      assert sync_mode(c) == 2
    end

    test "deferred?/0 needs both the flag and replication" do
      for {flag, repl, want} <- [
            {false, false, false},
            {true, false, false},
            {false, true, false},
            {true, true, true}
          ] do
        Application.put_env(:fathom, :replication_deferred_wal_sync, flag)
        Application.put_env(:fathom, :replication_enabled, repl)
        assert WalSync.deferred?() == want, "flag=#{flag} replication=#{repl}"
      end
    end
  end

  describe "Session ack ordering" do
    test "the commit is not acked until the round's fdatasync completes", ctx do
      followers = replicate!(ctx.root)
      {coordinator, conn, wal} = primary!(ctx.id, followers)
      seam!(:hold)

      insert!(conn, 1)
      task = Task.async(fn -> Session.commit(ctx.id, wal, coordinator, sync: true) end)

      assert_receive {:wal_sync_called, syncer, ^wal}, 2_000

      # The quorum round has every chance to finish meanwhile; the ack must still wait.
      assert Task.yield(task, 300) == nil, "the commit was acked before its fdatasync finished"

      send(syncer, {:release, :ok})
      assert Task.await(task) == :ok
    end

    test "an fdatasync error fails the round and poisons every later ack", ctx do
      followers = replicate!(ctx.root)
      {coordinator, conn, wal} = primary!(ctx.id, followers)
      seam!({:error, {:datasync, :eio}})

      insert!(conn, 1)

      assert {:error, {:wal_sync_failed, {:datasync, :eio}}} =
               Session.commit(ctx.id, wal, coordinator, sync: true)

      # A later sync that SUCCEEDS cannot vouch for the pages the failed one may have dropped.
      seam!(:ok)
      insert!(conn, 2)

      assert {:error, {:wal_sync_failed, {:poisoned, {:datasync, :eio}}}} =
               Session.commit(ctx.id, wal, coordinator, sync: true)

      # Including a FULL caller on the same Session: its frames sit after the suspect ones.
      assert {:error, {:wal_sync_failed, {:poisoned, _}}} =
               Session.commit(ctx.id, wal, coordinator)
    end

    test "an open error fails only its own round (no sync ran, nothing was dropped)", ctx do
      followers = replicate!(ctx.root)
      {coordinator, conn, wal} = primary!(ctx.id, followers)
      seam!({:error, {:open, :emfile}})

      insert!(conn, 1)

      assert {:error, {:wal_sync_failed, {:open, :emfile}}} =
               Session.commit(ctx.id, wal, coordinator, sync: true)

      seam!(:ok)
      insert!(conn, 2)
      assert :ok = Session.commit(ctx.id, wal, coordinator, sync: true)
    end

    test "a sync still running at the deadline fails the round; its late failure poisons", ctx do
      followers = replicate!(ctx.root)
      {coordinator, conn, wal} = primary!(ctx.id, followers)
      Application.put_env(:fathom, :replication_timeout_ms, 800)
      seam!(:hold)

      insert!(conn, 1)

      assert {:error, {:wal_sync_failed, :pending}} =
               Session.commit(ctx.id, wal, coordinator, sync: true)

      assert_receive {:wal_sync_called, syncer, ^wal}
      send(syncer, {:release, {:error, {:datasync, :eio}}})

      # The next round must not start another sync over an unresolved one: the late result lands
      # first (in `handle_info/2` or at the round's start, whichever wins) and fails the round.
      seam!(:ok)
      insert!(conn, 2)

      assert {:error, {:wal_sync_failed, _}} =
               Session.commit(ctx.id, wal, coordinator, sync: true)

      # ...and it poisoned the Session, though the sync now succeeds.
      insert!(conn, 3)

      assert {:error, {:wal_sync_failed, {:poisoned, {:datasync, :eio}}}} =
               Session.commit(ctx.id, wal, coordinator, sync: true)

      refute_received {:wal_sync_called, _, _}
    end

    test "commits queued behind a held sync are not acked when it fails", ctx do
      followers = replicate!(ctx.root)
      {coordinator, conn, wal} = primary!(ctx.id, followers)
      seam!(:hold)

      insert!(conn, 1)
      first = Task.async(fn -> Session.commit(ctx.id, wal, coordinator, sync: true) end)
      assert_receive {:wal_sync_called, syncer, ^wal}, 2_000

      queued =
        for k <- 2..4 do
          insert!(conn, k)
          Task.async(fn -> Session.commit(ctx.id, wal, coordinator, sync: true) end)
        end

      send(syncer, {:release, {:error, {:datasync, :eio}}})

      for t <- [first | queued] do
        assert {:error, {:wal_sync_failed, _}} = Task.await(t),
               "a commit was acked although the fdatasync covering it failed"
      end
    end

    test "flag-off rounds and the catch-up cycle never sync", ctx do
      followers = replicate!(ctx.root)
      {coordinator, conn, wal} = primary!(ctx.id, followers)
      seam!(:ok)

      insert!(conn, 1)
      assert :ok = Session.commit(ctx.id, wal, coordinator)

      [{session, _}] = Registry.lookup(Fathom.Shard.Replication.SessionRegistry, ctx.id)
      insert!(conn, 2)
      _ = GenServer.call(session, {:commit, wal, :no_checkpoint})

      refute_received {:wal_sync_called, _, _}
    end

    test "the real fdatasync succeeds across a WAL restart between COMMIT and the sync", ctx do
      followers = replicate!(ctx.root)
      {coordinator, conn, wal} = primary!(ctx.id, followers)
      assert :ok = Session.commit(ctx.id, wal, coordinator, sync: true)

      insert!(conn, 1)
      # A checkpoint lands before the Session's sync, on a fathom-side (FULL) connection as the
      # coordinator's would: everything is backfilled into the .db and synced there, and the WAL is
      # truncated, so the next writer restarts it.
      {:ok, ckpt} = Connection.open(String.replace_suffix(wal, "-wal", ""))
      {:ok, %{rows: [[0, _, _]]}} = Connection.query(ckpt, "PRAGMA wal_checkpoint(TRUNCATE)", [])
      Connection.close(ckpt)
      assert File.stat!(wal).size == 0, "fixture: the WAL was not truncated"
      insert!(conn, 2)

      seam!(:real)
      assert :ok = Session.commit(ctx.id, wal, coordinator, sync: true)
      assert_received {:wal_sync_result, :ok}

      {:ok, %{rows: [[n]]}} = Connection.query(conn, "SELECT count(*) FROM t", [])
      assert n == 2
    end
  end

  describe "through ShardExecutor" do
    defp run(h, sql), do: ShardExecutor.execute(h, %Stmt{sql: sql, args: []})

    defp stream!(id, followers) do
      {:ok, h} = ShardExecutor.open(id)
      on_exit(fn -> ShardExecutor.close(h) end)
      {:ok, _} = Shards.ensure(id)
      for name <- followers, do: Follower.seed(name, id, 0, 0, 0, 0)
      h
    end

    test "flag on + replication on: NORMAL handle, and a failed sync is never acked", ctx do
      followers = replicate!(ctx.root)
      Application.put_env(:fathom, :replication_deferred_wal_sync, true)
      h = stream!(ctx.id, followers)
      assert sync_mode(elem(h, 2)) == 1

      seam!(:ok)
      {:ok, _} = run(h, "CREATE TABLE t (k INTEGER PRIMARY KEY)")
      assert_received {:wal_sync_called, _, _}
      assert {:ok, _} = run(h, "INSERT INTO t VALUES (1)")

      seam!({:error, {:datasync, :eio}})

      assert {:error, %Filo.Error{code: "FILO_WAL_SYNC_FAILED"}} =
               run(h, "INSERT INTO t VALUES (2)")

      # Poisoned: a later write fails too even though the sync would now succeed.
      seam!(:ok)

      assert {:error, %Filo.Error{code: "FILO_WAL_SYNC_FAILED"}} =
               run(h, "INSERT INTO t VALUES (3)")
    end

    test "flag on + replication off: the handle stays FULL and nothing defers", %{id: id} do
      Application.put_env(:fathom, :replication_deferred_wal_sync, true)
      Application.put_env(:fathom, :replication_enabled, false)
      seam!(:ok)

      {:ok, h} = ShardExecutor.open(id)
      on_exit(fn -> ShardExecutor.close(h) end)
      assert sync_mode(elem(h, 2)) == 2

      {:ok, _} = run(h, "CREATE TABLE t (k INTEGER PRIMARY KEY)")
      assert {:ok, _} = run(h, "INSERT INTO t VALUES (1)")
      refute_received {:wal_sync_called, _, _}
    end

    test "replication switched off under a live deferred handle still syncs before the ack",
         ctx do
      followers = replicate!(ctx.root)
      Application.put_env(:fathom, :replication_deferred_wal_sync, true)
      h = stream!(ctx.id, followers)
      assert sync_mode(elem(h, 2)) == 1
      {:ok, _} = run(h, "CREATE TABLE t (k INTEGER PRIMARY KEY)")

      Application.put_env(:fathom, :replication_enabled, false)
      seam!({:error, {:datasync, :eio}})

      assert {:error, %Filo.Error{code: "FILO_WAL_SYNC_FAILED"}} =
               run(h, "INSERT INTO t VALUES (1)")

      assert_received {:wal_sync_called, _, _}
    end
  end
end
