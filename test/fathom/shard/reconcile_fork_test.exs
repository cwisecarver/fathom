defmodule Fathom.Shard.ReconcileForkTest do
  @moduledoc """
  A flush 412 with the lock still ours is NOT proof the stored object is ours (expert review
  2026-10-15 #6).

  The 412-reconcile used to adopt the object's current etag whenever `check_lease/2` said the lock
  was ours, on the premise "lock ours ⇒ the object is our own lost-response PUT". That premise is
  false for a warm open whose evidence HEADs were unreachable: `post_lease_warm_check/4` keeps
  serving the local copy fenced by its sidecar etag, a peer's newer lineage sits in the store, the
  flush 412s — and the reconcile re-fenced onto the PEER's etag, so the very next flush PUT the stale
  local bytes `If-Match` it and silently overwrote the peer's acked, durable writes. Nothing was
  quarantined; nothing was left to recover.

  ## The fixture

  The finding's trigger (a storage brownout in which lock PUTs succeed while both evidence HEADs
  fail) is a way of REACHING one state: the coordinator holds the lock while the stored object is a
  lineage it never wrote. These tests construct that state directly — plant a peer's database as
  the stored object under a live coordinator — because the reconcile is what decides the outcome
  from there, whatever got it there. `Shards.flush/1` is the probe: pre-fix it re-kicked after the
  reconcile and returned `:ok` having clobbered the peer.

  The benign case (our own PUT landed, response lost) must still reconcile: that is
  `Fathom.ShardFlushReconcileTest`, which these changes must leave green.

  Both liveness modes: the flush fence and the drop/release paths differ between them.
  """
  use ExUnit.Case, async: false

  alias Fathom.{Shard, ShardExecutor, Shards}
  alias Fathom.Shard.{Connection, Heartbeat, Storage}
  alias Filo.Stmt

  @modes [:legacy, :heartbeat]

  setup do
    shard = "recfork_#{System.unique_integer([:positive])}"
    prev_storage = Application.get_env(:fathom, :shard_storage)
    prev_flush = Application.get_env(:fathom, :shard_flush_interval_ms)

    Application.put_env(:fathom, :shard_storage, Fathom.Test.FaultyStorage)
    Application.put_env(:fathom, :shard_flush_interval_ms, 60_000)

    on_exit(fn ->
      restore(:shard_storage, prev_storage)
      restore(:shard_flush_interval_ms, prev_flush)
      Shards.drain(shard, 2_000)

      for dir <- [Shard.data_dir(), Storage.Local.dir()],
          f <- Path.wildcard(Path.join(dir, shard <> ".*")),
          do: File.rm(f)
    end)

    %{shard: shard}
  end

  defp restore(key, nil), do: Application.delete_env(:fathom, key)
  defp restore(key, value), do: Application.put_env(:fathom, key, value)

  defp stmt(sql), do: %Stmt{sql: sql, args: []}

  defp set_mode!(:legacy), do: refute(Heartbeat.running?())

  defp set_mode!(:heartbeat) do
    hb = start_supervised!({Heartbeat, ttl_ms: 30_000})
    _ = :sys.get_state(hb)
    assert Heartbeat.running?()
  end

  defp assert_mode!(pid, :heartbeat), do: assert(is_integer(:sys.get_state(pid).acquire_gen))
  defp assert_mode!(pid, :legacy), do: assert(is_nil(:sys.get_state(pid).acquire_gen))

  defp rows_of(path) do
    {:ok, conn} = Connection.open(path)
    {:ok, %{rows: rows}} = Connection.query(conn, "SELECT v FROM kv ORDER BY v", [])
    :ok = Connection.close(conn)
    List.flatten(rows)
  end

  defp stored_rows(shard) do
    dst = Path.join(System.tmp_dir!(), "recfork_verify_#{System.unique_integer([:positive])}.db")
    assert {:ok, _etag} = Storage.pull(shard, dst)
    rows = rows_of(dst)
    for s <- ["", "-wal", "-shm"], do: File.rm(dst <> s)
    rows
  end

  # A peer's lineage of the same shard: a complete database the coordinator never wrote, planted as
  # the stored object (Storage.Local's object is the raw file; its etag is the content hash).
  defp plant_peer_object!(shard) do
    peer = Path.join(System.tmp_dir!(), "recfork_peer_#{System.unique_integer([:positive])}.db")
    {:ok, conn} = Connection.open(peer)
    {:ok, _} = Connection.query(conn, "CREATE TABLE kv (v TEXT)", [])
    {:ok, _} = Connection.query(conn, "INSERT INTO kv VALUES ('peer_acked')", [])
    :ok = Connection.close(conn)
    {:ok, _} = File.copy(peer, Path.join(Storage.Local.dir(), shard <> ".db"))
    for s <- ["", "-wal", "-shm"], do: File.rm(peer <> s)
    :ok
  end

  defp open_with_object!(shard, mode) do
    set_mode!(mode)
    {:ok, conn} = ShardExecutor.open(shard)
    {:ok, pid} = Shards.ensure(shard)
    assert_mode!(pid, mode)
    {:ok, _} = ShardExecutor.execute(conn, stmt("CREATE TABLE kv (v TEXT)"))
    {:ok, _} = ShardExecutor.execute(conn, stmt("INSERT INTO kv VALUES ('mine_flushed')"))
    assert :ok = Shards.flush(shard)
    assert stored_rows(shard) == ["mine_flushed"]
    {conn, pid}
  end

  for mode <- @modes do
    describe "#{mode} mode" do
      @tag mode: mode
      test "a periodic-flush 412 onto a peer's object quarantines instead of clobbering",
           %{shard: shard, mode: mode} do
        {conn, pid} = open_with_object!(shard, mode)
        ref = Process.monitor(pid)

        plant_peer_object!(shard)
        {:ok, _} = ShardExecutor.execute(conn, stmt("INSERT INTO kv VALUES ('mine_unflushed')"))

        result = Shards.flush(shard)

        # THE INVARIANT: the peer's acked, durable write is still the stored object. Pre-fix the
        # reconcile adopted the peer's etag and the re-kicked flush PUT our lineage over it.
        assert stored_rows(shard) == ["peer_acked"],
               "the 412-reconcile re-fenced onto an object it did not write and clobbered it"

        assert result == {:error, :lease_lost}
        assert_receive {:DOWN, ^ref, :process, ^pid, {:shutdown, :lease_lost}}, 5_000

        # Our side is quarantined, not destroyed: every local write is in the `.fenced` copy.
        assert [fenced] =
                 Path.wildcard(Path.join(Shard.data_dir(), shard <> ".db.fenced.*"))
                 |> Enum.reject(&String.ends_with?(&1, ["-wal", "-shm"]))

        assert rows_of(fenced) == ["mine_flushed", "mine_unflushed"]
        refute File.exists?(Shard.db_path(shard))

        # The lock we still held was given back (keeping its claim), not stranded behind this
        # node's live heartbeat.
        assert Storage.lease_holder(shard) == :free

        _ = ShardExecutor.close(conn)
      end

      @tag mode: mode
      test "the drop-flush 412 onto a peer's object quarantines instead of re-uploading",
           %{shard: shard, mode: mode} do
        {conn, pid} = open_with_object!(shard, mode)
        {:ok, _} = ShardExecutor.execute(conn, stmt("INSERT INTO kv VALUES ('mine_unflushed')"))
        :ok = ShardExecutor.close(conn)

        plant_peer_object!(shard)

        ref = Process.monitor(pid)
        :ok = Shards.stop(shard)
        assert_receive {:DOWN, ^ref, :process, ^pid, _}, 5_000

        # Pre-fix `retry_drop_upload/1` re-fenced to the peer's etag and uploaded over it.
        assert stored_rows(shard) == ["peer_acked"],
               "the drop path re-fenced onto an object it did not write and clobbered it"

        assert [fenced] =
                 Path.wildcard(Path.join(Shard.data_dir(), shard <> ".db.fenced.*"))
                 |> Enum.reject(&String.ends_with?(&1, ["-wal", "-shm"]))

        assert rows_of(fenced) == ["mine_flushed", "mine_unflushed"]
        assert Storage.lease_holder(shard) == :free
      end
    end
  end
end
