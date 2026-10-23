defmodule Fathom.Shard.RetainedReopenTest do
  @moduledoc """
  Expert review 2026-10-22 r8 #26 (user decision Q4 = B: local retention will be used).

  SYMPTOM: a claimed clean-retained copy's reopen issued a speculative pre-lease fork-evidence HEAD,
  then the lock PUT, then the post-lease HEAD that is the actual authority for it: 3 requests,
  ~2 RTT, one RTT slower than a small shard's cold reopen with replication off.

  INVARIANTS pinned, in BOTH lease modes (mode asserted):

    * a retained reopen reads the object etag exactly ONCE (the post-lease check), and serves warm;
    * a retained copy whose lineage moved is still discarded and pulled (the post-lease check alone
      decides it);
    * the size floor: below `Retained.min_bytes/0` a clean drop does not retain; unset, the floor is
      1 MiB with replication off and 0 with it on; an explicit setting wins.

  Not covered here: a TAKEOVER reopen of a retained copy (the evidence is then gathered after the
  acquire, sequentially). Staging a takeover of a cleanly released lock on one node needs a foreign
  lock holder; the branch is the pre-#26 code path moved after the acquire.

  Not async: app env, telemetry, real shard files, the node-global retention index.
  """
  use ExUnit.Case, async: false

  alias Fathom.{Shard, ShardExecutor, Shards}
  alias Fathom.Shard.{Heartbeat, Provenance, Retained, Storage}
  alias Filo.Stmt

  setup do
    id = "retreopen_#{System.unique_integer([:positive])}"
    keys = [:shard_retain_local_bytes, :shard_retain_min_bytes, :shard_idle_ms, :shard_storage]
    prev = for k <- keys, do: {k, Application.fetch_env(:fathom, k)}
    Application.put_env(:fathom, :shard_retain_local_bytes, 64 * 1024 * 1024)
    Application.put_env(:fathom, :shard_retain_min_bytes, 0)
    Application.put_env(:fathom, :shard_idle_ms, 600_000)
    Application.put_env(:fathom, :shard_storage, Fathom.Test.FaultyStorage)
    Retained.reconcile([], Shard.data_dir())
    _ = Retained.stats()

    on_exit(fn ->
      Application.delete_env(:fathom, :faulty_object_etag)

      for {k, v} <- prev do
        case v do
          {:ok, val} -> Application.put_env(:fathom, k, val)
          :error -> Application.delete_env(:fathom, k)
        end
      end

      Shards.drain(id, 2_000)
      Storage.purge_shard(id)
      for f <- Path.wildcard(Path.join(Shard.data_dir(), "#{id}.db*")), do: File.rm(f)
      Retained.reconcile([], Shard.data_dir())
    end)

    %{id: id, path: Shard.db_path(id)}
  end

  defp stmt(sql), do: %Stmt{sql: sql, args: []}

  defp write!(id, sqls) do
    {:ok, conn} = ShardExecutor.open(id)
    Enum.each(sqls, fn s -> {:ok, _} = ShardExecutor.execute(conn, stmt(s)) end)
    :ok = ShardExecutor.close(conn)
  end

  defp rows!(id) do
    {:ok, conn} = ShardExecutor.open(id)
    {:ok, %{rows: rows}} = ShardExecutor.execute(conn, stmt("SELECT v FROM t ORDER BY v"))
    :ok = ShardExecutor.close(conn)
    rows
  end

  defp drop!(id) do
    {:ok, pid} = Shards.ensure(id)
    ref = Process.monitor(pid)
    assert :ok = Shards.drain(id, 5_000)
    assert_receive {:DOWN, ^ref, :process, ^pid, _}, 5_000
  end

  defp set_mode!(:legacy), do: refute(Heartbeat.running?())

  defp set_mode!(:heartbeat) do
    hb = start_supervised!({Heartbeat, ttl_ms: 30_000})
    _ = :sys.get_state(hb)
    assert Heartbeat.running?()
  end

  defp assert_mode!(id, mode) do
    {:ok, pid} = Shards.ensure(id)
    gen = :sys.get_state(pid).acquire_gen

    if mode == :heartbeat,
      do: assert(is_integer(gen), "heartbeat mode did not take"),
      else: assert(is_nil(gen), "legacy mode did not take")
  end

  # Count this shard's object-etag reads (object_etag/1 and object_head/1) during the reopen.
  defp count_reads!(id) do
    reads = :counters.new(1, [])

    Application.put_env(
      :fathom,
      :faulty_object_etag,
      {id,
       fn ->
         :counters.add(reads, 1, 1)
         :pass
       end}
    )

    reads
  end

  defp attach_cold_open!(id) do
    test = self()
    name = "retreopen-#{id}"

    :telemetry.attach(
      name,
      [:fathom, :shard, :cold_open],
      fn _, _, %{shard_id: sid, warm: warm}, _ ->
        if sid == id, do: send(test, {:opened, warm})
      end,
      nil
    )

    on_exit(fn -> :telemetry.detach(name) end)
  end

  for mode <- [:legacy, :heartbeat] do
    test "[#{mode}] a retained reopen reads the object etag once and serves warm", %{
      id: id,
      path: path
    } do
      set_mode!(unquote(mode))
      write!(id, ["CREATE TABLE t (v TEXT)", "INSERT INTO t VALUES ('a')"])
      drop!(id)
      assert File.exists?(Retained.marker(path)), "fixture: the clean drop did not retain"
      {:ok, etag} = Provenance.read(path)
      assert {:ok, ^etag} = Storage.object_etag(id)

      attach_cold_open!(id)
      reads = count_reads!(id)
      assert rows!(id) == [["a"]]
      assert_receive {:opened, true}, 1_000

      assert :counters.get(reads, 1) == 1,
             "a retained reopen read the object etag #{:counters.get(reads, 1)} times (pre-#26: 2)"

      assert_mode!(id, unquote(mode))
    end

    test "[#{mode}] a retained copy the lineage moved past is still discarded and pulled", %{
      id: id,
      path: path
    } do
      set_mode!(unquote(mode))
      write!(id, ["CREATE TABLE t (v TEXT)", "INSERT INTO t VALUES ('old')"])
      drop!(id)

      stash = Path.join(System.tmp_dir!(), "#{id}_stash")
      File.mkdir_p!(stash)
      on_exit(fn -> File.rm_rf(stash) end)
      files = [path, path <> ".etag", Retained.marker(path)]
      for f <- files, do: :ok = File.rename(f, Path.join(stash, Path.basename(f)))
      write!(id, ["INSERT INTO t VALUES ('new')"])
      drop!(id)
      for f <- Path.wildcard(path <> "*"), do: File.rm(f)
      for f <- files, do: :ok = File.rename(Path.join(stash, Path.basename(f)), f)
      {:ok, sidecar} = Provenance.read(path)
      {:ok, store} = Storage.object_etag(id)
      assert sidecar != store, "fixture: the restored copy still matches the store"

      assert rows!(id) == [["new"], ["old"]], "the stale retained copy was served"
      assert Path.wildcard(path <> ".forked.*") == []
      assert_mode!(id, unquote(mode))
    end
  end

  describe "the size floor" do
    defp fixture!(path, bytes) do
      File.mkdir_p!(Path.dirname(path))
      File.write!(path, :binary.copy("x", bytes))
      :ok = Provenance.write(path, "etag-floor")
    end

    test "below the floor a clean drop does not retain", %{id: id, path: path} do
      Application.put_env(:fathom, :shard_retain_min_bytes, 4_096)
      fixture!(path, 1_000)
      me = self()
      assert :dropped = Retained.retain_or_drop(id, path, fn p -> send(me, {:dropped, p}) end)
      assert_received {:dropped, ^path}
      refute File.exists?(Retained.marker(path))

      fixture!(path, 8_192)
      assert :retained = Retained.retain_or_drop(id, path, &flunk("dropped #{&1}"))
    end

    test "unset: 1 MiB with replication off, 0 with it on; an explicit value wins" do
      prev = Application.fetch_env(:fathom, :replication_enabled)

      on_exit(fn ->
        case prev do
          {:ok, v} -> Application.put_env(:fathom, :replication_enabled, v)
          :error -> Application.delete_env(:fathom, :replication_enabled)
        end
      end)

      Application.delete_env(:fathom, :shard_retain_min_bytes)
      Application.put_env(:fathom, :replication_enabled, false)
      assert Retained.min_bytes() == 1_048_576
      Application.put_env(:fathom, :replication_enabled, true)
      assert Retained.min_bytes() == 0
      Application.put_env(:fathom, :shard_retain_min_bytes, 123)
      assert Retained.min_bytes() == 123
    end
  end
end
