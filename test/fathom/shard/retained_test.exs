defmodule Fathom.Shard.RetainedTest do
  @moduledoc """
  Local `.db` retention after a clean durable drop (expert review 2026-10-20-2027 #22).

  What is pinned:

    * a clean durable drop KEEPS `.db` + sidecar under a `.retained` marker, and the reopen is a
      WARM open that is also CLEAN (no re-upload on the next drop) — both liveness modes;
    * a retained copy whose lineage moved on elsewhere is DISCARDED and pulled, never moved to a
      `.forked.<ts>` quarantine (it holds nothing the store lacks);
    * eviction is LRU by bytes under the budget and never touches a copy an open has claimed;
    * a non-empty `-wal` is never retained (the db alone would be stale against the sidecar);
    * every "is there an unflushed local copy?" caller (drain, flush, export, fork's flush-before)
      reads a retained copy as clean, and a tenant erase removes the markers too;
    * a pre-open stop puts a claimed copy back under the budget.
  """
  use Fathom.DataCase, async: false

  alias Fathom.{Shard, ShardExecutor, Shards, Tenants}
  alias Fathom.Shard.{Heartbeat, Provenance, Retained, Storage, WriteCounter}
  alias Fathom.Tenants.Tombstones
  alias Filo.Stmt

  @modes [:legacy, :heartbeat]

  setup do
    id = "retain_#{System.unique_integer([:positive])}"
    prev_budget = Application.get_env(:fathom, :shard_retain_local_bytes)
    prev_idle = Application.get_env(:fathom, :shard_idle_ms)
    Application.put_env(:fathom, :shard_retain_local_bytes, 64 * 1024 * 1024)
    # Pin the idle timer high: an idle stop racing an assertion would answer for the coordinator.
    Application.put_env(:fathom, :shard_idle_ms, 600_000)
    forget_index!()

    on_exit(fn ->
      restore_env(:shard_retain_local_bytes, prev_budget)
      restore_env(:shard_idle_ms, prev_idle)
      :ets.delete(Tombstones, id)

      for shard <- [id, id <> "dst"] do
        Shards.drain(shard, 2_000)
        Storage.purge_shard(shard)
        for f <- Path.wildcard(Path.join(Shard.data_dir(), "#{shard}.db*")), do: File.rm(f)
      end

      forget_index!()
    end)

    %{id: id, path: Shard.db_path(id)}
  end

  # The index is node-global; drop entries left by earlier tests (their files are gone).
  defp forget_index! do
    Retained.reconcile([], Shard.data_dir())
    _ = Retained.stats()
  end

  defp restore_env(key, nil), do: Application.delete_env(:fathom, key)
  defp restore_env(key, value), do: Application.put_env(:fathom, key, value)

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
    pid
  end

  defp assert_retained!(path) do
    assert File.exists?(path), "the clean drop deleted the local db"
    assert File.exists?(path <> ".etag"), "the clean drop deleted the sidecar"
    assert File.exists?(Retained.marker(path)), "no clean-retained marker was written"
    refute File.exists?(path <> "-wal"), "a retained copy kept its -wal"
  end

  defp set_mode!(:legacy), do: refute(Heartbeat.running?())

  defp set_mode!(:heartbeat) do
    hb = start_supervised!({Heartbeat, ttl_ms: 30_000})
    _ = :sys.get_state(hb)
    assert Heartbeat.running?()
  end

  defp assert_mode!(pid, :heartbeat), do: assert(is_integer(:sys.get_state(pid).acquire_gen))
  defp assert_mode!(pid, :legacy), do: assert(is_nil(:sys.get_state(pid).acquire_gen))

  defp attach_cold_open!(id) do
    name = "retained-cold-open-#{id}"
    test = self()

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

  for mode <- @modes do
    test "[#{mode}] a clean durable drop retains the copy; the reopen is warm and stays clean",
         %{id: id, path: path} do
      set_mode!(unquote(mode))
      write!(id, ["CREATE TABLE t (v TEXT)", "INSERT INTO t VALUES ('a')"])
      {:ok, pid} = Shards.ensure(id)
      assert_mode!(pid, unquote(mode))
      drop!(id)

      assert_retained!(path)
      assert {:ok, etag} = Storage.object_etag(id)
      assert {:ok, ^etag} = Provenance.read(path)

      attach_cold_open!(id)
      assert rows!(id) == [["a"]]
      assert_receive {:opened, true}, 1_000

      {:ok, pid} = Shards.ensure(id)
      assert_mode!(pid, unquote(mode))
      st = :sys.get_state(pid)

      # Clean: a retained copy equals the object, so it must NOT be seeded dirty like a warm restart.
      assert st.flushed_through >= WriteCounter.count(id),
             "the retained reopen was seeded dirty; its next drop re-uploads unchanged bytes"

      refute File.exists?(Retained.marker(path))
      refute File.exists?(Retained.claimed_marker(path)), "the claim outlived the open"

      # The second drop is the clean path: no upload, retained again.
      drop!(id)
      assert {:ok, ^etag} = Storage.object_etag(id)
      assert_retained!(path)
    end

    test "[#{mode}] a retained copy the lineage moved past is discarded and pulled, not quarantined",
         %{id: id, path: path} do
      set_mode!(unquote(mode))
      write!(id, ["CREATE TABLE t (v TEXT)", "INSERT INTO t VALUES ('old')"])
      drop!(id)
      assert_retained!(path)

      # Set the old retained copy aside, as if this node kept it while another node took the shard,
      # wrote, and dropped (a realistic moved lineage: the object's etag changes).
      stash = Path.join(System.tmp_dir!(), "#{id}_stash")
      File.mkdir_p!(stash)
      on_exit(fn -> File.rm_rf(stash) end)
      files = [path, path <> ".etag", Retained.marker(path)]
      for f <- files, do: :ok = File.rename(f, Path.join(stash, Path.basename(f)))

      write!(id, ["INSERT INTO t VALUES ('new')"])
      {:ok, pid} = Shards.ensure(id)
      assert_mode!(pid, unquote(mode))
      drop!(id)
      for f <- Path.wildcard(path <> "*"), do: File.rm(f)
      for f <- files, do: :ok = File.rename(Path.join(stash, Path.basename(f)), f)

      assert {:ok, sidecar} = Provenance.read(path)
      assert {:ok, store} = Storage.object_etag(id)
      assert sidecar != store, "fixture: the restored copy still matches the store"

      test = self()

      :telemetry.attach(
        "retained-discard-#{id}",
        [:fathom, :shard, :retained, :discarded],
        fn _, _, meta, _ -> send(test, {:discarded, meta.shard_id}) end,
        nil
      )

      on_exit(fn -> :telemetry.detach("retained-discard-#{id}") end)

      assert rows!(id) == [["new"], ["old"]]
      assert_receive {:discarded, ^id}, 1_000

      assert Path.wildcard(path <> ".forked.*") == [],
             "a clean retained copy was quarantined instead of discarded"
    end
  end

  # Integration review of expert review 2026-10-20-2027 #22. Symptom: this node retains E1; another
  # node takes the shard, writes, flushes E2 and releases; the reopen here is a fresh create (no
  # takeover) and the store HEADs fail. The ordinary warm fail-safe ("unreachable: keep it, fenced
  # on its provenance") then served the retained E1 — stale reads, and writes fenced on E1 that
  # 412 into the self-fence. Invariant: a claimed clean-retained copy is NEVER served on an
  # unverifiable verdict; it is discarded and pulled, and when the pull fails too the open fails.
  #
  # Staged on ONE node like the moved-lineage test above: set the E1 copy aside, advance the store
  # to E2 through a normal open + drop, put E1 back. `:faulty_object_etag` makes this shard's
  # object_etag/object_head answer an error tuple (a real S3 timeout's shape).
  defp stage_moved_lineage!(id, path) do
    write!(id, ["CREATE TABLE t (v TEXT)", "INSERT INTO t VALUES ('old')"])
    drop!(id)
    assert_retained!(path)

    stash = Path.join(System.tmp_dir!(), "#{id}_stash")
    File.mkdir_p!(stash)
    on_exit(fn -> File.rm_rf(stash) end)
    files = [path, path <> ".etag", Retained.marker(path)]
    for f <- files, do: :ok = File.rename(f, Path.join(stash, Path.basename(f)))

    write!(id, ["INSERT INTO t VALUES ('new')"])
    drop!(id)
    for f <- Path.wildcard(path <> "*"), do: File.rm(f)
    for f <- files, do: :ok = File.rename(Path.join(stash, Path.basename(f)), f)

    assert {:ok, sidecar} = Provenance.read(path)
    assert {:ok, store} = Storage.object_etag(id)
    assert sidecar != store, "fixture: the restored copy still matches the store"
    {sidecar, store}
  end

  defp faulty_storage!(id, etag_fault) do
    prev_storage = Application.get_env(:fathom, :shard_storage)
    Application.put_env(:fathom, :shard_storage, Fathom.Test.FaultyStorage)
    Application.put_env(:fathom, :faulty_object_etag, {id, etag_fault})

    on_exit(fn ->
      Application.delete_env(:fathom, :faulty_object_etag)
      Application.delete_env(:fathom, :storage_fault)
      restore_env(:shard_storage, prev_storage)
    end)
  end

  defp assert_opened_mode!(id, mode) do
    {:ok, pid} = Shards.ensure(id)
    assert_mode!(pid, mode)
  end

  for mode <- @modes do
    test "[#{mode}] a retained copy is never served when the fork-evidence HEAD is unreachable",
         %{id: id, path: path} do
      set_mode!(unquote(mode))
      stage_moved_lineage!(id, path)
      faulty_storage!(id, {:error, :timeout})
      assert {:error, :timeout} = Storage.object_etag(id), "fixture: the fault did not take"

      assert rows!(id) == [["new"], ["old"]],
             "the unverified retained copy (E1) was served instead of the stored lineage (E2)"

      assert_opened_mode!(id, unquote(mode))
      assert Path.wildcard(path <> ".forked.*") == []
    end

    test "[#{mode}] a retained copy is never served when only the post-lease HEAD fails",
         %{id: id, path: path} do
      set_mode!(unquote(mode))
      {sidecar, _store} = stage_moved_lineage!(id, path)

      # The pre-lease evidence HEAD reads the copy's own etag (as if the peer's flush + release
      # landed in the gap before our acquire, the #19 window); every later read times out.
      reads = :counters.new(1, [])

      faulty_storage!(id, fn ->
        :counters.add(reads, 1, 1)
        if :counters.get(reads, 1) == 1, do: {:ok, sidecar}, else: {:error, :timeout}
      end)

      assert rows!(id) == [["new"], ["old"]],
             "the unverified retained copy (E1) was served on a failed post-lease check"

      assert :counters.get(reads, 1) >= 2, "fixture: the post-lease check never read the etag"
      assert_opened_mode!(id, unquote(mode))
    end

    test "[#{mode}] a retained copy with an unreachable store fails the open instead of serving",
         %{id: id, path: path} do
      set_mode!(unquote(mode))
      stage_moved_lineage!(id, path)
      faulty_storage!(id, {:error, :timeout})
      Application.put_env(:fathom, :storage_fault, :pull)

      assert {:error, _} = ShardExecutor.open(id),
             "an open served a retained copy it could not verify against the store"

      Application.delete_env(:fathom, :storage_fault)
      Application.delete_env(:fathom, :faulty_object_etag)

      # Retryable: once the store answers, the next open serves the stored lineage.
      assert rows!(id) == [["new"], ["old"]]
      assert_opened_mode!(id, unquote(mode))
    end
  end

  # --- Retained in isolation (no coordinator) ---

  defp fixture!(dir_id, bytes) do
    path = Shard.db_path(dir_id)
    File.mkdir_p!(Path.dirname(path))
    File.write!(path, :binary.copy("x", bytes))
    :ok = Provenance.write(path, "etag-" <> dir_id)
    on_exit(fn -> for f <- Path.wildcard(path <> "*"), do: File.rm(f) end)
    path
  end

  test "eviction is LRU by bytes under the budget", %{id: id} do
    Application.put_env(:fathom, :shard_retain_local_bytes, 2_500)
    [a, b, c] = for n <- 1..3, do: fixture!("#{id}x#{n}", 1_000)

    for {p, n} <- Enum.with_index([a, b, c], 1) do
      assert :retained = Retained.retain_or_drop("#{id}x#{n}", p, &flunk("dropped #{&1}"))
      # Distinct LRU stamps (millisecond clock).
      Process.sleep(2)
    end

    assert Retained.enforce() == 2_000
    refute File.exists?(a), "the oldest retained copy survived over budget"
    refute File.exists?(Retained.marker(a))
    refute File.exists?(a <> ".etag")
    assert File.exists?(b) and File.exists?(c)
  end

  test "eviction never touches a copy an open has claimed", %{id: id} do
    path = fixture!("#{id}c", 1_000)
    assert :retained = Retained.retain_or_drop("#{id}c", path, &flunk("dropped #{&1}"))
    assert :ok = Retained.claim("#{id}c", path)

    Application.put_env(:fathom, :shard_retain_local_bytes, 0)
    assert Retained.enforce() == 0
    assert File.exists?(path), "eviction deleted a file an open had claimed"
    assert Retained.claimed?(path)
    assert Retained.settle_claim!(path)
  end

  test "retention off (budget 0) drops as before", %{id: id} do
    Application.put_env(:fathom, :shard_retain_local_bytes, 0)
    path = fixture!("#{id}o", 100)
    test = self()
    assert :dropped = Retained.retain_or_drop("#{id}o", path, &send(test, {:dropped, &1}))
    assert_receive {:dropped, ^path}
    refute File.exists?(Retained.marker(path))
  end

  test "a non-empty WAL is never retained", %{id: id} do
    path = fixture!("#{id}w", 100)
    File.write!(path <> "-wal", "frames")
    test = self()
    assert :dropped = Retained.retain_or_drop("#{id}w", path, &send(test, {:dropped, &1}))
    assert_receive {:dropped, ^path}
    refute File.exists?(Retained.marker(path))
  end

  test "the reaper's listing indexes retained files it never saw and evicts over budget",
       %{id: id} do
    path = fixture!("#{id}r", 1_000)
    File.write!(Retained.marker(path), "")
    Application.put_env(:fathom, :shard_retain_local_bytes, 500)
    {:ok, names} = File.ls(Shard.data_dir())
    Retained.reconcile(names, Shard.data_dir())
    assert Retained.enforce() == 0
    refute File.exists?(path)
  end

  # --- the callers that ask "is there an unflushed local copy?" ---

  test "drain, flush and export treat a retained copy as clean and start no coordinator",
       %{id: id, path: path} do
    write!(id, ["CREATE TABLE t (v TEXT)", "INSERT INTO t VALUES ('a')"])
    drop!(id)
    assert_retained!(path)

    assert :ok = Shards.drain(id, 1_000)
    assert :ok = Shards.flush(id)
    assert {:ok, %{path: export, dir: dir}} = Tenants.export(id)
    on_exit(fn -> File.rm_rf(dir) end)
    assert File.exists?(export)

    assert Registry.lookup(Fathom.ShardRegistry, id) == [],
           "a retained copy was reopened as if it held unflushed writes"

    assert_retained!(path)
  end

  test "fork with flush_source over a retained source succeeds without reopening it",
       %{id: id, path: path} do
    {:ok, _} = Fathom.Directory.resolve(id)
    write!(id, ["CREATE TABLE t (v TEXT)", "INSERT INTO t VALUES ('src')"])
    drop!(id)
    assert_retained!(path)

    dst = id <> "dst"
    assert {:ok, %{shard_id: ^dst}} = Tenants.fork(id, dst, flush_source: true)
    assert Registry.lookup(Fathom.ShardRegistry, id) == []
    assert rows!(dst) == [["src"]]
  end

  test "a tenant erase removes the retained copy and its marker", %{id: id, path: path} do
    write!(id, ["CREATE TABLE t (v TEXT)", "INSERT INTO t VALUES ('pii')"])
    drop!(id)
    assert_retained!(path)

    :ets.insert(Tombstones, {id})
    assert :ok = Tenants.erase_local(id)

    assert Path.wildcard(path <> "*") == [],
           "an erased tenant's retained copy is still on disk: #{inspect(Path.wildcard(path <> "*"))}"

    # The reaper's next pass forgets it from the index.
    {:ok, names} = File.ls(Shard.data_dir())
    Retained.reconcile(names, Shard.data_dir())
    assert Retained.stats().bytes == 0
  end

  test "a tombstoned tenant's retained copy is never reopened", %{id: id, path: path} do
    write!(id, ["CREATE TABLE t (v TEXT)"])
    drop!(id)
    assert_retained!(path)
    :ets.insert(Tombstones, {id})
    assert {:error, _} = ShardExecutor.open(id)
    assert File.exists?(Retained.marker(path)), "a refused open claimed the copy"
  end

  test "a pre-open stop puts the claimed copy back under the budget", %{id: id, path: path} do
    write!(id, ["CREATE TABLE t (v TEXT)"])
    drop!(id)
    assert_retained!(path)

    # A foreign live holder: the open claims the copy, then is refused the lease.
    assert {:ok, _} = Storage.acquire_lease(id, "other@node#1", 60_000)
    assert {:error, _} = ShardExecutor.open(id)

    assert File.exists?(Retained.marker(path)), "the claim was not restored"
    refute Retained.claimed?(path)
    assert Retained.stats().count == 1
  end
end
