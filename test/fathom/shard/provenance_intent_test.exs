defmodule Fathom.Shard.ProvenanceIntentTest do
  @moduledoc """
  The provenance sidecar lagged the object PUT, and a crash in the gap made `Fork` quarantine a
  good, NEWER local copy (expert review 2026-10-10 #6). Two gaps, one symptom:

    * **Rename gap.** The flush stamped the sidecar with temp+rename and no directory fsync, so a
      lost rename reverted it to the previous etag. The sidecar is now a fixed-width record
      overwritten IN PLACE (`pwrite` + `fsync`) — nothing to rename.
    * **Mailbox gap.** The sidecar learned the new etag only when the coordinator handled the flush
      task's result, after the PUT had landed. The flush now durably records the plaintext md5 of
      the bytes it is about to PUT; `Fork` adopts a stored object carrying that md5 instead of
      quarantining.

  Not async: shards are global and back onto real files.
  """
  use ExUnit.Case, async: false

  import ExUnit.CaptureLog

  alias Fathom.{Shard, ShardExecutor, Shards}
  alias Fathom.Shard.{Connection, Fork, Provenance, Storage}
  alias Filo.{Stmt, StmtResult}

  @md5_a String.duplicate("a", 32)
  @md5_b String.duplicate("b", 32)

  setup do
    shard = "intent_#{System.unique_integer([:positive])}"
    prev_storage = Application.get_env(:fathom, :shard_storage)
    prev_flush = Application.get_env(:fathom, :shard_flush_interval_ms)

    Application.put_env(:fathom, :shard_storage, Fathom.Test.FaultyStorage)
    Application.put_env(:fathom, :shard_flush_interval_ms, 60_000)

    on_exit(fn ->
      Application.delete_env(:fathom, :storage_fault)
      restore(:shard_storage, prev_storage)
      restore(:shard_flush_interval_ms, prev_flush)
      Shards.drain(shard, 2_000)

      for dir <- [local_dir(), remote_dir()],
          suffix <- [".db", ".db-wal", ".db-shm", ".lock", ".db.etag", ".pos", ".lineage"],
          do: File.rm(Path.join(dir, shard <> suffix))

      for f <- Path.wildcard(Path.join(local_dir(), "#{shard}.db.*")), do: File.rm(f)
    end)

    %{shard: shard}
  end

  defp restore(key, nil), do: Application.delete_env(:fathom, key)
  defp restore(key, value), do: Application.put_env(:fathom, key, value)

  defp stmt(sql, args \\ []), do: %Stmt{sql: sql, args: args}
  defp local_dir, do: Shard.data_dir()
  defp remote_dir, do: Storage.Local.dir()
  defp local_db(shard), do: Path.join(local_dir(), "#{shard}.db")

  defp flush_now(coordinator) do
    send(coordinator, :durability_flush)
    wait_flush_settled(coordinator, 400)
  end

  defp wait_flush_settled(_coordinator, 0), do: flunk("durability flush task never settled")

  defp wait_flush_settled(coordinator, tries) do
    case :sys.get_state(coordinator) do
      %{flush_task: nil} ->
        :ok

      _ ->
        Process.sleep(5)
        wait_flush_settled(coordinator, tries - 1)
    end
  end

  defp kill_coordinator(shard, conn) do
    {:ok, coordinator} = Shards.ensure(shard)
    ref = Process.monitor(coordinator)
    Process.exit(coordinator, :kill)
    assert_receive {:DOWN, ^ref, :process, ^coordinator, :killed}, 1_000
    _ = :sys.get_state(Fathom.ShardSupervisor)
    ShardExecutor.close(conn)
  end

  defp quarantined?(shard), do: Path.wildcard(local_db(shard) <> ".forked.*") != []

  defp rows(conn),
    do: ShardExecutor.execute(conn, stmt("SELECT v FROM kv ORDER BY v"))

  # The mailbox gap, staged for real: the flush PUT lands (the object advances) but the
  # coordinator never learns it — `:flush_lands_then_errors` returns an error after the object is
  # written, which is the same observable state as a node killed between the PUT and the task
  # result being handled. The sidecar still names the OLD etag; then the coordinator is killed.
  describe "mailbox gap — the PUT landed, the sidecar never learned" do
    test "a warm reopen adopts the node's own landed flush instead of quarantining it",
         %{shard: shard} do
      {:ok, conn} = ShardExecutor.open(shard)
      {:ok, coordinator} = Shards.ensure(shard)
      {:ok, _} = ShardExecutor.execute(conn, stmt("CREATE TABLE kv (v TEXT)"))
      {:ok, _} = ShardExecutor.execute(conn, stmt("INSERT INTO kv VALUES ('first')"))
      :ok = flush_now(coordinator)
      {:ok, e0} = Storage.object_etag(shard)
      assert {:ok, ^e0} = Provenance.read(local_db(shard)) |> normalize()

      {:ok, _} = ShardExecutor.execute(conn, stmt("INSERT INTO kv VALUES ('landed')"))
      Application.put_env(:fathom, :storage_fault, :flush_lands_then_errors)
      capture_log(fn -> :ok = flush_now(coordinator) end)
      Application.delete_env(:fathom, :storage_fault)

      {:ok, e1} = Storage.object_etag(shard)
      refute e1 == e0, "the PUT must have landed for this scenario to mean anything"

      assert {:ok, ^e0} = Provenance.read(local_db(shard)) |> normalize(),
             "precondition: the sidecar lags the object"

      assert is_binary(Provenance.read_intent(local_db(shard))),
             "the flush must have durably recorded its intent BEFORE the PUT"

      kill_coordinator(shard, conn)

      log =
        capture_log(fn ->
          {:ok, conn2} = ShardExecutor.open(shard)

          # THE INVARIANT: the warm copy descends from the stored object — serve it, do not hide
          # it in .forked.<ts>.
          assert {:ok, %StmtResult{rows: [["first"], ["landed"]]}} = rows(conn2)
          :ok = ShardExecutor.close(conn2)
        end)

      refute quarantined?(shard),
             "a good, newer local copy was quarantined because the sidecar lagged our own PUT"

      assert log =~ "adopted etag"
      assert {:ok, ^e1} = Provenance.read(local_db(shard)) |> normalize()
    end

    test "the first-ever flush of a born-here shard is adopted the same way", %{shard: shard} do
      {:ok, conn} = ShardExecutor.open(shard)
      {:ok, coordinator} = Shards.ensure(shard)
      {:ok, _} = ShardExecutor.execute(conn, stmt("CREATE TABLE kv (v TEXT)"))
      {:ok, _} = ShardExecutor.execute(conn, stmt("INSERT INTO kv VALUES ('born')"))

      assert :no_object = Provenance.read(local_db(shard))

      Application.put_env(:fathom, :storage_fault, :flush_lands_then_errors)
      capture_log(fn -> :ok = flush_now(coordinator) end)
      Application.delete_env(:fathom, :storage_fault)

      assert {:ok, etag} = Storage.object_etag(shard)
      assert is_binary(etag), "the first flush must have landed"
      assert :no_object = Provenance.read(local_db(shard))

      kill_coordinator(shard, conn)

      capture_log(fn ->
        {:ok, conn2} = ShardExecutor.open(shard)
        assert {:ok, %StmtResult{rows: [["born"]]}} = rows(conn2)
        :ok = ShardExecutor.close(conn2)
      end)

      refute quarantined?(shard)
    end

    test "a DIFFERENT stored object is still a fork — intent adoption is md5-exact",
         %{shard: shard} do
      {:ok, conn} = ShardExecutor.open(shard)
      {:ok, coordinator} = Shards.ensure(shard)
      {:ok, _} = ShardExecutor.execute(conn, stmt("CREATE TABLE kv (v TEXT)"))
      {:ok, _} = ShardExecutor.execute(conn, stmt("INSERT INTO kv VALUES ('first')"))
      :ok = flush_now(coordinator)

      # A flush that was ATTEMPTED (intent recorded) but did not land...
      {:ok, _} = ShardExecutor.execute(conn, stmt("INSERT INTO kv VALUES ('a-fork')"))
      Application.put_env(:fathom, :storage_fault, :flush)
      capture_log(fn -> :ok = flush_now(coordinator) end)
      Application.delete_env(:fathom, :storage_fault)
      kill_coordinator(shard, conn)

      # ...and a peer wrote and released a different lineage meanwhile.
      b = Path.join(System.tmp_dir!(), "intent_b_#{shard}.db")
      {:ok, cb} = Connection.open(b)
      :ok = Connection.exec(cb, "CREATE TABLE kv (v TEXT)")
      :ok = Connection.exec(cb, "INSERT INTO kv VALUES ('b-write')")
      :ok = Connection.exec(cb, "PRAGMA wal_checkpoint(TRUNCATE)")
      Connection.close(cb)
      :ok = Storage.flush(shard, b)
      for s <- ["", "-wal", "-shm"], do: File.rm(b <> s)
      File.rm(Path.join(remote_dir(), "#{shard}.lock"))

      capture_log(fn ->
        {:ok, conn2} = ShardExecutor.open(shard)
        assert {:ok, %StmtResult{rows: [["b-write"]]}} = rows(conn2)
        :ok = ShardExecutor.close(conn2)
      end)

      assert quarantined?(shard), "a real fork must still be quarantined"
    end
  end

  describe "Fork.resolve/4 on a diverged sidecar" do
    setup %{shard: shard} do
      path = local_db(shard)
      File.mkdir_p!(Path.dirname(path))
      {:ok, c} = Connection.open(path)
      :ok = Connection.exec(c, "CREATE TABLE kv (v TEXT)")
      :ok = Connection.exec(c, "INSERT INTO kv VALUES ('x')")
      :ok = Connection.exec(c, "PRAGMA wal_checkpoint(TRUNCATE)")
      Connection.close(c)
      for s <- ["-wal", "-shm"], do: File.rm(path <> s)
      :ok = Storage.flush(shard, path)
      {:ok, store_etag} = Storage.object_etag(shard)
      {:ok, %{md5: md5}} = Storage.object_head(shard)
      %{path: path, store_etag: store_etag, md5: md5}
    end

    test "intent == stored md5 adopts and re-stamps", %{
      shard: shard,
      path: path,
      store_etag: store_etag,
      md5: md5
    } do
      Provenance.write_durable(path, "stale-etag")
      Provenance.record_intent(path, md5)

      capture_log(fn ->
        verdict = Fork.evidence(shard, path)
        assert {:diverged, "stale-etag", ^store_etag} = verdict
        refute Fork.resolve(verdict, shard, path, %{})
      end)

      assert {:ok, ^store_etag} = Provenance.read(path) |> normalize()
      assert Provenance.read_intent(path) == nil, "adoption clears the intent"
      refute quarantined?(shard)
    end

    test "intent != stored md5 quarantines", %{shard: shard, path: path, md5: md5} do
      other = if md5 == @md5_a, do: @md5_b, else: @md5_a
      Provenance.write_durable(path, "stale-etag")
      Provenance.record_intent(path, other)

      capture_log(fn ->
        assert Fork.resolve(Fork.evidence(shard, path), shard, path, %{})
      end)

      assert quarantined?(shard)
    end

    # Symptom (expert review 2026-10-10 R2-1): flush #1's PUT landed (object md5 = m1) but its result
    # was never stamped; the retry flush recorded m2 over the single intent slot, its PUT 412'd, and
    # a crash left intent m2 vs object m1 -> a good copy quarantined. Two slots keep m1 resolvable.
    test "a retry's newer intent does not erase the unresolved earlier one", %{
      shard: shard,
      path: path,
      store_etag: store_etag,
      md5: md5
    } do
      retry = if md5 == @md5_a, do: @md5_b, else: @md5_a
      Provenance.write_durable(path, "stale-etag")
      Provenance.record_intent(path, md5)
      Provenance.record_intent(path, retry)
      assert Provenance.read_intents(path) == [retry, md5]

      capture_log(fn ->
        verdict = Fork.evidence(shard, path)
        assert {:diverged, "stale-etag", ^store_etag} = verdict
        refute Fork.resolve(verdict, shard, path, %{})
      end)

      assert {:ok, ^store_etag} = Provenance.read(path) |> normalize()
      refute quarantined?(shard)
    end

    test "re-recording the same intent keeps the older one; a third drops the oldest", %{
      path: path
    } do
      Provenance.write_durable(path, "e")
      Provenance.record_intent(path, @md5_a)
      Provenance.record_intent(path, @md5_b)
      Provenance.record_intent(path, @md5_b)
      assert Provenance.read_intents(path) == [@md5_b, @md5_a]
      c = String.duplicate("c", 32)
      Provenance.record_intent(path, c)
      assert Provenance.read_intents(path) == [c, @md5_b]
    end

    test "no intent quarantines (the pre-existing behaviour)", %{shard: shard, path: path} do
      Provenance.write_durable(path, "stale-etag")

      capture_log(fn ->
        assert Fork.resolve(Fork.evidence(shard, path), shard, path, %{})
      end)

      assert quarantined?(shard)
    end
  end

  describe "the fixed-width sidecar" do
    setup %{shard: shard} do
      path = local_db(shard)
      File.mkdir_p!(Path.dirname(path))
      %{path: path, sidecar: Provenance.sidecar_path(path)}
    end

    test "a durable stamp is overwritten IN PLACE (same inode — no rename to lose)", %{
      path: path,
      sidecar: sidecar
    } do
      Provenance.write_durable(path, "etag-one")
      %{inode: inode, size: size} = File.stat!(sidecar)

      Provenance.write_durable(path, "etag-two-which-is-longer-than-the-first")
      Provenance.record_intent(path, @md5_a)

      assert %{inode: ^inode, size: ^size} = File.stat!(sidecar)
      assert {:ok, "etag-two-which-is-longer-than-the-first"} = Provenance.read(path)
      assert Provenance.read_intent(path) == @md5_a

      Provenance.write_durable(path, "etag-three")
      assert {:ok, "etag-three"} = Provenance.read(path)
      assert Provenance.read_intent(path) == nil, "a stamp clears the intent"
      assert %{inode: ^inode} = File.stat!(sidecar)
    end

    test "the no-object sentinel survives the record format", %{path: path} do
      Provenance.write_no_object(path)
      Provenance.make_durable(path)
      assert :no_object = Provenance.read(path)
      Provenance.record_intent(path, @md5_a)
      assert :no_object = Provenance.read(path)
      assert Provenance.read_intent(path) == @md5_a
    end

    test "a LEGACY variable-width sidecar still reads, and converts on the first durable write",
         %{path: path, sidecar: sidecar} do
      File.write!(sidecar, "legacy-etag-0123456789abcdef")
      assert {:ok, "legacy-etag-0123456789abcdef"} = Provenance.read(path)
      assert Provenance.read_intent(path) == nil

      Provenance.record_intent(path, @md5_a)
      assert {:ok, "legacy-etag-0123456789abcdef"} = Provenance.read(path)
      assert Provenance.read_intent(path) == @md5_a

      File.write!(sidecar, "-")
      assert :no_object = Provenance.read(path)
    end

    test "intent is not recorded over a missing or corrupt sidecar", %{
      path: path,
      sidecar: sidecar
    } do
      Provenance.record_intent(path, @md5_a)
      refute File.exists?(sidecar), "an intent must not invent provenance"

      File.write!(sidecar, "")
      Provenance.record_intent(path, @md5_a)
      assert :corrupt = Provenance.read(path)
    end

    test "a torn record reads as corrupt (the safe direction)", %{path: path, sidecar: sidecar} do
      Provenance.write_durable(path, "etag-one")
      File.write!(sidecar, binary_part(File.read!(sidecar), 0, 40))
      assert :corrupt = Provenance.read(path)
    end

    # Symptom (expert review 2026-10-10 R2-5): the in-place record had no checksum, so a write torn
    # between old and new bytes (right length, right framing) parsed as a chimera of two stamps.
    test "a full-length record mixing old and new bytes reads as corrupt", %{
      path: path,
      sidecar: sidecar
    } do
      Provenance.write_durable(path, "etag-old")
      old = File.read!(sidecar)
      Provenance.write_durable(path, "etag-new")
      Provenance.record_intent(path, @md5_a)
      new = File.read!(sidecar)
      assert byte_size(old) == byte_size(new)
      assert {:ok, "etag-new"} = Provenance.read(path)

      # The new stamp's etag bytes but the old tail (intent/crc region): same length, valid framing.
      split = 5 + 96 + 1
      torn = binary_part(new, 0, split) <> binary_part(old, split, byte_size(old) - split)
      File.write!(sidecar, torn)
      assert :corrupt = Provenance.read(path)
      assert Provenance.read_intents(path) == []

      File.write!(sidecar, new)
      assert {:ok, "etag-new"} = Provenance.read(path)
    end

    test "an etag too wide for the record falls back to the legacy format and still reads", %{
      path: path
    } do
      wide = String.duplicate("e", 120)
      Provenance.write_durable(path, wide)
      assert {:ok, ^wide} = Provenance.read(path)
    end
  end

  defp normalize({:ok, etag}), do: {:ok, etag}
  defp normalize(other), do: other
end
