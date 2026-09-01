defmodule Fathom.TenantsTest do
  @moduledoc """
  Tenant deletion orchestration (expert review 2026-07-14 #15): `Fathom.Tenants.delete/1`
  tombstones + broadcasts + enqueues, and `purge/1` (the DeleteJob body) erases every stored
  object and drains the coordinator. The re-mint refusal itself is pinned in
  `Fathom.Tenants.TombstonesTest`.
  """
  use Fathom.DataCase, async: false
  use Oban.Testing, repo: Fathom.Repo

  alias Fathom.{Directory, ShardExecutor, Shards, Snapshots, Tenants}
  alias Fathom.Shard.Storage
  alias Fathom.Tenants.{DeleteJob, Suspensions, Tombstones}
  alias Filo.Stmt

  setup do
    id = "ten_#{System.unique_integer([:positive])}"

    on_exit(fn ->
      # The gate ETS tables are app-global — forget this id so it can't gate another test's shard.
      :ets.delete(Tombstones, id)
      :ets.delete(Suspensions, id)
      Shards.drain(id, 2_000)
      Storage.purge_shard(id)

      for path <- Path.wildcard(Path.join([Fathom.Shard.data_dir(), "#{id}*"])),
          do: File.rm(path)
    end)

    %{id: id}
  end

  describe "delete/1" do
    test "tombstones, sets the re-mint gate, and enqueues DeleteJob", %{id: id} do
      {:ok, _} = Directory.resolve(id)

      assert {:ok, :scheduled} = Tenants.delete(id)

      # Durable tombstone (directory), immediate re-mint block (ETS gate), physical erase queued.
      assert {:ok, %{status: "deleted"}} = Directory.get(id)
      assert Tenants.tombstoned?(id)
      assert_enqueued(worker: DeleteJob, args: %{shard_id: id})
    end

    test "registers + tombstones a shard the directory never recorded", %{id: id} do
      assert {:ok, :scheduled} = Tenants.delete(id)
      assert {:ok, %{status: "deleted"}} = Directory.get(id)
    end

    test "refuses an invalid id" do
      assert {:error, :invalid_shard_id} = Tenants.delete("Not Valid!")
    end
  end

  describe "purge/1 (the DeleteJob body)" do
    test "erases every stored object of an idle tenant", %{id: id} do
      write!(id, ["CREATE TABLE t (v TEXT)", "INSERT INTO t VALUES ('secret')"])
      flush!(id)
      assert :ok = Storage.retain(id, 1)
      assert {:ok, _snap} = Snapshots.create(id)

      assert :ok = Tenants.purge(id)

      assert {:ok, nil} == Storage.object_etag(id)
      assert {:ok, []} == Snapshots.list(id)
      refute File.exists?(Path.join(remote_dir(), "#{id}@1.db"))
      refute File.exists?(Path.join(remote_dir(), "#{id}.lock"))
    end

    # The regression: purging a shard that is ACTIVELY SERVED must force-stop its coordinator
    # (not graceful-drain, which can't stop a busy one). Pre-fix, the coordinator stayed alive,
    # then self-fenced on its next flush and quarantined the deleted tenant's data to a
    # `.fenced.<ts>` file on local disk — an erased tenant's rows surviving the erase.
    test "force-stops a live coordinator and leaves NO quarantine copy on disk", %{id: id} do
      # Hold an open connection with an uncommitted-to-storage write, so the coordinator is
      # busy (a graceful drain would return :busy and leave it running).
      {:ok, handle} = ShardExecutor.open(id)
      {:ok, _} = ShardExecutor.execute(handle, stmt("CREATE TABLE t (v TEXT)"))
      {:ok, _} = ShardExecutor.execute(handle, stmt("INSERT INTO t VALUES ('pii')"))

      assert :ok = Tenants.purge(id)

      # Coordinator is gone (force-stopped, not left busy) ...
      assert [] == Registry.lookup(Fathom.ShardRegistry, id)
      # ... storage is gone ...
      assert {:ok, nil} == Storage.object_etag(id)
      # ... and NO local copy survives — neither the working file nor a self-fence quarantine.
      local_base = Fathom.Shard.db_path(id)
      assert [] == Path.wildcard(local_base <> "*")
      refute Enum.any?(Path.wildcard(local_base <> "*"), &String.contains?(&1, ".fenced."))
    end

    test "is idempotent on an already-erased tenant", %{id: id} do
      assert :ok = Tenants.purge(id)
      assert :ok = Tenants.purge(id)
      assert {:ok, nil} == Storage.object_etag(id)
    end
  end

  describe "export/1" do
    test "exports the tenant's durable data as an openable SQLite file", %{id: id} do
      write!(id, ["CREATE TABLE t (v TEXT)", "INSERT INTO t VALUES ('portable')"])
      flush!(id)

      assert {:ok, %{path: path, filename: filename}} = Tenants.export(id)
      on_exit(fn -> File.rm(path) end)

      assert filename == "#{id}.db"
      assert File.exists?(path)

      # It's a real SQLite database carrying exactly this tenant's rows.
      {:ok, db} = Exqlite.Sqlite3.open(path)
      {:ok, stmt} = Exqlite.Sqlite3.prepare(db, "SELECT v FROM t")
      assert {:row, ["portable"]} = Exqlite.Sqlite3.step(db, stmt)
      :ok = Exqlite.Sqlite3.release(db, stmt)
      :ok = Exqlite.Sqlite3.close(db)
    end

    test "not_stored for a shard that was never flushed", %{id: id} do
      assert {:error, :not_stored} = Tenants.export(id)
    end

    test "refuses an invalid id" do
      assert {:error, :invalid_shard_id} = Tenants.export("Not Valid!")
    end

    # Expert review #22: a portability export must reflect the tenant's newest writes, not just the
    # last periodic flush — otherwise a departing customer's most recent data is silently missing.
    test "flush: true (the default) captures writes still buffered on the coordinator", %{id: id} do
      write!(id, ["CREATE TABLE t (v TEXT)", "INSERT INTO t VALUES ('flushed')"])
      flush!(id)
      # A newer write, left buffered on a running coordinator (not yet durably flushed).
      write!(id, ["INSERT INTO t VALUES ('buffered')"])

      # flush: false reflects only the last durable flush (the pre-#22 behavior).
      assert {:ok, %{path: stale}} = Tenants.export(id, flush: false)
      on_exit(fn -> File.rm(stale) end)
      assert export_rows(stale) == ["flushed"], "flush: false omits the buffered write"

      # The default (flush: true) force-flushes first, so the export includes the newest write.
      assert {:ok, %{path: fresh}} = Tenants.export(id)
      on_exit(fn -> File.rm(fresh) end)

      assert export_rows(fresh) == ["buffered", "flushed"],
             "the default export captures the newest writes"
    end

    # #22: a corrupt stored object must never be handed to the tenant as a "complete" export.
    test "refuses to export a corrupt stored object (integrity check)", %{id: id} do
      File.mkdir_p!(remote_dir())
      File.write!(Path.join(remote_dir(), "#{id}.db"), "definitely not a sqlite database")

      assert {:error, {:corrupt_export, _}} = Tenants.export(id, flush: false)
    end

    # #22 rides the #14 fix: a flush timeout must FAIL the export, not silently ship stale bytes.
    test "surfaces a flush timeout instead of exporting stale", %{id: id} do
      prev_timeout = Application.get_env(:fathom, :flush_now_timeout_ms)
      prev_idle = Application.get_env(:fathom, :shard_idle_ms)
      Application.put_env(:fathom, :flush_now_timeout_ms, 100)
      Application.put_env(:fathom, :shard_idle_ms, 60_000)

      on_exit(fn ->
        restore_env(:flush_now_timeout_ms, prev_timeout)
        restore_env(:shard_idle_ms, prev_idle)
      end)

      write!(id, ["CREATE TABLE t (v TEXT)", "INSERT INTO t VALUES ('x')"])
      {:ok, coordinator} = Shards.ensure(id)

      # A hung coordinator: the flush-before-export can't complete.
      :sys.suspend(coordinator)

      try do
        assert {:error, :flush_timeout} = Tenants.export(id),
               "a flush timeout must fail the export, not silently ship stale data"
      after
        :sys.resume(coordinator)
      end

      _ = Shards.drain(id, 5_000)
    end
  end

  describe "fork/1 (database forking, #14)" do
    setup %{id: id} do
      dst = "#{id}fork"

      on_exit(fn ->
        :ets.delete(Tombstones, dst)
        Shards.drain(dst, 2_000)
        Storage.purge_shard(dst)

        for path <- Path.wildcard(Path.join([Fathom.Shard.data_dir(), "#{dst}*"])),
            do: File.rm(path)
      end)

      %{dst: dst}
    end

    test "clones a live tenant to a new id, independent of the source", %{id: src, dst: dst} do
      {:ok, _} = Directory.resolve(src)
      write!(src, ["CREATE TABLE t (v TEXT)", "INSERT INTO t VALUES ('orig')"])
      flush!(src)

      assert {:ok, tenant} = Tenants.fork(src, dst)
      assert tenant.shard_id == dst
      assert tenant.url == "libsql://#{dst}.local"

      # The fork has the source's data and its own active directory row.
      assert {:ok, %{status: "active"}} = Directory.get(dst)
      assert read_one(dst, "SELECT v FROM t") == [["orig"]]

      # It's a real independent copy: writing the fork doesn't touch the source.
      write!(dst, ["INSERT INTO t VALUES ('fork-only')"])
      flush!(dst)
      assert read_one(src, "SELECT v FROM t") == [["orig"]]
      assert length(read_one(dst, "SELECT v FROM t")) == 2
    end

    test "carries the source's schema version so the laggard sweep won't re-migrate it",
         %{id: src, dst: dst} do
      {:ok, _} = Directory.resolve(src)
      {:ok, _} = Directory.cutover(src, 4)
      write!(src, ["CREATE TABLE t (v TEXT)"])
      flush!(src)

      assert {:ok, _} = Tenants.fork(src, dst)
      assert {:ok, %{schema_version: 4}} = Directory.get(dst)
    end

    test "refuses to fork onto an existing tenant", %{id: src, dst: dst} do
      {:ok, _} = Directory.resolve(src)
      write!(src, ["CREATE TABLE t (v TEXT)"])
      flush!(src)
      {:ok, _} = Directory.resolve(dst)

      assert {:error, :already_exists} = Tenants.fork(src, dst)
    end

    test "refuses a source with no stored object", %{id: src, dst: dst} do
      {:ok, _} = Directory.resolve(src)
      assert {:error, :no_source} = Tenants.fork(src, dst)
    end

    # Expert review 2026-07-18 #14: refuse_if_taken (a directory check) then an unconditional
    # Storage.fork_shard copy was a TOCTOU — a dst opened organically or by a second fork between
    # the check and the copy got clobbered, and an organic coordinator holding the dst lease (with
    # acked-but-unflushed writes) had its first flush 412 + self-fence, silently losing them. The
    # fork now holds the dst lease across the copy, so a held lease makes it refuse, not clobber.
    test "refuses a dst whose lease is held — no clobber of a concurrent open/fork (#14)",
         %{id: src, dst: dst} do
      {:ok, _} = Directory.resolve(src)
      write!(src, ["CREATE TABLE t (v TEXT)", "INSERT INTO t VALUES ('orig')"])
      flush!(src)

      # A concurrent owner (an organic coordinator on another node, or a second in-flight fork)
      # holds the dst lease; a fresh 30s TTL keeps it :live via the lock's own TTL (no heartbeat
      # needed, per the Local backend's owner_live? fallback).
      {:ok, _held} = Storage.acquire_lease(dst, "other-node@holder", 30_000)

      # The fork loses the acquire and refuses. Pre-fix (no lease guard) it copied src over dst and
      # returned {:ok, _}, clobbering the holder / self-fencing its writes.
      assert {:error, :dst_busy} = Tenants.fork(src, dst)

      # It created no dst object and no dst directory row — the fork never touched it.
      assert {:ok, nil} == Storage.object_etag(dst)
      assert Directory.get(dst) == :error
    end

    # The storage-layer guard (now atomic under the dst mutex): a dst that already has a stored
    # object but NO directory row (e.g. a prior fork that never finished registering) slips past the
    # directory-only refuse_if_taken, so Storage.fork_shard's :dst_exists must catch it.
    test "refuses when the dst already has a stored object (no clobber) (#14)",
         %{id: src, dst: dst} do
      {:ok, _} = Directory.resolve(src)
      write!(src, ["CREATE TABLE t (v TEXT)"])
      flush!(src)

      :ok = Storage.fork_shard(src, dst)
      assert {:ok, etag} = Storage.object_etag(dst)
      assert etag != nil

      assert {:error, :dst_exists} = Tenants.fork(src, dst)
    end
  end

  describe "flush/1 + fork(flush_source:) — the keystone-fork affordance (#10)" do
    setup %{id: id} do
      dst = "#{id}fork"

      on_exit(fn ->
        :ets.delete(Tombstones, dst)
        Shards.drain(dst, 2_000)
        Storage.purge_shard(dst)

        for path <- Path.wildcard(Path.join([Fathom.Shard.data_dir(), "#{dst}*"])),
            do: File.rm(path)
      end)

      %{dst: dst}
    end

    test "flush/1 makes an open shard's writes durable WITHOUT stopping it", %{id: src, dst: dst} do
      {:ok, _} = Directory.resolve(src)
      write!(src, ["CREATE TABLE t (v TEXT)", "INSERT INTO t VALUES ('a')"])

      # The coordinator is running with an UN-flushed write (no drain, no idle-drop yet).
      [{pid_before, _}] = Registry.lookup(Fathom.ShardRegistry, src)
      assert Fathom.Shard.dirty?(pid_before)

      assert :ok = Shards.flush(src)

      # Same coordinator, still running (non-disruptive) and now durably clean.
      [{pid_after, _}] = Registry.lookup(Fathom.ShardRegistry, src)
      assert pid_after == pid_before
      refute Fathom.Shard.dirty?(pid_after)

      # A plain fork (no flush_source) now carries the just-flushed write — the whole point.
      assert {:ok, _} = Tenants.fork(src, dst)
      assert read_one(dst, "SELECT v FROM t") == [["a"]]
    end

    test "fork(flush_source: true) carries writes made since the last flush",
         %{id: src, dst: dst} do
      {:ok, _} = Directory.resolve(src)
      write!(src, ["CREATE TABLE t (v TEXT)", "INSERT INTO t VALUES ('a')"])
      flush!(src)
      # 'b' is written to the re-opened coordinator and left UN-flushed.
      write!(src, ["INSERT INTO t VALUES ('b')"])

      assert {:ok, _} = Tenants.fork(src, dst, flush_source: true)
      assert read_one(dst, "SELECT v FROM t") == [["a"], ["b"]]
    end

    test "flush is a no-op :ok when no coordinator is running locally", %{id: id} do
      assert :ok = Shards.flush(id)
      assert :ok = Tenants.flush(id)
    end

    test "Tenants.flush/1 rejects an invalid id" do
      assert {:error, :invalid_shard_id} = Tenants.flush("Not Valid!")
    end
  end

  describe "suspend/1 and resume/1" do
    test "suspend denies admission (403) and resume restores service", %{id: id} do
      {:ok, _} = Directory.resolve(id)

      assert :ok = Tenants.suspend(id)
      assert Tenants.suspended?(id)
      assert {:ok, %{status: "suspended"}} = Directory.get(id)
      # New streams are refused while suspended.
      assert {:error, :shard_suspended} = Shards.checkout(id)

      assert :ok = Tenants.resume(id)
      refute Tenants.suspended?(id)
      assert {:ok, %{status: "active"}} = Directory.get(id)
      assert {:ok, _pid, _ref, _path} = Shards.checkout(id)
    end

    # THE LEVER MUST REACH A STREAM THAT IS ALREADY OPEN (expert review 2026-08-24 #5).
    #
    # The test above asserts only `Shards.checkout(id) == {:error, :shard_suspended}` — the NEW
    # stream path — and that is exactly the gap. Both denies lived in `Shards.ensure/1`, which
    # runs once per CHECKOUT, and a checkout is one Hrana stream, which a django-libsql client
    # holds for HOURS. So an attacker already holding a stream kept reading and writing after the
    # suspend: `Tenants.suspend/1` drains with a 5 s budget, discarded the result, and a stream
    # that outlived it left the coordinator serving. On a multi-node fleet it is worse — the
    # drain is a LOCAL Registry lookup, so a suspend issued on any other node never touches the
    # coordinator at all, and for a DELETED tenant its data stays readable through that stream.
    #
    # These levers exist for abusive or compromised clients, which is precisely the posture of a
    # client that already has a stream open.
    test "suspend and delete reach a stream that was ALREADY open", %{id: id} do
      {:ok, _} = Directory.resolve(id)
      {:ok, h} = ShardExecutor.open(id)

      {:ok, _} = ShardExecutor.execute(h, %Stmt{sql: "CREATE TABLE t (v TEXT)", args: []})
      {:ok, _} = ShardExecutor.execute(h, %Stmt{sql: "SELECT count(*) FROM t", args: []})

      assert :ok = Tenants.suspend(id)

      # SAME handle, same connection, no re-checkout anywhere.
      assert {:error, %Filo.Error{status: 403, code: "FILO_TENANT_SUSPENDED"}} =
               ShardExecutor.execute(h, %Stmt{sql: "SELECT count(*) FROM t", args: []}),
             "a suspended tenant kept serving reads on an already-open stream"

      assert {:error, %Filo.Error{status: 403}} =
               ShardExecutor.execute(h, %Stmt{sql: "INSERT INTO t VALUES ('x')", args: []})

      # A script is not a loophole, and neither is describe — a suspended tenant's schema is
      # still its own.
      assert {:error, %Filo.Error{status: 403}} =
               ShardExecutor.execute_sequence(h, "SELECT 1; SELECT 2")

      assert {:error, %Filo.Error{status: 403}} = ShardExecutor.describe(h, "SELECT v FROM t")

      # Resume restores service on that same open stream — the deny is a live ETS read, not a
      # latch, so it must lift as cleanly as it applied.
      assert :ok = Tenants.resume(id)
      assert {:ok, _} = ShardExecutor.execute(h, %Stmt{sql: "SELECT count(*) FROM t", args: []})

      # And deletion is the stronger case: GDPR Article 17 erasure that a held stream can read
      # through is not erasure. 410, not 403.
      Tombstones.put(id)

      assert {:error, %Filo.Error{status: 410, code: "FILO_TENANT_DELETED"}} =
               ShardExecutor.execute(h, %Stmt{sql: "SELECT v FROM t", args: []}),
             "a deleted tenant's data was still readable through an open stream"

      :ok = ShardExecutor.close(h)
    end

    test "refuses to suspend a deleted (tombstoned) tenant", %{id: id} do
      {:ok, _} = Directory.tombstone(id)
      assert {:error, :deleted} = Tenants.suspend(id)
    end

    test "refuses an unknown tenant and an invalid id", %{id: id} do
      assert {:error, :not_found} = Tenants.suspend(id)
      assert {:error, :invalid_shard_id} = Tenants.suspend("Not Valid!")
    end
  end

  describe "provision/1" do
    test "registers the tenant active and returns a libsql url + token", %{id: id} do
      assert {:ok, tenant} = Tenants.provision(id)

      assert tenant.shard_id == id
      assert tenant.url == "libsql://#{id}.local"
      assert is_binary(tenant.auth_token)
      # test config has :hrana_auth disabled — the token is informational there.
      assert tenant.auth_required == false
      assert {:ok, %{status: "active"}} = Directory.get(id)
    end

    test "refuses a tenant that already exists", %{id: id} do
      {:ok, _} = Directory.resolve(id)
      assert {:error, :already_exists} = Tenants.provision(id)
    end

    test "refuses a tombstoned (deleted) id — no resurrection via provisioning", %{id: id} do
      {:ok, _} = Directory.tombstone(id)
      assert {:error, :tombstoned} = Tenants.provision(id)
    end

    test "refuses an invalid id" do
      assert {:error, :invalid_shard_id} = Tenants.provision("Not Valid!")
    end

    # Expert review 2026-08-31 #10: with :fork_from_template ON the fork IS the birth path, so a
    # failed fork means the tenant has no schema. provision used to swallow the outcome and return
    # {:ok} — a silently broken tenant the rollout then quarantines. It now fails loudly and rolls
    # the directory row back so the id is cleanly retriable.
    test "fails loudly and rolls back when the template fork fails (fork ON)", %{id: id} do
      prev = Application.get_env(:fathom, :fork_from_template)
      Application.put_env(:fathom, :fork_from_template, true)

      on_exit(fn ->
        if prev == nil,
          do: Application.delete_env(:fathom, :fork_from_template),
          else: Application.put_env(:fathom, :fork_from_template, prev)
      end)

      # No release and no template@HEAD snapshot exist in this test, so fork_from_template cannot
      # birth the tenant at HEAD — the born-empty failure.
      assert {:error, {:fork_failed, _reason}} = Tenants.provision(id)

      assert Directory.get(id) == :error,
             "a failed fork must roll the directory row back, not leave a schema-less active@0 " <>
               "tenant that reports success"
    end
  end

  # Expert review #35: provision/fork is the one place that KNOWS the deployment's address (it
  # composes the libsql://<id>.<zone> URL), so a non-DNS-safe id (an underscore no *.<zone> cert can
  # serve) is warned about (default) or refused (when the deployment terminates wildcard TLS) — never
  # handed back as an un-servable URL discovered days later as a TLS failure.
  describe "provision/1 + fork/2 DNS safety (#35)" do
    setup do
      prev = Application.get_env(:fathom, :wildcard_tls_serving)

      on_exit(fn ->
        if prev == nil,
          do: Application.delete_env(:fathom, :wildcard_tls_serving),
          else: Application.put_env(:fathom, :wildcard_tls_serving, prev)
      end)

      :ok
    end

    test "a DNS-safe id provisions with no warnings" do
      id = "dns-safe-#{System.unique_integer([:positive])}"
      assert {:ok, tenant} = Tenants.provision(id)
      assert tenant.warnings == []
    end

    test "warn mode (default): an underscore id still provisions but carries a warning" do
      id = "dns_unsafe_#{System.unique_integer([:positive])}"
      assert {:ok, tenant} = Tenants.provision(id)
      assert [warning] = tenant.warnings
      assert warning =~ "wildcard TLS"
      assert {:ok, %{status: "active"}} = Directory.get(id), "warn mode still creates the tenant"
    end

    test "reject mode (:wildcard_tls_serving): an underscore id is refused and no row is created" do
      Application.put_env(:fathom, :wildcard_tls_serving, true)
      id = "dns_reject_#{System.unique_integer([:positive])}"

      assert {:error, :id_not_dns_safe} = Tenants.provision(id)
      assert Directory.get(id) == :error, "a refused provision must not leave a directory row"
    end

    test "reject mode: a DNS-safe id still provisions" do
      Application.put_env(:fathom, :wildcard_tls_serving, true)
      id = "dns-ok-#{System.unique_integer([:positive])}"
      assert {:ok, %{warnings: []}} = Tenants.provision(id)
    end

    test "reject mode: fork refuses a non-DNS-safe DESTINATION id (before touching the source)" do
      Application.put_env(:fathom, :wildcard_tls_serving, true)
      # The dns_safety(dst) gate fires before fetch_src, so no real source is needed to prove it.
      assert {:error, :id_not_dns_safe} =
               Tenants.fork("some-src", "fork_bad_#{System.unique_integer([:positive])}")
    end
  end

  test "the DeleteJob worker runs purge end to end", %{id: id} do
    write!(id, ["CREATE TABLE t (v TEXT)", "INSERT INTO t VALUES ('x')"])
    flush!(id)

    assert :ok = perform_job(DeleteJob, %{shard_id: id})
    assert {:ok, nil} == Storage.object_etag(id)
  end

  defp stmt(sql, args \\ []), do: %Stmt{sql: sql, args: args}

  defp write!(shard, sqls) do
    {:ok, handle} = ShardExecutor.open(shard)
    Enum.each(sqls, fn s -> {:ok, _} = ShardExecutor.execute(handle, stmt(s)) end)
    :ok = ShardExecutor.close(handle)
  end

  defp read_one(shard, sql) do
    {:ok, handle} = ShardExecutor.open(shard)
    {:ok, result} = ShardExecutor.execute(handle, stmt(sql))
    :ok = ShardExecutor.close(handle)
    result.rows
  end

  defp flush!(shard), do: :ok = Shards.drain(shard, 5_000)

  defp restore_env(key, nil), do: Application.delete_env(:fathom, key)
  defp restore_env(key, value), do: Application.put_env(:fathom, key, value)

  # Read `SELECT v FROM t ORDER BY v` from an exported SQLite file.
  defp export_rows(path) do
    {:ok, db} = Exqlite.Sqlite3.open(path)
    {:ok, stmt} = Exqlite.Sqlite3.prepare(db, "SELECT v FROM t ORDER BY v")
    rows = drain_rows(db, stmt, [])
    :ok = Exqlite.Sqlite3.release(db, stmt)
    :ok = Exqlite.Sqlite3.close(db)
    rows
  end

  defp drain_rows(db, stmt, acc) do
    case Exqlite.Sqlite3.step(db, stmt) do
      {:row, [v]} -> drain_rows(db, stmt, [v | acc])
      :done -> Enum.reverse(acc)
    end
  end

  defp remote_dir, do: Fathom.Shard.Storage.Local.dir()
end
