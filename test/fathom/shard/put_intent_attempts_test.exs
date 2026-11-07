defmodule Fathom.Shard.PutIntentAttemptsTest do
  @moduledoc """
  Expert review 2026-11-07 #23: a landed-but-unacknowledged flush must still be recognised as
  THIS node's own after more failed attempts than the sidecar has slots for.

  ## The defect

  Flush A records its PUT intent (md5) in the provenance sidecar BEFORE the PUT. The PUT lands
  (E0 → EA) but the reply is lost. The sidecar keeps only TWO intents, so flushes B and C — each
  recording its own intent and then failing before reaching the store — push A's md5 out of the
  record. Flush D then 412s (its fence etag is still E0); `Fork.put_origin/2` HEADs the object, finds
  md5(A) in NEITHER sidecar slot and answers `:fork`; the coordinator self-fences
  (`{:shutdown, :lease_lost}`), `quarantine_fenced!` renames the `.db` away with every write acked
  since A's snapshot, and the tenant is served the older object. The moduledoc's "an unresolved
  intent is never dropped by a retry" was false for three attempts, and `provenance_intent_test.exs`
  pinned the defect ("a third drops the oldest").

  ## The fix under test

  The coordinator ALSO remembers the md5s its flush attempts recorded (the flush task `send`s
  `{:put_intent, md5}` from its `:pre_put` hook; the set is cleared when a flush succeeds or
  reconciles, exactly when the sidecar's intents are) and `put_origin/3` accepts an md5 in the
  sidecar OR that set. The sidecar stays authoritative across a crash and keeps its two slots — NO
  format change, because every existing 177-byte sidecar would otherwise read `:corrupt` on warm
  open (the review's option (a)).

  A fork is still a fork: the last test pins that an object whose md5 is in NEITHER set quarantines.

  Both liveness modes, mode asserted. Not async: global storage-fault env.
  """
  use ExUnit.Case, async: false

  import ExUnit.CaptureLog

  alias Fathom.{Shard, ShardExecutor, Shards}
  alias Fathom.Shard.{Fork, Heartbeat, Provenance, Storage}
  alias Fathom.Shard.Storage.Local
  alias Filo.Stmt

  @modes [:legacy, :heartbeat]

  @env [
    :shard_storage,
    :storage_fault,
    :shard_idle_ms,
    :shard_flush_interval_ms,
    :replication_enabled,
    :replication_promote_on_open
  ]

  setup do
    shard = "intents_#{System.unique_integer([:positive])}"
    prev = Map.new(@env, &{&1, Application.get_env(:fathom, &1)})

    on_exit(fn ->
      Application.delete_env(:fathom, :storage_fault)
      capture_log(fn -> Shards.drain(shard, 2_000) end)

      for {k, v} <- prev do
        if is_nil(v),
          do: Application.delete_env(:fathom, k),
          else: Application.put_env(:fathom, k, v)
      end

      for f <- Path.wildcard(Path.join(Shard.data_dir(), "#{shard}.db*")), do: File.rm_rf(f)
      Local.purge_shard(shard)
    end)

    Application.put_env(:fathom, :shard_storage, Fathom.Test.FaultyStorage)
    # Every flush here is driven by hand: pin both timers high (the idle-stop trap).
    Application.put_env(:fathom, :shard_idle_ms, 600_000)
    Application.put_env(:fathom, :shard_flush_interval_ms, 600_000)
    Application.put_env(:fathom, :replication_enabled, false)
    Application.put_env(:fathom, :replication_promote_on_open, false)

    %{shard: shard, db: Shard.db_path(shard)}
  end

  defp set_mode!(:legacy), do: refute(Heartbeat.running?(), "legacy mode needs the heartbeat OFF")

  defp set_mode!(:heartbeat) do
    hb = start_supervised!({Heartbeat, ttl_ms: 30_000})
    _ = :sys.get_state(hb)
    assert Heartbeat.running?()
  end

  defp assert_mode!(pid, :heartbeat),
    do: assert(is_integer(:sys.get_state(pid).acquire_gen), "heartbeat mode did not take")

  defp assert_mode!(pid, :legacy),
    do: assert(is_nil(:sys.get_state(pid).acquire_gen), "legacy mode did not take")

  defp stmt(sql), do: %Stmt{sql: sql, args: []}

  defp exec!(conn, sql), do: {:ok, _} = ShardExecutor.execute(conn, stmt(sql))

  # Open the shard, give it one durable flush (so a real stored object and a real fence etag exist),
  # and return `{conn, pid, e0}`.
  defp open_with_object!(shard, mode) do
    {:ok, conn} = ShardExecutor.open(shard)
    {:ok, pid} = Shards.ensure(shard)
    assert_mode!(pid, mode)
    exec!(conn, "CREATE TABLE kv (v TEXT)")
    exec!(conn, "INSERT INTO kv VALUES ('base')")
    assert :ok = Shards.flush(shard)
    {:ok, e0} = Local.object_etag(shard)
    {conn, pid, e0}
  end

  # Attempt A: the PUT lands server-side and the reply is lost. Returns `{etag, md5}` of the object.
  defp attempt_a_lands_reply_lost!(shard, conn, e0) do
    Application.put_env(:fathom, :storage_fault, :flush_lands_then_errors)
    exec!(conn, "INSERT INTO kv VALUES ('a')")
    assert {:error, _} = capture_flush(shard)

    assert {:ok, %{etag: ea, md5: ma}} = Storage.object_head(shard)
    assert ea != e0, "fixture: attempt A's PUT must have LANDED"
    {ea, ma}
  end

  # Attempts B and C: each records its intent, then fails before reaching the store.
  defp attempts_b_c_fail_after_recording!(shard, conn, ma) do
    Application.put_env(:fathom, :storage_fault, :flush_fails_after_pre_put)

    for v <- ["b", "c"] do
      exec!(conn, "INSERT INTO kv VALUES ('#{v}')")
      assert {:error, _} = capture_flush(shard)
    end

    Application.delete_env(:fathom, :storage_fault)

    sidecar = Provenance.read_intents(Shard.db_path(shard))

    assert length(sidecar) == 2 and ma not in sidecar,
           "fixture: A's intent must have been pushed out of the sidecar's two slots " <>
             "(sidecar #{inspect(sidecar)}, A #{ma})"
  end

  # A flush that is EXPECTED to fail logs warnings (and the 3rd consecutive one an error); keep them
  # out of the test output.
  defp capture_flush(shard) do
    {result, _log} = with_log(fn -> Shards.flush(shard) end)
    result
  end

  # Read the stored object back: a local file is not durability.
  defp stored_rows(shard) do
    tmp = Path.join(System.tmp_dir!(), "intents_verify_#{System.unique_integer([:positive])}.db")
    assert {:ok, _} = Storage.pull(shard, tmp)
    {:ok, c} = Fathom.Shard.Connection.open(tmp)
    {:ok, %{rows: rows}} = Fathom.Shard.Connection.query(c, "SELECT v FROM kv ORDER BY v", [])
    :ok = Fathom.Shard.Connection.close(c)
    for s <- ["", "-wal", "-shm"], do: File.rm(tmp <> s)
    List.flatten(rows)
  end

  defp fenced_files(db), do: Path.wildcard(db <> ".fenced.*") ++ Path.wildcard(db <> ".forked.*")

  for mode <- @modes do
    describe "#{mode} mode" do
      setup do
        set_mode!(unquote(mode))
        :ok
      end

      test "periodic flush: A lands unacked, B and C fail, D's 412 is still recognised as OWN",
           %{shard: shard, db: db} do
        {conn, pid, e0} = open_with_object!(shard, unquote(mode))
        {ea, ma} = attempt_a_lands_reply_lost!(shard, conn, e0)
        attempts_b_c_fail_after_recording!(shard, conn, ma)
        ref = Process.monitor(pid)

        # D: healthy, fenced on E0 — the object is at EA, so it 412s into the reconcile.
        exec!(conn, "INSERT INTO kv VALUES ('d')")

        {result, _log} = with_log(fn -> Shards.flush(shard) end)

        refute_received {:DOWN, ^ref, :process, ^pid, _},
                        "the coordinator self-fenced its own landed flush as a FORK"

        assert fenced_files(db) == [],
               "the healthy owner quarantined its own acked writes: #{inspect(fenced_files(db))}"

        assert result == :ok
        assert Process.alive?(pid)

        # Every acked row is durable, and the fence follows the object (it moved past EA when the
        # re-kicked flush landed D's snapshot).
        {:ok, final} = Local.object_etag(shard)
        assert :sys.get_state(pid).etag == final
        assert final not in [e0, ea], "the follow-up flush never landed"
        assert stored_rows(shard) == ["a", "b", "base", "c", "d"]

        assert Map.get(:sys.get_state(pid), :put_intents, []) == [],
               "the attempted-intent set must clear once a flush succeeds"
      end

      test "drop flush: the same history, then an idle drop whose 412 must also be recognised",
           %{shard: shard, db: db} do
        {conn, pid, e0} = open_with_object!(shard, unquote(mode))
        {_ea, ma} = attempt_a_lands_reply_lost!(shard, conn, e0)
        attempts_b_c_fail_after_recording!(shard, conn, ma)
        exec!(conn, "INSERT INTO kv VALUES ('d')")
        :ok = ShardExecutor.close(conn)

        ref = Process.monitor(pid)
        {_, _log} = with_log(fn -> Shards.drain(shard, 10_000) end)
        assert_receive {:DOWN, ^ref, :process, ^pid, _}, 10_000

        assert fenced_files(db) == [],
               "the drop flush read its own landed PUT as a fork and quarantined the copy"

        # The drop uploaded everything (the retry re-fenced onto the object and PUT the full db).
        assert stored_rows(shard) == ["a", "b", "base", "c", "d"]
      end

      test "an object whose md5 is in NEITHER the sidecar nor the attempted set is still a FORK",
           %{shard: shard, db: db} do
        {conn, pid, e0} = open_with_object!(shard, unquote(mode))
        exec!(conn, "INSERT INTO kv VALUES ('mine')")

        # A PEER writes the object (different bytes) behind our back: nothing of ours matches it.
        peer =
          Path.join(System.tmp_dir!(), "intents_peer_#{System.unique_integer([:positive])}.db")

        {:ok, pc} = Fathom.Shard.Connection.open(peer)
        :ok = Fathom.Shard.Connection.exec(pc, "CREATE TABLE kv (v TEXT)")
        :ok = Fathom.Shard.Connection.exec(pc, "INSERT INTO kv VALUES ('peer')")
        :ok = Fathom.Shard.Connection.exec(pc, "PRAGMA wal_checkpoint(TRUNCATE)")
        Fathom.Shard.Connection.close(pc)
        :ok = Local.flush(shard, peer)
        for s <- ["", "-wal", "-shm"], do: File.rm(peer <> s)
        assert {:ok, e_peer} = Local.object_etag(shard)
        assert e_peer != e0

        ref = Process.monitor(pid)

        capture_log(fn ->
          Shards.flush(shard)
          assert_receive {:DOWN, ^ref, :process, ^pid, {:shutdown, :lease_lost}}, 5_000
        end)

        assert fenced_files(db) != [], "a genuine fork must still be quarantined"
      end
    end
  end

  test "put_origin/3 accepts an md5 from the caller's attempted set; /2 does not", %{shard: shard} do
    path = Shard.db_path(shard)
    src = Path.join(System.tmp_dir!(), "intents_src_#{System.unique_integer([:positive])}.db")
    {:ok, c} = Fathom.Shard.Connection.open(src)
    :ok = Fathom.Shard.Connection.exec(c, "CREATE TABLE kv (v TEXT)")
    :ok = Fathom.Shard.Connection.exec(c, "PRAGMA wal_checkpoint(TRUNCATE)")
    Fathom.Shard.Connection.close(c)
    :ok = Local.flush(shard, src)
    for s <- ["", "-wal", "-shm"], do: File.rm(src <> s)
    assert {:ok, %{etag: etag, md5: md5}} = Storage.object_head(shard)

    File.mkdir_p!(Path.dirname(path))
    :ok = Provenance.write_durable(path, "E0")

    for m <- [String.duplicate("1", 32), String.duplicate("2", 32), String.duplicate("3", 32)],
        do: Provenance.record_intent(path, m)

    assert length(Provenance.read_intents(path)) == 2
    assert Fork.put_origin(shard, path) == :fork
    assert Fork.put_origin(shard, path, [String.duplicate("9", 32)]) == :fork
    assert Fork.put_origin(shard, path, [md5]) == {:own, etag}
  end
end
