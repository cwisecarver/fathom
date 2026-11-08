defmodule Fathom.Shard.ColdAbsentTombstoneTest do
  @moduledoc """
  Expert review 2026-11-07 #14, maintainer decision (d) of 2026-11-08: a COLD open that finds no local
  copy AND no stored object consults the durable `tombstones/<id>` marker before it mints a shard.

  `erased_open_gate_test` pins the open that holds a local copy (`Shard.object_gone/3`). The residue
  was the node that holds nothing: it booted with the directory half of the `Tombstones` gate not
  loaded yet (the ETS set is the part that was NOT loaded — these tests never populate it), a request
  for an erased id met no object, and the open minted a fresh EMPTY shard for it (the 410 contract
  broken; the empty shard's flush creates objects the erasure sweep must re-drive).
  `Shard.cold_absent_gate/4` asks the marker on exactly that branch:

    * marker present ⇒ refused `:shard_tombstoned` (the 410 `ShardExecutor.open_error/1` maps), and
      nothing is created in storage or left on disk;
    * marker unreadable ⇒ refused RETRYABLY (`{:tombstone_check_failed, _}` → 503), NOT fail-open: an
      erased id must not be re-created because storage hiccuped;
    * marker absent ⇒ a novel shard opens as before, having paid exactly ONE marker read.

  The read is ONLY on this path: an open that finds the object pays none (counted below, and
  `S3OpCountTest` pins the request counts per lifecycle event for the S3 backend).

  Both liveness modes, mode asserted (on the control shard that does open: a refused open has no
  coordinator to read `acquire_gen` from). Backend: `Fathom.Test.FaultyStorage` over `Storage.Local`,
  whose `{:tombstone_read, id}` fault makes the marker read of ONE id fail.
  """
  use ExUnit.Case, async: false

  @moduletag :capture_log

  alias Fathom.{Shard, ShardExecutor, Shards}
  alias Fathom.Shard.{Connection, Heartbeat}
  alias Fathom.Shard.Storage.Local
  alias Fathom.Tenants.Tombstones

  @env [
    :shard_storage,
    :faulty_before,
    :storage_fault,
    :replication_enabled,
    :shard_idle_ms,
    :shard_flush_interval_ms
  ]

  setup do
    prev = Map.new(@env, &{&1, Application.get_env(:fathom, &1)})

    ids =
      for tag <- ~w(erased novel other),
          do: "coldabs_#{tag}_#{System.unique_integer([:positive])}"

    on_exit(fn ->
      Application.delete_env(:fathom, :faulty_before)
      Application.delete_env(:fathom, :storage_fault)

      for id <- ids do
        Shards.drain(id, 2_000)
        Local.purge_shard(id)
        File.rm(Path.join([Local.dir(), "tombstones", id]))
        for f <- Path.wildcard(Path.join(Shard.data_dir(), "#{id}*")), do: File.rm_rf(f)
      end

      for {k, v} <- prev do
        if is_nil(v),
          do: Application.delete_env(:fathom, k),
          else: Application.put_env(:fathom, k, v)
      end
    end)

    Application.put_env(:fathom, :shard_storage, Fathom.Test.FaultyStorage)
    Application.put_env(:fathom, :shard_idle_ms, 600_000)
    Application.put_env(:fathom, :shard_flush_interval_ms, 600_000)
    Application.put_env(:fathom, :replication_enabled, false)

    [erased, novel, other] = ids
    %{erased: erased, novel: novel, other: other}
  end

  defp set_mode!(:legacy) do
    refute Heartbeat.running?(), "legacy mode needs the heartbeat OFF"
    :ok
  end

  defp set_mode!(:heartbeat) do
    hb = start_supervised!({Heartbeat, ttl_ms: 30_000})
    _ = :sys.get_state(hb)
    assert Heartbeat.running?()
    :ok
  end

  # Opens `id` as a CONTROL and asserts the coordinator took the mode the test names.
  defp assert_mode_took!(id, mode) do
    {:ok, pid, ref, _path} = Shards.checkout(id)
    gen = :sys.get_state(pid).acquire_gen

    if mode == :heartbeat,
      do: assert(is_integer(gen), "heartbeat mode did not take"),
      else: assert(is_nil(gen), "legacy mode did not take")

    Shard.checkin(pid, ref)
  end

  # What the DeleteJob leaves for an id this node never held: the durable marker, no object, and
  # (the premise) a `Tombstones` set that has not loaded it.
  defp erase_remote!(id) do
    assert :ok = Local.put_tombstone(id)
    assert {:ok, false} = Local.any_shard_object?(id), "fixture: nothing may be stored"

    refute Tombstones.tombstoned?(id),
           "fixture: the in-memory gate is the part that is not loaded"

    refute File.exists?(Shard.db_path(id)), "fixture: this node holds no local copy"
  end

  # Counts the marker reads of ONE id: `:faulty_before` is global app env, so the hook is scoped to
  # the id the test owns and ignores every other shard's reads.
  defp count_marker_reads!(id) do
    test_pid = self()

    Application.put_env(
      :fathom,
      :faulty_before,
      {:tombstone_present,
       fn
         ^id -> send(test_pid, {:marker_read, id})
         _ -> :ok
       end}
    )
  end

  defp marker_reads(id, acc \\ 0) do
    receive do
      {:marker_read, ^id} -> marker_reads(id, acc + 1)
    after
      0 -> acc
    end
  end

  defp put_object!(id) do
    tmp = Path.join(System.tmp_dir!(), "coldabs_seed_#{System.unique_integer([:positive])}.db")
    {:ok, c} = Connection.open(tmp)
    :ok = Connection.exec(c, "CREATE TABLE kv (v TEXT)")
    :ok = Connection.exec(c, "INSERT INTO kv VALUES ('x')")
    :ok = Connection.exec(c, "PRAGMA wal_checkpoint(TRUNCATE)")
    Connection.close(c)
    :ok = Local.flush(id, tmp)
    for s <- ["", "-wal", "-shm"], do: File.rm(tmp <> s)
    assert {:ok, true} = Local.any_shard_object?(id)
  end

  defp assert_nothing_left!(id) do
    assert {:ok, false} = Local.any_shard_object?(id),
           "the refused open created an object or lock"

    for s <- ["", "-wal", "-shm", ".etag", ".pull"] do
      refute File.exists?(Shard.db_path(id) <> s), "the refused open left #{s} on disk"
    end
  end

  for mode <- [:legacy, :heartbeat] do
    describe "#{mode}: a cold open with no local copy and no stored object" do
      setup do: set_mode!(unquote(mode))

      test "an erased id (marker, directory half not loaded) is refused 410 and nothing is created",
           %{erased: id, other: control} do
        erase_remote!(id)
        count_marker_reads!(id)

        assert {:error, :shard_tombstoned} = Shards.checkout(id)

        assert %{status: 410, code: "FILO_TENANT_DELETED"} =
                 ShardExecutor.open_error(:shard_tombstoned)

        assert marker_reads(id) == 1, "the refusal should cost exactly one marker read"
        assert_nothing_left!(id)

        # A flush after the refusal has nothing to re-create either (no coordinator exists).
        assert :ok = Shards.flush(id)
        assert_nothing_left!(id)

        assert_mode_took!(control, unquote(mode))
      end

      test "an unreadable marker is a RETRYABLE refusal: not fail-open, nothing is created",
           %{erased: id, other: control} do
        erase_remote!(id)
        Application.put_env(:fathom, :storage_fault, {:tombstone_read, id})

        assert {:error, {:pull_failed, {:tombstone_check_failed, :s3_unreachable}} = reason} =
                 Shards.checkout(id)

        assert %{status: status} = ShardExecutor.open_error(reason)
        assert status in [502, 503], "an unreadable marker must be retryable, got #{status}"
        assert_nothing_left!(id)

        # Storage heals: the id is still erased, so the retry is the 410 (it was never opened).
        Application.delete_env(:fathom, :storage_fault)
        assert {:error, :shard_tombstoned} = Shards.checkout(id)
        assert_nothing_left!(id)

        assert_mode_took!(control, unquote(mode))
      end

      test "a novel id (no marker) opens normally and the marker read happens once",
           %{novel: id} do
        count_marker_reads!(id)

        assert_mode_took!(id, unquote(mode))
        assert marker_reads(id) == 1

        # The live coordinator serves later checkouts without another open or another read.
        {:ok, pid, ref, _} = Shards.checkout(id)
        Shard.checkin(pid, ref)
        assert marker_reads(id) == 0
      end

      test "an open that FINDS the object pays no marker read", %{novel: id} do
        put_object!(id)
        count_marker_reads!(id)

        assert_mode_took!(id, unquote(mode))
        assert marker_reads(id) == 0, "an object-present open must not read the marker"
      end

      test "a marker is per shard: an erased neighbour does not refuse a novel id",
           %{erased: erased, novel: novel} do
        erase_remote!(erased)

        assert_mode_took!(novel, unquote(mode))
        assert {:error, :shard_tombstoned} = Shards.checkout(erased)

        # ...and the novel shard's own coordinator is the one serving it.
        {:ok, pid, ref, path} = Shards.checkout(novel)
        assert path == Shard.db_path(novel)
        refute path == Shard.db_path(erased)
        Shard.checkin(pid, ref)
      end
    end
  end
end
