defmodule Fathom.ShardsRemoteFlushDirtyMarkerTest do
  @moduledoc """
  `Shards.flush/1` on a node that is not the holder fails CLOSED over a DIRTY released marker
  (expert review 2026-10-13 #7).

  ## The defect

  `RemoteFlush.flush/1` mapped `Storage.lease_holder/1 == :free` to `:ok` ("nobody is writing, the
  stored object is current"). A released marker reads as `:free` (its sentinel owner is dead by
  construction), but a marker left by a FAILED drop flush (`keep_local_release_lease/3`) means the
  releaser kept a local copy with acked-but-unflushed writes the object lacks. Export, keystone fork
  and `flush: true` snapshots from another node then read the stale object silently.

  ## What is pinned

  The failed-drop marker records `dirty: true` (`Storage.with_dirty_release/1`); `RemoteFlush`
  answers `{:error, :remote_holder_unflushed}` for it and `:ok` for no lock or a clean marker, on
  BOTH backends. The end-to-end case drives a real coordinator through a failed drop, both liveness
  modes (mode asserted).

  Not covered, by design: the replication-OFF configuration releases by DELETING the lock (no claim
  to keep), so there is no marker to carry the flag and the answer stays `:ok`. And the flag lasts
  until the next opener overwrites the marker; it is not a durable "this tenant has lost writes"
  record.
  """
  use ExUnit.Case, async: false

  import ExUnit.CaptureLog

  alias Fathom.{Shard, ShardExecutor, Shards}
  alias Fathom.Shard.{Heartbeat, Storage}
  alias Fathom.Shard.Storage.S3
  alias Fathom.Shards.RemoteFlush
  alias Fathom.Test.S3EtagStore
  alias Filo.Stmt

  @modes [:legacy, :heartbeat]

  setup do
    shard = "rfdirty_#{System.unique_integer([:positive])}"

    for k <- [:shard_storage, S3, :replication_enabled] do
      prev = Application.get_env(:fathom, k)

      on_exit(fn ->
        if is_nil(prev),
          do: Application.delete_env(:fathom, k),
          else: Application.put_env(:fathom, k, prev)
      end)
    end

    on_exit(fn ->
      Application.delete_env(:fathom, :storage_fault)
      Shards.drain(shard, 2_000)

      for dir <- [Shard.data_dir(), Storage.Local.dir()],
          p <- Path.wildcard(Path.join(dir, "#{shard}*")),
          do: File.rm_rf(p)
    end)

    %{shard: shard}
  end

  defp dirty_marker, do: %{epoch: 3} |> Storage.with_dirty_release() |> Storage.released_marker(5)
  defp clean_marker, do: Storage.released_marker(%{epoch: 3}, 5)

  describe "the marker encoding" do
    test "dirty round-trips; a clean marker's body is byte-identical to before" do
      assert {:ok, %{dirty: true, claim: 5}} =
               dirty_marker() |> Storage.encode_lease() |> Storage.decode_lease()

      clean = Storage.encode_lease(clean_marker())
      refute clean =~ "dirty"
      assert {:ok, lease} = Storage.decode_lease(clean)
      refute Map.has_key?(lease, :dirty)
    end
  end

  describe "Local backend" do
    setup %{shard: shard} do
      Application.put_env(:fathom, :shard_storage, Storage.Local)
      %{lock: Path.join(Storage.Local.dir(), "#{shard}.lock")}
    end

    test "no lock is :ok", %{shard: shard} do
      assert RemoteFlush.flush(shard) == :ok
    end

    test "a clean released marker is :ok", %{shard: shard, lock: lock} do
      File.write!(lock, Storage.encode_lease(clean_marker()))
      assert Storage.lease_holder(shard) == :free
      assert RemoteFlush.flush(shard) == :ok
    end

    test "a DIRTY released marker is not :ok", %{shard: shard, lock: lock} do
      File.write!(lock, Storage.encode_lease(dirty_marker()))

      assert Storage.lease_holder(shard) == :free,
             "the marker must still read as free to every other caller"

      assert RemoteFlush.flush(shard) == {:error, :remote_holder_unflushed}
    end
  end

  describe "S3 backend" do
    setup %{shard: shard} do
      Application.put_env(:fathom, :shard_storage, S3)
      store = start_supervised!({Agent, fn -> S3EtagStore.initial(%{}) end})

      Application.put_env(:fathom, S3,
        bucket: "b",
        region: "us-east-1",
        access_key_id: "k",
        secret_access_key: "s",
        endpoint: "https://s3.example",
        path_style: true,
        req_plug: fn conn -> S3EtagStore.serve(conn, store) end
      )

      %{store: store, key: "#{shard}.lock"}
    end

    defp put_lock!(store, key, lease) do
      body = Storage.encode_lease(lease)
      Agent.update(store, &put_in(&1, [:objects, key], %{body: body, form: :single, meta: %{}}))
    end

    test "no lock is :ok", %{shard: shard} do
      assert RemoteFlush.flush(shard) == :ok
    end

    test "a clean released marker is :ok", %{shard: shard, store: store, key: key} do
      put_lock!(store, key, clean_marker())
      assert Storage.lease_holder(shard) == :free
      assert RemoteFlush.flush(shard) == :ok
    end

    test "a DIRTY released marker is not :ok", %{shard: shard, store: store, key: key} do
      put_lock!(store, key, dirty_marker())
      assert Storage.lease_holder(shard) == :free
      assert RemoteFlush.flush(shard) == {:error, :remote_holder_unflushed}
    end
  end

  describe "end to end: a coordinator whose drop flush FAILS" do
    for mode <- @modes do
      test "#{mode} mode: leaves a dirty marker that RemoteFlush refuses", %{shard: shard} do
        Application.put_env(:fathom, :shard_storage, Fathom.Test.FaultyStorage)

        case unquote(mode) do
          :legacy ->
            refute Heartbeat.running?()

          :heartbeat ->
            _ = :sys.get_state(start_supervised!({Heartbeat, ttl_ms: 30_000}))
        end

        # Open while replicating (an integer lineage is what makes the release leave a marker).
        Application.put_env(:fathom, :replication_enabled, true)
        {:ok, a} = Shards.ensure(shard)

        if unquote(mode) == :heartbeat,
          do: assert(is_integer(:sys.get_state(a).acquire_gen)),
          else: assert(is_nil(:sys.get_state(a).acquire_gen))

        Application.put_env(:fathom, :replication_enabled, false)
        {:ok, conn} = ShardExecutor.open(shard)
        {:ok, _} = ShardExecutor.execute(conn, %Stmt{sql: "CREATE TABLE kv (v TEXT)", args: []})

        {:ok, _} =
          ShardExecutor.execute(conn, %Stmt{sql: "INSERT INTO kv VALUES ('x')", args: []})

        :ok = ShardExecutor.close(conn)
        assert Shard.dirty?(a), "the fixture must be dirty or the drop takes the clean path"

        Application.put_env(:fathom, :storage_fault, :flush)
        ref = Process.monitor(a)

        capture_log(fn ->
          Shards.drain(shard, 5_000)
          assert_receive {:DOWN, ^ref, :process, ^a, _}, 5_000
        end)

        body = Path.join(Storage.Local.dir(), "#{shard}.lock") |> File.read!() |> Jason.decode!()
        assert body["owner"] == Storage.released_owner()
        assert body["dirty"] == true, "a failed drop flush must mark its release dirty"

        Application.delete_env(:fathom, :storage_fault)
        assert RemoteFlush.flush(shard) == {:error, :remote_holder_unflushed}
      end
    end
  end
end
