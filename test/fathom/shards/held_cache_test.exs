defmodule Fathom.Shards.HeldCacheTest do
  @moduledoc """
  Expert review 2026-10-14 N4 (bytes half): while a dead owner's lease is still held, every client
  retry started a coordinator whose overlapped cold pull was cancelled by the held acquire, wasting
  the bytes already downloaded. `Fathom.Shards.HeldCache` replays a recent held answer for a short
  window so those retries make ZERO Storage calls and see the same error.

  Both liveness modes run (`Heartbeat.running?/0` asserted): a foreign lock whose TTL is fresh is
  live in each. The coordinator of a refused open is gone by the time the call returns, so the mode
  is asserted on the process-level switch rather than on `acquire_gen`.
  """
  use ExUnit.Case, async: false

  alias Fathom.Shard.Connection
  alias Fathom.Shard.Heartbeat
  alias Fathom.Shard.Storage
  alias Fathom.Shard.Storage.S3
  alias Fathom.Shards
  alias Fathom.Shards.HeldCache
  alias Fathom.Test.S3EtagStore

  @env [:shard_storage, S3, :shard_idle_ms, :held_refusal_cache_ms, :crash_failover_hold_ms]

  setup do
    id = "heldcache_#{System.unique_integer([:positive])}"
    prev = Map.new(@env, &{&1, Application.get_env(:fathom, &1)})

    on_exit(fn ->
      Shards.stop(id)
      HeldCache.clear(id)

      for {k, v} <- prev do
        if is_nil(v),
          do: Application.delete_env(:fathom, k),
          else: Application.put_env(:fathom, k, v)
      end

      for f <- Path.wildcard(Fathom.Shard.db_path(id) <> "*"), do: File.rm_rf(f)
    end)

    test_pid = self()
    base = build_db!(["base"])
    store = start_supervised!({Agent, fn -> S3EtagStore.initial(%{"#{id}.db" => base}) end})
    put_lock(store, id, foreign_lock(60_000))

    Application.put_env(:fathom, S3,
      bucket: "b",
      region: "us-east-1",
      access_key_id: "k",
      secret_access_key: "s",
      endpoint: "https://s3.example",
      path_style: true,
      req_plug: fn conn ->
        send(test_pid, :s3_call)
        S3EtagStore.serve(conn, store)
      end
    )

    Application.put_env(:fathom, :shard_storage, S3)
    Application.put_env(:fathom, :shard_idle_ms, 600_000)
    Application.put_env(:fathom, :held_refusal_cache_ms, 600)
    Application.put_env(:fathom, :crash_failover_hold_ms, 0)
    %{id: id, store: store}
  end

  defp set_mode!(:legacy) do
    refute Heartbeat.running?()
    :ok
  end

  defp set_mode!(:heartbeat) do
    hb = start_supervised!({Heartbeat, ttl_ms: 30_000})
    _ = :sys.get_state(hb)
    assert Heartbeat.running?()
    :ok
  end

  for mode <- [:legacy, :heartbeat] do
    describe "#{mode} mode" do
      setup do: set_mode!(unquote(mode))

      # Symptom: each retry against a held lease re-paid a cancelled cold pull.
      test "retries inside the window make zero Storage calls and return the same error", %{
        id: id
      } do
        assert {:error, {:shard_held, "dead@node", at}} = Shards.checkout(id)
        assert is_integer(at)
        assert_received :s3_call, "the first attempt must have reached the object store"
        flush_messages()

        for _ <- 1..3, do: assert({:error, {:shard_held, "dead@node", ^at}} = Shards.checkout(id))
        refute_received :s3_call

        # Same retryable status a real held acquire maps to.
        assert %{status: 503} = Fathom.ShardExecutor.open_error({:shard_held, "dead@node", at})
      end

      test "after the window the attempt goes through to Storage again", %{id: id} do
        assert {:error, {:shard_held, _, _}} = Shards.checkout(id)
        flush_messages()
        await_expiry(id)

        assert {:error, {:shard_held, _, _}} = Shards.checkout(id)
        assert_received :s3_call
      end

      test "ceiling 0 disables the cache", %{id: id} do
        Application.put_env(:fathom, :held_refusal_cache_ms, 0)
        assert {:error, {:shard_held, _, _}} = Shards.checkout(id)
        flush_messages()

        assert {:error, {:shard_held, _, _}} = Shards.checkout(id)
        assert_received :s3_call
      end

      test "a bare ensure/1 is never answered from the cache and clears it", %{id: id} do
        assert {:error, {:shard_held, _, _}} = Shards.checkout(id)
        assert HeldCache.recent(id) != nil
        flush_messages()

        # A checkout would be refused from the cache with zero calls; a bare ensure starts a
        # coordinator, which goes to Storage.
        assert {:ok, pid} = Shards.ensure(id)
        ref = Process.monitor(pid)
        assert_receive :s3_call, 2_000
        assert_receive {:DOWN, ^ref, :process, ^pid, _}, 5_000
        assert HeldCache.recent(id) == nil
        Shards.stop(id)
      end

      # A holder that releases early stays refused for at most the ceiling.
      test "an early release is bounded by the ceiling", %{id: id, store: store} do
        assert {:error, {:shard_held, _, _}} = Shards.checkout(id)

        marker = Storage.encode_lease(Storage.released_marker(%{epoch: 7}, 5))
        put_lock(store, id, marker)

        assert {:error, {:shard_held, _, _}} = Shards.checkout(id),
               "inside the window the replay is still served"

        await_expiry(id)
        assert {:ok, pid, ref, _path} = Shards.checkout(id)
        Fathom.Shard.checkin(pid, ref)
        Shards.stop(id)
      end
    end
  end

  describe "HeldCache" do
    test "only an integer, future stealable_at is cached; the window is capped by the ceiling",
         %{id: id} do
      now = System.system_time(:millisecond)
      HeldCache.put(id, "o", nil)
      assert HeldCache.recent(id) == nil
      HeldCache.put(id, "o", now - 10)
      assert HeldCache.recent(id) == nil

      Application.put_env(:fathom, :held_refusal_cache_ms, 60)
      HeldCache.put(id, "o", now + 600_000)
      assert {:shard_held, "o", _} = HeldCache.recent(id)
      await_expiry(id)

      HeldCache.put(id, "o", now + 600_000)
      HeldCache.clear(id)
      assert HeldCache.recent(id) == nil
    end
  end

  defp foreign_lock(expires_in_ms) do
    Storage.encode_lease(%{
      owner: "dead@node",
      epoch: 7,
      expires_at_ms: System.system_time(:millisecond) + expires_in_ms
    })
  end

  defp put_lock(store, id, body), do: S3EtagStore.set_body(store, "#{id}.lock", body)

  defp await_expiry(id, tries \\ 100) do
    cond do
      HeldCache.recent(id) == nil ->
        :ok

      tries == 0 ->
        flunk("held cache entry never expired")

      true ->
        Process.sleep(20)
        await_expiry(id, tries - 1)
    end
  end

  defp flush_messages do
    receive do
      :s3_call -> flush_messages()
    after
      0 -> :ok
    end
  end

  defp build_db!(rows) do
    path = Path.join(System.tmp_dir!(), "hc_#{System.unique_integer([:positive])}.db")
    {:ok, c} = Connection.open(path)
    :ok = Connection.exec(c, "CREATE TABLE kv (v TEXT)")
    for r <- rows, do: :ok = Connection.exec(c, "INSERT INTO kv VALUES ('#{r}')")
    :ok = Connection.exec(c, "PRAGMA wal_checkpoint(TRUNCATE)")
    Connection.close(c)
    bytes = File.read!(path)
    for s <- ["", "-wal", "-shm"], do: File.rm(path <> s)
    bytes
  end
end
