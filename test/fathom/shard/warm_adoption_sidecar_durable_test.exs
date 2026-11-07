defmodule Fathom.Shard.WarmAdoptionSidecarDurableTest do
  @moduledoc """
  Expert review 2026-11-07 #28(b): a WARM takeover that adopts the post-touch etag must record it
  DURABLY.

  `revalidate_touched/5`'s warm branch adopts the steal-touch's output as the local copy's
  provenance with the open path's plain `write/2` — `File.write` (open + `O_TRUNC` + write, no
  fsync) — replacing the previous DURABLE record while the file is dirty from the previous life.
  The shard's first-write `make_durable/1` runs only on `:became_dirty`, so a read-only reopen kept
  the downgraded sidecar until the next flush tick, and an OS crash in that window left an
  empty/torn one: `:corrupt` → a self-inflicted false fork that quarantines acked writes existing
  only locally. The four warm-adoption writes now go through `write_durable/2`
  (`Provenance`'s `:write_durable` test-hook stage marks it), and a failed write fails the open.

  Driven over the S3 steal-touch double (`S3EtagStore`), like `RevalidateTouchedTest` B1, which this
  extends: the warm copy's sidecar equals the store's pre-touch etag, a dead lock forces the steal.
  A cold takeover (nothing acked locally) keeps the cheap plain write — the +24-28% `cold_open_p50`
  that motivated it — and a test pins that too.
  """
  use ExUnit.Case, async: false

  import ExUnit.CaptureLog

  alias Fathom.{ShardExecutor, Shards}
  alias Fathom.Shard.{Connection, Provenance, Storage}
  alias Fathom.Shard.Storage.S3
  alias Fathom.Test.S3EtagStore

  setup do
    shard = "warm_adopt_#{System.unique_integer([:positive])}"
    prev_storage = Application.get_env(:fathom, :shard_storage)
    prev_s3 = Application.get_env(:fathom, S3)
    prev_flush = Application.get_env(:fathom, :shard_flush_interval_ms)
    prev_idle = Application.get_env(:fathom, :shard_idle_ms)
    prev_hook = Application.get_env(:fathom, :provenance_write_hook)

    # No periodic flush (it would rotate the etag under the assertions); a long idle so the coordinator
    # and its local copy survive until the test reads them.
    Application.put_env(:fathom, :shard_flush_interval_ms, 0)
    Application.put_env(:fathom, :shard_idle_ms, 300_000)

    on_exit(fn ->
      # Drain FIRST, while the S3 double is still configured and its Agent store is still alive
      # (on_exit callbacks run before the supervised store is stopped).
      capture_log(fn -> Shards.drain(shard, 2_000) end)
      restore(:shard_storage, prev_storage)
      restore(S3, prev_s3)
      restore(:shard_flush_interval_ms, prev_flush)
      restore(:shard_idle_ms, prev_idle)
      restore(:provenance_write_hook, prev_hook)
      for f <- Path.wildcard(local_path(shard) <> "*"), do: File.rm_rf(f)
    end)

    %{shard: shard}
  end

  defp restore(key, nil), do: Application.delete_env(:fathom, key)
  defp restore(key, val), do: Application.put_env(:fathom, key, val)

  defp local_path(shard), do: Path.join(Fathom.Shard.data_dir(), "#{shard}.db")

  defp build_db!(value) do
    path = Path.join(System.tmp_dir!(), "wad_#{System.unique_integer([:positive])}.db")
    {:ok, c} = Connection.open(path)
    :ok = Connection.exec(c, "CREATE TABLE kv (v TEXT)")
    :ok = Connection.exec(c, "INSERT INTO kv VALUES ('#{value}')")
    :ok = Connection.exec(c, "PRAGMA wal_checkpoint(TRUNCATE)")
    Connection.close(c)
    bytes = File.read!(path)
    for s <- ["", "-wal", "-shm"], do: File.rm(path <> s)
    bytes
  end

  # An expired lock owned by a crashed node: acquire STEALS and touches the data object, threading
  # touch_pre_etag / touch_post_etag through the lease.
  defp dead_lock do
    Storage.encode_lease(%{
      owner: "dead@node#old",
      epoch: 5,
      expires_at_ms: Storage.now_ms() - Storage.steal_margin_ms() - 60_000
    })
  end

  defp configure_s3(store) do
    Application.put_env(:fathom, S3,
      bucket: "b",
      region: "us-east-1",
      access_key_id: "k",
      secret_access_key: "s",
      endpoint: "https://s3.example",
      path_style: true,
      req_plug: fn conn -> S3EtagStore.serve(conn, store) end
    )

    Application.put_env(:fathom, :shard_storage, S3)
  end

  # Report every durable sidecar write for THIS shard's path to the test process.
  defp watch_durable_writes!(shard) do
    test = self()
    path = local_path(shard)

    Application.put_env(:fathom, :provenance_write_hook, fn
      :write_durable, ^path -> send(test, {:write_durable, path})
      _stage, _path -> :ok
    end)
  end

  defp start_store(objects) do
    start_supervised!({Agent, fn -> S3EtagStore.initial(objects) end})
  end

  defp open_close(shard) do
    {:ok, conn} = ShardExecutor.open(shard)
    :ok = ShardExecutor.close(conn)
  end

  test "a warm takeover adopts the post-touch etag with a DURABLE sidecar write", %{shard: shard} do
    base = build_db!("base-write")
    data_key = "#{shard}.db"
    store = start_store(%{data_key => base, "#{shard}.lock" => dead_lock()})
    pre = S3EtagStore.etag_of(store, data_key)

    # The warm copy: its own live file, sidecar naming the pre-touch etag (B1's fixture).
    path = local_path(shard)
    File.mkdir_p!(Path.dirname(path))
    File.write!(path, base)
    :ok = Provenance.write(path, pre)

    configure_s3(store)
    watch_durable_writes!(shard)

    capture_log(fn -> open_close(shard) end)

    post = S3EtagStore.etag_of(store, data_key)
    assert post != pre, "fixture: the steal touch must rotate the etag"
    assert Provenance.read(path) == {:ok, post}, "the adopted post-touch etag is the sidecar"

    assert_received {:write_durable, ^path}

    # (no refute-after-the-fact: the message above is sent from the coordinator's own process, before
    # the open returns, so it is already in the mailbox when `open_close/1` comes back.)
  end

  test "a COLD takeover keeps the cheap plain write (nothing acked locally to protect)",
       %{shard: shard} do
    base = build_db!("base-write")
    data_key = "#{shard}.db"
    store = start_store(%{data_key => base, "#{shard}.lock" => dead_lock()})

    configure_s3(store)
    watch_durable_writes!(shard)

    capture_log(fn -> open_close(shard) end)

    path = local_path(shard)
    assert {:ok, _etag} = Provenance.read(path)
    refute_received {:write_durable, _}, "the cold-open path must not pay an fsync (+24-28% p50)"
  end
end
