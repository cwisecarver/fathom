defmodule Fathom.Shard.StealTouchWarmStandbyTest do
  # Expert review #15: every crash-steal touches the data object (invalidating its
  # etag so a zombie flush 412s — review #3), but that touch ROTATES the etag the
  # survivor's speculative pull captured. On a data-bearing crash failover — the
  # warm standby's HEADLINE scenario — the survivor's live path is empty (the warm
  # copy lives in the follower cache), so `warm?` is false and the takeover lands in
  # revalidate_touched's COLD branch. Pre-fix that branch saw `etag != touch_post_etag`
  # and re-pulled the WHOLE object unconditionally — discarding the bytes it already
  # holds for a full body transfer, the exact cost the warm follower exists to avoid.
  #
  # The touch is a self-copy: it moves no bytes. So when the pulled object's etag
  # equals the touch's If-Match SOURCE (`touch_pre_etag`), the bytes are byte-identical
  # to the post-touch object and the survivor can adopt the post-touch etag with NO
  # re-pull. The invariant: a cold takeover whose pull captured the touch's source
  # object serves it without a second full-body pull.
  #
  # Driven end-to-end: a REAL coordinator over the S3 backend against the MD5-faithful
  # S3EtagStore, with the touch held until the speculative pull's GET has served (so the
  # pull deterministically captures the pre-touch object — scenario a). This is the
  # takeover-revalidation coverage review #30 flagged as absent. Not async: shards/
  # storage are global.
  use ExUnit.Case, async: false

  import ExUnit.CaptureLog

  alias Fathom.{ShardExecutor, Shards}
  alias Fathom.Shard.{Connection, Storage}
  alias Fathom.Shard.Storage.S3
  alias Fathom.Test.S3EtagStore
  alias Filo.{Stmt, StmtResult}

  @local_dir Path.join(System.tmp_dir!(), "fathom_shards")

  setup do
    shard = "steal_warm_#{System.unique_integer([:positive])}"
    prev_storage = Application.get_env(:fathom, :shard_storage)
    prev_s3 = Application.get_env(:fathom, S3)

    on_exit(fn ->
      if prev_storage,
        do: Application.put_env(:fathom, :shard_storage, prev_storage),
        else: Application.delete_env(:fathom, :shard_storage)

      if prev_s3,
        do: Application.put_env(:fathom, S3, prev_s3),
        else: Application.delete_env(:fathom, S3)

      for f <- Path.wildcard(Path.join(@local_dir, "#{shard}*")), do: File.rm_rf(f)
    end)

    %{shard: shard}
  end

  defp stmt(sql), do: %Stmt{sql: sql, args: []}

  defp build_db!(rows) do
    path = Path.join(System.tmp_dir!(), "stw_#{System.unique_integer([:positive])}.db")
    {:ok, c} = Connection.open(path)
    :ok = Connection.exec(c, "CREATE TABLE kv (v TEXT)")
    for r <- rows, do: :ok = Connection.exec(c, "INSERT INTO kv VALUES ('#{r}')")
    :ok = Connection.exec(c, "PRAGMA wal_checkpoint(TRUNCATE)")
    Connection.close(c)
    bytes = File.read!(path)
    for s <- ["", "-wal", "-shm"], do: File.rm(path <> s)
    bytes
  end

  defp dead_lock do
    Storage.encode_lease(%{
      owner: "dead@node#old",
      epoch: 5,
      expires_at_ms: Storage.now_ms() - Storage.steal_margin_ms() - 60_000
    })
  end

  # Bounded poll (never Process.sleep-forever if the pull never fires): signals the
  # test process on timeout so a broken ordering surfaces as a failed refute, not a hang.
  defp wait_until(test_pid, fun, tries \\ 500) do
    cond do
      fun.() -> :ok
      tries <= 0 -> send(test_pid, :pull_never_served)
      true -> Process.sleep(5) && wait_until(test_pid, fun, tries - 1)
    end
  end

  test "a cold crash-steal takeover adopts the touched etag without a full body re-pull",
       %{shard: shard} do
    # The last flushed object the survivor's pull will capture (single-form MD5 etag).
    base_bytes = build_db!(["base-write"])
    data_key = "#{shard}.db"

    store =
      start_supervised!(
        {Agent,
         fn -> S3EtagStore.initial(%{data_key => base_bytes, "#{shard}.lock" => dead_lock()}) end}
      )

    pre_etag = S3EtagStore.etag_of(store, data_key)
    refute pre_etag =~ "-", "seed must carry a single-form (MD5) etag so the touch rotates it"

    test_pid = self()
    pull_served = start_supervised!({Agent, fn -> false end}, id: :pull_served)
    body_gets = start_supervised!({Agent, fn -> 0 end}, id: :body_gets)

    plug = fn conn ->
      query = URI.decode_query(conn.query_string || "")
      data? = String.ends_with?(conn.request_path, data_key)

      cond do
        # Hold the crash-steal touch (its first multipart step) until the speculative
        # pull's GET has served, so the pull deterministically captures the PRE-touch
        # object (scenario a — the pull races ahead of the touch). Deterministic
        # ordering replaces a flaky wall-clock race; the object doesn't actually rotate
        # until CompleteMultipartUpload, well after this hold releases.
        conn.method == "POST" and data? and Map.has_key?(query, "uploads") ->
          wait_until(test_pid, fn -> Agent.get(pull_served, & &1) end)
          S3EtagStore.serve(conn, store)

        # A correct open pulls the data object ONCE (the speculative pull). The pre-#15
        # cold branch re-pulled it a SECOND time; count full-body GETs to pin that the
        # fix avoids the re-pull. (The touch uses HEAD/POST/PUT, never GET.)
        conn.method == "GET" and data? ->
          resp = S3EtagStore.serve(conn, store)
          Agent.update(body_gets, &(&1 + 1))
          Agent.update(pull_served, fn _ -> true end)
          resp

        true ->
          S3EtagStore.serve(conn, store)
      end
    end

    Application.put_env(:fathom, S3,
      bucket: "b",
      region: "us-east-1",
      access_key_id: "k",
      secret_access_key: "s",
      endpoint: "https://s3.example",
      path_style: true,
      req_plug: plug
    )

    Application.put_env(:fathom, :shard_storage, S3)

    capture_log(fn ->
      {:ok, conn} = ShardExecutor.open(shard)

      refute_received :pull_never_served,
                      "the speculative pull's GET must have served before the touch"

      # The steal genuinely rotated the etag (single -> multipart form), and it moved
      # no bytes — so the survivor serves the last acknowledged write from the copy it
      # already holds.
      post_etag = S3EtagStore.etag_of(store, data_key)
      assert post_etag =~ "-1", "the crash-steal touch must rotate the etag to multipart form"
      assert post_etag != pre_etag

      assert {:ok, %StmtResult{rows: [["base-write"]]}} =
               ShardExecutor.execute(conn, stmt("SELECT v FROM kv")),
             "the takeover must serve the touched object's bytes"

      # The core #15 assertion: exactly one full-body pull. Pre-fix this was two (the
      # cold branch's unconditional repull); the fix adopts the post-touch etag from
      # the copy already pulled.
      assert Agent.get(body_gets, & &1) == 1,
             "a cold crash-steal takeover must NOT re-pull the whole object (pre-#15: it did)"

      # The provenance sidecar is stamped to the post-touch etag, so a later warm
      # restart fences with the store's real etag instead of self-fencing on its first
      # flush (or needlessly quarantining + re-pulling).
      assert File.read!(Path.join(@local_dir, "#{shard}.db.etag")) == post_etag,
             "the adopted post-touch etag must be persisted as the local provenance"

      {:ok, coordinator} = Shards.ensure(shard)
      ref = Process.monitor(coordinator)
      :ok = ShardExecutor.close(conn)
      _ = Shards.drain(shard)

      receive do
        {:DOWN, ^ref, :process, ^coordinator, _} -> :ok
      after
        5_000 -> :ok
      end
    end)
  end
end
