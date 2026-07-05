defmodule Fathom.Shard.WarmTakeoverLineageTest do
  # Round-2 expert review #6: the warm-takeover branch assumed ANY etag change
  # since the fork check was "our own steal-touch, moved no bytes" and stamped the
  # local file's sidecar with the new etag — FORGED provenance. But the dead prior
  # owner's in-flight fenced flush can land BETWEEN quarantined_fork?'s HEAD and
  # the steal's touch: the touched object then holds the ZOMBIE's acknowledged
  # durable writes while the local warm file is a diverged lineage. The forged
  # sidecar let the next flush If-Match successfully and OVERWRITE the zombie's
  # durable flush — silent, unrecoverable loss on a same-machine fast restart.
  # The invariant (#1's contract): a warm takeover adopts the post-touch etag ONLY
  # when the touch's SOURCE was this file's own provenance; anything else is a
  # fork — quarantine the local copy and serve the stored lineage.
  #
  # Driven end-to-end: a REAL coordinator over the S3 backend against the
  # MD5-faithful S3EtagStore, with the zombie flush injected after the first HEAD
  # (the fork check) and before the acquire's touch.
  use ExUnit.Case, async: false

  alias Fathom.{ShardExecutor, Shards}
  alias Fathom.Shard.{Connection, Storage}
  alias Fathom.Shard.Storage.S3
  alias Fathom.Test.S3EtagStore
  alias Filo.{Stmt, StmtResult}

  import ExUnit.CaptureLog

  @local_dir Path.join(System.tmp_dir!(), "fathom_shards")

  setup do
    shard = "lineage_#{System.unique_integer([:positive])}"
    prev_storage = Application.get_env(:fathom, :shard_storage)
    prev_s3 = Application.get_env(:fathom, S3)
    prev_idle = Application.get_env(:fathom, :shard_idle_ms)
    Application.put_env(:fathom, :shard_idle_ms, 50)

    on_exit(fn ->
      if prev_storage,
        do: Application.put_env(:fathom, :shard_storage, prev_storage),
        else: Application.delete_env(:fathom, :shard_storage)

      if prev_s3,
        do: Application.put_env(:fathom, S3, prev_s3),
        else: Application.delete_env(:fathom, S3)

      if prev_idle,
        do: Application.put_env(:fathom, :shard_idle_ms, prev_idle),
        else: Application.delete_env(:fathom, :shard_idle_ms)

      for f <- Path.wildcard(Path.join(@local_dir, "#{shard}*")), do: File.rm_rf(f)
    end)

    %{shard: shard}
  end

  defp build_db!(rows) do
    path = Path.join(System.tmp_dir!(), "lin_#{System.unique_integer([:positive])}.db")
    {:ok, c} = Connection.open(path)
    :ok = Connection.exec(c, "CREATE TABLE kv (v TEXT)")
    for r <- rows, do: :ok = Connection.exec(c, "INSERT INTO kv VALUES ('#{r}')")
    :ok = Connection.exec(c, "PRAGMA wal_checkpoint(TRUNCATE)")
    Connection.close(c)
    bytes = File.read!(path)
    for s <- ["", "-wal", "-shm"], do: File.rm(path <> s)
    bytes
  end

  test "a zombie flush landing between the fork check and the touch is quarantined, not laundered",
       %{shard: shard} do
    base_bytes = build_db!(["base"])
    zombie_bytes = build_db!(["base", "z-write"])

    dead_lock =
      Storage.encode_lease(%{
        owner: "dead@node#old",
        epoch: 5,
        expires_at_ms: Storage.now_ms() - Storage.steal_margin_ms() - 60_000
      })

    store =
      start_supervised!(
        {Agent,
         fn ->
           S3EtagStore.initial(%{"#{shard}.db" => base_bytes, "#{shard}.lock" => dead_lock})
         end}
      )

    # After the FIRST data-key HEAD (quarantined_fork?'s provenance check passes),
    # the dying old owner's in-flight fenced flush LANDS — before the steal's touch.
    heads = start_supervised!({Agent, fn -> 0 end}, id: :head_counter)

    plug = fn conn ->
      resp = S3EtagStore.serve(conn, store)

      if conn.method == "HEAD" and String.ends_with?(conn.request_path, "#{shard}.db") do
        n = Agent.get_and_update(heads, fn n -> {n + 1, n + 1} end)

        if n == 1 do
          Agent.update(store, fn s ->
            %{
              s
              | objects:
                  Map.put(s.objects, "#{shard}.db", %{
                    body: zombie_bytes,
                    form: :single,
                    meta: %{}
                  })
            }
          end)
        end
      end

      resp
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

    # This node's warm local file: a DIVERGED lineage (its own un-flushed write)
    # whose provenance sidecar honestly says it derives from the base object.
    local = Path.join(@local_dir, "#{shard}.db")
    File.mkdir_p!(@local_dir)
    File.write!(local, base_bytes)
    {:ok, lc} = Connection.open(local)
    :ok = Connection.exec(lc, "INSERT INTO kv VALUES ('a-fork')")
    :ok = Connection.exec(lc, "PRAGMA wal_checkpoint(TRUNCATE)")
    Connection.close(lc)
    File.write!(local <> ".etag", S3EtagStore.etag_of(store, "#{shard}.db"))

    capture_log(fn ->
      {:ok, conn} = ShardExecutor.open(shard)

      # The zombie's acknowledged durable write must be served — pre-fix the warm
      # branch stamped the local fork with the post-touch etag and served it.
      assert {:ok, %StmtResult{rows: [["z-write"]]}} =
               ShardExecutor.execute(conn, stmt("SELECT v FROM kv WHERE v = 'z-write'"))

      assert {:ok, %StmtResult{rows: []}} =
               ShardExecutor.execute(conn, stmt("SELECT v FROM kv WHERE v = 'a-fork'"))

      assert Path.wildcard(local <> ".forked.*") != [],
             "the diverged local copy must be quarantined for recovery"

      {:ok, coordinator} = Shards.ensure(shard)
      ref = Process.monitor(coordinator)
      :ok = ShardExecutor.close(conn)
      assert_receive {:DOWN, ^ref, :process, ^coordinator, :normal}, 5_000
    end)

    # After the idle flush, the durable object still contains the zombie's write.
    final = S3EtagStore.body_of(store, "#{shard}.db")
    tmp = Path.join(System.tmp_dir!(), "lin_check_#{System.unique_integer([:positive])}.db")
    File.write!(tmp, final)
    on_exit(fn -> File.rm(tmp) end)
    {:ok, rc} = Connection.open(tmp)

    assert {:ok, %{rows: [["z-write"]]}} =
             Connection.query(rc, "SELECT v FROM kv WHERE v = 'z-write'", []),
           "the zombie's acknowledged durable write must never be clobbered by the fork"

    Connection.close(rc)
  end

  defp stmt(sql), do: %Stmt{sql: sql, args: []}
end
