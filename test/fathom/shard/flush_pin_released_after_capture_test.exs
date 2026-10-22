defmodule Fathom.Shard.FlushPinReleasedAfterCaptureTest do
  @moduledoc """
  Expert review 2026-10-21 r7 #23 (panel 3 #5; rv6 F9 documented the cost, did not fix it).

  The pinned-live flush uploads the live `.db` under a read pin, which freezes the main file while
  its bytes are read. On an ENCODING backend (prod's zstd) the bytes are fully captured once the
  compressed `.z` temp and its digests are written and the stat-before/stat-after guard passes;
  the PUT then streams only from that temp. The pin was nevertheless held through the pre-PUT
  intent fsync and the whole S3 PUT. A pin holding read-lock 0 (or a WAL read mark) stops every
  checkpoint from backfilling, so every commit during the PUT stayed in the `-wal` (growth = write
  rate x PUT duration; a due Session threshold checkpoint moved nothing and deferred itself).

  Pinned here, in BOTH liveness modes (asserted to have taken), through the real `Storage.S3`
  client against a stateful stub, with commits landing DURING the PUT:

    * zstd: a PASSIVE checkpoint run during the PUT backfills every frame (the pin is gone). Pre-fix
      it backfilled 0 frames.
    * zstd: the uploaded object is the captured state: it holds the pre-flush rows and NOT the rows
      committed during the PUT (the release must come after the capture, never before).
    * `:none`: the body streams from the live `.db`, so the pin MUST be held through the PUT: the
      same PASSIVE backfills 0 frames. (This is also what shows the probe can see a held pin.)

  Not async: storage backend, app env and telemetry are global.
  """
  use ExUnit.Case, async: false

  alias Fathom.Shard.{Connection, Heartbeat}
  alias Fathom.Shard.Storage.S3
  alias Fathom.ShardExecutor
  alias Fathom.Shards
  alias Fathom.Test.S3EtagStore

  @modes [:legacy, :heartbeat]

  setup do
    id = "pinrel_#{System.unique_integer([:positive])}"
    path = Fathom.Shard.db_path(id)

    # Unlinked and stopped LAST (the first-registered on_exit runs last): the coordinator's and the
    # heartbeat's teardown still talk to the store after the test's supervised children are gone.
    {:ok, store} = Agent.start(fn -> S3EtagStore.initial(%{}) end)
    {:ok, armed} = Agent.start(fn -> false end)

    on_exit(fn ->
      Agent.stop(store)
      Agent.stop(armed)
    end)

    for k <- [:shard_storage, S3, :shard_idle_ms, :shard_object_encoding] do
      prev = Application.get_env(:fathom, k)

      on_exit(fn ->
        if is_nil(prev),
          do: Application.delete_env(:fathom, k),
          else: Application.put_env(:fathom, k, prev)
      end)
    end

    # The idle-stop would answer a pending flush with :coordinator_stopped (AGENTS.md testing note).
    Application.put_env(:fathom, :shard_idle_ms, 600_000)

    test_pid = self()
    handler = "pinrel-#{id}"

    :telemetry.attach(
      handler,
      [:fathom, :shard, :flush, :route],
      fn _, _, meta, _ -> if meta.shard_id == id, do: send(test_pid, {:route, meta.route}) end,
      nil
    )

    on_exit(fn ->
      :telemetry.detach(handler)
      Shards.stop(id)
      for f <- Path.wildcard(Path.join(Fathom.Shard.data_dir(), "#{id}*")), do: File.rm_rf(f)
    end)

    use_s3!(store, armed, id, path)
    %{id: id, path: path, store: store, armed: armed}
  end

  defp stmt(sql, args \\ []), do: %Filo.Stmt{sql: sql, args: args}

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

  defp assert_mode!(c, :heartbeat),
    do: assert(is_integer(:sys.get_state(c).acquire_gen), "heartbeat mode did not take")

  defp assert_mode!(c, :legacy),
    do: assert(is_nil(:sys.get_state(c).acquire_gen), "legacy mode did not take")

  # The S3 stub, with a hook run INSIDE the data-object PUT (after the client has sent it, before
  # the store answers): tenant commits land and a PASSIVE checkpoint runs, both on the live file.
  defp use_s3!(store, armed, id, path) do
    test_pid = self()
    key = "/b/#{id}.db"

    plug = fn conn ->
      if conn.method == "PUT" and conn.request_path == key and Agent.get(armed, & &1) do
        {:ok, raw} = Connection.open(path)

        for i <- 1..20,
            do:
              {:ok, _} =
                Connection.query(raw, "INSERT INTO t VALUES (?1, zeroblob(500))", [1000 + i])

        {:ok, %{rows: [[busy, log, ckpt]]}} =
          Connection.query(raw, "PRAGMA wal_checkpoint(PASSIVE)", [])

        :ok = Connection.close(raw)
        send(test_pid, {:during_put, busy, log, ckpt})
      end

      S3EtagStore.serve(conn, store)
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
  end

  defp flush!(c, tries \\ 400) do
    send(c, :durability_flush)
    settle(c, tries)
  end

  defp settle(_c, 0), do: flunk("flush task never settled")

  defp settle(c, tries) do
    if :sys.get_state(c).flush_task == nil do
      :ok
    else
      receive do
      after
        10 -> :ok
      end

      settle(c, tries - 1)
    end
  end

  defp object_row_count(store, id) do
    body = S3EtagStore.plain_body_of(store, "#{id}.db")
    assert is_binary(body), "no object was uploaded"
    dst = Path.join(System.tmp_dir!(), "pinrel_obj_#{System.unique_integer([:positive])}.db")
    File.write!(dst, body)

    try do
      {:ok, conn} = Connection.open(dst, scope: :ro)
      {:ok, %{rows: [[n]]}} = Connection.query(conn, "SELECT count(*) FROM t", [])
      :ok = Connection.close(conn)
      n
    after
      for s <- ["", "-wal", "-shm"], do: File.rm(dst <> s)
    end
  end

  # Builds the shard and a stream that stays open (so no close in the test is the LAST connection,
  # which would checkpoint on close and hide the pin), then flushes with commits landing mid-PUT.
  defp flush_with_commits_during_put(ctx, mode, encoding) do
    %{id: id, store: store, armed: armed} = ctx
    Application.put_env(:fathom, :shard_object_encoding, encoding)

    {:ok, c} = Shards.ensure(id)
    assert_mode!(c, mode)
    {:ok, h} = ShardExecutor.open(id)
    on_exit(fn -> ShardExecutor.close(h) end)
    {:ok, _} = ShardExecutor.execute(h, stmt("CREATE TABLE t (a INTEGER, b BLOB)"))

    for i <- 1..50,
        do:
          {:ok, _} =
            ShardExecutor.execute(h, stmt("INSERT INTO t VALUES (?1, zeroblob(500))", [i]))

    # Arm the PUT hook only now, so the shard's earlier lifecycle PUTs (lease) are not hooked.
    Agent.update(armed, fn _ -> true end)
    flush!(c)

    assert_received {:route, :pinned_live},
                    "precondition: the flush must take the pinned-live route"

    assert_received {:during_put, busy, log, ckpt}
    assert log > 0, "precondition: the commits during the PUT must have written WAL frames"
    {store, busy, log, ckpt}
  end

  for mode <- @modes do
    describe "#{mode} mode" do
      setup do: set_mode!(unquote(mode))

      test "zstd: the pin is released once the body is captured, before the PUT", ctx do
        {store, _busy, log, ckpt} =
          flush_with_commits_during_put(ctx, unquote(mode), :zstd)

        # THE REGRESSION: the pin was held through the PUT, so this backfilled 0 frames.
        assert ckpt == log,
               "a checkpoint during the PUT backfilled #{ckpt} of #{log} frames: " <>
                 "the flush pin is still held after the body was captured"

        # Captured state only: the rows committed during the PUT are not in the object.
        assert object_row_count(store, ctx.id) == 50
      end

      test ":none: the body streams from the live file, so the pin is held through the PUT",
           ctx do
        {store, _busy, log, ckpt} =
          flush_with_commits_during_put(ctx, unquote(mode), :none)

        assert ckpt == 0,
               "a checkpoint during an unencoded PUT backfilled #{ckpt} of #{log} frames: " <>
                 "the live .db was not frozen while it was the PUT body"

        assert object_row_count(store, ctx.id) == 50
      end
    end
  end
end
