defmodule Fathom.OrphanStreamTest do
  # Finding #8, the residual orphan-writer race. A coordinator crash is not restarted
  # (restart: :temporary), but the Hrana stream processes survive it, each holding an open
  # exqlite connection. Pre-fix, such a stream kept serving as an ORPHAN: the next checkout
  # re-creates a fresh coordinator that doesn't know the orphan's connection exists, so its
  # idle flush-and-drop checkpoints past the orphan's WAL frames and unlinks the files under
  # it — the orphan's writes vanish into unlinked inodes. The invariant pinned here: a stream
  # must not outlive its coordinator (ShardExecutor.owner/1 + Filo's owner monitor), so no
  # connection can exist that a successor coordinator doesn't track.
  #
  # Not async: shards are addressed by a global Registry and back onto files.
  use ExUnit.Case, async: false

  import Plug.Test
  import Plug.Conn

  @streams __MODULE__.Streams

  setup do
    shard = "orphan_#{System.unique_integer([:positive])}"
    start_supervised!({Filo.Streams, name: @streams})
    key = Filo.Baton.new_key()

    opts =
      Filo.Plug.init(
        executor: Fathom.ShardExecutor,
        streams: @streams,
        key: key,
        open_arg: fn _conn -> shard end
      )

    on_exit(fn ->
      local = Path.join([System.tmp_dir!(), "fathom_shards", "#{shard}.db"])
      remote = Path.join([System.tmp_dir!(), "fathom_remote_test", "#{shard}.db"])
      for base <- [local, remote], suffix <- ["", "-wal", "-shm"], do: File.rm(base <> suffix)
    end)

    %{opts: opts, shard: shard, key: key}
  end

  defp pipeline(opts, body) do
    conn(:post, "/v3/pipeline", Jason.encode!(body))
    |> put_req_header("content-type", "application/json")
    |> Filo.Plug.call(opts)
  end

  defp execute_req(sql), do: %{"type" => "execute", "stmt" => %{"sql" => sql}}

  test "a stream does not outlive its crashed coordinator", %{
    opts: opts,
    shard: shard,
    key: key
  } do
    # Open a stream (the baton keeps it alive server-side) with a real write on it.
    conn = pipeline(opts, %{"baton" => nil, "requests" => [execute_req("CREATE TABLE t (v)")]})
    assert conn.status == 200
    baton = Jason.decode!(conn.resp_body)["baton"]
    assert is_binary(baton)

    # Locate the server-side stream process (via the baton's stream id) and the shard's
    # coordinator (via the registry).
    {:ok, {stream_id, _seq}} = Filo.Baton.decode(baton, key)
    {:ok, stream_pid} = Filo.Streams.lookup(@streams, stream_id)
    [{coordinator, _}] = Registry.lookup(Fathom.ShardRegistry, shard)

    # Crash the coordinator. The stream must tear itself down (closing its exqlite
    # connection) rather than survive as an orphan writer.
    stream_ref = Process.monitor(stream_pid)
    Process.exit(coordinator, :kill)

    assert_receive {:DOWN, ^stream_ref, :process, ^stream_pid, :normal},
                   1_000,
                   "the stream must tear down when its coordinator dies (pre-fix it survives)"

    # The baton now names a dead stream — the client is told, and reopens.
    conn = pipeline(opts, %{"baton" => baton, "requests" => [execute_req("SELECT 1")]})
    assert conn.status == 400
    assert %{"code" => "STREAM_NOT_FOUND"} = Jason.decode!(conn.resp_body)

    # A fresh open lands on a brand-new coordinator (temporary: re-created on demand).
    conn = pipeline(opts, %{"baton" => nil, "requests" => [execute_req("SELECT 1")]})
    assert conn.status == 200
  end
end
