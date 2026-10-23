defmodule Fathom.Shard.CloseDepthLazyTest do
  @moduledoc """
  Expert review 2026-10-22 r8 #24a: the close-chain depth counter (`close_depth`, ~154 B off-heap)
  is created on the first close-chain link, not at open, because most coordinators never spawn one.
  Pins: a fresh coordinator carries none, the first link creates it, and the `close_chain` gauge
  still reports depth from that first link. Not async: telemetry handler and real shard files.
  """
  use ExUnit.Case, async: false

  alias Fathom.{Shard, Shards}
  alias Fathom.Shard.{Connection, Storage}

  setup do
    id = "closedepth_#{System.unique_integer([:positive])}"
    prev = Application.fetch_env(:fathom, :shard_idle_ms)
    Application.put_env(:fathom, :shard_idle_ms, 600_000)
    test_pid = self()
    handler = "closedepth-#{id}"

    :telemetry.attach(
      handler,
      [:fathom, :shard, :close_chain],
      fn _, %{depth: d}, _, _ -> send(test_pid, {:close_chain, d}) end,
      nil
    )

    on_exit(fn ->
      :telemetry.detach(handler)

      case prev do
        {:ok, v} -> Application.put_env(:fathom, :shard_idle_ms, v)
        :error -> Application.delete_env(:fathom, :shard_idle_ms)
      end

      Shards.stop(id)

      for dir <- [Shard.data_dir(), Storage.Local.dir()],
          s <- [".db", ".db-wal", ".db-shm", ".db.etag", ".lock"],
          do: File.rm(Path.join(dir, id <> s))
    end)

    %{id: id}
  end

  test "no counter until the first close-chain link, then one", %{id: id} do
    {:ok, pid} = Shards.ensure(id)
    assert :sys.get_state(pid).close_depth == nil, "a fresh coordinator allocated the counter"

    # A handle checked IN to the coordinator (pool off in test) is closed on the chain.
    hand_back! = fn ->
      {:ok, ^pid, ref, path} = Shards.checkout(id)
      {:ok, conn} = Connection.open(path)
      Shard.checkin(pid, ref, conn, :rw)
    end

    hand_back!.()
    depth = :sys.get_state(pid).close_depth
    assert depth != nil and :counters.get(depth, 1) >= 0, "the first link must create the counter"
    assert_receive {:close_chain, d} when is_integer(d) and d >= 1, 1_000

    # Reused, not re-created, by the next link.
    hand_back!.()
    assert :sys.get_state(pid).close_depth == depth
  end
end
