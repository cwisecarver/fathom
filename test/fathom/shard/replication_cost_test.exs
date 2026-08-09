defmodule Fathom.Shard.ReplicationCostTest do
  @moduledoc """
  **What turning A2 on costs a write.** See `docs/a2-quorum-replication.md`.

  This closes a gap in the earlier measurements. `mix fathom.bench` gates the hot path, but every
  gated run so far had `:replication_enabled` **off** — so what was measured is the cost of the
  *gate check* (one `Application.get_env` per write), not the cost of the feature working. Gate 2
  measured the quorum ack in isolation on raw sockets; neither number tells you what a real fathom
  write costs with replication on, because the composition adds work gate 2 never touched:

    * a WAL header read (`Wal.read/1`) and a `pread` of the delta
    * a `GenServer.call` hop into the per-shard `Session`
    * building and encoding the push

  So this measures the same write both ways and reports the difference. Loopback followers, so the
  network term is near zero and what is left is **fathom's own overhead**. Deployment cost is this
  plus one RTT to the 2nd-fastest follower — measured separately in the gate-2 sweep
  (`deploy/chaos/a2_rtt_split.exs`), and the reason placement matters more than replica count.

  Tagged `:bench`, excluded by default. Run with:

      mix test --include bench test/fathom/shard/replication_cost_test.exs
  """
  use ExUnit.Case, async: false

  @moduletag :bench

  alias Fathom.Shard.Connection
  alias Fathom.Shard.Replication.Fleet
  alias Fathom.Shard.Replication.Follower
  alias Fathom.Shard.Replication.Session
  alias Fathom.Shard.Replication.Shipper
  alias Fathom.Shards

  @samples 200
  @followers 3
  @quorum 2

  setup do
    id = "repl_cost_#{System.unique_integer([:positive])}"
    root = Path.join(System.tmp_dir!(), "replcost_#{System.unique_integer([:positive])}")
    File.mkdir_p!(root)

    prev = %{
      enabled: Application.get_env(:fathom, :replication_enabled),
      followers: Application.get_env(:fathom, :replication_followers),
      quorum: Application.get_env(:fathom, :replication_quorum),
      fsync: Application.get_env(:fathom, :replication_fsync)
    }

    on_exit(fn ->
      Session.stop(id)

      for {k, v} <- [
            replication_enabled: prev.enabled,
            replication_followers: prev.followers,
            replication_quorum: prev.quorum,
            replication_fsync: prev.fsync
          ] do
        if is_nil(v),
          do: Application.delete_env(:fathom, k),
          else: Application.put_env(:fathom, k, v)
      end

      File.rm_rf(root)
      for s <- ["", "-wal", "-shm"], do: File.rm(Fathom.Shard.db_path(id) <> s)
    end)

    %{id: id, root: root}
  end

  defp p50(list) do
    sorted = Enum.sort(list)
    Enum.at(sorted, div(length(sorted), 2))
  end

  defp time_us(fun) do
    t0 = System.monotonic_time(:microsecond)
    fun.()
    System.monotonic_time(:microsecond) - t0
  end

  defp start_fleet!(root, fsync?) do
    followers =
      for i <- 1..@followers do
        name = :"cost_f#{i}_#{System.unique_integer([:positive])}"
        dir = Path.join(root, to_string(name))
        pid = start_supervised!({Follower, name: name, port: 0, dir: dir}, id: name)
        {:ok, port} = Follower.port(pid)
        {name, port}
      end

    Application.put_env(:fathom, :replication_enabled, true)
    Application.put_env(:fathom, :replication_quorum, @quorum)
    Application.put_env(:fathom, :replication_fsync, fsync?)

    Application.put_env(
      :fathom,
      :replication_followers,
      for({_n, port} <- followers, do: {~c"127.0.0.1", port})
    )

    start_supervised!(Fleet)

    # Shippers connect in handle_continue; measuring before they are up would time a
    # :disconnected reject, not a replicated write.
    deadline = System.monotonic_time(:millisecond) + 5_000

    Stream.repeatedly(fn ->
      if Enum.all?(Fleet.shippers(), &Shipper.connected?/1),
        do: :up,
        else: Process.sleep(20)
    end)
    |> Enum.find(fn
      :up -> true
      _ -> System.monotonic_time(:millisecond) > deadline
    end)
    |> case do
      :up -> :ok
      _ -> flunk("shippers never connected — this would measure nothing")
    end

    followers
  end

  @tag timeout: 300_000
  test "measures what replication adds to a write", ctx do
    %{id: id, root: root} = ctx

    {:ok, coordinator, ref, path} = Shards.checkout(id)
    on_exit(fn -> Fathom.Shard.checkin(coordinator, ref) end)
    {:ok, conn} = Connection.open(path)
    on_exit(fn -> Connection.close(conn) end)

    {:ok, _} = Connection.query(conn, "CREATE TABLE t (a INTEGER, b TEXT)", [])
    row = String.duplicate("x", 200)

    insert = fn n ->
      {:ok, _} = Connection.query(conn, "INSERT INTO t VALUES (?1, ?2)", [n, row])
    end

    # ---- replication OFF: the local write alone --------------------------------------------
    Application.put_env(:fathom, :replication_enabled, false)
    for n <- 1..50, do: insert.(n)
    off = p50(for n <- 51..(50 + @samples), do: time_us(fn -> insert.(n) end))

    # ---- replication ON, ack from RAM (the default, Waterpark parity) -----------------------
    followers = start_fleet!(root, false)
    wal = path <> "-wal"

    # Seed at offset 0 so the first push carries the whole WAL and the followers accept it; the
    # steady-state deltas that follow are what is being timed.
    for {name, _} <- followers, do: Follower.seed(name, id, 0, 0, 0)

    assert :ok = Session.commit(id, wal, coordinator),
           "replication is not working; nothing to time"

    replicated = fn n ->
      insert.(n)
      :ok = Session.commit(id, wal, coordinator)
    end

    for n <- 1..50, do: replicated.(n + 100_000)
    base = 50 + @samples + 1
    on_ram = p50(for n <- base..(base + @samples), do: time_us(fn -> replicated.(n) end))

    IO.puts("""

    === A2 per-write cost (loopback followers; NO inter-node latency) ===
      N=#{@followers} followers, Q=#{@quorum}, #{@samples} samples, p50

      write only (replication off)        #{off} µs
      write + replication (RAM ack)       #{on_ram} µs
      ---------------------------------------------
      added by replication                #{on_ram - off} µs  (#{Float.round(on_ram / max(off, 1), 2)}x)

      Deployment cost = this + ONE RTT to the 2nd-fastest follower.
      Placement, not replica count, dominates that term — see the gate-2 RTT sweep.
    """)

    # Loose ceiling, in the spirit of the other :bench guards: this exists to catch an
    # order-of-magnitude break on a contended dev box, not to police noise.
    assert on_ram < 50_000,
           "a replicated write took #{on_ram}µs on loopback — that is an order of magnitude past " <>
             "the expected cost and something in the commit path is blocking"

    assert on_ram > off,
           "replication appears free (#{off} -> #{on_ram}µs), which means the commit is not " <>
             "actually reaching a quorum — this measured nothing"
  end
end
