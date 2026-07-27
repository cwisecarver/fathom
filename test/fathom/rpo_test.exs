defmodule Fathom.RpoTest do
  # The loss-window (RPO) contract, quantified — what `Fathom.Rpo` / `mix fathom.rpo` measures,
  # pinned as deterministic invariants:
  #
  #   * NODE/DISK loss: a survivor cold-opens the last FLUSHED object, so it loses exactly the
  #     writes committed after that flush — nothing before it, and no more than the unflushed tail.
  #   * The flush interval IS the knob: flushing again mid-stream shrinks the window.
  #   * PROCESS crash, disk intact: `synchronous=FULL` fsyncs every commit to the local WAL, and a
  #     present local file is authoritative on wake, so a same-node restart loses nothing — even
  #     when the stored object is older (the local copy wins, it is not re-pulled).
  #
  # The magnitude-vs-rate curve is the harness's job (timing-based); these tests fix the boundary.
  # Not async: shards + lock files are global.
  use ExUnit.Case, async: false

  alias Fathom.{ShardExecutor, Shards}
  alias Fathom.Shard.{Connection, Storage}
  alias Filo.Stmt

  setup do
    shard = "rpo_#{System.unique_integer([:positive])}"
    prev_flush = Application.get_env(:fathom, :shard_flush_interval_ms)
    prev_idle = Application.get_env(:fathom, :shard_idle_ms)

    on_exit(fn ->
      restore(:shard_flush_interval_ms, prev_flush)
      restore(:shard_idle_ms, prev_idle)

      for dir <- [local_dir(), remote_dir()],
          suffix <- [".db", ".db-wal", ".db-shm", ".lock"],
          do: File.rm(Path.join(dir, shard <> suffix))

      for f <- Path.wildcard(Path.join(local_dir(), "#{shard}.db.*")), do: File.rm(f)
    end)

    %{shard: shard}
  end

  defp restore(key, nil), do: Application.delete_env(:fathom, key)
  defp restore(key, value), do: Application.put_env(:fathom, key, value)

  defp stmt(sql, args \\ []), do: %Stmt{sql: sql, args: args}

  # Drive one durability flush and wait for the off-process snapshot/upload task to settle.
  defp flush_now(coordinator) do
    send(coordinator, :durability_flush)
    wait_flush_settled(coordinator, 400)
  end

  defp wait_flush_settled(_coordinator, 0), do: flunk("durability flush task never settled")

  defp wait_flush_settled(coordinator, tries) do
    case :sys.get_state(coordinator) do
      %{flush_task: nil} -> :ok
      _ -> Process.sleep(5) && wait_flush_settled(coordinator, tries - 1)
    end
  end

  # The survivor's view: pull the stored object and read its newest seq — exactly what a failover
  # cold-open would see.
  defp survivor_max_seq(shard) do
    dst = Path.join(System.tmp_dir!(), "rpo_peek_#{System.unique_integer([:positive])}.db")
    assert {:ok, _etag} = Storage.pull(shard, dst)
    {:ok, conn} = Connection.open(dst)
    {:ok, %{rows: [[m]]}} = Connection.query(conn, "SELECT COALESCE(MAX(seq), 0) FROM t", [])
    Connection.close(conn)
    for s <- ["", "-wal", "-shm"], do: File.rm(dst <> s)
    m
  end

  defp insert_range(h, lo, hi) do
    for s <- lo..hi do
      {:ok, _} = ShardExecutor.execute(h, stmt("INSERT INTO t (seq) VALUES (?)", [s]))
    end

    :ok
  end

  test "node loss loses exactly the writes committed after the last flush", %{shard: shard} do
    # Interval 0 ⇒ only the explicit flush_now reaches storage, so the loss boundary is exact.
    Application.put_env(:fathom, :shard_flush_interval_ms, 0)
    Application.put_env(:fathom, :shard_idle_ms, 600_000)

    {:ok, h} = ShardExecutor.open(shard)
    {:ok, _} = ShardExecutor.execute(h, stmt("CREATE TABLE t (seq INTEGER PRIMARY KEY)"))
    {:ok, coordinator} = Shards.ensure(shard)

    insert_range(h, 1, 10)
    flush_now(coordinator)
    insert_range(h, 11, 15)

    survivor = survivor_max_seq(shard)

    assert survivor == 10,
           "the survivor sees the flushed state (1..10); the 5-row tail (11..15) is the loss window"

    # loss = acked(15) - survivor(10) = the post-flush tail, exactly.
    assert 15 - survivor == 5

    stop(shard, h)
  end

  test "a tighter flush cadence shrinks the loss window", %{shard: shard} do
    Application.put_env(:fathom, :shard_flush_interval_ms, 0)
    Application.put_env(:fathom, :shard_idle_ms, 600_000)

    {:ok, h} = ShardExecutor.open(shard)
    {:ok, _} = ShardExecutor.execute(h, stmt("CREATE TABLE t (seq INTEGER PRIMARY KEY)"))
    {:ok, coordinator} = Shards.ensure(shard)

    insert_range(h, 1, 10)
    flush_now(coordinator)
    insert_range(h, 11, 13)
    # A second flush at 13 — the extra cadence a smaller :shard_flush_interval_ms buys.
    flush_now(coordinator)
    insert_range(h, 14, 15)

    assert survivor_max_seq(shard) == 13,
           "flushing again at 13 cuts the loss window to 2 rows (14..15), vs 5 with one flush"

    stop(shard, h)
  end

  test "a process crash with the local disk intact loses nothing (synchronous=FULL)",
       %{shard: shard} do
    # No periodic flush + a long idle window: the ONLY thing in storage is one early flush, so a
    # survival of the later writes can only come from the local disk, not a re-pull.
    Application.put_env(:fathom, :shard_flush_interval_ms, 0)
    Application.put_env(:fathom, :shard_idle_ms, 600_000)

    {:ok, h} = ShardExecutor.open(shard)
    {:ok, _} = ShardExecutor.execute(h, stmt("CREATE TABLE t (seq INTEGER PRIMARY KEY)"))
    {:ok, coordinator} = Shards.ensure(shard)

    insert_range(h, 1, 5)
    flush_now(coordinator)
    # The stored object now lags at 5; the local disk keeps advancing.
    insert_range(h, 6, 20)

    assert survivor_max_seq(shard) == 5,
           "baseline: a node-loss survivor would drop back to the last flush (5)"

    # A hard kill skips terminate/2 — no final flush, exactly like a lost BEAM.
    ref = Process.monitor(coordinator)
    Process.exit(coordinator, :kill)
    assert_receive {:DOWN, ^ref, :process, ^coordinator, :killed}, 2_000
    # Release the now-orphaned connection (its coordinator is gone) so it doesn't pin the WAL
    # against the fresh coordinator's checkpoint, and let the supervisor reap the dead child so
    # its registry key clears before the re-open.
    _ = ShardExecutor.close(h)
    _ = :sys.get_state(Fathom.ShardSupervisor)

    # Re-open on the same node: the present local file is authoritative (adopt, don't re-pull the
    # stale store), and SQLite recovers the fsynced WAL.
    {:ok, h2} = ShardExecutor.open(shard)

    assert {:ok, %{rows: [[20]]}} =
             ShardExecutor.execute(h2, stmt("SELECT COALESCE(MAX(seq), 0) FROM t")),
           "every committed write survives a process crash on an intact disk (a re-pull would give 5)"

    stop(shard, h2)
  end

  # Check the connection in, then drain (flush + drop + stop) deterministically — the tests use a
  # long idle window, so the coordinator won't idle-stop on its own.
  defp stop(shard, h) do
    _ = ShardExecutor.close(h)
    _ = Shards.drain(shard, 5_000)
    :ok
  end

  defp local_dir, do: Fathom.Shard.data_dir()
  defp remote_dir, do: Fathom.Shard.Storage.Local.dir()
end
