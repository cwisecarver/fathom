defmodule Fathom.ShardPositionSeedTest do
  @moduledoc """
  Expert review 2026-09-05 #7 "seed-on-known-short", root-caused 2026-09-07 from the chaos-rig `rpo`
  INVALID (`audits/expert-review-2026-09-05-002121.md.progress.md`).

  THE INVARIANT: a shard that has SHIPPED a replication ordinal this open must not flush a durable
  object with NO position stamp, even when its WAL is empty at flush time (the last Hrana stream's
  close checkpointed and unlinked `-wal`). A nil stamp is un-rankable in `Promote.fresher?/2`, so it
  silently disables A2 promote-on-open for the window from a cold reopen to the first live-WAL flush
  — exactly what the rig's `rpo` scenario reported as "a stamping/durability problem".

  THE SYMPTOM this pins: with `wal_ordinal > 0`, a plain insert then idle-drop used to leave
  `Storage.object_position/1 == {:ok, nil}`; it must now carry a rankable `wal_ordinal`.

  `wal_ordinal/2` is called directly to stand in for a replication ship having assigned the ordinal
  (this test does not run the replication stack); that is the only input the stamp path reads, so it
  reproduces the bug deterministically without a follower/quorum. The over-claim's soundness against
  a real un-flushed peer is a chaos-rig invariant (`rpo`), not something a unit test asserts.
  """
  use ExUnit.Case, async: false

  alias Fathom.{Shard, ShardExecutor, Shards}
  alias Fathom.Shard.Storage
  alias Filo.Stmt

  defp stmt(sql, args \\ []), do: %Stmt{sql: sql, args: args}

  setup do
    shard = "posseed_#{System.unique_integer([:positive])}"
    prev_idle = Application.get_env(:fathom, :shard_idle_ms)
    Application.put_env(:fathom, :shard_idle_ms, 50)

    on_exit(fn ->
      if prev_idle,
        do: Application.put_env(:fathom, :shard_idle_ms, prev_idle),
        else: Application.delete_env(:fathom, :shard_idle_ms)

      for suffix <- [".db", ".db-wal", ".db-shm", ".lock"],
          do: File.rm(Path.join(Storage.Local.dir(), shard <> suffix))
    end)

    %{shard: shard}
  end

  test "a shipped shard idle-dropping with an empty WAL stamps a rankable ordinal, not nil", %{
    shard: shard
  } do
    {:ok, conn} = ShardExecutor.open(shard)
    {:ok, _} = ShardExecutor.execute(conn, stmt("CREATE TABLE kv (k INTEGER, v TEXT)"))
    {:ok, _} = ShardExecutor.execute(conn, stmt("INSERT INTO kv VALUES (1, 'alice')"))

    {:ok, pid} = Shards.ensure(shard)

    # Stand in for a replication ship: assigning an ordinal for a WAL salt bumps state.wal_ordinal
    # to 1 (the same call the Replication.Session makes). This is the ONLY state the stamp path reads.
    assert 1 = Shard.wal_ordinal(pid, 987_654_321)

    # Close the last connection -> its close checkpoints+unlinks the WAL -> the idle-drop flush reads
    # an empty WAL both before and after, the exact condition that used to stamp nil.
    ref = Process.monitor(pid)
    :ok = ShardExecutor.close(conn)
    assert_receive {:DOWN, ^ref, :process, ^pid, _}, 5_000

    assert {:ok, pos} = Storage.object_position(shard)

    assert is_map(pos),
           "a shipped shard's durable object must carry a position stamp, not nil — a nil stamp is " <>
             "un-rankable in Promote.fresher?/2 and silently disables A2 promote-on-open"

    assert pos.wal_ordinal == 2,
           "the empty-WAL over-claim must be wal_ordinal + 1 (1 -> 2), strictly ahead of every " <>
             "replica the coordinator shipped at ordinal <= 1"
  end
end
