defmodule Fathom.Shard.FlushPositionTest do
  @moduledoc """
  The stored object's position stamp — Phase 2 A2 promote-on-open groundwork.

  A failover has to order a node's local **replica** against the **stored object**, and nothing
  else in fathom can: an etag is a content hash with no ordering, the lock carries the *holder's*
  epoch rather than the object's, and comparing wall-clock across nodes is unsound. The stamp is
  what makes that comparison possible, and everything here exists to keep it from lying.

  ## The read order is the whole safety argument

  `Fathom.Shard.flush_position/1` is read **after** the snapshot, so the stamp claims at least as
  much as the object holds. A replica is promoted only when strictly ahead of the claim, so an
  over-claim costs at most one flush interval of RPO and can never lose a write.

  Reading it *before* the snapshot would under-claim, and a replica sitting between the claim and
  the truth would then be judged fresher and promoted — dropping exactly the writes that landed
  during the snapshot.

  **`over_claims_never_under_claims` does NOT reproduce that mistake, and it is worth knowing why
  before trusting it.** The two orderings differ only when a write lands *during* the snapshot, and
  with no concurrent writer both read the same WAL — so the flipped version passes it. Forcing a
  write into that window is inherently racy (there is no storage hook between the snapshot and the
  PUT; `run_before(:flush)` fires inside the backend, after the position is computed either way),
  and a probabilistic guard on a data-loss path is worse than an honest one.

  So the runtime test is kept as an invariant guard — the stamp must never sit behind the WAL that
  existed before the flush — and the actual read-order guard is `reads_the_position_after_the_
  snapshot`, which asserts the call order in the source. That one does fail when the read is
  hoisted, which is the change being defended against.
  """
  use ExUnit.Case, async: false

  alias Fathom.Shard.Replication.Promote
  alias Fathom.Shard.Storage
  alias Fathom.ShardExecutor
  alias Fathom.Shards

  describe "encode/parse round trip" do
    test "round-trips a position" do
      pos = %{epoch: 7, wal_gen: 3, offset: 12_392}
      assert Storage.parse_position(Storage.encode_position(pos)) == pos
    end

    # Every one of these must be `nil` rather than a guess. `nil` means "unknown", and the only
    # consumer treats unknown as "never override the stored object" — so garbage degrades to the
    # safe answer instead of a fabricated ordering that could discard a lineage.
    test "refuses anything that is not exactly a position" do
      for bad <- [
            nil,
            "",
            "1:2",
            "1:2:3:4",
            "a:2:3",
            "1:2:x",
            "-1:2:3",
            "1:-2:3",
            "1:2:-3",
            "1: 2:3",
            "1.5:2:3",
            :not_a_string,
            %{epoch: 1}
          ] do
        assert Storage.parse_position(bad) == nil, "accepted #{inspect(bad)}"
      end
    end
  end

  describe "Promote.fresher?/2" do
    defp replica(e, g, o), do: %{epoch: e, wal_gen: g, next_offset: o}
    defp stamp(e, g, o), do: %{epoch: e, wal_gen: g, offset: o}

    test "orders lexicographically on epoch, then generation, then offset" do
      assert Promote.fresher?(replica(1, 0, 100), stamp(1, 0, 50))
      assert Promote.fresher?(replica(1, 1, 0), stamp(1, 0, 999_999))
      assert Promote.fresher?(replica(2, 0, 0), stamp(1, 9, 999_999))

      refute Promote.fresher?(replica(1, 0, 50), stamp(1, 0, 100))
      refute Promote.fresher?(replica(1, 0, 999_999), stamp(1, 1, 0))
      refute Promote.fresher?(replica(1, 9, 999_999), stamp(2, 0, 0))
    end

    # Equal means both hold the same history, and the stored object is the one with provenance.
    test "equal positions are not fresher" do
      refute Promote.fresher?(replica(3, 2, 400), stamp(3, 2, 400))
    end

    # The rollout state: every object written before stamping existed carries no stamp, and none
    # of them may be overridden. This is what makes the feature inert rather than dangerous while
    # the fleet catches up.
    test "an unstamped object is never overridable" do
      refute Promote.fresher?(replica(99, 99, 99_999), nil)
    end

    test "no replica, or a shape it does not recognise, is never fresher" do
      refute Promote.fresher?(nil, stamp(1, 0, 0))
      refute Promote.fresher?(%{epoch: 1}, stamp(1, 0, 0))
      refute Promote.fresher?(replica(1, 0, 5), %{epoch: 1})
    end
  end

  describe "the stamp on a real flush" do
    # Writes go through ShardExecutor, not Connection directly. `Connection.query/3` never bumps
    # `WriteCounter` — that is the executor's job — so a shard written to underneath it reads
    # CLEAN, and `flush_now` then returns `:ok` having flushed nothing. The first version of these
    # tests did exactly that and reported "no stamp" for a flush that never ran.
    setup do
      id = "flushpos_#{System.unique_integer([:positive])}"

      on_exit(fn ->
        Shards.stop(id)
        for s <- ["", "-wal", "-shm", ".etag"], do: File.rm(Fathom.Shard.db_path(id) <> s)
      end)

      {:ok, coordinator} = Shards.ensure(id)
      {:ok, conn} = ShardExecutor.open(id)
      on_exit(fn -> ShardExecutor.close(conn) end)

      %{id: id, coordinator: coordinator, conn: conn, path: Fathom.Shard.db_path(id)}
    end

    defp stmt(sql, args \\ []), do: %Filo.Stmt{sql: sql, args: args}

    # The snapshot + upload run off-process, so syncing the mailbox is not enough.
    defp flush!(coordinator) do
      send(coordinator, :durability_flush)
      settle(coordinator, 400)
    end

    defp settle(_coordinator, 0), do: flunk("durability flush task never settled")

    defp settle(coordinator, tries) do
      if :sys.get_state(coordinator).flush_task == nil do
        :ok
      else
        Process.sleep(10)
        settle(coordinator, tries - 1)
      end
    end

    test "a flush stamps the object and the stamp advances with writes", ctx do
      %{id: id, coordinator: coordinator, conn: conn} = ctx

      {:ok, _} = ShardExecutor.execute(conn, stmt("CREATE TABLE t (a INTEGER)"))
      flush!(coordinator)

      assert {:ok, first} = Storage.object_position(id)
      refute is_nil(first), "the flush wrote no position stamp"
      assert first.epoch > 0

      for i <- 1..20 do
        {:ok, _} = ShardExecutor.execute(conn, stmt("INSERT INTO t VALUES (?1)", [i]))
      end

      flush!(coordinator)
      assert {:ok, second} = Storage.object_position(id)

      assert {second.epoch, second.wal_gen, second.offset} >=
               {first.epoch, first.wal_gen, first.offset},
             "the stamp went BACKWARDS across a flush"

      refute second == first, "the stamp did not move across a flush that wrote 20 rows"
    end

    # THE DIRECTION TEST. Everything the stamp is for rests on it claiming at least as much as the
    # object holds, so this pins it against the WAL state that existed BEFORE the flush ran: the
    # object cannot contain more than that plus whatever landed during it, and the stamp must not
    # be behind it.
    test "over_claims_never_under_claims", ctx do
      %{id: id, coordinator: coordinator, conn: conn, path: path} = ctx

      {:ok, _} = ShardExecutor.execute(conn, stmt("CREATE TABLE t (a INTEGER, b TEXT)"))

      for i <- 1..200 do
        {:ok, _} =
          ShardExecutor.execute(
            conn,
            stmt("INSERT INTO t VALUES (?1, ?2)", [i, String.duplicate("x", 200)])
          )
      end

      before = File.stat!(path <> "-wal").size
      assert before > 0, "no WAL to measure against — this test would prove nothing"

      flush!(coordinator)
      assert {:ok, pos} = Storage.object_position(id)

      assert pos.offset >= before,
             "the stamp (#{pos.offset}) is BEHIND the WAL that existed before the flush " <>
               "(#{before}) — it is under-claiming, and a replica between the two would be " <>
               "promoted over bytes the object actually holds"
    end

    # The real read-order guard. Structural rather than behavioural, deliberately: the runtime
    # difference only appears when a write lands during the snapshot, which cannot be forced
    # deterministically (see the moduledoc). Hoisting the read above the snapshot is a one-line
    # change that looks equivalent and silently converts an over-claim into an under-claim, so the
    # thing worth pinning is where the call sits.
    test "reads_the_position_after_the_snapshot" do
      source = File.read!("lib/fathom/shard.ex")

      [_, body] = String.split(source, "defp snapshot_and_upload(state) do", parts: 2)
      [body, _] = String.split(body, "\n  defp ", parts: 2)

      snapshot_at = :binary.match(body, "snapshot(state.path, temp)") |> elem(0)
      position_at = :binary.match(body, "flush_position(state)") |> elem(0)

      assert position_at > snapshot_at,
             "flush_position/1 is read BEFORE the snapshot in snapshot_and_upload/1. That makes " <>
               "the stamp UNDER-claim: the object then contains more than it says, and a replica " <>
               "sitting between the claim and the truth is judged fresher and promoted over it — " <>
               "silently dropping the writes that landed during the snapshot. Read it after."
    end

    test "an object flushed without a stamp reports none", ctx do
      %{id: id, conn: conn, path: path} = ctx

      {:ok, _} = ShardExecutor.execute(conn, stmt("CREATE TABLE t (a INTEGER)"))

      # The unfenced 2-arity flush is what migration copies and benchmarks use; it must leave the
      # object unstamped rather than carrying somebody else's position.
      :ok = Storage.flush(id, path)
      assert {:ok, nil} = Storage.object_position(id)
    end
  end
end
