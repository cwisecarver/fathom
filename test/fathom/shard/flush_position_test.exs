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
  alias Fathom.Shard.Replication.Wal
  alias Fathom.Shard.Storage
  alias Fathom.ShardExecutor
  alias Fathom.Shards

  describe "encode/parse round trip" do
    test "round-trips a position" do
      pos = %{epoch: 7, wal_gen: 3, offset: 12_392}
      assert Storage.parse_position(Storage.encode_position(pos)) == pos
    end

    # THIS TEST PREVIOUSLY ASSERTED THE OPPOSITE FIELD ORDER — `"7:3:12392:978380554"`, salt fourth
    # — and updating it is deliberate (expert review 2026-08-26 #2, step 3).
    #
    # The salt-bearing form was groundwork written before #2's design settled, when the plan was to
    # rank on the salt. It is not: a salt is an IDENTITY, so the most it can say is "these are
    # different WALs", and refusing to rank on that turns promote inert for the common
    # post-checkpoint case. The settled design ranks on a monotone ORDINAL, and that made the field
    # order matter for a concrete reason: `position_after_checkpoint/3` has an ordinal to over-claim
    # for a WAL that no longer exists, so it has no current salt to name. The ranking field has to
    # be expressible ALONE, which means fourth.
    #
    # Reordering was safe because NOTHING EVER WROTE a four-field stamp — the salt clause was
    # reachable only from a `position` map carrying `:salt1`, and no code path built one. That was
    # verified by grep before the change; this test is the thing that will notice if it stops
    # being true.
    test "round-trips the ordinal-bearing form, and treats a three-field stamp as unknown" do
      with_ord = %{epoch: 7, wal_gen: 3, offset: 12_392, wal_ordinal: 11}
      assert Storage.encode_position(with_ord) == "7:3:12392:11"
      assert Storage.parse_position(Storage.encode_position(with_ord)) == with_ord

      # …and the salt rides fifth when there IS a live WAL to name.
      with_both = Map.put(with_ord, :salt1, 978_380_554)
      assert Storage.encode_position(with_both) == "7:3:12392:11:978380554"
      assert Storage.parse_position(Storage.encode_position(with_both)) == with_both

      # A stamp written before #2 parses as before and simply carries no ordinal — ABSENT, not
      # zero. `Promote.fresher?/2` must read that as "unknown ⇒ the object wins", never as a zeroth
      # generation it can rank against.
      parsed = Storage.parse_position("7:3:12392")
      assert parsed == %{epoch: 7, wal_gen: 3, offset: 12_392}
      refute Map.has_key?(parsed, :wal_ordinal)

      # A malformed field refuses the WHOLE stamp rather than degrading to a shorter form: this
      # function's rule is that anything unexpected is nil, never a partial guess.
      assert Storage.parse_position("7:3:12392:x") == nil
      assert Storage.parse_position("7:3:12392:-1") == nil
      assert Storage.parse_position("7:3:12392:11:x") == nil
      assert Storage.parse_position("7:3:12392:11:-1") == nil
    end

    # Every one of these must be `nil` rather than a guess. `nil` means "unknown", and the only
    # consumer treats unknown as "never override the stored object" — so garbage degrades to the
    # safe answer instead of a fabricated ordering that could discard a lineage.
    test "refuses anything that is not exactly a position" do
      for bad <- [
            nil,
            "",
            "1:2",
            # "1:2:3:4" was here until expert review 2026-08-26 #2 — see the round-trip test above
            # for why four fields is now a VALID stamp. "1:2:3:4:5" left the list at step 3, when
            # the salt joined the ordinal as a fifth field. Six still is not a stamp.
            "1:2:3:4:5:6",
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
    # THE FIRST FIELD IS THE LINEAGE ON BOTH SIDES (expert review 2026-08-24 #12). This helper used
    # to build the replica with `epoch:` and it was the LOCK epoch — a counter `release_lease`
    # resets to 1 on every clean drop — while the stamp's `epoch:` slot carries the monotonic
    # lineage. Same field name, two different counters, and the comparison silently meant nothing
    # from a shard's second replicating open onward. The replica now carries both: `lineage` for
    # ranking, `epoch` for `FollowerLog.decide/2`'s fencing check, which is a different question.
    #
    # THE SECOND ARGUMENT IS NOW THE ORDINAL (expert review 2026-08-26 #2, step 3b). `fresher?/2`
    # ranks on `{lineage, wal_ordinal, offset}`; `wal_gen` is SQLite's `ckpt_seq` and restarts at 0
    # on a WAL recreate, so it never was an order. Both helpers still set `wal_gen` because
    # `FollowerLog.decide/2` reads it for a different question.
    #
    # `g + 1`, not `g`: an ordinal of 0 means "NOT STATED" and is refused outright, so a fixture
    # written as generation 0 would stop being ranked rather than ranking lowest. The shift is
    # monotone, so every ordering asserted below is unchanged.
    defp replica(l, g, o),
      do: %{lineage: l, epoch: 1, wal_gen: g, wal_ordinal: g + 1, next_offset: o}

    defp stamp(l, g, o), do: %{epoch: l, wal_gen: g, wal_ordinal: g + 1, offset: o}

    test "orders lexicographically on lineage, then WAL ordinal, then offset" do
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

    # THE 3b GUARD. An ordinal of 0 is "not stated" — a replica seeded but not yet pushed to, or one
    # whose primary has `Protocol.ordinal_wire?/0` off, which is the DEFAULT. It must answer false,
    # and specifically not "equal": equal would be a claim about a relationship nobody measured.
    #
    # This is what makes A2 promote inert until `REPLICATION_ORDINAL_WIRE` is rolled out, and that
    # is the intended trade. The ordering it replaces could rank two unrelated WALs and promote a
    # replica over a NEWER object, losing the acked tail silently.
    test "a replica with no stated ordinal is never fresher, however far ahead it reads" do
      refute Promote.fresher?(
               %{lineage: 5, epoch: 1, wal_gen: 99, wal_ordinal: 0, next_offset: 999_999},
               stamp(5, 0, 0)
             )
    end

    # …and the object's side of the same rule. A stamp written before step 3a, or one whose salt
    # the coordinator had never seen, carries NO `:wal_ordinal` key at all.
    test "an object stamped with no ordinal is never overridable" do
      refute Promote.fresher?(replica(5, 99, 999_999), %{epoch: 5, wal_gen: 0, offset: 0})
    end

    # A replica seeded before the lineage travelled on the wire — or while
    # `Protocol.lineage_wire?/0` is off — carries 0, meaning "not stated". It must never be ranked,
    # in EITHER direction: the honest answer is that we do not know how it relates to the object,
    # and falling back to the stored object is what "we do not know" means here. This is what keeps
    # the two-step rollout inert rather than dangerous while the fleet catches up.
    test "a replica with no stated lineage is never fresher, however far ahead it reads" do
      refute Promote.fresher?(
               %{lineage: 0, epoch: 9, wal_gen: 99, next_offset: 999_999},
               stamp(1, 0, 0)
             )

      # …and the same on the object's side: an object stamped before lineages existed.
      refute Promote.fresher?(replica(9, 99, 999_999), stamp(0, 0, 0))
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

      # The call was `snapshot(state.path, temp)` until expert review 2026-08-26 #12 folded the
      # integrity check and the snapshot onto one connection as `verify_and_snapshot/2`. Only the
      # NAME moved — the ordering invariant this test pins is unchanged, and the snapshot is still
      # the thing the position read must come after.
      snapshot_at =
        case :binary.match(body, "verify_and_snapshot(state, temp)") do
          {at, _} ->
            at

          :nomatch ->
            flunk(
              "could not find the snapshot call in snapshot_and_upload/1. This test pins a " <>
                "read ORDER by reading the source, so a rename breaks it by design — re-point it " <>
                "at whatever now performs the snapshot rather than deleting the guard."
            )
        end

      position_at = :binary.match(body, "flush_position(state, pre)") |> elem(0)

      assert position_at > snapshot_at,
             "flush_position is read BEFORE the snapshot in snapshot_and_upload/1. That makes " <>
               "the stamp UNDER-claim: the object then contains more than it says, and a replica " <>
               "sitting between the claim and the truth is judged fresher and promoted over it — " <>
               "silently dropping the writes that landed during the snapshot. Read it after."

      # The OTHER half of the same invariant (expert review 2026-08-20 #4). The live read above
      # must stay after the snapshot; the `pre` header it falls back to must be captured BEFORE
      # anything mutates the WAL, or the fallback is measuring the same destroyed file the live
      # read already failed on.
      pre_at = :binary.match(body, "pre = Fathom.Shard.Replication.Wal.read") |> elem(0)

      assert pre_at < snapshot_at,
             "the `pre` WAL header is captured AFTER the snapshot in snapshot_and_upload/1. " <>
               "snapshot/2 is frequently the last connection and its close unlinks -wal, so a " <>
               "late capture reads :empty and the stamp collapses to {epoch, 0, 0} — the lowest " <>
               "position for the epoch, on a complete object. Capture it first."
    end

    # THE SAME GUARD ON THE OTHER PATH (expert review 2026-08-20 #38). The one above scoped itself
    # to `snapshot_and_upload/1`, so it could not see `upload_for_drop/1` at all — and the drop
    # path is where #4 bit hardest: `checkpoint_and_verify/1` runs `wal_checkpoint(TRUNCATE)` and
    # then closes the connection, which on the LAST one unlinks `-wal`, so a late read finds
    # nothing and stamps `{epoch, 0, 0}` on the most complete copy of the shard that will ever
    # exist. Every clean idle-drop, graceful drain and rebalance handoff went out that way.
    #
    # Structural for the same reason as its sibling: the runtime difference needs a write landing
    # inside the checkpoint, and hoisting the read is a one-line change that looks equivalent.
    test "the drop path captures the WAL header before the checkpoint destroys it" do
      source = File.read!("lib/fathom/shard.ex")

      [_, body] = String.split(source, "defp upload_for_drop(state) do", parts: 2)
      [body, _] = String.split(body, "\n  defp ", parts: 2)

      # Expert review 2026-08-26 #3 split the checkpoint out into `checkpoint_then_upload/2`, so
      # `upload_for_drop/1` now holds the WAL read and the ROUTE. This guards the outer half: the
      # header is captured before either route can run. Its sibling below guards the inner half.
      pre_at = :binary.match(body, "pre = Fathom.Shard.Replication.Wal.read") |> elem(0)
      route_at = :binary.match(body, "if map_size(state.conns) > 0") |> elem(0)

      assert pre_at < route_at,
             "upload_for_drop/1 reads the WAL header AFTER routing. Either route can destroy it " <>
               "— the checkpoint route truncates the WAL and the connection close unlinks it — so " <>
               "a late read finds :empty and the object is stamped {epoch, 0, 0}: the LOWEST " <>
               "position for its epoch, on the most complete copy of the shard. Any lagging " <>
               "replica then outranks it and is promoted over it."
    end

    # The drop path is where this bug was DETERMINISTIC, and it had no guard at all — the one
    # above was scoped to snapshot_and_upload/1 by a string split on that function name, so the
    # path that stamps every clean idle-drop, graceful drain and rebalance handoff was unwatched.
    test "captures_the_generation_before_the_checkpoint_on_the_drop_path" do
      source = File.read!("lib/fathom/shard.ex")

      # THE ORDERING NOW SPANS TWO FUNCTIONS (expert review 2026-08-26 #3). `upload_for_drop/1`
      # reads the WAL header and then ROUTES: a shard with connections still checked out goes to
      # `snapshot_and_upload/1`, and only an idle one reaches `checkpoint_then_upload/2`, which is
      # where the checkpoint and the stamp now live. The invariant is unchanged — the header must
      # be captured before any checkpoint, and the position computed after it — so the guard is
      # re-scoped rather than relaxed, asserted once per function.
      drop_body = fn name ->
        [_, body] = String.split(source, "defp #{name}", parts: 2)
        [body, _] = String.split(body, "\n  defp ", parts: 2)
        body
      end

      inner = drop_body.("checkpoint_then_upload(state, pre) do")

      checkpoint_at = :binary.match(inner, "checkpoint_and_verify(state.path)") |> elem(0)
      position_at = :binary.match(inner, "flush_position(state, pre)") |> elem(0)

      assert position_at > checkpoint_at,
             "checkpoint_then_upload/2 computes the position BEFORE the checkpoint, which " <>
               "under-claims for the same reason the snapshot path must not."
    end

    # THE ORDINAL, WHICH IS WHAT MAKES THE STAMP RANKABLE (expert review 2026-08-26 #2, step 3).
    #
    # Positive and negative in one place because the negative is the whole safety argument: an
    # ordinal is stamped ONLY when the coordinator has already assigned one for the salt the flush
    # reads off the WAL. That is what keeps the object and the replicas on ONE scale — the same
    # coordinator answers both, and `wal_ordinal/2` returns the same number for the same salt — and
    # a coordinator that has never seen this WAL has no number the replicas were also given, so it
    # states none rather than inventing one.
    test "the stamp carries the ordinal the coordinator assigned for THIS WAL", ctx do
      %{id: id, coordinator: coordinator, conn: conn, path: path} = ctx

      {:ok, _} = ShardExecutor.execute(conn, stmt("CREATE TABLE t (a INTEGER)"))
      {:ok, _} = ShardExecutor.execute(conn, stmt("INSERT INTO t VALUES (1)"))

      # NEGATIVE FIRST, on the same shard: nothing has asked for an ordinal, so the coordinator is
      # standing on no salt and must refuse to state one. With replication off this is every flush,
      # which is why it is the case that has to be silent rather than zero.
      flush!(coordinator)
      assert {:ok, unstamped} = Storage.object_position(id)

      refute Map.has_key?(unstamped, :wal_ordinal),
             "an ordinal was stamped for a WAL the coordinator has never seen — the object and " <>
               "its replicas would then be on two different scales, which is the #2 defect rather " <>
               "than the fix"

      # Now do what `Replication.Session` does on every salt change — and do it AFTER the write
      # that will be flushed, not before. The first flush checkpoints, SQLite then RESTARTS the log
      # on the next write with FRESH SALTS, and a salt read any earlier names a WAL that no longer
      # exists by the time the flush reads it. (Which is the #2 defect itself, observed from the
      # other side: the first draft of this test read the salt before the write and the stamp came
      # back with no ordinal at all.)
      {:ok, _} = ShardExecutor.execute(conn, stmt("INSERT INTO t VALUES (2)"))

      assert {:ok, %{salt1: salt}} = Wal.read(path <> "-wal")
      ordinal = Fathom.Shard.wal_ordinal(coordinator, salt)

      assert ordinal > 0,
             "fixture: the coordinator assigned no ordinal, so the positive half below would " <>
               "pass for the same reason the negative half does"

      flush!(coordinator)
      assert {:ok, stamped} = Storage.object_position(id)

      assert stamped.wal_ordinal == ordinal,
             "the object claims a different ordinal than the one the replicas were pushed"

      # …and the salt rides along to name the WAL that ordinal belongs to.
      assert stamped.salt1 == salt
    end

    # THE OVER-CLAIM HAS TO APPLY TO THE ORDINAL TOO, and getting this wrong inverts the fix.
    #
    # A checkpoint folds the WAL into the database, so the object then holds everything through the
    # end of that WAL. Once ranking is `{lineage, wal_ordinal, offset}`, stamping the folded WAL's
    # own ordinal N at offset 0 would LOSE to every replica still sitting in it at any offset — on
    # the most complete copy of the shard that exists. `N + 1` at offset 0 is the same reasoning
    # `wal_gen + 1` already uses, one field over.
    test "a checkpointed flush over-claims the ordinal, and names no salt" do
      source = File.read!("lib/fathom/shard.ex")

      [_, body] = String.split(source, "defp position_after_checkpoint(epoch,", parts: 2)
      [body, _] = String.split(body, "\n  # ", parts: 2)

      assert body =~ "stamp_ordinal(%{epoch: epoch, wal_gen: gen + 1, offset: 0}, state, salt, 1)",
             "position_after_checkpoint/3 no longer over-claims the ordinal. Stamping N rather " <>
               "than N + 1 makes every replica still in the folded WAL outrank the object that " <>
               "just absorbed it."

      # And the salt must NOT ride along here: it would assert the object sits inside a WAL it is
      # actually past. `put_salt/3` drops it whenever the ordinal was bumped.
      assert Storage.encode_position(%{epoch: 4, wal_gen: 9, offset: 0, wal_ordinal: 12}) ==
               "4:9:0:12"
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
