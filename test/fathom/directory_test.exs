defmodule Fathom.DirectoryTest do
  use Fathom.DataCase, async: true

  alias Fathom.Directory
  alias Fathom.Directory.Shard

  describe "resolve/1" do
    test "registers a new shard at version 0 / active and records the access" do
      assert {:ok, %Shard{shard_id: "acme", schema_version: 0, status: "active"} = entry} =
               Directory.resolve("acme")

      assert entry.last_active_at
    end

    test "is idempotent: re-resolve bumps recency without resetting version or status" do
      {:ok, _} = Directory.resolve("acme")
      {:ok, _} = Directory.cutover("acme", 3)
      {:ok, _} = Directory.mark_migrating("acme")

      {:ok, again} = Directory.resolve("acme")
      assert again.schema_version == 3
      assert again.status == "migrating"
    end

    test "advances last_active_at on re-resolve" do
      {:ok, first} = Directory.resolve("acme")
      {:ok, second} = Directory.resolve("acme")
      assert DateTime.compare(second.last_active_at, first.last_active_at) in [:gt, :eq]
    end

    test "rejects an invalid shard id" do
      assert {:error, changeset} = Directory.resolve("bad/../id")
      refute changeset.valid?
    end
  end

  describe "bump_token_version/1" do
    # Round-2 #32: bump_token_version called resolve/1, phantom-bumping
    # last_active_at — a revoke is operator action, not tenant activity, so
    # revoking during an incident made the subsequent revert's write-age guard
    # cancel untouched shards (the #40 class via the token path). And the
    # {:ok, _} = resolve match CRASHED on an invalid id.
    test "does not bump last_active_at on an existing shard" do
      {:ok, before} = Directory.resolve("acme")
      {:ok, _} = Directory.cutover("acme", 1)
      {:ok, at_cutover} = Directory.get("acme")

      assert {:ok, 2} = Directory.bump_token_version("acme")

      {:ok, after_bump} = Directory.get("acme")

      assert DateTime.compare(after_bump.last_active_at, at_cutover.last_active_at) == :eq,
             "a revoke must not read as tenant activity (it would guard-cancel the next revert)"

      assert after_bump.token_version == before.token_version + 1
    end

    test "registers an unknown shard so the revoke is never lost" do
      assert {:ok, 2} = Directory.bump_token_version("brand_new")
      assert {:ok, %Shard{token_version: 2, schema_version: 0}} = Directory.get("brand_new")
    end

    test "returns an error tuple for an invalid id instead of crashing" do
      assert {:error, changeset} = Directory.bump_token_version("bad/../id")
      refute changeset.valid?
    end
  end

  describe "get/1" do
    test "returns :error for an unknown shard, the entry once registered" do
      assert Directory.get("missing") == :error
      {:ok, _} = Directory.resolve("acme")
      assert {:ok, %Shard{shard_id: "acme"}} = Directory.get("acme")
    end
  end

  describe "lifecycle transitions" do
    setup do
      {:ok, _} = Directory.resolve("acme")
      :ok
    end

    test "cutover sets the version and (re)activates" do
      {:ok, _} = Directory.mark_migrating("acme")
      assert {:ok, %Shard{schema_version: 5, status: "active"}} = Directory.cutover("acme", 5)
    end

    # The revert force-guard (finding #13) reads "activity since cutover" as strictly
    # last_active_at > cutover_at, which only works if cutover stamps BOTH with the same
    # instant — two separate now() calls would make every fresh cutover read as active.
    test "cutover stamps cutover_at and last_active_at with the same instant" do
      assert {:ok, %Shard{} = entry} = Directory.cutover("acme", 5)
      assert entry.cutover_at
      assert DateTime.compare(entry.cutover_at, entry.last_active_at) == :eq
    end

    # `retained_version` (expert review 2026-08-24 #16b) — which version this shard has a retained
    # copy of, so a fleet revert can tell whether the target it names is reachable for THIS shard.
    # `cutover/2` must leave it alone: a caller that retained nothing has nothing to record, and
    # writing nil there would erase a live recovery point.
    test "cutover/3 records the retained version; cutover/2 leaves it untouched" do
      assert {:ok, %Shard{retained_version: 4}} = Directory.cutover("acme", 5, 4)
      assert {:ok, %Shard{retained_version: 4}} = Directory.cutover("acme", 6)
    end

    # THE CONDITION IS THE TEST. `RetirementJob` clears this after dropping `<shard>@<version>`, so
    # the column stops claiming a copy that no longer exists — but a forward migration that ran
    # between the retirement's guards and its drop has already pointed the column at a NEWER copy.
    # An unconditional clear would erase that live recovery point's record while the object itself
    # survives, turning a revert that would have worked into a refusal.
    test "clear_retained_version only clears when it still names that version" do
      {:ok, _} = Directory.cutover("acme", 5, 4)

      assert :ok = Directory.clear_retained_version("acme", 3)

      assert {:ok, %Shard{retained_version: 4}} = Directory.get("acme"),
             "clearing a version the column does not name must be a no-op"

      assert :ok = Directory.clear_retained_version("acme", 4)
      assert {:ok, %Shard{retained_version: nil}} = Directory.get("acme")

      # Idempotent, and a shard it does not know is not an error — a retirement can outlive its
      # tenant, and raising there would strand the job.
      assert :ok = Directory.clear_retained_version("acme", 4)
      assert :ok = Directory.clear_retained_version("no-such-shard", 1)
    end

    test "mark_migrating / mark_failed set status" do
      assert {:ok, %Shard{status: "migrating"}} = Directory.mark_migrating("acme")
      assert {:ok, %Shard{status: "migration_failed"}} = Directory.mark_failed("acme")
    end

    test "retire sets status and retain_until" do
      retain_until = DateTime.add(DateTime.utc_now(), 7 * 24 * 3600, :second)

      assert {:ok, %Shard{status: "retired", retain_until: ^retain_until}} =
               Directory.retire("acme", retain_until)
    end

    test "transitions on an unknown shard return :not_found" do
      assert Directory.cutover("missing", 2) == {:error, :not_found}
      assert Directory.mark_migrating("missing") == {:error, :not_found}
      assert Directory.retire("missing", DateTime.utc_now()) == {:error, :not_found}
    end
  end

  describe "laggards/2 and count_laggards/1" do
    test "returns active shards behind HEAD, most-recently-used first" do
      {:ok, _} = Directory.resolve("old")
      {:ok, _} = Directory.resolve("new")

      assert Enum.map(Directory.laggards(1, 10), & &1.shard_id) == ["new", "old"]
      assert Directory.count_laggards(1) == 2
    end

    test "excludes up-to-date and non-active shards" do
      {:ok, _} = Directory.resolve("behind")
      {:ok, _} = Directory.resolve("current")
      {:ok, _} = Directory.cutover("current", 2)
      {:ok, _} = Directory.resolve("retired")
      {:ok, _} = Directory.retire("retired", DateTime.utc_now())

      assert Enum.map(Directory.laggards(2, 10), & &1.shard_id) == ["behind"]
      assert Directory.count_laggards(2) == 1
    end

    test "respects the limit" do
      for id <- ~w(a b c), do: Directory.resolve(id)
      assert length(Directory.laggards(1, 2)) == 2
    end
  end

  # Finding #20: a migration whose Oban job is lost leaves the shard in `migrating` forever,
  # and every laggard/reconcile query filters status == "active", so it is invisible to every
  # sweep and never converges. The reconcile reclaims rows stuck past a timeout back to active.
  describe "reclaim_stale_migrating/1 (finding #20)" do
    test "mark_migrating stamps migrating_since; cutover and mark_failed clear it" do
      {:ok, _} = Directory.resolve("m")

      {:ok, migrating} = Directory.mark_migrating("m")
      assert migrating.status == "migrating"
      assert migrating.migrating_since

      {:ok, cut} = Directory.cutover("m", 2)
      assert cut.status == "active"
      assert is_nil(cut.migrating_since)

      {:ok, _} = Directory.mark_migrating("m")
      {:ok, failed} = Directory.mark_failed("m")
      assert failed.status == "migration_failed"
      assert is_nil(failed.migrating_since)
    end

    test "flips shards stuck past the timeout back to active, leaving fresh/active alone" do
      {:ok, _} = Directory.resolve("stuck")
      {:ok, _} = Directory.mark_migrating("stuck")
      # Backdate its migrating_since to well past the timeout — the lost-job scenario.
      backdate_migrating("stuck", 7_200)

      {:ok, _} = Directory.resolve("fresh")
      {:ok, _} = Directory.mark_migrating("fresh")

      {:ok, _} = Directory.resolve("plain")

      assert Directory.reclaim_stale_migrating(3_600) == ["stuck"]

      assert {:ok, %Shard{status: "active", migrating_since: nil}} = Directory.get("stuck")
      assert {:ok, %Shard{status: "migrating"}} = Directory.get("fresh")
      assert {:ok, %Shard{status: "active"}} = Directory.get("plain")
    end

    # Expert review 2026-07-18 #11: mark_migrating stamps migrating_since ONCE, so a genuinely
    # running >stale_after copy was flipped back to `active` mid-run. touch_migrating (called by the
    # migration lease renewer on the lease cadence) renews the stamp — a live long copy keeps a
    # fresh stamp and is NOT reclaimed; only a lost-job migration (renewer dead, stamp goes stale)
    # is reclaimed.
    test "touch_migrating renews the stamp so a live long migration is not reclaimed" do
      {:ok, _} = Directory.resolve("live_long")
      {:ok, _} = Directory.mark_migrating("live_long")
      # The copy started 2h ago — well past the 1h timeout — but the renewer is alive.
      backdate_migrating("live_long", 7_200)

      # A renewer tick lands, renewing the stamp to now.
      assert Directory.touch_migrating("live_long") == 1

      # With a fresh stamp it survives the reclaim sweep (pre-fix: nothing renewed it → reclaimed).
      assert Directory.reclaim_stale_migrating(3_600) == []
      assert {:ok, %Shard{status: "migrating"}} = Directory.get("live_long")

      # Contrast: once the renewer stops (job lost) the stamp goes stale and it IS reclaimed —
      # proving the stamp, not wall-clock-since-mark, is the signal.
      backdate_migrating("live_long", 7_200)
      assert Directory.reclaim_stale_migrating(3_600) == ["live_long"]
      assert {:ok, %Shard{status: "active"}} = Directory.get("live_long")
    end

    test "touch_migrating is a no-op for a non-migrating shard (never resurrects it)" do
      # A fork holds the lease and drives the renewer, but its dst is never `migrating` — the touch
      # must not stamp migrating_since onto an active row.
      {:ok, _} = Directory.resolve("not_migrating")
      assert Directory.touch_migrating("not_migrating") == 0

      assert {:ok, %Shard{status: "active", migrating_since: nil}} =
               Directory.get("not_migrating")

      # And an unknown shard is simply 0 rows, not an error.
      assert Directory.touch_migrating("no_such_shard") == 0
    end
  end

  # Expert review 2026-07-18 #12: shards_at_version/1 is an unbounded Repo.all — a fleet revert
  # (millions of shards at a version) materialized every one as a full struct. revert/3 now
  # keyset-streams the ids in pages and revert_status/1 uses an aggregate count, so memory stays
  # bounded. These pin the two new primitives: the aggregate count and the keyset pagination
  # (which must enumerate the full set exactly once across page boundaries — an off-by-one on the
  # `shard_id > last` cursor would drop or duplicate shards from a revert).
  describe "count_at_version/1 and stream_ids_at_version/2 (finding #12)" do
    test "count_at_version aggregates the active set at a version, excluding others" do
      for id <- ~w(a b c), do: Directory.resolve(id)
      {:ok, _} = Directory.cutover("a", 5)
      {:ok, _} = Directory.cutover("b", 5)
      # c stays at version 0.
      {:ok, _} = Directory.resolve("ret")
      {:ok, _} = Directory.cutover("ret", 5)
      {:ok, _} = Directory.retire("ret", DateTime.utc_now())

      assert Directory.count_at_version(5) == 2, "only active shards at v5 (ret is retired)"
      assert Directory.count_at_version(0) == 1
      assert Directory.count_at_version(99) == 0
    end

    test "stream_ids_at_version keyset-pages the full active set exactly once across boundaries" do
      ids = for n <- 1..7, do: "sv#{n}"

      for id <- ids do
        {:ok, _} = Directory.resolve(id)
        {:ok, _} = Directory.cutover(id, 5)
      end

      # A non-active shard at the same version must be excluded.
      {:ok, _} = Directory.resolve("sv_gone")
      {:ok, _} = Directory.cutover("sv_gone", 5)
      {:ok, _} = Directory.mark_failed("sv_gone")

      # page_size 3 < 7 forces multiple keyset pages (3 + 3 + 1).
      streamed = Directory.stream_ids_at_version(5, 3) |> Enum.to_list()

      assert Enum.sort(streamed) == Enum.sort(ids)
      assert length(streamed) == 7, "no dupes or misses across the keyset page boundary"
      refute "sv_gone" in streamed
    end

    test "stream_ids_at_version yields nothing for an empty set" do
      assert Directory.stream_ids_at_version(42, 3) |> Enum.to_list() == []
    end
  end

  # Finding #29: touches are coalesced and batch-flushed, and a stale flush (out-of-order
  # across a remap) must not rewind last_active_at — the recency heuristics depend on it.
  # record_batch keeps the newer of incoming vs stored (GREATEST).
  describe "record_batch/1 recency monotonicity (finding #29)" do
    test "a stale coalesced flush never moves last_active_at backward" do
      {:ok, first} = Directory.resolve("recency")
      t1 = first.last_active_at
      older = DateTime.add(t1, -60, :second)

      assert Directory.record_batch([{"recency", older}]) == 1
      {:ok, after_stale} = Directory.get("recency")
      assert DateTime.compare(after_stale.last_active_at, t1) == :eq

      # A genuinely newer touch still advances recency.
      newer = DateTime.add(t1, 60, :second)
      assert Directory.record_batch([{"recency", newer}]) == 1
      {:ok, after_fresh} = Directory.get("recency")
      assert DateTime.compare(after_fresh.last_active_at, t1) == :gt
    end
  end

  # Finding #12: `status` was in no index, so every status='active'-filtered query
  # (active_recent every warm-follower poll; laggards/count_laggards every reconcile +
  # rollout sweep; shards_at_version on revert) sequentially scanned the whole shards
  # table — linear in fleet size (millions at target scale). These partial indexes make
  # the status-filtered reads index-bound. Pin their existence + partial predicate so a
  # future migration can't silently drop the fleet back to full scans.
  describe "status-filtered query indexes (finding #12)" do
    test "partial indexes on active shards exist for active_recent and the laggard path" do
      defs =
        Fathom.Repo.query!(
          "SELECT indexname, indexdef FROM pg_indexes WHERE tablename = 'shards'"
        ).rows
        |> Map.new(fn [name, definition] -> {name, definition} end)

      active_recent_idx = defs["shards_active_last_active_at_index"]
      assert active_recent_idx =~ "(last_active_at)"
      assert active_recent_idx =~ ~r/where.*status.*=.*'active'/i

      laggard_idx = defs["shards_active_schema_version_last_active_at_index"]
      assert laggard_idx =~ "(schema_version, last_active_at)"
      assert laggard_idx =~ ~r/where.*status.*=.*'active'/i
    end
  end

  describe "tombstone/1 (tenant deletion, #15)" do
    test "flips an existing row to deleted" do
      {:ok, _} = Directory.resolve("acme")

      assert {:ok, %Shard{shard_id: "acme", status: "deleted"}} = Directory.tombstone("acme")
      assert {:ok, %Shard{status: "deleted"}} = Directory.get("acme")
    end

    test "registers a deleted row for a shard the directory never recorded" do
      assert {:ok, %Shard{shard_id: "ghost", status: "deleted"}} = Directory.tombstone("ghost")
      assert {:ok, %Shard{status: "deleted"}} = Directory.get("ghost")
    end

    test "a tombstoned shard is NOT resurrected by a later resolve/record_batch" do
      {:ok, _} = Directory.resolve("acme")
      {:ok, _} = Directory.tombstone("acme")

      # A stray access after deletion only bumps recency — status stays deleted (the
      # on-conflict never resets status), so the directory can't un-delete a tenant.
      {:ok, again} = Directory.resolve("acme")
      assert again.status == "deleted"

      Directory.record_batch([{"acme", DateTime.utc_now()}])
      assert {:ok, %Shard{status: "deleted"}} = Directory.get("acme")
    end

    test "deleted_shard_ids/0 returns exactly the tombstoned ids" do
      {:ok, _} = Directory.resolve("live1")
      {:ok, _} = Directory.tombstone("dead1")
      {:ok, _} = Directory.tombstone("dead2")

      ids = Directory.deleted_shard_ids()
      assert "dead1" in ids and "dead2" in ids
      refute "live1" in ids
    end
  end

  # Expert review 2026-07-24 #15: `all/0` materialized every row as a struct and `Reconcile`
  # applied `:limit` in Elixir afterwards — so `--limit 100` still pulled the whole table, and at
  # fleet scale that is hundreds of MB in one process. An OOM, not a slow query, on the documented
  # DR-completion path.
  describe "all_paged/1 (#15)" do
    test "walks every row exactly once, in shard_id order, across page boundaries" do
      ids = for i <- 1..7, do: "pg_#{String.pad_leading("#{i}", 2, "0")}"
      for id <- ids, do: {:ok, _} = Directory.resolve(id)

      # Page size 2 against 7 rows: several full pages plus a short final one.
      walked = Directory.all_paged(2) |> Enum.map(& &1.shard_id)

      assert walked == Enum.sort(walked), "keyset pagination must yield shard_id order"
      assert Enum.uniq(walked) == walked, "no row may be visited twice"
      assert Enum.sort(ids) == Enum.sort(walked -- (walked -- ids))
    end

    # The boundary that breaks a hand-rolled cursor loop: a final page that is exactly full, so the
    # walk must issue one more query and get an empty result rather than stopping early or looping.
    test "terminates when the last page is exactly full" do
      ids = for i <- 1..4, do: "pgx_#{i}"
      for id <- ids, do: {:ok, _} = Directory.resolve(id)

      walked = Directory.all_paged(2) |> Enum.map(& &1.shard_id)

      for id <- ids, do: assert(id in walked)
    end

    test "is lazy — taking N does not walk the whole table" do
      for i <- 1..6, do: {:ok, _} = Directory.resolve("pgl_#{i}")

      taken = Directory.all_paged(2) |> Stream.take(3) |> Enum.to_list()
      assert length(taken) == 3
    end
  end

  # Expert review 2026-07-24 #12: the only status indexes were partial on `status = 'active'`, and
  # Postgres cannot derive `status = 'active'` from `status = 'deleted'` — so every non-active
  # status predicate sequentially scanned the whole table, twice per node every 5 minutes for
  # Tombstones + Suspensions alone.
  #
  # A partial index is only worth anything if its PREDICATE MATCHES the query — an index whose
  # predicate the planner can't imply is dead weight at any scale, and that mismatch is invisible
  # on a small table because a seq scan wins anyway. `enable_seqscan = off` forces the planner to
  # reveal whether it *can* use the index, which is exactly the property under test. (Volume-based
  # plan verification belongs in scripts/directory_scale.exs, not the unit suite.)
  describe "non-active status partial indexes (#12)" do
    for {status, index} <- [
          {"deleted", "shards_deleted_index"},
          {"suspended", "shards_suspended_index"},
          {"migration_failed", "shards_migration_failed_index"}
        ] do
      test "a status = '#{status}' predicate can use #{index}" do
        status = unquote(status)
        index = unquote(index)

        %{rows: [[exists]]} =
          Fathom.Repo.query!(
            "SELECT count(*) FROM pg_indexes WHERE tablename = 'shards' AND indexname = $1",
            [index]
          )

        assert exists == 1, "#{index} is missing — the migration did not apply"

        Fathom.Repo.query!("SET LOCAL enable_seqscan = off")

        %{rows: rows} =
          Fathom.Repo.query!(
            "EXPLAIN SELECT s0.shard_id FROM shards AS s0 WHERE s0.status = $1",
            [status]
          )

        plan = rows |> List.flatten() |> Enum.join("\n")

        # Assert an INDEX is used, not which one. #30 added shards_deleted_updated_at_index, which
        # is also valid for this predicate (partial on the same status, with shard_id INCLUDEd), so
        # the planner may legitimately prefer either. The property under test is that the predicate
        # can be served by an index at all rather than falling back to a scan — pinning a specific
        # index name made this brittle against any future index on the same rows.
        assert plan =~ "Index" and not (plan =~ "Seq Scan"),
               "the status = '#{status}' predicate is not served by any index, so it still scans " <>
                 "the whole table. Expected #{index} or an equivalent. Plan:\n#{plan}"
      end
    end
  end

  # Review #35: the non-partial (schema_version, last_active_at) index from 20260628001559 was
  # superseded by the `WHERE status = 'active'` partial one from 20260702011500, because every
  # schema_version predicate in lib/ also filters status = 'active'. It was serving no read while
  # still taking a write on every Recorder flush — `last_active_at` is in three of the indexes on
  # this table, so HOT updates are already impossible and every flush writes an entry to EVERY
  # index. Dropping it is ~20% fewer index-entry writes on the directory's hottest write path.
  describe "redundant schema_version index (#35)" do
    test "the non-partial index is gone" do
      %{rows: [[exists]]} =
        Fathom.Repo.query!(
          "SELECT count(*) FROM pg_indexes WHERE tablename = 'shards' AND indexname = $1",
          ["shards_schema_version_last_active_at_index"]
        )

      assert exists == 0,
             "the redundant non-partial index is still present — the drop migration did not apply"
    end

    # The point of the drop is that it costs no read. Same technique as the #12 tests above:
    # force the planner to reveal whether an index CAN serve the predicate.
    test "the laggard predicate is still served by an index, not a seq scan" do
      Fathom.Repo.query!("SET LOCAL enable_seqscan = off")

      %{rows: rows} =
        Fathom.Repo.query!(
          "EXPLAIN SELECT s0.shard_id FROM shards AS s0 " <>
            "WHERE s0.schema_version < $1 AND s0.status = 'active' " <>
            "ORDER BY s0.last_active_at DESC",
          [5]
        )

      plan = rows |> List.flatten() |> Enum.join("\n")

      assert plan =~ "Index" and not (plan =~ "Seq Scan"),
             "dropping the non-partial index cost a read path — the schema_version + active " <>
               "predicate now scans. The partial index should still serve it. Plan:\n#{plan}"
    end
  end

  describe "flush accounting (#28)" do
    test "record_flush_batch sets last_flushed_at and leaves last_active_at alone" do
      {:ok, row} = Directory.resolve("fa")
      assert row.last_flushed_at == nil

      flushed = DateTime.utc_now()
      assert 1 = Directory.record_flush_batch([{"fa", flushed}])

      {:ok, updated} = Directory.get("fa")
      assert updated.last_flushed_at
      assert DateTime.compare(updated.last_active_at, row.last_active_at) == :eq
    end

    test "record_flush_batch advances but never rewinds the watermark (GREATEST)" do
      {:ok, _} = Directory.resolve("fa2")
      t2 = DateTime.utc_now() |> DateTime.truncate(:microsecond)
      t1 = DateTime.add(t2, -60, :second)

      Directory.record_flush_batch([{"fa2", t2}])
      Directory.record_flush_batch([{"fa2", t1}])

      {:ok, row} = Directory.get("fa2")
      assert DateTime.compare(row.last_flushed_at, t2) == :eq
    end

    test "flush_lag_report lists shards active since their last flush, excluding clean/deleted" do
      now = DateTime.utc_now()

      {:ok, _} = Directory.resolve("fl_dirty")
      Directory.record_flush_batch([{"fl_dirty", DateTime.add(now, -60, :second)}])

      {:ok, _} = Directory.resolve("fl_clean")
      Directory.record_flush_batch([{"fl_clean", DateTime.add(now, 60, :second)}])

      {:ok, _} = Directory.resolve("fl_never")

      {:ok, _} = Directory.resolve("fl_gone")
      Directory.record_flush_batch([{"fl_gone", DateTime.add(now, -60, :second)}])
      {:ok, _} = Directory.tombstone("fl_gone")

      ids = Directory.flush_lag_report(100) |> Enum.map(& &1.shard_id)

      assert "fl_dirty" in ids, "active since last flush ⇒ potentially lost"
      assert "fl_never" in ids, "never flushed ⇒ potentially lost"
      refute "fl_clean" in ids, "flushed after last activity ⇒ not lost"
      refute "fl_gone" in ids, "deleted ⇒ excluded"
    end
  end

  # Push a shard's migrating_since into the past so reclaim_stale_migrating sees it as stuck,
  # without a sleep (the real clock is only ever advanced by the migration lifecycle).
  defp backdate_migrating(shard_id, seconds) do
    ts = DateTime.add(DateTime.utc_now(), -seconds, :second)
    {:ok, shard} = Directory.get(shard_id)

    shard
    |> Ecto.Changeset.change(migrating_since: ts)
    |> Fathom.Repo.update!()
  end
end
