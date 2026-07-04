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
