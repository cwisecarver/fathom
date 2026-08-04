defmodule Fathom.Migrator.RolloutRateTest do
  @moduledoc """
  Fleet rollout rate + ETA (expert review 2026-08-01 #43).

  `ReconcileJob` converges the cold tail at `:reconcile_batch_size` per hourly cron, and the
  finding was that an operator had nothing to raise that knob *from*: `Migrator.status/0`
  reported head/laggards/failed/converged and no throughput at all.

  These pin the two things that make the rate meaningful rather than merely present: it counts
  only shards that reached HEAD (a **revert** stamps `cutover_at` on the way back down and must
  not read as progress), and it counts only the trailing window (a rollout that finished last
  week must not read as an in-flight rate). The ETA is `nil` — never a large number — while the
  rate is 0, because a stalled rollout has no finish time and any number renders on a dashboard
  as if it were moving.
  """
  use Fathom.DataCase, async: false

  # Releases here run with whatever :template_shard_id the test env sets; `Migrator.release/6`
  # warns about novel tenants born empty, which is correct and not what these tests are about.
  @moduletag :capture_log

  alias Fathom.Directory
  alias Fathom.Directory.Shard
  alias Fathom.Migrator
  alias Fathom.Repo

  import Ecto.Query

  defp shard_at_head!(id, version) do
    {:ok, _} = Directory.resolve(id)
    {:ok, _} = Directory.cutover(id, version)
    id
  end

  # Backdate BOTH stamps together, the way real time passing does. Backdating `cutover_at` alone
  # would fabricate `cutover_at < last_active_at` for a shard nothing touched, which the fleet
  # cannot produce (`cutover/2` writes them equal; only traffic advances `last_active_at`).
  defp age!(id, seconds) do
    then = DateTime.add(DateTime.utc_now(), -seconds)

    {1, _} =
      Repo.update_all(
        from(s in Shard, where: s.shard_id == ^id),
        set: [cutover_at: then, last_active_at: then]
      )

    id
  end

  describe "rollout_rate/1" do
    test "counts shards that reached HEAD inside the trailing hour" do
      {:ok, _} = Migrator.release(1, "v1", ["SELECT 1"])
      shard_at_head!("rate_a", 1)
      shard_at_head!("rate_b", 1)

      assert Migrator.rollout_rate() == 2
    end

    test "excludes a cutover older than the window" do
      {:ok, _} = Migrator.release(1, "v1", ["SELECT 1"])
      shard_at_head!("rate_recent", 1)
      "rate_old" |> shard_at_head!(1) |> age!(7200)

      assert Migrator.rollout_rate() == 1
    end

    # A revert calls the same `Directory.cutover/2` and so stamps a fresh `cutover_at`. Counted
    # naively, a fleet-wide revert would report as the fastest rollout the system has ever had —
    # exactly backwards. Scoping to `schema_version == head` excludes it, because a reverted
    # shard lands below head by definition.
    test "a revert is not counted as rollout progress" do
      {:ok, _} = Migrator.release(1, "v1", ["SELECT 1"])
      {:ok, _} = Migrator.release(2, "v2", ["SELECT 1"])
      shard_at_head!("rate_fwd", 2)
      # Reverted: cut over just now, but back down to v1 while HEAD is 2.
      shard_at_head!("rate_reverted", 1)

      assert Migrator.head() == 2
      assert Migrator.rollout_rate() == 1
    end

    test "is zero before any release" do
      {:ok, _} = Directory.resolve("rate_norelease")
      assert Migrator.head() == 0
      assert Migrator.rollout_rate() == 0
    end
  end

  describe "status/0 rate and ETA" do
    test "projects an ETA from laggards and the measured rate" do
      {:ok, _} = Migrator.release(1, "v1", ["SELECT 1"])
      # Two shards migrated in the last hour; four still behind.
      shard_at_head!("eta_done_1", 1)
      shard_at_head!("eta_done_2", 1)
      for i <- 1..4, do: {:ok, _} = Directory.resolve("eta_behind_#{i}")

      status = Migrator.status()

      assert status.laggards == 4
      assert status.rate_per_hour == 2
      # 4 laggards / 2 per hour = 2 hours.
      assert status.eta_seconds == 7200
      refute status.converged
    end

    # The whole point of nil: a rollout that is stuck has no finish time. Reporting a huge integer
    # (or :infinity) puts a number on a dashboard that reads as progress.
    test "ETA is nil, not a large number, when the rollout is stalled" do
      {:ok, _} = Migrator.release(1, "v1", ["SELECT 1"])
      for i <- 1..3, do: {:ok, _} = Directory.resolve("stall_behind_#{i}")

      status = Migrator.status()

      assert status.laggards == 3
      assert status.rate_per_hour == 0
      assert status.eta_seconds == nil
    end

    test "ETA is zero once converged" do
      {:ok, _} = Migrator.release(1, "v1", ["SELECT 1"])
      shard_at_head!("conv_a", 1)

      status = Migrator.status()

      assert status.converged
      assert status.eta_seconds == 0
    end

    # Rounds UP: 3 laggards at 2/hour is 1.5 hours of work, and an ETA that lands before the
    # rollout finishes is the failure mode an operator notices.
    test "a partial hour of remaining work rounds up" do
      {:ok, _} = Migrator.release(1, "v1", ["SELECT 1"])
      shard_at_head!("ceil_done_1", 1)
      shard_at_head!("ceil_done_2", 1)
      for i <- 1..3, do: {:ok, _} = Directory.resolve("ceil_behind_#{i}")

      assert Migrator.status().eta_seconds == 5400
    end
  end

  describe "Directory.count_cutovers_since/2" do
    # The query carries a redundant `last_active_at >= since` predicate so it can range-scan the
    # existing `(schema_version, last_active_at) WHERE status = 'active'` partial index instead of
    # needing a new index on the system's hottest write table. It is only sound because
    # `cutover/2` stamps both columns with the same instant. This test drives the REAL writer, so
    # if anyone changes `cutover/2` to stop stamping `last_active_at`, the rate silently reading
    # zero shows up here instead of in production.
    test "a shard cut over by Directory.cutover/2 is inside the window" do
      {:ok, _} = Migrator.release(1, "v1", ["SELECT 1"])
      shard_at_head!("implied_a", 1)

      since = DateTime.add(DateTime.utc_now(), -3600)
      assert Directory.count_cutovers_since(1, since) == 1

      {:ok, row} = Directory.get("implied_a")
      assert DateTime.compare(row.last_active_at, row.cutover_at) != :lt
    end

    test "ignores a non-active shard" do
      {:ok, _} = Migrator.release(1, "v1", ["SELECT 1"])
      shard_at_head!("inactive_a", 1)

      {1, _} =
        Repo.update_all(from(s in Shard, where: s.shard_id == "inactive_a"),
          set: [status: "suspended"]
        )

      since = DateTime.add(DateTime.utc_now(), -3600)
      assert Directory.count_cutovers_since(1, since) == 0
    end
  end
end
