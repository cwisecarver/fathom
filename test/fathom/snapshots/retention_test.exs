defmodule Fathom.Snapshots.RetentionTest do
  @moduledoc """
  The grandfather-father-son classification for expert review 2026-08-01 #18.

  Pure-function tests, deliberately: bucket boundaries, "newest per period", year ends and gaps are
  where this goes wrong, and none of it needs an object store. `RetentionJob` is a thin shell over
  `plan/3`, so this is where the coverage belongs.

  Every test here is about **deleting the wrong thing**, because that is the only failure mode that
  matters. Keeping too much costs storage; deleting too much costs the backup.
  """
  use ExUnit.Case, async: true

  alias Fathom.Snapshots.Retention

  @policy %{hourly: 24, daily: 7, weekly: 4}

  defp now, do: ~U[2026-08-05 12:00:00Z]

  # Build an id in `Fathom.Snapshots.new_snapshot_id/1`'s shape.
  defp id(%DateTime{} = at, label \\ "auto") do
    ts = Calendar.strftime(at, "%Y%m%dT%H%M%SZ")

    uniq =
      at |> DateTime.to_unix() |> rem(10_000) |> Integer.to_string() |> String.pad_leading(4, "0")

    if label, do: "#{ts}-#{uniq}-#{label}", else: "#{ts}-#{uniq}"
  end

  defp hours_ago(n), do: DateTime.add(now(), -n * 3600, :second)
  defp days_ago(n), do: DateTime.add(now(), -n * 86_400, :second)

  describe "timestamp parsing" do
    test "reads the fixed 16-character prefix" do
      assert Retention.parse_timestamp("20260805T120000Z-1234-auto") == ~U[2026-08-05 12:00:00Z]
      assert Retention.parse_timestamp("20240229T235959Z-0001-auto") == ~U[2024-02-29 23:59:59Z]
    end

    test "a label containing digits and dashes cannot confuse it" do
      # Only the prefix is read, so a label like "2026-backup-9999" is inert.
      assert Retention.parse_timestamp("20260805T120000Z-1234-2026-backup-9999") ==
               ~U[2026-08-05 12:00:00Z]
    end

    test "rejects malformed ids rather than guessing" do
      for bad <- [
            "",
            "not-a-snapshot",
            "20261305T120000Z-1234-auto",
            "20260805T250000Z-1234-auto",
            "20260230T120000Z-1234-auto",
            "2026085T120000Z-1234-auto"
          ] do
        assert Retention.parse_timestamp(bad) == :error, bad
      end
    end
  end

  describe "eligibility — the safety property" do
    test "only `-auto` snapshots are eligible" do
      assert Retention.auto?("20260805T120000Z-1234-auto")
      refute Retention.auto?("20260805T120000Z-1234")
      refute Retention.auto?("20260805T120000Z-1234-pre-migration")
      refute Retention.auto?("20260805T120000Z-1234-autopilot")
    end

    test "a manual snapshot is NEVER dropped, however old" do
      # The case this property exists for: an operator's deliberate pre-migration backup, older
      # than every retention window. Expiring it would delete exactly the backup someone took on
      # purpose.
      manual = id(days_ago(400), "pre-migration")
      unlabelled = id(days_ago(400), nil)
      autos = for h <- 1..50, do: id(hours_ago(h))

      plan = Retention.plan([manual, unlabelled | autos], @policy, now())

      refute manual in plan.drop
      refute unlabelled in plan.drop
      assert manual in plan.ineligible
      assert unlabelled in plan.ineligible
      # And it did still expire the automatic ones, so the test is not passing vacuously.
      assert plan.drop != []
    end

    test "an unparseable id is reported ineligible, not deleted" do
      junk = "totally-not-a-snapshot-auto"
      plan = Retention.plan([junk | for(h <- 1..30, do: id(hours_ago(h)))], @policy, now())

      assert junk in plan.ineligible
      refute junk in plan.drop
    end
  end

  describe "hourly retention" do
    test "keeps the newest snapshot in each of the last N hours" do
      # Two per hour for 30 hours; hourly: 24 should keep 24 (the newest of each of the 24 most
      # recent hours) and the daily/weekly buckets add their own.
      ids =
        for h <- 0..29, m <- [0, 30] do
          id(DateTime.add(now(), -(h * 3600 + m * 60), :second))
        end

      plan = Retention.plan(ids, %{hourly: 24}, now())

      assert length(plan.keep) == 24
      assert length(plan.drop) == length(ids) - 24

      # Which snapshot is "newest in its hour" is NOT the one on the hour mark. Generating at
      # 12:00 minus (h hours + m minutes) puts 11:30 and 11:00 both in hour 11 — so 11:30 is the
      # keeper and 11:00 is dropped. Pinned in this direction because the intuitive reading (the
      # :00 one wins) is wrong, and a test written to it would have "failed" against correct code.
      assert id(now()) in plan.keep, "12:00 is alone in hour 12"

      assert id(DateTime.add(now(), -1800, :second)) in plan.keep,
             "11:30 is the newest in hour 11"

      assert id(DateTime.add(now(), -3600, :second)) in plan.drop, "11:00 loses to 11:30"
    end

    test "counts POPULATED hours, not wall-clock hours" do
      # After a week of downtime there are no snapshots in the last 24 wall-clock hours. An
      # implementation that stepped back 24 hours from `now` would keep NOTHING and fall through to
      # deleting the history it exists to protect.
      ids = for h <- 168..191, do: id(hours_ago(h))

      plan = Retention.plan(ids, %{hourly: 24}, now())

      assert plan.drop == [], "a gap must not cause the whole history to expire"
      assert length(plan.keep) == 24
    end
  end

  describe "daily and weekly retention" do
    test "keeps the newest snapshot per day" do
      ids = for d <- 0..13, h <- [1, 13], do: id(DateTime.add(days_ago(d), h * 3600, :second))

      plan = Retention.plan(ids, %{daily: 7}, now())

      assert length(plan.keep) == 7
    end

    test "keeps the newest snapshot per ISO week" do
      ids = for w <- 0..9, do: id(days_ago(w * 7))

      plan = Retention.plan(ids, %{weekly: 4}, now())

      assert length(plan.keep) == 4
    end

    test "a snapshot kept by ANY bucket is kept" do
      # The oldest snapshot here falls outside hourly and daily but inside weekly. Union, not
      # intersection — an intersection would delete almost everything.
      ids = [id(now()), id(hours_ago(30)), id(days_ago(20))]

      plan = Retention.plan(ids, @policy, now())

      assert plan.drop == []
      assert length(plan.keep) == 3
    end

    test "the ISO week boundary is handled across a year end" do
      # 2027-01-01 is a Friday, in ISO week 53 of ISO YEAR 2026 — a plain `.year` would bucket it
      # into 2027 and split one week into two.
      a = ~U[2026-12-31 12:00:00Z]
      b = ~U[2027-01-01 12:00:00Z]

      plan = Retention.plan([id(a), id(b)], %{weekly: 1}, ~U[2027-01-02 00:00:00Z])

      assert length(plan.keep) == 1, "both fall in ISO week 2026-W53, so only the newest is kept"
      assert id(b) in plan.keep
    end
  end

  describe "degenerate and dangerous inputs" do
    test "an empty policy drops every eligible snapshot" do
      # Documented, and the reason RetentionJob refuses to run without a policy configured: a
      # sample size with no rule is a delete sweep with no rule.
      ids = for h <- 1..5, do: id(hours_ago(h))
      plan = Retention.plan(ids, %{}, now())
      assert length(plan.drop) == 5
    end

    test "a single snapshot under a non-empty policy is kept" do
      one = id(days_ago(365))
      plan = Retention.plan([one], @policy, now())
      assert plan.keep == [one]
      assert plan.drop == []
    end

    test "a future-dated snapshot is kept, not dropped" do
      # Clock skew between nodes makes this reachable, and deleting the most recent backup because
      # a peer's clock ran fast is the worst outcome available here.
      future = id(DateTime.add(now(), 3600, :second))
      plan = Retention.plan([future | for(h <- 1..40, do: id(hours_ago(h)))], @policy, now())

      assert future in plan.keep
      refute future in plan.drop
    end

    test "no snapshots at all" do
      assert Retention.plan([], @policy, now()) == %{keep: [], drop: [], ineligible: []}
    end

    test "keep and drop together account for every eligible id, with no overlap" do
      ids = for h <- 0..99, do: id(hours_ago(h))
      plan = Retention.plan(ids, @policy, now())

      assert MapSet.new(plan.keep ++ plan.drop) == MapSet.new(ids)
      assert plan.keep -- plan.drop == plan.keep, "an id must not be in both lists"
      assert length(plan.keep) + length(plan.drop) == length(ids)
    end

    test "duplicate timestamps in the same period keep exactly one" do
      ids = for n <- 1..5, do: "20260805T110000Z-000#{n}-auto"
      plan = Retention.plan(ids, %{hourly: 24}, now())

      assert length(plan.keep) == 1
      assert length(plan.drop) == 4
    end
  end

  # #13 — an empty policy is "keep nothing", and it was reachable from a set-but-empty env var.
  # Pure half only: this is what the empty policy actually COMPUTED. The guards that stop it being
  # treated as configured live in config/runtime.exs and RetentionJob.policy/0, and are covered in
  # the storage-backed suite.
  describe "an empty policy computes total deletion (#13)" do
    test "plan/3 over %{} marks every automatic snapshot for deletion" do
      snaps = for h <- 0..5, do: id(hours_ago(h))
      plan = Retention.plan(snaps, %{}, now())

      assert plan.keep == [],
             "an empty policy is 'delete every automatic snapshot' — which is exactly what a " <>
               "set-but-empty SNAPSHOT_RETENTION parsed to, with a normal success log"

      assert length(plan.drop) == length(snaps)
    end

    test "plan/3 over an all-zero policy does the same" do
      snaps = for h <- 0..5, do: id(hours_ago(h))
      plan = Retention.plan(snaps, %{hourly: 0, daily: 0, weekly: 0}, now())
      assert plan.keep == []
      assert length(plan.drop) == length(snaps)
    end
  end
end
