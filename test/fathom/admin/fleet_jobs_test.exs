defmodule Fathom.Admin.FleetJobsTest do
  @moduledoc """
  Expert review 2026-10-15 #12/#18/#19: the Oban, lifecycle and rollout aggregates behind the
  Migrations page. Seeded against real `shards` / `oban_jobs` rows, including the empty case.
  """
  use Fathom.DataCase, async: true

  alias Fathom.Admin.{Fleet, ObanHealth}
  alias Fathom.Directory
  alias Fathom.Directory.Shard
  alias Fathom.Tenants.DenyList

  defp insert_job(attrs) do
    now = DateTime.utc_now()

    defaults = %{
      args: %{},
      worker: "Fathom.Test.Worker",
      queue: "migrations",
      state: "available",
      inserted_at: now,
      scheduled_at: now
    }

    Repo.insert!(struct(Oban.Job, Map.merge(defaults, Map.new(attrs))))
  end

  describe "ObanHealth.snapshot/1 (shared with Measurements.oban_health/0)" do
    test "reports depth and oldest-available age per configured queue, zeros when empty" do
      old = DateTime.add(DateTime.utc_now(), -120, :second)
      insert_job(%{state: "available", scheduled_at: old})
      insert_job(%{state: "retryable"})

      %{queues: queues} = ObanHealth.snapshot()

      mig = Enum.find(queues, &(&1.queue == "migrations"))
      assert mig.available == 1
      assert mig.retryable == 1
      assert mig.oldest_age_ms >= 110_000

      tenants = Enum.find(queues, &(&1.queue == "tenants"))
      assert %{available: 0, retryable: 0, oldest_age_ms: 0} = tenants
    end

    test "cron freshness counts only cron-inserted rows" do
      [worker | _] = cron_worker_names()
      insert_job(%{worker: worker, queue: "migrations", meta: %{"cron" => true}})

      assert %{crons: crons} = ObanHealth.snapshot()
      assert Enum.any?(crons, &(&1.worker == worker and &1.age_ms >= 0))
    end

    test "a hand-enqueued job of a cron worker is not freshness evidence" do
      [worker | _] = cron_worker_names()
      insert_job(%{worker: worker, meta: %{}})

      assert %{crons: crons} = ObanHealth.snapshot()
      refute Enum.any?(crons, &(&1.worker == worker))
    end
  end

  defp cron_worker_names do
    :fathom
    |> Application.get_env(Oban, [])
    |> Keyword.get(:plugins, [])
    |> Enum.find_value([], fn
      {Oban.Plugins.Cron, opts} -> Keyword.get(opts, :crontab, [])
      _ -> nil
    end)
    |> Enum.map(&(&1 |> elem(1) |> Atom.to_string() |> String.replace_leading("Elixir.", "")))
  end

  describe "oban_counts/0" do
    test "groups by queue, worker and state" do
      insert_job(%{worker: "A.Worker", state: "discarded"})
      insert_job(%{worker: "B.Worker", state: "discarded"})
      insert_job(%{worker: "B.Worker", state: "discarded"})

      rows = Fleet.oban_counts()
      assert %{count: 1} = Enum.find(rows, &(&1.worker == "A.Worker" and &1.state == "discarded"))
      assert %{count: 2} = Enum.find(rows, &(&1.worker == "B.Worker" and &1.state == "discarded"))
    end
  end

  describe "failed_jobs/0" do
    test "empty when nothing was discarded" do
      assert Fleet.failed_jobs() == []
    end

    test "returns worker, shard_id arg, attempt and the first line of the last error" do
      insert_job(%{
        worker: "Fathom.Migrator.ShardMigrationJob",
        state: "discarded",
        args: %{"shard_id" => "acme"},
        attempt: 5,
        max_attempts: 5,
        errors: [
          %{"error" => "old failure"},
          %{"error" => "boom: storage down\n  stacktrace line 2"}
        ]
      })

      assert [row] = Fleet.failed_jobs()
      assert row.worker == "Fathom.Migrator.ShardMigrationJob"
      assert row.shard_id == "acme"
      assert row.attempt == 5
      assert row.error == "boom: storage down"
    end

    test "is capped at 25 rows" do
      for _ <- 1..30, do: insert_job(%{state: "discarded"})
      assert length(Fleet.failed_jobs()) == 25
    end

    test "last_error_line tolerates no errors" do
      assert Fleet.last_error_line([]) == nil
      assert Fleet.last_error_line(nil) == nil
    end
  end

  describe "failed_details/1" do
    test "empty ids is empty, with no query" do
      assert Fleet.failed_details([]) == %{}
    end

    test "reads only the requested ids, newest discard wins" do
      insert_job(%{
        state: "discarded",
        args: %{"shard_id" => "a"},
        attempt: 3,
        errors: [%{"error" => "first"}]
      })

      insert_job(%{
        state: "discarded",
        args: %{"shard_id" => "a"},
        attempt: 5,
        errors: [%{"error" => "second"}]
      })

      insert_job(%{
        state: "discarded",
        args: %{"shard_id" => "other"},
        attempt: 1,
        errors: [%{"error" => "x"}]
      })

      assert %{"a" => %{attempt: 5, error: "second"}} = details = Fleet.failed_details(["a", "b"])
      refute Map.has_key?(details, "other")
      refute Map.has_key?(details, "b")
    end
  end

  describe "rollout/1" do
    test "empty fleet: empty histogram, no cutovers, converged ETA" do
      assert %{histogram: [], cutovers_last_hour: 0, eta_seconds: 0} = Fleet.rollout(1)
    end

    test "histogram counts active shards per version; ETA = laggards / hourly rate" do
      for id <- ~w(a b c d) do
        {:ok, _} = Directory.resolve(id)
      end

      # Three at head (v2, cut over just now), one laggard at v1.
      for id <- ~w(a b c), do: {:ok, _} = Directory.cutover(id, 2)

      assert %{histogram: hist, cutovers_last_hour: 3, eta_seconds: eta} = Fleet.rollout(2)
      assert {2, 3} in hist
      assert Enum.sum(for {_, n} <- hist, do: n) == 4
      # 1 laggard at 3 cutovers/hour = 1200 s.
      assert eta == 1200
    end

    test "laggards but no recent cutovers has no ETA" do
      {:ok, _} = Directory.resolve("a")
      assert %{eta_seconds: nil} = Fleet.rollout(5)
    end
  end

  describe "lifecycle/1" do
    test "empty: no unverified deletion, zero discarded delete jobs" do
      assert %{oldest_unverified_deletion_age_s: nil, delete_jobs_discarded: 0} =
               Fleet.lifecycle([])
    end

    test "oldest unverified tombstone age ignores purge-verified rows" do
      {:ok, _} = Directory.resolve("gone-verified")
      {:ok, _} = Directory.resolve("gone-pending")
      {:ok, _} = Directory.tombstone("gone-verified")
      {:ok, _} = Directory.tombstone("gone-pending")

      old = DateTime.add(DateTime.utc_now(), -7_200, :second)

      Repo.update_all(from(s in Shard, where: s.shard_id == "gone-verified"),
        set: [
          updated_at: DateTime.add(old, -3_600, :second),
          purge_verified_at: DateTime.utc_now()
        ]
      )

      Repo.update_all(from(s in Shard, where: s.shard_id == "gone-pending"),
        set: [updated_at: old]
      )

      assert %{oldest_unverified_deletion_age_s: age} = Fleet.lifecycle([])
      assert age >= 7_190 and age < 7_400
    end

    test "counts discarded DeleteJob rows from the oban rows and surfaces deny-set state" do
      rows = [
        %{queue: "tenants", worker: "Fathom.Tenants.DeleteJob", state: "discarded", count: 3},
        %{queue: "tenants", worker: "Fathom.Tenants.DeleteJob", state: "available", count: 9}
      ]

      assert %{delete_jobs_discarded: 3, denylist: %{tombstones: _, suspensions: _}} =
               Fleet.lifecycle(rows)
    end

    test "DenyList.status reflects degraded and recovered" do
      on_exit(fn -> DenyList.recovered(:suspensions) end)
      DenyList.degraded(:suspensions, :boom)
      assert %{suspensions: :degraded} = DenyList.status()
      DenyList.recovered(:suspensions)
      assert %{suspensions: :ok} = DenyList.status()
    end
  end

  test "Directory.oldest_unverified_deletion is nil with no tombstones" do
    assert Directory.oldest_unverified_deletion() == nil
  end
end
