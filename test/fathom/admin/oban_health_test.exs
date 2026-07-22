defmodule Fathom.Admin.ObanHealthTest do
  # Expert review #18: the exception counter (fathom.oban.job.exception.count) catches jobs that
  # FAIL. oban_health/0 catches jobs that DON'T RUN — a backlogged/paused queue and a wedged
  # fleet-singleton cron (which stop silently, with zero exceptions, so every existing alert stays
  # green). Drives the measurement against seeded oban_jobs rows and asserts the emitted gauges.
  use Fathom.DataCase, async: true

  alias Fathom.Admin.Measurements

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

  defp attach(event) do
    ref = make_ref()
    test = self()

    :telemetry.attach(
      {__MODULE__, ref, event},
      event,
      fn ^event, meas, meta, _ -> send(test, {:telem, ref, meas, meta}) end,
      nil
    )

    on_exit(fn -> :telemetry.detach({__MODULE__, ref, event}) end)
    ref
  end

  test "emits per-queue depth and the oldest runnable job's age, 0 for configured-but-empty queues" do
    old = DateTime.add(DateTime.utc_now(), -90, :second)
    insert_job(%{queue: "migrations", state: "available", scheduled_at: old})
    insert_job(%{queue: "migrations", state: "available"})
    insert_job(%{queue: "migrations", state: "retryable"})

    ref = attach([:fathom, :oban, :queue])
    assert :ok = Measurements.oban_health()

    assert_receive {:telem, ^ref, %{available: 2, retryable: 1, oldest_age_ms: age},
                    %{queue: "migrations"}}

    assert age >= 80_000, "oldest_age_ms must reflect the ~90s-old available job (got #{age})"

    # A configured queue with no jobs still reports a stable 0 gauge (not an absent series).
    assert_receive {:telem, ^ref, %{available: 0, retryable: 0, oldest_age_ms: 0},
                    %{queue: "tenants"}}
  end

  test "emits cron freshness (age since last insert) only for crons that have run" do
    old = DateTime.add(DateTime.utc_now(), -300, :second)

    # A completed cron job still proves the cron ran — freshness counts any state.
    insert_job(%{
      worker: "Fathom.Rebalancer.RebalanceJob",
      queue: "rebalance",
      state: "completed",
      inserted_at: old
    })

    ref = attach([:fathom, :oban, :cron])
    assert :ok = Measurements.oban_health()

    assert_receive {:telem, ^ref, %{age_ms: age}, %{worker: "Fathom.Rebalancer.RebalanceJob"}}
    assert age >= 290_000, "cron freshness must be the age since the last insert (got #{age})"

    # ReconcileJob never inserted a job here → no series (it appears once it runs, then tracks).
    refute_receive {:telem, ^ref, _, %{worker: "Fathom.Migrator.ReconcileJob"}}
  end

  test "a stalled (silent) cron surfaces as a growing freshness gauge — the whole point of #18" do
    # A cron that was running then WEDGED: its last insert recedes into the past and the gauge grows,
    # even though no job is failing (zero exceptions). This is exactly what the exception counter
    # cannot see.
    stalled = DateTime.add(DateTime.utc_now(), -3 * 3600, :second)

    insert_job(%{
      worker: "Fathom.Migrator.ReconcileJob",
      queue: "migrations",
      state: "completed",
      inserted_at: stalled
    })

    ref = attach([:fathom, :oban, :cron])
    assert :ok = Measurements.oban_health()

    assert_receive {:telem, ^ref, %{age_ms: age}, %{worker: "Fathom.Migrator.ReconcileJob"}}

    assert age > 7_200_000,
           "a cron stale for 3h must exceed the 2x-hourly-period alert threshold (got #{age})"
  end
end
