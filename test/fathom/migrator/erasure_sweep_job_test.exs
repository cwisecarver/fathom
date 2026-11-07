defmodule Fathom.Migrator.ErasureSweepJobTest do
  @moduledoc """
  Expert review 2026-10-13 #8. Symptoms, both pinned here:

    * The delete re-drive and promotion sweep ran serially and unbounded at the top of
      `ReconcileJob`, so a slow object store (an S3 brownout, exactly when DeleteJobs exhaust)
      froze the fleet-wide rollout on the shared `:migrations` queue. The sweeps now live in
      `ErasureSweepJob`, on the `:erasure` queue, bounded by per-probe timeouts and an
      early-exit on consecutive failures; `ReconcileJob` never touches storage for them.
    * The re-drive took the newest 500 tombstones every hour, so older stuck ids past the 500th
      deletion were never revisited. The window is now walked with a keyset cursor, and a run that
      runs out of budget hands the cursor to a continuation job.

  The slow-store fixture uses `Fathom.Test.FaultyStorage`'s `:any_shard_object` hook, which sleeps
  inside the storage call itself (where a real S3 LIST would hang).
  """
  use Fathom.DataCase, async: false
  use Oban.Testing, repo: Fathom.Repo

  alias Fathom.Directory
  alias Fathom.Migrator.{ErasureSweepJob, ReconcileJob}
  alias Fathom.Shard.Storage
  alias Fathom.Tenants.DeleteJob

  setup do
    prev = for k <- [:shard_storage, :faulty_before], do: {k, Application.get_env(:fathom, k)}

    keys = [
      :erasure_sweep_page,
      :erasure_sweep_budget_ms,
      :erasure_sweep_probe_timeout_ms,
      :erasure_sweep_clock
    ]

    prev_cfg = for k <- keys, do: {k, Application.get_env(:fathom, k)}

    on_exit(fn ->
      for {k, v} <- prev ++ prev_cfg do
        if is_nil(v),
          do: Application.delete_env(:fathom, k),
          else: Application.put_env(:fathom, k, v)
      end
    end)

    :ok
  end

  defp insert_deleted!(count, prefix) do
    now = DateTime.utc_now()

    rows =
      for n <- 1..count do
        %{
          shard_id: "#{prefix}_#{n}",
          schema_version: 0,
          status: "deleted",
          last_active_at: now,
          # n = 1 is the NEWEST deletion.
          updated_at: DateTime.add(now, -n * 60, :second),
          inserted_at: now
        }
      end

    rows |> Enum.chunk_every(200) |> Enum.each(&Fathom.Repo.insert_all(Directory.Shard, &1))
  end

  defp put_object!(id) do
    src = Path.join(System.tmp_dir!(), "#{id}.src")
    File.write!(src, "x")
    assert :ok = Storage.flush(id, src)
    File.rm(src)
    on_exit(fn -> Storage.purge_shard(id) end)
  end

  defp slow_store!(ms) do
    Application.put_env(:fathom, :shard_storage, Fathom.Test.FaultyStorage)

    Application.put_env(
      :fathom,
      :faulty_before,
      {:any_shard_object, fn _id -> Process.sleep(ms) end}
    )
  end

  defp ms(fun) do
    t0 = System.monotonic_time(:millisecond)
    result = fun.()
    {System.monotonic_time(:millisecond) - t0, result}
  end

  describe "ReconcileJob no longer runs the storage sweeps" do
    test "a slow object store does not delay the rollout steps" do
      insert_deleted!(12, "slow")
      slow_store!(1_000)

      # Pre-fix: 12 serial probes x 1 s = 12 s inside ReconcileJob.perform before the rollout.
      {elapsed, result} = ms(fn -> perform_job(ReconcileJob, %{}) end)

      assert result == :ok
      assert elapsed < 3_000, "ReconcileJob took #{elapsed} ms behind a slow store"
    end

    test "has a unique guard over its live states" do
      assert ReconcileJob.__opts__()[:unique][:period] == :infinity
      refute :completed in ReconcileJob.__opts__()[:unique][:states]
    end
  end

  describe "ErasureSweepJob bounds" do
    test "stops after consecutive probe timeouts, well inside the job budget" do
      insert_deleted!(40, "stuck")
      slow_store!(30_000)
      Application.put_env(:fathom, :erasure_sweep_probe_timeout_ms, 100)

      {elapsed, result} = ms(fn -> perform_job(ErasureSweepJob, %{}) end)

      # 40 probes x 100 ms / 8 would be ~0.5 s anyway; the point is it ENDS (kill_task, not 30 s
      # sleeps) and re-drives nothing, and it halts early rather than grinding the whole page.
      assert result == :ok
      assert elapsed < 5_000
      refute_enqueued(worker: DeleteJob)
    end

    test "is unique across the cron job and a continuation" do
      opts = ErasureSweepJob.__opts__()
      assert opts[:queue] == :erasure
      assert opts[:unique][:fields] == [:worker, :queue]
      assert opts[:unique][:period] == :infinity
    end
  end

  describe "keyset walk over the 30-day window" do
    test "with 600 deletions where only the OLDEST has leftovers, one run re-drives it" do
      insert_deleted!(600, "rd")
      put_object!("rd_600")

      assert :ok = perform_job(ErasureSweepJob, %{})

      assert_enqueued(worker: DeleteJob, args: %{shard_id: "rd_600"})
    end

    test "an exhausted budget hands a cursor to a continuation that finishes the walk" do
      insert_deleted!(600, "kc")
      put_object!("kc_600")
      Application.put_env(:fathom, :erasure_sweep_page, 100)
      # Budget of 0: every run stops after ONE page and enqueues a continuation.
      Application.put_env(:fathom, :erasure_sweep_budget_ms, 0)

      assert :ok = perform_job(ErasureSweepJob, %{})
      refute_enqueued(worker: DeleteJob, args: %{shard_id: "kc_600"})

      [cont] = all_enqueued(worker: ErasureSweepJob)
      assert %{"cursor_at" => _, "cursor_id" => "kc_100"} = cont.args

      # Five more continuations reach the oldest page (kc_501..kc_600).
      final =
        Enum.reduce(1..5, cont, fn _, job ->
          assert :ok = perform_job(ErasureSweepJob, job.args)
          all_enqueued(worker: ErasureSweepJob) |> Enum.max_by(& &1.id)
        end)

      assert_enqueued(worker: DeleteJob, args: %{shard_id: "kc_600"})

      # The last page was short of a full page's worth of NEW rows? It is exactly full, so one more
      # continuation exists, and it finds the window empty.
      assert %{"cursor_id" => "kc_600"} = final.args
      assert :ok = perform_job(ErasureSweepJob, final.args)
    end
  end

  # Perf review 2026-10-14 #15: the sweep re-LISTed EVERY tombstone in the 30-day window every hour
  # (~720 LISTs per deletion). A clean probe now stamps `purge_verified_at` and the row is due again
  # only past the 1 h / 6 h / 24 h / 7 d / 29 d checkpoints. The clock is injected, so 30 days of
  # hourly runs take milliseconds. Pre-fix the first test counts ~720, not <= 6.
  describe "decaying probe schedule" do
    setup do
      t0 = DateTime.utc_now()
      clock = start_supervised!({Agent, fn -> DateTime.add(t0, 60, :second) end})
      Application.put_env(:fathom, :erasure_sweep_clock, fn -> Agent.get(clock, & &1) end)
      Application.put_env(:fathom, :shard_storage, Fathom.Test.FaultyStorage)
      {:ok, t0: t0, clock: clock}
    end

    # Counts LISTs per shard: the hook runs inside the probe's Task, so it reports by message.
    defp count_lists!(test_pid) do
      Application.put_env(
        :fathom,
        :faulty_before,
        {:any_shard_object, fn id -> send(test_pid, {:list, id}) end}
      )
    end

    defp lists_in_mailbox(acc \\ []) do
      receive do
        {:list, id} -> lists_in_mailbox([id | acc])
      after
        0 -> Enum.reverse(acc)
      end
    end

    defp insert_one!(id, t0) do
      Fathom.Repo.insert_all(Directory.Shard, [
        %{
          shard_id: id,
          schema_version: 0,
          status: "deleted",
          last_active_at: t0,
          updated_at: t0,
          inserted_at: t0
        }
      ])
    end

    defp advance!(clock, seconds), do: Agent.update(clock, &DateTime.add(&1, seconds, :second))
    defp sweep!, do: assert(:ok = perform_job(ErasureSweepJob, %{}))

    test "a clean tombstone is LISTed at most 6 times over its 30-day window, not ~720",
         %{t0: t0, clock: clock} do
      insert_one!("ds_clean", t0)
      count_lists!(self())

      for _hour <- 1..720 do
        sweep!()
        advance!(clock, 3_600)
      end

      lists = lists_in_mailbox()
      assert length(lists) in 5..6, "expected <= 6 LISTs, got #{length(lists)}"
      refute_enqueued(worker: DeleteJob)
    end

    test "stamping purge_verified_at leaves updated_at (the deletion time) alone", %{t0: t0} do
      insert_one!("ds_stamp", t0)
      count_lists!(self())
      sweep!()

      row = Fathom.Repo.get_by!(Directory.Shard, shard_id: "ds_stamp")
      assert %DateTime{} = row.purge_verified_at
      assert DateTime.compare(row.updated_at, t0) == :eq
    end

    test "a row with leftovers is never stamped: it is re-probed and re-driven every run",
         %{t0: t0, clock: clock} do
      insert_one!("ds_left", t0)
      put_object!("ds_left")
      count_lists!(self())

      for _ <- 1..4 do
        sweep!()
        advance!(clock, 3_600)
      end

      assert lists_in_mailbox() == List.duplicate("ds_left", 4)
      assert_enqueued(worker: DeleteJob, args: %{shard_id: "ds_left"})
      assert Fathom.Repo.get_by!(Directory.Shard, shard_id: "ds_left").purge_verified_at == nil
    end

    test "a probe error stamps nothing, so the row is probed again on the next run",
         %{t0: t0, clock: clock} do
      insert_one!("ds_err", t0)
      test_pid = self()

      Application.put_env(
        :fathom,
        :faulty_before,
        {:any_shard_object,
         fn id ->
           send(test_pid, {:list, id})
           raise "store down"
         end}
      )

      sweep!()
      advance!(clock, 60)
      sweep!()

      assert lists_in_mailbox() == ["ds_err", "ds_err"]
    end

    # The GDPR-relevant case: an object that appears AFTER a clean verification (a late stale-writer
    # flush) is still found, at the next checkpoint, and re-purged.
    test "an object that appears after a clean probe is re-driven at the next checkpoint",
         %{t0: t0, clock: clock} do
      insert_one!("ds_late", t0)
      count_lists!(self())
      sweep!()
      assert lists_in_mailbox() == ["ds_late"]

      put_object!("ds_late")

      # Not yet at the 1 h checkpoint: no LIST, no re-drive.
      advance!(clock, 600)
      sweep!()
      assert lists_in_mailbox() == []
      refute_enqueued(worker: DeleteJob, args: %{shard_id: "ds_late"})

      # Past 1 h: due again, the object is found, DeleteJob re-enqueued.
      advance!(clock, 3_600)
      sweep!()
      assert lists_in_mailbox() == ["ds_late"]
      assert_enqueued(worker: DeleteJob, args: %{shard_id: "ds_late"})
    end

    test "the final probe near the window's end catches an object that appeared after day 7",
         %{t0: t0, clock: clock} do
      insert_one!("ds_final", t0)
      count_lists!(self())

      # Fast-forward through the early checkpoints with a clean store.
      for secs <- [60, 3_600, 21_600, 86_400, 604_800] do
        advance!(clock, secs)
        sweep!()
      end

      _ = lists_in_mailbox()
      put_object!("ds_final")

      # Day ~10: not a checkpoint, so no LIST and no re-drive.
      advance!(clock, 3 * 86_400)
      sweep!()
      assert lists_in_mailbox() == []
      refute_enqueued(worker: DeleteJob, args: %{shard_id: "ds_final"})

      # Past the 29-day checkpoint (still inside the 30-day window): the final probe fires.
      Agent.update(clock, fn _ -> DateTime.add(t0, 29 * 86_400 + 600, :second) end)
      sweep!()
      assert lists_in_mailbox() == ["ds_final"]
      assert_enqueued(worker: DeleteJob, args: %{shard_id: "ds_final"})
    end
  end

  # Expert review 2026-10-15 #19. A drill killed between fork and drop leaks a full tenant copy
  # (objects + an active scratch row) that no tenant delete reaches. The sweep purges stale ones.
  describe "restore-drill scratch fork GC" do
    defp insert_row!(id, age_s, opts \\ []) do
      at = DateTime.add(DateTime.utc_now(), -age_s, :second)

      Fathom.Repo.insert_all(Directory.Shard, [
        %{
          shard_id: id,
          schema_version: 0,
          status: Keyword.get(opts, :status, "active"),
          scratch: Keyword.get(opts, :scratch, false),
          last_active_at: Keyword.get(opts, :last_active_at, at),
          updated_at: at,
          inserted_at: at
        }
      ])
    end

    test "purges objects and row of a MARKED scratch fork older than 24h, and nothing else" do
      stale = "restoredrill1#{System.unique_integer([:positive])}"
      fresh = "restoredrill2#{System.unique_integer([:positive])}"
      real_old = "restoredrill-eu-#{System.unique_integer([:positive])}"

      for id <- [stale, fresh, real_old], do: put_object!(id)
      insert_row!(stale, 25 * 3600, scratch: true)
      insert_row!(fresh, 3600, scratch: true)
      insert_row!(real_old, 25 * 3600)

      assert ErasureSweepJob.sweep_scratch_forks() == 1

      refute Fathom.Repo.get_by(Directory.Shard, shard_id: stale)
      assert {:ok, false} = Storage.any_shard_object?(stale)

      # A fresh scratch may be mid-drill; a non-scratch id that merely shares the prefix is a tenant.
      assert Fathom.Repo.get_by(Directory.Shard, shard_id: fresh)
      assert {:ok, true} = Storage.any_shard_object?(fresh)
      assert Fathom.Repo.get_by(Directory.Shard, shard_id: real_old)
      assert {:ok, true} = Storage.any_shard_object?(real_old)

      for id <- [fresh, real_old], do: Storage.purge_shard(id)
    end

    # Expert review 2026-11-07 #15 (tenant-lifecycle#2). The reservation of the `restoredrill<digits>`
    # shape is enforced only at `Tenants.provision/2` and `fork/3`; a Host subdomain or the token mint
    # births a row with it. The sweep used to select `inserted_at < cutoff AND shard_id ~ pattern`, so
    # this LIVE tenant — created a day ago, active a minute ago — had its objects and row destroyed.
    test "a LIVE tenant whose id merely has the scratch shape is never purged" do
      live = "restoredrill3#{System.unique_integer([:positive])}"
      put_object!(live)
      insert_row!(live, 25 * 3600, last_active_at: DateTime.utc_now())

      assert Fathom.Repo.get_by(Directory.Shard, shard_id: live).scratch == false,
             "fixture: an organic row is not marked"

      assert ErasureSweepJob.sweep_scratch_forks() == 0

      assert Fathom.Repo.get_by(Directory.Shard, shard_id: live)
      assert {:ok, true} = Storage.any_shard_object?(live), "a live tenant's object was purged"

      Storage.purge_shard(live)
    end

    test "a marked row that has been TOMBSTONED keeps its row: it is the directory half of the guard" do
      gone = "restoredrill4#{System.unique_integer([:positive])}"
      insert_row!(gone, 25 * 3600, scratch: true, status: "deleted")

      assert ErasureSweepJob.sweep_scratch_forks() == 0
      assert %{status: "deleted"} = Fathom.Repo.get_by(Directory.Shard, shard_id: gone)
    end
  end

  # Expert review 2026-11-07 #54. A `RetirementJob` that failed 5 times in ~90 s (Oban's default
  # backoff) was discarded, and nothing ever re-enqueued it: the retained `<shard>@<version>` copy —
  # a full duplicate of the tenant — and its `retained_version` pointer leaked forever. The moduledoc
  # promised "an S3 lifecycle rule backstops any it misses"; no such rule ships.
  describe "retirement re-drive and retry shape (expert review 2026-11-07 #54)" do
    alias Fathom.Migrator.RetirementJob

    @day 24 * 3600

    # A tenant at v2 whose backup `@1` should have been retired `age_days` ago.
    defp insert_retained!(id, age_days, opts \\ []) do
      at = DateTime.add(DateTime.utc_now(), -round(age_days * @day), :second)

      Fathom.Repo.insert_all(Directory.Shard, [
        %{
          shard_id: id,
          schema_version: Keyword.get(opts, :schema_version, 2),
          status: Keyword.get(opts, :status, "active"),
          retained_version: Keyword.get(opts, :retained_version, 1),
          cutover_at: at,
          last_active_at: at,
          updated_at: at,
          inserted_at: at
        }
      ])
    end

    defp retained_object!(id) do
      put_object!(id)
      assert :ok = Storage.retain(id, 1)
      assert Storage.version_present?(id, 1), "fixture: the retained @1 must exist"
    end

    test "an overdue retained copy is re-enqueued once, and the job drops it and clears the pointer" do
      id = "redrive_#{System.unique_integer([:positive])}"
      retained_object!(id)
      insert_retained!(id, 9)

      assert ErasureSweepJob.redrive_retirements() == 1
      assert_enqueued(worker: RetirementJob, args: %{"shard_id" => id, "version" => 1})

      # An hourly sweep must not stack a second job behind one that is still live.
      assert ErasureSweepJob.redrive_retirements() == 0
      assert [_one] = all_enqueued(worker: RetirementJob, args: %{"shard_id" => id})

      assert :ok = perform_job(RetirementJob, %{"shard_id" => id, "version" => 1})
      refute Storage.version_present?(id, 1), "the retained copy must be dropped"
      assert {:ok, %{retained_version: nil}} = Directory.get(id)
    end

    test "only what is genuinely overdue is selected" do
      tag = System.unique_integer([:positive])
      overdue = "rd_over_#{tag}"
      suspended = "rd_susp_#{tag}"
      in_window = "rd_win_#{tag}"
      live_version = "rd_live_#{tag}"
      deleted = "rd_del_#{tag}"
      no_pointer = "rd_none_#{tag}"

      insert_retained!(overdue, 9)
      insert_retained!(suspended, 9, status: "suspended")
      # 7 d retention + 1 d grace: 7.5 d old is still the original job's to do.
      insert_retained!(in_window, 7.5)
      # The pointer names the LIVE version: RetirementJob would cancel it :version_live forever.
      insert_retained!(live_version, 9, schema_version: 1, retained_version: 1)
      insert_retained!(deleted, 9, status: "deleted")
      insert_retained!(no_pointer, 9, retained_version: nil)

      # A row whose retirement is still PENDING (a revert's reschedule, a job backing off) is the
      # job's to finish: a re-drive dropping it now would shorten that backup's retention.
      pending = "rd_pend_#{tag}"
      insert_retained!(pending, 9)
      {:ok, _} = Oban.insert(RetirementJob.schedule_changeset(pending, 1))

      assert ErasureSweepJob.redrive_retirements() == 2

      enqueued =
        all_enqueued(worker: RetirementJob)
        |> Enum.map(& &1.args["shard_id"])
        |> Enum.reject(&(&1 == pending))

      assert Enum.sort(enqueued) == Enum.sort([overdue, suspended])

      assert [_only_the_original] =
               all_enqueued(worker: RetirementJob, args: %{"shard_id" => pending})
    end

    test "the cron run drives it (a continuation does not repeat the page)" do
      id = "redrive_cron_#{System.unique_integer([:positive])}"
      insert_retained!(id, 9)

      assert :ok = perform_job(ErasureSweepJob, %{})
      assert_enqueued(worker: RetirementJob, args: %{"shard_id" => id, "version" => 1})
    end

    # Oban's default backoff is `15 + 2^attempt` seconds: five attempts inside ~90 s, so any storage
    # brownout longer than that discarded every retirement that started early in it — and a
    # rollout schedules ALL of its retirements for the same hours, a week later.
    test "the retry window outlasts a brownout: many attempts, long capped backoff" do
      max_attempts = RetirementJob.new(%{shard_id: "x", version: 1}).changes.max_attempts
      assert max_attempts >= 10

      gaps = for a <- 1..(max_attempts - 1), do: RetirementJob.backoff(%Oban.Job{attempt: a})
      assert Enum.all?(gaps, &(&1 >= 120)), "the first retry must not come within the minute"
      assert Enum.max(gaps) <= 3 * 3600, "capped, so a long outage is retried every few hours"
      assert Enum.sum(gaps) >= 6 * 3600, "the window must cover an hours-long brownout"
    end
  end
end
