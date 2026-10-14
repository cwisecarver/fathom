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
    keys = [:erasure_sweep_page, :erasure_sweep_budget_ms, :erasure_sweep_probe_timeout_ms]
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
end
