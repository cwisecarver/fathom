defmodule Fathom.Migrator.ErasureSweepJob do
  @moduledoc """
  The erasure-completeness sweeps, split out of `Fathom.Migrator.ReconcileJob` (expert review
  2026-10-13 #8):

    * **delete re-drive** (expert review 2026-10-12 #10, #7): a DeleteJob that exhausted its
      attempts (long storage outage) or whose enqueue failed leaves a tombstoned tenant's objects in
      storage with nothing to re-drive the purge. Re-enqueue the (unique, idempotent) DeleteJob for
      recently tombstoned rows that still have any stored object.
    * **promotion-snapshot expiry** (expert review 2026-10-12 #8): expire the pre-promotion
      snapshots whose `promotion_snapshot_at` stamp has aged out.

  Why a separate worker on its own queue. Both sweeps are storage I/O per row. They used to run
  serially and unbounded at the top of ReconcileJob (on the 10-slot `:migrations` queue, no
  `unique:`), so during an S3 brownout, which is exactly when DeleteJobs exhaust, every hourly
  cron added another stuck copy and the fleet-wide rollout, revert, reclaim and cool-off requeue
  froze behind them. They were put FIRST on purpose (2026-10-12 #7: a raise in the migration steps
  used to skip them), so the fix is to move them to their own worker rather than behind the
  rollout. This worker owns the `:erasure` queue (one slot) and is `unique` over its live states, so
  a stuck run blocks only the next sweep, never the rollout.

  Bounds:

    * every storage probe runs in `Task.async_stream` (`@concurrency`, a per-row `timeout:` with
      `on_timeout: :kill_task`), and the run stops after `@max_consecutive_errors` failed or timed
      out probes in a row (the store is down; the next hourly run retries);
    * the 30-day tombstone window is walked with a keyset cursor `{updated_at, shard_id}`, newest
      first, in pages. A run that exhausts its time budget enqueues a CONTINUATION job carrying the
      cursor, so the older ids past the first page are visited rather than the same newest page
      being re-listed forever;
    * a tombstone is not re-LISTed every hour (perf review 2026-10-14 #15; it used to be ~720 LISTs
      per deletion). A clean probe stamps `purge_verified_at`, and `Directory.deleted_page/4` then
      returns the row again only after it ages past a checkpoint of 1 h, 6 h, 24 h, 7 d and 29 d, so
      at most six LISTs per deletion. A probe that finds leftovers, errors or times out stamps
      NOTHING: the row stays due every hourly run until a probe comes back clean, so a deletion with
      objects is still re-driven within the hour. The cost of the schedule is detection latency for
      an object that appears AFTER a clean probe (a late stale-writer flush): up to the next
      checkpoint, 7 days at worst, with the 29-day probe as the last chance inside the window;
    * `timeout/1` (50 min) bounds the whole job under Lifeline's 60-minute rescue.

  Idempotent: DeleteJob is unique per shard, and the promotion stamp is cleared only if unchanged.
  """
  use Oban.Worker,
    queue: :erasure,
    max_attempts: 1,
    # `fields: [:worker, :queue]` so the cron job (empty args) and a continuation (cursor args) dedup
    # against each other; `period: :infinity` because a keyword list inherits `period: 60`; no
    # `:completed`, so only an in-flight run blocks the next one. A continuation is inserted with
    # `unique: false` (the run that enqueues it is itself `executing`).
    unique: [
      fields: [:worker, :queue],
      period: :infinity,
      states: [:available, :scheduled, :executing, :retryable]
    ]

  require Logger

  alias Fathom.{Directory, Snapshots}
  alias Fathom.Shard.Storage
  alias Fathom.Snapshots.Retention
  alias Fathom.Tenants.DeleteJob

  @window_days 30
  @default_page 500
  @concurrency 8
  @default_probe_timeout_ms 20_000
  # A promotion row is a LIST plus its drops, so it gets a longer leash than a one-key probe.
  @promo_timeout_ms 60_000
  @max_consecutive_errors 10
  @default_budget_ms :timer.minutes(30)
  @promo_max 100

  # Expert review 2026-10-13 #6: bound the run under Lifeline's `rescue_after`.
  @impl Oban.Worker
  def timeout(_job), do: :timer.minutes(50)

  @impl Oban.Worker
  def perform(%Oban.Job{args: args}) do
    cursor = decode_cursor(args)

    # A continuation only resumes the tombstone walk; the promotion sweep is one bounded page and
    # already ran in the run that spawned it.
    if is_nil(cursor), do: sweep_promotion_snapshots()
    redrive_deletes(cursor)
    :ok
  end

  defp decode_cursor(%{"cursor_at" => at, "cursor_id" => id}) do
    {:ok, dt, _} = DateTime.from_iso8601(at)
    {dt, id}
  end

  defp decode_cursor(_), do: nil

  defp encode_cursor({%DateTime{} = at, id}),
    do: %{"cursor_at" => DateTime.to_iso8601(at), "cursor_id" => id}

  defp redrive_deletes(cursor) do
    now = now()
    since = DateTime.add(now, -@window_days * 86_400, :second)
    deadline = System.monotonic_time(:millisecond) + budget_ms()
    walk(since, now, cursor, deadline, 0)
  rescue
    e -> Logger.warning("erasure sweep: delete re-drive failed (#{inspect(e)}); continuing")
  catch
    :exit, reason ->
      Logger.warning("erasure sweep: delete re-drive failed (#{inspect(reason)}); continuing")
  end

  defp walk(since, now, cursor, deadline, total) do
    page = page_size()
    rows = Directory.deleted_page(since, page, cursor, now)

    {leftovers, clean, outcome} = probe_page(Enum.map(rows, &elem(&1, 0)))

    # Perf review 2026-10-14 #15: remember the clean verdicts so the next sweeps skip these rows
    # until their next checkpoint. ONLY clean probes are stamped; a leftover, an error and a
    # timeout leave the row due, so a tombstone with objects is re-driven every sweep.
    Directory.mark_purge_verified(clean, now)

    for id <- leftovers do
      {:ok, _} = Oban.insert(DeleteJob.new(%{shard_id: id}))
    end

    total = total + length(leftovers)

    if leftovers != [] do
      Logger.warning(
        "erasure sweep: re-enqueued DeleteJob for #{length(leftovers)} tombstoned shard(s) " <>
          "whose objects still exist: #{inspect(leftovers)}"
      )
    end

    last = List.last(rows)

    cond do
      outcome == :aborted ->
        Logger.warning(
          "erasure sweep: #{@max_consecutive_errors} consecutive storage probe failures; " <>
            "stopping this run (the next run restarts from the newest deletion)"
        )

        total

      length(rows) < page ->
        total

      System.monotonic_time(:millisecond) >= deadline ->
        # Out of budget with the window not exhausted: hand the cursor to a continuation so the
        # older slice is visited, instead of the next hourly run re-listing the newest page.
        {:ok, _} =
          Oban.insert(
            new(encode_cursor({elem(last, 1), elem(last, 0)}), unique: false, schedule_in: 1)
          )

        total

      true ->
        walk(since, now, {elem(last, 1), elem(last, 0)}, deadline, total)
    end
  end

  # Bounded, ordered probe of one page: `{ids_with_leftovers, clean_ids, :ok | :aborted}`. Halting
  # the reduction kills the in-flight tasks.
  defp probe_page(ids) do
    ids
    |> Task.async_stream(&probe/1,
      max_concurrency: @concurrency,
      timeout: probe_timeout_ms(),
      on_timeout: :kill_task,
      ordered: true
    )
    |> Stream.zip(ids)
    |> Enum.reduce_while({[], [], 0}, fn
      {{:ok, {:ok, true}}, id}, {left, clean, _errs} ->
        {:cont, {[id | left], clean, 0}}

      {{:ok, {:ok, false}}, id}, {left, clean, _errs} ->
        {:cont, {left, [id | clean], 0}}

      {_error_or_timeout, _id}, {left, clean, errs} when errs + 1 >= @max_consecutive_errors ->
        {:halt, {left, clean, :aborted}}

      {_error_or_timeout, _id}, {left, clean, errs} ->
        {:cont, {left, clean, errs + 1}}
    end)
    |> case do
      {left, clean, :aborted} -> {Enum.reverse(left), Enum.reverse(clean), :aborted}
      {left, clean, _} -> {Enum.reverse(left), Enum.reverse(clean), :ok}
    end
  end

  defp probe(id) do
    Storage.any_shard_object?(id)
  rescue
    e -> {:error, e}
  catch
    :exit, reason -> {:error, reason}
  end

  # Expert review 2026-10-12 #8 (moved here from ReconcileJob). The 14-day `-promoexp` expiry used
  # to run only inside RetentionJob, which is off by default and only samples active,
  # scheduler-snapshotted rows. `PromoteOnOpen` stamps `promotion_snapshot_at`; this always-on sweep
  # expires the snapshots of any row (any status) whose stamp has aged out, then clears the stamp. A
  # failed list/drop leaves the stamp so the next hourly run retries. Operator-labelled snapshots
  # are never touched (`Retention.expired_pre_promotion/3` matches only the reserved marker).
  defp sweep_promotion_snapshots do
    now = DateTime.utc_now()
    cutoff = DateTime.add(now, -Retention.pre_promotion_max_age_s(), :second)

    cutoff
    |> Directory.promotion_snapshots_due(@promo_max)
    |> Task.async_stream(&expire_promotion_snapshots(&1, now),
      max_concurrency: @concurrency,
      timeout: @promo_timeout_ms,
      on_timeout: :kill_task
    )
    |> Stream.run()
  rescue
    e ->
      Logger.warning("erasure sweep: promotion snapshot sweep failed (#{inspect(e)}); continuing")
  catch
    :exit, reason ->
      Logger.warning(
        "erasure sweep: promotion snapshot sweep failed (#{inspect(reason)}); continuing"
      )
  end

  defp expire_promotion_snapshots(%{shard_id: id, promotion_snapshot_at: stamped}, now) do
    with {:ok, snaps} <- Snapshots.list(id),
         expired = Retention.expired_pre_promotion(Enum.map(snaps, & &1.id), now),
         true <- Enum.all?(expired, &(Snapshots.drop(id, &1) == :ok)) do
      Directory.clear_promotion_snapshot(id, stamped)
    else
      other ->
        Logger.warning(
          "erasure sweep: promotion snapshot expiry incomplete for #{id}: #{inspect(other)}"
        )
    end
  end

  # Injectable clock (perf review 2026-10-14 #15): the probe schedule is a function of time, and the
  # test advances it rather than sleeping.
  defp now, do: Application.get_env(:fathom, :erasure_sweep_clock, &DateTime.utc_now/0).()

  defp page_size, do: Application.get_env(:fathom, :erasure_sweep_page, @default_page)

  defp probe_timeout_ms,
    do: Application.get_env(:fathom, :erasure_sweep_probe_timeout_ms, @default_probe_timeout_ms)

  defp budget_ms, do: Application.get_env(:fathom, :erasure_sweep_budget_ms, @default_budget_ms)
end
