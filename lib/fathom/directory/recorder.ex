defmodule Fathom.Directory.Recorder do
  @moduledoc """
  Keeps the per-checkout directory write off the shard data path.

  `Fathom.Shards.checkout/1` used to call `Fathom.Directory.resolve/1` inline — a
  synchronous Postgres upsert on **every** checkout, blocking the caller on the
  control plane. That is exactly the wrong cost for a system whose thesis is
  millions of small shards opened constantly.

  This GenServer replaces that with a coalescing, batched, fire-and-forget buffer:

    * `record/2` is a lock-free ETS write (microseconds, no Postgres, no GenServer
      mailbox) — and, for a touch from the coordinator that wrote the buffered row less
      than a second ago, only a read (expert review 2026-10-19 #5). A shard hit 1000×/s
      collapses to one buffered row. Once a touch is WRITTEN, a same-coordinator touch is also
      only a read until the directory's 30 s write guard would admit another (expert review
      2026-10-21 #19), so a continuously busy shard ships one row per window, not one per second.
    * a periodic flush (default 1s) drains the buffer and batch-upserts every
      touched shard in one chunked `Repo.insert_all` (see
      `Fathom.Directory.record_batch/1`).

  Flushing is best-effort: a Postgres outage drops a flush and logs, it never
  breaks shard serving — the checkout path no longer depends on Postgres at all.
  """
  use GenServer

  require Logger

  alias Fathom.Directory

  @table __MODULE__
  @flush_table Module.concat(__MODULE__, Flushes)
  # One row: `{:last_ok_ms, monotonic_ms}`, the last flush cycle that wrote (or had nothing to
  # write). Read lock-free by `stats/0` for the admin Node-health strip (expert review 2026-10-15
  # #17).
  @meta_table Module.concat(__MODULE__, Meta)
  # `{shard_id, coordinator_pid}` of the buffered touch — the coalescing key of `record/2` (expert
  # review 2026-10-19 #5). Deleted when the touch is drained; a missing or stale row only costs a
  # touch its coalescing, never its stamp.
  @owner_table Module.concat(__MODULE__, Owners)
  # `{shard_id, coordinator_pid, quiet_until_us}`: the touch this coordinator last got WRITTEN, and
  # the node-clock instant before which the directory's write guard would reject another (expert
  # review 2026-10-21 #19). Set only from what the database returned (see `mark_quiet/3`), deleted by
  # `record_flush/1` and before a buffered flush is drained, swept once expired.
  @quiet_table Module.concat(__MODULE__, Quiet)
  # A touch within this window of a buffered one from the same coordinator only reads (`record/2`).
  @coalesce_us 1_000_000
  # How often the expired rows of `@quiet_table` are swept. An expired row is dead weight only: it
  # never suppresses anything.
  @quiet_sweep_ms 10_000
  @default_flush_ms 1_000

  @doc false
  @spec start_link(keyword()) :: GenServer.on_start()
  def start_link(opts) do
    GenServer.start_link(__MODULE__, opts, name: __MODULE__)
  end

  @doc """
  Buffers a shard access for the next flush. Lock-free ETS write; returns `:ok`
  without ever touching Postgres. Safe to call before the recorder is up (a
  missed touch never breaks serving) — the access is simply dropped.

  `coordinator` is the pid of the shard coordinator that granted the checkout; with it, a touch
  inside the coalescing window of a buffered one from the same coordinator is a read, not a write
  (see the body). `nil` always buffers.
  """
  @spec record(String.t(), pid() | nil) :: :ok
  def record(shard_id, coordinator \\ nil) do
    now = System.os_time(:microsecond)

    # COALESCED for a known coordinator (expert review 2026-10-19 #5). Every checkout of a shard
    # inserted the SAME key, so one hot shard's streams convoyed on its lock: a probe measured
    # 143 us per insert at 18 concurrent callers (1.0 alone), and same-shard checkout+checkin at 72
    # workers swung between 124k and 603k/s with this on against a steady ~608k/s off. A touch now
    # only READS when the buffer already holds one from the last `@coalesce_us`.
    #
    # NOT a plain "skip if fresh": `last_active_at` must stay exact at two edges (see
    # `Fathom.Directory.record_batch/1`), and a skip that keeps an OLDER buffered stamp can lose
    # the first touch past either of them:
    #
    #   * a CUTOVER — the revert write-age guard refuses on `last_active_at > cutover_at`, so a
    #     dropped post-cutover touch lets a revert discard post-cutover writes. A migration holds
    #     the S3 lease, so the shard's coordinator must stop first: every touch after a cutover
    #     comes from a NEW coordinator pid. The skip requires the buffered touch to be from the
    #     SAME coordinator (`@owner_table`), so the first touch of a new incarnation always lands.
    #   * a FLUSH — the loss report calls a shard dirty on `last_active_at > last_flushed_at`. The
    #     skip also requires NO buffered flush row for the shard, and the joint drain takes a
    #     shard's touch BEFORE its flush row (`take_joint/1`), so from the moment `record_flush/1`
    #     buffers a stamp until both are written, every touch inserts its own stamp.
    #
    # Inside one incarnation, between flushes, `last_active_at` may trail by up to `@coalesce_us`,
    # far finer than the 30 s write granularity downstream. `coordinator: nil` never coalesces.
    #
    # QUIET WINDOW (expert review 2026-10-21 #19). With nothing buffered, a touch is also only a
    # read when the same coordinator's last WRITTEN touch left the directory guard rejecting
    # re-touches until a later instant (`@quiet_table`). Before this, a shard checked out
    # continuously (the HTTP Hrana path: one checkout per stream) shipped a row every second and
    # the guard threw away 29 of every 30 (probe: 1000 busy shards, 983 rows/s shipped, 2000
    # written in 60 s). The same two edges hold: the window is keyed by coordinator (cutover), is
    # never set when the stored stamp is at or before `last_flushed_at`/`cutover_at`, and is
    # ignored while a flush row is buffered and deleted by `record_flush/1` (flush).
    if coalesced?(shard_id, coordinator, now) do
      :ok
    else
      buffer_touch(shard_id, coordinator, now)
    end
  rescue
    # Table not up yet (boot/teardown). Best-effort: a dropped touch is harmless,
    # the next checkout re-records.
    ArgumentError -> :ok
  end

  defp coalesced?(_shard_id, nil, _now), do: false

  defp coalesced?(shard_id, coordinator, now) do
    case :ets.lookup(@table, shard_id) do
      [{_, at}] when now - at < @coalesce_us and now >= at ->
        :ets.lookup(@owner_table, shard_id) == [{shard_id, coordinator}] and
          not :ets.member(@flush_table, shard_id)

      [] ->
        quiet?(shard_id, coordinator, now)

      _ ->
        false
    end
  end

  # Only with NOTHING buffered: a buffered touch is written whatever this says, and its write sets
  # the next window.
  defp quiet?(shard_id, coordinator, now) do
    case :ets.lookup(@quiet_table, shard_id) do
      [{_, ^coordinator, until}] when now < until -> not :ets.member(@flush_table, shard_id)
      _ -> false
    end
  end

  defp buffer_touch(shard_id, coordinator, now) do
    # A bare integer (µs — the schema's :utc_datetime_usec precision), not DateTime.utc_now():
    # the buffer only needs an ordering timestamp, and the calendar conversion + 9-field
    # struct allocation ran on EVERY checkout (review 2026-07-23 #24). The flush converts once
    # per distinct buffered shard (coalescing means far fewer conversions than checkouts) —
    # and the integer is a smaller ETS object too.
    #
    # `System.os_time/1`, NOT `System.system_time/1`. Both are cheap integer BIFs, so #24's
    # optimization is unaffected — but they are DIFFERENT CLOCKS. `System.system_time` is Erlang
    # system time (monotonic + a continuously-slewed offset); `DateTime.utc_now/0` is
    # `System.os_time`. Every other directory timestamp — `cutover_at`, `migrating_since`,
    # `inserted_at` — comes from `DateTime.utc_now/0`, and the revert write-age guard compares
    # this stamp against `cutover_at` with a strict `DateTime.compare(...) == :gt`.
    #
    # Under `multi_time_warp` those clocks drift apart by microseconds to milliseconds, and on the
    # machine this was found on Erlang system time ran 2-10 µs BEHIND OS time: a touch recorded
    # AFTER a cutover compared as BEFORE it in 4994 of 5000 samples. When the gap between the
    # cutover and the touch is smaller than the skew, the guard fails to refuse an unforced revert
    # and DISCARDS committed post-cutover tenant writes — exactly the loss the guard exists to
    # prevent. It surfaced as an intermittent failure in ShardMigrationTest.
    #
    # Since expert review 2026-10-12 #8 `cutover_at` is on the POSTGRES clock and the flush rebases
    # these stamps onto it by an offset sampled with `System.os_time` (`do_flush/2`). This stamp and
    # that sample must still be the same clock, or the offset carries the slew above.
    :ets.insert(@table, {shard_id, now})
    if coordinator, do: :ets.insert(@owner_table, {shard_id, coordinator})
    :ok
  end

  @doc """
  Buffers a shard's durable-flush time for the next batch (expert review #28) — the flush
  counterpart of `record/1`, keeping the coordinator's flush hot path off Postgres. Lock-free ETS;
  best-effort (a missed flush record just reads as a slightly-larger loss window later — the safe
  direction). Called by `Fathom.Shard` after a successful upload.
  """
  @spec record_flush(String.t()) :: :ok
  def record_flush(shard_id) do
    # The flush edge (expert review 2026-10-21 #19): the first touch after this flush must land, so
    # end the quiet window. BEFORE the flush row goes in: a touch between the two then finds
    # neither and buffers. A window a concurrent drain sets after this delete is harmless, since
    # the buffered flush row disables it and `take_pair/2` deletes it before taking that row.
    :ets.delete(@quiet_table, shard_id)

    # `System.os_time`, for the same reason as `record/1` above: `last_flushed_at` is compared
    # against `last_active_at` to compute each tenant's loss window, so it must be on the same
    # clock or the window is off by the skew — in the direction that UNDER-reports loss.
    :ets.insert(@flush_table, {shard_id, System.os_time(:microsecond)})
    :ok
  rescue
    ArgumentError -> :ok
  end

  @doc """
  Flushes the buffer to Postgres now and returns the number of rows written.
  Synchronous — used by tests (deterministic, no sleep) and the graceful-stop
  path. The periodic timer does the same work in the background.
  """
  @spec flush() :: non_neg_integer()
  def flush, do: GenServer.call(__MODULE__, :flush, flush_timeout_ms())

  # The flush is a Postgres write over an unbounded buffer, so the default 5 s call timeout could
  # expire mid-flush under load (expert review 2026-10-12 #35), and the revert guard's caller then
  # read a directory the Recorder had not finished writing. `:recorder_flush_timeout_ms` is the
  # test seam for exercising that timeout.
  defp flush_timeout_ms, do: Application.get_env(:fathom, :recorder_flush_timeout_ms, 60_000)

  @doc false
  # Test seam (expert review 2026-10-12 #8): `node_clock_us:` replaces this node's clock reading
  # when the flush samples its offset to the Postgres clock, to simulate a node whose clock is
  # skewed. Never used outside tests.
  @spec flush(keyword()) :: non_neg_integer()
  def flush(opts), do: GenServer.call(__MODULE__, {:flush, opts})

  @impl true
  def init(opts) do
    # Trap exits so a supervisor :shutdown runs terminate/2's final flush (expert
    # review #30): without this a GenServer is killed outright by the shutdown
    # signal and terminate never runs — the documented "don't lose the last window
    # of touches on a graceful stop" was dead code on every deploy, and those
    # touches feed last_active_at, the revert write-age guard's input.
    Process.flag(:trap_exit, true)

    # public + write_concurrency: many checkout processes insert concurrently;
    # the recorder is the only reader (during flush).
    #
    # read_concurrency since expert review 2026-10-19 #5: a coalesced `record/2` only READS these,
    # and the writes drop to about one per shard per second.
    :ets.new(@table, [
      :set,
      :public,
      :named_table,
      write_concurrency: true,
      read_concurrency: true
    ])

    # Parallel buffer for durable-flush times (#28), same coalesce/batch shape.
    :ets.new(@flush_table, [
      :set,
      :public,
      :named_table,
      write_concurrency: true,
      read_concurrency: true
    ])

    :ets.new(@owner_table, [
      :set,
      :public,
      :named_table,
      write_concurrency: true,
      read_concurrency: true
    ])

    :ets.new(@quiet_table, [
      :set,
      :public,
      :named_table,
      write_concurrency: true,
      read_concurrency: true
    ])

    :ets.new(@meta_table, [:set, :public, :named_table])
    note_flush_ok()

    flush_ms =
      Keyword.get(
        opts,
        :flush_ms,
        Application.get_env(:fathom, :directory_flush_ms, @default_flush_ms)
      )

    schedule(flush_ms)
    {:ok, %{flush_ms: flush_ms, swept_ms: System.monotonic_time(:millisecond)}}
  end

  @impl true
  def handle_call(:flush, _from, state) do
    {:reply, do_flush(), state}
  end

  def handle_call({:flush, opts}, _from, state) do
    {:reply, do_flush(:infinity, Keyword.get(opts, :node_clock_us, &node_clock_us/0)), state}
  end

  @impl true
  def handle_info(:flush, state) do
    do_flush()
    schedule(state.flush_ms)
    {:noreply, maybe_sweep_quiet(state)}
  end

  # Bounds `@quiet_table` to shards written in about the last granularity plus `@quiet_sweep_ms`
  # (expert review 2026-10-21 #19). A stopped coordinator's row is never matched again, so it only
  # waits here for its expiry.
  defp maybe_sweep_quiet(state) do
    now_ms = System.monotonic_time(:millisecond)

    if now_ms - state.swept_ms >= @quiet_sweep_ms do
      _ = sweep_quiet(node_clock_us())
      %{state | swept_ms: now_ms}
    else
      state
    end
  end

  @doc false
  # Deletes the expired quiet windows and returns how many. Public for the sweep test.
  @spec sweep_quiet(integer()) :: non_neg_integer()
  def sweep_quiet(now_us) do
    :ets.select_delete(@quiet_table, [{{:_, :_, :"$1"}, [{:"=<", :"$1", now_us}], [true]}])
  rescue
    ArgumentError -> 0
  end

  @impl true
  def terminate(_reason, _state) do
    # Don't lose the last window of touches on a graceful stop (e.g. a deploy).
    #
    # DEADLINE-AWARE, and it has an explicit `shutdown:` in the supervision tree (expert review
    # 2026-08-01 #38). This ran under `use GenServer`'s default 5 s at the worst possible moment:
    # shutdown order is Edge → DataPlane → ControlPlane → Infra, so EVERY coordinator's
    # terminate-flush has already run and each successful upload called `record_flush/1`. The
    # `@flush_table` therefore holds a `last_flushed_at` row for every shard the node just
    # flushed, and this is the only chance to persist them before the process is `:kill`ed.
    #
    # A truncated flush is SILENT and wrong in the misleading direction: `flush_lag_report/1` and
    # `mix fathom.shard loss-report` then show those shards as flushed LONGER AGO than they were,
    # so an operator assessing post-incident loss under-reads it — on exactly the node that just
    # went down. Bound it, and say so when it gives up.
    deadline = System.monotonic_time(:millisecond) + shutdown_budget_ms()
    _ = do_flush(deadline)

    case pending() do
      0 ->
        :ok

      n ->
        Logger.error(
          "Directory.Recorder: shutdown flush gave up with #{n} buffered row(s) undrained " <>
            "after #{shutdown_budget_ms()}ms — flush_lag_report/loss-report will under-report " <>
            "how recently these shards flushed. Raise the Recorder's :shutdown budget."
        )

        :ok
    end
  end

  defp pending, do: table_size(@table) + table_size(@flush_table)

  @doc """
  Buffer depth and flush freshness, read straight from ETS (no call into the recorder, so a wedged
  Postgres flush cannot block the 10 s poller that reads this). `buffered` is the shard-access
  rows awaiting upsert, `flush_buffered` the durable-flush-time rows; `last_flush_age_ms` is how
  long ago a flush cycle last succeeded (or found nothing to do), `nil` before the first.

  A growing `buffered` with a growing `last_flush_age_ms` is a Postgres outage the recorder is
  deliberately hiding from the data path (expert review 2026-10-15 #17). Zeros/`nil` when the
  recorder is not running.
  """
  @spec stats() :: %{
          buffered: non_neg_integer(),
          flush_buffered: non_neg_integer(),
          last_flush_age_ms: non_neg_integer() | nil
        }
  def stats do
    age =
      case :ets.lookup(@meta_table, :last_ok_ms) do
        [{_, at}] -> max(System.monotonic_time(:millisecond) - at, 0)
        _ -> nil
      end

    %{
      buffered: table_size(@table),
      flush_buffered: table_size(@flush_table),
      last_flush_age_ms: age
    }
  rescue
    ArgumentError -> %{buffered: 0, flush_buffered: 0, last_flush_age_ms: nil}
  end

  defp note_flush_ok do
    :ets.insert(@meta_table, {:last_ok_ms, System.monotonic_time(:millisecond)})
  rescue
    ArgumentError -> :ok
  end

  defp table_size(table) do
    case :ets.info(table, :size) do
      n when is_integer(n) -> n
      _ -> 0
    end
  end

  # Under the 30_000 child `shutdown:` in `Fathom.Application.control_plane_children/0`, leaving
  # room for the log above — the supervisor's timer is a hard `:kill`, so overshooting it means
  # losing the very message that says data was lost.
  defp shutdown_budget_ms,
    do: Application.get_env(:fathom, :directory_recorder_shutdown_budget_ms, 25_000)

  defp schedule(ms), do: Process.send_after(self(), :flush, ms)

  # `deadline` is a monotonic-ms instant to stop draining at, or `:infinity` for the periodic
  # path (which has a whole interval and re-runs a second later anyway).
  #
  # FLUSHES BEFORE ACCESSES (expert review 2026-10-08 #24). `record_batch/1` skips a re-touch within
  # the granularity window unless the stored `last_active_at` is at or behind `last_flushed_at` (or
  # `cutover_at`) — the escape that keeps "dirty vs clean" exact. With accesses written first, a
  # flush at t1 and a later access at t2 in the SAME tick lost that access: the touch was skipped
  # against the OLD flush stamp, then the flush stamp moved past `last_active_at`, and
  # `flush_lag_report/1` called the shard clean while it held unflushed writes from t2 — wrong
  # until its next flush, which for a shard gone quiet may never come. Landing the flush first lets
  # the escape see it.
  #
  # ONE CLOCK: POSTGRES'S (expert review 2026-10-12 #8). The buffered stamps are this node's
  # `System.os_time`, but `cutover_at` is stamped by whichever node ran the migration, and the
  # revert write-age guard compares the two. A serving node whose clock ran behind the migrator's
  # recorded a post-cutover write as pre-cutover, the guard let an unforced revert through, and the
  # revert discarded that write. So every stamp is rebased onto the Postgres clock before it is
  # written — the clock `Directory.cutover/2` also stamps `cutover_at` from.
  #
  # The rebase is an OFFSET sampled once per flush (`db_offset_us/1`), not a per-row
  # `clock_timestamp()` in the upsert: one offset applied to both buffers keeps every relative
  # order between this node's stamps exact to the microsecond — the flush-vs-access ordering the
  # loss report depends on (#24 above) — while moving all of them onto the shared clock. The node
  # clock is read BEFORE the database's, so the offset can only overshoot (by the query's latency):
  # a rebased stamp is never earlier than the event, which errs toward REFUSING a revert, the
  # guard's documented safe direction. Do not swap the two reads.
  #
  # No offset (Postgres unreachable) ⇒ write nothing and leave the buffer for the next tick: the
  # upsert would fail in the same outage, and writing node-clock stamps is the bug.
  defp do_flush(deadline \\ :infinity, node_clock \\ &node_clock_us/0) do
    if pending() == 0 do
      note_flush_ok()
      0
    else
      case db_offset_us(node_clock) do
        {:ok, offset} ->
          flush_joint(deadline, offset, 0) +
            flush_table(
              @table,
              &Directory.record_batch_with_quiet/1,
              [:fathom, :directory, :flush],
              deadline,
              offset
            )

        :error ->
          0
      end
    end
  end

  # The clock `record/1` and `record_flush/1` stamp with.
  defp node_clock_us, do: System.os_time(:microsecond)

  defp db_offset_us(node_clock) do
    before = node_clock.()
    {:ok, DateTime.to_unix(Directory.db_now(), :microsecond) - before}
  rescue
    e ->
      Logger.warning("Directory.Recorder flush skipped, no database clock: #{inspect(e)}")
      :error
  catch
    :exit, reason ->
      Logger.warning("Directory.Recorder flush skipped, no database clock: #{inspect(reason)}")
      :error
  end

  # Drain and upsert in bounded chunks (expert review 2026-07-24 #23) rather than materializing the
  # whole buffer. At 30k active shards the old shape allocated, every second: a full `tab2list`
  # copy, a second full copy from the per-key `take`, and a `DateTime` per row — ~8-15 MB of
  # transient heap in a process with `fullsweep_after: 65535` and no post-flush GC, so it grew to
  # its worst-ever batch size and stayed there. It also did not scale: at 300k active shards the
  # peak batch is ~100 MB in one process heap.
  #
  # Chunking is strictly SAFER than the old shape, not just cheaper: a failed upsert now re-buffers
  # one chunk instead of the entire drain, and the un-drained remainder was never removed from ETS
  # in the first place.
  @drain_chunk 2_000

  defp flush_table(table, upsert, event, deadline, offset) do
    flush_chunks(table, upsert, event, 0, deadline, offset)
  end

  # STOPS on a failed chunk. `restore/2` re-buffers the failed rows into the same table, so
  # continuing would immediately re-drain them and loop forever. Halting leaves them for the next
  # flush cycle, which is exactly the pre-chunking behaviour.
  defp flush_chunks(table, upsert, event, acc, deadline, offset) do
    if past?(deadline) do
      acc
    else
      case drain_chunk(table) do
        {[], _owners} ->
          acc

        {rows, owners} ->
          case flush_rows(table, rows, upsert, event, offset) do
            {:ok, n, quiet} ->
              mark_quiet(quiet, owners, offset)
              flush_chunks(table, upsert, event, acc + n, deadline, offset)

            :failed ->
              acc
          end
      end
    end
  end

  # Expert review 2026-10-17 #21. A shard flushed AND touched in one tick used to cost two statements,
  # each a non-HOT row version. Drain the flush buffer chunk by chunk; for each flushed shard take its
  # buffered touch, if there is one (`:ets.take`, atomic per key), and write the pair in ONE statement
  # via `Directory.record_flush_and_touch_batch/1`. A flush with no buffered touch goes through the
  # unchanged `record_flush_batch/1`. Touch-only shards stay in the touch buffer for `flush_table/5`.
  #
  # This keeps the flush-before-access ordering the comment in `do_flush/2` describes: the joint
  # statement takes the touch guard against the post-flush watermark itself, so the edge is the same
  # as the sequential pair.
  defp flush_joint(deadline, offset, acc) do
    if past?(deadline) do
      acc
    else
      case take_joint(@drain_chunk) do
        {[], [], _owners} ->
          acc

        {both, only, owners} ->
          case write_joint(both, offset, owners) do
            {:ok, n} ->
              case write_flush_only(only, offset) do
                {:ok, m} -> flush_joint(deadline, offset, acc + n + m)
                :failed -> acc + n
              end

            :failed ->
              # `write_joint/2` re-buffered its own touches and flushes. The rest were drained, so
              # put them back too, or they wait for a next tick that never sees them.
              restore(@flush_table, only)
              acc
          end
      end
    end
  end

  # For each buffered flush, take the shard's touch FIRST and its flush row second (expert review
  # 2026-10-19 #5). `record/2` coalesces only while no flush row is buffered, so taking the flush row
  # first opened a gap — flush row gone, touch still buffered — in which a post-flush touch was
  # coalesced into the PRE-flush stamp and the loss report then called the shard clean. Touch-first,
  # a touch in that gap finds no touch row and buffers its own.
  defp take_joint(limit) do
    case :ets.select(@flush_table, [{{:"$1", :_}, [], [:"$1"]}], limit) do
      {ids, _cont} -> Enum.reduce(ids, {[], [], %{}}, &take_pair/2)
      :"$end_of_table" -> {[], [], %{}}
    end
  rescue
    ArgumentError -> {[], [], %{}}
  end

  defp take_pair(id, {both, only, owners}) do
    {touch, owner} = take_touch(id)
    # End the quiet window BEFORE the flush row leaves the buffer (expert review 2026-10-21 #19),
    # for the same reason the touch is taken first: once the row is gone nothing else disables the
    # window, and a touch in that gap would be dropped although it is the first after the flush.
    # The joint write sets a fresh window only if the stored stamp ends up past the flush.
    :ets.delete(@quiet_table, id)
    owners = if owner, do: Map.put(owners, id, owner), else: owners

    case {:ets.take(@flush_table, id), touch} do
      {[{^id, flushed_us}], [{^id, touched_us}]} ->
        {[{id, flushed_us, touched_us} | both], only, owners}

      {[{^id, _} = row], []} ->
        {both, [row | only], owners}

      # The flush row went between the select and the take (only a drain takes it, so this is a
      # concurrent shutdown drain): hand the touch back for the touch-only pass.
      {[], taken} ->
        Enum.each(taken, &:ets.insert_new(@table, &1))
        {both, only, owners}
    end
  end

  # Takes a buffered touch and its coalescing owner, and returns both. Taking the owner AFTER the
  # touch can only cost a racing touch its coalescing (it then inserts), never lose a stamp. A racing
  # touch from a NEW coordinator can make this return the new owner with the old stamp; the quiet
  # window set from that is keyed to the new coordinator, whose own touch is still buffered and is
  # written on the next tick regardless.
  defp take_touch(id) do
    case :ets.take(@table, id) do
      [] ->
        {[], nil}

      taken ->
        case :ets.take(@owner_table, id) do
          [{^id, owner}] -> {taken, owner}
          [] -> {taken, nil}
        end
    end
  end

  # Records the windows the directory returned for the touches it just wrote (expert review
  # 2026-10-21 #19), keyed by the coordinator whose touch was written. A `nil` coordinator never
  # coalesces, so it gets no window. The stamps come back on the Postgres clock; `offset` (the one
  # this flush rebased with) turns them back into the node clock `record/2` compares against. A
  # later flush's offset can differ by its query latency, so the window can be off by that much
  # (milliseconds) against the guard in either direction.
  defp mark_quiet([], _owners, _offset), do: :ok

  defp mark_quiet(quiet, owners, offset) do
    Enum.each(quiet, fn {id, until} ->
      case Map.fetch(owners, id) do
        {:ok, owner} ->
          :ets.insert(@quiet_table, {id, owner, DateTime.to_unix(until, :microsecond) - offset})

        :error ->
          :ok
      end
    end)
  rescue
    ArgumentError -> :ok
  end

  defp write_joint([], _offset, _owners), do: {:ok, 0}

  defp write_joint(both, offset, owners) do
    entries =
      Enum.map(both, fn {id, flushed_us, touched_us} ->
        {id, to_datetime(flushed_us, offset), to_datetime(touched_us, offset)}
      end)

    try do
      {n, quiet} = Directory.record_flush_and_touch_batch_with_quiet(entries)
      mark_quiet(quiet, owners, offset)

      if n > 0 do
        :telemetry.execute([:fathom, :directory, :flush_recorded], %{count: n}, %{})
        :telemetry.execute([:fathom, :directory, :flush], %{count: n}, %{})
      end

      note_flush_ok()
      {:ok, n}
    rescue
      e ->
        Logger.warning("Directory.Recorder flush failed: #{inspect(e)}")
        restore_joint(both)
        :failed
    catch
      :exit, reason ->
        Logger.warning("Directory.Recorder flush exited: #{inspect(reason)}")
        restore_joint(both)
        :failed
    end
  end

  # Re-buffers both halves of a failed joint chunk, with the same `insert_new` semantics as `restore/2`.
  defp restore_joint(both) do
    restore(@flush_table, Enum.map(both, fn {id, flushed_us, _} -> {id, flushed_us} end))
    restore(@table, Enum.map(both, fn {id, _, touched_us} -> {id, touched_us} end))
  end

  defp write_flush_only([], _offset), do: {:ok, 0}

  defp write_flush_only(rows, offset) do
    case flush_rows(
           @flush_table,
           rows,
           &{Directory.record_flush_batch(&1), []},
           [:fathom, :directory, :flush_recorded],
           offset
         ) do
      {:ok, n, _quiet} -> {:ok, n}
      :failed -> :failed
    end
  end

  # The same rebase `flush_rows/5` applies: once per distinct shard, on the Postgres clock.
  defp to_datetime(us, offset), do: DateTime.from_unix!(us + offset, :microsecond)

  defp past?(:infinity), do: false
  defp past?(deadline), do: System.monotonic_time(:millisecond) >= deadline

  # No empty-list branch: `flush_chunks/6` is the only caller and already returns on `drain_chunk`
  # giving `[]`, so `rows` is always non-empty here. The check used to live in both places and was
  # left behind when chunking moved it up.
  defp flush_rows(table, rows, upsert, event, offset) do
    case rows do
      rows ->
        try do
          # The buffer holds integer microsecond stamps (see record/1); Directory's batch API
          # keeps its DateTime contract (:utc_datetime_usec — hence microsecond, not ms), so
          # convert here — once per distinct shard per flush — rebased onto the Postgres clock
          # (#8, see do_flush/2). `restore/2` re-buffers the RAW node stamps, so a retried row
          # is rebased with the retry's fresh offset, never twice.
          {n, quiet} =
            upsert.(
              Enum.map(rows, fn {id, us} ->
                {id, DateTime.from_unix!(us + offset, :microsecond)}
              end)
            )

          if n > 0, do: :telemetry.execute(event, %{count: n}, %{})
          note_flush_ok()
          {:ok, n, quiet}
        rescue
          e ->
            Logger.warning("Directory.Recorder flush failed: #{inspect(e)}")
            restore(table, rows)
            :failed
        catch
          :exit, reason ->
            Logger.warning("Directory.Recorder flush exited: #{inspect(reason)}")
            restore(table, rows)
            :failed
        end
    end
  end

  # A failed batch must not lose the drained touches (expert review #11): these feed
  # `last_active_at`, the sole input to the revert write-age force-guard — dropping
  # them during a Postgres outage is exactly when operators are reverting things.
  # Re-buffer them for the next cycle. `insert_new` (not `insert`) so a fresher touch
  # recorded DURING the failed flush wins — anything re-recorded post-drain is by
  # construction newer than the drained value.
  defp restore(table, rows) do
    Enum.each(rows, &:ets.insert_new(table, &1))
    :telemetry.execute([:fathom, :directory, :flush_retry], %{count: length(rows)}, %{})
  rescue
    # Table gone (teardown) — nothing to restore into.
    ArgumentError -> :ok
  end

  # Atomically take each buffered key (`:ets.take` reads + deletes in one op), so a
  # touch arriving mid-flush is either captured with its freshest value or left in
  # the table for the next cycle — never silently lost.
  #
  # `:ets.select/3` with a limit walks the keys without copying the whole table first, so peak heap
  # is O(@drain_chunk) rather than O(active shards). The per-key `take` is unchanged and still the
  # thing that makes each row's removal atomic.
  defp drain_chunk(table) do
    case :ets.select(table, [{{:"$1", :_}, [], [:"$1"]}], @drain_chunk) do
      # Only the touch buffer is drained here (the flush buffer goes through `take_joint/1`), so
      # every take also takes the touch's coalescing owner, which keys its quiet window (#19).
      {ids, _cont} ->
        Enum.reduce(ids, {[], %{}}, fn id, {rows, owners} ->
          case take_touch(id) do
            {[], _} -> {rows, owners}
            {taken, nil} -> {taken ++ rows, owners}
            {taken, owner} -> {taken ++ rows, Map.put(owners, id, owner)}
          end
        end)

      :"$end_of_table" ->
        {[], %{}}
    end
  rescue
    # Table gone (teardown).
    ArgumentError -> {[], %{}}
  end
end
