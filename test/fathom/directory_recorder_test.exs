defmodule Fathom.Directory.RecorderTest do
  # Exercises the app-global recorder + Postgres, so not async (shared sandbox lets
  # the recorder process write through the test's connection).
  use Fathom.DataCase, async: false

  alias Fathom.Directory
  alias Fathom.Directory.Recorder

  setup do
    # Drain any buffer leftover from a prior test so flush counts are deterministic.
    Recorder.flush()
    :ok
  end

  defp uniq, do: "rec_#{System.unique_integer([:positive])}"

  test "coalesces repeated accesses into one row per shard and batch-flushes" do
    a = uniq()
    b = uniq()

    assert :ok = Recorder.record(a)
    assert :ok = Recorder.record(a)
    assert :ok = Recorder.record(b)

    # Two distinct shards, despite three records (a was coalesced).
    assert Recorder.flush() == 2

    assert {:ok, %{shard_id: ^a, schema_version: 0, status: "active"}} = Directory.get(a)
    assert {:ok, %{shard_id: ^b, schema_version: 0, status: "active"}} = Directory.get(b)
  end

  # Expert review 2026-10-08 #24: a flush (t1) and a later access (t2, inside the 30 s re-touch
  # window) buffered in the SAME tick. Accesses were written first, so the touch was skipped against
  # the OLD flush stamp, then the flush stamp moved past `last_active_at` — and `flush_lag_report/1`
  # called the shard clean while it held unflushed writes from t2. The buffers are filled directly
  # with chosen instants so the ordering is exact rather than raced.
  test "an access after a flush in the same tick still marks the shard dirty" do
    a = uniq()
    {:ok, _} = Directory.resolve(a)
    now = System.os_time(:microsecond)
    at = fn us_ago -> DateTime.from_unix!(now - us_ago, :microsecond) end

    # Stored: dirty — last active 10 s ago, last flushed 20 s ago.
    {1, _} =
      Fathom.Repo.update_all(
        Ecto.Query.from(s in Fathom.Directory.Shard, where: s.shard_id == ^a),
        set: [last_active_at: at.(10_000_000), last_flushed_at: at.(20_000_000)]
      )

    # Buffered in one tick: a flush 5 s ago, then an access 2 s ago (8 s after the stored touch,
    # inside the re-touch window).
    :ets.insert(Module.concat(Recorder, Flushes), {a, now - 5_000_000})
    :ets.insert(Recorder, {a, now - 2_000_000})
    Recorder.flush()

    assert Enum.any?(Directory.flush_lag_report(1_000), &(&1.shard_id == a)),
           "the shard holds a write made after its last flush but the report calls it clean"
  end

  test "flushing an empty buffer is a no-op" do
    assert Recorder.flush() == 0
  end

  test "re-recording bumps recency without resetting version or status" do
    a = uniq()

    assert :ok = Recorder.record(a)
    assert Recorder.flush() == 1
    {:ok, first} = Directory.get(a)

    # Advance the shard's lifecycle the way a migration would.
    {:ok, _} = Directory.cutover(a, 5)

    assert :ok = Recorder.record(a)
    assert Recorder.flush() == 1
    {:ok, second} = Directory.get(a)

    # The on-conflict path only touches recency.
    assert second.schema_version == 5
    assert second.status == "active"
    assert DateTime.compare(second.last_active_at, first.last_active_at) in [:gt, :eq]
  end

  # Expert review #11: do_flush drained the ETS buffer BEFORE the Postgres write, and a
  # failed batch (outage) just logged — the drained touches were gone. Those touches feed
  # last_active_at, the sole input to the revert write-age force-guard, and a Postgres
  # outage is exactly when operators revert things. The invariant: a failed flush
  # re-buffers what it drained, so the touches land once Postgres recovers.
  test "a failed flush re-buffers the drained touches instead of dropping them" do
    import ExUnit.CaptureLog

    a = uniq()
    assert :ok = Recorder.record(a)

    # The outage: cut the recorder process off from Postgres, so record_batch raises
    # an ownership error inside the flush.
    Ecto.Adapters.SQL.Sandbox.mode(Fathom.Repo, :manual)

    log = capture_log(fn -> assert Recorder.flush() == 0 end)
    assert log =~ "Recorder flush"

    # Postgres recovers (a fresh shared owner): the touch must still be buffered
    # and flush through.
    owner = Ecto.Adapters.SQL.Sandbox.start_owner!(Fathom.Repo, shared: true)
    on_exit(fn -> Ecto.Adapters.SQL.Sandbox.stop_owner(owner) end)

    assert Recorder.flush() == 1
    assert {:ok, %{shard_id: ^a}} = Directory.get(a)
  end

  # Expert review #30: terminate/2 documents "don't lose the last window of touches on
  # a graceful stop" and flushes the buffer — but the Recorder never trapped exits, so a
  # supervisor :shutdown killed it outright and terminate NEVER ran: the documented
  # guarantee was dead code on every deploy, and the dropped touches feed
  # last_active_at, the revert write-age guard's sole input. The invariant: a graceful
  # stop flushes the buffered window.
  test "a supervisor shutdown flushes the buffered touches via terminate/2" do
    a = uniq()
    assert :ok = Recorder.record(a)

    pid = Process.whereis(Recorder)
    ref = Process.monitor(pid)
    :ok = Supervisor.terminate_child(Fathom.ControlPlane.Supervisor, Recorder)
    assert_receive {:DOWN, ^ref, :process, ^pid, :shutdown}, 1_000

    on_exit(fn -> Supervisor.restart_child(Fathom.ControlPlane.Supervisor, Recorder) end)

    assert {:ok, %{shard_id: ^a}} = Directory.get(a),
           "the buffered touch must be flushed by the graceful stop"

    {:ok, _} = Supervisor.restart_child(Fathom.ControlPlane.Supervisor, Recorder)
  end

  # #28: durable-flush times ride the same coalesce/batch buffer, off the coordinator's flush hot
  # path, so they survive node death for the post-loss report.
  test "record_flush buffers a durable-flush time and batch-writes last_flushed_at" do
    a = uniq()
    {:ok, %{last_flushed_at: nil}} = Directory.resolve(a)

    assert :ok = Recorder.record_flush(a)
    assert Recorder.flush() >= 1

    assert {:ok, %{last_flushed_at: flushed}} = Directory.get(a)
    assert flushed, "the buffered flush time must reach last_flushed_at"
  end

  @tag :bench
  test "record/1 stays off the Postgres hot path (sub-50µs ETS write)" do
    id = uniq()
    # Warm the path, then measure a steady-state buffer write.
    Recorder.record(id)
    {us, :ok} = :timer.tc(fn -> Recorder.record(id) end)

    # An ETS insert is single-digit µs; a synchronous Postgres upsert (~100µs+,
    # see dir_resolve_p50_us) could never land under this ceiling.
    assert us < 50
  end

  # Expert review 2026-07-24 #23: the drain used to `tab2list` the whole buffer and then re-read
  # every row with `take` — two full copies plus a DateTime per row, ~8-15 MB of transient heap per
  # second at 30k active shards, in a process that never sheds it. It is now chunked, so peak heap
  # is O(chunk) rather than O(active shards).
  #
  # The load-bearing property is that chunking must not change what gets written: every buffered
  # touch still lands exactly once, across as many chunks as it takes.
  test "a buffer larger than one drain chunk is flushed completely and exactly once" do
    # @drain_chunk is 2_000, so this spans several chunks.
    ids = for i <- 1..4_500, do: "chunk_#{i}"
    for id <- ids, do: :ok = Recorder.record(id)

    written = Recorder.flush()

    assert written == length(ids),
           "every buffered touch must be written, across chunk boundaries"

    # The buffer is empty afterwards — nothing stranded by the chunk loop.
    assert Recorder.flush() == 0

    # And each one really reached Postgres.
    sample = ["chunk_1", "chunk_2000", "chunk_2001", "chunk_4500"]

    for id <- sample do
      assert {:ok, %{shard_id: ^id}} = Fathom.Directory.get(id),
             "#{id} was buffered but never written"
    end
  end

  # Regression: the recorder stamped `last_active_at` from `System.system_time/1` (Erlang system
  # time) while every other directory timestamp — `cutover_at`, `migrating_since`, `inserted_at` —
  # comes from `DateTime.utc_now/0`, which is `System.os_time/1`. Two clocks.
  #
  # `Fathom.Migrator.ShardMigration`'s revert write-age guard compares them directly
  # (`DateTime.compare(last_active, cutover_at) == :gt`). Under `multi_time_warp` the offset
  # between them is continuously slewed; on the machine this was found Erlang system time ran
  # 2-10 µs BEHIND OS time, so a touch recorded AFTER a cutover compared as BEFORE it in 4994 of
  # 5000 samples. Whenever the skew exceeds the real gap between the cutover and the touch, the
  # guard fails to refuse an unforced revert and DISCARDS committed post-cutover tenant writes.
  #
  # It surfaced as an intermittent `Fathom.Migrator.ShardMigrationTest` failure — that test puts a
  # touch a few hundred µs after the cutover, which is close enough to the skew to flip. It is a
  # real correctness bug, not a test bug; the test was just the narrowest gap in the suite.
  #
  # Many samples deliberately: one sample discriminates ~99.9% of the time, so ALL of N must hold
  # for the assertion to be effectively deterministic rather than merely likely.
  #
  # UPDATED for expert review 2026-10-12 #8: `cutover_at` is now stamped from the POSTGRES clock
  # (`Directory.db_now/0`), and the flush rebases every buffered stamp onto it, so the bounds are
  # Postgres clock readings — taken before the touch and after the flush (the rebase can place a
  # stamp up to the offset query's latency after the touch, never before it). This previously
  # asserted `DateTime.utc_now/0` bounds, which pinned the two-node-clocks design #8 removed.
  test "record/1 stamps land on cutover_at's clock (Postgres), so they order against it" do
    samples =
      for _ <- 1..50 do
        id = uniq()
        before = Directory.db_now()
        :ok = Recorder.record(id)
        {id, before}
      end

    Recorder.flush()
    later = Directory.db_now()

    for {id, before} <- samples do
      {:ok, row} = Directory.get(id)

      assert DateTime.compare(row.last_active_at, before) != :lt,
             "last_active_at (#{inspect(row.last_active_at)}) landed BEFORE a database clock reading " <>
               "taken before the touch (#{inspect(before)}) — the recorder is on a different " <>
               "clock from cutover_at, so the revert write-age guard can discard real writes"

      assert DateTime.compare(row.last_active_at, later) != :gt,
             "last_active_at (#{inspect(row.last_active_at)}) landed AFTER a database clock reading " <>
               "taken after the flush (#{inspect(later)})"
    end
  end

  # Same invariant for the flush buffer: `last_flushed_at` is subtracted from `last_active_at` to
  # report each tenant's loss window, so a cross-clock stamp skews that window — in the direction
  # that UNDER-reports how much a node loss cost.
  test "record_flush/1 stamps land on the same (Postgres) clock too" do
    samples =
      for _ <- 1..50 do
        id = uniq()
        {:ok, _} = Directory.resolve(id)
        before = Directory.db_now()
        :ok = Recorder.record_flush(id)
        {id, before}
      end

    Recorder.flush()
    later = Directory.db_now()

    for {id, before} <- samples do
      {:ok, row} = Directory.get(id)

      assert DateTime.compare(row.last_flushed_at, before) != :lt,
             "last_flushed_at landed before a prior database clock reading — cross-clock stamp"

      assert DateTime.compare(row.last_flushed_at, later) != :gt,
             "last_flushed_at landed after a later database clock reading — cross-clock stamp"
    end
  end

  # Expert review 2026-10-19 #5: every checkout inserted the shard's key, so one hot shard's
  # streams convoyed on its lock (143 us/insert at 18 callers; same-shard checkout swung 124k-603k/s
  # with the Recorder on). A touch from the SAME coordinator inside the window now only reads. The
  # two edges `last_active_at` must stay exact at are pinned beside it: a new coordinator (every
  # touch after a cutover comes from one, because the migration's lease forces a stand-down) and a
  # buffered flush. A plain "skip if fresh" fails both edge tests.
  describe "2026-10-19 #5 — coalescing a hot shard's touches" do
    defp buffered(id) do
      [{^id, at}] = :ets.lookup(Recorder, id)
      at
    end

    defp coordinator, do: spawn(fn -> :ok end)

    test "a touch from the same coordinator inside the window does not rewrite the buffer" do
      a = uniq()
      p = coordinator()

      :ok = Recorder.record(a, p)
      first = buffered(a)
      for _ <- 1..100, do: :ok = Recorder.record(a, p)

      assert buffered(a) == first, "a same-coordinator touch inside the window rewrote the row"
      assert Recorder.flush() == 1
    end

    test "the first touch from a NEW coordinator always buffers its own stamp (the cutover edge)" do
      a = uniq()
      :ok = Recorder.record(a, coordinator())
      first = buffered(a)

      # Make the next stamp distinguishable without sleeping: back-date the buffered one by 1 ms,
      # still well inside the window.
      :ets.insert(Recorder, {a, first - 1_000})
      :ok = Recorder.record(a, coordinator())

      assert buffered(a) > first - 1_000,
             "a new incarnation's touch was coalesced into the previous one's stamp — after a " <>
               "cutover that is a dropped post-cutover touch, and the revert guard passes"
    end

    test "a touch while a flush is buffered always buffers its own stamp (the flush edge)" do
      a = uniq()
      p = coordinator()
      :ok = Recorder.record(a, p)
      :ets.insert(Recorder, {a, buffered(a) - 1_000})
      stale = buffered(a)

      :ok = Recorder.record_flush(a)
      :ok = Recorder.record(a, p)

      assert buffered(a) > stale,
             "a post-flush touch was coalesced into the pre-flush stamp — the loss report then " <>
               "calls a shard with post-flush checkouts clean"
    end

    test "a drained touch takes its coalescing owner with it, so nothing accumulates" do
      a = uniq()
      :ok = Recorder.record(a, coordinator())
      assert [{^a, _}] = :ets.lookup(Module.concat(Recorder, Owners), a)

      Recorder.flush()
      assert :ets.lookup(Module.concat(Recorder, Owners), a) == []
    end

    test "record/1 (no coordinator) never coalesces" do
      a = uniq()
      :ok = Recorder.record(a)
      :ets.insert(Recorder, {a, buffered(a) - 1_000})
      stale = buffered(a)
      :ok = Recorder.record(a)
      assert buffered(a) > stale
    end
  end

  describe "#38 — the shutdown flush is budgeted and says so when it gives up" do
    # This buffer holds a `last_flushed_at` row for every shard the node just flushed, and
    # terminate/2 is the only chance to persist them. Shutdown order is
    # Edge -> DataPlane -> ControlPlane -> Infra, so EVERY coordinator's terminate-flush has
    # already run by then; under `use GenServer`'s default 5s the drain could be cut off, and it
    # was SILENT. A truncated flush makes `flush_lag_report/1` and `mix fathom.shard loss-report`
    # show those shards as flushed LONGER AGO than they were — under-reading loss on exactly the
    # node that just went down.

    setup do
      prev = Application.get_env(:fathom, :directory_recorder_shutdown_budget_ms)

      on_exit(fn ->
        if prev,
          do: Application.put_env(:fathom, :directory_recorder_shutdown_budget_ms, prev),
          else: Application.delete_env(:fathom, :directory_recorder_shutdown_budget_ms)

        Recorder.flush()
      end)

      :ok
    end

    test "the supervised child has an explicit 30s shutdown, not the 5s default" do
      # Asserted against the LIVE tree, not a function's return value: the default only bites if
      # the running spec actually carries it.
      assert {:ok, spec} =
               :supervisor.get_childspec(
                 Fathom.ControlPlane.Supervisor,
                 Fathom.Directory.Recorder
               )

      assert spec.shutdown >= 30_000,
             "the Recorder still stops on a budget shorter than the data plane's flush burst"

      refute spec.shutdown == 5_000
    end

    test "an exhausted budget gives up loudly and leaves the rows buffered" do
      import ExUnit.CaptureLog

      Application.put_env(:fathom, :directory_recorder_shutdown_budget_ms, 0)
      assert :ok = Recorder.record_flush(uniq())
      assert :ok = Recorder.record_flush(uniq())

      log = capture_log(fn -> assert :ok = Recorder.terminate(:shutdown, nil) end)

      assert log =~ "shutdown flush gave up",
             "a truncated shutdown flush must not be silent — it corrupts loss-report in the " <>
               "misleading direction"

      assert log =~ "buffered row(s) undrained"
    end

    test "a sufficient budget drains everything and logs nothing" do
      import ExUnit.CaptureLog

      Application.put_env(:fathom, :directory_recorder_shutdown_budget_ms, 25_000)
      id = uniq()
      assert :ok = Recorder.record_flush(id)

      log = capture_log(fn -> assert :ok = Recorder.terminate(:shutdown, nil) end)

      refute log =~ "gave up", "the happy path must stay quiet"
    end
  end

  # Expert review 2026-10-17 #21: a shard flushed AND touched in one Recorder tick used to cost two
  # statements on `shards` (the flush insert, then the touch UPDATE), each a non-HOT row version.
  # They are one statement now. The stamps are chosen relative to the Postgres clock so the
  # assertions are about ordering, not about wall time.
  describe "#21 — a flush and a touch of one shard in one tick are one statement" do
    setup do
      test_pid = self()
      handler = "recorder-stmts-#{System.unique_integer([:positive])}"

      :ok =
        :telemetry.attach(
          handler,
          [:fathom, :repo, :query],
          fn _event, _measurements, meta, _config ->
            if meta[:source] == "shards" and
                 String.starts_with?(meta[:query] || "", ["UPDATE", "INSERT"]),
               do: send(test_pid, {:shard_write, meta[:query]})
          end,
          nil
        )

      on_exit(fn -> :telemetry.detach(handler) end)
      :ok
    end

    # The shard writes sent since the last call.
    defp shard_writes(acc \\ []) do
      receive do
        {:shard_write, sql} -> shard_writes([sql | acc])
      after
        0 -> acc
      end
    end

    # The stored state and the buffered stamps are set in terms of `db`, the Postgres clock.
    defp dirty_shard(id, db, last_active_s_ago, last_flushed_s_ago) do
      {:ok, _} = Directory.resolve(id)

      {1, _} =
        Fathom.Repo.update_all(
          Ecto.Query.from(s in Fathom.Directory.Shard, where: s.shard_id == ^id),
          set: [
            last_active_at: DateTime.add(db, -last_active_s_ago, :second),
            last_flushed_at: DateTime.add(db, -last_flushed_s_ago, :second)
          ]
        )
    end

    defp buffer_flush(id, node_us),
      do: :ets.insert(Module.concat(Recorder, Flushes), {id, node_us})

    defp buffer_touch(id, node_us), do: :ets.insert(Recorder, {id, node_us})

    test "one statement writes both stamps, and each lands at its own instant" do
      a = uniq()
      db = Directory.db_now()
      dirty_shard(a, db, 10, 20)
      Recorder.flush()
      _ = shard_writes()

      # Flushed 5 s ago, touched 2 s ago: both inside one tick, the touch after the flush.
      node = System.os_time(:microsecond)
      buffer_flush(a, node - 5_000_000)
      buffer_touch(a, node - 2_000_000)
      Recorder.flush()

      assert [_one] = shard_writes(),
             "a shard flushed and touched in one tick must be written by one statement"

      {:ok, shard} = Directory.get(a)
      assert_in_delta unix_s(shard.last_flushed_at), unix_s(db) - 5, 1
      assert_in_delta unix_s(shard.last_active_at), unix_s(db) - 2, 1
    end

    test "a touch the guard rejects after the flush leaves last_active_at alone" do
      a = uniq()
      db = Directory.db_now()

      # Last active 3 s ago, flushed 20 s ago. The flush at 10 s ago is then the watermark, and the
      # touch 2 s ago is inside the 30 s window of last_active_at, so the guard rejects it.
      dirty_shard(a, db, 3, 20)
      Recorder.flush()
      _ = shard_writes()

      node = System.os_time(:microsecond)
      buffer_flush(a, node - 10_000_000)
      buffer_touch(a, node - 2_000_000)
      Recorder.flush()

      assert [_one] = shard_writes()

      {:ok, shard} = Directory.get(a)
      assert_in_delta unix_s(shard.last_flushed_at), unix_s(db) - 10, 1
      assert_in_delta unix_s(shard.last_active_at), unix_s(db) - 3, 1
    end

    test "a flush-only and a touch-only shard in the same tick are written as before" do
      flush_only = uniq()
      touch_only = uniq()
      db = Directory.db_now()
      dirty_shard(flush_only, db, 10, 20)
      # Last active 40 s ago, so a touch 1 s ago clears the 30 s window (no flush to pair with).
      dirty_shard(touch_only, db, 40, 50)
      Recorder.flush()
      _ = shard_writes()

      node = System.os_time(:microsecond)
      buffer_flush(flush_only, node - 5_000_000)
      buffer_touch(touch_only, node - 1_000_000)
      Recorder.flush()

      {:ok, f} = Directory.get(flush_only)
      assert_in_delta unix_s(f.last_flushed_at), unix_s(db) - 5, 1

      assert_in_delta unix_s(f.last_active_at),
                      unix_s(db) - 10,
                      1,
                      "a flush must not move last_active_at"

      {:ok, t} = Directory.get(touch_only)
      assert_in_delta unix_s(t.last_active_at), unix_s(db) - 1, 1

      assert_in_delta unix_s(t.last_flushed_at),
                      unix_s(db) - 50,
                      1,
                      "a touch must not move last_flushed_at"
    end

    defp unix_s(%DateTime{} = dt), do: DateTime.to_unix(dt, :microsecond) / 1_000_000
  end
end
