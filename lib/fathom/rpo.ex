defmodule Fathom.Rpo do
  @moduledoc """
  Loss-window (RPO) measurement harness — quantifies how much a shard would lose
  on **node/disk loss**, as a function of the flush-interval knob
  (`:shard_flush_interval_ms`) and the write rate, and confirms the
  **process-crash** case loses nothing (`synchronous=FULL` local durability).

  Two deaths, two numbers (see `docs/durability.md`):

    * **Process / OS crash, disk intact** → the WAL is fsynced per commit
      (`synchronous=FULL`), so a same-node restart re-adopts the present local
      file and loses nothing. Measured by `process_kill/1`: write N rows with the
      flush disabled, hard-kill the coordinator, re-open on the same disk, count
      survivors.
    * **Node / disk loss** → a survivor cold-opens the **last flushed object**, so
      it loses every write committed since that flush = the RPO window. Measured
      by `measure/1`: drive a steady write stream and, at points spread across the
      flush sawtooth, compare the acked max-seq against what the stored object
      holds.

  The stored object *is* the survivor's view — a failover cold-opens exactly it —
  so the node-loss magnitude is measured directly, without killing a real node.
  The multi-node / real-disk complement is `deploy/chaos/chaos.sh soak`.

  Not started in the app: the harness stands up the minimal subsystem it needs
  (ShardRegistry + ShardSupervisor + WriteCounter + Heartbeat; no Repo/Oban/
  endpoint), mirroring `Fathom.Scale`. Run prod-compiled via `mix fathom.rpo`.
  """

  alias Fathom.{ShardExecutor, Shards}
  alias Fathom.Shard.{Connection, Storage}
  alias Filo.Stmt

  @default_rate 100
  @default_samples 30
  @default_intervals [0, 5_000, 30_000]

  @default_cost_intervals [5_000, 30_000]
  @default_cost_window_ms 20_000

  @doc """
  Measure the node-loss window across a sweep of flush intervals, then the
  process-crash case. Options:

    * `:rate` — writes/sec to drive (default `#{@default_rate}`)
    * `:samples` — loss samples per interval, spread across the flush cycles
      (default `#{@default_samples}`)
    * `:intervals` — `:shard_flush_interval_ms` values to sweep
      (default `#{inspect(@default_intervals)}`; `0` = idle-only, an unbounded
      window for a never-idle shard)
  """
  @spec measure(keyword()) :: map()
  def measure(opts \\ []) do
    rate = Keyword.get(opts, :rate, @default_rate)
    samples = Keyword.get(opts, :samples, @default_samples)
    intervals = Keyword.get(opts, :intervals, @default_intervals)

    setup()

    interval_rows = Enum.map(intervals, &sweep_interval(&1, rate, samples))
    pk = process_kill()

    %{
      rate_per_s: rate,
      samples_per_interval: samples,
      synchronous: "FULL",
      intervals: interval_rows,
      process_kill: pk
    }
  end

  @doc """
  Measure the COST side of the flush-interval knob (expert review 2026-07-18 #20 — the complement to
  `measure/1`'s RPO benefit). At each interval, drive a continuously-write-active shard for a window
  so it's ALWAYS dirty (a flush every interval), and count the flushes (each a VACUUM INTO snapshot +
  full-object PUT) and their per-flush duration. A tighter interval flushes proportionally more often
  (≈ `window/interval`), each paying the snapshot + upload cost — so 5s vs 30s is ~6× the VACUUM/PUT
  rate for a tighter RPO.

  Storage backend: **Local by default**, which measures the VACUUM + local-copy cost only. Setting
  `FATHOM_S3_TEST_ENDPOINT`, alongside the other `FATHOM_S3_TEST_` variables, switches the run to the
  real S3 backend so the PUT and its Finch-pool contention with cold-opens are priced. `storage_kind/0`
  reports which one a run will use — until expert review 2026-08-26 #38 this docstring promised the
  S3 behaviour while `setup/0` unconditionally forced Local, so an operator who set the variables
  measured a local file copy and had no way to tell.

  Options: `:rate` (writes/sec, default `#{@default_rate}`), `:window_ms`
  (write window per interval, default `#{@default_cost_window_ms}`), `:intervals`
  (default `#{inspect(@default_cost_intervals)}`).
  """
  @spec flush_cost(keyword()) :: map()
  def flush_cost(opts \\ []) do
    rate = Keyword.get(opts, :rate, @default_rate)
    window_ms = Keyword.get(opts, :window_ms, @default_cost_window_ms)
    intervals = Keyword.get(opts, :intervals, @default_cost_intervals)

    setup()
    rows = Enum.map(intervals, &cost_interval(&1, rate, window_ms))

    %{rate_per_s: rate, window_ms: window_ms, intervals: rows}
  end

  defp cost_interval(interval_ms, rate, window_ms) do
    Application.put_env(:fathom, :shard_flush_interval_ms, interval_ms)

    test_pid = self()
    handler = "rpo-flush-cost-#{System.unique_integer([:positive])}"

    :telemetry.attach(
      handler,
      [:fathom, :shard, :flush],
      fn _e, %{duration: d}, %{outcome: o}, _cfg -> send(test_pid, {:flush_cost, o, d}) end,
      nil
    )

    shard = "rpo_cost_#{System.unique_integer([:positive])}"
    {:ok, h} = ShardExecutor.open(shard)

    {:ok, _} =
      ShardExecutor.execute(
        h,
        %Stmt{sql: "CREATE TABLE t (seq INTEGER PRIMARY KEY, ts_ms INTEGER)"}
      )

    # Continuous writes for the window keep the shard dirty, so the periodic flush fires every
    # interval — the sustained-write-load case where the cost is highest.
    drive_writes_until(h, rate, now_ms() + window_ms)

    # Let the last in-flight flush land, then drain and collect the telemetry.
    sleep_ms(min(interval_ms, 2_000))
    drain(shard, h)
    :telemetry.detach(handler)

    durations_us = collect_flushes([])

    %{
      interval_ms: interval_ms,
      flushes: length(durations_us),
      flushes_per_s: Float.round(length(durations_us) * 1_000 / window_ms, 2),
      flush_us: dist(durations_us)
    }
  end

  defp drive_writes_until(h, rate, t_end) do
    gap = 1_000 / rate

    Enum.reduce_while(Stream.iterate(1, &(&1 + 1)), :ok, fn s, _ ->
      if now_ms() >= t_end do
        {:halt, :ok}
      else
        {:ok, _} =
          ShardExecutor.execute(
            h,
            %Stmt{sql: "INSERT INTO t (seq, ts_ms) VALUES (?, ?)", args: [s, now_ms()]}
          )

        sleep_ms(gap)
        {:cont, :ok}
      end
    end)
  end

  # Drain the flush telemetry mailbox, keeping only the outcomes that actually did a VACUUM+PUT.
  defp collect_flushes(acc) do
    receive do
      {:flush_cost, outcome, duration} when outcome in [:uploaded, :reconciled] ->
        collect_flushes([System.convert_time_unit(duration, :native, :microsecond) | acc])

      {:flush_cost, _outcome, _duration} ->
        collect_flushes(acc)
    after
      0 -> acc
    end
  end

  # --- node-loss sweep -----------------------------------------------------

  defp sweep_interval(interval_ms, rate, samples) do
    # The coordinator reads the interval fresh on each timer re-arm, but set it
    # before the open so the shard's very first schedule picks it up.
    Application.put_env(:fathom, :shard_flush_interval_ms, interval_ms)

    shard = "rpo_#{System.unique_integer([:positive])}"
    {:ok, h} = ShardExecutor.open(shard)

    {:ok, _} =
      ShardExecutor.execute(
        h,
        %Stmt{sql: "CREATE TABLE t (seq INTEGER PRIMARY KEY, ts_ms INTEGER)"}
      )

    t0 = now_ms()

    # ~5 samples per flush cycle so we catch the sawtooth from trough (just
    # flushed, loss ~0) to peak (just before the next flush). An idle-only shard
    # has no cycle, so sample on a fixed 1s cadence — the window just grows.
    window_ms = if interval_ms > 0, do: max(div(interval_ms, 5), 100), else: 1_000

    {seq, samples_list} =
      Enum.reduce(1..samples, {0, []}, fn _, {seq, acc} ->
        {seq, acked_ts} = write_window(h, seq, rate, window_ms)
        sample = loss_sample(shard, seq, acked_ts, t0)
        {seq, [sample | acc]}
      end)

    samples_list = Enum.reverse(samples_list)
    drain(shard, h)

    lost_rows = Enum.map(samples_list, & &1.lost_rows)
    lost_ms = Enum.map(samples_list, & &1.lost_ms)
    flush_points = samples_list |> Enum.map(& &1.flushed_seq) |> Enum.uniq() |> length()

    %{
      interval_ms: interval_ms,
      writes: seq,
      distinct_flush_points: flush_points,
      lost_rows: dist(lost_rows),
      lost_ms: dist(lost_ms)
    }
  end

  # Write ~`rate * window/1s` rows spaced to fill the window at the target rate.
  # Returns {new_seq, ts_of_last_acked_write}.
  defp write_window(h, seq, rate, window_ms) do
    n = max(round(rate * window_ms / 1_000), 1)
    gap = window_ms / n

    Enum.reduce(1..n, {seq, now_ms()}, fn _, {s, _last} ->
      s = s + 1
      ts = now_ms()

      {:ok, _} =
        ShardExecutor.execute(
          h,
          %Stmt{sql: "INSERT INTO t (seq, ts_ms) VALUES (?, ?)", args: [s, ts]}
        )

      sleep_ms(gap)
      {s, ts}
    end)
  end

  # One "if the node died right now" reading: acked (on local disk, durable) vs
  # what a survivor would cold-open (the stored object).
  defp loss_sample(shard, acked_seq, acked_ts, t0) do
    {flushed_seq, flushed_ts} = flushed_state(shard, t0)

    %{
      acked_seq: acked_seq,
      flushed_seq: flushed_seq,
      lost_rows: acked_seq - flushed_seq,
      lost_ms: max(acked_ts - flushed_ts, 0)
    }
  end

  # The survivor's view: pull the stored object and read its newest row. An
  # absent/unreadable object (never flushed, or a torn mid-PUT read) reads as
  # "nothing durable since open" — flushed_seq 0, window measured from t0.
  defp flushed_state(shard, t0) do
    dst = Path.join(scratch(), "peek_#{System.unique_integer([:positive])}.db")

    result =
      case Storage.pull(shard, dst) do
        {:ok, _etag} ->
          peek(dst) || {0, t0}

        # No bytes written (no object / steal sentinel) — nothing durable yet, which is a real
        # measurement, not a failure (#24).
        {:absent, _} ->
          {0, t0}

        # A TRANSPORT error is NOT "nothing was durable" (expert review 2026-08-26 #38). This used
        # to fall into the same `_` branch as `:absent` and report flushed_seq 0 — i.e. MAXIMAL
        # loss — so a flaky store manufactured a catastrophic-looking RPO curve, and the number
        # that sets `:shard_flush_interval_ms` would have been derived from it. Fail the run loudly
        # instead: AGENTS.md requires a bench metric to assert its own preconditions rather than
        # report a spectacular result for work that never happened.
        {:error, reason} ->
          raise "rpo: could not pull #{shard} to measure the durable position (#{inspect(reason)}). " <>
                  "Refusing to report this as flushed_seq 0 — that would read as total loss and " <>
                  "silently bias the RPO curve."
      end

    for s <- ["", "-wal", "-shm"], do: File.rm(dst <> s)
    result
  end

  # HONOUR `FATHOM_S3_TEST_*` (expert review 2026-08-26 #38).
  #
  # `setup/0` used to force `Storage.Local` unconditionally, while BOTH docstrings — this module's
  # and `mix fathom.rpo`'s — told the operator to "point FATHOM_S3_TEST_* at real S3 to price the
  # PUT (and its Finch-pool contention with cold-opens)". Neither file ever read those variables:
  # the promise was documentation only, and the forced `put_env` came AFTER whatever the operator
  # had configured, so it overrode it.
  #
  # That matters more than a wrong docstring usually would. This harness produces the per-flush
  # cost table in `docs/durability.md` that prices `:shard_flush_interval_ms` — the RPO knob, and
  # per AGENTS.md's loud warning also the biggest throughput dial on a replicating fleet. It was
  # timing a local `File.cp`, not an S3 PUT, and not the Finch-pool contention that is the whole
  # reason the interval is contentious.
  #
  # Default stays Local, so the harness remains S3-free and offline-runnable unless an endpoint is
  # explicitly set — the same opt-in rule `Fathom.Bench.s3_opt_in?/0` uses, and the config is read
  # the same way so the two harnesses cannot drift apart.
  defp configure_storage(store) do
    if s3_opt_in?() do
      {:ok, _} = Application.ensure_all_started(:req)
      Application.put_env(:fathom, :shard_storage, Fathom.Shard.Storage.S3)
      Application.put_env(:fathom, Fathom.Shard.Storage.S3, s3_config_from_env())

      {Finch, finch_opts} = Fathom.Shard.Storage.S3.finch_child_spec()
      ensure_started(Finch, finch_opts)
    else
      Application.put_env(:fathom, :shard_storage, Fathom.Shard.Storage.Local)
      Application.put_env(:fathom, Fathom.Shard.Storage.Local, dir: store)
    end
  end

  defp s3_opt_in?, do: System.get_env("FATHOM_S3_TEST_ENDPOINT") != nil

  defp s3_config_from_env do
    [
      bucket: System.get_env("FATHOM_S3_TEST_BUCKET", "fathom-shards-test"),
      region: System.get_env("FATHOM_S3_TEST_REGION", "us-east-1"),
      endpoint: System.get_env("FATHOM_S3_TEST_ENDPOINT", "http://localhost:9100"),
      path_style: true,
      prefix: "rpo/",
      access_key_id: System.get_env("FATHOM_S3_TEST_ACCESS_KEY", "fathomtest"),
      secret_access_key: System.get_env("FATHOM_S3_TEST_SECRET_KEY", "fathomtest123")
    ]
  end

  @doc """
  Which storage backend a run will use, for the harness to print. Reported rather than assumed:
  an operator who exported the S3 variables and silently got Local is exactly the failure #38 was.
  """
  @spec storage_kind() :: :s3 | :local
  def storage_kind, do: if(s3_opt_in?(), do: :s3, else: :local)

  defp peek(path) do
    {:ok, conn} = Connection.open(path)

    row =
      case Connection.query(conn, "SELECT seq, ts_ms FROM t ORDER BY seq DESC LIMIT 1", []) do
        {:ok, %{rows: [[seq, ts]]}} when is_integer(seq) and is_integer(ts) -> {seq, ts}
        _ -> nil
      end

    Connection.close(conn)
    row
  rescue
    _ -> nil
  catch
    :exit, _ -> nil
  end

  # --- process-crash case (disk intact ⇒ zero loss) ------------------------

  @doc """
  The process-crash case: with the periodic + idle flush disabled (nothing
  reaches storage), write N rows, hard-kill the coordinator, re-open on the same
  local file, and count survivors. `synchronous=FULL` ⇒ zero loss. The stored
  object stays empty, so survival can only come from the local WAL.
  """
  @spec process_kill(pos_integer()) :: map()
  def process_kill(n \\ 200) do
    prev_flush = Application.get_env(:fathom, :shard_flush_interval_ms)
    Application.put_env(:fathom, :shard_flush_interval_ms, 0)

    shard = "rpo_pk_#{System.unique_integer([:positive])}"
    {:ok, h} = ShardExecutor.open(shard)
    {:ok, _} = ShardExecutor.execute(h, %Stmt{sql: "CREATE TABLE t (seq INTEGER PRIMARY KEY)"})

    Enum.each(1..n, fn s ->
      {:ok, _} = ShardExecutor.execute(h, %Stmt{sql: "INSERT INTO t (seq) VALUES (?)", args: [s]})
    end)

    stored_before? = File.exists?(Path.join(remote_dir(), "#{shard}.db"))

    {:ok, coordinator} = Shards.ensure(shard)
    ref = Process.monitor(coordinator)
    # A hard kill skips terminate/2 — no final flush, like a lost node.
    Process.exit(coordinator, :kill)

    receive do
      {:DOWN, ^ref, :process, ^coordinator, _} -> :ok
    after
      5_000 -> :ok
    end

    # Let the supervisor reap the dead child so its registry key clears before
    # the re-open starts a fresh coordinator.
    _ = :sys.get_state(Fathom.ShardSupervisor)

    # Re-open: the present local file is authoritative on wake → adopt it (no
    # pull), and SQLite recovers the fsynced WAL.
    {:ok, h2} = ShardExecutor.open(shard)

    survived =
      case ShardExecutor.execute(h2, %Stmt{sql: "SELECT COALESCE(MAX(seq), 0) FROM t"}) do
        {:ok, %{rows: [[m]]}} -> m
        _ -> 0
      end

    drain(shard, h2)
    put_or_delete(:shard_flush_interval_ms, prev_flush)

    %{
      written: n,
      survived: survived,
      lost_rows: n - survived,
      stored_before_kill?: stored_before?
    }
  end

  # --- distribution --------------------------------------------------------

  defp dist([]), do: %{p50: 0, p90: 0, p99: 0, max: 0, mean: 0}

  defp dist(xs) do
    sorted = Enum.sort(xs)

    %{
      p50: pct(sorted, 50),
      p90: pct(sorted, 90),
      p99: pct(sorted, 99),
      max: List.last(sorted),
      mean: round(Enum.sum(xs) / length(xs))
    }
  end

  defp pct(sorted, p) do
    n = length(sorted)
    Enum.at(sorted, min(n - 1, round(p / 100 * (n - 1))))
  end

  # --- setup / teardown ----------------------------------------------------

  defp setup do
    data = Path.join(scratch(), "live")
    store = remote_dir()
    File.rm_rf!(data)
    File.rm_rf!(store)
    File.mkdir_p!(data)
    File.mkdir_p!(store)

    {:ok, _} = Application.ensure_all_started(:telemetry)

    Application.put_env(:fathom, :shard_data_dir, data)
    configure_storage(store)
    Application.put_env(:fathom, :directory_touch, false)
    Application.put_env(:fathom, :lazy_migrate, false)
    # Hold the connection open the whole run, so a shard never idle-drops mid-
    # measurement; only the periodic flush should reach storage.
    Application.put_env(:fathom, :shard_idle_ms, 600_000)

    ensure_started(Registry, keys: :unique, name: Fathom.ShardRegistry)
    ensure_started(DynamicSupervisor, name: Fathom.ShardSupervisor, strategy: :one_for_one)
    ensure_started(Fathom.Shard.WriteCounter, [])
    start_heartbeat(30_000)
    :ok
  end

  defp start_heartbeat(ttl_ms) do
    case Fathom.Shard.Heartbeat.start_link(ttl_ms: ttl_ms) do
      {:ok, _} -> :ok
      {:error, {:already_started, _}} -> :ok
    end
  end

  defp ensure_started(mod, opts) do
    case mod.start_link(opts) do
      {:ok, _pid} -> :ok
      {:error, {:already_started, _pid}} -> :ok
    end
  end

  @doc "Drains any live coordinators and removes the harness scratch dir."
  @spec cleanup() :: :ok
  def cleanup do
    Fathom.ShardSupervisor
    |> DynamicSupervisor.which_children()
    |> Enum.each(fn {_, pid, _, _} ->
      if is_pid(pid), do: DynamicSupervisor.terminate_child(Fathom.ShardSupervisor, pid)
    end)

    File.rm_rf!(scratch())
    :ok
  rescue
    _ -> :ok
  end

  # --- helpers -------------------------------------------------------------

  defp drain(shard, h) do
    _ = ShardExecutor.close(h)
    _ = Shards.drain(shard, 10_000)
    :ok
  end

  defp scratch, do: Path.join(System.tmp_dir!(), "fathom_rpo")
  defp remote_dir, do: Path.join(scratch(), "remote")
  defp now_ms, do: System.system_time(:millisecond)

  defp sleep_ms(ms) when ms >= 1, do: Process.sleep(round(ms))
  defp sleep_ms(_), do: :ok

  defp put_or_delete(key, nil), do: Application.delete_env(:fathom, key)
  defp put_or_delete(key, value), do: Application.put_env(:fathom, key, value)
end
