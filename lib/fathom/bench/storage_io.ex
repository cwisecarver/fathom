defmodule Fathom.Bench.StorageIO do
  @moduledoc """
  Storage-IO benches (expert review 2026-10-19 #3, #7): the flush codec, the S3 pull decode and the
  S3 client itself, none of which any gated metric reached. `flush_p50_us` is `Storage.Local` on a
  small shard, so it never runs the codec, the MD5 passes or a streamed PUT; `cold_open_s3_p50_us`
  is null in all but a few history lines because it needs a MinIO.

  Every metric here owns its setup and needs no environment: the fixture is built in a scratch
  directory and, for the S3 metrics, served by a loopback Bandit stub. Nothing here touches the
  shared `Fathom.Bench.setup/1`, so the series the other metrics measure is unchanged.

  ## The fixture

  A ~20 MB SQLite file with a REALISTIC compression ratio (~2.3:1: a low-entropy text column, a
  monotone timestamp, a few-valued tag, a secondary index). A random-bytes fixture compresses
  ~1:1 and measures a different codec regime (zstd's incompressible fast path), so each metric
  asserts its own preconditions (plain size, ratio) and raises rather than report a number from the
  wrong regime.
  """

  alias Exqlite.Sqlite3
  alias Fathom.Bench.S3Stub
  alias Fathom.Shard.Connection
  alias Fathom.Shard.Storage
  alias Fathom.Shard.Storage.Codec
  alias Fathom.Shard.Storage.S3
  alias Fathom.Shards

  @fixture_rows 160_000
  @min_plain_bytes 10_000_000
  @min_ratio 2.0
  @codec_trials 3
  @pull_trials 5
  @s3_head_samples 300
  @s3_put_samples 100
  @cold_stub_samples 50
  @cold_cycle_workers 32
  @cold_cycle_per_worker 4
  @cold_cycle_trials 3
  @s3_bucket "benchbucket"
  @s3_prefix "s3io/"

  @words ~w(payment invoice transfer deposit withdrawal refund merchant grocery fuel utilities
            rent salary subscription coffee airline hotel taxi pharmacy insurance interest fee
            adjustment)

  @doc """
  `flush_codec_ms_per_mb` — median ms per plaintext MB of `Codec.compress_to_temp_hashed/2` (zstd)
  over the fixture: the compress, the plaintext and body MD5s, and the inline decode proof. This is
  the largest per-flush CPU component on a real shard (~75% of a flush), the one `flush_p50_us`
  never runs.
  """
  @spec flush_codec_ms_per_mb(keyword()) :: float()
  def flush_codec_ms_per_mb(opts \\ []) do
    trials = Keyword.get(opts, :codec_trials, @codec_trials)

    with_fixture(opts, fn path, plain ->
      # One untimed pass: loads the NIF and warms the page cache, so trial 1 is not an outlier.
      run_codec(path, plain)

      1..trials
      |> Enum.map(fn _ -> run_codec(path, plain) end)
      |> median()
    end)
  end

  defp run_codec(path, plain) do
    {us, {:ok, tmp, size, _md5, _hex}} =
      :timer.tc(fn -> Codec.compress_to_temp_hashed(path, :zstd) end)

    File.rm(tmp)
    check_ratio!("flush_codec_ms_per_mb", plain, size)
    us / 1_000 / (plain / 1_000_000)
  end

  @doc false
  # Builds the fixture, runs `fun.(path, plain_bytes)`, and removes the scratch directory.
  @spec with_fixture(keyword(), (Path.t(), pos_integer() -> result)) :: result when result: term()
  def with_fixture(opts, fun) do
    dir = Path.join(tmp_dir(), "storage_io_#{System.unique_integer([:positive])}")
    File.mkdir_p!(dir)
    path = Path.join(dir, "fixture.db")

    try do
      plain = build_fixture(path, Keyword.get(opts, :fixture_rows, @fixture_rows))

      if plain < @min_plain_bytes,
        do: raise("storage_io fixture is #{plain} bytes; it must exceed #{@min_plain_bytes}")

      fun.(path, plain)
    after
      File.rm_rf(dir)
    end
  end

  @doc false
  @spec check_ratio!(String.t(), pos_integer(), pos_integer()) :: :ok
  def check_ratio!(metric, plain, compressed) do
    if plain / compressed < @min_ratio,
      do:
        raise(
          "#{metric}: ratio #{Float.round(plain / compressed, 2)} < #{@min_ratio}; " <>
            "the fixture is not compressible and this would measure the wrong codec regime"
        ),
      else: :ok
  end

  defp build_fixture(path, rows) do
    {:ok, db} = Sqlite3.open(path)

    try do
      :ok = Sqlite3.execute(db, "PRAGMA journal_mode=OFF")

      :ok =
        Sqlite3.execute(
          db,
          "CREATE TABLE t(id INTEGER PRIMARY KEY, acct INTEGER, ts INTEGER, memo TEXT, " <>
            "amt REAL, tag TEXT)"
        )

      :ok = Sqlite3.execute(db, "CREATE INDEX t_acct ON t(acct, ts)")

      {:ok, st} =
        Sqlite3.prepare(db, "INSERT INTO t(acct,ts,memo,amt,tag) VALUES(?1,?2,?3,?4,?5)")

      words = List.to_tuple(@words)
      nwords = tuple_size(words)
      # Seeded in a process-local state so every run builds the same bytes.
      state = :rand.seed_s(:exsss, {1, 2, 3})

      :ok = Sqlite3.execute(db, "BEGIN")

      Enum.reduce(1..rows, state, fn i, s ->
        {memo_words, s} =
          Enum.map_reduce(1..8, s, fn _, s ->
            {n, s} = :rand.uniform_s(nwords, s)
            {elem(words, n - 1), s}
          end)

        {ref, s} = :rand.uniform_s(10_000_000, s)
        {acct, s} = :rand.uniform_s(5_000, s)
        {amt, s} = :rand.uniform_s(s)
        {tag, s} = :rand.uniform_s(nwords, s)
        memo = Enum.join(memo_words, " ") <> " ref " <> Integer.to_string(ref)

        :ok =
          Sqlite3.bind(st, [acct, 1_700_000_000 + i * 37, memo, amt * 1000, elem(words, tag - 1)])

        :done = Sqlite3.step(db, st)
        s
      end)

      :ok = Sqlite3.execute(db, "COMMIT")
    after
      Sqlite3.close(db)
    end

    File.stat!(path).size
  end

  @doc """
  `pull_decode_ms_per_mb` — median ms per plaintext MB of `Storage.S3.pull/2` against a loopback
  stub serving the compressed fixture: the Finch GET, the streamed zstd decode, the plaintext MD5
  and its verify, the buffered write, and the fsync + rename into place. It is a whole-pull number
  (the loopback transfer of the compressed body is in it, ~1 ms/MB), the quantity a cold open or a
  failover pays for a large shard. Preconditions: plain > 10 MB, ratio > 2, and the pulled file is
  byte-identical to the fixture (so the decode actually ran to completion).
  """
  @spec pull_decode_ms_per_mb(keyword()) :: float()
  def pull_decode_ms_per_mb(opts \\ []) do
    trials = Keyword.get(opts, :pull_trials, @pull_trials)

    with_fixture(opts, fn path, plain ->
      {:ok, tmp, size, body_md5, plain_hex} = Codec.compress_to_temp_hashed(path, :zstd)
      check_ratio!("pull_decode_ms_per_mb", plain, size)
      body = File.read!(tmp)
      File.rm(tmp)
      expect = :crypto.hash(:md5, File.read!(path))

      stub = start_s3()

      try do
        shard = "pulldecode"

        S3Stub.put_object(stub.table, object_path(shard), body, [
          {"etag", ~s("#{Base.encode16(body_md5, case: :lower)}")},
          {"x-amz-meta-fathom-md5", plain_hex},
          {Codec.meta_header(), "zstd"}
        ])

        out = path <> ".pulled"

        pull = fn ->
          {us, result} = :timer.tc(fn -> S3.pull(shard, out) end)
          {:ok, _etag} = result

          if :crypto.hash(:md5, File.read!(out)) != expect,
            do: raise("pull_decode_ms_per_mb: the pulled file differs from the fixture")

          File.rm!(out)
          us / 1_000 / (plain / 1_000_000)
        end

        pull.()
        samples = Enum.map(1..trials, fn _ -> pull.() end)
        assert_finch_used!(stub, trials + 1)
        median(samples)
      after
        stop_s3(stub)
      end
    end)
  end

  @doc """
  `s3_loopback_op_us` / `s3_loopback_put_us` — median µs of `Storage.S3.object_head/1` and of a
  conditional (If-Match) `Storage.S3.flush/6` of a tiny shard, against the loopback stub through
  the REAL dedicated S3 Finch pool, started the way `Fathom.Bench.setup_s3/1` starts it. Needs no
  MinIO and no environment.

  The Finch path is asserted, not assumed: a `req_plug` double bypasses `Req.Finch` and would hide
  the per-request Req deprecation `IO.warn` (expert review 2026-10-19 #3, ~118 µs a request) that
  this metric exists to expose. A telemetry counter on `[:finch, :request, :stop]` for the S3 pool
  must account for every timed request, or the metric raises.
  """
  @spec s3_loopback(keyword()) :: %{op_us: float(), put_us: float()}
  def s3_loopback(opts \\ []) do
    heads = Keyword.get(opts, :s3_head_samples, @s3_head_samples)
    puts = Keyword.get(opts, :s3_put_samples, @s3_put_samples)
    dir = Path.join(tmp_dir(), "s3loop_#{System.unique_integer([:positive])}")
    File.mkdir_p!(dir)
    small = Path.join(dir, "small.db")
    File.write!(small, String.duplicate("fathom s3 loopback payload ", 600))

    stub = start_s3()

    try do
      shard = "s3loop"

      S3Stub.put_object(stub.table, object_path(shard), "x", [
        {"etag", ~s("seed")},
        {"x-amz-meta-fathom-md5", "00"}
      ])

      head_us =
        timed(heads, fn ->
          {:ok, %{etag: ~s("seed")}} = S3.object_head(shard)
        end)

      Process.put(:s3io_etag, ~s("seed"))

      put_us =
        timed(puts, fn ->
          {:ok, new, _carried} =
            S3.flush(shard, small, Process.get(:s3io_etag), nil, {:carried, :none})

          Process.put(:s3io_etag, new)
        end)

      # `timed/2` runs its function once untimed first, hence the + 2.
      assert_finch_used!(stub, heads + puts + 2)
      %{op_us: median(head_us), put_us: median(put_us)}
    after
      stop_s3(stub)
      File.rm_rf(dir)
    end
  end

  @doc """
  `cold_open_stub_p50_us` - median µs of the same cold open `cold_open_p50_us` times
  (`Shards.checkout/1` + connection open + first query), but with the REAL `Storage.S3` backend, the
  real S3 Finch pool and a loopback `S3Stub` instead of the `Storage.Local` double (expert review
  2026-10-21 #2). The Local double is ~2/3 of the Local cold-open checkout and is partly double-only
  (JSON lock files under `:global.trans`, `File.cp` metadata calls), so a coordinator-side
  regression has to exceed ~60% of coordinator cost to trip `cold_open_p50_us`. This arm runs the
  lease PUT and the pull over a real socket (the lease-release DELETE runs in the untimed
  teardown). Loopback, so the S3 PROTOCOL cost, not a network RTT. WATCH-ONLY (new series; do not
  compare with `cold_open_p50_us`).

  Preconditions (raises otherwise): every sample left a pulled, non-empty local file, the S3 Finch
  pool carried at least the pull's requests, and after the run every lease was released by a
  DELETE (no `.lock` object remains).
  """
  @spec cold_open_stub(keyword()) :: float()
  def cold_open_stub(opts \\ []) do
    n = Keyword.get(opts, :cold_stub_samples, @cold_stub_samples)

    with_stub_shards(fn stub ->
      cycle = fn id ->
        seed_stub_shard(id)
        {us, {pid, ref, conn, path}} = :timer.tc(fn -> open_query(id) end)
        assert_pulled!("cold_open_stub_p50_us", path)
        Connection.close(conn)
        Fathom.Shard.checkin(pid, ref)
        Shards.drain(id)
        us
      end

      # Untimed warm-up: module/NIF load and the first Finch connection.
      cycle.(stub_id("warm"))

      samples = for i <- 1..n, do: cycle.(stub_id("cold#{i}"))

      # warm-up + n cycles, each at least a seed PUT + a lease PUT + the pull through the pool.
      assert_finch_used!(stub, 2 * (n + 1))
      assert_no_locks!(stub, "cold_open_stub_p50_us")
      median(samples)
    end)
  end

  @doc """
  `concurrent_cold_cycle_per_s` - cold checkout -> checkin -> drain cycles per second with
  #{@cold_cycle_workers} workers each cycling distinct shards, against the same real-S3-client +
  loopback-stub topology as `cold_open_stub/1` (expert review 2026-10-21 #2). Every other
  cold-open metric is serial, so nothing node-wide (the file server, the Finch pool, the Registry,
  the DynamicSupervisor) could show. Median of #{@cold_cycle_trials} trials after one untimed
  trial; seeding is untimed. WATCH-ONLY (new series).

  Preconditions: each cycle's checkout returned a non-empty local file (checked with a RAW file
  call, so the check itself does not load `file_server_2`), the Finch pool carried at least the
  pulls and releases, and no `.lock` object remains (every drain released by DELETE).
  """
  @spec concurrent_cold_cycle(keyword()) :: float()
  def concurrent_cold_cycle(opts \\ []) do
    workers = Keyword.get(opts, :cold_cycle_workers, @cold_cycle_workers)
    per = Keyword.get(opts, :cold_cycle_per_worker, @cold_cycle_per_worker)
    trials = Keyword.get(opts, :cold_cycle_trials, @cold_cycle_trials)

    with_stub_shards(fn stub ->
      run_trial = fn t ->
        batches =
          for w <- 1..workers do
            for c <- 1..per, do: stub_id("cc#{t}_#{w}_#{c}")
          end

        Enum.each(List.flatten(batches), &seed_stub_shard/1)

        {us, _} =
          :timer.tc(fn ->
            batches
            |> Task.async_stream(&cycle_all/1,
              max_concurrency: workers,
              ordered: false,
              timeout: 120_000
            )
            |> Enum.each(fn {:ok, :ok} -> :ok end)
          end)

        workers * per / (us / 1_000_000)
      end

      # Untimed trial 0: warms the pool's connections and the code paths.
      run_trial.(0)
      rates = for t <- 1..trials, do: run_trial.(t)

      assert_finch_used!(stub, 2 * workers * per * (trials + 1))
      assert_no_locks!(stub, "concurrent_cold_cycle_per_s")
      median(rates)
    end)
  end

  defp cycle_all(ids) do
    Enum.each(ids, fn id ->
      {:ok, pid, ref, path} = Shards.checkout(id)
      assert_pulled!("concurrent_cold_cycle_per_s", path)
      Fathom.Shard.checkin(pid, ref)
      Shards.drain(id)
    end)
  end

  # ---------------------------------------------------------------------------------------------
  # Flush-route arms (expert review 2026-10-21 r7 #2 / panel 3 #3). `flush_p50_us` alone measures
  # the pinned-live route; these measure the VACUUM INTO route, the drop flush, and the VACUUM
  # route through the real `Storage.S3` client + codec. The route is read from the existing
  # `[:fathom, :shard, :flush, :route]` / `[:fathom, :shard, :drop_flush, :route]` counters, so a
  # trial that took a different route raises instead of reporting the wrong series.
  # ---------------------------------------------------------------------------------------------

  @doc false
  # Attaches a handler on the two route events that mails `{ref, kind, route}` to the caller.
  @spec attach_routes() :: reference()
  def attach_routes do
    # `mix fathom.bench` does not boot the app; the handler table lives in the telemetry app.
    {:ok, _} = Application.ensure_all_started(:telemetry)
    ref = make_ref()
    parent = self()

    :telemetry.attach_many(
      {__MODULE__, :routes, ref},
      [[:fathom, :shard, :flush, :route], [:fathom, :shard, :drop_flush, :route]],
      fn [:fathom, :shard, kind, :route], _m, %{route: route}, _ ->
        send(parent, {ref, kind, route})
      end,
      nil
    )

    ref
  end

  @doc false
  @spec detach_routes(reference()) :: :ok
  def detach_routes(ref) do
    :telemetry.detach({__MODULE__, :routes, ref})
    :ok
  end

  @doc false
  # Drains the mailbox of route messages, oldest first, as `[{kind, route}]`.
  @spec take_routes(reference()) :: [{:flush | :drop_flush, atom()}]
  def take_routes(ref) do
    receive do
      {^ref, kind, route} -> [{kind, route} | take_routes(ref)]
    after
      0 -> []
    end
  end

  @doc false
  # Builds a Keystone shard behind `id` through the coordinator and returns what a flush trial
  # needs. The caller owns draining it.
  @spec open_flush_shard(String.t(), pos_integer()) :: {pid(), reference(), Path.t()}
  def open_flush_shard(id, rows) do
    {:ok, pid, ref, path} = Shards.checkout(id)
    {:ok, _} = Fathom.Keystone.build!(path, rows: rows)
    :ok = Fathom.Shard.stamp_local_provenance(id)
    {pid, ref, path}
  end

  @doc false
  # `trials` timed `flush_now/1` calls on the VACUUM INTO route (µs each). A reader holds an open
  # read transaction on the shard for the whole run, so the WAL can never be fully backfilled
  # (the replication-ON prod shape: a write-active shard almost never is), and one small write is
  # committed before every trial so the WAL is live. Preconditions, raised on: the shard is dirty
  # before the flush, and the ONLY route reported by every flush is `:vacuum`.
  @spec vacuum_route_flush_us(String.t(), pid(), Path.t(), pos_integer(), String.t()) ::
          [non_neg_integer()]
  def vacuum_route_flush_us(id, pid, path, trials, metric) do
    {:ok, reader} = Connection.open(path)
    {:ok, writer} = Connection.open(path)
    :ok = Connection.exec(writer, "CREATE TABLE IF NOT EXISTS bench_flush_w (v INTEGER)")
    :ok = Connection.exec(reader, "BEGIN")
    {:ok, _} = Connection.query(reader, "SELECT count(*) FROM sqlite_master", [])
    ref = attach_routes()

    try do
      one = fn ->
        :ok = Connection.exec(writer, "INSERT INTO bench_flush_w VALUES (1)")
        Fathom.Shard.WriteCounter.bump(id)

        unless Fathom.Shard.dirty?(pid),
          do: raise("#{metric} is measuring nothing: the shard is not dirty before flush_now/1")

        {t, res} = :timer.tc(fn -> Fathom.Shard.flush_now(pid) end)
        if res != :ok, do: raise("#{metric}: flush_now returned #{inspect(res)}")
        t
      end

      # Untimed warm-up flush.
      one.()
      _ = take_routes(ref)
      us = for _ <- 1..trials, do: one.()
      routes = take_routes(ref)

      if routes != List.duplicate({:flush, :vacuum}, trials),
        do:
          raise(
            "#{metric}: expected #{trials} x {:flush, :vacuum} routes, got #{inspect(routes)}; " <>
              "this is not measuring the VACUUM INTO route"
          )

      us
    after
      detach_routes(ref)
      Connection.exec(reader, "ROLLBACK")
      Connection.close(reader)
      Connection.close(writer)
    end
  end

  @doc """
  `flush_stub_p50_us` - median µs of one durability flush on the VACUUM INTO route through the REAL
  `Storage.S3` client, the codec (zstd + MD5 passes + decode proof) and the Finch pool, against a
  loopback `S3Stub` (expert review 2026-10-21 r7 #2). `flush_p50_us` runs `Storage.Local`, where
  the codec (~65% of a VACUUM-route flush's CPU) does not run. Loopback: the S3 PROTOCOL and codec
  cost, not a network RTT. WATCH-ONLY (new series).

  Preconditions (raises otherwise): every flush took the `:vacuum` route, the shard was dirty, and
  the S3 Finch pool carried at least one request per flush.
  """
  @spec flush_stub(keyword()) :: float()
  def flush_stub(opts \\ []) do
    trials = Keyword.get(opts, :trials, 5)
    rows = Keyword.get(opts, :flush_rows, 2_000)

    with_stub_shards(fn stub ->
      id = stub_id("flush")
      {pid, ref, path} = open_flush_shard(id, rows)
      before = :counters.get(stub.counter, 1)

      try do
        us = vacuum_route_flush_us(id, pid, path, trials, "flush_stub_p50_us")
        assert_finch_used!(stub, before + trials)
        median(us)
      after
        Fathom.Shard.checkin(pid, ref)
        Shards.drain(id, 5_000)
      end
    end)
  end

  defp open_query(id) do
    {:ok, pid, ref, path} = Shards.checkout(id)
    {:ok, conn} = Connection.open(path)
    {:ok, _} = Connection.query(conn, "SELECT 1", [])
    {pid, ref, conn, path}
  end

  # A RAW file call goes straight to the prim_file NIF instead of through `file_server_2`, so the
  # precondition check does not add load to the very server the concurrent arm exists to expose.
  defp assert_pulled!(metric, path) do
    case :file.read_file_info(String.to_charlist(path), [:raw]) do
      {:ok, info} when elem(info, 1) > 0 ->
        :ok

      other ->
        raise "#{metric}: the checkout did not leave a pulled, non-empty file at #{path} " <>
                "(#{inspect(other)}); this would measure a cycle that never pulled"
    end
  end

  defp assert_no_locks!(%{table: table}, metric) do
    locks = for [k] <- :ets.match(table, {:"$1", :_, :_}), String.ends_with?(k, ".lock"), do: k

    if locks != [],
      do:
        raise(
          "#{metric}: #{length(locks)} lease lock object(s) remain in the stub; the release " <>
            "DELETE did not run, so the drain path is not what this metric measures"
        ),
      else: :ok
  end

  defp stub_id(suffix), do: "stubcold-#{System.unique_integer([:positive])}-#{suffix}"

  # Creates a tiny shard locally and flushes it to the stub through the production flush path
  # (`Storage.flush/2` -> S3 PUT with the md5 metadata), then drops the local copy so the next
  # checkout is a genuine cold pull.
  defp seed_stub_shard(id) do
    data = Application.fetch_env!(:fathom, :shard_data_dir)
    tmp = Path.join(data, "#{id}.seed")
    {:ok, conn} = Connection.open(tmp)
    Connection.exec(conn, "CREATE TABLE t (id INTEGER PRIMARY KEY)")
    Connection.exec(conn, "INSERT INTO t DEFAULT VALUES")
    Connection.exec(conn, "PRAGMA wal_checkpoint(TRUNCATE)")
    Connection.close(conn)
    :ok = Storage.flush(id, tmp)
    Enum.each(["", "-wal", "-shm"], &File.rm(tmp <> &1))
  end

  # Runs `fun.(stub)` with `Storage.S3` pointed at a loopback stub AND the shard data path pointed
  # at a scratch dir, with the same hot-path knobs `Fathom.Bench.setup/1` turns off, then restores
  # every env key. Starts the registries/supervisors a checkout needs if no earlier metric did.
  # Owns its setup so the shared `Fathom.Bench.setup/1` (and every series that reads it) is
  # unchanged.
  defp with_stub_shards(fun) do
    keys = [
      :shard_data_dir,
      :shard_storage,
      :shard_flush_interval_ms,
      :directory_touch,
      :lazy_migrate
    ]

    prev = for k <- keys, do: {k, Application.fetch_env(:fathom, k)}
    dir = Path.join(tmp_dir(), "stubshards_#{System.unique_integer([:positive])}")
    File.mkdir_p!(dir)
    stub = start_s3()

    try do
      Application.put_env(:fathom, :shard_data_dir, dir)
      Application.put_env(:fathom, :shard_storage, Storage.S3)
      Application.put_env(:fathom, :shard_flush_interval_ms, 0)
      Application.put_env(:fathom, :directory_touch, false)
      Application.put_env(:fathom, :lazy_migrate, false)
      ensure_started(Registry, keys: :unique, name: Fathom.ShardRegistry)
      ensure_started(DynamicSupervisor, name: Fathom.ShardSupervisor, strategy: :one_for_one)
      ensure_started(Fathom.ShardLoad, [])
      ensure_started(Fathom.ShardLatency, [])
      fun.(stub)
    after
      stop_s3(stub)

      for {k, v} <- prev do
        case v do
          {:ok, val} -> Application.put_env(:fathom, k, val)
          :error -> Application.delete_env(:fathom, k)
        end
      end

      File.rm_rf(dir)
    end
  end

  defp ensure_started(mod, opts) do
    case mod.start_link(opts) do
      {:ok, _pid} -> :ok
      {:error, {:already_started, _pid}} -> :ok
    end
  end

  defp timed(n, fun) do
    fun.()
    Enum.map(1..n, fn _ -> elem(:timer.tc(fun), 0) end)
  end

  # Points `Storage.S3` at a loopback stub, starts the real S3 Finch pool, counts requests that go
  # through it, and restores everything afterwards.
  defp start_s3 do
    {:ok, _} = Application.ensure_all_started(:req)
    {:ok, _} = Application.ensure_all_started(:bandit)
    prev = Application.fetch_env(:fathom, S3)
    {sup, port, table} = S3Stub.start()
    counter = :counters.new(1, [])
    handler = {__MODULE__, System.unique_integer([:positive])}

    Application.put_env(:fathom, S3,
      bucket: @s3_bucket,
      region: "us-east-1",
      endpoint: "http://127.0.0.1:#{port}",
      path_style: true,
      prefix: @s3_prefix,
      access_key_id: "benchbench",
      secret_access_key: "benchbenchbench"
    )

    # `mix fathom.bench` does not boot the supervision tree, so the pool is started here (as
    # `setup_s3/1` does); under `mix test` the app already runs it, and that one is reused.
    finch =
      if Process.whereis(S3.finch_name()) do
        nil
      else
        {Finch, finch_opts} = S3.finch_child_spec()
        {:ok, pid} = Finch.start_link(finch_opts)
        pid
      end

    :ok =
      :telemetry.attach(
        handler,
        [:finch, :request, :stop],
        &__MODULE__.count_finch/4,
        {counter, S3.finch_name()}
      )

    %{
      table: table,
      port: port,
      counter: counter,
      sup: sup,
      finch: finch,
      handler: handler,
      prev: prev
    }
  end

  defp stop_s3(stub) do
    :telemetry.detach(stub.handler)
    if stub.finch, do: Supervisor.stop(stub.finch)
    Supervisor.stop(stub.sup)

    case stub.prev do
      {:ok, v} -> Application.put_env(:fathom, S3, v)
      :error -> Application.delete_env(:fathom, S3)
    end
  end

  @doc false
  def count_finch(_event, _measurements, %{name: name}, {counter, name}),
    do: :counters.add(counter, 1, 1)

  def count_finch(_event, _measurements, _meta, _config), do: :ok

  defp assert_finch_used!(%{counter: counter}, at_least) do
    n = :counters.get(counter, 1)

    if n < at_least,
      do:
        raise(
          "storage_io: only #{n} of #{at_least} requests went through the S3 Finch pool; " <>
            "this is not measuring the production client path"
        ),
      else: :ok
  end

  defp object_path(shard), do: "/#{@s3_bucket}/#{@s3_prefix}#{shard}.db"

  defp tmp_dir, do: Path.join(System.tmp_dir!(), "fathom_bench")

  @doc false
  @spec median([number()]) :: float()
  def median(samples) do
    sorted = Enum.sort(samples)
    n = length(sorted)
    mid = div(n, 2)

    if rem(n, 2) == 1,
      do: Enum.at(sorted, mid) / 1,
      else: (Enum.at(sorted, mid - 1) + Enum.at(sorted, mid)) / 2
  end
end
