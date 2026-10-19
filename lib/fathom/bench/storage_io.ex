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
  alias Fathom.Shard.Storage.Codec
  alias Fathom.Shard.Storage.S3

  @fixture_rows 160_000
  @min_plain_bytes 10_000_000
  @min_ratio 2.0
  @codec_trials 3
  @pull_trials 5
  @s3_head_samples 300
  @s3_put_samples 100
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

      with_s3(fn stub ->
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
      end)
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

    try do
      with_s3(fn stub ->
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
      end)
    after
      File.rm_rf(dir)
    end
  end

  defp timed(n, fun) do
    fun.()
    Enum.map(1..n, fn _ -> elem(:timer.tc(fun), 0) end)
  end

  # Points `Storage.S3` at a loopback stub, starts the real S3 Finch pool, counts requests that go
  # through it, and restores everything afterwards.
  defp with_s3(fun) do
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

    try do
      fun.(%{table: table, port: port, counter: counter})
    after
      :telemetry.detach(handler)
      if finch, do: Supervisor.stop(finch)
      Supervisor.stop(sup)

      case prev do
        {:ok, v} -> Application.put_env(:fathom, S3, v)
        :error -> Application.delete_env(:fathom, S3)
      end
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
