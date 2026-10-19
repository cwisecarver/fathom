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
  alias Fathom.Shard.Storage.Codec

  @fixture_rows 160_000
  @min_plain_bytes 10_000_000
  @min_ratio 2.0
  @codec_trials 3

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
