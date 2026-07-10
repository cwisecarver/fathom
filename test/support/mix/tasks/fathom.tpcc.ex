defmodule Mix.Tasks.Fathom.Tpcc do
  @shortdoc "TPC-C W-sweep over the loopback Hrana WS wire (recorded-only) — MIX_ENV=test only"
  @moduledoc """
  Runs the TPC-C warehouse sweep (docs/tpc-benchmark-plan.md Phase 3) through the full Hrana
  WebSocket path and appends one row per warehouse count to `scripts/tpc_history.jsonl`.

  **Recorded-only** — TPC-C latencies are host/fsync-sensitive, so this is a peer-comparability
  trend line, **never a commit gate** (it writes `tpc_history.jsonl`, never `perf_history.jsonl`).
  Test-env only (the WS client is a dev/test dep).

      MIX_ENV=test mix fathom.tpcc                        # W=1..5, spec scale (~100 MB/W, slow)
      MIX_ENV=test mix fathom.tpcc --tpcc-max-w 2 --tpcc-scale 0.01 --tpcc-txns 500  # quick

  Options:
    * `--tpcc-max-w N`   sweep W = 1..N (default 5)
    * `--tpcc-threads T` concurrent loopback WS streams per W (default 8)
    * `--tpcc-txns N`    total transactions per W, split across threads (default 2000)
    * `--tpcc-scale F`   cardinality scale; 1.0 = spec (100k items, 3k/district), smaller = quick
                         (default 1.0)
    * `--no-append`      print only; do NOT write `scripts/tpc_history.jsonl`
  """
  use Mix.Task

  @history "scripts/tpc_history.jsonl"

  @impl true
  def run(argv) do
    unless Mix.env() == :test do
      Mix.raise("fathom.tpcc runs in MIX_ENV=test only (the WS client is a dev/test dep)")
    end

    {opts, _, _} =
      OptionParser.parse(argv,
        strict: [
          tpcc_max_w: :integer,
          tpcc_threads: :integer,
          tpcc_txns: :integer,
          tpcc_scale: :float,
          append: :boolean
        ]
      )

    Mix.Task.run("app.start")

    sweep_opts =
      for {k, sk} <- [
            tpcc_max_w: :max_w,
            tpcc_threads: :threads,
            tpcc_txns: :txns,
            tpcc_scale: :scale
          ],
          v = opts[k],
          do: {sk, v}

    rows = Fathom.Bench.Tpcc.sweep(sweep_opts)
    append? = Keyword.get(opts, :append, true)
    meta = meta()
    dirty = if meta["dirty"], do: "-dirty", else: ""

    IO.puts("\n=== fathom.tpcc (#{meta["commit"]}#{dirty}, #{meta["host"]}) ===")

    for row <- rows do
      line = Map.merge(meta, row)
      print(row)
      if append?, do: File.write!(@history, Jason.encode!(line) <> "\n", [:append])
    end

    unless append?, do: IO.puts("  (--no-append: nothing written to #{@history})")
  end

  defp print(row) do
    IO.puts(
      "  W=#{row["warehouses"]} threads=#{row["threads"]}  tpmC=#{row["tpcc_tpmc"]}  " <>
        "neworder p50/p99=#{row["tpcc_neworder_p50_us"]}/#{row["tpcc_neworder_p99_us"]}µs  " <>
        "payment p50/p99=#{row["tpcc_payment_p50_us"]}/#{row["tpcc_payment_p99_us"]}µs"
    )
  end

  defp meta do
    %{
      "ts" => DateTime.utc_now() |> DateTime.to_iso8601(),
      "commit" => git(["rev-parse", "--short", "HEAD"], "unknown"),
      "commit_full" => git(["rev-parse", "HEAD"], "unknown"),
      "branch" => git(["rev-parse", "--abbrev-ref", "HEAD"], "unknown"),
      "dirty" => git(["status", "--porcelain"], "") != "",
      "host" => to_string(:os.type() |> elem(1)),
      "mix_env" => "test"
    }
  end

  defp git(args, default) do
    case System.cmd("git", args, stderr_to_stdout: true) do
      {out, 0} -> String.trim(out)
      _ -> default
    end
  end
end
