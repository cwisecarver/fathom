#!/usr/bin/env elixir
#
# Fathom chaos-rig remote TPC driver — ELIXIR (rides Filo.Client).
#
# Same job as tpc_driver.py: a libSQL/Hrana **remote client** that drives TPC-B over the real
# network through the LB, one Hrana stream per tenant shard, routing by the `Host: <shard>.<domain>`
# header exactly as django-libsql / libsql-experimental do.
#
# The Hrana wire protocol lives in Filo — this driver is now a thin consumer of `Filo.Client`
# (connect / execute / reconnect / close), so it shares the server's own value + result codec
# instead of hand-rolling one. What's left here is purely the load model: the TPC-B workload and
# the one-BEAM-process-per-client orchestration.
#
# WHY ELIXIR: the Python driver runs one OS thread per client and collapses past ~128 clients/proc
# (GIL + per-thread memory), so replicating "hundreds of ECS containers" needs many heavy
# processes. Here each client is a lightweight BEAM process holding one owned Filo.Client stream
# (~KB of state, no GIL), so one BEAM node models thousands of concurrent clients on one box.
# (It does NOT exercise django-libsql itself; keep the Python path for that.)
#
# Run (standalone; Mix.install compiles Filo from ../filo on first run):
#   elixir deploy/chaos/tpc_driver.exs tpcb --lb http://localhost:8080 --domain fathom.test \
#     --shard tpc --txns 20000 --clients 256 --accounts 100000
#   elixir deploy/chaos/tpc_driver.exs rtt  --lb http://localhost:8080 --domain fathom.test --shard tpc
#
# Prints a human summary to stderr and a single JSON result object to stdout (so chaos.sh can tee
# it into a docs/reviews report), matching tpc_driver.py's contract.

Mix.install([{:filo, path: "../filo"}, {:mint, "~> 1.6"}, {:jason, "~> 1.4"}])

defmodule Tpc do
  @moduledoc "TPC workloads + one-BEAM-process-per-client orchestration over Filo.Client."

  alias Filo.Client

  @tpcb_tellers 10
  @tpcb_schema [
    "CREATE TABLE IF NOT EXISTS branches (bid INTEGER PRIMARY KEY, bbalance INTEGER)",
    "CREATE TABLE IF NOT EXISTS tellers (tid INTEGER PRIMARY KEY, bid INTEGER, tbalance INTEGER)",
    "CREATE TABLE IF NOT EXISTS accounts (aid INTEGER PRIMARY KEY, bid INTEGER, abalance INTEGER)",
    "CREATE TABLE IF NOT EXISTS history (tid INTEGER, bid INTEGER, aid INTEGER, delta INTEGER, mtime TEXT)"
  ]

  # --- percentiles (samples are µs), mirrors pctls() -------------------------------------------
  def pctls([]), do: %{"p50_us" => nil, "p95_us" => nil, "p99_us" => nil, "max_us" => nil}

  def pctls(samples) do
    xs = Enum.sort(samples) |> List.to_tuple()

    %{
      "p50_us" => q(xs, 0.50),
      "p95_us" => q(xs, 0.95),
      "p99_us" => q(xs, 0.99),
      "max_us" => Float.round(elem(xs, tuple_size(xs) - 1) / 1, 1)
    }
  end

  defp q(xs, p) do
    n = tuple_size(xs)

    v =
      if n == 1 do
        elem(xs, 0)
      else
        rank = p * (n - 1)
        lo = trunc(rank)
        hi = min(lo + 1, n - 1)
        frac = rank - lo
        elem(xs, lo) * (1 - frac) + elem(xs, hi) * frac
      end

    Float.round(v / 1, 1)
  end

  # --- RTT probe -------------------------------------------------------------------------------
  def mode_rtt(a) do
    {:ok, c} = Client.connect(a.lb, authority: authority(a, 0))
    # warm the stream (open + cold-open), untimed
    c = exec!(c, "SELECT 1")

    {c, lat} =
      Enum.reduce(1..a.samples, {c, []}, fn _, {c, lat} ->
        t0 = System.monotonic_time(:microsecond)
        c = exec!(c, "SELECT 1")
        {c, [System.monotonic_time(:microsecond) - t0 | lat]}
      end)

    Client.close(c)
    p = pctls(lat)

    IO.puts(
      :stderr,
      "  rtt over #{a.samples} warm SELECT 1 round-trips through the LB: " <>
        "p50=#{p["p50_us"]}µs p95=#{p["p95_us"]}µs p99=#{p["p99_us"]}µs"
    )

    %{"mode" => "rtt", "shard" => a.shard, "samples" => a.samples, "rtt_us" => p["p50_us"]}
    |> Map.merge(for {k, v} <- p, into: %{}, do: {"rtt_#{k}", v})
  end

  # --- TPC-B (pgbench bank txn: 7 statements on a held stream) ---------------------------------
  def mode_tpcb(args) do
    a = Map.put(args, :per_client, max(1, div(args.txns, args.clients)))

    results =
      Task.async_stream(0..(a.clients - 1), fn cid -> worker(cid, a) end,
        max_concurrency: a.clients,
        timeout: :infinity,
        ordered: false
      )
      |> Enum.map(fn {:ok, r} -> r end)

    lat = Enum.flat_map(results, fn {l, _span, _e} -> l end)
    spans = Enum.map(results, fn {_l, span, _e} -> span end)
    errs = Enum.sum(Enum.map(results, fn {_l, _span, e} -> e end))

    window_us =
      Enum.max(Enum.map(spans, fn {_t0, t1} -> t1 end)) -
        Enum.min(Enum.map(spans, fn {t0, _t1} -> t0 end))

    tps = if window_us > 0, do: Float.round(length(lat) / (window_us / 1_000_000), 1), else: 0.0
    p = pctls(lat)

    IO.puts(
      :stderr,
      "  tpcb: #{length(lat)} txns across #{a.clients} tenant shard(s), #{tps} " <>
        "txn/s through the LB (#{errs} transient errs) — p50=#{p["p50_us"]}µs p95=#{p["p95_us"]}µs " <>
        "p99=#{p["p99_us"]}µs"
    )

    %{
      "mode" => "tpcb",
      "shard" => a.shard,
      "tenant_shards" => a.clients,
      "txns" => length(lat),
      "errors" => errs,
      "tpcb_tps" => tps
    }
    |> Map.merge(for {k, v} <- p, into: %{}, do: {"tpcb_#{k}", v})
  end

  # One client = one tenant shard = one BEAM process holding one Filo.Client stream.
  defp worker(cid, a) do
    :rand.seed(:exsss, {(cid + 1) * 7919, 104_729, 15_485_863})
    {:ok, c} = Client.connect(a.lb, authority: authority(a, cid))
    c = seed_with_retry(c, a.accounts, 3)

    t0 = System.monotonic_time(:microsecond)

    {lat, errs, c} =
      Enum.reduce(1..a.per_client, {[], 0, c}, fn _, {lat, e, c} ->
        start = System.monotonic_time(:microsecond)

        try do
          c = tpcb_txn(c, a.accounts)
          {[System.monotonic_time(:microsecond) - start | lat], e, c}
        rescue
          # Transient: a Hrana-level error (rebalancer flip) OR a stale keepalive the server
          # closed between requests. Real SDKs retry both; reconnect and count as a transient err.
          _ -> {lat, e + 1, reconnect(c)}
        end
      end)

    t1 = System.monotonic_time(:microsecond)
    Client.close(c)
    {lat, {t0, t1}, errs}
  end

  # `--clients C` = C tenant shards, each its own single-writer file, so there is no intra-shard
  # lock convoy — the per-txn latency is clean and the aggregate is the realistic multi-tenant node
  # throughput. The Host authority is what the LB consistent-hashes to a node.
  defp authority(%{clients: n, shard: s, domain: d}, cid) when n > 1, do: "#{s}_#{cid}.#{d}"
  defp authority(%{shard: s, domain: d}, _cid), do: "#{s}.#{d}"

  defp seed_with_retry(c, accounts, attempts) do
    tpcb_seed(c, accounts)
  rescue
    e ->
      if attempts <= 1, do: reraise(e, __STACKTRACE__)
      seed_with_retry(reconnect(c), accounts, attempts - 1)
  end

  defp tpcb_seed(c, accounts) do
    c = Enum.reduce(@tpcb_schema, c, fn ddl, c -> exec!(c, ddl) end)
    c = exec!(c, "INSERT OR IGNORE INTO branches (bid, bbalance) VALUES (1, 0)")

    c =
      exec!(
        c,
        "WITH RECURSIVE seq(i) AS (SELECT 1 UNION ALL SELECT i+1 FROM seq WHERE i < #{@tpcb_tellers}) " <>
          "INSERT OR IGNORE INTO tellers (tid,bid,tbalance) SELECT i,1,0 FROM seq"
      )

    exec!(
      c,
      "WITH RECURSIVE seq(i) AS (SELECT 1 UNION ALL SELECT i+1 FROM seq WHERE i < #{accounts}) " <>
        "INSERT OR IGNORE INTO accounts (aid,bid,abalance) SELECT i,1,0 FROM seq"
    )
  end

  defp tpcb_txn(c, accounts) do
    aid = :rand.uniform(accounts)
    tid = :rand.uniform(@tpcb_tellers)
    delta = :rand.uniform(10_001) - 5_001
    mtime = Integer.to_string(System.system_time(:second))
    # Values bound positionally — Filo.Client encodes them via Filo.Value. Only our own integer
    # counts go into the seed CTEs above.
    c
    |> exec!("BEGIN IMMEDIATE")
    |> exec!("UPDATE accounts SET abalance=abalance+? WHERE aid=?", [delta, aid])
    |> exec!("SELECT abalance FROM accounts WHERE aid=?", [aid])
    |> exec!("UPDATE tellers SET tbalance=tbalance+? WHERE tid=?", [delta, tid])
    |> exec!("UPDATE branches SET bbalance=bbalance+? WHERE bid=1", [delta])
    |> exec!(
      "INSERT INTO history (tid,bid,aid,delta,mtime) VALUES (?,1,?,?,?)",
      [tid, aid, delta, mtime]
    )
    |> exec!("COMMIT")
  end

  # Run one statement, threading the advanced client; raise on any error so the worker's rescue
  # reconnects and retries — the real-SDK recovery path.
  defp exec!(client, sql, args \\ []) do
    case Client.execute(client, sql, args) do
      {:ok, _result, client} ->
        client

      {:error, reason, _client} ->
        raise "tpc exec failed (#{String.slice(sql, 0, 40)}): #{inspect(reason)}"
    end
  end

  defp reconnect(client) do
    case Client.reconnect(client) do
      {:ok, client} -> client
      {:error, _reason} -> client
    end
  end

  # --- CLI -------------------------------------------------------------------------------------
  def main(argv) do
    {mode, rest} =
      case argv do
        [m | r] -> {m, r}
        [] -> {"tpcb", []}
      end

    defaults = %{
      lb: "http://localhost:8080",
      domain: "fathom.test",
      shard: "tpc",
      samples: 200,
      txns: 2000,
      clients: 8,
      accounts: 100_000
    }

    a = parse_opts(rest, defaults)

    out =
      case mode do
        "rtt" -> mode_rtt(a)
        "tpcb" -> mode_tpcb(a)
        other -> raise "unknown mode #{other} (rtt|tpcb; tpcc TODO)"
      end

    IO.puts(Jason.encode!(out))
  end

  @ints [:samples, :txns, :clients, :accounts]

  defp parse_opts([], acc), do: acc

  defp parse_opts(["--" <> key, val | rest], acc) do
    k = safe_key(String.replace(key, "-", "_"))

    acc =
      if k, do: Map.put(acc, k, if(k in @ints, do: String.to_integer(val), else: val)), else: acc

    parse_opts(rest, acc)
  end

  defp parse_opts([_unknown | rest], acc), do: parse_opts(rest, acc)

  defp safe_key(s) do
    String.to_existing_atom(s)
  rescue
    ArgumentError -> nil
  end
end

Tpc.main(System.argv())
