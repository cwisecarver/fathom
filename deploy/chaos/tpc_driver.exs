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
# Run (standalone; Mix.install compiles Filo — resolved beside this repo, or $FILO_PATH — on first run):
#   elixir deploy/chaos/tpc_driver.exs tpcb --lb http://localhost:8080 --domain fathom.test \
#     --shard tpc --txns 20000 --clients 256 --accounts 100000
#   elixir deploy/chaos/tpc_driver.exs rtt  --lb http://localhost:8080 --domain fathom.test --shard tpc
#
# Prints a human summary to stderr and a single JSON result object to stdout (so chaos.sh can tee
# it into a docs/reviews report), matching tpc_driver.py's contract.

# Filo sits beside the fathom repo; resolve it from THIS script's location so cwd doesn't matter
# (chaos.sh runs from deploy/chaos). $FILO_PATH overrides for other layouts / containers.
filo_path = System.get_env("FILO_PATH") || Path.expand("../../../filo", __DIR__)
Mix.install([{:filo, path: filo_path}, {:mint, "~> 1.6"}, {:jason, "~> 1.4"}])

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

    raw =
      Task.async_stream(0..(a.clients - 1), fn cid -> worker(cid, a) end,
        max_concurrency: a.clients,
        timeout: :infinity,
        ordered: false
      )
      |> Enum.to_list()

    # Workers are crash-proof (a tenant that can't connect returns an errored result), but treat any
    # async_stream exit defensively as a fully-errored tenant rather than crashing the whole run.
    results = for {:ok, r} <- raw, do: r
    crashed = length(raw) - length(results)

    lat = Enum.flat_map(results, fn {l, _span, _e} -> l end)
    spans = Enum.map(results, fn {_l, span, _e} -> span end)
    errs = Enum.sum(Enum.map(results, fn {_l, _span, e} -> e end)) + crashed * a.per_client

    window_us =
      case spans do
        [] ->
          0

        _ ->
          Enum.max(Enum.map(spans, fn {_t0, t1} -> t1 end)) -
            Enum.min(Enum.map(spans, fn {t0, _t1} -> t0 end))
      end

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
  # Crash-proof: a tenant that can't establish its stream returns an errored result, never an
  # exception (an uncaught worker exit would take down the whole run's aggregate).
  defp worker(cid, a) do
    :rand.seed(:exsss, {(cid + 1) * 7919, 104_729, 15_485_863})
    # Stagger the initial connect so thousands of workers don't hit the LB in one lockstep burst
    # (a real fleet never connects simultaneously). Spread over ~N ms; negligible at small N.
    Process.sleep(:rand.uniform(max(1, a.clients)))

    case establish(a, cid, 8) do
      {:ok, c} ->
        run_txns(c, a)

      :error ->
        # Couldn't connect+seed after retries — on the single-host rig this is CPU starvation (the
        # driver shares the VM with the nodes), so the server closes the idle-between-statements
        # connection mid-seed. Count the whole tenant as errored with a valid span: an honest errs
        # number, not a crashed run.
        t = System.monotonic_time(:microsecond)
        {[], {t, t}, a.per_client}
    end
  end

  defp run_txns(c, a) do
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

  # Open the stream and seed the shard, retrying the WHOLE thing (fresh connection) through a
  # transient failure — a connect refused by the herd, or a mid-seed `:closed` when the server drops
  # an idle-between-statements keepalive under load. The seed statements are idempotent
  # (CREATE TABLE IF NOT EXISTS / INSERT OR IGNORE), so re-running on a fresh connection is safe.
  defp establish(a, cid, attempts) do
    with {:ok, c} <- Client.connect(a.lb, authority: authority(a, cid)),
         {:ok, c} <- seed(c, a.accounts) do
      {:ok, c}
    else
      _ ->
        if attempts <= 1 do
          :error
        else
          Process.sleep(50 + :rand.uniform(200))
          establish(a, cid, attempts - 1)
        end
    end
  end

  defp seed(c, accounts) do
    {:ok, tpcb_seed(c, accounts)}
  rescue
    _ ->
      Client.close(c)
      :error
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
      accounts: 100_000,
      max_w: 5,
      threads: 8,
      scale: 0.02
    }

    a = parse_opts(rest, defaults)

    out =
      case mode do
        "rtt" -> mode_rtt(a)
        "tpcb" -> mode_tpcb(a)
        "tpcc" -> Tpcc.run(a)
        other -> raise "unknown mode #{other} (rtt|tpcb|tpcc)"
      end

    IO.puts(Jason.encode!(out))
  end

  @ints [:samples, :txns, :clients, :accounts, :threads, :max_w]
  @floats [:scale]

  defp parse_opts([], acc), do: acc

  defp parse_opts(["--" <> key, val | rest], acc) do
    k = safe_key(String.replace(key, "-", "_"))

    acc =
      cond do
        is_nil(k) -> acc
        k in @ints -> Map.put(acc, k, String.to_integer(val))
        k in @floats -> Map.put(acc, k, parse_float(val))
        true -> Map.put(acc, k, val)
      end

    parse_opts(rest, acc)
  end

  defp parse_opts([_unknown | rest], acc), do: parse_opts(rest, acc)

  defp safe_key(s) do
    String.to_existing_atom(s)
  rescue
    ArgumentError -> nil
  end

  # Float.parse tolerates integer strings ("1" -> 1.0) and trailing junk; 0.0 on total failure.
  defp parse_float(v) do
    case Float.parse(v) do
      {f, _} -> f
      :error -> 0.0
    end
  end
end

defmodule Tpcc do
  @moduledoc """
  TPC-C over Filo.Client — the port of tpcc_deck.py (which itself ports Fathom.Bench.Tpcc): the
  9-table schema, the scaled recursive-CTE seed, the five weighted transactions with value-feeding,
  and the W=1..max_w sweep (each W its own fresh shard). Every statement rides a held stream through
  the real LB, so per-txn latency is the true remote-client cost. Recorded-only, never a gate.
  """
  alias Filo.Client

  @now "2020-01-01 00:00:00"
  @txn_types [:new_order, :payment, :order_status, :delivery, :stock_level]
  @tables ~w(warehouse district customer history new_order oorder order_line item stock)

  # --- W-sweep -------------------------------------------------------------------------------
  def run(a) do
    runid = System.system_time(:millisecond)
    rows = for w <- 1..a.max_w, do: run_one_w(a, w, runid)

    Enum.each(rows, fn r ->
      IO.puts(
        :stderr,
        "  W=#{r["warehouses"]} threads=#{r["threads"]} tpmC=#{r["tpcc_tpmc"]} " <>
          "(#{r["errors"]} errs)  neworder p50/p99=#{r["tpcc_neworder_p50_us"]}/" <>
          "#{r["tpcc_neworder_p99_us"]}µs  payment p50/p99=#{r["tpcc_payment_p50_us"]}/" <>
          "#{r["tpcc_payment_p99_us"]}µs"
      )
    end)

    %{"mode" => "tpcc", "scale" => a.scale, "threads" => a.threads, "results" => rows}
  end

  defp run_one_w(a, w, runid) do
    shard = "tpcc_w#{w}_#{runid}"
    auth = "#{shard}.#{a.domain}"

    # Schema + seed on ONE connection (a single writer, no herd — kept simple, unlike the tpcb
    # per-client seed). A fresh shard per W, so the DROP TABLEs are no-ops here.
    {:ok, setup} = Client.connect(a.lb, authority: auth)

    setup =
      Enum.reduce(schema_ddl() ++ seed_sql(w, a.scale), setup, fn stmt, c -> x!(c, stmt) end)

    Client.close(setup)

    ctx = %{w_count: w, card: cardinalities(a.scale)}
    per_thread = max(1, div(a.txns, a.threads))

    worker_results =
      Task.async_stream(
        0..(a.threads - 1),
        fn tid -> worker(tid, a, w, auth, ctx, per_thread) end,
        max_concurrency: a.threads,
        timeout: :infinity,
        ordered: false
      )
      |> Enum.flat_map(fn
        {:ok, r} -> [r]
        {:exit, _} -> []
      end)

    samples =
      Enum.reduce(worker_results, empty_samples(), fn wr, acc ->
        Enum.reduce(@txn_types, acc, fn t, acc -> Map.update!(acc, t, &(&1 ++ wr.samples[t])) end)
      end)

    new_order = Enum.sum(Enum.map(worker_results, & &1.new_order))
    errors = Enum.sum(Enum.map(worker_results, & &1.errors))
    spans = Enum.map(worker_results, & &1.span)

    window_us =
      case spans do
        [] ->
          0

        _ ->
          Enum.max(Enum.map(spans, fn {_t0, t1} -> t1 end)) -
            Enum.min(Enum.map(spans, fn {t0, _t1} -> t0 end))
      end

    # tpmC = NewOrder txns per minute = new_order / (window_seconds / 60), window_us in µs.
    tpmc = if window_us > 0, do: Float.round(new_order / (window_us / 60_000_000), 1), else: 0.0
    row = %{"warehouses" => w, "threads" => a.threads, "errors" => errors, "tpcc_tpmc" => tpmc}

    Enum.reduce(@txn_types, row, fn t, row ->
      Enum.reduce(Tpc.pctls(samples[t]), row, fn {k, v}, row ->
        Map.put(row, "tpcc_#{txn_name(t)}_#{k}", v)
      end)
    end)
  end

  # Crash-proof: a thread that can't connect/warm returns an all-errored result, never an exception.
  defp worker(tid, a, w, auth, ctx, per_thread) do
    :rand.seed(:exsss, {(tid + 1) * (w + 1) * 4242, 104_729, 15_485_863})

    case Client.connect(a.lb, authority: auth) do
      {:ok, c} ->
        {c, _} = q!(c, "SELECT 1")
        run_txns(c, ctx, per_thread)

      {:error, _} ->
        errored(per_thread)
    end
  rescue
    _ -> errored(per_thread)
  end

  defp run_txns(c, ctx, per_thread) do
    t0 = System.monotonic_time(:microsecond)

    acc =
      Enum.reduce(1..per_thread, %{c: c, samples: empty_samples(), new_order: 0, errors: 0}, fn _,
                                                                                                acc ->
        typ = random_type()
        start = System.monotonic_time(:microsecond)

        try do
          {c, status} = run_txn(typ, acc.c, ctx)
          lat = System.monotonic_time(:microsecond) - start
          counted = typ == :new_order and status in [:committed, :rolled_back]

          %{
            acc
            | c: c,
              samples: Map.update!(acc.samples, typ, &[lat | &1]),
              new_order: acc.new_order + if(counted, do: 1, else: 0)
          }
        rescue
          # Transient (e.g. a 502 while the rebalancer flips this shard): reconnect + count, skip.
          _ -> %{acc | c: reconnect(acc.c), errors: acc.errors + 1}
        end
      end)

    t1 = System.monotonic_time(:microsecond)
    Client.close(acc.c)
    %{samples: acc.samples, new_order: acc.new_order, errors: acc.errors, span: {t0, t1}}
  end

  defp errored(per_thread) do
    t = System.monotonic_time(:microsecond)
    %{samples: empty_samples(), new_order: 0, errors: per_thread, span: {t, t}}
  end

  # --- transactions (value-fed: read a value, bind it into the next statement) ---------------
  defp run_txn(:new_order, c, ctx), do: new_order(c, ctx)
  defp run_txn(:payment, c, ctx), do: payment(c, ctx)
  defp run_txn(:order_status, c, ctx), do: order_status(c, ctx)
  defp run_txn(:delivery, c, ctx), do: delivery(c, ctx)
  defp run_txn(:stock_level, c, ctx), do: stock_level(c, ctx)

  defp new_order(c, ctx) do
    w = rw(ctx)
    d = :rand.uniform(10)
    c_id = :rand.uniform(ctx.card.per_district)
    ol_cnt = 4 + :rand.uniform(11)
    rollback = :rand.uniform(100) == 1
    items = ctx.card.items

    c = x!(c, "BEGIN IMMEDIATE")
    c = x!(c, "SELECT w_tax FROM warehouse WHERE w_id=?", [w])

    {c, drows} =
      q!(c, "SELECT d_tax, d_next_o_id FROM district WHERE d_w_id=? AND d_id=?", [w, d])

    o_id = drows |> hd() |> Enum.at(1)
    c = x!(c, "UPDATE district SET d_next_o_id=d_next_o_id+1 WHERE d_w_id=? AND d_id=?", [w, d])

    c =
      x!(
        c,
        "SELECT c_discount,c_last,c_credit FROM customer WHERE c_w_id=? AND c_d_id=? AND c_id=?",
        [
          w,
          d,
          c_id
        ]
      )

    c =
      x!(
        c,
        "INSERT INTO oorder (o_id,o_d_id,o_w_id,o_c_id,o_entry_d,o_ol_cnt,o_all_local) VALUES (?,?,?,?,?,?,1)",
        [o_id, d, w, c_id, @now, ol_cnt]
      )

    c = x!(c, "INSERT INTO new_order (no_o_id,no_d_id,no_w_id) VALUES (?,?,?)", [o_id, d, w])
    dist_col = "s_dist_" <> pad2(d)

    1..ol_cnt
    |> Enum.reduce_while({c, :committed}, fn n, {c, _} ->
      i_id = if rollback and n == ol_cnt, do: items + 1, else: :rand.uniform(items)
      {c, item} = q!(c, "SELECT i_price,i_name,i_data FROM item WHERE i_id=?", [i_id])

      case item do
        [] ->
          {:halt, {x!(c, "ROLLBACK"), :rolled_back}}

        [[price | _] | _] ->
          {c, stock} =
            q!(c, "SELECT s_quantity,#{dist_col},s_data FROM stock WHERE s_w_id=? AND s_i_id=?", [
              w,
              i_id
            ])

          [s_qty, dist_info | _] = hd(stock)
          qty = 5
          new_qty = if s_qty - qty >= 10, do: s_qty - qty, else: s_qty - qty + 91

          c =
            x!(
              c,
              "UPDATE stock SET s_quantity=?,s_ytd=s_ytd+?,s_order_cnt=s_order_cnt+1 WHERE s_w_id=? AND s_i_id=?",
              [new_qty, qty, w, i_id]
            )

          c =
            x!(
              c,
              "INSERT INTO order_line (ol_o_id,ol_d_id,ol_w_id,ol_number,ol_i_id,ol_supply_w_id,ol_quantity,ol_amount,ol_dist_info) VALUES (?,?,?,?,?,?,?,?,?)",
              [o_id, d, w, n, i_id, w, qty, qty * price, dist_info]
            )

          {:cont, {c, :committed}}
      end
    end)
    |> case do
      {c, :rolled_back} -> {c, :rolled_back}
      {c, :committed} -> {x!(c, "COMMIT"), :committed}
    end
  end

  defp payment(c, ctx) do
    w = rw(ctx)
    d = :rand.uniform(10)
    amount = (:rand.uniform(500_000) + 100) / 100.0

    c = x!(c, "BEGIN IMMEDIATE")
    c = x!(c, "UPDATE warehouse SET w_ytd=w_ytd+? WHERE w_id=?", [amount, w])
    c = x!(c, "SELECT w_name,w_street_1,w_city,w_state,w_zip FROM warehouse WHERE w_id=?", [w])
    c = x!(c, "UPDATE district SET d_ytd=d_ytd+? WHERE d_w_id=? AND d_id=?", [amount, w, d])

    c =
      x!(
        c,
        "SELECT d_name,d_street_1,d_city,d_state,d_zip FROM district WHERE d_w_id=? AND d_id=?",
        [w, d]
      )

    {c, c_id, credit} = pick_customer(c, ctx, w, d)

    c =
      x!(
        c,
        "UPDATE customer SET c_balance=c_balance-?,c_ytd_payment=c_ytd_payment+?,c_payment_cnt=c_payment_cnt+1 WHERE c_w_id=? AND c_d_id=? AND c_id=?",
        [amount, amount, w, d, c_id]
      )

    c =
      if credit == "BC" do
        x!(c, "UPDATE customer SET c_data=? WHERE c_w_id=? AND c_d_id=? AND c_id=?", [
          "#{c_id} #{d} #{w} #{amount} |bc-data",
          w,
          d,
          c_id
        ])
      else
        c
      end

    c =
      x!(
        c,
        "INSERT INTO history (h_c_id,h_c_d_id,h_c_w_id,h_d_id,h_w_id,h_date,h_amount,h_data) VALUES (?,?,?,?,?,?,?,?)",
        [c_id, d, w, d, w, @now, amount, "payment"]
      )

    {x!(c, "COMMIT"), :committed}
  end

  defp order_status(c, ctx) do
    w = rw(ctx)
    d = :rand.uniform(10)
    c = x!(c, "BEGIN")
    {c, c_id, _credit} = pick_customer(c, ctx, w, d)

    {c, order} =
      q!(
        c,
        "SELECT o_id,o_entry_d,o_carrier_id FROM oorder WHERE o_w_id=? AND o_d_id=? AND o_c_id=? ORDER BY o_id DESC LIMIT 1",
        [w, d, c_id]
      )

    c =
      case order do
        [] ->
          c

        [[o_id | _] | _] ->
          x!(
            c,
            "SELECT ol_i_id,ol_supply_w_id,ol_quantity,ol_amount,ol_delivery_d FROM order_line WHERE ol_w_id=? AND ol_d_id=? AND ol_o_id=?",
            [w, d, o_id]
          )
      end

    {x!(c, "COMMIT"), :committed}
  end

  defp delivery(c, ctx) do
    w = rw(ctx)
    carrier = :rand.uniform(10)
    c = x!(c, "BEGIN IMMEDIATE")

    c =
      Enum.reduce(1..10, c, fn d, c ->
        {c, pending} =
          q!(
            c,
            "SELECT no_o_id FROM new_order WHERE no_w_id=? AND no_d_id=? ORDER BY no_o_id LIMIT 1",
            [
              w,
              d
            ]
          )

        case pending do
          [] ->
            c

          [[o_id | _] | _] ->
            c =
              x!(c, "DELETE FROM new_order WHERE no_w_id=? AND no_d_id=? AND no_o_id=?", [
                w,
                d,
                o_id
              ])

            {c, crows} =
              q!(c, "SELECT o_c_id FROM oorder WHERE o_w_id=? AND o_d_id=? AND o_id=?", [
                w,
                d,
                o_id
              ])

            c_id = crows |> hd() |> hd()

            c =
              x!(c, "UPDATE oorder SET o_carrier_id=? WHERE o_w_id=? AND o_d_id=? AND o_id=?", [
                carrier,
                w,
                d,
                o_id
              ])

            c =
              x!(
                c,
                "UPDATE order_line SET ol_delivery_d=? WHERE ol_w_id=? AND ol_d_id=? AND ol_o_id=?",
                [
                  @now,
                  w,
                  d,
                  o_id
                ]
              )

            {c, arows} =
              q!(
                c,
                "SELECT COALESCE(SUM(ol_amount),0) FROM order_line WHERE ol_w_id=? AND ol_d_id=? AND ol_o_id=?",
                [w, d, o_id]
              )

            amount = arows |> hd() |> hd()

            x!(
              c,
              "UPDATE customer SET c_balance=c_balance+?,c_delivery_cnt=c_delivery_cnt+1 WHERE c_w_id=? AND c_d_id=? AND c_id=?",
              [amount, w, d, c_id]
            )
        end
      end)

    {x!(c, "COMMIT"), :committed}
  end

  defp stock_level(c, ctx) do
    w = rw(ctx)
    d = :rand.uniform(10)
    threshold = 9 + :rand.uniform(11)
    c = x!(c, "BEGIN")
    {c, nrows} = q!(c, "SELECT d_next_o_id FROM district WHERE d_w_id=? AND d_id=?", [w, d])
    next_oid = nrows |> hd() |> hd()

    c =
      x!(
        c,
        "SELECT COUNT(DISTINCT s_i_id) FROM order_line, stock WHERE ol_w_id=? AND ol_d_id=? AND ol_o_id>=? AND ol_o_id<? AND s_w_id=? AND s_i_id=ol_i_id AND s_quantity<?",
        [w, d, next_oid - 20, next_oid, w, threshold]
      )

    {x!(c, "COMMIT"), :committed}
  end

  # 60% pick a customer by last name (multi-row → middle by c_first), 40% by id.
  defp pick_customer(c, ctx, w, d) do
    if :rand.uniform(100) <= 60 do
      last = "LAST" <> Integer.to_string(:rand.uniform(100) - 1)

      {c, rows} =
        q!(
          c,
          "SELECT c_id, c_credit FROM customer WHERE c_w_id=? AND c_d_id=? AND c_last=? ORDER BY c_first",
          [w, d, last]
        )

      case rows do
        [] ->
          pick_by_id(c, ctx, w, d)

        _ ->
          [c_id, credit] = Enum.at(rows, div(length(rows) - 1, 2))
          {c, c_id, credit}
      end
    else
      pick_by_id(c, ctx, w, d)
    end
  end

  defp pick_by_id(c, ctx, w, d) do
    c_id = :rand.uniform(ctx.card.per_district)

    {c, rows} =
      q!(c, "SELECT c_id, c_credit FROM customer WHERE c_w_id=? AND c_d_id=? AND c_id=?", [
        w,
        d,
        c_id
      ])

    [cid, credit] = hd(rows)
    {c, cid, credit}
  end

  # --- weighting + helpers -------------------------------------------------------------------
  defp random_type do
    r = :rand.uniform(100)

    cond do
      r <= 45 -> :new_order
      r <= 88 -> :payment
      r <= 92 -> :order_status
      r <= 96 -> :delivery
      true -> :stock_level
    end
  end

  defp rw(ctx), do: :rand.uniform(ctx.w_count)

  # Query returning {advanced_client, rows}; raise on error so the worker's rescue reconnects.
  defp q!(c, sql, args \\ []) do
    case Client.execute(c, sql, args) do
      {:ok, res, c} ->
        {c, res.rows}

      {:error, reason, _c} ->
        raise "tpcc exec failed (#{String.slice(sql, 0, 40)}): #{inspect(reason)}"
    end
  end

  # Statement whose rows we discard; returns the advanced client.
  defp x!(c, sql, args \\ []) do
    {c, _} = q!(c, sql, args)
    c
  end

  defp reconnect(c) do
    case Client.reconnect(c) do
      {:ok, c} -> c
      {:error, _} -> c
    end
  end

  defp empty_samples,
    do: %{new_order: [], payment: [], order_status: [], delivery: [], stock_level: []}

  defp txn_name(:new_order), do: "neworder"
  defp txn_name(:payment), do: "payment"
  defp txn_name(:order_status), do: "order_status"
  defp txn_name(:delivery), do: "delivery"
  defp txn_name(:stock_level), do: "stock_level"

  defp pad2(n), do: String.pad_leading(Integer.to_string(n), 2, "0")

  defp cardinalities(scale) do
    per_district = max(20, round(3000 * scale))

    %{
      items: max(50, round(100_000 * scale)),
      per_district: per_district,
      new_orders: max(5, round(per_district * 0.3)),
      districts: 10
    }
  end

  # --- schema + seed (interpolated counts are driver-controlled ints; row VALUES bind via ?) --
  defp schema_ddl do
    dist = Enum.map_join(1..10, ", ", fn n -> "s_dist_#{pad2(n)} TEXT" end)

    Enum.map(@tables, fn t -> "DROP TABLE IF EXISTS #{t}" end) ++
      [
        "CREATE TABLE warehouse (w_id INTEGER PRIMARY KEY, w_tax REAL, w_ytd REAL, w_name TEXT, w_street_1 TEXT, w_street_2 TEXT, w_city TEXT, w_state TEXT, w_zip TEXT)",
        "CREATE TABLE district (d_w_id INTEGER, d_id INTEGER, d_tax REAL, d_ytd REAL, d_next_o_id INTEGER, d_name TEXT, d_street_1 TEXT, d_street_2 TEXT, d_city TEXT, d_state TEXT, d_zip TEXT, PRIMARY KEY (d_w_id, d_id))",
        "CREATE TABLE customer (c_w_id INTEGER, c_d_id INTEGER, c_id INTEGER, c_first TEXT, c_last TEXT, c_discount REAL, c_credit TEXT, c_balance REAL, c_ytd_payment REAL, c_payment_cnt INTEGER, c_delivery_cnt INTEGER, c_data TEXT, PRIMARY KEY (c_w_id, c_d_id, c_id))",
        "CREATE INDEX idx_customer_last ON customer (c_w_id, c_d_id, c_last, c_first)",
        "CREATE TABLE history (h_c_id INTEGER, h_c_d_id INTEGER, h_c_w_id INTEGER, h_d_id INTEGER, h_w_id INTEGER, h_date TEXT, h_amount REAL, h_data TEXT)",
        "CREATE TABLE new_order (no_o_id INTEGER, no_d_id INTEGER, no_w_id INTEGER, PRIMARY KEY (no_w_id, no_d_id, no_o_id))",
        "CREATE TABLE oorder (o_id INTEGER, o_d_id INTEGER, o_w_id INTEGER, o_c_id INTEGER, o_entry_d TEXT, o_carrier_id INTEGER, o_ol_cnt INTEGER, o_all_local INTEGER, PRIMARY KEY (o_w_id, o_d_id, o_id))",
        "CREATE INDEX idx_order_cust ON oorder (o_w_id, o_d_id, o_c_id, o_id)",
        "CREATE TABLE order_line (ol_o_id INTEGER, ol_d_id INTEGER, ol_w_id INTEGER, ol_number INTEGER, ol_i_id INTEGER, ol_supply_w_id INTEGER, ol_delivery_d TEXT, ol_quantity INTEGER, ol_amount REAL, ol_dist_info TEXT, PRIMARY KEY (ol_w_id, ol_d_id, ol_o_id, ol_number))",
        "CREATE TABLE item (i_id INTEGER PRIMARY KEY, i_im_id INTEGER, i_name TEXT, i_price REAL, i_data TEXT)",
        "CREATE TABLE stock (s_w_id INTEGER, s_i_id INTEGER, s_quantity INTEGER, s_ytd REAL, s_order_cnt INTEGER, s_remote_cnt INTEGER, s_data TEXT, #{dist}, PRIMARY KEY (s_w_id, s_i_id))"
      ]
  end

  defp rng(name, var, lo, hi),
    do:
      "#{name}(#{var}) AS (SELECT #{lo} UNION ALL SELECT #{var}+1 FROM #{name} WHERE #{var} < #{hi})"

  defp ctes(rs), do: "WITH RECURSIVE " <> Enum.join(rs, ", ") <> " "

  defp seed_sql(w, scale) do
    card = cardinalities(scale)
    items = card.items
    p = card.per_district
    delivered_hi = p - card.new_orders
    dist_cols = Enum.map_join(1..10, ", ", fn n -> "s_dist_#{pad2(n)}" end)
    dist_vals = Enum.map_join(1..10, ", ", fn _ -> "'tpcc-dist-info-24-chars!'" end)

    [
      ctes([rng("iq", "i", 1, items)]) <>
        "INSERT INTO item (i_id,i_im_id,i_name,i_price,i_data) SELECT i, abs(random()%10000)+1, 'item-'||i, (abs(random()%9900)+100)/100.0, 'idata-'||i FROM iq",
      ctes([rng("wq", "w", 1, w)]) <>
        "INSERT INTO warehouse (w_id,w_tax,w_ytd,w_name,w_street_1,w_street_2,w_city,w_state,w_zip) SELECT w, 0.1, 300000.0, 'wh-'||w, 's1','s2','city','CA','12345' FROM wq",
      ctes([rng("wq", "w", 1, w), rng("dq", "d", 1, 10)]) <>
        "INSERT INTO district (d_w_id,d_id,d_tax,d_ytd,d_next_o_id,d_name,d_street_1,d_street_2,d_city,d_state,d_zip) SELECT w, d, 0.1, 30000.0, #{p + 1}, 'd-'||d, 's1','s2','city','CA','12345' FROM wq, dq",
      ctes([rng("wq", "w", 1, w), rng("dq", "d", 1, 10), rng("cq", "c", 1, p)]) <>
        "INSERT INTO customer (c_w_id,c_d_id,c_id,c_first,c_last,c_discount,c_credit,c_balance,c_ytd_payment,c_payment_cnt,c_delivery_cnt,c_data) SELECT w, d, c, 'first-'||c, 'LAST'||(c%100), (abs(random()%5000))/10000.0, CASE WHEN c%10=0 THEN 'BC' ELSE 'GC' END, -10.0, 10.0, 1, 0, 'cdata' FROM wq, dq, cq",
      ctes([rng("wq", "w", 1, w), rng("dq", "d", 1, 10), rng("cq", "c", 1, p)]) <>
        "INSERT INTO history (h_c_id,h_c_d_id,h_c_w_id,h_d_id,h_w_id,h_date,h_amount,h_data) SELECT c, d, w, d, w, '2020-01-01 00:00:00', 10.0, 'hdata' FROM wq, dq, cq",
      ctes([rng("wq", "w", 1, w), rng("iq", "i", 1, items)]) <>
        "INSERT INTO stock (s_w_id,s_i_id,s_quantity,s_ytd,s_order_cnt,s_remote_cnt,s_data,#{dist_cols}) SELECT w, i, abs(random()%90)+10, 0.0, 0, 0, 'sdata', #{dist_vals} FROM wq, iq",
      ctes([rng("wq", "w", 1, w), rng("dq", "d", 1, 10), rng("oq", "o", 1, p)]) <>
        "INSERT INTO oorder (o_id,o_d_id,o_w_id,o_c_id,o_entry_d,o_carrier_id,o_ol_cnt,o_all_local) SELECT o, d, w, o, '2020-01-01 00:00:00', CASE WHEN o > #{delivered_hi} THEN NULL ELSE abs(random()%10)+1 END, 10, 1 FROM wq, dq, oq",
      ctes([rng("wq", "w", 1, w), rng("dq", "d", 1, 10), rng("oq", "o", delivered_hi + 1, p)]) <>
        "INSERT INTO new_order (no_o_id,no_d_id,no_w_id) SELECT o, d, w FROM wq, dq, oq",
      ctes([
        rng("wq", "w", 1, w),
        rng("dq", "d", 1, 10),
        rng("oq", "o", 1, p),
        rng("nq", "n", 1, 10)
      ]) <>
        "INSERT INTO order_line (ol_o_id,ol_d_id,ol_w_id,ol_number,ol_i_id,ol_supply_w_id,ol_delivery_d,ol_quantity,ol_amount,ol_dist_info) SELECT o, d, w, n, abs(random()%#{items})+1, w, '2020-01-01 00:00:00', 5, (abs(random()%1000000))/100.0, 'tpcc-ol-dist-info-24char' FROM wq, dq, oq, nq"
    ]
  end
end

Tpc.main(System.argv())
