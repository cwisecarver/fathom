defmodule Fathom.Bench.Tpcc do
  @moduledoc """
  TPC-C schema + seed (Phase 3, docs/tpc-benchmark-plan.md) for the **recorded-only**
  comparability benchmark. This module owns the 9-table schema and an in-process seeder; the
  weighted transaction deck + driver live in the sibling sections (added in later units) and the
  `mix fathom.tpcc` task drives them over the loopback Hrana-WS wire.

  Test-env only (it rides the same dev/test wire harness as `Fathom.Bench.Wire`), and it never
  gates anything — TPC-C latencies are host/fsync-sensitive, so they are a peer-comparability
  trend line written to `scripts/tpc_history.jsonl`, not a commit gate.

  ## Schema (9 tables, per TPC-C)

  `warehouse` (W), `district` (10·W), `customer` (P·10·W), `history` (P·10·W), `new_order`
  (~0.3·P·10·W), `oorder` (P·10·W), `order_line` (~10·P·10·W), `item` (I, warehouse-independent),
  `stock` (I·W) — where I is the item count and P the customers/orders per district, both scaled
  by `--tpcc-scale` (default 1.0 = spec: I=100k, P=3k, so W=1 ≈ 100 MB).

  ## Seeding

  Seeding is **setup, not the measured workload**, so it runs in-process (a direct
  `Fathom.Shard.Connection` into a throwaway `.seed`, flushed to `Fathom.Shard.Storage`, local
  copy dropped) exactly like `Fathom.Bench.Wire`'s TPC-B seed — the timed wire open then
  cold-pulls it. Row counts are harness constants (never user input), so they are interpolated
  into recursive-CTE bulk inserts; every *value a transaction later binds* is a bound `?`.
  """

  alias Fathom.Bench.HranaClient
  alias Fathom.Shard.{Connection, Storage}
  alias Fathom.Shards

  @base_items 100_000
  @base_per_district 3_000
  @districts 10
  @new_order_frac 0.3

  @doc """
  Scaled table cardinalities for a `--tpcc-scale` factor. `scale: 1.0` is spec (100k items,
  3k customers/orders per district). Minimums keep every code path exercisable at tiny scale
  (enough items to draw, enough customers per district for the by-last-name lookup).
  """
  @spec cardinalities(number()) :: %{
          items: pos_integer(),
          per_district: pos_integer(),
          new_orders: pos_integer(),
          districts: pos_integer()
        }
  def cardinalities(scale) when scale > 0 do
    per_district = max(20, round(@base_per_district * scale))

    %{
      items: max(50, round(@base_items * scale)),
      per_district: per_district,
      new_orders: max(5, round(per_district * @new_order_frac)),
      districts: @districts
    }
  end

  @doc "The item count for a scale (New-Order / Stock-Level draw `i_id ∈ 1..items`)."
  def items(scale), do: cardinalities(scale).items

  @doc "Customers/orders per district for a scale."
  def per_district(scale), do: cardinalities(scale).per_district

  @doc "Districts per warehouse (always 10 — not scaled)."
  def districts, do: @districts

  @doc """
  Seed a fresh TPC-C shard (`w_count` warehouses at `scale`) into storage, in-process, leaving
  no local copy — the timed wire open then cold-pulls it. Mirrors `Fathom.Bench.Wire`'s storage
  seed.
  """
  @spec seed_storage_shard(String.t(), pos_integer(), number()) :: :ok
  def seed_storage_shard(id, w_count, scale) do
    tmp = Path.join(System.tmp_dir!(), "#{id}.seed")
    drop_local(tmp)
    {:ok, conn} = Connection.open(tmp)

    try do
      Enum.each(schema_ddl(), &(:ok = Connection.exec(conn, &1)))
      Enum.each(seed_sql(w_count, scale), &(:ok = Connection.exec(conn, &1)))
      :ok = Connection.exec(conn, "PRAGMA wal_checkpoint(TRUNCATE)")
    after
      Connection.close(conn)
    end

    :ok = Storage.flush(id, tmp)
    drop_local(tmp)
    :ok
  end

  @doc "The 9-table schema + supporting indexes (warehouse-count-independent DDL)."
  @spec schema_ddl() :: [String.t()]
  def schema_ddl do
    [
      "CREATE TABLE warehouse (w_id INTEGER PRIMARY KEY, w_tax REAL, w_ytd REAL, w_name TEXT, " <>
        "w_street_1 TEXT, w_street_2 TEXT, w_city TEXT, w_state TEXT, w_zip TEXT)",
      "CREATE TABLE district (d_w_id INTEGER, d_id INTEGER, d_tax REAL, d_ytd REAL, " <>
        "d_next_o_id INTEGER, d_name TEXT, d_street_1 TEXT, d_street_2 TEXT, d_city TEXT, " <>
        "d_state TEXT, d_zip TEXT, PRIMARY KEY (d_w_id, d_id))",
      "CREATE TABLE customer (c_w_id INTEGER, c_d_id INTEGER, c_id INTEGER, c_first TEXT, " <>
        "c_last TEXT, c_discount REAL, c_credit TEXT, c_balance REAL, c_ytd_payment REAL, " <>
        "c_payment_cnt INTEGER, c_delivery_cnt INTEGER, c_data TEXT, " <>
        "PRIMARY KEY (c_w_id, c_d_id, c_id))",
      "CREATE INDEX idx_customer_last ON customer (c_w_id, c_d_id, c_last, c_first)",
      "CREATE TABLE history (h_c_id INTEGER, h_c_d_id INTEGER, h_c_w_id INTEGER, " <>
        "h_d_id INTEGER, h_w_id INTEGER, h_date TEXT, h_amount REAL, h_data TEXT)",
      "CREATE TABLE new_order (no_o_id INTEGER, no_d_id INTEGER, no_w_id INTEGER, " <>
        "PRIMARY KEY (no_w_id, no_d_id, no_o_id))",
      "CREATE TABLE oorder (o_id INTEGER, o_d_id INTEGER, o_w_id INTEGER, o_c_id INTEGER, " <>
        "o_entry_d TEXT, o_carrier_id INTEGER, o_ol_cnt INTEGER, o_all_local INTEGER, " <>
        "PRIMARY KEY (o_w_id, o_d_id, o_id))",
      "CREATE INDEX idx_order_cust ON oorder (o_w_id, o_d_id, o_c_id, o_id)",
      "CREATE TABLE order_line (ol_o_id INTEGER, ol_d_id INTEGER, ol_w_id INTEGER, " <>
        "ol_number INTEGER, ol_i_id INTEGER, ol_supply_w_id INTEGER, ol_delivery_d TEXT, " <>
        "ol_quantity INTEGER, ol_amount REAL, ol_dist_info TEXT, " <>
        "PRIMARY KEY (ol_w_id, ol_d_id, ol_o_id, ol_number))",
      "CREATE TABLE item (i_id INTEGER PRIMARY KEY, i_im_id INTEGER, i_name TEXT, " <>
        "i_price REAL, i_data TEXT)",
      "CREATE TABLE stock (s_w_id INTEGER, s_i_id INTEGER, s_quantity INTEGER, s_ytd REAL, " <>
        "s_order_cnt INTEGER, s_remote_cnt INTEGER, s_data TEXT, " <>
        Enum.map_join(1..10, ", ", &"s_dist_#{pad(&1)} TEXT") <>
        ", PRIMARY KEY (s_w_id, s_i_id))"
    ]
  end

  @doc """
  The bulk seed inserts for `w_count` warehouses at `scale`, as single CTE statements (one per
  table). Row counts are harness constants — safe to interpolate; SQLite `random()` fills the
  variable columns per row. Orders 1..P are seeded per district; the last `new_orders` of them are
  pending (rows in `new_order`) and carrier-less; each order gets 10 order lines; `o_c_id = o_id`
  (a valid customer, since customers and orders per district are equal).
  """
  @spec seed_sql(pos_integer(), number()) :: [String.t()]
  def seed_sql(w_count, scale) do
    %{items: items, per_district: p, new_orders: no} = cardinalities(scale)
    delivered_hi = p - no

    dist_cols = Enum.map_join(1..10, ", ", &"s_dist_#{pad(&1)}")
    dist_vals = Enum.map_join(1..10, ", ", fn _ -> "'tpcc-dist-info-24-chars!'" end)

    [
      # item (I rows, warehouse-independent)
      with_ctes([rng("iq", "i", 1, items)]) <>
        "INSERT INTO item (i_id, i_im_id, i_name, i_price, i_data) " <>
        "SELECT i, abs(random() % 10000) + 1, 'item-' || i, " <>
        "(abs(random() % 9900) + 100) / 100.0, 'idata-' || i FROM iq",

      # warehouse (W rows)
      with_ctes([rng("wq", "w", 1, w_count)]) <>
        "INSERT INTO warehouse (w_id, w_tax, w_ytd, w_name, w_street_1, w_street_2, w_city, " <>
        "w_state, w_zip) SELECT w, 0.1, 300000.0, 'wh-' || w, 's1', 's2', 'city', 'CA', " <>
        "'12345' FROM wq",

      # district (10·W rows); d_next_o_id is the next unused order id (P + 1)
      with_ctes([rng("wq", "w", 1, w_count), rng("dq", "d", 1, 10)]) <>
        "INSERT INTO district (d_w_id, d_id, d_tax, d_ytd, d_next_o_id, d_name, d_street_1, " <>
        "d_street_2, d_city, d_state, d_zip) SELECT w, d, 0.1, 30000.0, #{p + 1}, 'd-' || d, " <>
        "'s1', 's2', 'city', 'CA', '12345' FROM wq, dq",

      # customer (P·10·W rows); c_last groups ~P/100 customers per name for the by-last-name path
      with_ctes([rng("wq", "w", 1, w_count), rng("dq", "d", 1, 10), rng("cq", "c", 1, p)]) <>
        "INSERT INTO customer (c_w_id, c_d_id, c_id, c_first, c_last, c_discount, c_credit, " <>
        "c_balance, c_ytd_payment, c_payment_cnt, c_delivery_cnt, c_data) " <>
        "SELECT w, d, c, 'first-' || c, 'LAST' || (c % 100), (abs(random() % 5000)) / 10000.0, " <>
        "CASE WHEN c % 10 = 0 THEN 'BC' ELSE 'GC' END, -10.0, 10.0, 1, 0, 'cdata' FROM wq, dq, cq",

      # history (one per customer)
      with_ctes([rng("wq", "w", 1, w_count), rng("dq", "d", 1, 10), rng("cq", "c", 1, p)]) <>
        "INSERT INTO history (h_c_id, h_c_d_id, h_c_w_id, h_d_id, h_w_id, h_date, h_amount, " <>
        "h_data) SELECT c, d, w, d, w, '2020-01-01 00:00:00', 10.0, 'hdata' FROM wq, dq, cq",

      # stock (I·W rows), one row per (warehouse, item), all 10 district-info columns filled
      with_ctes([rng("wq", "w", 1, w_count), rng("iq", "i", 1, items)]) <>
        "INSERT INTO stock (s_w_id, s_i_id, s_quantity, s_ytd, s_order_cnt, s_remote_cnt, " <>
        "s_data, #{dist_cols}) SELECT w, i, abs(random() % 90) + 10, 0.0, 0, 0, 'sdata', " <>
        "#{dist_vals} FROM wq, iq",

      # oorder (P·10·W rows); the last `new_orders` per district are pending (carrier NULL)
      with_ctes([rng("wq", "w", 1, w_count), rng("dq", "d", 1, 10), rng("oq", "o", 1, p)]) <>
        "INSERT INTO oorder (o_id, o_d_id, o_w_id, o_c_id, o_entry_d, o_carrier_id, o_ol_cnt, " <>
        "o_all_local) SELECT o, d, w, o, '2020-01-01 00:00:00', " <>
        "CASE WHEN o > #{delivered_hi} THEN NULL ELSE abs(random() % 10) + 1 END, 10, 1 " <>
        "FROM wq, dq, oq",

      # new_order (the pending tail of orders per district)
      with_ctes([
        rng("wq", "w", 1, w_count),
        rng("dq", "d", 1, 10),
        rng("oq", "o", delivered_hi + 1, p)
      ]) <>
        "INSERT INTO new_order (no_o_id, no_d_id, no_w_id) SELECT o, d, w FROM wq, dq, oq",

      # order_line (10 per order = ~10·P·10·W rows)
      with_ctes([
        rng("wq", "w", 1, w_count),
        rng("dq", "d", 1, 10),
        rng("oq", "o", 1, p),
        rng("nq", "n", 1, 10)
      ]) <>
        "INSERT INTO order_line (ol_o_id, ol_d_id, ol_w_id, ol_number, ol_i_id, ol_supply_w_id, " <>
        "ol_delivery_d, ol_quantity, ol_amount, ol_dist_info) SELECT o, d, w, n, " <>
        "abs(random() % #{items}) + 1, w, '2020-01-01 00:00:00', 5, " <>
        "(abs(random() % 1000000)) / 100.0, 'tpcc-ol-dist-info-24char' FROM wq, dq, oq, nq"
    ]
  end

  # --- transaction deck ----------------------------------------------------

  @txn_types [:new_order, :payment, :order_status, :delivery, :stock_level]

  @doc "The five TPC-C transaction types (deck order)."
  def txn_types, do: @txn_types

  @doc "Driver context for a shard: warehouse count + scaled cardinalities (id-draw ranges)."
  @spec context(pos_integer(), number()) :: map()
  def context(w_count, scale), do: %{w_count: w_count, card: cardinalities(scale)}

  @doc """
  A weighted transaction-type pick per the authoritative TPC-C deck: New-Order ~45%, Payment
  ~43%, Order-Status / Delivery / Stock-Level ~4% each (TPC-C spec / Jim Gray Benchmark
  Handbook ch. 12). `tpmC` counts New-Order.
  """
  @spec random_type() :: atom()
  def random_type do
    case :rand.uniform(100) do
      r when r <= 45 -> :new_order
      r when r <= 88 -> :payment
      r when r <= 92 -> :order_status
      r when r <= 96 -> :delivery
      _ -> :stock_level
    end
  end

  @doc """
  Run one transaction of `type` over the wire client `c` with driver `ctx`. Returns
  `{client, status}` where status is `:committed`, `:rolled_back` (New-Order's spec ~1% invalid-
  item rollback), or `:skipped` (Delivery found no pending order). All values are bound `?`, never
  interpolated (only harness-controlled column names — e.g. `s_dist_NN` — are). Read-modify-write
  transactions use `BEGIN IMMEDIATE` so concurrent writers on the one shard file serialize on the
  write lock instead of racing the district's `d_next_o_id` (single-writer SQLite).
  """
  @spec run(atom(), struct(), map()) :: {struct(), atom()}
  def run(:new_order, c, ctx), do: new_order(c, ctx)
  def run(:payment, c, ctx), do: payment(c, ctx)
  def run(:order_status, c, ctx), do: order_status(c, ctx)
  def run(:delivery, c, ctx), do: delivery(c, ctx)
  def run(:stock_level, c, ctx), do: stock_level(c, ctx)

  # New-Order: read Item+Stock, write Order+Order-Line+New-Order, update Stock + District. ~1% of
  # txns draw an invalid item on the last line and roll back (spec-required rollback exercise).
  defp new_order(c, %{card: card} = ctx) do
    w_id = rand_w(ctx)
    d_id = rand_d()
    c_id = rand_c(ctx)
    ol_cnt = 4 + :rand.uniform(11)
    rollback? = :rand.uniform(100) == 1
    entry_d = now_str()

    c = exec(c, "BEGIN IMMEDIATE")
    {c, _} = step(c, "SELECT w_tax FROM warehouse WHERE w_id = ?", [w_id])

    {c, dist} =
      step(c, "SELECT d_tax, d_next_o_id FROM district WHERE d_w_id = ? AND d_id = ?", [
        w_id,
        d_id
      ])

    [[_d_tax, o_id]] = dist.rows

    c =
      exec(c, "UPDATE district SET d_next_o_id = d_next_o_id + 1 WHERE d_w_id = ? AND d_id = ?", [
        w_id,
        d_id
      ])

    {c, _} =
      step(
        c,
        "SELECT c_discount, c_last, c_credit FROM customer WHERE c_w_id = ? AND c_d_id = ? AND c_id = ?",
        [w_id, d_id, c_id]
      )

    c =
      exec(
        c,
        "INSERT INTO oorder (o_id, o_d_id, o_w_id, o_c_id, o_entry_d, o_ol_cnt, o_all_local) " <>
          "VALUES (?, ?, ?, ?, ?, ?, 1)",
        [o_id, d_id, w_id, c_id, entry_d, ol_cnt]
      )

    c =
      exec(c, "INSERT INTO new_order (no_o_id, no_d_id, no_w_id) VALUES (?, ?, ?)", [
        o_id,
        d_id,
        w_id
      ])

    new_order_lines(c, ctx, w_id, d_id, o_id, ol_cnt, rollback?, card.items)
  end

  # Process ol_cnt order lines; on the invalid last item (forced-rollback path) roll back the
  # whole txn, else commit. Read via reduce_while so the rollback halts the loop.
  defp new_order_lines(c, ctx, w_id, d_id, o_id, ol_cnt, rollback?, items) do
    dist_col = "s_dist_#{pad(d_id)}"

    {c, status} =
      Enum.reduce_while(1..ol_cnt, {c, :committed}, fn n, {c, _} ->
        i_id = if rollback? and n == ol_cnt, do: items + 1, else: rand_i(ctx)
        {c, item} = step(c, "SELECT i_price, i_name, i_data FROM item WHERE i_id = ?", [i_id])

        case item.rows do
          [] ->
            {c, _} = step(c, "ROLLBACK")
            {:halt, {c, :rolled_back}}

          [[price, _name, _data]] ->
            {c, stock} =
              step(
                c,
                "SELECT s_quantity, #{dist_col}, s_data FROM stock WHERE s_w_id = ? AND s_i_id = ?",
                [w_id, i_id]
              )

            [[s_qty, dist_info, _s_data]] = stock.rows
            qty = 5
            new_qty = if s_qty - qty >= 10, do: s_qty - qty, else: s_qty - qty + 91

            c =
              exec(
                c,
                "UPDATE stock SET s_quantity = ?, s_ytd = s_ytd + ?, s_order_cnt = s_order_cnt + 1 " <>
                  "WHERE s_w_id = ? AND s_i_id = ?",
                [new_qty, qty, w_id, i_id]
              )

            c =
              exec(
                c,
                "INSERT INTO order_line (ol_o_id, ol_d_id, ol_w_id, ol_number, ol_i_id, " <>
                  "ol_supply_w_id, ol_quantity, ol_amount, ol_dist_info) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
                [o_id, d_id, w_id, n, i_id, w_id, qty, qty * price, dist_info]
              )

            {:cont, {c, :committed}}
        end
      end)

    case status do
      :rolled_back -> {c, :rolled_back}
      :committed -> {exec(c, "COMMIT"), :committed}
    end
  end

  # Payment: update Warehouse+District+Customer balances, insert History. 60% by-last-name (the
  # multi-row select), 40% by-id.
  defp payment(c, ctx) do
    w_id = rand_w(ctx)
    d_id = rand_d()
    amount = (:rand.uniform(500_000) + 100) / 100.0
    h_date = now_str()

    c = exec(c, "BEGIN IMMEDIATE")
    c = exec(c, "UPDATE warehouse SET w_ytd = w_ytd + ? WHERE w_id = ?", [amount, w_id])

    {c, _} =
      step(c, "SELECT w_name, w_street_1, w_city, w_state, w_zip FROM warehouse WHERE w_id = ?", [
        w_id
      ])

    c =
      exec(c, "UPDATE district SET d_ytd = d_ytd + ? WHERE d_w_id = ? AND d_id = ?", [
        amount,
        w_id,
        d_id
      ])

    {c, _} =
      step(
        c,
        "SELECT d_name, d_street_1, d_city, d_state, d_zip FROM district WHERE d_w_id = ? AND d_id = ?",
        [w_id, d_id]
      )

    {c, c_id, credit} = pick_customer(c, ctx, w_id, d_id)

    c =
      exec(
        c,
        "UPDATE customer SET c_balance = c_balance - ?, c_ytd_payment = c_ytd_payment + ?, " <>
          "c_payment_cnt = c_payment_cnt + 1 WHERE c_w_id = ? AND c_d_id = ? AND c_id = ?",
        [amount, amount, w_id, d_id, c_id]
      )

    # Bad-credit customers get their c_data rewritten with the payment trail (spec behavior).
    c =
      if credit == "BC" do
        exec(
          c,
          "UPDATE customer SET c_data = ? WHERE c_w_id = ? AND c_d_id = ? AND c_id = ?",
          ["#{c_id} #{d_id} #{w_id} #{amount} |bc-data", w_id, d_id, c_id]
        )
      else
        c
      end

    c =
      exec(
        c,
        "INSERT INTO history (h_c_id, h_c_d_id, h_c_w_id, h_d_id, h_w_id, h_date, h_amount, h_data) " <>
          "VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
        [c_id, d_id, w_id, d_id, w_id, h_date, amount, "payment"]
      )

    {exec(c, "COMMIT"), :committed}
  end

  # Order-Status (read-only): the customer's latest order + its order lines.
  defp order_status(c, ctx) do
    w_id = rand_w(ctx)
    d_id = rand_d()

    c = exec(c, "BEGIN")
    {c, c_id, _credit} = pick_customer(c, ctx, w_id, d_id)

    {c, order} =
      step(
        c,
        "SELECT o_id, o_entry_d, o_carrier_id FROM oorder WHERE o_w_id = ? AND o_d_id = ? AND " <>
          "o_c_id = ? ORDER BY o_id DESC LIMIT 1",
        [w_id, d_id, c_id]
      )

    c =
      case order.rows do
        [[o_id, _entry, _carrier]] ->
          {c, _lines} =
            step(
              c,
              "SELECT ol_i_id, ol_supply_w_id, ol_quantity, ol_amount, ol_delivery_d FROM " <>
                "order_line WHERE ol_w_id = ? AND ol_d_id = ? AND ol_o_id = ?",
              [w_id, d_id, o_id]
            )

          c

        [] ->
          c
      end

    {exec(c, "COMMIT"), :committed}
  end

  # Delivery (batch): for each district, deliver the oldest pending New-Order.
  defp delivery(c, ctx) do
    w_id = rand_w(ctx)
    carrier = :rand.uniform(10)
    delivery_d = now_str()

    c = exec(c, "BEGIN IMMEDIATE")

    c =
      Enum.reduce(1..10, c, fn d_id, c -> deliver_district(c, w_id, d_id, carrier, delivery_d) end)

    {exec(c, "COMMIT"), :committed}
  end

  defp deliver_district(c, w_id, d_id, carrier, delivery_d) do
    {c, pending} =
      step(
        c,
        "SELECT no_o_id FROM new_order WHERE no_w_id = ? AND no_d_id = ? ORDER BY no_o_id LIMIT 1",
        [w_id, d_id]
      )

    case pending.rows do
      [] ->
        c

      [[o_id]] ->
        c =
          exec(c, "DELETE FROM new_order WHERE no_w_id = ? AND no_d_id = ? AND no_o_id = ?", [
            w_id,
            d_id,
            o_id
          ])

        {c, ord} =
          step(c, "SELECT o_c_id FROM oorder WHERE o_w_id = ? AND o_d_id = ? AND o_id = ?", [
            w_id,
            d_id,
            o_id
          ])

        [[c_id]] = ord.rows

        c =
          exec(
            c,
            "UPDATE oorder SET o_carrier_id = ? WHERE o_w_id = ? AND o_d_id = ? AND o_id = ?",
            [
              carrier,
              w_id,
              d_id,
              o_id
            ]
          )

        c =
          exec(
            c,
            "UPDATE order_line SET ol_delivery_d = ? WHERE ol_w_id = ? AND ol_d_id = ? AND ol_o_id = ?",
            [delivery_d, w_id, d_id, o_id]
          )

        {c, sum} =
          step(
            c,
            "SELECT COALESCE(SUM(ol_amount), 0) FROM order_line WHERE ol_w_id = ? AND ol_d_id = ? AND ol_o_id = ?",
            [w_id, d_id, o_id]
          )

        [[amount]] = sum.rows

        exec(
          c,
          "UPDATE customer SET c_balance = c_balance + ?, c_delivery_cnt = c_delivery_cnt + 1 " <>
            "WHERE c_w_id = ? AND c_d_id = ? AND c_id = ?",
          [amount, w_id, d_id, c_id]
        )
    end
  end

  # Stock-Level (read-only join): distinct low-stock items across the last 20 orders' lines.
  defp stock_level(c, ctx) do
    w_id = rand_w(ctx)
    d_id = rand_d()
    threshold = 10 + :rand.uniform(11)

    c = exec(c, "BEGIN")

    {c, dist} =
      step(c, "SELECT d_next_o_id FROM district WHERE d_w_id = ? AND d_id = ?", [w_id, d_id])

    [[next_oid]] = dist.rows

    {c, _} =
      step(
        c,
        "SELECT COUNT(DISTINCT s_i_id) FROM order_line, stock WHERE ol_w_id = ? AND ol_d_id = ? " <>
          "AND ol_o_id >= ? AND ol_o_id < ? AND s_w_id = ? AND s_i_id = ol_i_id AND s_quantity < ?",
        [w_id, d_id, next_oid - 20, next_oid, w_id, threshold]
      )

    {exec(c, "COMMIT"), :committed}
  end

  # Customer selection: 60% by last name (multi-row, pick the middle by c_first), 40% by id. The
  # by-name path falls back to by-id when a name group is empty (small scale). Returns credit too
  # (Payment's bad-credit branch needs it).
  defp pick_customer(c, ctx, w_id, d_id) do
    if :rand.uniform(100) <= 60 do
      last = "LAST" <> Integer.to_string(:rand.uniform(100) - 1)

      {c, res} =
        step(
          c,
          "SELECT c_id, c_credit FROM customer WHERE c_w_id = ? AND c_d_id = ? AND c_last = ? ORDER BY c_first",
          [w_id, d_id, last]
        )

      case res.rows do
        [] ->
          pick_customer_by_id(c, ctx, w_id, d_id)

        rows ->
          [c_id, credit] = Enum.at(rows, div(length(rows) - 1, 2))
          {c, c_id, credit}
      end
    else
      pick_customer_by_id(c, ctx, w_id, d_id)
    end
  end

  defp pick_customer_by_id(c, ctx, w_id, d_id) do
    c_id = rand_c(ctx)

    {c, res} =
      step(
        c,
        "SELECT c_id, c_credit FROM customer WHERE c_w_id = ? AND c_d_id = ? AND c_id = ?",
        [w_id, d_id, c_id]
      )

    [[found_id, credit]] = res.rows
    {c, found_id, credit}
  end

  # execute-and-thread: `exec` discards the result (writes/DDL/COMMIT); `step` keeps it (reads).
  # Both raise on any wire error — fail-loud for the bench (busy is near-impossible under
  # BEGIN IMMEDIATE at bench scale; the ~1% New-Order rollback rides the empty-rows path, not an
  # error).
  defp exec(c, sql, args \\ []) do
    {:ok, c, _} = HranaClient.execute(c, sql, args)
    c
  end

  defp step(c, sql, args \\ []) do
    {:ok, c, res} = HranaClient.execute(c, sql, args)
    {c, res}
  end

  defp rand_w(%{w_count: w}), do: :rand.uniform(w)
  defp rand_d, do: :rand.uniform(10)
  defp rand_c(%{card: %{per_district: p}}), do: :rand.uniform(p)
  defp rand_i(%{card: %{items: i}}), do: :rand.uniform(i)
  defp now_str, do: DateTime.utc_now() |> DateTime.to_iso8601()

  # --- driver / W-sweep ----------------------------------------------------

  @default_max_w 5
  @default_threads 8
  @default_txns 2_000
  @default_scale 1.0

  @doc """
  Run the TPC-C W-sweep and return one **string-keyed, JSON-ready** result map per warehouse
  count `W ∈ 1..max_w` (recorded-only — the caller appends to `scripts/tpc_history.jsonl`). For
  each W: seed a shard (`scale`), then drive the weighted deck with `threads` concurrent loopback
  WS streams for `txns` total transactions, timing each transaction and reporting `tpcc_tpmc`
  (New-Order/min) + per-txn-type `p50/p95/p99/max` (µs). One shard per W, run in sequence.

  Options: `:max_w` (5), `:threads` (8), `:txns` (2000 total per W), `:scale` (1.0 = spec).
  """
  @spec sweep(keyword()) :: [map()]
  def sweep(opts \\ []) do
    max_w = Keyword.get(opts, :max_w, @default_max_w)
    threads = Keyword.get(opts, :threads, @default_threads)
    txns = Keyword.get(opts, :txns, @default_txns)
    scale = Keyword.get(opts, :scale, @default_scale)

    {:ok, sup, port} = HranaClient.start_listener()

    try do
      Enum.map(1..max_w, fn w -> run_one(port, w, scale, threads, txns) end)
    after
      HranaClient.stop_listener(sup)
    end
  end

  # One warehouse count: seed, drive `threads` concurrent streams (each its own share of the txn
  # budget on a persistent stream), collect per-thread latency samples + New-Order counts + the
  # thread's txn-loop window, then summarize.
  defp run_one(port, w, scale, threads, total_txns) do
    id = uniq("tpcc_w#{w}")
    :ok = seed_storage_shard(id, w, scale)
    ctx = context(w, scale)
    per_thread = max(1, div(total_txns, threads))

    results =
      1..threads
      |> Task.async_stream(fn t -> drive_thread(port, id, ctx, t, w, per_thread) end,
        max_concurrency: threads,
        timeout: :infinity
      )
      |> Enum.map(fn {:ok, r} -> r end)

    Shards.drain(id, 5_000)
    rm_shard(id)
    summarize(results, w, threads)
  end

  # One stream: connect (untimed — cold-open lives here, out of the per-txn timings), then run
  # `n` weighted txns timing each. Deterministic per-(thread,W) PRNG.
  defp drive_thread(port, id, ctx, t, w, n) do
    :rand.seed(:exsss, {t, w, 4242})
    {:ok, c} = HranaClient.connect(port, id)
    t0 = System.monotonic_time(:microsecond)

    {c, samples, no_count} =
      Enum.reduce(1..n, {c, [], 0}, fn _, {c, acc, no} ->
        type = random_type()
        {us, {c, status}} = :timer.tc(fn -> run(type, c, ctx) end)
        no2 = if type == :new_order and status in [:committed, :rolled_back], do: no + 1, else: no
        {c, [{type, us} | acc], no2}
      end)

    t1 = System.monotonic_time(:microsecond)
    HranaClient.close(c)
    %{samples: samples, no_count: no_count, t0: t0, t1: t1}
  end

  # tpmC = New-Orders / minute over the concurrent window (max end − min start across threads);
  # per-txn-type percentiles over all threads' samples. String-keyed for the JSON row.
  defp summarize(results, w, threads) do
    window_us = max(1, Enum.max_by(results, & &1.t1).t1 - Enum.min_by(results, & &1.t0).t0)
    no_count = Enum.sum(Enum.map(results, & &1.no_count))
    tpmc = no_count / (window_us / 60_000_000)

    by_type =
      results
      |> Enum.flat_map(& &1.samples)
      |> Enum.group_by(&elem(&1, 0), &elem(&1, 1))

    base = %{"warehouses" => w, "threads" => threads, "tpcc_tpmc" => Float.round(tpmc, 1)}
    Enum.reduce(@txn_types, base, fn type, acc -> Map.merge(acc, pctl_keys(type, by_type)) end)
  end

  defp pctl_keys(type, by_type) do
    samples = Map.get(by_type, type, [])
    prefix = "tpcc_#{name(type)}"

    %{
      "#{prefix}_p50_us" => pctl(samples, 0.50),
      "#{prefix}_p95_us" => pctl(samples, 0.95),
      "#{prefix}_p99_us" => pctl(samples, 0.99),
      "#{prefix}_max_us" =>
        if(samples == [], do: nil, else: Float.round(Enum.max(samples) / 1, 1))
    }
  end

  defp pctl([], _q), do: nil

  defp pctl(samples, q) do
    sorted = Enum.sort(samples)
    n = length(sorted)
    rank = q * (n - 1)
    lo = trunc(rank)
    hi = min(lo + 1, n - 1)
    frac = rank - lo
    Float.round((Enum.at(sorted, lo) * (1 - frac) + Enum.at(sorted, hi) * frac) / 1.0, 1)
  end

  defp name(:new_order), do: "neworder"
  defp name(:payment), do: "payment"
  defp name(:order_status), do: "order_status"
  defp name(:delivery), do: "delivery"
  defp name(:stock_level), do: "stock_level"

  defp uniq(prefix), do: "#{prefix}_#{System.unique_integer([:positive])}"

  defp rm_shard(id) do
    for dir <- [Fathom.Shard.data_dir(), Fathom.Shard.Storage.Local.dir()],
        s <- ["", "-wal", "-shm"] do
      File.rm(Path.join([dir, "#{id}.db"]) <> s)
    end
  end

  # --- internals -----------------------------------------------------------

  # A recursive-CTE integer range `name(var)` over lo..hi (inclusive).
  defp rng(name, var, lo, hi) do
    "#{name}(#{var}) AS (SELECT #{lo} UNION ALL SELECT #{var} + 1 FROM #{name} WHERE #{var} < #{hi})"
  end

  defp with_ctes(ctes), do: "WITH RECURSIVE " <> Enum.join(ctes, ", ") <> " "

  defp pad(n), do: String.pad_leading(Integer.to_string(n), 2, "0")

  defp drop_local(path), do: Enum.each(["", "-wal", "-shm"], &File.rm(path <> &1))
end
