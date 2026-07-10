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

  alias Fathom.Shard.{Connection, Storage}

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

  # --- internals -----------------------------------------------------------

  # A recursive-CTE integer range `name(var)` over lo..hi (inclusive).
  defp rng(name, var, lo, hi) do
    "#{name}(#{var}) AS (SELECT #{lo} UNION ALL SELECT #{var} + 1 FROM #{name} WHERE #{var} < #{hi})"
  end

  defp with_ctes(ctes), do: "WITH RECURSIVE " <> Enum.join(ctes, ", ") <> " "

  defp pad(n), do: String.pad_leading(Integer.to_string(n), 2, "0")

  defp drop_local(path), do: Enum.each(["", "-wal", "-shm"], &File.rm(path <> &1))
end
