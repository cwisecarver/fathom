"""TPC-C deck for the remote rig driver (Phase 4) — imported by tpc_driver.py.

Ports Fathom.Bench.Tpcc (the loopback bench) to a remote libSQL/Hrana client: the 9-table
schema, the scaled recursive-CTE seed, all five weighted transactions (with value-feeding for
the read-modify-write ones), and the W = 1..max_w sweep. Every transaction is driven
statement-by-statement on a held stream over the real LB, so the per-txn latency is the true
remote client cost. Recorded-only (never a gate).
"""

import random
import threading
import time

TXN_TYPES = ["new_order", "payment", "order_status", "delivery", "stock_level"]
TXN_NAME = {"new_order": "neworder", "payment": "payment", "order_status": "order_status",
            "delivery": "delivery", "stock_level": "stock_level"}


def cardinalities(scale):
    per_district = max(20, round(3000 * scale))
    return {"items": max(50, round(100000 * scale)),
            "per_district": per_district,
            "new_orders": max(5, round(per_district * 0.3)),
            "districts": 10}


# --- schema + seed ---------------------------------------------------------------------------

def schema_ddl():
    dist = ", ".join(f"s_dist_{n:02d} TEXT" for n in range(1, 11))
    return [
        "CREATE TABLE warehouse (w_id INTEGER PRIMARY KEY, w_tax REAL, w_ytd REAL, w_name TEXT, "
        "w_street_1 TEXT, w_street_2 TEXT, w_city TEXT, w_state TEXT, w_zip TEXT)",
        "CREATE TABLE district (d_w_id INTEGER, d_id INTEGER, d_tax REAL, d_ytd REAL, "
        "d_next_o_id INTEGER, d_name TEXT, d_street_1 TEXT, d_street_2 TEXT, d_city TEXT, "
        "d_state TEXT, d_zip TEXT, PRIMARY KEY (d_w_id, d_id))",
        "CREATE TABLE customer (c_w_id INTEGER, c_d_id INTEGER, c_id INTEGER, c_first TEXT, "
        "c_last TEXT, c_discount REAL, c_credit TEXT, c_balance REAL, c_ytd_payment REAL, "
        "c_payment_cnt INTEGER, c_delivery_cnt INTEGER, c_data TEXT, PRIMARY KEY (c_w_id, c_d_id, c_id))",
        "CREATE INDEX idx_customer_last ON customer (c_w_id, c_d_id, c_last, c_first)",
        "CREATE TABLE history (h_c_id INTEGER, h_c_d_id INTEGER, h_c_w_id INTEGER, h_d_id INTEGER, "
        "h_w_id INTEGER, h_date TEXT, h_amount REAL, h_data TEXT)",
        "CREATE TABLE new_order (no_o_id INTEGER, no_d_id INTEGER, no_w_id INTEGER, "
        "PRIMARY KEY (no_w_id, no_d_id, no_o_id))",
        "CREATE TABLE oorder (o_id INTEGER, o_d_id INTEGER, o_w_id INTEGER, o_c_id INTEGER, "
        "o_entry_d TEXT, o_carrier_id INTEGER, o_ol_cnt INTEGER, o_all_local INTEGER, "
        "PRIMARY KEY (o_w_id, o_d_id, o_id))",
        "CREATE INDEX idx_order_cust ON oorder (o_w_id, o_d_id, o_c_id, o_id)",
        "CREATE TABLE order_line (ol_o_id INTEGER, ol_d_id INTEGER, ol_w_id INTEGER, "
        "ol_number INTEGER, ol_i_id INTEGER, ol_supply_w_id INTEGER, ol_delivery_d TEXT, "
        "ol_quantity INTEGER, ol_amount REAL, ol_dist_info TEXT, "
        "PRIMARY KEY (ol_w_id, ol_d_id, ol_o_id, ol_number))",
        "CREATE TABLE item (i_id INTEGER PRIMARY KEY, i_im_id INTEGER, i_name TEXT, i_price REAL, i_data TEXT)",
        "CREATE TABLE stock (s_w_id INTEGER, s_i_id INTEGER, s_quantity INTEGER, s_ytd REAL, "
        "s_order_cnt INTEGER, s_remote_cnt INTEGER, s_data TEXT, " + dist + ", PRIMARY KEY (s_w_id, s_i_id))",
    ]


def _rng(name, var, lo, hi):
    return f"{name}({var}) AS (SELECT {lo} UNION ALL SELECT {var}+1 FROM {name} WHERE {var} < {hi})"


def _ctes(*rs):
    return "WITH RECURSIVE " + ", ".join(rs) + " "


def seed_sql(w, scale):
    c = cardinalities(scale)
    items, p, no = c["items"], c["per_district"], c["new_orders"]
    delivered_hi = p - no
    dist_cols = ", ".join(f"s_dist_{n:02d}" for n in range(1, 11))
    dist_vals = ", ".join("'tpcc-dist-info-24-chars!'" for _ in range(10))
    return [
        _ctes(_rng("iq", "i", 1, items)) +
        "INSERT INTO item (i_id,i_im_id,i_name,i_price,i_data) SELECT i, abs(random()%10000)+1, "
        "'item-'||i, (abs(random()%9900)+100)/100.0, 'idata-'||i FROM iq",
        _ctes(_rng("wq", "w", 1, w)) +
        "INSERT INTO warehouse (w_id,w_tax,w_ytd,w_name,w_street_1,w_street_2,w_city,w_state,w_zip) "
        "SELECT w, 0.1, 300000.0, 'wh-'||w, 's1','s2','city','CA','12345' FROM wq",
        _ctes(_rng("wq", "w", 1, w), _rng("dq", "d", 1, 10)) +
        "INSERT INTO district (d_w_id,d_id,d_tax,d_ytd,d_next_o_id,d_name,d_street_1,d_street_2,"
        f"d_city,d_state,d_zip) SELECT w, d, 0.1, 30000.0, {p + 1}, 'd-'||d, 's1','s2','city','CA','12345' FROM wq, dq",
        _ctes(_rng("wq", "w", 1, w), _rng("dq", "d", 1, 10), _rng("cq", "c", 1, p)) +
        "INSERT INTO customer (c_w_id,c_d_id,c_id,c_first,c_last,c_discount,c_credit,c_balance,"
        "c_ytd_payment,c_payment_cnt,c_delivery_cnt,c_data) SELECT w, d, c, 'first-'||c, 'LAST'||(c%100), "
        "(abs(random()%5000))/10000.0, CASE WHEN c%10=0 THEN 'BC' ELSE 'GC' END, -10.0, 10.0, 1, 0, 'cdata' FROM wq, dq, cq",
        _ctes(_rng("wq", "w", 1, w), _rng("dq", "d", 1, 10), _rng("cq", "c", 1, p)) +
        "INSERT INTO history (h_c_id,h_c_d_id,h_c_w_id,h_d_id,h_w_id,h_date,h_amount,h_data) "
        "SELECT c, d, w, d, w, '2020-01-01 00:00:00', 10.0, 'hdata' FROM wq, dq, cq",
        _ctes(_rng("wq", "w", 1, w), _rng("iq", "i", 1, items)) +
        f"INSERT INTO stock (s_w_id,s_i_id,s_quantity,s_ytd,s_order_cnt,s_remote_cnt,s_data,{dist_cols}) "
        f"SELECT w, i, abs(random()%90)+10, 0.0, 0, 0, 'sdata', {dist_vals} FROM wq, iq",
        _ctes(_rng("wq", "w", 1, w), _rng("dq", "d", 1, 10), _rng("oq", "o", 1, p)) +
        "INSERT INTO oorder (o_id,o_d_id,o_w_id,o_c_id,o_entry_d,o_carrier_id,o_ol_cnt,o_all_local) "
        f"SELECT o, d, w, o, '2020-01-01 00:00:00', CASE WHEN o > {delivered_hi} THEN NULL ELSE abs(random()%10)+1 END, 10, 1 FROM wq, dq, oq",
        _ctes(_rng("wq", "w", 1, w), _rng("dq", "d", 1, 10), _rng("oq", "o", delivered_hi + 1, p)) +
        "INSERT INTO new_order (no_o_id,no_d_id,no_w_id) SELECT o, d, w FROM wq, dq, oq",
        _ctes(_rng("wq", "w", 1, w), _rng("dq", "d", 1, 10), _rng("oq", "o", 1, p), _rng("nq", "n", 1, 10)) +
        "INSERT INTO order_line (ol_o_id,ol_d_id,ol_w_id,ol_number,ol_i_id,ol_supply_w_id,ol_delivery_d,"
        f"ol_quantity,ol_amount,ol_dist_info) SELECT o, d, w, n, abs(random()%{items})+1, w, "
        "'2020-01-01 00:00:00', 5, (abs(random()%1000000))/100.0, 'tpcc-ol-dist-info-24char' FROM wq, dq, oq, nq",
    ]


# --- transactions ----------------------------------------------------------------------------

_NOW = "2020-01-01 00:00:00"


def random_type():
    r = random.randint(1, 100)
    if r <= 45:
        return "new_order"
    if r <= 88:
        return "payment"
    if r <= 92:
        return "order_status"
    if r <= 96:
        return "delivery"
    return "stock_level"


def run_txn(typ, s, ctx):
    return {"new_order": new_order, "payment": payment, "order_status": order_status,
            "delivery": delivery, "stock_level": stock_level}[typ](s, ctx)


def _rw(ctx):
    return random.randint(1, ctx["w_count"])


def _pick_customer(s, ctx, w, d):
    """60% by last name (multi-row → middle by c_first), 40% by id. Returns (c_id, credit)."""
    if random.randint(1, 100) <= 60:
        last = "LAST" + str(random.randint(0, 99))
        rows = s.execute("SELECT c_id, c_credit FROM customer WHERE c_w_id=? AND c_d_id=? AND c_last=? ORDER BY c_first",
                         (w, d, last))
        if rows:
            return tuple(rows[(len(rows) - 1) // 2])
    c_id = random.randint(1, ctx["card"]["per_district"])
    rows = s.execute("SELECT c_id, c_credit FROM customer WHERE c_w_id=? AND c_d_id=? AND c_id=?", (w, d, c_id))
    return tuple(rows[0])


def new_order(s, ctx):
    w = _rw(ctx); d = random.randint(1, 10); c_id = random.randint(1, ctx["card"]["per_district"])
    ol_cnt = random.randint(5, 15)
    rollback = random.randint(1, 100) == 1
    items = ctx["card"]["items"]
    s.execute("BEGIN IMMEDIATE")
    s.execute("SELECT w_tax FROM warehouse WHERE w_id=?", (w,))
    o_id = s.execute("SELECT d_tax, d_next_o_id FROM district WHERE d_w_id=? AND d_id=?", (w, d))[0][1]
    s.execute("UPDATE district SET d_next_o_id=d_next_o_id+1 WHERE d_w_id=? AND d_id=?", (w, d))
    s.execute("SELECT c_discount,c_last,c_credit FROM customer WHERE c_w_id=? AND c_d_id=? AND c_id=?", (w, d, c_id))
    s.execute("INSERT INTO oorder (o_id,o_d_id,o_w_id,o_c_id,o_entry_d,o_ol_cnt,o_all_local) VALUES (?,?,?,?,?,?,1)",
              (o_id, d, w, c_id, _NOW, ol_cnt))
    s.execute("INSERT INTO new_order (no_o_id,no_d_id,no_w_id) VALUES (?,?,?)", (o_id, d, w))
    dist_col = "s_dist_%02d" % d
    for n in range(1, ol_cnt + 1):
        i_id = items + 1 if (rollback and n == ol_cnt) else random.randint(1, items)
        item = s.execute("SELECT i_price,i_name,i_data FROM item WHERE i_id=?", (i_id,))
        if not item:
            s.execute("ROLLBACK")
            return "rolled_back"
        price = item[0][0]
        stock = s.execute(f"SELECT s_quantity,{dist_col},s_data FROM stock WHERE s_w_id=? AND s_i_id=?", (w, i_id))
        s_qty, dist_info = stock[0][0], stock[0][1]
        qty = 5
        new_qty = s_qty - qty if s_qty - qty >= 10 else s_qty - qty + 91
        s.execute("UPDATE stock SET s_quantity=?,s_ytd=s_ytd+?,s_order_cnt=s_order_cnt+1 WHERE s_w_id=? AND s_i_id=?",
                  (new_qty, qty, w, i_id))
        s.execute("INSERT INTO order_line (ol_o_id,ol_d_id,ol_w_id,ol_number,ol_i_id,ol_supply_w_id,"
                  "ol_quantity,ol_amount,ol_dist_info) VALUES (?,?,?,?,?,?,?,?,?)",
                  (o_id, d, w, n, i_id, w, qty, qty * price, dist_info))
    s.execute("COMMIT")
    return "committed"


def payment(s, ctx):
    w = _rw(ctx); d = random.randint(1, 10); amount = (random.randint(1, 500000) + 100) / 100.0
    s.execute("BEGIN IMMEDIATE")
    s.execute("UPDATE warehouse SET w_ytd=w_ytd+? WHERE w_id=?", (amount, w))
    s.execute("SELECT w_name,w_street_1,w_city,w_state,w_zip FROM warehouse WHERE w_id=?", (w,))
    s.execute("UPDATE district SET d_ytd=d_ytd+? WHERE d_w_id=? AND d_id=?", (amount, w, d))
    s.execute("SELECT d_name,d_street_1,d_city,d_state,d_zip FROM district WHERE d_w_id=? AND d_id=?", (w, d))
    c_id, credit = _pick_customer(s, ctx, w, d)
    s.execute("UPDATE customer SET c_balance=c_balance-?,c_ytd_payment=c_ytd_payment+?,c_payment_cnt=c_payment_cnt+1 "
              "WHERE c_w_id=? AND c_d_id=? AND c_id=?", (amount, amount, w, d, c_id))
    if credit == "BC":
        s.execute("UPDATE customer SET c_data=? WHERE c_w_id=? AND c_d_id=? AND c_id=?",
                  (f"{c_id} {d} {w} {amount} |bc-data", w, d, c_id))
    s.execute("INSERT INTO history (h_c_id,h_c_d_id,h_c_w_id,h_d_id,h_w_id,h_date,h_amount,h_data) "
              "VALUES (?,?,?,?,?,?,?,?)", (c_id, d, w, d, w, _NOW, amount, "payment"))
    s.execute("COMMIT")
    return "committed"


def order_status(s, ctx):
    w = _rw(ctx); d = random.randint(1, 10)
    s.execute("BEGIN")
    c_id, _ = _pick_customer(s, ctx, w, d)
    order = s.execute("SELECT o_id,o_entry_d,o_carrier_id FROM oorder WHERE o_w_id=? AND o_d_id=? AND o_c_id=? "
                      "ORDER BY o_id DESC LIMIT 1", (w, d, c_id))
    if order:
        s.execute("SELECT ol_i_id,ol_supply_w_id,ol_quantity,ol_amount,ol_delivery_d FROM order_line "
                  "WHERE ol_w_id=? AND ol_d_id=? AND ol_o_id=?", (w, d, order[0][0]))
    s.execute("COMMIT")
    return "committed"


def delivery(s, ctx):
    w = _rw(ctx); carrier = random.randint(1, 10)
    s.execute("BEGIN IMMEDIATE")
    for d in range(1, 11):
        pending = s.execute("SELECT no_o_id FROM new_order WHERE no_w_id=? AND no_d_id=? ORDER BY no_o_id LIMIT 1", (w, d))
        if not pending:
            continue
        o_id = pending[0][0]
        s.execute("DELETE FROM new_order WHERE no_w_id=? AND no_d_id=? AND no_o_id=?", (w, d, o_id))
        c_id = s.execute("SELECT o_c_id FROM oorder WHERE o_w_id=? AND o_d_id=? AND o_id=?", (w, d, o_id))[0][0]
        s.execute("UPDATE oorder SET o_carrier_id=? WHERE o_w_id=? AND o_d_id=? AND o_id=?", (carrier, w, d, o_id))
        s.execute("UPDATE order_line SET ol_delivery_d=? WHERE ol_w_id=? AND ol_d_id=? AND ol_o_id=?", (_NOW, w, d, o_id))
        amount = s.execute("SELECT COALESCE(SUM(ol_amount),0) FROM order_line WHERE ol_w_id=? AND ol_d_id=? AND ol_o_id=?",
                           (w, d, o_id))[0][0]
        s.execute("UPDATE customer SET c_balance=c_balance+?,c_delivery_cnt=c_delivery_cnt+1 "
                  "WHERE c_w_id=? AND c_d_id=? AND c_id=?", (amount, w, d, c_id))
    s.execute("COMMIT")
    return "committed"


def stock_level(s, ctx):
    w = _rw(ctx); d = random.randint(1, 10); threshold = random.randint(10, 20)
    s.execute("BEGIN")
    next_oid = s.execute("SELECT d_next_o_id FROM district WHERE d_w_id=? AND d_id=?", (w, d))[0][0]
    s.execute("SELECT COUNT(DISTINCT s_i_id) FROM order_line, stock WHERE ol_w_id=? AND ol_d_id=? "
              "AND ol_o_id>=? AND ol_o_id<? AND s_w_id=? AND s_i_id=ol_i_id AND s_quantity<?",
              (w, d, next_oid - 20, next_oid, w, threshold))
    s.execute("COMMIT")
    return "committed"


# --- driver: W-sweep -------------------------------------------------------------------------

def mode_tpcc(args, Stream, pctls, run_threads):
    runid = random.randint(1, 10 ** 9)
    rows = []
    for w in range(1, args.max_w + 1):
        rows.append(_run_one_w(args, Stream, pctls, run_threads, w, runid))
    import sys
    for r in rows:
        print(f"  W={r['warehouses']} threads={r['threads']} tpmC={r['tpcc_tpmc']}  "
              f"neworder p50/p99={r['tpcc_neworder_p50_us']}/{r['tpcc_neworder_p99_us']}µs  "
              f"payment p50/p99={r['tpcc_payment_p50_us']}/{r['tpcc_payment_p99_us']}µs", file=sys.stderr)
    return {"mode": "tpcc", "scale": args.scale, "threads": args.threads, "results": rows}


def _run_one_w(args, Stream, pctls, run_threads, w, runid):
    shard = f"tpcc_w{w}_{runid}"
    setup = Stream(args.lb, args.domain, shard)
    for stmt in schema_ddl():
        setup.execute(stmt)
    for stmt in seed_sql(w, args.scale):
        setup.execute(stmt)
    setup.close()

    ctx = {"w_count": w, "card": cardinalities(args.scale)}
    per_thread = max(1, args.txns // args.threads)
    lock = threading.Lock()
    samples = {t: [] for t in TXN_TYPES}
    counts = {"new_order": 0, "errors": 0}
    spans = []

    def worker(tid):
        random.seed((tid + 1) * (w + 1) * 4242)
        s = Stream(args.lb, args.domain, shard)
        s.execute("SELECT 1")  # warm/open, untimed
        local = {t: [] for t in TXN_TYPES}
        no, err = 0, 0
        t0 = time.perf_counter()
        for _ in range(per_thread):
            typ = random_type()
            a = time.perf_counter()
            try:
                status = run_txn(typ, s, ctx)
                local[typ].append(time.perf_counter() - a)
                if typ == "new_order" and status in ("committed", "rolled_back"):
                    no += 1
            except Exception:
                # Transient (e.g. a 502 while the rebalancer flips this hot shard): reconnect and
                # skip the txn rather than kill the worker — a real client retries the same way.
                err += 1
                s.reconnect()
        t1 = time.perf_counter()
        s.close()
        with lock:
            for t in TXN_TYPES:
                samples[t].extend(local[t])
            counts["new_order"] += no
            counts["errors"] += err
            spans.append((t0, t1))

    run_threads([threading.Thread(target=worker, args=(t,)) for t in range(args.threads)])

    window = max(e for _, e in spans) - min(a for a, _ in spans)
    tpmc = round(counts["new_order"] / (window / 60), 1) if window > 0 else 0.0
    row = {"warehouses": w, "threads": args.threads, "errors": counts["errors"], "tpcc_tpmc": tpmc}
    for t in TXN_TYPES:
        for k, v in pctls(samples[t]).items():
            row[f"tpcc_{TXN_NAME[t]}_{k}"] = v
    return row
