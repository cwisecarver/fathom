#!/usr/bin/env python3
"""Generate `sql_cases.jsonl` — full-query results from **Django's actual backend**.

The other two oracles compare functions in isolation. This one opens a real `sqlite3` connection,
registers Django's UDFs on it exactly the way `django/db/backends/sqlite3/base.py` does, and runs
whole queries. That is Django's SQLite backend at the SQL level, so a disagreement here is a query
returning different rows or different TYPES on fathom than on Django.

It covers what per-function comparison structurally cannot:

  * **result types.** `django_format_dtdelta('*',3,4)` returning `'12'` (text) instead of `12`
    (integer) makes `WHERE duration * 2 = 12` stop matching, with no error. Every cell here carries
    SQLite's own `typeof()`.
  * **row filtering.** A UDF in a `WHERE` clause decides which rows come back — the actual
    user-visible behaviour, not the function's return value.
  * **NULL columns.** Aggregates over a nullable column, which is where fathom deliberately
    diverges (documented below) and where every other divergence would be accidental.
  * **GROUP BY / ORDER BY** over UDF results.

    cd native/fathom_udf/tests/oracle && python3 generate_sql_cases.py

Note the two KNOWN, deliberate divergences, recorded here rather than hidden: Django's aggregate
step is `list.append`, so `STDDEV_POP` over a column containing NULL raises `TypeError` and
`VAR_SAMP` over a single row raises `StatisticsError`. fathom skips NULLs and returns NULL for a
degenerate group (matching PostgreSQL). Those cases are marked `divergence_expected` so the replay
asserts the *intended* difference instead of silently tolerating any difference.
"""

import json
import platform
import sqlite3
import sys

import django_reference as dj


def register(conn):
    """Exactly what django/db/backends/sqlite3/base.py registers, in the same order."""
    d = lambda name, n, fn: conn.create_function(name, n, fn, deterministic=True)  # noqa: E731

    d("django_date_extract", 2, dj._sqlite_datetime_extract)
    d("django_date_trunc", 4, dj._sqlite_date_trunc)
    d("django_datetime_cast_date", 3, dj._sqlite_datetime_cast_date)
    d("django_datetime_cast_time", 3, dj._sqlite_datetime_cast_time)
    d("django_datetime_extract", 4, dj._sqlite_datetime_extract)
    d("django_datetime_trunc", 4, dj._sqlite_datetime_trunc)
    d("django_time_extract", 2, dj._sqlite_time_extract)
    d("django_time_trunc", 4, dj._sqlite_time_trunc)
    d("django_time_diff", 2, dj._sqlite_time_diff)
    d("django_timestamp_diff", 2, dj._sqlite_timestamp_diff)
    d("django_format_dtdelta", 3, dj._sqlite_format_dtdelta)
    d("regexp", 2, lambda p, s: bool(__import__("re").search(p, s)) if p is not None and s is not None else None)
    d("BITXOR", 2, dj._sqlite_bitxor)
    d("LPAD", 3, dj._sqlite_lpad)
    d("MD5", 1, dj._sqlite_md5)
    d("REPEAT", 2, dj._sqlite_repeat)
    d("REVERSE", 1, dj._sqlite_reverse)
    d("RPAD", 3, dj._sqlite_rpad)
    d("SHA1", 1, dj._sqlite_sha1)
    d("SHA224", 1, dj._sqlite_sha224)
    d("SHA256", 1, dj._sqlite_sha256)
    d("SHA384", 1, dj._sqlite_sha384)
    d("SHA512", 1, dj._sqlite_sha512)


SCHEMA = [
    """CREATE TABLE app_order (
        id INTEGER PRIMARY KEY,
        created TEXT,
        due DATE,
        at TIME,
        qty INTEGER,
        price REAL,
        note TEXT,
        span INTEGER
    )""",
    # Row values chosen to hit the edges the function oracles found: a trailing-newline note (the
    # `$` divergence), a NULL in every nullable column, a bare DATE alongside DATETIMEs, unicode.
    """INSERT INTO app_order (id, created, due, at, qty, price, note, span) VALUES
        (1, '2026-08-05 13:45:59.123456', '2026-08-05', '13:45:59', 3, 1.5, 'alpha',      3600000000),
        (2, '2026-08-05 03:30:00',        '2026-08-04', '03:30:00', 0, 0.0, 'beta\n',     1),
        (3, '2027-01-01 02:00:00',        '2027-01-01', '02:00:00', 7, 2.25, 'Gamma',     86400000000),
        (4, '2024-02-29 12:00:00',        '2024-02-29', '12:00:00', 2, 9.99, 'délta',     -1),
        (5, NULL,                          NULL,         NULL,      NULL, NULL, NULL,     NULL),
        (6, '2026-12-31 23:59:59',        '2026-12-31', '23:59:59', 4, 4.0, 'a-slug-1',   0)
    """,
]

# Each case is a full query. `typeof()` is selected alongside every computed value, because a
# right-looking value of the wrong type is the failure mode that does not raise.
QUERIES = [
    # --- extracts, the __year / __month / __day lookups -------------------------------------
    "SELECT id, django_date_extract('year', due), typeof(django_date_extract('year', due)) FROM app_order ORDER BY id",
    "SELECT id, django_datetime_extract('hour', created, NULL, NULL) FROM app_order ORDER BY id",
    "SELECT id, django_datetime_extract('week_day', created, NULL, NULL) FROM app_order ORDER BY id",
    "SELECT id, django_datetime_extract('quarter', created, NULL, NULL) FROM app_order ORDER BY id",
    "SELECT id, django_datetime_extract('iso_year', created, NULL, NULL) FROM app_order ORDER BY id",
    "SELECT id, django_time_extract('minute', at) FROM app_order ORDER BY id",
    # --- truncation, the Trunc* / __date lookups ---------------------------------------------
    "SELECT id, django_date_trunc('month', due, NULL, NULL) FROM app_order ORDER BY id",
    "SELECT id, django_date_trunc('week', due, NULL, NULL) FROM app_order ORDER BY id",
    "SELECT id, django_datetime_trunc('day', created, NULL, NULL) FROM app_order ORDER BY id",
    "SELECT id, django_datetime_trunc('hour', created, NULL, NULL) FROM app_order ORDER BY id",
    "SELECT id, django_datetime_cast_date(created, NULL, NULL) FROM app_order ORDER BY id",
    "SELECT id, django_datetime_cast_time(created, NULL, NULL) FROM app_order ORDER BY id",
    "SELECT id, django_time_trunc('hour', at, NULL, NULL) FROM app_order ORDER BY id",
    # --- timezone-aware, the USE_TZ=True default ----------------------------------------------
    "SELECT id, django_datetime_trunc('day', created, 'America/New_York', 'UTC') FROM app_order ORDER BY id",
    "SELECT id, django_datetime_cast_date(created, 'America/New_York', 'UTC') FROM app_order ORDER BY id",
    "SELECT id, django_datetime_extract('year', created, 'Asia/Tokyo', 'UTC') FROM app_order ORDER BY id",
    "SELECT id, django_datetime_cast_time(created, 'Asia/Kolkata', 'UTC') FROM app_order ORDER BY id",
    # --- WHERE: which ROWS come back, the user-visible thing ----------------------------------
    "SELECT id FROM app_order WHERE django_date_extract('year', due) = 2026 ORDER BY id",
    "SELECT id FROM app_order WHERE django_datetime_cast_date(created, NULL, NULL) = '2026-08-05' ORDER BY id",
    "SELECT id FROM app_order WHERE django_date_trunc('month', due, NULL, NULL) = '2026-08-01' ORDER BY id",
    # --- GROUP BY / ORDER BY over UDF results -------------------------------------------------
    "SELECT django_date_extract('year', due) AS y, COUNT(*) FROM app_order GROUP BY y ORDER BY y",
    "SELECT django_date_trunc('month', due, NULL, NULL) AS m, COUNT(*), SUM(qty) FROM app_order GROUP BY m ORDER BY m",
    "SELECT id FROM app_order ORDER BY django_datetime_cast_time(created, NULL, NULL), id",
    # --- duration arithmetic: the TYPE bug lives here ------------------------------------------
    "SELECT id, django_format_dtdelta('+', span, span), typeof(django_format_dtdelta('+', span, span)) FROM app_order ORDER BY id",
    "SELECT id, django_format_dtdelta('-', span, 1), typeof(django_format_dtdelta('-', span, 1)) FROM app_order ORDER BY id",
    "SELECT id, django_format_dtdelta('*', qty, 2), typeof(django_format_dtdelta('*', qty, 2)) FROM app_order ORDER BY id",
    "SELECT id, django_format_dtdelta('/', qty, 2), typeof(django_format_dtdelta('/', qty, 2)) FROM app_order ORDER BY id",
    "SELECT id FROM app_order WHERE django_format_dtdelta('*', qty, 2) = 6 ORDER BY id",
    "SELECT id, django_time_diff(at, '00:00:00'), typeof(django_time_diff(at, '00:00:00')) FROM app_order WHERE at IS NOT NULL ORDER BY id",
    "SELECT id, django_timestamp_diff(created, '2026-01-01 00:00:00') FROM app_order WHERE created IS NOT NULL ORDER BY id",
    # --- regexp, incl. the trailing-newline row -----------------------------------------------
    "SELECT id FROM app_order WHERE note REGEXP '^[a-z]' ORDER BY id",
    "SELECT id FROM app_order WHERE note REGEXP 'a$' ORDER BY id",
    "SELECT id FROM app_order WHERE note REGEXP '^[a-z0-9-]+$' ORDER BY id",
    "SELECT id FROM app_order WHERE note REGEXP '(?i)GAMMA' ORDER BY id",
    "SELECT id FROM app_order WHERE note REGEXP '(.)\\1' ORDER BY id",
    "SELECT id FROM app_order WHERE note REGEXP '^(?=.*a).*$' ORDER BY id",
    "SELECT id, note REGEXP 'a', typeof(note REGEXP 'a') FROM app_order ORDER BY id",
    # --- text functions -------------------------------------------------------------------------
    "SELECT id, LPAD(note, 8, '.'), RPAD(note, 8, '.') FROM app_order ORDER BY id",
    "SELECT id, REVERSE(note), REPEAT(note, 2) FROM app_order ORDER BY id",
    "SELECT id, MD5(note), SHA1(note), SHA256(note) FROM app_order ORDER BY id",
    "SELECT id, BITXOR(qty, 3), typeof(BITXOR(qty, 3)) FROM app_order ORDER BY id",
    # --- aggregates over a column that CONTAINS a NULL (the deliberate divergence) -------------
    "SELECT COUNT(*), SUM(qty), AVG(price) FROM app_order",
]

# Queries whose Django behaviour is a RAISE that fathom deliberately does not reproduce. Recorded
# separately so the replay asserts fathom's intended answer rather than tolerating anything.
DIVERGENT = [
    {
        "sql": "SELECT STDDEV_POP(qty) FROM app_order",
        "why": "Django's ListAggregate appends None and statistics.pstdev raises TypeError; fathom skips NULLs",
        "fathom_expects": "a number",
    },
    {
        "sql": "SELECT VAR_SAMP(qty) FROM app_order WHERE id = 1",
        "why": "statistics.variance raises StatisticsError on one value; fathom returns NULL (as PostgreSQL does)",
        "fathom_expects": "NULL",
    },
]


def cell(v):
    if v is None:
        return {"null": True}
    if isinstance(v, bool):
        return {"int": int(v)}
    if isinstance(v, int):
        return {"int": v}
    if isinstance(v, float):
        return {"float": v}
    if isinstance(v, bytes):
        return {"blob": list(v)}
    return {"text": str(v)}


def main():
    conn = sqlite3.connect(":memory:")
    register(conn)
    for stmt in SCHEMA:
        conn.execute(stmt)
    conn.commit()

    cases = []
    for sql in QUERIES:
        try:
            rows = [[cell(v) for v in row] for row in conn.execute(sql).fetchall()]
            cases.append({"sql": sql, "result": {"rows": rows}})
        except Exception as exc:  # noqa: BLE001 - classification IS the point
            cases.append({"sql": sql, "result": {"error": type(exc).__name__}})

    for entry in DIVERGENT:
        try:
            conn.execute(entry["sql"]).fetchall()
            outcome = "ok"
        except Exception as exc:  # noqa: BLE001
            outcome = type(exc).__name__
        entry["django_outcome"] = outcome

    meta = {
        "_meta": True,
        "generated_by": "native/fathom_udf/tests/oracle/generate_sql_cases.py",
        "source": "python sqlite3 with Django's UDFs registered",
        "python": platform.python_version(),
        "sqlite": sqlite3.sqlite_version,
        "count": len(cases),
        "divergences": DIVERGENT,
    }

    with open("sql_cases.jsonl", "w", encoding="utf-8") as fh:
        fh.write(json.dumps(meta, separators=(",", ":"), ensure_ascii=False) + "\n")
        for case in cases:
            fh.write(json.dumps(case, separators=(",", ":"), ensure_ascii=False) + "\n")

    errs = sum(1 for c in cases if "error" in c["result"])
    print(
        f"wrote sql_cases.jsonl: {len(cases)} queries ({errs} that Django itself errors on), "
        f"sqlite {sqlite3.sqlite_version}, python {platform.python_version()}"
    )
    for e in DIVERGENT:
        print(f"  known divergence: {e['sql']} -> django={e['django_outcome']}, fathom={e['fathom_expects']}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
