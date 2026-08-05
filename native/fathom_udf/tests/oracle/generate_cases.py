#!/usr/bin/env python3
"""Generate ``cases.json`` — a differential-test table produced by running Django's own code.

Every expected value in ``cases.json`` is what Django's real ``_sqlite_*`` function returned for
those exact arguments, including the cases where it raises. The Rust integration test
(``tests/oracle.rs``) replays the table against this crate and fails on any disagreement.

This is the Django-test pattern applied properly: rather than hand-writing expected strings (which
encodes *my* belief about Django's behaviour, and was wrong four times while building this), the
expectations are generated from the reference implementation. Hand-written tests still live in the
module ``#[cfg(test)]`` blocks — they document *why* a behaviour is what it is. This table
establishes *that* it matches.

    cd native/fathom_udf/tests/oracle && python3 generate_cases.py

Regenerate whenever the vendored Django code in ``django_reference.py`` is updated, and commit the
result — CI has no Python/Django dependency and reads the committed file.
"""

import json
import platform
import sys

import django_reference as dj

# Zones chosen for the properties that break implementations, not for variety:
#   UTC                 - the identity case
#   America/New_York    - a DST zone west of UTC (day boundary shifts backwards)
#   Asia/Tokyo          - east of UTC, no DST (day boundary shifts forwards)
#   Asia/Kolkata        - a half-hour offset (+05:30)
#   Pacific/Chatham     - a quarter-hour offset (+12:45) WITH DST
#   Australia/Lord_Howe - a half-hour DST STEP (+10:30 / +11:00)
#   Europe/London       - offset 0 in winter, +1 in summer, so "UTC-equivalent" only half the year
ZONES = [
    None,
    "UTC",
    "America/New_York",
    "Asia/Tokyo",
    "Asia/Kolkata",
    "Pacific/Chatham",
    "Australia/Lord_Howe",
    "Europe/London",
]

# Offset-delta tznames, the shape Django emits for `Trunc(..., tzinfo=...)`.
DELTA_ZONES = ["UTC+05:00", "UTC-05:00", "UTC+05:30", "UTC+5", "America/New_York+02:00"]

DATETIMES = [
    None,
    "",
    "garbage",
    # Plain values.
    "2026-08-05 12:34:56",
    "2026-08-05 12:34:56.123456",
    "2026-08-05 00:00:00",
    "2026-08-05 23:59:59.999999",
    # Near a UTC day boundary, where a tz conversion changes the DATE.
    "2026-08-05 03:30:00",
    "2026-08-04 22:15:00",
    # Year boundary, both directions.
    "2027-01-01 02:00:00",
    "2026-12-31 20:00:00",
    "2026-01-01 00:00:00",
    "2026-12-31 23:59:59",
    # ISO-year edge: 2027-01-01 is in ISO week 53 of ISO year 2026.
    "2027-01-01 12:00:00",
    # Leap day.
    "2024-02-29 12:00:00",
    "2024-02-29 00:00:00",
    # Month/quarter boundaries.
    "2026-03-31 23:00:00",
    "2026-04-01 01:00:00",
    "2026-07-01 00:00:00",
    "2026-10-01 00:00:00",
    # Week boundaries: Sunday, Monday, and a week that spans a month change.
    "2026-08-09 12:00:00",  # Sunday
    "2026-08-03 12:00:00",  # Monday
    "2026-08-01 12:00:00",  # Saturday, week starts in July
    "2026-03-01 12:00:00",
    # US DST transitions in 2026: spring forward Mar 8, fall back Nov 1.
    "2026-03-08 06:30:00",
    "2026-03-08 07:30:00",
    "2026-11-01 05:30:00",
    "2026-11-01 06:30:00",
    # Southern-hemisphere DST (Lord Howe / Chatham) transitions in 2026.
    "2026-04-05 16:00:00",
    "2026-10-04 16:00:00",
    # Bare dates - typecast_timestamp returns a `date`, not a midnight datetime.
    "2026-08-05",
    "2024-02-29",
    "2027-01-01",
    # An embedded offset, which typecast_timestamp DISCARDS.
    "2005-07-29 15:48:00.590358-05",
    "2005-07-29 09:56:00+02:00",
    # Fractional-second padding/truncation.
    "2026-08-05 12:00:00.5",
    "2026-08-05 12:00:00.1234567",
]

TIMES = [
    None,
    "",
    "garbage",
    "00:00:00",
    "12:00:00",
    "12:34:56",
    "12:34:56.000001",
    "12:34:56.5",
    "23:59:59.999999",
    "01:02:03",
]

DATE_TRUNC_LOOKUPS = ["year", "quarter", "month", "week", "day", "fortnight"]
DATETIME_TRUNC_LOOKUPS = [
    "year",
    "quarter",
    "month",
    "week",
    "day",
    "hour",
    "minute",
    "second",
    "fortnight",
]
TIME_TRUNC_LOOKUPS = ["hour", "minute", "second", "fortnight"]
EXTRACT_LOOKUPS = [
    "year",
    "month",
    "day",
    "hour",
    "minute",
    "second",
    "microsecond",
    "week_day",
    "iso_week_day",
    "week",
    "quarter",
    "iso_year",
    "nonsense",
]
TIME_EXTRACT_LOOKUPS = ["hour", "minute", "second", "microsecond", "nonsense"]

TEXTS = [None, "", "x", "abc", "abcdef", "é", "héllo", "a🙂b", "日本語", "  padded  "]
PAD_LENGTHS = [-1, 0, 1, 3, 5, 10]
FILLS = [None, "", "0", "-", "ab", "é"]


INT64_MIN = -(2**63)
INT64_MAX = 2**63 - 1


def call(fn, *args):
    """Run a reference function, classifying the outcome the way the Rust side reports it.

    Python integers are unbounded but SQLite's INTEGER is 64-bit, so a value Django computes
    happily can still fail on its way into the database — ``sqlite3`` raises ``OverflowError:
    Python int too large to convert to SQLite INTEGER``. ``django_format_dtdelta('*', ...)`` on two
    large microsecond counts reaches that easily. Classifying it as an error here is what makes the
    Rust side's ``checked_mul`` the correct behaviour rather than a silent wrap.
    """
    try:
        value = fn(*args)
    except Exception as exc:  # noqa: BLE001 - the classification IS the point
        return {"error": type(exc).__name__}
    if value is None:
        return {"null": True}
    if isinstance(value, bool):
        return {"int": int(value)}
    if isinstance(value, int):
        if not (INT64_MIN <= value <= INT64_MAX):
            return {"error": "OverflowError"}
        return {"int": value}
    if isinstance(value, float):
        return {"float": value}
    return {"text": str(value)}


def main():
    cases = []

    def add(func, args, result):
        cases.append({"func": func, "args": list(args), "result": result})

    # ---- django_date_trunc / django_datetime_trunc ------------------------------------------
    for lookup in DATE_TRUNC_LOOKUPS:
        for dt in DATETIMES:
            for tz, conn_tz in [(None, None), ("UTC", "UTC"), ("America/New_York", "UTC")]:
                add(
                    "django_date_trunc",
                    (lookup, dt, tz, conn_tz),
                    call(dj._sqlite_date_trunc, lookup, dt, tz, conn_tz),
                )

    for lookup in DATETIME_TRUNC_LOOKUPS:
        for dt in DATETIMES:
            for tz in ZONES:
                conn_tz = None if tz is None else "UTC"
                add(
                    "django_datetime_trunc",
                    (lookup, dt, tz, conn_tz),
                    call(dj._sqlite_datetime_trunc, lookup, dt, tz, conn_tz),
                )

    # ---- django_time_trunc -------------------------------------------------------------------
    for lookup in TIME_TRUNC_LOOKUPS:
        for value in TIMES + DATETIMES:
            for tz, conn_tz in [(None, None), ("America/New_York", "UTC")]:
                add(
                    "django_time_trunc",
                    (lookup, value, tz, conn_tz),
                    call(dj._sqlite_time_trunc, lookup, value, tz, conn_tz),
                )

    # ---- extracts ----------------------------------------------------------------------------
    for lookup in EXTRACT_LOOKUPS:
        for dt in DATETIMES:
            add(
                "django_date_extract",
                (lookup, dt),
                call(dj._sqlite_datetime_extract, lookup, dt),
            )
            for tz in ZONES:
                conn_tz = None if tz is None else "UTC"
                add(
                    "django_datetime_extract",
                    (lookup, dt, tz, conn_tz),
                    call(dj._sqlite_datetime_extract, lookup, dt, tz, conn_tz),
                )

    for lookup in TIME_EXTRACT_LOOKUPS:
        for value in TIMES:
            add(
                "django_time_extract",
                (lookup, value),
                call(dj._sqlite_time_extract, lookup, value),
            )

    # ---- offset-delta tznames ----------------------------------------------------------------
    for tz in DELTA_ZONES:
        for dt in DATETIMES:
            add(
                "django_datetime_cast_date",
                (dt, tz, "UTC"),
                call(dj._sqlite_datetime_cast_date, dt, tz, "UTC"),
            )
            add(
                "django_datetime_cast_time",
                (dt, tz, "UTC"),
                call(dj._sqlite_datetime_cast_time, dt, tz, "UTC"),
            )

    # ---- casts over the full zone matrix -----------------------------------------------------
    for dt in DATETIMES:
        for tz in ZONES:
            conn_tz = None if tz is None else "UTC"
            add(
                "django_datetime_cast_date",
                (dt, tz, conn_tz),
                call(dj._sqlite_datetime_cast_date, dt, tz, conn_tz),
            )
            add(
                "django_datetime_cast_time",
                (dt, tz, conn_tz),
                call(dj._sqlite_datetime_cast_time, dt, tz, conn_tz),
            )
        # Conversions OUT of a zone as well as into one - the fold/gap cases live here.
        for conn_tz in ["America/New_York", "Australia/Lord_Howe", "Pacific/Chatham"]:
            add(
                "django_datetime_cast_time",
                (dt, "UTC", conn_tz),
                call(dj._sqlite_datetime_cast_time, dt, "UTC", conn_tz),
            )
            add(
                "django_datetime_cast_date",
                (dt, "UTC", conn_tz),
                call(dj._sqlite_datetime_cast_date, dt, "UTC", conn_tz),
            )

    # ---- diffs -------------------------------------------------------------------------------
    for lhs in TIMES:
        for rhs in TIMES:
            add(
                "django_time_diff",
                (lhs, rhs),
                call(dj._sqlite_time_diff, lhs, rhs),
            )

    for lhs in DATETIMES:
        for rhs in DATETIMES:
            add(
                "django_timestamp_diff",
                (lhs, rhs),
                call(dj._sqlite_timestamp_diff, lhs, rhs),
            )

    # ---- django_format_dtdelta ---------------------------------------------------------------
    DELTA_INTS = [0, 1, 2, 3, 4, -1, 500, 1_000_000, 3_600_000_000, 86_400_000_000, -86_400_000_001]
    for connector in ["+", "-", " + ", " - ", "*", "/"]:
        for lhs in DELTA_INTS:
            for rhs in DELTA_INTS:
                add(
                    "django_format_dtdelta",
                    (connector, lhs, rhs),
                    call(dj._sqlite_format_dtdelta, connector, lhs, rhs),
                )
    # datetime/date operands (only meaningful for + and -)
    DT_OPERANDS = ["2026-08-05 12:00:00", "2026-08-05 12:00:00.500000", "2026-08-05"]
    for connector in ["+", "-"]:
        for lhs in DT_OPERANDS:
            for rhs in DELTA_INTS:
                add(
                    "django_format_dtdelta",
                    (connector, lhs, rhs),
                    call(dj._sqlite_format_dtdelta, connector, lhs, rhs),
                )
            for rhs in DT_OPERANDS:
                add(
                    "django_format_dtdelta",
                    (connector, lhs, rhs),
                    call(dj._sqlite_format_dtdelta, connector, lhs, rhs),
                )
    # NULL propagation
    for connector in [None, "+"]:
        for lhs in [None, 1]:
            for rhs in [None, 1]:
                add(
                    "django_format_dtdelta",
                    (connector, lhs, rhs),
                    call(dj._sqlite_format_dtdelta, connector, lhs, rhs),
                )

    # ---- text --------------------------------------------------------------------------------
    for text in TEXTS:
        for length in PAD_LENGTHS:
            for fill in FILLS:
                add("LPAD", (text, length, fill), call(dj._sqlite_lpad, text, length, fill))
                add("RPAD", (text, length, fill), call(dj._sqlite_rpad, text, length, fill))
        for count in [-1, 0, 1, 2, 3]:
            add("REPEAT", (text, count), call(dj._sqlite_repeat, text, count))
        add("REVERSE", (text,), call(dj._sqlite_reverse, text))

    # ---- hashes ------------------------------------------------------------------------------
    HASH_INPUTS = TEXTS + ["the quick brown fox", "0", "\n", "🙂" * 10]
    for text in HASH_INPUTS:
        for name, fn in [
            ("MD5", dj._sqlite_md5),
            ("SHA1", dj._sqlite_sha1),
            ("SHA224", dj._sqlite_sha224),
            ("SHA256", dj._sqlite_sha256),
            ("SHA384", dj._sqlite_sha384),
            ("SHA512", dj._sqlite_sha512),
        ]:
            add(name, (text,), call(fn, text))

    # ---- bitxor ------------------------------------------------------------------------------
    for x in [None, 0, 1, 2, 3, 5, -1, 255, 2**40]:
        for y in [None, 0, 1, 2, 3, 5, -1, 255, 2**40]:
            add("BITXOR", (x, y), call(dj._sqlite_bitxor, x, y))

    # JSONL rather than one big JSON array: 1.4 MB of cases is ~69 KB once git compresses it, and
    # line-per-case means a regeneration produces a diff a human can actually read — a changed
    # expectation shows up as one changed line instead of a wholesale rewrite.
    meta = {
        "_meta": True,
        "generated_by": "native/fathom_udf/tests/oracle/generate_cases.py",
        "source": "Django _functions.py (stable/5.2.x + main), vendored in django_reference.py",
        "python": platform.python_version(),
        "count": len(cases),
    }

    with open("cases.jsonl", "w", encoding="utf-8") as fh:
        fh.write(json.dumps(meta, separators=(",", ":"), ensure_ascii=False) + "\n")
        for case in cases:
            fh.write(json.dumps(case, separators=(",", ":"), ensure_ascii=False) + "\n")

    print(f"wrote cases.jsonl with {len(cases)} cases (python {platform.python_version()})")
    return 0


if __name__ == "__main__":
    sys.exit(main())
