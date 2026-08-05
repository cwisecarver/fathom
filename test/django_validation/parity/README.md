# Django parity harness

The one check in this repo that uses **real Django** rather than a reconstruction of it.

Everything else compares fathom against a vendored copy of Django's `_sqlite_*` functions
(`native/fathom_udf/tests/oracle/`) or against CPython's `re`. Those are fast, offline, and run in
CI — but they are all comparisons against *my model* of Django. This one runs the actual ORM
through `django-libsql` over the Hrana wire and, in the same process, the same ORM through Django's
stock `sqlite3` backend, then diffs the results.

A disagreement here is a query that returns different answers for an unchanged Django app, which is
the whole compatibility claim.

## What it compares

44 probes across the surface where fathom adds behaviour Django's backend supplies client-side:

- `__year` / `__month` / `__day` / `__hour` / `__week_day` / `__quarter` / `__iso_year` / `__date`
- `TruncYear` / `TruncMonth` / `TruncWeek` / `TruncDay` / `TruncHour` / `TruncDate`
- the same truncations **with `tzinfo=`**, i.e. the `USE_TZ = True` default
- `__regex` / `__iregex`, including backreferences, lookahead, and a row whose value ends in a
  newline (the case that used to differ silently)
- `DurationField` arithmetic — `F("span") * 2`, `F("span") + F("span")`, `Sum`
- `Reverse` / `Repeat` / `Length` / `Upper` / `Lower`
- `Count` / `Sum` / `Avg` / `Max` / `StdDev` / `Variance`, and `GROUP BY` over a truncation
- ordering and slicing

## Running it

Needs Python 3.12 (the pins in `../requirements.txt` explain why not 3.13/3.14) and a running
fathom with the Hrana listener up.

```bash
# 1. a 3.12 venv with the pinned Django + django-libsql
uv venv --python 3.12 /tmp/djvenv
uv pip install --python /tmp/djvenv/bin/python -r ../requirements.txt

# 2. fathom, with the Hrana listener
MIX_ENV=dev mix phx.server

# 3. the differential
cd test/django_validation/parity
FATHOM_URL="ws://localhost:8080" /tmp/djvenv/bin/python manage_parity.py
```

Exit 0 and `PARITY: identical on every probe.` is a pass. Any divergence is printed as
`django : …` / `fathom : …` per probe.

`FATHOM_URL` defaults to `ws://localhost:8080`, which routes to the `demo` shard via
`:default_shard`. Point it at the machine's LAN IP if something else already holds
`127.0.0.1:8080` — `django-libsql` rejects URL query parameters, so `?db=` is not available as an
override here.

## Result

**2026-08-05: 44 probes, identical on every one.**

Verified to DISCRIMINATE rather than merely pass: with the `$`-anchor translation in
`native/fathom_udf/src/pyre.rs` deliberately disabled and the extension rebuilt, the same run
reports exactly the three regex probes that depend on it —

```
regex_slug      django : [1, 2, 5]        fathom : [1, 5]
regex_end_a     django : [1, 2, 3, 4]     fathom : [1, 3, 4]
regex_lookahead django : [1, 2, 3, 4, 5]  fathom : [1, 3, 4, 5]
```

Row 2 is the one whose `note` ends in a newline. Python's `$` matches before a trailing newline and
Rust's does not, so a slug validator silently dropped it — no error, just a missing row. That is
what this harness exists to catch.

## Why it is not in CI

It needs a Python 3.12 toolchain, a network install of Django, and a running fathom node with a
Postgres behind it. The generated oracles cover the same ground offline and do run in CI; this is
the periodic end-to-end confirmation that those oracles still model the real thing.

Re-run it when touching `native/fathom_udf/`, when bumping the Django pin, or before claiming
compatibility in anything user-facing.
