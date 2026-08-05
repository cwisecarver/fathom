#!/usr/bin/env python3
"""End-to-end Django parity: the SAME ORM queries against fathom and against Django's own SQLite.

This is the one test that uses real Django rather than a reconstruction of it. Everything else in
the repo compares fathom against a vendored copy of Django's functions or against CPython's `re`;
this drives the actual ORM through `django-libsql` over the Hrana wire, and the same ORM through
Django's stock `sqlite3` backend, and diffs the results.

Any disagreement is a query that returns different answers for an unchanged Django app — which is
exactly the compatibility claim.

    python manage_parity.py            # run the differential
"""

import os
import sys
import django
from django.conf import settings

FATHOM_URL = os.environ.get("FATHOM_URL", "ws://localhost:8080")
LOCAL_DB = os.environ.get("LOCAL_DB", "/tmp/django_parity_local.sqlite3")

settings.configure(
    DEBUG=False,
    USE_TZ=True,
    TIME_ZONE="UTC",
    SECRET_KEY="parity",
    INSTALLED_APPS=["parityapp"],
    DATABASES={
        # Django's own backend — the reference.
        "default": {"ENGINE": "django.db.backends.sqlite3", "NAME": LOCAL_DB},
        # fathom, over Hrana, through the unchanged django-libsql backend.
        "fathom": {"ENGINE": "libsql.db.backends.sqlite3", "NAME": FATHOM_URL},
    },
    DEFAULT_AUTO_FIELD="django.db.models.AutoField",
)

django.setup()

from django.db import connections  # noqa: E402
from django.db.models import (  # noqa: E402
    Avg,
    Count,
    DurationField,
    ExpressionWrapper,
    F,
    Max,
    StdDev,
    Sum,
    Variance,
)
from django.db.models.functions import (  # noqa: E402
    Extract,
    Length,
    Lower,
    Repeat,
    Reverse,
    TruncDate,
    TruncDay,
    TruncHour,
    TruncMonth,
    TruncWeek,
    TruncYear,
    Upper,
)
from parityapp.models import Order  # noqa: E402

import datetime as dt  # noqa: E402
from zoneinfo import ZoneInfo  # noqa: E402

UTC = ZoneInfo("UTC")

ROWS = [
    # created (aware), due (date), at (time), qty, price, note, span (duration)
    (dt.datetime(2026, 8, 5, 13, 45, 59, 123456, tzinfo=UTC), dt.date(2026, 8, 5),
     dt.time(13, 45, 59), 3, 1.5, "alpha", dt.timedelta(hours=1)),
    (dt.datetime(2026, 8, 5, 3, 30, tzinfo=UTC), dt.date(2026, 8, 4),
     dt.time(3, 30), 0, 0.0, "beta\n", dt.timedelta(microseconds=1)),
    (dt.datetime(2027, 1, 1, 2, 0, tzinfo=UTC), dt.date(2027, 1, 1),
     dt.time(2, 0), 7, 2.25, "Gamma", dt.timedelta(days=1)),
    (dt.datetime(2024, 2, 29, 12, 0, tzinfo=UTC), dt.date(2024, 2, 29),
     dt.time(12, 0), 2, 9.99, "délta", dt.timedelta(microseconds=-1)),
    (dt.datetime(2026, 12, 31, 23, 59, 59, tzinfo=UTC), dt.date(2026, 12, 31),
     dt.time(23, 59, 59), 4, 4.0, "a-slug-1", dt.timedelta(0)),
]


def seed(alias):
    Order.objects.using(alias).all().delete()
    for i, (created, due, at, qty, price, note, span) in enumerate(ROWS, start=1):
        Order.objects.using(alias).create(
            id=i, created=created, due=due, at=at, qty=qty, price=price, note=note, span=span
        )


def probes(alias):
    """Every probe returns a plain, comparable Python value."""
    qs = lambda: Order.objects.using(alias)  # noqa: E731
    ny = "America/New_York"

    out = {}

    # --- date/time lookups: the __year / __month / __day family -------------------------------
    out["year=2026"] = sorted(qs().filter(created__year=2026).values_list("id", flat=True))
    out["month=8"] = sorted(qs().filter(created__month=8).values_list("id", flat=True))
    out["day=5"] = sorted(qs().filter(created__day=5).values_list("id", flat=True))
    out["hour=13"] = sorted(qs().filter(created__hour=13).values_list("id", flat=True))
    out["week_day=4"] = sorted(qs().filter(created__week_day=4).values_list("id", flat=True))
    out["quarter=3"] = sorted(qs().filter(created__quarter=3).values_list("id", flat=True))
    out["iso_year"] = sorted(qs().filter(created__iso_year=2026).values_list("id", flat=True))
    out["date="] = sorted(
        qs().filter(created__date=dt.date(2026, 8, 5)).values_list("id", flat=True)
    )
    out["due__year"] = sorted(qs().filter(due__year=2026).values_list("id", flat=True))
    out["at__hour"] = sorted(qs().filter(at__hour=13).values_list("id", flat=True))

    # --- Trunc*, the time-series shape --------------------------------------------------------
    out["TruncYear"] = sorted(
        (i, str(v)) for i, v in qs().annotate(v=TruncYear("created")).values_list("id", "v")
    )
    out["TruncMonth"] = sorted(
        (i, str(v)) for i, v in qs().annotate(v=TruncMonth("created")).values_list("id", "v")
    )
    out["TruncWeek"] = sorted(
        (i, str(v)) for i, v in qs().annotate(v=TruncWeek("created")).values_list("id", "v")
    )
    out["TruncDay"] = sorted(
        (i, str(v)) for i, v in qs().annotate(v=TruncDay("created")).values_list("id", "v")
    )
    out["TruncHour"] = sorted(
        (i, str(v)) for i, v in qs().annotate(v=TruncHour("created")).values_list("id", "v")
    )
    out["TruncDate"] = sorted(
        (i, str(v)) for i, v in qs().annotate(v=TruncDate("created")).values_list("id", "v")
    )

    # --- timezone-aware truncation (USE_TZ=True, the default) ---------------------------------
    out["TruncDay_NY"] = sorted(
        (i, str(v))
        for i, v in qs().annotate(v=TruncDay("created", tzinfo=ZoneInfo(ny))).values_list("id", "v")
    )
    out["TruncDate_NY"] = sorted(
        (i, str(v))
        for i, v in qs().annotate(v=TruncDate("created", tzinfo=ZoneInfo(ny))).values_list("id", "v")
    )
    out["Extract_year_NY"] = sorted(
        (i, v)
        for i, v in qs()
        .annotate(v=Extract("created", "year", tzinfo=ZoneInfo(ny)))
        .values_list("id", "v")
    )

    # --- regex, incl. the trailing-newline row -------------------------------------------------
    out["regex_slug"] = sorted(qs().filter(note__regex=r"^[a-z0-9-]+$").values_list("id", flat=True))
    out["regex_start"] = sorted(qs().filter(note__regex=r"^[a-z]").values_list("id", flat=True))
    out["regex_end_a"] = sorted(qs().filter(note__regex=r"a$").values_list("id", flat=True))
    out["iregex"] = sorted(qs().filter(note__iregex=r"GAMMA").values_list("id", flat=True))
    out["regex_backref"] = sorted(qs().filter(note__regex=r"(.)\1").values_list("id", flat=True))
    out["regex_lookahead"] = sorted(
        qs().filter(note__regex=r"^(?=.*a).*$").values_list("id", flat=True)
    )

    # --- duration arithmetic on a DurationField ------------------------------------------------
    out["span_gt"] = sorted(
        qs().filter(span__gt=dt.timedelta(0)).values_list("id", flat=True)
    )
    out["span_sum"] = str(qs().aggregate(s=Sum("span"))["s"])
    doubled = ExpressionWrapper(F("span") * 2, output_field=DurationField())
    out["span_doubled"] = sorted(
        (i, str(v)) for i, v in qs().annotate(v=doubled).values_list("id", "v")
    )
    out["span_plus"] = sorted(
        (i, str(v))
        for i, v in qs()
        .annotate(v=ExpressionWrapper(F("span") + F("span"), output_field=DurationField()))
        .values_list("id", "v")
    )

    # --- text functions -------------------------------------------------------------------------
    out["Reverse"] = sorted(
        (i, v) for i, v in qs().annotate(v=Reverse("note")).values_list("id", "v")
    )
    out["Repeat"] = sorted(
        (i, v) for i, v in qs().annotate(v=Repeat("note", 2)).values_list("id", "v")
    )
    out["Length"] = sorted((i, v) for i, v in qs().annotate(v=Length("note")).values_list("id", "v"))
    out["Upper"] = sorted((i, v) for i, v in qs().annotate(v=Upper("note")).values_list("id", "v"))
    out["Lower"] = sorted((i, v) for i, v in qs().annotate(v=Lower("note")).values_list("id", "v"))

    # --- aggregates ------------------------------------------------------------------------------
    out["count"] = qs().count()
    out["sum_qty"] = qs().aggregate(v=Sum("qty"))["v"]
    out["avg_price"] = round(qs().aggregate(v=Avg("price"))["v"], 9)
    out["max_created"] = str(qs().aggregate(v=Max("created"))["v"])
    out["stddev"] = round(qs().aggregate(v=StdDev("qty"))["v"], 9)
    out["variance"] = round(qs().aggregate(v=Variance("qty"))["v"], 9)
    out["group_by_year"] = sorted(
        (str(r["y"]), r["n"])
        for r in qs().annotate(y=TruncYear("created")).values("y").annotate(n=Count("id"))
    )

    # --- ordering / slicing -----------------------------------------------------------------------
    out["order_by_note"] = list(qs().order_by("note").values_list("id", flat=True))
    out["order_by_-created"] = list(qs().order_by("-created").values_list("id", flat=True))
    out["slice"] = list(qs().order_by("id").values_list("id", flat=True)[1:3])

    return out


def main():
    from django.core.management import call_command

    for alias in ("default", "fathom"):
        call_command("migrate", database=alias, run_syncdb=True, verbosity=0)
        seed(alias)

    ref = probes("default")
    got = probes("fathom")

    keys = sorted(set(ref) | set(got))
    diffs = []
    for k in keys:
        a, b = ref.get(k, "<missing>"), got.get(k, "<missing>")
        if a != b:
            diffs.append((k, a, b))

    print(f"probes compared: {len(keys)}")
    if not diffs:
        print("\nPARITY: identical on every probe.")
        return 0

    print(f"\nDIVERGENCES: {len(diffs)}\n")
    for k, a, b in diffs:
        print(f"  {k}")
        print(f"    django : {a!r}")
        print(f"    fathom : {b!r}")
    return 1


if __name__ == "__main__":
    sys.exit(main())
