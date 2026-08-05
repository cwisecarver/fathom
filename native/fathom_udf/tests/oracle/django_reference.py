"""Django's own SQLite UDF implementations, used as a test oracle for the Rust extension.

The function bodies below are copied VERBATIM from Django (with only the import plumbing
adjusted so this file runs without Django installed):

  * ``django/db/backends/sqlite3/_functions.py``   — the ``_sqlite_*`` functions
  * ``django/db/backends/utils.py``                — ``typecast_date`` / ``typecast_time`` /
                                                     ``typecast_timestamp``
  * ``django/utils/timezone.py``                   — ``split_tzname_delta`` / ``localtime``
  * ``django/utils/duration.py``                   — ``duration_microseconds``

Fetched from the ``stable/5.2.x`` and ``main`` branches on 2026-08-05.

**Why a copy rather than `pip install django`:** this has to run offline, in CI, and years from
now. Vendoring the ~150 lines that matter — with provenance — makes the oracle reproducible and
reviewable, and a drift between this file and upstream Django is a thing a human can diff. An
oracle that silently changes when a dependency resolves differently is not an oracle.

Run ``python3 generate_cases.py`` to regenerate ``cases.json``; ``tests/oracle.rs`` consumes it.
"""

import datetime
import hashlib
import re
import zoneinfo
from datetime import timedelta
from math import ceil

# --------------------------------------------------------------------------------------------
# django/db/backends/utils.py
# --------------------------------------------------------------------------------------------


def typecast_date(s):
    return (
        datetime.date(*map(int, s.split("-"))) if s else None
    )  # return None if s is null


def typecast_time(s):  # does NOT store time zone information
    if not s:
        return None
    hour, minutes, seconds = s.split(":")
    if "." in seconds:  # check whether seconds have a fractional part
        seconds, microseconds = seconds.split(".")
    else:
        microseconds = "0"
    return datetime.time(
        int(hour), int(minutes), int(seconds), int((microseconds + "000000")[:6])
    )


def typecast_timestamp(s):  # does NOT store time zone information
    # "2005-07-29 15:48:00.590358-05"
    # "2005-07-29 09:56:00-05"
    if not s:
        return None
    if " " not in s:
        return typecast_date(s)
    d, t = s.split()
    # Remove timezone information.
    if "-" in t:
        t, _ = t.split("-", 1)
    elif "+" in t:
        t, _ = t.split("+", 1)
    dates = d.split("-")
    times = t.split(":")
    seconds = times[2]
    if "." in seconds:  # check whether seconds have a fractional part
        seconds, microseconds = seconds.split(".")
    else:
        microseconds = "0"
    return datetime.datetime(
        int(dates[0]),
        int(dates[1]),
        int(dates[2]),
        int(times[0]),
        int(times[1]),
        int(seconds),
        int((microseconds + "000000")[:6]),
    )


# --------------------------------------------------------------------------------------------
# django/utils/dateparse.py (the time_re half) + django/utils/timezone.py
# --------------------------------------------------------------------------------------------

time_re = re.compile(
    r"(?P<hour>\d{1,2}):(?P<minute>\d{1,2})"
    r"(?::(?P<second>\d{1,2})(?:[\.,](?P<microsecond>\d{1,6})\d{0,6})?)?$"
)


def parse_time(value):
    try:
        # The fromisoformat() method takes time zone info into account and
        # returns a time with a tzinfo component, if possible. However, there
        # are no circumstances where aware datetime.time objects make sense, so
        # remove the time zone offset.
        return datetime.time.fromisoformat(value).replace(tzinfo=None)
    except ValueError:
        if match := time_re.match(value):
            kw = {k: int(v) for k, v in match.groupdict().items() if v is not None}
            return datetime.time(**kw)


def split_tzname_delta(tzname):
    """
    Split a time zone name into a 3-tuple of (name, sign, offset).
    """
    for sign in ["+", "-"]:
        if sign in tzname:
            name, offset = tzname.rsplit(sign, 1)
            if offset and parse_time(offset):
                if ":" not in offset:
                    offset = f"{offset}:00"
                return name, sign, offset
    return tzname, None, None


def is_naive(value):
    return value.utcoffset() is None


def localtime(value, timezone):
    if is_naive(value):
        raise ValueError("localtime() cannot be applied to a naive datetime")
    return value.astimezone(timezone)


def duration_microseconds(delta):
    return (24 * 60 * 60 * delta.days + delta.seconds) * 1000000 + delta.microseconds


# --------------------------------------------------------------------------------------------
# django/db/backends/sqlite3/_functions.py
# --------------------------------------------------------------------------------------------


def _sqlite_datetime_parse(dt, tzname=None, conn_tzname=None):
    if dt is None:
        return None
    try:
        dt = typecast_timestamp(dt)
    except (TypeError, ValueError):
        return None
    if conn_tzname:
        dt = dt.replace(tzinfo=zoneinfo.ZoneInfo(conn_tzname))
    if tzname is not None and tzname != conn_tzname:
        tzname, sign, offset = split_tzname_delta(tzname)
        if offset:
            hours, minutes = offset.split(":")
            offset_delta = timedelta(hours=int(hours), minutes=int(minutes))
            dt += offset_delta if sign == "+" else -offset_delta
        dt = localtime(dt, zoneinfo.ZoneInfo(tzname))
    return dt


def _sqlite_date_trunc(lookup_type, dt, tzname, conn_tzname):
    dt = _sqlite_datetime_parse(dt, tzname, conn_tzname)
    if dt is None:
        return None
    if lookup_type == "year":
        return f"{dt.year:04d}-01-01"
    elif lookup_type == "quarter":
        month_in_quarter = dt.month - (dt.month - 1) % 3
        return f"{dt.year:04d}-{month_in_quarter:02d}-01"
    elif lookup_type == "month":
        return f"{dt.year:04d}-{dt.month:02d}-01"
    elif lookup_type == "week":
        dt -= timedelta(days=dt.weekday())
        return f"{dt.year:04d}-{dt.month:02d}-{dt.day:02d}"
    elif lookup_type == "day":
        return f"{dt.year:04d}-{dt.month:02d}-{dt.day:02d}"
    raise ValueError(f"Unsupported lookup type: {lookup_type!r}")


def _sqlite_time_trunc(lookup_type, dt, tzname, conn_tzname):
    if dt is None:
        return None
    dt_parsed = _sqlite_datetime_parse(dt, tzname, conn_tzname)
    if dt_parsed is None:
        try:
            dt = typecast_time(dt)
        except (ValueError, TypeError):
            return None
    else:
        dt = dt_parsed
    if lookup_type == "hour":
        return f"{dt.hour:02d}:00:00"
    elif lookup_type == "minute":
        return f"{dt.hour:02d}:{dt.minute:02d}:00"
    elif lookup_type == "second":
        return f"{dt.hour:02d}:{dt.minute:02d}:{dt.second:02d}"
    raise ValueError(f"Unsupported lookup type: {lookup_type!r}")


def _sqlite_datetime_cast_date(dt, tzname, conn_tzname):
    dt = _sqlite_datetime_parse(dt, tzname, conn_tzname)
    if dt is None:
        return None
    return dt.date().isoformat()


def _sqlite_datetime_cast_time(dt, tzname, conn_tzname):
    dt = _sqlite_datetime_parse(dt, tzname, conn_tzname)
    if dt is None:
        return None
    return dt.time().isoformat()


def _sqlite_datetime_extract(lookup_type, dt, tzname=None, conn_tzname=None):
    dt = _sqlite_datetime_parse(dt, tzname, conn_tzname)
    if dt is None:
        return None
    if lookup_type == "week_day":
        return (dt.isoweekday() % 7) + 1
    elif lookup_type == "iso_week_day":
        return dt.isoweekday()
    elif lookup_type == "week":
        return dt.isocalendar().week
    elif lookup_type == "quarter":
        return ceil(dt.month / 3)
    elif lookup_type == "iso_year":
        return dt.isocalendar().year
    else:
        return getattr(dt, lookup_type)


def _sqlite_datetime_trunc(lookup_type, dt, tzname, conn_tzname):
    dt = _sqlite_datetime_parse(dt, tzname, conn_tzname)
    if dt is None:
        return None
    if lookup_type == "year":
        return f"{dt.year:04d}-01-01 00:00:00"
    elif lookup_type == "quarter":
        month_in_quarter = dt.month - (dt.month - 1) % 3
        return f"{dt.year:04d}-{month_in_quarter:02d}-01 00:00:00"
    elif lookup_type == "month":
        return f"{dt.year:04d}-{dt.month:02d}-01 00:00:00"
    elif lookup_type == "week":
        dt -= timedelta(days=dt.weekday())
        return f"{dt.year:04d}-{dt.month:02d}-{dt.day:02d} 00:00:00"
    elif lookup_type == "day":
        return f"{dt.year:04d}-{dt.month:02d}-{dt.day:02d} 00:00:00"
    elif lookup_type == "hour":
        return f"{dt.year:04d}-{dt.month:02d}-{dt.day:02d} {dt.hour:02d}:00:00"
    elif lookup_type == "minute":
        return (
            f"{dt.year:04d}-{dt.month:02d}-{dt.day:02d} "
            f"{dt.hour:02d}:{dt.minute:02d}:00"
        )
    elif lookup_type == "second":
        return (
            f"{dt.year:04d}-{dt.month:02d}-{dt.day:02d} "
            f"{dt.hour:02d}:{dt.minute:02d}:{dt.second:02d}"
        )
    raise ValueError(f"Unsupported lookup type: {lookup_type!r}")


def _sqlite_time_extract(lookup_type, dt):
    if dt is None:
        return None
    try:
        dt = typecast_time(dt)
    except (ValueError, TypeError):
        return None
    return getattr(dt, lookup_type)


def _sqlite_prepare_dtdelta_param(conn, param):
    if conn in ["+", "-"]:
        if isinstance(param, int):
            return timedelta(0, 0, param)
        else:
            return typecast_timestamp(param)
    return param


def _sqlite_format_dtdelta(connector, lhs, rhs):
    if connector is None or lhs is None or rhs is None:
        return None
    connector = connector.strip()
    try:
        real_lhs = _sqlite_prepare_dtdelta_param(connector, lhs)
        real_rhs = _sqlite_prepare_dtdelta_param(connector, rhs)
    except (ValueError, TypeError):
        return None
    if connector == "+":
        # typecast_timestamp() returns a date or a datetime without timezone.
        # It will be formatted as "%Y-%m-%d" or "%Y-%m-%d %H:%M:%S[.%f]"
        out = str(real_lhs + real_rhs)
    elif connector == "-":
        out = str(real_lhs - real_rhs)
    elif connector == "*":
        out = real_lhs * real_rhs
    else:
        out = real_lhs / real_rhs
    return out


def _sqlite_time_diff(lhs, rhs):
    if lhs is None or rhs is None:
        return None
    left = typecast_time(lhs)
    right = typecast_time(rhs)
    return (
        (left.hour * 60 * 60 * 1000000)
        + (left.minute * 60 * 1000000)
        + (left.second * 1000000)
        + (left.microsecond)
        - (right.hour * 60 * 60 * 1000000)
        - (right.minute * 60 * 1000000)
        - (right.second * 1000000)
        - (right.microsecond)
    )


def _sqlite_timestamp_diff(lhs, rhs):
    if lhs is None or rhs is None:
        return None
    left = typecast_timestamp(lhs)
    right = typecast_timestamp(rhs)
    return duration_microseconds(left - right)


def _sqlite_lpad(text, length, fill_text):
    if text is None or length is None or fill_text is None:
        return None
    delta = length - len(text)
    if delta <= 0:
        return text[:length]
    return (fill_text * length)[:delta] + text


def _sqlite_rpad(text, length, fill_text):
    if text is None or length is None or fill_text is None:
        return None
    return (text + fill_text * length)[:length]


def _sqlite_repeat(text, count):
    if text is None or count is None:
        return None
    return text * count


def _sqlite_reverse(text):
    if text is None:
        return None
    return text[::-1]


def _sqlite_md5(text):
    if text is None:
        return None
    return hashlib.md5(text.encode()).hexdigest()


def _sqlite_sha1(text):
    if text is None:
        return None
    return hashlib.sha1(text.encode()).hexdigest()


def _sqlite_sha224(text):
    if text is None:
        return None
    return hashlib.sha224(text.encode()).hexdigest()


def _sqlite_sha256(text):
    if text is None:
        return None
    return hashlib.sha256(text.encode()).hexdigest()


def _sqlite_sha384(text):
    if text is None:
        return None
    return hashlib.sha384(text.encode()).hexdigest()


def _sqlite_sha512(text):
    if text is None:
        return None
    return hashlib.sha512(text.encode()).hexdigest()


def _sqlite_bitxor(x, y):
    if x is None or y is None:
        return None
    return x ^ y
