//! The eleven `django_*` date/time UDFs, as pure functions over already-decoded arguments.
//!
//! Kept free of any `rusqlite` types on purpose: the SQLite binding in `lib.rs` is a thin decode /
//! encode shell, and everything with a decision in it is here where it can be unit-tested against
//! the values CPython produces. `docs/quickstart-django.md` records which Django lookup reaches
//! which of these.
//!
//! Where Django raises, we return `Err` — a raised exception in a Python UDF surfaces to the
//! client as a SQLite error, so an `Err` here reproduces the same observable behaviour rather than
//! inventing a fallback Django does not have.

use chrono::{
    DateTime, Datelike, Duration, LocalResult, NaiveDate, NaiveDateTime, NaiveTime, Offset,
    TimeZone, Timelike,
};
use chrono_tz::Tz;

use crate::pytypes::{
    self, duration_microseconds, format_date, format_datetime, format_time, format_timedelta,
    iso_week, iso_year, isoweekday, offset_micros, split_tzname_delta, typecast_time,
    typecast_timestamp, weekday, Parsed,
};

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum UdfError {
    /// Django's own `raise ValueError(f"Unsupported lookup type: {lookup_type!r}")`.
    UnsupportedLookup(String),
    /// `zoneinfo.ZoneInfoNotFoundError`.
    UnknownTimezone(String),
    /// Django reaching for a time attribute on a `datetime.date` (`AttributeError`), or
    /// `timezone.localtime` refusing a naive value (`ValueError`).
    Type(String),
}

impl std::fmt::Display for UdfError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            UdfError::UnsupportedLookup(l) => write!(f, "Unsupported lookup type: '{l}'"),
            UdfError::UnknownTimezone(t) => write!(f, "No time zone found with key {t}"),
            UdfError::Type(m) => write!(f, "{m}"),
        }
    }
}

impl std::error::Error for UdfError {}

type R<T> = Result<T, UdfError>;

/// Treat an empty string the way Python treats a falsy `tzname`: `if conn_tzname:` and
/// `if tzname is not None` are different tests, so the caller keeps `Option` and this only
/// collapses `Some("")`.
fn truthy(s: Option<&str>) -> Option<&str> {
    s.filter(|v| !v.is_empty())
}

/// Attach a zone to a naive local datetime — Python's `dt.replace(tzinfo=ZoneInfo(name))`.
///
/// The two non-obvious cases both come from `fold=0`, which is `replace`'s default:
///   * **Ambiguous** (the hour repeated when DST ends): take the FIRST occurrence.
///   * **Nonexistent** (the hour skipped when DST starts): Python does not raise. It resolves
///     using the UTC offset in effect *before* the transition, so 02:30 on a spring-forward date
///     in `America/New_York` is read as EST (-05:00), i.e. 07:30 UTC. chrono reports this as
///     `LocalResult::None`, and a naive `.unwrap()`/`.single()` here would turn a legal Django
///     query into a panic or an error on exactly two dates a year, per zone.
fn attach(tz: Tz, naive: NaiveDateTime) -> R<DateTime<Tz>> {
    match tz.from_local_datetime(&naive) {
        LocalResult::Single(dt) => Ok(dt),
        LocalResult::Ambiguous(earliest, _latest) => Ok(earliest),
        LocalResult::None => {
            // Sample the offset well before the gap (transitions are never within a day of each
            // other), then interpret the naive value as being at that offset.
            let before = naive - Duration::days(1);
            let offset = match tz.offset_from_local_datetime(&before) {
                LocalResult::Single(o) => o,
                LocalResult::Ambiguous(o, _) => o,
                LocalResult::None => {
                    return Err(UdfError::Type(format!(
                        "cannot resolve {naive} in time zone {tz}"
                    )))
                }
            };
            let fixed = offset.fix();
            Ok(DateTime::from_naive_utc_and_offset(
                naive - fixed,
                offset,
            ))
        }
    }
}

fn zone(name: &str) -> R<Tz> {
    name.parse::<Tz>()
        .map_err(|_| UdfError::UnknownTimezone(name.to_string()))
}

/// `_sqlite_datetime_parse`.
///
/// Returns `Ok(None)` where Django returns `None` (NULL in, or unparseable — Django catches
/// `TypeError`/`ValueError` around `typecast_timestamp` only), and `Err` where Django would let an
/// exception escape.
pub fn datetime_parse(
    dt: Option<&str>,
    tzname: Option<&str>,
    conn_tzname: Option<&str>,
) -> R<Option<Parsed>> {
    let Some(raw) = dt else { return Ok(None) };

    let conn_tz = truthy(conn_tzname);
    let needs_convert = tzname.is_some() && tzname != conn_tzname;

    let Some(parsed) = typecast_timestamp(raw) else {
        // Django distinguishes two ways `typecast_timestamp` fails to produce a value, and only
        // one of them is caught:
        //
        //   * Unparseable input ("garbage", month 13, hour 25) RAISES ValueError, which
        //     `_sqlite_datetime_parse`'s `try` catches and turns into `return None` — before any
        //     timezone work happens.
        //   * The EMPTY STRING returns `None` normally (`if not s: return None`), so control falls
        //     through to `dt.replace(tzinfo=...)` / `localtime(dt, ...)` on a `None`, which raises
        //     AttributeError and is NOT caught.
        //
        // So `''` is NULL without a timezone and an ERROR with one. Found by the generated oracle
        // table — 25 cases, and the only class that survived the first full run. Nobody would have
        // written this test by hand.
        if raw.is_empty() && (conn_tz.is_some() || needs_convert) {
            return Err(UdfError::Type(
                "'NoneType' object has no attribute 'replace'".to_string(),
            ));
        }
        return Ok(None);
    };

    if conn_tz.is_none() && !needs_convert {
        // The NULL-timezone path: no zone work at all, so a DateField value stays a date and no
        // tzdata is consulted. This is the branch `USE_TZ = False` apps and every DateField lookup
        // take (docs/quickstart-django.md).
        return Ok(Some(parsed));
    }

    // From here Django is doing datetime arithmetic. A `date` has no `tzinfo` to replace and no
    // `utcoffset()` for `localtime` to read, so both paths raise in Python.
    let naive = match parsed {
        Parsed::DateTime(dt) => dt,
        Parsed::Date(_) => {
            return Err(UdfError::Type(
                "a date value has no time zone to convert; \
                 django passed a tzname for a DateField-shaped value"
                    .to_string(),
            ))
        }
    };

    let mut aware = match conn_tz {
        Some(name) => attach(zone(name)?, naive)?,
        None => {
            // `timezone.localtime()` on a naive datetime raises ValueError. Reached only when a
            // tzname is supplied without a connection timezone, which Django itself does not do.
            return Err(UdfError::Type(
                "localtime() cannot be applied to a naive datetime".to_string(),
            ));
        }
    };

    if needs_convert {
        let target = tzname.expect("needs_convert implies Some");
        let (name, sign, offset) = split_tzname_delta(target);
        if let (Some(sign), Some(offset)) = (sign, offset) {
            let micros = offset_micros(&offset).ok_or_else(|| {
                UdfError::Type(format!("could not read a time offset from {target}"))
            })?;
            let delta = Duration::microseconds(micros);
            aware = if sign == '+' { aware + delta } else { aware - delta };
        }
        aware = aware.with_timezone(&zone(name)?);
    }

    Ok(Some(Parsed::DateTime(aware.naive_local())))
}

/// `_sqlite_date_trunc` — `django_date_trunc(lookup_type, dt, tzname, conn_tzname)`.
pub fn date_trunc(
    lookup: Option<&str>,
    dt: Option<&str>,
    tzname: Option<&str>,
    conn_tzname: Option<&str>,
) -> R<Option<String>> {
    let Some(lookup) = lookup else { return Ok(None) };
    let Some(parsed) = datetime_parse(dt, tzname, conn_tzname)? else {
        return Ok(None);
    };
    let d = parsed.date();

    let out = match lookup {
        "year" => format!("{:04}-01-01", d.year()),
        "quarter" => {
            let month_in_quarter = d.month() - (d.month() - 1) % 3;
            format!("{:04}-{:02}-01", d.year(), month_in_quarter)
        }
        "month" => format!("{:04}-{:02}-01", d.year(), d.month()),
        "week" => {
            // Django subtracts `dt.weekday()` days — Monday-based, so the week starts Monday.
            let start = d - Duration::days(weekday(d) as i64);
            format!("{:04}-{:02}-{:02}", start.year(), start.month(), start.day())
        }
        "day" => format!("{:04}-{:02}-{:02}", d.year(), d.month(), d.day()),
        other => return Err(UdfError::UnsupportedLookup(other.to_string())),
    };
    Ok(Some(out))
}

/// `_sqlite_datetime_trunc` — same lookups as `date_trunc` plus the time-of-day ones, and a
/// `00:00:00` time component on every result.
pub fn datetime_trunc(
    lookup: Option<&str>,
    dt: Option<&str>,
    tzname: Option<&str>,
    conn_tzname: Option<&str>,
) -> R<Option<String>> {
    let Some(lookup) = lookup else { return Ok(None) };
    let Some(parsed) = datetime_parse(dt, tzname, conn_tzname)? else {
        return Ok(None);
    };
    let d = parsed.date();
    let t = parsed.time();

    // The hour/minute/second lookups read time attributes, which a bare date does not have.
    let need_time = matches!(lookup, "hour" | "minute" | "second");
    if need_time && t.is_none() {
        return Err(UdfError::Type(format!(
            "'datetime.date' object has no attribute '{lookup}'"
        )));
    }
    let t = t.unwrap_or_else(|| NaiveTime::from_hms_opt(0, 0, 0).unwrap());

    let out = match lookup {
        "year" => format!("{:04}-01-01 00:00:00", d.year()),
        "quarter" => {
            let month_in_quarter = d.month() - (d.month() - 1) % 3;
            format!("{:04}-{:02}-01 00:00:00", d.year(), month_in_quarter)
        }
        "month" => format!("{:04}-{:02}-01 00:00:00", d.year(), d.month()),
        "week" => {
            let start = d - Duration::days(weekday(d) as i64);
            format!(
                "{:04}-{:02}-{:02} 00:00:00",
                start.year(),
                start.month(),
                start.day()
            )
        }
        "day" => format!("{:04}-{:02}-{:02} 00:00:00", d.year(), d.month(), d.day()),
        "hour" => format!(
            "{:04}-{:02}-{:02} {:02}:00:00",
            d.year(),
            d.month(),
            d.day(),
            t.hour()
        ),
        "minute" => format!(
            "{:04}-{:02}-{:02} {:02}:{:02}:00",
            d.year(),
            d.month(),
            d.day(),
            t.hour(),
            t.minute()
        ),
        "second" => format!(
            "{:04}-{:02}-{:02} {:02}:{:02}:{:02}",
            d.year(),
            d.month(),
            d.day(),
            t.hour(),
            t.minute(),
            t.second()
        ),
        other => return Err(UdfError::UnsupportedLookup(other.to_string())),
    };
    Ok(Some(out))
}

/// `_sqlite_time_trunc` — `django_time_trunc(lookup_type, dt, tzname, conn_tzname)`.
///
/// Falls back to `typecast_time` when the value is a bare time string, which is how a TimeField
/// reaches here. Django's fallback triggers on `_sqlite_datetime_parse` returning `None`, so a
/// tz-conversion error still propagates rather than being swallowed by the fallback.
pub fn time_trunc(
    lookup: Option<&str>,
    dt: Option<&str>,
    tzname: Option<&str>,
    conn_tzname: Option<&str>,
) -> R<Option<String>> {
    let Some(lookup) = lookup else { return Ok(None) };
    let Some(raw) = dt else { return Ok(None) };

    let t = match datetime_parse(Some(raw), tzname, conn_tzname)? {
        Some(parsed) => match parsed.time() {
            Some(t) => t,
            // A bare date parsed successfully but has no time attributes.
            None => {
                return Err(UdfError::Type(format!(
                    "'datetime.date' object has no attribute '{lookup}'"
                )))
            }
        },
        None => match typecast_time(raw) {
            Some(t) => t,
            // Same empty-string quirk as `datetime_parse`: `typecast_time("")` returns None
            // without raising, so Django reaches `dt.hour` on a `None` and raises AttributeError,
            // while genuinely unparseable input raises inside the `try` and is caught as NULL.
            None if raw.is_empty() => {
                return Err(UdfError::Type(format!(
                    "'NoneType' object has no attribute '{lookup}'"
                )))
            }
            None => return Ok(None),
        },
    };

    let out = match lookup {
        "hour" => format!("{:02}:00:00", t.hour()),
        "minute" => format!("{:02}:{:02}:00", t.hour(), t.minute()),
        "second" => format!("{:02}:{:02}:{:02}", t.hour(), t.minute(), t.second()),
        other => return Err(UdfError::UnsupportedLookup(other.to_string())),
    };
    Ok(Some(out))
}

/// `_sqlite_datetime_cast_date` — `dt.date().isoformat()`.
///
/// Errors on a bare DATE value, because `datetime.date` has no `.date()` method — Django raises
/// AttributeError. Counter-intuitive (the "cast to date" of a date looks like a no-op) and exactly
/// the sort of thing a hand-written test would have asserted the friendly way round.
pub fn datetime_cast_date(
    dt: Option<&str>,
    tzname: Option<&str>,
    conn_tzname: Option<&str>,
) -> R<Option<String>> {
    match datetime_parse(dt, tzname, conn_tzname)? {
        None => Ok(None),
        Some(Parsed::DateTime(dt)) => Ok(Some(format_date(dt.date()))),
        Some(Parsed::Date(_)) => Err(UdfError::Type(
            "'datetime.date' object has no attribute 'date'".to_string(),
        )),
    }
}

/// `_sqlite_datetime_cast_time` — `dt.time().isoformat()`.
pub fn datetime_cast_time(
    dt: Option<&str>,
    tzname: Option<&str>,
    conn_tzname: Option<&str>,
) -> R<Option<String>> {
    match datetime_parse(dt, tzname, conn_tzname)? {
        None => Ok(None),
        Some(p) => match p.time() {
            Some(t) => Ok(Some(format_time(t))),
            None => Err(UdfError::Type(
                "'datetime.date' object has no attribute 'time'".to_string(),
            )),
        },
    }
}

/// The value an `extract` lookup yields — an integer for every lookup Django supports.
pub type Extracted = i64;

/// `_sqlite_datetime_extract`, shared by `django_date_extract` (arity 2) and
/// `django_datetime_extract` (arity 4).
///
/// The two are the SAME Python function — a detail worth stating because the names imply
/// otherwise, and registering a separate date-only implementation would drift from Django the
/// first time one of them changed.
pub fn datetime_extract(
    lookup: Option<&str>,
    dt: Option<&str>,
    tzname: Option<&str>,
    conn_tzname: Option<&str>,
) -> R<Option<Extracted>> {
    let Some(lookup) = lookup else { return Ok(None) };
    let Some(parsed) = datetime_parse(dt, tzname, conn_tzname)? else {
        return Ok(None);
    };
    let d = parsed.date();

    let value = match lookup {
        // Django's own arithmetic, kept verbatim: Sunday == 1 … Saturday == 7.
        "week_day" => ((isoweekday(d) % 7) + 1) as i64,
        "iso_week_day" => isoweekday(d) as i64,
        "week" => iso_week(d) as i64,
        // `ceil(month / 3)` — months 1-3 => 1, 4-6 => 2, …
        "quarter" => ((d.month() + 2) / 3) as i64,
        "iso_year" => iso_year(d) as i64,
        "year" => d.year() as i64,
        "month" => d.month() as i64,
        "day" => d.day() as i64,
        // The remaining lookups are `getattr(dt, lookup_type)` on the time half.
        "hour" | "minute" | "second" | "microsecond" => {
            let Some(t) = parsed.time() else {
                return Err(UdfError::Type(format!(
                    "'datetime.date' object has no attribute '{lookup}'"
                )));
            };
            match lookup {
                "hour" => t.hour() as i64,
                "minute" => t.minute() as i64,
                "second" => t.second() as i64,
                _ => (t.nanosecond() / 1000) as i64,
            }
        }
        other => {
            return Err(UdfError::Type(format!(
                "'datetime.datetime' object has no attribute '{other}'"
            )))
        }
    };
    Ok(Some(value))
}

/// `_sqlite_time_extract` — `getattr(typecast_time(dt), lookup_type)`.
pub fn time_extract(lookup: Option<&str>, dt: Option<&str>) -> R<Option<Extracted>> {
    let Some(lookup) = lookup else { return Ok(None) };
    let Some(raw) = dt else { return Ok(None) };
    let Some(t) = typecast_time(raw) else {
        // `getattr(None, lookup)` — see `datetime_parse` for why only the empty string gets here
        // without having raised first.
        if raw.is_empty() {
            return Err(UdfError::Type(format!(
                "'NoneType' object has no attribute '{lookup}'"
            )));
        }
        return Ok(None);
    };

    let value = match lookup {
        "hour" => t.hour() as i64,
        "minute" => t.minute() as i64,
        "second" => t.second() as i64,
        "microsecond" => (t.nanosecond() / 1000) as i64,
        other => {
            return Err(UdfError::Type(format!(
                "'datetime.time' object has no attribute '{other}'"
            )))
        }
    };
    Ok(Some(value))
}

/// `_sqlite_time_diff` — the signed difference in microseconds.
///
/// Unlike the trunc/extract functions, this one has **no `try`/`except`**: `typecast_time` raises
/// straight out of it on unparseable input, and the empty string yields `None` whose `.hour` then
/// raises. So every input that is not a valid time is an ERROR, not a NULL.
pub fn time_diff(lhs: Option<&str>, rhs: Option<&str>) -> R<Option<i64>> {
    let (Some(lhs), Some(rhs)) = (lhs, rhs) else {
        return Ok(None);
    };
    let parse = |s: &str| {
        typecast_time(s).ok_or_else(|| UdfError::Type(format!("could not parse a time from {s:?}")))
    };
    let (l, r) = (parse(lhs)?, parse(rhs)?);
    let micros = |t: NaiveTime| {
        t.hour() as i64 * 3_600_000_000
            + t.minute() as i64 * 60_000_000
            + t.second() as i64 * 1_000_000
            + (t.nanosecond() / 1000) as i64
    };
    Ok(Some(micros(l) - micros(r)))
}

/// `_sqlite_timestamp_diff` — `duration_microseconds(left - right)`.
///
/// Also un-guarded (see `time_diff`), and additionally: Python refuses to subtract a `datetime`
/// from a `date` or vice versa (TypeError), while `date - date` and `datetime - datetime` both
/// work. So a query diffing a DateField against a DateTimeField errors rather than silently
/// treating the date as midnight.
pub fn timestamp_diff(lhs: Option<&str>, rhs: Option<&str>) -> R<Option<i64>> {
    let (Some(lhs), Some(rhs)) = (lhs, rhs) else {
        return Ok(None);
    };
    let parse = |s: &str| {
        typecast_timestamp(s)
            .ok_or_else(|| UdfError::Type(format!("could not parse a timestamp from {s:?}")))
    };
    let (l, r) = (parse(lhs)?, parse(rhs)?);

    match (l, r) {
        (Parsed::DateTime(a), Parsed::DateTime(b)) => Ok(Some(duration_microseconds(a, b))),
        (Parsed::Date(a), Parsed::Date(b)) => Ok(Some(duration_microseconds(
            a.and_hms_opt(0, 0, 0).unwrap(),
            b.and_hms_opt(0, 0, 0).unwrap(),
        ))),
        _ => Err(UdfError::Type(
            "unsupported operand type(s) for -: 'datetime.date' and 'datetime.datetime'"
                .to_string(),
        )),
    }
}

/// One operand of `django_format_dtdelta`, after `_sqlite_prepare_dtdelta_param`.
#[derive(Debug, Clone, PartialEq)]
pub enum DtParam {
    /// An integer count of microseconds, promoted to `timedelta(0, 0, param)`.
    Delta(i64),
    Date(NaiveDate),
    DateTime(NaiveDateTime),
    /// Passed through untouched — only reachable for the `*` and `/` connectors.
    Number(f64),
    Integer(i64),
}

/// What `django_format_dtdelta` yields.
///
/// **Not always text.** Django only wraps the `+` and `-` results in `str()`; the `*` and `/`
/// branches return the raw Python number, which the sqlite3 driver then stores as INTEGER or REAL.
/// Returning a string for those would give a tenant `'12'` where Django gives `12` — and since
/// SQLite compares text and integers as different types, `WHERE duration * 2 = 12` would silently
/// stop matching. Caught by the generated oracle table, not by reading.
#[derive(Debug, Clone, PartialEq)]
pub enum DtOut {
    Text(String),
    Int(i64),
    Float(f64),
}

/// `_sqlite_format_dtdelta(connector, lhs, rhs)`.
///
/// The connector decides how the operands are read, which is the part that surprises: under `+`
/// or `-` a bare integer means MICROSECONDS, while under `*` or `/` the same integer is a plain
/// scalar. Django uses this single function for both duration arithmetic and duration scaling.
pub fn format_dtdelta(
    connector: Option<&str>,
    lhs: Option<DtParam>,
    rhs: Option<DtParam>,
) -> R<Option<DtOut>> {
    let (Some(connector), Some(lhs), Some(rhs)) = (connector, lhs, rhs) else {
        return Ok(None);
    };
    let connector = connector.trim();

    match connector {
        "+" | "-" => {
            let negate = connector == "-";
            add_or_sub(lhs, rhs, negate).map(|s| Some(DtOut::Text(s)))
        }
        "*" => {
            let (l, r) = (as_number(&lhs), as_number(&rhs));
            match (l, r) {
                // `int * int` stays an int in Python; anything else becomes a float.
                //
                // `checked_mul`, not `*`: Python integers are unbounded, so Django computes a
                // product that SQLite's 64-bit INTEGER cannot hold and its driver raises
                // OverflowError. A plain `*` would panic in a debug build and — far worse —
                // silently WRAP in the release build this ships as, handing a tenant a negative
                // duration. Two 86_400_000_000 microsecond operands are enough to reach it.
                (Some(_), Some(_)) => Ok(Some(match (&lhs, &rhs) {
                    (DtParam::Integer(a), DtParam::Integer(b)) => match a.checked_mul(*b) {
                        Some(v) => DtOut::Int(v),
                        None => {
                            return Err(UdfError::Type(
                                "Python int too large to convert to SQLite INTEGER".to_string(),
                            ))
                        }
                    },
                    _ => DtOut::Float(l.unwrap() * r.unwrap()),
                })),
                _ => Ok(None),
            }
        }
        _ => {
            // Python 3's `/` is true division and always yields a float, even when it divides
            // evenly — `8 / 2` is `4.0`, not `4`.
            let (l, r) = (as_number(&lhs), as_number(&rhs));
            match (l, r) {
                (Some(_), Some(r)) if r == 0.0 => {
                    Err(UdfError::Type("float division by zero".to_string()))
                }
                (Some(l), Some(r)) => Ok(Some(DtOut::Float(l / r))),
                _ => Ok(None),
            }
        }
    }
}

fn as_number(p: &DtParam) -> Option<f64> {
    match p {
        DtParam::Integer(i) => Some(*i as f64),
        DtParam::Number(f) => Some(*f),
        DtParam::Delta(d) => Some(*d as f64),
        _ => None,
    }
}

fn add_or_sub(lhs: DtParam, rhs: DtParam, negate: bool) -> R<String> {
    use DtParam::*;

    let sign = if negate { -1 } else { 1 };

    let out = match (lhs, rhs) {
        // Checked, for the same reason the `*` branch is: a release build would wrap silently and
        // report a duration of the wrong sign. Python's own `timedelta` has a wider range than
        // i64 microseconds (it caps at 999999999 days, we cap at ~106751), so on absurd inputs we
        // error where Django would still produce a string. Erroring is the safe direction, and the
        // reachable Django values — durations in a row — are nowhere near either bound.
        (Delta(a), Delta(b)) => {
            let scaled = b
                .checked_mul(sign)
                .ok_or_else(|| UdfError::Type("duration out of range".to_string()))?;
            let total = a
                .checked_add(scaled)
                .ok_or_else(|| UdfError::Type("duration out of range".to_string()))?;
            format_timedelta(total)
        }

        (DateTime(dt), Delta(d)) => format_datetime(dt + Duration::microseconds(sign * d)),
        (Delta(d), DateTime(dt)) if !negate => format_datetime(dt + Duration::microseconds(d)),

        // `date ± timedelta` uses ONLY the delta's normalized `.days` field — Python's date
        // arithmetic has no sub-day resolution.
        //
        // The subtlety is where the sign goes. CPython's `date_subtract` computes
        // `-GET_TD_DAYS(delta)`: it negates the DAYS FIELD, not the whole timedelta. Those differ
        // for any sub-day delta, because normalization pushes the borrow into `days`:
        // `timedelta(microseconds=1).days` is 0, so `date - timedelta(microseconds=1)` is the
        // SAME DAY, whereas negating first gives `timedelta(days=-1, ...)` and moves the date back
        // one. Ten of the thirteen final oracle divergences were this one character of ordering.
        (Date(d), Delta(delta)) => {
            let days = delta.div_euclid(86_400_000_000);
            format_date(d + Duration::days(sign * days))
        }
        (Delta(delta), Date(d)) if !negate => {
            let days = delta.div_euclid(86_400_000_000);
            format_date(d + Duration::days(days))
        }

        (DateTime(a), DateTime(b)) if negate => {
            format_timedelta((a - b).num_microseconds().unwrap_or(0))
        }
        (Date(a), Date(b)) if negate => {
            format_timedelta((a - b).num_microseconds().unwrap_or(0))
        }
        // NOTE: `date - datetime` and `datetime - date` are deliberately NOT handled — Python
        // raises TypeError for both, so a query diffing a DateField against a DateTimeField errors
        // rather than silently coercing the date to midnight. They fall through to the catch-all.

        (l, r) => {
            return Err(UdfError::Type(format!(
                "unsupported operand types for {}: {:?} and {:?}",
                if negate { "-" } else { "+" },
                l,
                r
            )))
        }
    };
    Ok(out)
}

/// Convert a raw SQLite argument into a `DtParam` the way `_sqlite_prepare_dtdelta_param` does.
pub fn prepare_dtdelta_param(connector: &str, int: Option<i64>, text: Option<&str>, float: Option<f64>) -> Option<DtParam> {
    let connector = connector.trim();
    if connector == "+" || connector == "-" {
        if let Some(i) = int {
            return Some(DtParam::Delta(i));
        }
        let text = text?;
        return match typecast_timestamp(text)? {
            Parsed::Date(d) => Some(DtParam::Date(d)),
            Parsed::DateTime(dt) => Some(DtParam::DateTime(dt)),
        };
    }
    if let Some(i) = int {
        return Some(DtParam::Integer(i));
    }
    if let Some(f) = float {
        return Some(DtParam::Number(f));
    }
    text.and_then(|t| t.parse::<f64>().ok()).map(DtParam::Number)
}

// Re-exported so `lib.rs` does not need a second `use` path for the one helper it shares.
pub use pytypes::typecast_timestamp as parse_timestamp;

#[cfg(test)]
mod tests {
    use super::*;

    fn dt(s: &str) -> Option<&str> {
        Some(s)
    }

    // ---- the NULL-timezone path -------------------------------------------------------------
    //
    // This is the branch `USE_TZ = False` apps and every DateField lookup take, and the one the
    // 2026-08-05 analysis in docs/quickstart-django.md verified against Django's source.

    #[test]
    fn date_trunc_null_tz() {
        let cases: &[(&str, &str, &str)] = &[
            ("year", "2026-08-05", "2026-01-01"),
            ("quarter", "2026-08-05", "2026-07-01"),
            ("quarter", "2026-01-15", "2026-01-01"),
            ("quarter", "2026-12-31", "2026-10-01"),
            ("month", "2026-08-05", "2026-08-01"),
            ("day", "2026-08-05", "2026-08-05"),
            // 2026-08-05 is a Wednesday; the week starts Monday 2026-08-03.
            ("week", "2026-08-05", "2026-08-03"),
            // A Monday is its own week start.
            ("week", "2026-08-03", "2026-08-03"),
            // A Sunday belongs to the week that STARTED the previous Monday — the case a
            // Sunday-based week implementation gets wrong.
            ("week", "2026-08-09", "2026-08-03"),
            // Week truncation across a month boundary.
            ("week", "2026-08-01", "2026-07-27"),
        ];
        for (lookup, input, expected) in cases {
            assert_eq!(
                date_trunc(Some(lookup), dt(input), None, None).unwrap(),
                Some(expected.to_string()),
                "{lookup} of {input}"
            );
        }
    }

    #[test]
    fn datetime_trunc_null_tz() {
        let i = "2026-08-05 13:45:59.123456";
        let cases: &[(&str, &str)] = &[
            ("year", "2026-01-01 00:00:00"),
            ("quarter", "2026-07-01 00:00:00"),
            ("month", "2026-08-01 00:00:00"),
            ("week", "2026-08-03 00:00:00"),
            ("day", "2026-08-05 00:00:00"),
            ("hour", "2026-08-05 13:00:00"),
            ("minute", "2026-08-05 13:45:00"),
            ("second", "2026-08-05 13:45:59"),
        ];
        for (lookup, expected) in cases {
            assert_eq!(
                datetime_trunc(Some(lookup), dt(i), None, None).unwrap(),
                Some(expected.to_string()),
                "{lookup}"
            );
        }
    }

    #[test]
    fn time_trunc_null_tz() {
        for (lookup, expected) in [
            ("hour", "13:00:00"),
            ("minute", "13:45:00"),
            ("second", "13:45:59"),
        ] {
            assert_eq!(
                time_trunc(Some(lookup), dt("13:45:59.5"), None, None).unwrap(),
                Some(expected.to_string()),
                "{lookup} of a bare time"
            );
            assert_eq!(
                time_trunc(Some(lookup), dt("2026-08-05 13:45:59.5"), None, None).unwrap(),
                Some(expected.to_string()),
                "{lookup} of a datetime"
            );
        }
    }

    #[test]
    fn extract_null_tz() {
        let i = "2026-08-05 13:45:59.123456";
        let cases: &[(&str, i64)] = &[
            ("year", 2026),
            ("month", 8),
            ("day", 5),
            ("hour", 13),
            ("minute", 45),
            ("second", 59),
            ("microsecond", 123456),
            ("quarter", 3),
            ("week", 32),
            ("iso_year", 2026),
            // Wednesday: isoweekday 3, Django week_day (3 % 7) + 1 == 4.
            ("iso_week_day", 3),
            ("week_day", 4),
        ];
        for (lookup, expected) in cases {
            assert_eq!(
                datetime_extract(Some(lookup), dt(i), None, None).unwrap(),
                Some(*expected),
                "{lookup}"
            );
        }
    }

    #[test]
    fn quarter_boundaries() {
        for (month, quarter) in [
            (1, 1), (2, 1), (3, 1),
            (4, 2), (5, 2), (6, 2),
            (7, 3), (8, 3), (9, 3),
            (10, 4), (11, 4), (12, 4),
        ] {
            let s = format!("2026-{month:02}-15");
            assert_eq!(
                datetime_extract(Some("quarter"), dt(&s), None, None).unwrap(),
                Some(quarter),
                "month {month}"
            );
        }
    }

    #[test]
    fn week_day_covers_the_whole_week() {
        // Django's week_day is Sunday==1 … Saturday==7. 2026-08-03 is a Monday.
        let expected = [
            ("2026-08-03", 2), // Mon
            ("2026-08-04", 3), // Tue
            ("2026-08-05", 4), // Wed
            ("2026-08-06", 5), // Thu
            ("2026-08-07", 6), // Fri
            ("2026-08-08", 7), // Sat
            ("2026-08-09", 1), // Sun
        ];
        for (day, week_day) in expected {
            assert_eq!(
                datetime_extract(Some("week_day"), dt(day), None, None).unwrap(),
                Some(week_day),
                "{day}"
            );
        }
    }

    // ---- timezone-aware behaviour -----------------------------------------------------------
    //
    // The whole reason #19 chose a bundled tzdata. Each case is a real UTC instant converted into
    // a real zone, checked against what Django would compute.

    #[test]
    fn datetime_trunc_converts_into_the_target_zone_first() {
        // 2026-08-05 03:30 UTC is 2026-08-04 23:30 in New York (EDT, -04:00). Truncating to the
        // day must therefore yield the FOURTH, not the fifth — the exact bug a tz-ignoring
        // implementation ships, and it is invisible until a user near midnight complains.
        assert_eq!(
            datetime_trunc(
                Some("day"),
                dt("2026-08-05 03:30:00"),
                Some("America/New_York"),
                Some("UTC")
            )
            .unwrap(),
            Some("2026-08-04 00:00:00".to_string())
        );
        // And the date-cast agrees.
        assert_eq!(
            datetime_cast_date(dt("2026-08-05 03:30:00"), Some("America/New_York"), Some("UTC"))
                .unwrap(),
            Some("2026-08-04".to_string())
        );
    }

    #[test]
    fn extract_year_across_a_new_year_boundary_in_another_zone() {
        // 2027-01-01 02:00 UTC is still 2026-12-31 in New York. `__year` must say 2026.
        assert_eq!(
            datetime_extract(
                Some("year"),
                dt("2027-01-01 02:00:00"),
                Some("America/New_York"),
                Some("UTC")
            )
            .unwrap(),
            Some(2026)
        );
        // Eastward: 2026-12-31 20:00 UTC is already 2027-01-01 in Tokyo.
        assert_eq!(
            datetime_extract(
                Some("year"),
                dt("2026-12-31 20:00:00"),
                Some("Asia/Tokyo"),
                Some("UTC")
            )
            .unwrap(),
            Some(2027)
        );
    }

    #[test]
    fn a_half_hour_zone_is_handled() {
        // Kolkata is UTC+05:30 — a zone whose offset is not a whole number of hours, which catches
        // an implementation that stores offsets in hours.
        assert_eq!(
            datetime_cast_time(dt("2026-08-05 00:00:00"), Some("Asia/Kolkata"), Some("UTC"))
                .unwrap(),
            Some("05:30:00".to_string())
        );
    }

    #[test]
    fn dst_ambiguous_hour_takes_the_first_occurrence() {
        // 2026-11-01 01:30 America/New_York happens twice (EDT then EST). Python's fold=0 picks
        // the first. Converting that to UTC gives 05:30 rather than 06:30.
        assert_eq!(
            datetime_cast_time(dt("2026-11-01 01:30:00"), Some("UTC"), Some("America/New_York"))
                .unwrap(),
            Some("05:30:00".to_string())
        );
    }

    #[test]
    fn dst_nonexistent_hour_does_not_error() {
        // 2026-03-08 02:30 America/New_York does not exist (the clock jumps 02:00 -> 03:00).
        // Python resolves it with the pre-transition offset (EST, -05:00) => 07:30 UTC.
        // chrono reports LocalResult::None here, so this is the case a `.single().unwrap()`
        // implementation turns into a panic twice a year.
        assert_eq!(
            datetime_cast_time(dt("2026-03-08 02:30:00"), Some("UTC"), Some("America/New_York"))
                .unwrap(),
            Some("07:30:00".to_string())
        );
    }

    #[test]
    fn tzname_with_an_offset_delta_is_applied_before_conversion() {
        // Django encodes `TruncDay(..., tzinfo=...)` offsets into the tzname as "Zone+HH:MM".
        let plain =
            datetime_cast_time(dt("2026-08-05 12:00:00"), Some("UTC"), Some("UTC")).unwrap();
        // tzname == conn_tzname, so no conversion happens at all.
        assert_eq!(plain, Some("12:00:00".to_string()));

        let shifted =
            datetime_cast_time(dt("2026-08-05 12:00:00"), Some("UTC+05:00"), Some("UTC")).unwrap();
        assert_eq!(shifted, Some("17:00:00".to_string()));

        let back =
            datetime_cast_time(dt("2026-08-05 12:00:00"), Some("UTC-02:30"), Some("UTC")).unwrap();
        assert_eq!(back, Some("09:30:00".to_string()));
    }

    #[test]
    fn an_unknown_zone_is_an_error_not_a_silent_passthrough() {
        let err = datetime_cast_date(dt("2026-08-05 00:00:00"), Some("Mars/Olympus"), Some("UTC"))
            .unwrap_err();
        assert!(matches!(err, UdfError::UnknownTimezone(_)), "{err:?}");
    }

    #[test]
    fn a_date_value_with_a_timezone_is_an_error_as_in_python() {
        // typecast_timestamp returns a `date` here, and Django's `dt.replace(tzinfo=...)` on a
        // date raises. Pinned so nobody "fixes" it into a silent midnight assumption.
        let err = datetime_cast_date(dt("2026-08-05"), Some("UTC"), Some("UTC")).unwrap_err();
        assert!(matches!(err, UdfError::Type(_)), "{err:?}");
    }

    // ---- NULL propagation -------------------------------------------------------------------

    #[test]
    fn null_inputs_yield_null() {
        assert_eq!(date_trunc(Some("year"), None, None, None).unwrap(), None);
        assert_eq!(date_trunc(None, dt("2026-08-05"), None, None).unwrap(), None);
        assert_eq!(datetime_trunc(Some("day"), None, None, None).unwrap(), None);
        assert_eq!(datetime_extract(Some("year"), None, None, None).unwrap(), None);
        assert_eq!(time_extract(Some("hour"), None).unwrap(), None);
        assert_eq!(time_trunc(Some("hour"), None, None, None).unwrap(), None);
        assert_eq!(datetime_cast_date(None, None, None).unwrap(), None);
        assert_eq!(datetime_cast_time(None, None, None).unwrap(), None);
        assert_eq!(time_diff(None, Some("12:00:00")).unwrap(), None);
        assert_eq!(time_diff(Some("12:00:00"), None).unwrap(), None);
        assert_eq!(timestamp_diff(None, None).unwrap(), None);
    }

    #[test]
    fn the_diff_functions_error_on_unparseable_input_rather_than_returning_null() {
        // These two have no try/except in Django, unlike every trunc/extract above. A NULL here
        // would let a broken query look like an empty result.
        assert!(time_diff(Some("garbage"), Some("12:00:00")).is_err());
        assert!(time_diff(Some(""), Some("12:00:00")).is_err());
        assert!(timestamp_diff(Some("garbage"), Some("2026-08-05 00:00:00")).is_err());
        assert!(timestamp_diff(Some(""), Some("2026-08-05 00:00:00")).is_err());
        // Mixing a date and a datetime is a Python TypeError, not a midnight coercion.
        assert!(timestamp_diff(Some("2026-08-05"), Some("2026-08-05 00:00:00")).is_err());
    }

    #[test]
    fn casting_a_bare_date_errors_because_a_date_has_no_date_method() {
        // Reads like a no-op and is not: `datetime.date` has no `.date()`, so Django raises.
        assert!(datetime_cast_date(Some("2026-08-05"), None, None).is_err());
        assert!(datetime_cast_time(Some("2026-08-05"), None, None).is_err());
        // A real datetime casts fine.
        assert_eq!(
            datetime_cast_date(Some("2026-08-05 12:00:00"), None, None).unwrap(),
            Some("2026-08-05".to_string())
        );
    }

    #[test]
    fn unparseable_inputs_yield_null_not_an_error() {
        // Django catches TypeError/ValueError around typecast_timestamp and returns None.
        assert_eq!(date_trunc(Some("year"), dt("garbage"), None, None).unwrap(), None);
        assert_eq!(
            datetime_extract(Some("year"), dt("garbage"), None, None).unwrap(),
            None
        );
        assert_eq!(time_extract(Some("hour"), dt("garbage")).unwrap(), None);
    }

    // ---- unsupported lookups ----------------------------------------------------------------

    #[test]
    fn unsupported_lookup_types_error() {
        assert!(matches!(
            date_trunc(Some("fortnight"), dt("2026-08-05"), None, None),
            Err(UdfError::UnsupportedLookup(_))
        ));
        assert!(matches!(
            datetime_trunc(Some("fortnight"), dt("2026-08-05 00:00:00"), None, None),
            Err(UdfError::UnsupportedLookup(_))
        ));
        assert!(matches!(
            time_trunc(Some("fortnight"), dt("12:00:00"), None, None),
            Err(UdfError::UnsupportedLookup(_))
        ));
        assert!(datetime_extract(Some("nonsense"), dt("2026-08-05 00:00:00"), None, None).is_err());
        assert!(time_extract(Some("nonsense"), dt("12:00:00")).is_err());
    }

    #[test]
    fn time_attributes_on_a_bare_date_error() {
        // A DateField value has no hour. Django raises AttributeError; returning 0 would be worse
        // than erroring because the query would silently succeed with wrong results.
        assert!(datetime_extract(Some("hour"), dt("2026-08-05"), None, None).is_err());
        assert!(datetime_trunc(Some("hour"), dt("2026-08-05"), None, None).is_err());
        assert!(datetime_cast_time(dt("2026-08-05"), None, None).is_err());
    }

    // ---- diffs ------------------------------------------------------------------------------

    #[test]
    fn time_diff_is_signed_microseconds() {
        assert_eq!(
            time_diff(dt("12:00:00"), dt("11:00:00")).unwrap(),
            Some(3_600_000_000)
        );
        assert_eq!(
            time_diff(dt("11:00:00"), dt("12:00:00")).unwrap(),
            Some(-3_600_000_000)
        );
        assert_eq!(time_diff(dt("12:00:00"), dt("12:00:00")).unwrap(), Some(0));
        assert_eq!(
            time_diff(dt("00:00:00.000001"), dt("00:00:00")).unwrap(),
            Some(1)
        );
    }

    #[test]
    fn timestamp_diff_is_signed_microseconds() {
        assert_eq!(
            timestamp_diff(dt("2026-08-05 12:00:00"), dt("2026-08-05 11:00:00")).unwrap(),
            Some(3_600_000_000)
        );
        assert_eq!(
            timestamp_diff(dt("2026-08-05 11:00:00"), dt("2026-08-05 12:00:00")).unwrap(),
            Some(-3_600_000_000)
        );
        // Across a day boundary.
        assert_eq!(
            timestamp_diff(dt("2026-08-06 00:00:00"), dt("2026-08-05 00:00:00")).unwrap(),
            Some(86_400_000_000)
        );
        // Bare dates subtract as whole days.
        assert_eq!(
            timestamp_diff(dt("2026-08-06"), dt("2026-08-05")).unwrap(),
            Some(86_400_000_000)
        );
    }

    // ---- format_dtdelta ---------------------------------------------------------------------

    fn text(s: &str) -> Option<DtOut> {
        Some(DtOut::Text(s.to_string()))
    }

    #[test]
    fn dtdelta_adds_two_microsecond_counts() {
        // The finding's own smoke case: django_format_dtdelta('+', 1, 2).
        assert_eq!(
            format_dtdelta(Some("+"), Some(DtParam::Delta(1)), Some(DtParam::Delta(2))).unwrap(),
            text("0:00:00.000003")
        );
    }

    #[test]
    fn dtdelta_subtraction_can_go_negative_and_prints_pythons_way() {
        assert_eq!(
            format_dtdelta(Some("-"), Some(DtParam::Delta(1)), Some(DtParam::Delta(4))).unwrap(),
            text("-1 day, 23:59:59.999997")
        );
    }

    #[test]
    fn dtdelta_datetime_plus_duration() {
        let base = NaiveDate::from_ymd_opt(2026, 8, 5)
            .unwrap()
            .and_hms_opt(12, 0, 0)
            .unwrap();
        assert_eq!(
            format_dtdelta(
                Some("+"),
                Some(DtParam::DateTime(base)),
                Some(DtParam::Delta(3_600_000_000))
            )
            .unwrap(),
            text("2026-08-05 13:00:00")
        );
        assert_eq!(
            format_dtdelta(
                Some("-"),
                Some(DtParam::DateTime(base)),
                Some(DtParam::Delta(3_600_000_000))
            )
            .unwrap(),
            text("2026-08-05 11:00:00")
        );
    }

    #[test]
    fn dtdelta_datetime_minus_datetime_is_a_duration() {
        let a = NaiveDate::from_ymd_opt(2026, 8, 5).unwrap().and_hms_opt(12, 0, 0).unwrap();
        let b = NaiveDate::from_ymd_opt(2026, 8, 5).unwrap().and_hms_opt(11, 30, 0).unwrap();
        assert_eq!(
            format_dtdelta(Some("-"), Some(DtParam::DateTime(a)), Some(DtParam::DateTime(b)))
                .unwrap(),
            text("0:30:00")
        );
    }

    #[test]
    fn dtdelta_scaling_uses_plain_numbers_not_microseconds() {
        // The connector changes how the SAME integer is read. Under '*' a 2 is a scalar.
        //
        // And the RESULT TYPE differs from the +/- branches: Django does not `str()` these, so
        // they come back as SQLite INTEGER/REAL. Returning "12" as text here would make
        // `WHERE duration * 2 = 12` stop matching, because SQLite does not compare text to an
        // integer as equal.
        assert_eq!(
            format_dtdelta(Some("*"), Some(DtParam::Integer(3)), Some(DtParam::Integer(4)))
                .unwrap(),
            Some(DtOut::Int(12))
        );
        assert_eq!(
            format_dtdelta(Some("/"), Some(DtParam::Integer(9)), Some(DtParam::Integer(2)))
                .unwrap(),
            Some(DtOut::Float(4.5))
        );
        // Python 3 true division yields a float even when it divides evenly.
        assert_eq!(
            format_dtdelta(Some("/"), Some(DtParam::Integer(8)), Some(DtParam::Integer(2)))
                .unwrap(),
            Some(DtOut::Float(4.0))
        );
    }

    #[test]
    fn dtdelta_null_and_error_paths() {
        assert_eq!(format_dtdelta(None, Some(DtParam::Delta(1)), Some(DtParam::Delta(1))).unwrap(), None);
        assert_eq!(format_dtdelta(Some("+"), None, Some(DtParam::Delta(1))).unwrap(), None);
        assert_eq!(format_dtdelta(Some("+"), Some(DtParam::Delta(1)), None).unwrap(), None);
        assert!(format_dtdelta(
            Some("/"),
            Some(DtParam::Integer(1)),
            Some(DtParam::Integer(0))
        )
        .is_err());
    }

    // The following four were found by the generated oracle, not by reading. Each is pinned by
    // name here as well, because the oracle says only *that* they diverged — these say *why*, and
    // a future reader "simplifying" one needs to see the reason before the 12k-case table fails.

    #[test]
    fn dtdelta_date_minus_subday_duration_stays_on_the_same_day() {
        // CPython negates the timedelta's `.days` FIELD, not the whole timedelta. A sub-day delta
        // has `.days == 0`, so subtracting it does not move the date. Negating the delta first
        // normalizes to `days = -1` and moves the date back a day — wrong, and wrong in a way that
        // looks completely reasonable.
        let d = NaiveDate::from_ymd_opt(2026, 8, 5).unwrap();
        assert_eq!(
            format_dtdelta(Some("-"), Some(DtParam::Date(d)), Some(DtParam::Delta(1))).unwrap(),
            text("2026-08-05")
        );
        // A negative sub-day delta DOES carry a -1 day, so subtracting it moves forward.
        assert_eq!(
            format_dtdelta(Some("-"), Some(DtParam::Date(d)), Some(DtParam::Delta(-1))).unwrap(),
            text("2026-08-06")
        );
        // A whole day behaves the obvious way.
        assert_eq!(
            format_dtdelta(
                Some("-"),
                Some(DtParam::Date(d)),
                Some(DtParam::Delta(86_400_000_000))
            )
            .unwrap(),
            text("2026-08-04")
        );
    }

    #[test]
    fn dtdelta_refuses_to_mix_a_date_and_a_datetime() {
        let d = NaiveDate::from_ymd_opt(2026, 8, 5).unwrap();
        let dtv = d.and_hms_opt(12, 0, 0).unwrap();
        assert!(
            format_dtdelta(Some("-"), Some(DtParam::DateTime(dtv)), Some(DtParam::Date(d)))
                .is_err()
        );
        assert!(
            format_dtdelta(Some("-"), Some(DtParam::Date(d)), Some(DtParam::DateTime(dtv)))
                .is_err()
        );
    }

    #[test]
    fn dtdelta_multiplication_overflow_errors_instead_of_wrapping() {
        // Python ints are unbounded, so Django computes this and its sqlite3 driver then raises
        // OverflowError. A plain `*` would WRAP in the release build this ships as and hand back a
        // negative duration.
        let big = 86_400_000_000_i64;
        assert!(
            format_dtdelta(Some("*"), Some(DtParam::Integer(big)), Some(DtParam::Integer(big)))
                .is_err()
        );
    }

    #[test]
    fn the_empty_string_is_null_alone_but_an_error_with_a_timezone() {
        // `typecast_timestamp("")` returns None WITHOUT raising, so Django's try/except never
        // fires and the timezone step dereferences a None. Genuinely unparseable input raises
        // inside the try and comes back NULL. Two failure shapes, one input class apart.
        assert_eq!(date_trunc(Some("year"), dt(""), None, None).unwrap(), None);
        assert!(date_trunc(Some("year"), dt(""), Some("UTC"), Some("UTC")).is_err());
        assert_eq!(
            date_trunc(Some("year"), dt("garbage"), Some("UTC"), Some("UTC")).unwrap(),
            None
        );
    }

    #[test]
    fn dtdelta_connector_is_stripped() {
        // Django strips the connector because it arrives with surrounding whitespace from the SQL
        // template ("%s + %s" style interpolation).
        assert_eq!(
            format_dtdelta(Some(" + "), Some(DtParam::Delta(1)), Some(DtParam::Delta(2))).unwrap(),
            text("0:00:00.000003")
        );
    }

    #[test]
    fn prepare_param_reads_integers_as_microseconds_only_for_plus_and_minus() {
        assert_eq!(prepare_dtdelta_param("+", Some(5), None, None), Some(DtParam::Delta(5)));
        assert_eq!(prepare_dtdelta_param("-", Some(5), None, None), Some(DtParam::Delta(5)));
        assert_eq!(prepare_dtdelta_param("*", Some(5), None, None), Some(DtParam::Integer(5)));
        assert_eq!(prepare_dtdelta_param("/", Some(5), None, None), Some(DtParam::Integer(5)));
        assert_eq!(
            prepare_dtdelta_param("+", None, Some("2026-08-05 12:00:00"), None),
            Some(DtParam::DateTime(
                NaiveDate::from_ymd_opt(2026, 8, 5).unwrap().and_hms_opt(12, 0, 0).unwrap()
            ))
        );
    }
}
