//! Python-compatible date/time/timedelta parsing and formatting.
//!
//! Django's SQLite UDFs are Python functions whose return values SQLite stores verbatim, and
//! Django's ORM then reads those values back and parses them. So the contract this extension has
//! to meet is not "a correct date string" — it is **byte-identical to what CPython's `str()`
//! would have produced**, because the converters on the way back (`django.db.backends.sqlite3
//! .operations.convert_datetimefield_value` and friends) parse exactly that shape.
//!
//! Three places where the obvious Rust implementation is wrong, all of them silent:
//!
//!   * `str(timedelta)` is NOT a simple signed H:MM:SS. Python normalizes a negative duration to
//!     a negative *day* count plus a positive time-of-day, so `timedelta(microseconds=-3)` prints
//!     `-1 day, 23:59:59.999997`. Formatting the absolute value with a leading `-` gives
//!     `-0:00:00.000003`, which is a different string for the same instant.
//!   * Microseconds are printed only when non-zero, and always as exactly 6 digits.
//!   * `typecast_timestamp` returns a **date** (not a datetime) when the input has no space in it,
//!     which changes what downstream attribute access is even legal. See [`Parsed`].
//!
//! Everything here is pure and total; the SQLite-facing wrappers in the sibling modules turn
//! `None`/`Err` into NULL or an error the same way a raised Python exception would.

use chrono::{Datelike, NaiveDate, NaiveDateTime, NaiveTime, Timelike};

/// Microseconds in a day — Python's `timedelta` normalization modulus.
const MICROS_PER_DAY: i64 = 86_400_000_000;
const MICROS_PER_HOUR: i64 = 3_600_000_000;
const MICROS_PER_MINUTE: i64 = 60_000_000;
const MICROS_PER_SECOND: i64 = 1_000_000;

/// What `django.db.backends.utils.typecast_timestamp` actually returns.
///
/// It returns a `datetime.date` for an input with no space (`"2026-08-05"`) and a
/// `datetime.datetime` otherwise. That distinction is load-bearing rather than cosmetic: a
/// `date` has no `.hour`, so `_sqlite_datetime_extract(lookup_type="hour", ...)` on a DateField
/// raises `AttributeError` in Django rather than returning 0 — and code that silently treated a
/// date as midnight would return 0 where Django errors.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Parsed {
    Date(NaiveDate),
    DateTime(NaiveDateTime),
}

impl Parsed {
    pub fn date(&self) -> NaiveDate {
        match self {
            Parsed::Date(d) => *d,
            Parsed::DateTime(dt) => dt.date(),
        }
    }

    /// The time component, or `None` for a bare date — the caller decides whether that is an
    /// error (attribute access) or simply absent.
    pub fn time(&self) -> Option<NaiveTime> {
        match self {
            Parsed::Date(_) => None,
            Parsed::DateTime(dt) => Some(dt.time()),
        }
    }

    /// Python's `str(value)`: `"YYYY-MM-DD"` for a date, `"YYYY-MM-DD HH:MM:SS[.ffffff]"` for a
    /// datetime.
    pub fn py_str(&self) -> String {
        match self {
            Parsed::Date(d) => format_date(*d),
            Parsed::DateTime(dt) => format_datetime(*dt),
        }
    }
}

/// `str(datetime)` — the fractional part appears only when non-zero, and then as exactly 6 digits.
pub fn format_datetime(dt: NaiveDateTime) -> String {
    let micros = dt.time().nanosecond() / 1000;
    if micros == 0 {
        dt.format("%Y-%m-%d %H:%M:%S").to_string()
    } else {
        format!("{}.{:06}", dt.format("%Y-%m-%d %H:%M:%S"), micros)
    }
}

/// `str(date)`.
pub fn format_date(d: NaiveDate) -> String {
    d.format("%Y-%m-%d").to_string()
}

/// `time.isoformat()` — `"HH:MM:SS"`, or `"HH:MM:SS.ffffff"` when microseconds are non-zero.
/// Note this is `isoformat()`, which is what `_sqlite_datetime_cast_time` calls, not `str()`.
/// For `datetime.time` the two agree.
pub fn format_time(t: NaiveTime) -> String {
    let micros = t.nanosecond() / 1000;
    if micros == 0 {
        t.format("%H:%M:%S").to_string()
    } else {
        format!("{}.{:06}", t.format("%H:%M:%S"), micros)
    }
}

/// Python's `str(timedelta)`.
///
/// CPython normalizes a `timedelta` so that `0 <= microseconds < 1_000_000`,
/// `0 <= seconds < 86400`, and all the sign lives in `days`. The printed form is
/// `[-]D day[s], H:MM:SS[.ffffff]` with the day part omitted when days == 0, and the hour
/// **not** zero-padded.
///
/// So `-3µs` prints as `-1 day, 23:59:59.999997`, not `-0:00:00.000003`. Django parses these back
/// with `django.utils.dateparse.parse_duration`, whose regex expects exactly this shape.
pub fn format_timedelta(total_micros: i64) -> String {
    // Floor division / modulo, matching Python's `divmod` on negatives (Rust's `/` truncates
    // toward zero and `%` takes the sign of the dividend, which would give the wrong day count).
    let days = total_micros.div_euclid(MICROS_PER_DAY);
    let rem = total_micros.rem_euclid(MICROS_PER_DAY);

    let hours = rem / MICROS_PER_HOUR;
    let minutes = (rem % MICROS_PER_HOUR) / MICROS_PER_MINUTE;
    let seconds = (rem % MICROS_PER_MINUTE) / MICROS_PER_SECOND;
    let micros = rem % MICROS_PER_SECOND;

    let mut out = String::new();
    if days != 0 {
        // Python pluralizes on the day COUNT, so -1 is "-1 day" and -2 is "-2 days".
        let unit = if days == 1 || days == -1 { "day" } else { "days" };
        out.push_str(&format!("{days} {unit}, "));
    }
    out.push_str(&format!("{hours}:{minutes:02}:{seconds:02}"));
    if micros != 0 {
        out.push_str(&format!(".{micros:06}"));
    }
    out
}

/// `django.db.backends.utils.typecast_time`.
///
/// Deliberately permissive in the same way Python's is: it splits on `:` and pads the fractional
/// part out to 6 digits with `(microseconds + "000000")[:6]`, so `"12:00:00.5"` is 500000µs, not
/// 5µs. Truncating rather than rounding is also Python's behaviour for >6 digits.
pub fn typecast_time(s: &str) -> Option<NaiveTime> {
    if s.is_empty() {
        return None;
    }
    let mut parts = s.splitn(3, ':');
    let hour: u32 = parts.next()?.trim().parse().ok()?;
    let minute: u32 = parts.next()?.trim().parse().ok()?;
    let sec_part = parts.next()?;

    let (seconds, micros) = split_fractional_seconds(sec_part)?;
    NaiveTime::from_hms_micro_opt(hour, minute, seconds, micros)
}

/// `django.db.backends.utils.typecast_date`.
pub fn typecast_date(s: &str) -> Option<NaiveDate> {
    if s.is_empty() {
        return None;
    }
    let mut parts = s.split('-');
    let year: i32 = parts.next()?.trim().parse().ok()?;
    let month: u32 = parts.next()?.trim().parse().ok()?;
    let day: u32 = parts.next()?.trim().parse().ok()?;
    if parts.next().is_some() {
        return None;
    }
    NaiveDate::from_ymd_opt(year, month, day)
}

/// `django.db.backends.utils.typecast_timestamp`.
///
/// Faithful to two quirks that look like bugs and are not:
///   * No space in the input ⇒ a **date**, via `typecast_date`.
///   * Any timezone suffix on the TIME part is discarded, not applied — Python splits the time at
///     the first `-` or `+` and throws the remainder away ("does NOT store time zone information").
pub fn typecast_timestamp(s: &str) -> Option<Parsed> {
    if s.is_empty() {
        return None;
    }
    if !s.contains(' ') {
        return typecast_date(s).map(Parsed::Date);
    }

    let mut halves = s.splitn(2, ' ');
    let d = halves.next()?;
    let t = halves.next()?;

    // Strip (and discard) the offset exactly as Python does: first `-`, else first `+`.
    let t = match t.find('-') {
        Some(i) => &t[..i],
        None => match t.find('+') {
            Some(i) => &t[..i],
            None => t,
        },
    };

    let date = typecast_date(d)?;

    let mut times = t.splitn(3, ':');
    let hour: u32 = times.next()?.trim().parse().ok()?;
    let minute: u32 = times.next()?.trim().parse().ok()?;
    let sec_part = times.next()?;
    let (seconds, micros) = split_fractional_seconds(sec_part)?;

    let time = NaiveTime::from_hms_micro_opt(hour, minute, seconds, micros)?;
    Some(Parsed::DateTime(date.and_time(time)))
}

/// `"07"` -> `(7, 0)`, `"07.5"` -> `(7, 500000)`, `"07.1234567"` -> `(7, 123456)`.
/// Mirrors Python's `int((microseconds + "000000")[:6])` — right-pad then truncate.
fn split_fractional_seconds(s: &str) -> Option<(u32, u32)> {
    let s = s.trim();
    let (sec_str, frac_str) = match s.split_once('.') {
        Some((a, b)) => (a, b),
        None => (s, "0"),
    };
    let seconds: u32 = sec_str.parse().ok()?;
    if !frac_str.bytes().all(|b| b.is_ascii_digit()) {
        return None;
    }
    let padded: String = frac_str.chars().chain(std::iter::repeat('0')).take(6).collect();
    let micros: u32 = padded.parse().ok()?;
    Some((seconds, micros))
}

/// `django.utils.timezone.split_tzname_delta` — splits `"America/New_York+05:00"` into
/// `("America/New_York", '+', "05:00")`.
///
/// Uses `rsplit` on the sign (the LAST occurrence), which matters for zone names that legitimately
/// contain one: `"Etc/GMT-5"` must not be read as zone `"Etc/GMT"` with a `-5` delta unless the
/// remainder actually parses as a time. `+` is tried before `-`, as in Django.
pub fn split_tzname_delta(tzname: &str) -> (&str, Option<char>, Option<String>) {
    for sign in ['+', '-'] {
        if let Some(pos) = tzname.rfind(sign) {
            let name = &tzname[..pos];
            let offset = &tzname[pos + 1..];
            if !offset.is_empty() {
                if let Some(normalized) = parse_offset_time(offset) {
                    return (name, Some(sign), Some(normalized));
                }
            }
        }
    }
    (tzname, None, None)
}

/// Validate-and-normalize the offset half of a tzname delta, standing in for Django's
/// `parse_time(offset)` followed by its `if ":" not in offset` padding.
///
/// Requires `HH:MM[:SS[.ffffff]]` — a **colon is mandatory**.
///
/// Django's guard is `parse_time(offset)`, which tries `time.fromisoformat` and falls back to
/// `time_re`, a regex that requires `(\d{1,2}):(\d{1,2})`. A bare hour therefore does not parse,
/// and `split_tzname_delta("UTC+5")` returns the string WHOLE — so Django goes on to
/// `ZoneInfo("UTC+5")` and raises `ZoneInfoNotFoundError`.
///
/// An earlier version here accepted a bare hour on the theory that `time.fromisoformat("5")`
/// works on Python 3.11+. It does not (ISO 8601 wants two digits), and even `"05"` is
/// version-dependent — which is why this now matches the regex, the one behaviour that is the
/// same on every Python. It costs nothing: Django derives these offsets from
/// `timezone._get_timezone_name`, which always emits `±HH:MM`. Caught by the generated oracle
/// (12 cases), not by reading.
///
/// Returns the offset string **as Django leaves it**, un-normalized — `"5:30"` stays `"5:30"`.
fn parse_offset_time(offset: &str) -> Option<String> {
    if !offset.contains(':') {
        return None;
    }
    // Validate it really is a time before accepting the split — this is Django's `parse_time`
    // guard, and it is what keeps "Africa/Porto-Novo" from being read as a zone plus a delta.
    typecast_time(offset).or_else(|| {
        // `HH:MM` with no seconds is valid ISO time but not `typecast_time`'s 3-part shape.
        let mut parts = offset.splitn(2, ':');
        let hour: u32 = parts.next()?.trim().parse().ok()?;
        let minute: u32 = parts.next()?.trim().parse().ok()?;
        NaiveTime::from_hms_opt(hour, minute, 0)
    })?;
    Some(offset.to_string())
}

/// The offset in microseconds that a normalized `"HH:MM"` delta represents.
pub fn offset_micros(offset: &str) -> Option<i64> {
    let mut parts = offset.splitn(2, ':');
    let hours: i64 = parts.next()?.parse().ok()?;
    let minutes: i64 = parts.next()?.parse().ok()?;
    Some(hours * MICROS_PER_HOUR + minutes * MICROS_PER_MINUTE)
}

/// `django.utils.duration.duration_microseconds` — a timedelta as a whole number of microseconds.
pub fn duration_microseconds(dt: NaiveDateTime, other: NaiveDateTime) -> i64 {
    (dt - other).num_microseconds().unwrap_or(0)
}

/// Python's `date.isocalendar()` week number.
pub fn iso_week(d: NaiveDate) -> u32 {
    d.iso_week().week()
}

/// Python's `date.isocalendar()` year.
pub fn iso_year(d: NaiveDate) -> i32 {
    d.iso_week().year()
}

/// Python's `date.isoweekday()` — Monday = 1 … Sunday = 7.
pub fn isoweekday(d: NaiveDate) -> u32 {
    d.weekday().number_from_monday()
}

/// Python's `date.weekday()` — Monday = 0 … Sunday = 6.
pub fn weekday(d: NaiveDate) -> u32 {
    d.weekday().num_days_from_monday()
}

#[cfg(test)]
mod tests {
    use super::*;

    // ---- format_timedelta -------------------------------------------------------------------
    //
    // The values here are what CPython actually prints. The negative cases are the point of the
    // table: a "format the absolute value with a minus sign" implementation passes every positive
    // row and fails every negative one, and Django's parse_duration would then silently read the
    // wrong duration back rather than erroring.

    #[test]
    fn timedelta_matches_cpython_str() {
        let cases: &[(i64, &str)] = &[
            (0, "0:00:00"),
            (3, "0:00:00.000003"),
            (1_000_000, "0:00:01"),
            (1_500_000, "0:00:01.500000"),
            (60_000_000, "0:01:00"),
            (3_600_000_000, "1:00:00"),
            (86_400_000_000, "1 day, 0:00:00"),
            (172_800_000_000, "2 days, 0:00:00"),
            (90_061_000_000, "1 day, 1:01:01"),
            // Negatives: CPython carries the sign in `days` and keeps the time positive.
            (-3, "-1 day, 23:59:59.999997"),
            (-1_000_000, "-1 day, 23:59:59"),
            (-86_400_000_000, "-1 day, 0:00:00"),
            (-86_400_000_001, "-2 days, 23:59:59.999999"),
            (-172_800_000_000, "-2 days, 0:00:00"),
        ];
        for (micros, expected) in cases {
            assert_eq!(&format_timedelta(*micros), expected, "micros={micros}");
        }
    }

    // ---- typecast_timestamp -----------------------------------------------------------------

    #[test]
    fn timestamp_without_space_is_a_date_not_a_midnight_datetime() {
        // The distinction Django depends on: a DateField value has no time component at all.
        match typecast_timestamp("2026-08-05") {
            Some(Parsed::Date(d)) => assert_eq!(d, NaiveDate::from_ymd_opt(2026, 8, 5).unwrap()),
            other => panic!("expected a bare date, got {other:?}"),
        }
        assert!(typecast_timestamp("2026-08-05").unwrap().time().is_none());
    }

    #[test]
    fn timestamp_with_space_is_a_datetime() {
        let p = typecast_timestamp("2026-08-05 12:34:56").unwrap();
        assert_eq!(
            p,
            Parsed::DateTime(
                NaiveDate::from_ymd_opt(2026, 8, 5)
                    .unwrap()
                    .and_hms_opt(12, 34, 56)
                    .unwrap()
            )
        );
    }

    #[test]
    fn timestamp_discards_the_offset_rather_than_applying_it() {
        // Python's docstring is explicit: "does NOT store time zone information". A version that
        // APPLIED the offset would shift every value by hours and still look plausible.
        let with_offset = typecast_timestamp("2005-07-29 15:48:00.590358-05").unwrap();
        let without = typecast_timestamp("2005-07-29 15:48:00.590358").unwrap();
        assert_eq!(with_offset, without);

        let plus = typecast_timestamp("2005-07-29 09:56:00+02:00").unwrap();
        assert_eq!(plus, typecast_timestamp("2005-07-29 09:56:00").unwrap());
    }

    #[test]
    fn timestamp_fractional_seconds_are_right_padded_then_truncated() {
        // "12:00:00.5" is half a second (500000µs), NOT 5µs.
        let half = typecast_timestamp("2026-08-05 12:00:00.5").unwrap();
        assert_eq!(half.time().unwrap().nanosecond() / 1000, 500_000);

        // More than 6 digits truncates, it does not round.
        let long = typecast_timestamp("2026-08-05 12:00:00.1234567").unwrap();
        assert_eq!(long.time().unwrap().nanosecond() / 1000, 123_456);
    }

    #[test]
    fn timestamp_rejects_garbage() {
        assert!(typecast_timestamp("").is_none());
        assert!(typecast_timestamp("not a date").is_none());
        assert!(typecast_timestamp("2026-13-05").is_none());
        assert!(typecast_timestamp("2026-02-30").is_none());
        assert!(typecast_timestamp("2026-08-05 25:00:00").is_none());
    }

    // ---- typecast_time ----------------------------------------------------------------------

    #[test]
    fn time_parsing() {
        assert_eq!(
            typecast_time("12:34:56"),
            NaiveTime::from_hms_opt(12, 34, 56)
        );
        assert_eq!(
            typecast_time("12:34:56.000001"),
            NaiveTime::from_hms_micro_opt(12, 34, 56, 1)
        );
        assert_eq!(typecast_time(""), None);
        assert_eq!(typecast_time("12:34"), None, "typecast_time needs 3 parts");
        assert_eq!(typecast_time("99:00:00"), None);
    }

    // ---- formatting -------------------------------------------------------------------------

    #[test]
    fn datetime_str_omits_zero_microseconds_and_pads_non_zero_to_six() {
        let base = NaiveDate::from_ymd_opt(2026, 8, 5).unwrap();
        assert_eq!(
            format_datetime(base.and_hms_opt(1, 2, 3).unwrap()),
            "2026-08-05 01:02:03"
        );
        assert_eq!(
            format_datetime(base.and_hms_micro_opt(1, 2, 3, 7).unwrap()),
            "2026-08-05 01:02:03.000007"
        );
        assert_eq!(
            format_time(NaiveTime::from_hms_opt(0, 0, 0).unwrap()),
            "00:00:00"
        );
        assert_eq!(
            format_time(NaiveTime::from_hms_micro_opt(0, 0, 0, 500_000).unwrap()),
            "00:00:00.500000"
        );
    }

    // ---- split_tzname_delta -----------------------------------------------------------------

    #[test]
    fn tzname_delta_splitting() {
        assert_eq!(
            split_tzname_delta("America/New_York"),
            ("America/New_York", None, None)
        );
        assert_eq!(
            split_tzname_delta("America/New_York+05:00"),
            ("America/New_York", Some('+'), Some("05:00".to_string()))
        );
        assert_eq!(
            split_tzname_delta("America/New_York-03:30"),
            ("America/New_York", Some('-'), Some("03:30".to_string()))
        );
        // Un-normalized: a single-digit hour stays single-digit.
        assert_eq!(
            split_tzname_delta("Europe/London+5:30"),
            ("Europe/London", Some('+'), Some("5:30".to_string()))
        );
    }

    #[test]
    fn a_bare_hour_offset_does_not_split() {
        // Django's `parse_time` needs a colon, so "UTC+5" is NOT ("UTC", "+", "5:00") — it stays
        // whole and Django then fails to find a zone named "UTC+5". Accepting it here would make
        // fathom answer a query Django itself errors on.
        assert_eq!(split_tzname_delta("UTC+5"), ("UTC+5", None, None));
        assert_eq!(split_tzname_delta("Etc/GMT-5"), ("Etc/GMT-5", None, None));
    }

    #[test]
    fn a_tzname_whose_tail_is_not_a_time_is_left_whole() {
        // The `parse_time` guard is what keeps a hyphenated zone name intact.
        assert_eq!(
            split_tzname_delta("Africa/Porto-Novo"),
            ("Africa/Porto-Novo", None, None)
        );
        assert_eq!(
            split_tzname_delta("America/Port-au-Prince"),
            ("America/Port-au-Prince", None, None)
        );
    }

    #[test]
    fn offset_micros_conversion() {
        assert_eq!(offset_micros("05:00"), Some(5 * MICROS_PER_HOUR));
        assert_eq!(
            offset_micros("03:30"),
            Some(3 * MICROS_PER_HOUR + 30 * MICROS_PER_MINUTE)
        );
    }

    // ---- calendar helpers -------------------------------------------------------------------

    #[test]
    fn weekday_conventions_do_not_get_swapped() {
        // 2026-08-05 is a Wednesday. Python: weekday()==2 (Mon=0), isoweekday()==3 (Mon=1).
        let d = NaiveDate::from_ymd_opt(2026, 8, 5).unwrap();
        assert_eq!(weekday(d), 2);
        assert_eq!(isoweekday(d), 3);

        // Sunday is the case where the two conventions differ most and where Django's
        // week_day ((isoweekday % 7) + 1) is easiest to get wrong.
        let sunday = NaiveDate::from_ymd_opt(2026, 8, 9).unwrap();
        assert_eq!(weekday(sunday), 6);
        assert_eq!(isoweekday(sunday), 7);
        assert_eq!((isoweekday(sunday) % 7) + 1, 1, "Django week_day: Sunday == 1");

        let monday = NaiveDate::from_ymd_opt(2026, 8, 3).unwrap();
        assert_eq!((isoweekday(monday) % 7) + 1, 2, "Django week_day: Monday == 2");
    }

    #[test]
    fn iso_calendar_year_can_differ_from_the_calendar_year() {
        // 2027-01-01 is a Friday, in ISO week 53 of ISO year 2026. A naive `d.year()` would
        // report 2027 and the `__iso_year` lookup would silently disagree with Django.
        let d = NaiveDate::from_ymd_opt(2027, 1, 1).unwrap();
        assert_eq!(iso_year(d), 2026);
        assert_eq!(iso_week(d), 53);
    }
}
