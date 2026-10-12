//! Table-driven "hostile input" test: every pure function behind a registered scalar/aggregate is
//! called with the values a tenant can put in a literal, and must return `Ok` or `Err` — never
//! panic (expert review 2026-10-12 #1, #2).
//!
//! Why it matters: a panic in a UDF is reachable by ANY tenant (a `:ro` token included) with one
//! SELECT, and it is a node-wide outage. Before the 2026-10-12 fixes, the first row of this table
//! that reached `format_dtdelta` with an `i64::MAX` delta panicked inside chrono, and `REPEAT` /
//! `LPAD` / `RPAD` with `i64::MAX` panicked (or asked for tens of GB) before SQLite's
//! `SQLITE_LIMIT_LENGTH` could apply, because that limit checks only the returned value.
//!
//! What this does NOT cover: the SQLite binding layer in `src/lib.rs` (`opt_text` etc.), which
//! cannot run in a unit test because the crate links SQLite through the loadable-extension API.
//! That layer has no arithmetic; `opt_text` maps an invalid-UTF-8 text value to an `Err`, and a
//! BLOB to NULL, before any function here sees it, so the "invalid UTF-8" input below is the
//! lossy decoding (U+FFFD). The SQL-level behaviour is covered by the Elixir extension tests.

use std::panic::{catch_unwind, AssertUnwindSafe};
use std::time::{Duration, Instant};

use fathom_udf::aggregates::{BitAgg, BitOp, Variance};
use fathom_udf::datetime::{self, DtParam};
use fathom_udf::{pyre, scalars};

/// SQLite's tenant `SQLITE_LIMIT_LENGTH` (`pragma_guard::LIMIT_LENGTH`).
const CAP: usize = 64 * 1024 * 1024;

fn strings() -> Vec<String> {
    let mib = 1024 * 1024;
    vec![
        String::new(),
        " ".into(),
        "x".into(),
        "a".repeat(mib),
        // A 1 MiB string that is mostly separators, to stress the splitters/parsers.
        ": - + . ".repeat(mib / 8),
        String::from_utf8_lossy(&[0x66, 0xff, 0xfe, 0x80, 0x6f]).into_owned(),
        "\u{1F642}é".into(),
        "2026-01-01 00:00:00".into(),
        "2026-01-01".into(),
        "9999-12-31 23:59:59.999999".into(),
        "9999-12-31".into(),
        "0001-01-01 00:00:00".into(),
        "0001-01-01".into(),
        "0000-01-01 00:00:00".into(),
        "10000-01-01 00:00:00".into(),
        "262143-12-31 23:59:59".into(),
        "-262144-01-01 00:00:00".into(),
        "2147483647-12-31 23:59:59".into(),
        "99999999999999999999-01-01".into(),
        "2026-01-01 99999999999999999999:00:00".into(),
        "12:00:00".into(),
        "23:59:59.999999".into(),
        "UTC".into(),
        "UTC+23:59".into(),
        "UTC-23:59".into(),
        "UTC+99999999999999999999:00".into(),
        "UTC+9223372036854775807:00".into(),
        "America/New_York".into(),
        "America/New_York+05:00".into(),
        "Pacific/Kiritimati".into(),
        "Etc/GMT+12".into(),
        "+".into(),
        "-".into(),
        "*".into(),
        "/".into(),
        "year".into(),
        "week".into(),
        "week_day".into(),
        "quarter".into(),
        "hour".into(),
        "second".into(),
        "iso_year".into(),
        "(".into(),
        "[".into(),
        "(a*)*\\1$".into(),
        "(?P=x)".into(),
        "[]$".repeat(1000),
        "^(a|b)*\\1+$".into(),
    ]
}

fn ints() -> Vec<i64> {
    vec![
        i64::MIN,
        i64::MIN + 1,
        -86_400_000_000,
        -1,
        0,
        1,
        86_400_000_000,
        253_402_300_800_000_000, // 10000-01-01 in micros since 0001
        i64::MAX / 2,
        i64::MAX - 1,
        i64::MAX,
    ]
}

fn floats() -> Vec<f64> {
    vec![
        f64::NAN,
        f64::INFINITY,
        f64::NEG_INFINITY,
        f64::MAX,
        f64::MIN,
        f64::MIN_POSITIVE,
        0.0,
        -0.0,
        1.5,
    ]
}

/// Run `f`, recording any panic (with `what` naming the call) instead of unwinding the test.
fn guard(panics: &mut Vec<String>, what: impl FnOnce() -> String, f: impl FnOnce()) {
    if catch_unwind(AssertUnwindSafe(f)).is_err() {
        panics.push(what());
    }
}

fn short(s: &str) -> String {
    let t: String = s.chars().take(40).collect();
    format!("{t:?}(len {})", s.len())
}

#[test]
fn date_time_functions_never_panic() {
    let mut panics: Vec<String> = Vec::new();
    let strs = strings();
    let opt: Vec<Option<&str>> = std::iter::once(None)
        .chain(strs.iter().map(|s| Some(s.as_str())))
        .collect();

    // ---- date/time: text-in functions ---------------------------------------------------------
    // Keep the cross product tractable: the two 1 MiB strings only ride in the dt/lhs/rhs slots.
    let small: Vec<Option<&str>> = opt
        .iter()
        .copied()
        .filter(|s| s.map_or(true, |s| s.len() < 4096))
        .collect();

    for &lookup in &small {
        for &dt in &opt {
            guard(
                &mut panics,
                || format!("time_extract({lookup:?}, {})", dt.map_or("None".into(), short)),
                || {
                    let _ = datetime::time_extract(lookup, dt);
                },
            );
            for &tz in &small {
                for &conn in &small {
                    guard(
                        &mut panics,
                        || {
                            format!(
                                "date/time fn(lookup {lookup:?}, dt {}, tz {tz:?}, conn {conn:?})",
                                dt.map_or("None".into(), short)
                            )
                        },
                        || {
                            let _ = datetime::datetime_extract(lookup, dt, tz, conn);
                            let _ = datetime::date_trunc(lookup, dt, tz, conn);
                            let _ = datetime::datetime_trunc(lookup, dt, tz, conn);
                            let _ = datetime::time_trunc(lookup, dt, tz, conn);
                            let _ = datetime::datetime_cast_date(dt, tz, conn);
                            let _ = datetime::datetime_cast_time(dt, tz, conn);
                        },
                    );
                }
            }
        }
    }

    for &l in &opt {
        for &r in &opt {
            guard(
                &mut panics,
                || {
                    format!(
                        "time_diff/timestamp_diff({}, {})",
                        l.map_or("None".into(), short),
                        r.map_or("None".into(), short)
                    )
                },
                || {
                    let _ = datetime::time_diff(l, r);
                    let _ = datetime::timestamp_diff(l, r);
                },
            );
        }
    }

    report(&panics);
}

#[test]
fn format_dtdelta_never_panics() {
    let mut panics: Vec<String> = Vec::new();
    let strs = strings();
    // ---- format_dtdelta: every connector x every operand shape ---------------------------------
    let mut params: Vec<Option<DtParam>> = vec![None];
    for i in ints() {
        params.push(Some(DtParam::Delta(i)));
        params.push(Some(DtParam::Integer(i)));
    }
    for f in floats() {
        params.push(Some(DtParam::Number(f)));
    }
    for s in &strs {
        if s.len() < 4096 {
            if let Some(p) = datetime::prepare_dtdelta_param("+", None, Some(s), None) {
                params.push(Some(p));
            }
        }
    }
    for connector in ["+", "-", "*", "/", " + ", "", "%", "garbage"] {
        for l in &params {
            for r in &params {
                guard(
                    &mut panics,
                    || format!("format_dtdelta({connector:?}, {l:?}, {r:?})"),
                    || {
                        let _ = datetime::format_dtdelta(Some(connector), l.clone(), r.clone());
                    },
                );
            }
        }
        for &i in &ints() {
            for &f in &floats() {
                for s in strs.iter().filter(|s| s.len() < 4096) {
                    guard(
                        &mut panics,
                        || format!("prepare_dtdelta_param({connector:?}, {i}, {s:?}, {f})"),
                        || {
                            let _ = datetime::prepare_dtdelta_param(connector, Some(i), None, None);
                            let _ = datetime::prepare_dtdelta_param(connector, None, Some(s), Some(f));
                        },
                    );
                }
            }
        }
    }

    report(&panics);
}

#[test]
fn text_builders_never_panic_or_exceed_the_cap() {
    let mut panics: Vec<String> = Vec::new();
    let strs = strings();
    // ---- text builders, with the 64 MiB cap SQLite applies to a tenant handle -------------------
    for s in &strs {
        for &n in &ints() {
            for fill in strs.iter().filter(|f| f.len() < 4096) {
                guard(
                    &mut panics,
                    || format!("LPAD/RPAD({}, {n}, {})", short(s), short(fill)),
                    || {
                        if let Ok(out) = scalars::lpad(s, n, fill, CAP) {
                            assert!(out.len() <= CAP);
                        }
                        if let Ok(out) = scalars::rpad(s, n, fill, CAP) {
                            assert!(out.len() <= CAP);
                        }
                    },
                );
            }
            guard(
                &mut panics,
                || format!("REPEAT({}, {n})", short(s)),
                || {
                    if let Ok(out) = scalars::repeat(s, n, CAP) {
                        assert!(out.len() <= CAP);
                    }
                },
            );
        }
        guard(
            &mut panics,
            || format!("REVERSE/hashes({})", short(s)),
            || {
                let _ = scalars::reverse(s);
                let _ = scalars::md5_hex(s);
                let _ = scalars::sha1_hex(s);
                let _ = scalars::sha224_hex(s);
                let _ = scalars::sha256_hex(s);
                let _ = scalars::sha384_hex(s);
                let _ = scalars::sha512_hex(s);
            },
        );
    }

    report(&panics);
}

#[test]
fn numeric_scalars_and_aggregates_never_panic() {
    let mut panics: Vec<String> = Vec::new();
    // ---- numeric scalars and aggregates --------------------------------------------------------
    for &a in &ints() {
        for &b in &ints() {
            guard(
                &mut panics,
                || format!("BITXOR({a}, {b})"),
                || {
                    let _ = scalars::bitxor(a, b);
                },
            );
        }
    }
    for &f in &floats() {
        guard(
            &mut panics,
            || format!("COT({f})"),
            || {
                let _ = scalars::cot(f);
            },
        );
    }
    let mut v = Variance::default();
    guard(
        &mut panics,
        || "Variance aggregate".to_string(),
        || {
            for f in floats() {
                v.step(f);
            }
            for i in ints() {
                v.step(i as f64);
            }
            let _ = (v.var_pop(), v.var_samp(), v.stddev_pop(), v.stddev_samp(), v.count());
        },
    );
    for op in [BitOp::And, BitOp::Or, BitOp::Xor] {
        let mut b = BitAgg::default();
        guard(
            &mut panics,
            || format!("BitAgg {:?}", 0),
            || {
                for i in ints() {
                    b.step(op, i);
                }
                for f in floats() {
                    b.step(op, f as i64);
                }
                let _ = b.value();
            },
        );
    }

    report(&panics);
}

fn report(panics: &[String]) {
    assert!(
        panics.is_empty(),
        "{} call(s) PANICKED (each is a node-wide abort when reachable from SQL):\n{}",
        panics.len(),
        panics.iter().take(25).cloned().collect::<Vec<_>>().join("\n")
    );
}

/// `regexp` runs tenant-supplied patterns over tenant-supplied subjects: every call must return
/// (Ok or Err, never panic) and within a time ceiling. fancy-regex caps backtracking at 1M steps,
/// which this test relies on rather than changes (lowering it is a client-visible decision).
#[test]
fn regexp_never_panics_and_stays_under_a_time_ceiling() {
    const CEILING: Duration = Duration::from_secs(5);

    let strs = strings();
    let mut panics = Vec::new();
    let mut slow = Vec::new();
    for pattern in &strs {
        for subject in &strs {
            let t = Instant::now();
            guard(
                &mut panics,
                || format!("regexp({}, {})", short(pattern), short(subject)),
                || {
                    let _ = pyre::search(pattern, subject);
                },
            );
            if t.elapsed() > CEILING {
                slow.push(format!(
                    "regexp({}, {}) took {:?}",
                    short(pattern),
                    short(subject),
                    t.elapsed()
                ));
            }
        }
    }
    // Pathological backtracking shapes against a long subject.
    let subject = format!("{}d", "a".repeat(100_000));
    for pattern in ["^(a*)*\\1$", "(a|aa)+\\1c", "(a+)+\\1b", "^(?=(a+)+)\\1b"] {
        let t = Instant::now();
        guard(
            &mut panics,
            || format!("regexp({pattern:?}, long a-run)"),
            || {
                let _ = pyre::search(pattern, &subject);
            },
        );
        if t.elapsed() > CEILING {
            slow.push(format!("regexp({pattern:?}, long a-run) took {:?}", t.elapsed()));
        }
    }

    assert!(panics.is_empty(), "regexp panicked:\n{}", panics.join("\n"));
    assert!(slow.is_empty(), "regexp exceeded {CEILING:?}:\n{}", slow.join("\n"));
}
