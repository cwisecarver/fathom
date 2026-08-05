//! Differential test: replay every case in `tests/oracle/cases.jsonl` against this crate.
//!
//! Each expected value in that file is what **Django's own `_sqlite_*` function returned** for
//! those exact arguments, produced by `tests/oracle/generate_cases.py` running the vendored
//! reference implementation. So a failure here means fathom's extension and Django disagree about
//! what a query returns — which, for the "point an unchanged Django app at it" claim, is the whole
//! ballgame.
//!
//! This complements rather than replaces the hand-written `#[cfg(test)]` tests in each module:
//! those record *why* a behaviour is what it is (and are where a reader should look first), while
//! this establishes *that* the behaviour matches across ~12.5k combinations no one would write out
//! by hand. Four of my hand-written expectations were wrong when first written; generated ones
//! cannot be wrong in that direction.
//!
//! Regenerate with `cd tests/oracle && python3 generate_cases.py` after changing the vendored
//! Django code. CI needs no Python — it reads the committed file.

use std::collections::BTreeMap;
use std::fmt::Write as _;

use fathom_udf::datetime::{self, DtOut, DtParam};
use fathom_udf::scalars;
use serde_json::Value as J;

/// The shape of an outcome, normalized so Rust and Python results are comparable.
#[derive(Debug, PartialEq)]
enum Outcome {
    Null,
    Text(String),
    Int(i64),
    Float(f64),
    /// Django raised. We compare "did it fail", not which exception — the observable contract is
    /// that the tenant's query errors rather than silently returning something wrong.
    Error,
}

impl Outcome {
    fn matches(&self, other: &Outcome) -> bool {
        match (self, other) {
            (Outcome::Float(a), Outcome::Float(b)) => {
                (a - b).abs() <= 1e-9 * a.abs().max(b.abs()).max(1.0)
            }
            _ => self == other,
        }
    }
}

fn expected(v: &J) -> Outcome {
    let obj = v.as_object().expect("result must be an object");
    if obj.contains_key("null") {
        Outcome::Null
    } else if obj.contains_key("error") {
        Outcome::Error
    } else if let Some(t) = obj.get("text") {
        Outcome::Text(t.as_str().unwrap().to_string())
    } else if let Some(i) = obj.get("int") {
        Outcome::Int(i.as_i64().unwrap())
    } else if let Some(f) = obj.get("float") {
        Outcome::Float(f.as_f64().unwrap())
    } else {
        panic!("unrecognized result shape: {v}")
    }
}

/// `null` in the case file means SQL NULL; a JSON string is text.
fn arg_str(v: &J) -> Option<&str> {
    v.as_str()
}

fn arg_i64(v: &J) -> Option<i64> {
    v.as_i64()
}

fn from_str_result(r: Result<Option<String>, datetime::UdfError>) -> Outcome {
    match r {
        Ok(None) => Outcome::Null,
        Ok(Some(s)) => Outcome::Text(s),
        Err(_) => Outcome::Error,
    }
}

fn from_int_result(r: Result<Option<i64>, datetime::UdfError>) -> Outcome {
    match r {
        Ok(None) => Outcome::Null,
        Ok(Some(i)) => Outcome::Int(i),
        Err(_) => Outcome::Error,
    }
}

fn from_opt_int(o: Option<i64>) -> Outcome {
    match o {
        None => Outcome::Null,
        Some(i) => Outcome::Int(i),
    }
}

fn from_opt_text(o: Option<String>) -> Outcome {
    match o {
        None => Outcome::Null,
        Some(s) => Outcome::Text(s),
    }
}

/// Build a `DtParam` the way `lib.rs` does from a SQLite argument, so the oracle exercises the
/// same decode path the extension uses.
fn dtdelta_param(connector: &str, v: &J) -> Option<DtParam> {
    if v.is_null() {
        return None;
    }
    let int = v.as_i64();
    let text = v.as_str();
    let float = v.as_f64().filter(|_| v.is_f64());
    datetime::prepare_dtdelta_param(connector, int, text, float)
}

fn run(func: &str, args: &[J]) -> Outcome {
    match func {
        "django_date_trunc" => from_str_result(datetime::date_trunc(
            arg_str(&args[0]),
            arg_str(&args[1]),
            arg_str(&args[2]),
            arg_str(&args[3]),
        )),
        "django_datetime_trunc" => from_str_result(datetime::datetime_trunc(
            arg_str(&args[0]),
            arg_str(&args[1]),
            arg_str(&args[2]),
            arg_str(&args[3]),
        )),
        "django_time_trunc" => from_str_result(datetime::time_trunc(
            arg_str(&args[0]),
            arg_str(&args[1]),
            arg_str(&args[2]),
            arg_str(&args[3]),
        )),
        "django_date_extract" => from_int_result(datetime::datetime_extract(
            arg_str(&args[0]),
            arg_str(&args[1]),
            None,
            None,
        )),
        "django_datetime_extract" => from_int_result(datetime::datetime_extract(
            arg_str(&args[0]),
            arg_str(&args[1]),
            arg_str(&args[2]),
            arg_str(&args[3]),
        )),
        "django_time_extract" => {
            from_int_result(datetime::time_extract(arg_str(&args[0]), arg_str(&args[1])))
        }
        "django_datetime_cast_date" => from_str_result(datetime::datetime_cast_date(
            arg_str(&args[0]),
            arg_str(&args[1]),
            arg_str(&args[2]),
        )),
        "django_datetime_cast_time" => from_str_result(datetime::datetime_cast_time(
            arg_str(&args[0]),
            arg_str(&args[1]),
            arg_str(&args[2]),
        )),
        "django_time_diff" => {
            from_int_result(datetime::time_diff(arg_str(&args[0]), arg_str(&args[1])))
        }
        "django_timestamp_diff" => {
            from_int_result(datetime::timestamp_diff(arg_str(&args[0]), arg_str(&args[1])))
        }
        "django_format_dtdelta" => {
            let Some(connector) = arg_str(&args[0]) else {
                return Outcome::Null;
            };
            let lhs = dtdelta_param(connector, &args[1]);
            let rhs = dtdelta_param(connector, &args[2]);
            match datetime::format_dtdelta(Some(connector), lhs, rhs) {
                Ok(None) => Outcome::Null,
                Ok(Some(DtOut::Text(s))) => Outcome::Text(s),
                Ok(Some(DtOut::Int(i))) => Outcome::Int(i),
                Ok(Some(DtOut::Float(f))) => Outcome::Float(f),
                Err(_) => Outcome::Error,
            }
        }
        "LPAD" | "RPAD" => {
            let (Some(text), Some(len), Some(fill)) =
                (arg_str(&args[0]), arg_i64(&args[1]), arg_str(&args[2]))
            else {
                return Outcome::Null;
            };
            let out = if func == "LPAD" {
                scalars::lpad(text, len, fill)
            } else {
                scalars::rpad(text, len, fill)
            };
            Outcome::Text(out)
        }
        "REPEAT" => {
            let (Some(text), Some(count)) = (arg_str(&args[0]), arg_i64(&args[1])) else {
                return Outcome::Null;
            };
            Outcome::Text(scalars::repeat(text, count))
        }
        "REVERSE" => from_opt_text(arg_str(&args[0]).map(scalars::reverse)),
        "MD5" => from_opt_text(arg_str(&args[0]).map(scalars::md5_hex)),
        "SHA1" => from_opt_text(arg_str(&args[0]).map(scalars::sha1_hex)),
        "SHA224" => from_opt_text(arg_str(&args[0]).map(scalars::sha224_hex)),
        "SHA256" => from_opt_text(arg_str(&args[0]).map(scalars::sha256_hex)),
        "SHA384" => from_opt_text(arg_str(&args[0]).map(scalars::sha384_hex)),
        "SHA512" => from_opt_text(arg_str(&args[0]).map(scalars::sha512_hex)),
        "BITXOR" => {
            let (Some(x), Some(y)) = (arg_i64(&args[0]), arg_i64(&args[1])) else {
                return Outcome::Null;
            };
            Outcome::Int(scalars::bitxor(x, y))
        }
        other => panic!("no dispatch for {other}"),
    }
}

#[test]
fn matches_django_on_every_generated_case() {
    let raw = include_str!("oracle/cases.jsonl");
    let mut lines = raw.lines().filter(|l| !l.trim().is_empty());

    let meta: J = serde_json::from_str(lines.next().expect("meta line")).unwrap();
    assert_eq!(meta["_meta"], J::Bool(true), "first line must be the header");
    let declared = meta["count"].as_u64().unwrap() as usize;

    let mut failures: Vec<String> = Vec::new();
    let mut per_func: BTreeMap<String, usize> = BTreeMap::new();
    let mut diverging: BTreeMap<String, usize> = BTreeMap::new();
    let mut total = 0usize;

    for line in lines {
        let case: J = serde_json::from_str(line).expect("case line");
        let func = case["func"].as_str().unwrap();
        let args = case["args"].as_array().unwrap();
        let want = expected(&case["result"]);

        let got = run(func, args);
        total += 1;
        *per_func.entry(func.to_string()).or_default() += 1;

        if !got.matches(&want) {
            *diverging.entry(func.to_string()).or_default() += 1;
            // Cap the printed examples: a systematic break would otherwise emit 12k lines and
            // bury the signal about WHICH functions diverged. The per-function tally above is
            // uncapped, so the summary stays complete even when the examples are trimmed.
            if failures.len() < 40 {
                let mut s = String::new();
                let _ = write!(
                    s,
                    "{func}({}) => got {got:?}, django says {want:?}",
                    args.iter()
                        .map(|a| a.to_string())
                        .collect::<Vec<_>>()
                        .join(", ")
                );
                failures.push(s);
            }
        }
    }

    assert_eq!(
        total, declared,
        "the case file's declared count does not match the lines present"
    );

    if !failures.is_empty() {
        let mut summary = String::new();
        let diverging_total: usize = diverging.values().sum();
        let _ = writeln!(
            summary,
            "\n{diverging_total} of {total} generated cases disagree with Django.\n\
             Divergences by function: {diverging:?}\n\
             Coverage by function:    {per_func:?}\n\nExamples:",
        );
        for f in &failures {
            let _ = writeln!(summary, "  {f}");
        }
        panic!("{summary}");
    }

    // A guard on the guard: if a refactor silently stopped dispatching most cases, the test would
    // pass having checked almost nothing. The floor is well under the current 12.5k so ordinary
    // additions do not trip it.
    assert!(
        total > 10_000,
        "only {total} cases ran — the oracle table looks truncated"
    );
}
