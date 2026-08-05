//! Differential test: replay every case in `tests/oracle/re_cases.jsonl` against `pyre`.
//!
//! Each expectation is what **CPython's `re.search` actually returned** for that pattern and
//! subject (`tests/oracle/generate_re_cases.py`). Django's `__regex` / `__iregex` lookups are
//! exactly `re.search`, so a failure here is a query that returns different rows on fathom than it
//! does on Django's own SQLite backend — which is the whole compatibility claim.
//!
//! Two directions both count as divergence and both are checked:
//!
//!   * a pattern Python matches that we do not (or that we refuse to compile) — a working queryset
//!     that breaks or silently returns fewer rows;
//!   * a pattern Python REFUSES that we accept — rarer, but it means an app that fails fast on
//!     Django fails late here.

use std::collections::BTreeMap;
use std::fmt::Write as _;

use fathom_udf::pyre;
use serde_json::Value as J;

#[derive(Debug, PartialEq)]
enum Outcome {
    Match(bool),
    Error,
}

#[test]
fn matches_cpython_re_on_every_generated_case() {
    let raw = include_str!("oracle/re_cases.jsonl");
    let mut lines = raw.lines().filter(|l| !l.trim().is_empty());

    let meta: J = serde_json::from_str(lines.next().expect("meta line")).unwrap();
    assert_eq!(meta["_meta"], J::Bool(true));
    let declared = meta["count"].as_u64().unwrap() as usize;

    let mut failures: Vec<String> = Vec::new();
    let mut diverging: BTreeMap<String, usize> = BTreeMap::new();
    let mut total = 0usize;
    let mut matched = 0usize;

    for line in lines {
        let case: J = serde_json::from_str(line).expect("case line");
        let pattern = case["pattern"].as_str().unwrap();
        let subject = case["subject"].as_str().unwrap();

        let want = if case["result"].get("error").is_some() {
            Outcome::Error
        } else {
            Outcome::Match(case["result"]["match"].as_bool().unwrap())
        };

        let got = match pyre::search(pattern, subject) {
            Ok(m) => Outcome::Match(m),
            Err(_) => Outcome::Error,
        };

        total += 1;
        if let Outcome::Match(true) = want {
            matched += 1;
        }

        if got != want {
            *diverging.entry(pattern.to_string()).or_default() += 1;
            if failures.len() < 40 {
                let mut s = String::new();
                let _ = write!(
                    s,
                    "pattern {pattern:?} subject {subject:?} => got {got:?}, python says {want:?}"
                );
                failures.push(s);
            }
        }
    }

    assert_eq!(total, declared, "the case file looks truncated");

    if !failures.is_empty() {
        let n: usize = diverging.values().sum();
        let mut summary = String::new();
        let _ = writeln!(
            summary,
            "\n{n} of {total} cases disagree with CPython's re.search.\n\
             Diverging patterns: {diverging:?}\n\nExamples:"
        );
        for f in &failures {
            let _ = writeln!(summary, "  {f}");
        }
        panic!("{summary}");
    }

    // Guard on the guard: a corpus where nothing matches would pass trivially.
    assert!(total > 3_000, "only {total} cases ran");
    assert!(
        matched > 500,
        "only {matched} cases were expected to match — the corpus is not exercising much"
    );
}
