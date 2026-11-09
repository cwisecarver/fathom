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
//!
//! Zero disagreements, except the ones the generator lists in `known_divergences` (a documented,
//! deliberate choice each: `tests/oracle/generate_re_cases.py`). Those are replayed too, and the
//! test fails if a listed divergence has STOPPED diverging, so a fix forces its entry to be
//! deleted and the list cannot go stale (expert review 2026-11-07 #88).

use std::collections::{BTreeMap, BTreeSet};
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

    // pattern -> the subjects it is allowed to differ on (`None`: every subject).
    let known: BTreeMap<String, Option<BTreeSet<String>>> = meta["known_divergences"]
        .as_array()
        .expect("known_divergences")
        .iter()
        .map(|d| {
            let subjects = d["subjects"].as_array().map(|a| {
                a.iter()
                    .map(|s| s.as_str().unwrap().to_string())
                    .collect::<BTreeSet<_>>()
            });
            (d["pattern"].as_str().unwrap().to_string(), subjects)
        })
        .collect();
    // (pattern, subject) cases of a known divergence that did diverge.
    let mut diverged: BTreeSet<(String, String)> = BTreeSet::new();

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

        // The `regexp` UDF does not call `search`: it takes a `Compiled` handle once per statement
        // (SQLite auxdata) and runs `is_match` per row. That path must give CPython's answer too.
        let via_handle = match pyre::compile_cached(pattern).and_then(|(c, _)| c.is_match(subject))
        {
            Ok(m) => Outcome::Match(m),
            Err(_) => Outcome::Error,
        };
        assert_eq!(
            via_handle, got,
            "pattern {pattern:?} subject {subject:?}: the statement-handle path disagrees with search"
        );

        total += 1;
        if let Outcome::Match(true) = want {
            matched += 1;
        }

        let allowed = known
            .get(pattern)
            .is_some_and(|subjects| subjects.as_ref().is_none_or(|s| s.contains(subject)));

        if got != want && allowed {
            diverged.insert((pattern.to_string(), subject.to_string()));
        } else if got != want {
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

    // A known divergence that no longer diverges is a stale entry: the difference was fixed (or the
    // listed subject never differed), so the entry would now excuse a future regression.
    let mut stale: Vec<String> = Vec::new();
    for (pattern, subjects) in &known {
        match subjects {
            None => {
                if !diverged.iter().any(|(p, _)| p == pattern) {
                    stale.push(format!("{pattern:?} (all subjects)"));
                }
            }
            Some(subjects) => {
                for subject in subjects {
                    if !diverged.contains(&(pattern.clone(), subject.clone())) {
                        stale.push(format!("{pattern:?} on {subject:?}"));
                    }
                }
            }
        }
    }
    assert!(
        stale.is_empty(),
        "known_divergences entries that no longer diverge (delete them from \
         generate_re_cases.py): {stale:?}"
    );

    // Guard on the guard: a corpus where nothing matches would pass trivially.
    assert!(total > 9_000, "only {total} cases ran");
    assert!(
        matched > 500,
        "only {matched} cases were expected to match — the corpus is not exercising much"
    );
}
