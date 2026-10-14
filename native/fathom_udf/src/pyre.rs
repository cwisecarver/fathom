//! Python `re` compatibility for Django's `regexp` UDF.
//!
//! Django's `__regex` / `__iregex` lookups run `re.search(pattern, string)` client-side. Under
//! fathom the pattern crosses the wire and is matched here, so "exact compatibility with Django"
//! means: **for every pattern and subject, this module returns what `re.search` would return.**
//!
//! Two classes of difference had to be closed, and the second is the one that mattered more.
//!
//! ## Patterns Rust's `regex` crate refuses outright
//!
//! Lookahead `(?=)` `(?!)`, lookbehind `(?<=)` `(?<!)`, and backreferences `\1` are all ordinary
//! Python and all rejected at compile time by `regex`. A tenant's queryset got an error where
//! Django returned rows. Fixed by using `fancy-regex`, which backtracks.
//!
//! ## Patterns that compile in BOTH and quietly disagree
//!
//! `$`. In Python, without `re.MULTILINE`, `$` matches at the end of the string **or just before a
//! newline at the end of the string**. In Rust it matches only at the very end. So
//! `re.search("abc$", "abc\n")` is `True` and the Rust equivalent is `false` — same pattern, same
//! subject, opposite answer, no error anywhere. Text coming out of a form or a file very often has
//! a trailing newline, so this is not a corner case.
//!
//! The fix is to rewrite a bare `$` as `(?=\n?\z)`, which is exactly Python's rule. Doing that
//! needs to know whether a given `$` is a real anchor — not escaped, not inside a character class
//! — and whether `(?m)` is in effect, which is why [`translate`] is a small scanner rather than a
//! `String::replace`. A naive replace corrupts `[a$b]` and `\$`.
//!
//! **Not a general regex parser**, and deliberately not: it only classifies enough of the syntax to
//! find real `$`, `\Z` and `(?P=name)` tokens, and passes everything else through untouched. Every
//! rule here is pinned by a differential test against CPython (`tests/oracle/generate_re_cases.py`).

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use fancy_regex::Regex;

/// Rewrites a Python `re` pattern into the equivalent `fancy-regex` pattern.
///
/// Three substitutions, all of them tokens `fancy-regex` either spells differently or does not
/// have:
///
/// | Python        | here            | why |
/// |---------------|-----------------|-----|
/// | `$` (no `(?m)`) | `(?=\n?\z)`   | Python's `$` also matches before ONE trailing newline |
/// | `\Z`          | `\z`            | same meaning, different spelling |
/// | `(?P=name)`   | `\k<name>`      | same meaning, different spelling |
///
/// `^`, `\A`, `\b`, `\d`, `\w`, `(?P<name>…)`, `(?i)`, `(?s)`, `(?m)` and the rest already agree.
pub fn translate(pattern: &str) -> String {
    // Python applies inline global flags wherever they appear (and since 3.11 requires them at the
    // start). `(?m)` changes what `$` means, so find it before rewriting any anchor.
    let multiline = has_multiline_flag(pattern);

    let mut out = String::with_capacity(pattern.len() + 8);
    let chars: Vec<char> = pattern.chars().collect();
    let mut i = 0;
    // Position of the last `)`: an unclosed `(?P=` is then rejected in O(1) instead of rescanning
    // to the end of the pattern each time.
    let last_close = chars.iter().rposition(|&c| c == ')');
    // Depth is not enough: `[` inside a class is a literal, so this is a flag, not a counter.
    let mut in_class = false;
    // A `]` in the FIRST position of a class is a literal `]`, not the terminator — `[]]` is a
    // class matching one `]`. Kept as a BYTE offset into `out` (`out.len()` is O(1); the old
    // `out.chars().count()` was O(n) per bracket, so N bracket pairs cost O(N^2) — expert review
    // 2026-10-12 #1). Only relative position matters, and the `[` / `^` it is compared against
    // are ASCII.
    let mut class_start = 0usize;

    while i < chars.len() {
        let c = chars[i];

        match c {
            '\\' if i + 1 < chars.len() => {
                let next = chars[i + 1];
                if !in_class && next == 'Z' {
                    // Python `\Z` == absolute end of string == Rust `\z`. (Rust has no `\Z`, which
                    // is why this was a hard error rather than a wrong answer.)
                    out.push_str("\\z");
                } else {
                    out.push(c);
                    out.push(next);
                }
                i += 2;
                continue;
            }

            '[' if !in_class => {
                in_class = true;
                class_start = out.len();
                out.push(c);
            }

            ']' if in_class => {
                // Only closes the class if it is not the first character (`[]abc]`) and not first
                // after a leading negation (`[^]abc]`).
                let pos = out.len();
                let first =
                    pos == class_start + 1 || (pos == class_start + 2 && out.ends_with('^'));
                if !first {
                    in_class = false;
                }
                out.push(c);
            }

            '$' if !in_class && !multiline => {
                // The whole reason this module exists.
                out.push_str("(?=\\n?\\z)");
            }

            '(' if !in_class && named_backref_close(&chars, i, last_close).is_some() => {
                // `(?P=name)` -> `\k<name>`
                let close = named_backref_close(&chars, i, last_close).unwrap();
                let name: String = chars[i + 4..close].iter().collect();
                out.push_str("\\k<");
                out.push_str(&name);
                out.push('>');
                i = close + 1;
                continue;
            }

            _ => out.push(c),
        }

        i += 1;
    }

    out
}

/// Index of the `)` closing a `(?P=name` that starts at `i`, if it is one.
///
/// `last_close` is the index of the pattern's final `)`. With no `)` at or after `i` the answer is
/// known without scanning; otherwise the scan stops at the first `)` and the caller jumps past it,
/// so total work is linear (expert review 2026-10-12 #1: an unclosed `(?P=` used to rescan to the
/// end of the pattern each time, O(N^2)).
fn named_backref_close(chars: &[char], i: usize, last_close: Option<usize>) -> Option<usize> {
    if chars.get(i..i + 4) != Some(&['(', '?', 'P', '=']) || last_close.is_none_or(|l| l < i) {
        return None;
    }
    chars[i..].iter().position(|&c| c == ')').map(|p| p + i)
}

/// Whether an inline `(?m)`-style flag group enables multiline.
///
/// Scans for `(?<letters>)` and `(?<letters>:` groups rather than searching for the literal
/// `"(?m)"`, so `(?im)` and `(?ms:...)` are recognized too — both of which a Django app can send
/// and either of which would otherwise leave `$` rewritten the wrong way.
fn has_multiline_flag(pattern: &str) -> bool {
    let chars: Vec<char> = pattern.chars().collect();
    let mut i = 0;

    while i + 2 < chars.len() {
        if chars[i] == '\\' {
            i += 2;
            continue;
        }
        if chars[i] == '(' && chars[i + 1] == '?' {
            let mut j = i + 2;
            let mut seen_m = false;
            while j < chars.len() && chars[j].is_ascii_alphabetic() {
                if chars[j] == 'm' {
                    seen_m = true;
                }
                j += 1;
            }
            // Only a flag group if the letters are followed by `)` or `:`; `(?=`, `(?!`, `(?P<`
            // and friends are not.
            if seen_m && j < chars.len() && (chars[j] == ')' || chars[j] == ':') {
                return true;
            }
        }
        i += 1;
    }

    false
}

const CACHE_CAP: usize = 256;

/// Patterns longer than this are refused before any work is done (expert review 2026-10-12 #1/#2):
/// translation and compilation both run uninterruptibly inside one UDF call, where neither the
/// statement watchdog nor the progress handler can stop them. A Django regex this long is not a
/// realistic workload.
pub const MAX_PATTERN_BYTES: usize = 64 * 1024;

/// Compile-size tiers (expert review 2026-10-12 #2). A pattern is compiled under the smallest tier
/// that accepts it, and that tier is what the cache CHARGES for it, because `fancy_regex::Regex`
/// does not report its size. The top tier is the regex crate's own 10 MiB default: realistic
/// Django patterns need up to ~8 MiB (`(?i)\w{100}` and `^[\w.@+-]{1,150}$` measure between 4 and
/// 8 MiB because Unicode `\w` is large), so a tighter per-regex limit would be a compatibility
/// regression. Memory is bounded by [`CACHE_BYTE_BUDGET`] instead, and a single pathological
/// compile (`(?i)\w{1000}` needs ~128 MiB) is refused at the top tier. `backtrack_limit` is
/// deliberately left at the crate default (1M, expert review 2026-10-12 #5).
const COMPILE_TIERS: [usize; 4] = [256 * 1024, 1024 * 1024, 4 * 1024 * 1024, 10 * 1024 * 1024];

/// Lazy-DFA cache limit per regex.
const DFA_SIZE_LIMIT: usize = 4 * 1024 * 1024;

/// Budget for the whole cache, in charged bytes (see [`COMPILE_TIERS`]). 64 MiB holds eight of the
/// largest accepted programs, so 256 distinct `(?i)\w{100}x<i>` patterns (1.8 GB RSS before) now
/// churn the cache instead of accumulating.
const CACHE_BYTE_BUDGET: usize = 64 * 1024 * 1024;

/// A cached compiled pattern (see [`Cache`]).
struct Entry {
    re: Arc<Regex>,
    /// Needs fancy-regex's backtracking VM (lookaround, backreference, atomic group, ...), judged on
    /// the ORIGINAL pattern: the `$` translation introduces a lookahead that is not a reason to cap.
    fancy: bool,
    cost: usize,
    /// Last-use tick, for LRU eviction.
    used: u64,
}

/// Compiled-pattern cache.
///
/// SQLite's per-statement auxdata already caches a constant pattern for the life of a statement,
/// but a pattern bound as a PARAMETER (`WHERE col REGEXP ?`, which is exactly what a Django
/// queryset sends) gets no auxdata at all and would otherwise recompile per row. Bounded, because
/// a pattern is tenant-supplied: an unbounded map keyed on client input is a memory-growth
/// primitive. Over budget, least-recently-used entries are evicted one at a time (expert review
/// 2026-10-13 #32): the old whole-map `clear()` let one tenant cycling big patterns wipe every
/// other tenant's small hot ones.
struct Cache {
    map: HashMap<String, Entry>,
    bytes: usize,
    tick: u64,
    /// Patterns that failed to compile (expert review 2026-10-13 #32): a failing pattern re-ran all
    /// four compile tiers (32-85 ms) on every call. The error is deterministic per pattern.
    failed: HashMap<String, (fancy_regex::Error, u64)>,
    failed_bytes: usize,
}

static CACHE: Mutex<Option<Cache>> = Mutex::new(None);

/// Budget for the negative cache, in pattern bytes (patterns are the keys, up to 64 KiB each).
const FAILED_BYTE_BUDGET: usize = 4 * 1024 * 1024;

/// Fancy-path (backtracking) subjects over this are refused (expert review 2026-10-13 #1). The
/// whole match is one uninterruptible UDF call, and fancy-regex re-runs its delegate sub-regex
/// from every start position, so lookaround patterns are quadratic in subject length (measured
/// 47/181/719 ms at 20k/40k/80k bytes; 56 s at 1 MB), bypassing the statement deadline and the
/// backstop. `backtrack_limit` counts VM steps, not delegate rescans, so it never trips. Plain
/// patterns run on the linear `regex` engine and are not capped.
pub const MAX_FANCY_SUBJECT_BYTES: usize = 64 * 1024;

/// True when the pattern needs the backtracking engine. `regex-syntax` rejects exactly the
/// constructs fancy-regex adds (lookaround, backreferences, atomic groups); any other parse failure
/// is also treated as fancy, which is the conservative side.
fn needs_backtracking(pattern: &str) -> bool {
    regex_syntax::Parser::new().parse(pattern).is_err()
}

/// Why a REGEXP call failed.
#[derive(Debug)]
pub enum PyreError {
    /// The pattern exceeds [`MAX_PATTERN_BYTES`].
    PatternTooLong(usize),
    /// A lookaround/backreference pattern was asked to scan a subject over
    /// [`MAX_FANCY_SUBJECT_BYTES`].
    SubjectTooLong(usize),
    /// `fancy-regex` refused to compile or run it.
    Regex(fancy_regex::Error),
}

impl std::fmt::Display for PyreError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            PyreError::PatternTooLong(n) => write!(
                f,
                "regular expression is {n} bytes, over the {MAX_PATTERN_BYTES}-byte limit"
            ),
            PyreError::SubjectTooLong(n) => write!(
                f,
                "text is {n} bytes, over the {MAX_FANCY_SUBJECT_BYTES}-byte limit for \
                 regular expressions with lookaround or backreferences"
            ),
            PyreError::Regex(e) => e.fmt(f),
        }
    }
}

impl std::error::Error for PyreError {}

impl From<fancy_regex::Error> for PyreError {
    fn from(e: fancy_regex::Error) -> Self {
        PyreError::Regex(e)
    }
}

/// Compiles under the smallest tier that accepts the pattern; returns the regex and the bytes the
/// cache should charge for it. Parse errors return immediately (no tier will help).
fn compile(pattern: &str) -> Result<(Regex, usize), fancy_regex::Error> {
    let translated = translate(pattern);
    let mut last = None;
    for tier in COMPILE_TIERS {
        match fancy_regex::RegexBuilder::new(&translated)
            .delegate_size_limit(tier)
            .delegate_dfa_size_limit(DFA_SIZE_LIMIT)
            .build()
        {
            Ok(re) => return Ok((re, tier + pattern.len())),
            Err(e @ fancy_regex::Error::CompileError(_)) => last = Some(e),
            Err(e) => return Err(e),
        }
    }
    Err(last.expect("COMPILE_TIERS is non-empty"))
}

/// `re.search(pattern, subject)` — whether the pattern matches ANYWHERE in the subject.
///
/// Python's `re.search` is unanchored, which is also `fancy_regex::Regex::is_match`'s behaviour;
/// `re.match` (anchored at the start) is a different function and not what Django's `regexp` uses.
///
/// The cache lock is held only for the map lookup/insert, never across `Regex::new` or
/// `is_match` (expert review 2026-10-12 #5): fancy-regex allows up to 1M backtracking steps per
/// match, so a pathological pattern run over many rows used to hold this process-global mutex for
/// the whole match and serialize every other tenant's REGEXP behind it (each waiter pinning a
/// dirty-IO scheduler). The compiled regex is cloned out as an `Arc` and matched lock-free. Two
/// threads missing on the same new pattern both compile it and the second insert wins, which is
/// harmless.
pub fn search(pattern: &str, subject: &str) -> Result<bool, PyreError> {
    if pattern.len() > MAX_PATTERN_BYTES {
        return Err(PyreError::PatternTooLong(pattern.len()));
    }
    let (re, fancy) = match cache_get(pattern)? {
        Some(hit) => hit,
        None => match compile(pattern) {
            Ok((re, cost)) => {
                let re = Arc::new(re);
                let fancy = needs_backtracking(pattern);
                cache_put(pattern, &re, fancy, cost);
                (re, fancy)
            }
            Err(e) => {
                cache_put_failed(pattern, &e);
                return Err(e.into());
            }
        },
    };
    if fancy && subject.len() > MAX_FANCY_SUBJECT_BYTES {
        return Err(PyreError::SubjectTooLong(subject.len()));
    }
    Ok(re.is_match(subject)?)
}

/// A hit, `None` for a miss, or the remembered compile error.
fn cache_get(pattern: &str) -> Result<Option<(Arc<Regex>, bool)>, PyreError> {
    let mut guard = CACHE.lock().unwrap_or_else(|e| e.into_inner());
    let Some(cache) = guard.as_mut() else {
        return Ok(None);
    };
    cache.tick += 1;
    let tick = cache.tick;
    if let Some(entry) = cache.map.get_mut(pattern) {
        entry.used = tick;
        return Ok(Some((Arc::clone(&entry.re), entry.fancy)));
    }
    if let Some((err, used)) = cache.failed.get_mut(pattern) {
        *used = tick;
        return Err(err.clone().into());
    }
    Ok(None)
}

fn cache_put(pattern: &str, re: &Arc<Regex>, fancy: bool, cost: usize) {
    let mut guard = CACHE.lock().unwrap_or_else(|e| e.into_inner());
    let cache = guard.get_or_insert_with(new_cache);
    while !cache.map.is_empty()
        && (cache.map.len() >= CACHE_CAP || cache.bytes + cost > CACHE_BYTE_BUDGET)
    {
        let oldest = cache
            .map
            .iter()
            .min_by_key(|(_, e)| e.used)
            .map(|(k, _)| k.clone())
            .expect("non-empty");
        if let Some(e) = cache.map.remove(&oldest) {
            cache.bytes -= e.cost;
        }
    }
    cache.tick += 1;
    let entry = Entry {
        re: Arc::clone(re),
        fancy,
        cost,
        used: cache.tick,
    };
    // Two threads missing on one pattern both compile it; the second insert replaces the first.
    if let Some(old) = cache.map.insert(pattern.to_string(), entry) {
        cache.bytes -= old.cost;
    }
    cache.bytes += cost;
}

fn cache_put_failed(pattern: &str, err: &fancy_regex::Error) {
    let mut guard = CACHE.lock().unwrap_or_else(|e| e.into_inner());
    let cache = guard.get_or_insert_with(new_cache);
    while !cache.failed.is_empty()
        && (cache.failed.len() >= CACHE_CAP
            || cache.failed_bytes + pattern.len() > FAILED_BYTE_BUDGET)
    {
        let oldest = cache
            .failed
            .iter()
            .min_by_key(|(_, (_, used))| *used)
            .map(|(k, _)| k.clone())
            .expect("non-empty");
        cache.failed.remove(&oldest);
        cache.failed_bytes -= oldest.len();
    }
    cache.tick += 1;
    if cache
        .failed
        .insert(pattern.to_string(), (err.clone(), cache.tick))
        .is_none()
    {
        cache.failed_bytes += pattern.len();
    }
}

fn new_cache() -> Cache {
    Cache {
        map: HashMap::new(),
        bytes: 0,
        tick: 0,
        failed: HashMap::new(),
        failed_bytes: 0,
    }
}

/// Estimated bytes currently held by the cache (test hook).
#[cfg(test)]
fn cache_bytes() -> usize {
    let guard = CACHE.lock().unwrap_or_else(|e| e.into_inner());
    guard.as_ref().map_or(0, |c| c.bytes)
}

/// Whether a pattern is currently in the positive / negative cache (test hook).
#[cfg(test)]
fn cached(pattern: &str) -> (bool, bool) {
    let guard = CACHE.lock().unwrap_or_else(|e| e.into_inner());
    guard.as_ref().map_or((false, false), |c| {
        (c.map.contains_key(pattern), c.failed.contains_key(pattern))
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn m(pattern: &str, subject: &str) -> bool {
        search(pattern, subject).unwrap()
    }

    #[test]
    fn dollar_matches_before_one_trailing_newline() {
        // The silent divergence. Python: True. Rust's `regex`: false. No error either way, which
        // is what made it worth a translation layer rather than a documented limitation.
        assert!(m("abc$", "abc\n"));
        assert!(m("abc$", "abc"));
        // ...but only ONE trailing newline, and only at the very end.
        assert!(!m("abc$", "abc\n\n"));
        assert!(!m("abc$", "abc\ndef"));
    }

    #[test]
    fn dollar_inside_a_character_class_is_a_literal() {
        // A `String::replace("$", ...)` corrupts this into a class containing `(?=\n?\z)`.
        assert!(m("[a$]", "$"));
        assert!(m("[$]", "$"));
        assert!(!m("[a$]", "b"));
        // Including when `]` is the first class member.
        assert!(m("[]$]", "$"));
    }

    #[test]
    fn an_escaped_dollar_is_a_literal() {
        assert!(m("\\$", "$"));
        assert!(m("costs \\$5", "costs $5"));
        assert!(!m("\\$", "x"));
    }

    #[test]
    fn multiline_dollar_keeps_rust_semantics() {
        // With (?m), Python's `$` is end-of-line, which already matches Rust's.
        assert!(m("(?m)abc$", "abc\ndef"));
        assert!(m("(?im)ABC$", "abc\ndef"));
        assert!(m("(?ms:abc$)", "abc\ndef"));
    }

    #[test]
    fn lookaround_and_backreferences_work() {
        assert!(m("foo(?=bar)", "foobar"));
        assert!(!m("foo(?=bar)", "foobaz"));
        assert!(m("foo(?!bar)", "foobaz"));
        assert!(m("(?<=USD)\\d+", "USD100"));
        assert!(m("(?<!USD)\\d+", "EUR100"));
        assert!(m("(\\w)\\1", "aabb"));
        assert!(!m("(\\w)\\1", "abcd"));
        assert!(m("^(.+)\\1$", "abcabc"));
    }

    #[test]
    fn python_spellings_are_translated() {
        assert!(m("abc\\Z", "abc"));
        assert!(!m("abc\\Z", "abc\n"), "\\Z is the ABSOLUTE end, unlike $");
        assert!(m("(?P<x>a)(?P=x)", "aa"));
        assert!(!m("(?P<x>a)(?P=x)", "ab"));
    }

    #[test]
    fn translate_leaves_ordinary_patterns_alone() {
        assert_eq!(translate("^a"), "^a");
        assert_eq!(translate("\\d+"), "\\d+");
        assert_eq!(translate("(foo|bar)"), "(foo|bar)");
        assert_eq!(translate("(?i)HELLO"), "(?i)HELLO");
        assert_eq!(translate("a{2,3}"), "a{2,3}");
    }

    #[test]
    fn translate_rewrites_only_real_anchors() {
        assert_eq!(translate("a$"), "a(?=\\n?\\z)");
        assert_eq!(translate("\\$"), "\\$");
        assert_eq!(translate("[a$]"), "[a$]");
        assert_eq!(translate("(?m)a$"), "(?m)a$");
        assert_eq!(translate("a\\Z"), "a\\z");
    }

    #[test]
    fn the_cache_returns_correct_answers_across_patterns() {
        // A cache keyed on the pattern must not leak one pattern's compiled form to another.
        for _ in 0..3 {
            assert!(m("^a", "abc"));
            assert!(!m("^b", "abc"));
            assert!(m("c$", "abc"));
        }
    }

    #[test]
    fn an_invalid_pattern_is_an_error_not_a_panic() {
        assert!(search("(unclosed", "x").is_err());
        assert!(search("[", "x").is_err());
    }

    // expert review 2026-10-12 #1: `out.chars().count()` per bracket made translate O(N^2), run
    // uninterruptibly inside one UDF call. Unfixed this takes ~3.5 s at 200k pairs.
    #[test]
    fn translate_is_linear_in_pattern_length() {
        use std::time::{Duration, Instant};
        let n = 200_000;
        let big = "[a]".repeat(n);
        let t = Instant::now();
        let out = translate(&big);
        let t1 = t.elapsed();
        assert_eq!(out.len(), big.len());
        assert!(
            t1 < Duration::from_millis(200),
            "translate of {n} classes took {t1:?}"
        );

        // Doubling ratio: a quadratic algorithm is ~4x; linear is ~2x. Floor the base so noise on
        // a fast run cannot flip the ratio.
        let time = |k: usize| {
            let p = "[a]".repeat(k);
            let t = Instant::now();
            let _ = translate(&p);
            t.elapsed().max(Duration::from_millis(1))
        };
        let (a, b) = (time(100_000), time(200_000));
        assert!(b < a * 3, "doubling the pattern took {a:?} -> {b:?}");

        // Unclosed `(?P=` used to rescan to the end each time.
        let p = "(?P=".repeat(n);
        let t = Instant::now();
        let _ = translate(&p);
        assert!(
            t.elapsed() < Duration::from_millis(200),
            "unclosed (?P= is not linear"
        );
        // ...and a closed one still translates, with a later `(?P=` unclosed.
        assert_eq!(translate("(?P<x>a)(?P=x)"), "(?P<x>a)\\k<x>");
    }

    // expert review 2026-10-12 #2: a single pathological compile must be refused, and a
    // realistic Django pattern set must still compile (a too-tight limit is a compatibility
    // regression: `(?i)\w{100}` is ordinary).
    #[test]
    fn pathological_programs_and_patterns_are_refused() {
        // Needs ~128 MiB of program; the top tier is 10 MiB.
        assert!(
            search("(?i)\\w{1000}", "x").is_err(),
            "a pathological program compiled"
        );
        let long = "a".repeat(MAX_PATTERN_BYTES + 1);
        assert!(matches!(
            search(&long, "a"),
            Err(PyreError::PatternTooLong(_))
        ));
    }

    #[test]
    fn realistic_django_patterns_compile_and_match() {
        assert!(m("(?i)\\w{100}", &"A".repeat(100)));
        assert!(!m("(?i)\\w{100}", "short"));
        assert!(m("^[\\w.@+-]{1,150}$", "user.name+tag@example"));
        let alts: Vec<String> = (0..500).map(|i| format!("word{i}x")).collect();
        let p = format!("(?i)^({})$", alts.join("|"));
        assert!(m(&p, "WORD499X"));
        assert!(!m(&p, "word500x"));
        let lit = "a".repeat(16 * 1024);
        assert!(m(&lit, &lit));
    }

    #[test]
    fn the_cache_stays_within_its_byte_budget() {
        for i in 0..60 {
            // Each compiles at the 4-8 MiB tier and is charged for it: 60 would be ~480 MB, far
            // under the 256-entry count cap, so only the byte budget keeps this in bounds.
            let p = format!("(?i)\\w{{100}}x{i}");
            assert!(search(&p, "b").is_ok(), "{:?}", search(&p, "b"));
            // A literal 64 MiB, not the constant, so weakening the constant fails this.
            assert!(
                cache_bytes() <= 64 * 1024 * 1024,
                "cache over budget at {i}"
            );
        }
    }

    // expert review 2026-10-12 #5: the cache mutex was held across the whole match, so one
    // tenant's backtracking-heavy pattern blocked every other REGEXP call on the node for the
    // length of each match. A cache-hit lookup on another thread must not wait for a match.
    //
    // Timing-based: one pathological match is timed first (it runs to fancy-regex's backtrack
    // limit, tens of ms), then a second thread hammers it while this one does cheap lookups.
    // Pre-fix the worst cheap lookup waited roughly one full match; the bound is half a match,
    // far above scheduler noise on an idle box and far below the pre-fix wait.
    #[test]
    fn a_slow_match_does_not_block_other_lookups() {
        use std::time::{Duration, Instant};

        let slow = "^(a*)*\\1$";
        let subject = format!("{}d", "a".repeat(28));
        let t = Instant::now();
        let _ = search(slow, &subject); // Err (backtrack limit) is the expected, slow outcome
        let one_match = t.elapsed();
        assert!(
            one_match > Duration::from_millis(4),
            "the pathological pattern is not slow here ({one_match:?}); the test measures nothing"
        );
        assert!(m("^a", "abc")); // warm the fast pattern's cache entry

        let worker = std::thread::spawn(move || {
            let end = Instant::now() + one_match * 40;
            while Instant::now() < end {
                let _ = search(slow, &subject);
            }
        });

        let mut worst = Duration::ZERO;
        while !worker.is_finished() {
            let t = Instant::now();
            assert!(m("^a", "abc"));
            worst = worst.max(t.elapsed());
            std::thread::sleep(Duration::from_micros(200));
        }
        worker.join().unwrap();
        assert!(
            worst < one_match / 2,
            "a cached lookup waited {worst:?} behind a {one_match:?} match: the cache lock is held across matching"
        );
    }

    // expert review 2026-10-13 #1: lookaround re-runs fancy-regex's delegate from every start
    // position, so one match over a big subject is quadratic and uninterruptible (1 MB took 56 s).
    #[test]
    fn fancy_patterns_refuse_oversized_subjects_quickly() {
        use std::time::{Duration, Instant};
        let t = Instant::now();
        let r = search("(?<=a)a*b", &"a".repeat(1_000_000));
        assert!(
            matches!(r, Err(PyreError::SubjectTooLong(1_000_000))),
            "{r:?}"
        );
        assert!(
            t.elapsed() < Duration::from_millis(100),
            "{:?}",
            t.elapsed()
        );
        // The already-cached path refuses too, and backreferences take the same path.
        assert!(search("(?<=a)a*b", &"a".repeat(MAX_FANCY_SUBJECT_BYTES + 1)).is_err());
        assert!(search("(a)\\1", &"b".repeat(MAX_FANCY_SUBJECT_BYTES + 1)).is_err());
        // Small subjects still answer correctly, including exactly at the limit.
        assert!(m("(?<=a)b", "ab"));
        assert!(!m("(?<=a)b", "cb"));
        assert!(m("(a)\\1", "xaay"));
        assert!(search("(?<=a)b", &"c".repeat(MAX_FANCY_SUBJECT_BYTES)).is_ok());
    }

    // The cap is for the backtracking path only: plain patterns (including `$`, which `translate`
    // rewrites into a lookahead) stay linear and keep working on big subjects.
    #[test]
    fn plain_patterns_are_not_capped() {
        let big = format!("{}b", "a".repeat(1_000_000));
        assert!(m("a*b", &big));
        assert!(m("a+b$", &big));
        assert!(!m("^c", &big));
    }

    // expert review 2026-10-13 #32: a failing pattern re-ran every compile tier on each call.
    #[test]
    fn compile_failures_are_negative_cached() {
        use std::time::Instant;
        let bad = "(?i)\\w{1000}x_negcache"; // needs ~128 MiB: refused at the top tier
        assert_eq!(cached(bad), (false, false));
        let t1 = Instant::now();
        assert!(search(bad, "b").is_err());
        let first = t1.elapsed();
        assert_eq!(cached(bad), (false, true), "failure was not remembered");
        let t2 = Instant::now();
        let again = search(bad, "b");
        let second = t2.elapsed();
        assert!(again.is_err());
        assert!(
            second * 5 < first,
            "second call {second:?} vs first {first:?}"
        );
    }

    // expert review 2026-10-13 #32: over budget the cache used to clear() EVERYTHING, so one tenant
    // cycling big patterns evicted every other tenant's small hot pattern.
    #[test]
    fn a_hot_small_pattern_survives_a_fill_with_big_ones() {
        let hot = "^hot_lru_survivor$";
        assert!(m(hot, "hot_lru_survivor"));
        for i in 0..40 {
            let p = format!("(?i)\\w{{100}}lru{i}");
            assert!(search(&p, "b").is_ok());
            // Check BEFORE touching it again: a call re-inserts a missing pattern.
            assert!(cached(hot).0, "hot pattern evicted at fill {i}");
            assert!(m(hot, "hot_lru_survivor")); // keep it recently used
        }
        assert!(cache_bytes() <= 64 * 1024 * 1024);
    }
}
