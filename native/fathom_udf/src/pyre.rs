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
use std::sync::Mutex;

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
    // Depth is not enough: `[` inside a class is a literal, so this is a flag, not a counter.
    let mut in_class = false;
    // A `]` in the FIRST position of a class is a literal `]`, not the terminator — `[]]` is a
    // class matching one `]`.
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
                class_start = out.chars().count();
                out.push(c);
            }

            ']' if in_class => {
                // Only closes the class if it is not the first character (`[]abc]`) and not first
                // after a leading negation (`[^]abc]`).
                let pos = out.chars().count();
                let first = pos == class_start + 1
                    || (pos == class_start + 2 && out.ends_with('^'));
                if !first {
                    in_class = false;
                }
                out.push(c);
            }

            '$' if !in_class && !multiline => {
                // The whole reason this module exists.
                out.push_str("(?=\\n?\\z)");
            }

            '(' if !in_class && starts_named_backref(&chars, i) => {
                // `(?P=name)` -> `\k<name>`
                let close = chars[i..].iter().position(|&c| c == ')').unwrap() + i;
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

fn starts_named_backref(chars: &[char], i: usize) -> bool {
    chars.get(i..i + 4) == Some(&['(', '?', 'P', '='])
        && chars[i..].iter().any(|&c| c == ')')
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

/// Compiled-pattern cache.
///
/// SQLite's per-statement auxdata already caches a constant pattern for the life of a statement,
/// but a pattern bound as a PARAMETER (`WHERE col REGEXP ?`, which is exactly what a Django
/// queryset sends) gets no auxdata at all and would otherwise recompile per row. Backtracking
/// compilation is more expensive than `regex`'s, so this matters more than it did before.
///
/// Bounded, because a pattern is tenant-supplied: an unbounded map keyed on client input is a
/// memory-growth primitive. At the cap the cache is cleared rather than evicted one entry — a
/// pathological workload cycling thousands of distinct patterns is recompiling regardless, and a
/// clear keeps this a few lines instead of an LRU.
const CACHE_CAP: usize = 256;

static CACHE: Mutex<Option<HashMap<String, Regex>>> = Mutex::new(None);

/// `re.search(pattern, subject)` — whether the pattern matches ANYWHERE in the subject.
///
/// Python's `re.search` is unanchored, which is also `fancy_regex::Regex::is_match`'s behaviour;
/// `re.match` (anchored at the start) is a different function and not what Django's `regexp` uses.
pub fn search(pattern: &str, subject: &str) -> Result<bool, fancy_regex::Error> {
    let mut guard = CACHE.lock().unwrap_or_else(|e| e.into_inner());
    let cache = guard.get_or_insert_with(HashMap::new);

    if let Some(re) = cache.get(pattern) {
        return re.is_match(subject);
    }

    let re = Regex::new(&translate(pattern))?;
    let matched = re.is_match(subject);

    if cache.len() >= CACHE_CAP {
        cache.clear();
    }
    cache.insert(pattern.to_string(), re);

    matched
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
}
