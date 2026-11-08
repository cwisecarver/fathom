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
//! The fix is to rewrite a bare `$` as `(?=\n?\z)` (or, where nothing can follow it, the consuming
//! `(?:\n?\z)`: see [`translate`]), which is exactly Python's rule. Doing that needs to know whether
//! a given `$` is a real anchor — not escaped, not inside a character class — and whether `(?m)` is
//! in effect in ITS scope, which is why [`translate`] is a small scanner rather than a
//! `String::replace`. A naive replace corrupts `[a$b]` and `\$`.
//!
//! The same scanner rewrites the other tokens the two engines read differently: `\w`/`\W` (Python's
//! are letters + numbers, not marks), `\<`/`\>`, and the class members `[`, `&&`, `~~`, `--` that
//! Python takes literally.
//!
//! **Not a general regex parser**, and deliberately not: it only classifies enough of the syntax to
//! find the tokens above and passes everything else through untouched. Every rule here is pinned by
//! a differential test against CPython (`tests/oracle/generate_re_cases.py`), and what still differs
//! is enumerated there as `KNOWN_DIVERGENCES` and in `docs/component-notes.md`.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use fancy_regex::Regex;

/// Rewrites a Python `re` pattern into the equivalent `fancy-regex` pattern.
///
/// The substitutions, all of them tokens `fancy-regex` either spells differently, does not have, or
/// reads differently from CPython:
///
/// | Python          | here              | why |
/// |-----------------|-------------------|-----|
/// | `$` (no `(?m)`) | `(?:\n?\z)` when nothing can follow it, else `(?=\n?\z)` | Python's `$` also matches before ONE trailing newline; the consuming form keeps the pattern lookaround-free (see below) |
/// | `\Z`            | `\z`              | same meaning, different spelling |
/// | `(?P=name)`     | `\k<name>`        | same meaning, different spelling |
/// | `\w`, `\W`      | `[\p{L}\p{N}_]`, `[^\p{L}\p{N}_]` (only in a pattern with no `\b` / `\B`) | Python's `\w` is `str.isalnum()` or `_` (letters and every number, NOT combining marks); Rust's adds marks and drops `No` numbers (`²`, `①`). `\b` cannot follow without a lookaround, so a pattern that has one keeps the engine's `\w` and the two agree with each other |
/// | `\<`, `\>`      | `<`, `>`          | literals in Python; word-boundary assertions in `regex` |
/// | `(?#…)`         | (removed)         | Python skips it as if absent |
/// | `[` `&&` `~~` `-` inside a class | escaped, `-` range handling as Python's | Python takes them as literals / range operators; `regex` reads set operations and nested classes (expert review 2026-11-07 #88) |
///
/// `^`, `\A`, `\b`, `\d`, `(?P<name>…)`, `(?i)`, `(?s)`, `(?m)` and the rest already agree.
///
/// ## A terminal `$` is rewritten to a consuming form (expert review 2026-11-07 #74)
///
/// `(?=\n?\z)` is a lookaround, and any lookaround makes `fancy-regex` run the pattern on its
/// backtracking VM: ~O(n) per start position against shared 1M-step / 1M-entry limits, so
/// `^[a-z0-9-]+$|^x$` over a ~1 MB subject raised `Max limit for backtracking count exceeded` where
/// Python answers (measured: 1.5 MB, 1 MB and 120 KB subjects for three alternation shapes).
/// `fancy-regex` 0.19 already moves a lookahead that is the LAST element of the whole pattern into
/// pure `regex` syntax (`optimize_trailing_lookahead`), which is why `^abc$` and `\w+$` were fine;
/// it does not do it for a `$` that ends one alternative, a group, or anything but the root's last
/// atom. A `$` after which NOTHING can follow on any path — the end of the pattern, a `|`, or a `)`
/// whose group is itself followed only by those, and not under a quantifier or inside a lookaround —
/// only has to ASSERT, and consuming the optional `\n` is equivalent for an existence check, so it
/// becomes `(?:\n?\z)` and the pattern stays plain. Anything that follows (`a$\n`, `a$\b`, `(a$)b`)
/// keeps the lookahead, where consuming the `\n` would change what the rest can match.
///
/// ## Scoped flags
///
/// `(?m:…)` / `(?-m:…)` apply to their group only (and a global `(?m)` to the whole pattern), so
/// whether a `$` is multiline is tracked per group, and a `[`, `(?m)` or `$` inside a character
/// class is a class member, not syntax.
///
/// **Not a general regex parser**, and deliberately not: it only classifies enough of the syntax to
/// find real `$`, `\Z`, `\w`, class and group tokens, and passes everything else through untouched.
/// Every rule here is pinned by a differential test against CPython
/// (`tests/oracle/generate_re_cases.py`), and the differences that are left are listed in its
/// `KNOWN_DIVERGENCES`.
pub fn translate(pattern: &str) -> String {
    let chars: Vec<char> = pattern.chars().collect();
    // Position of the last `)`: an unclosed `(?P=` / `(?#` is then rejected in O(1) instead of
    // rescanning to the end of the pattern each time.
    let last_close = chars.iter().rposition(|&c| c == ')');
    // Python applies inline global flags wherever they appear (and since 3.11 requires them at the
    // start). `(?m)` changes what `$` means, so find it before rewriting any anchor.
    let Prescan {
        global_multiline,
        word_boundary,
    } = prescan(&chars, last_close);
    // Python's `\w` (letters + numbers) differs from `regex`'s (alphabetic + marks + `Nd`), and
    // `regex`'s `\b` is defined by ITS `\w`. `\b` cannot be given Python's definition without a
    // lookaround (the backtracking VM, capped at 64 KiB: the most common construct, `\bword\b`,
    // would pay it), so a pattern that uses `\b` / `\B` keeps `regex`'s `\w` as well and the two at
    // least agree with each other; without one, `\w` / `\W` are Python's (expert review 2026-11-07 #88).
    let python_word = !word_boundary;

    // The output is `segments[0] $0 segments[1] $1 … tail`: each real `$` ends a segment and is
    // spelled when its terminal-ness is known, at the end.
    let mut segments: Vec<String> = Vec::new();
    let mut terminal: Vec<bool> = Vec::new();
    let mut out = String::with_capacity(pattern.len() + 8);
    let mut frames: Vec<Frame> = Vec::new();
    let mut i = 0;

    while i < chars.len() {
        let c = chars[i];

        match c {
            '\\' if i + 1 < chars.len() => {
                push_escape(chars[i + 1], false, python_word, &mut out);
                i += 2;
            }

            '[' => i = translate_class(&chars, i, python_word, &mut out),

            '$' if !frames.last().map_or(global_multiline, |f| f.multiline) => {
                // The whole reason this module exists.
                let id = terminal.len();
                terminal.push(false);
                segments.push(std::mem::take(&mut out));
                if only_a_branch_end_follows(&chars, i) {
                    match frames.last_mut() {
                        Some(f) => f.pending.push(id),
                        None => terminal[id] = true,
                    }
                }
                i += 1;
            }

            '(' => {
                if let Some(close) = named_backref_close(&chars, i, last_close) {
                    // `(?P=name)` -> `\k<name>`
                    let name: String = chars[i + 4..close].iter().collect();
                    out.push_str("\\k<");
                    out.push_str(&name);
                    out.push('>');
                    i = close + 1;
                    continue;
                }
                let open = classify_group(&chars, i, last_close);
                match open.kind {
                    GroupKind::Comment => {}
                    GroupKind::GlobalFlags => out.extend(&chars[i..i + open.len]),
                    GroupKind::Group {
                        multiline,
                        lookaround,
                    } => {
                        let inherited = frames.last().map_or(global_multiline, |f| f.multiline);
                        frames.push(Frame {
                            multiline: multiline.unwrap_or(inherited),
                            lookaround,
                            pending: Vec::new(),
                        });
                        out.extend(&chars[i..i + open.len]);
                    }
                }
                i += open.len;
            }

            ')' => {
                if let Some(f) = frames.pop() {
                    // The group's pending `$`s are terminal only if the group itself ends a
                    // branch: its parent decides (the root says yes).
                    if !f.lookaround && only_a_branch_end_follows(&chars, i) {
                        match frames.last_mut() {
                            Some(parent) => parent.pending.extend(f.pending),
                            None => f.pending.iter().for_each(|&id| terminal[id] = true),
                        }
                    }
                }
                out.push(c);
                i += 1;
            }

            _ => {
                out.push(c);
                i += 1;
            }
        }
    }

    let mut result = String::with_capacity(pattern.len() + 8 + 10 * segments.len());
    for (segment, &is_terminal) in segments.iter().zip(&terminal) {
        result.push_str(segment);
        result.push_str(if is_terminal {
            "(?:\\n?\\z)"
        } else {
            "(?=\\n?\\z)"
        });
    }
    result.push_str(&out);
    result
}

/// An open group during [`translate`]'s scan.
struct Frame {
    /// Whether `$` is multiline here (an enclosing scope's flag unless this group sets its own).
    multiline: bool,
    /// A lookaround, atomic or conditional group: a `$` inside is never rewritten to the consuming
    /// form (a lookbehind needs a fixed width; a conditional's branches are not branch ends).
    lookaround: bool,
    /// `$`s directly in this group (or in groups that ended a branch of it) that are terminal iff
    /// this group ends a branch of ITS parent.
    pending: Vec<usize>,
}

/// Whether, after the `$` or `)` at `i`, the pattern ends, or an alternative does (`|`), or an
/// enclosing group does (`)`) — i.e. nothing can match after it on this path. A quantifier, a
/// literal, an escape or anything else means something can.
fn only_a_branch_end_follows(chars: &[char], i: usize) -> bool {
    matches!(chars.get(i + 1), None | Some('|') | Some(')'))
}

/// How a `(` at some index opens a group; `len` is how many chars the opener takes.
struct GroupOpen {
    len: usize,
    kind: GroupKind,
}

enum GroupKind {
    /// `(?#…)`: skipped entirely.
    Comment,
    /// `(?i)`, `(?im)`: a global flag group, no scope of its own.
    GlobalFlags,
    /// Everything that is closed by a `)`.
    Group {
        /// `Some` when the opener is `(?m:` / `(?-m:` (and so on): this scope's `$` mode.
        multiline: Option<bool>,
        lookaround: bool,
    },
}

fn classify_group(chars: &[char], i: usize, last_close: Option<usize>) -> GroupOpen {
    let group = |len, multiline, lookaround| GroupOpen {
        len,
        kind: GroupKind::Group {
            multiline,
            lookaround,
        },
    };
    if chars.get(i + 1) != Some(&'?') {
        return group(1, None, false);
    }
    match chars.get(i + 2) {
        Some(':') => group(3, None, false),
        Some('#') => {
            // The first `)` at or after `i`; the caller jumps past it, so total work is linear.
            let close = if last_close.is_some_and(|l| l > i) {
                chars[i..].iter().position(|&c| c == ')').map(|p| p + i)
            } else {
                None
            };
            match close {
                Some(close) => GroupOpen {
                    len: close + 1 - i,
                    kind: GroupKind::Comment,
                },
                None => group(3, None, false),
            }
        }
        Some('P') if chars.get(i + 3) == Some(&'<') => group(4, None, false),
        Some('=') | Some('!') | Some('>') | Some('(') => group(3, None, true),
        Some('<') => match chars.get(i + 3) {
            Some('=') | Some('!') => group(4, None, true),
            _ => group(3, None, false),
        },
        Some(c) if c.is_ascii_alphabetic() || *c == '-' => {
            let mut j = i + 2;
            let mut on_m = false;
            let mut off_m = false;
            while chars.get(j).is_some_and(|c| c.is_ascii_alphabetic()) {
                on_m |= chars[j] == 'm';
                j += 1;
            }
            if chars.get(j) == Some(&'-') {
                j += 1;
                while chars.get(j).is_some_and(|c| c.is_ascii_alphabetic()) {
                    off_m |= chars[j] == 'm';
                    j += 1;
                }
            }
            match chars.get(j) {
                Some(')') => GroupOpen {
                    len: j + 1 - i,
                    kind: GroupKind::GlobalFlags,
                },
                Some(':') => group(
                    j + 1 - i,
                    if on_m {
                        Some(true)
                    } else if off_m {
                        Some(false)
                    } else {
                        None
                    },
                    false,
                ),
                _ => group(2, None, false),
            }
        }
        _ => group(2, None, false),
    }
}

/// The text a backslash escape becomes. `in_class` selects the spelling of `\w` / `\W`, which sit
/// inside a bracket expression as members; `python_word` says whether to write them at all (see
/// [`translate`]'s `\b` rule).
fn push_escape(next: char, in_class: bool, python_word: bool, out: &mut String) {
    match next {
        // Python `\Z` == absolute end of string == Rust `\z`. (Rust has no `\Z`, which is why this
        // was a hard error rather than a wrong answer.) Inside a class both are an escaped letter.
        'Z' if !in_class => out.push_str("\\z"),
        // Python's `\w`: `str.isalnum()` or `_`, i.e. every letter and every number.
        'w' if python_word => out.push_str(if in_class { WORD_MEMBERS } else { WORD }),
        'W' if python_word => out.push_str(NOT_WORD),
        // Literals in Python; `regex` reads them as word-start / word-end assertions.
        '<' | '>' => out.push(next),
        _ => {
            out.push('\\');
            out.push(next);
        }
    }
}

/// Python's `\w` as `regex` syntax: Unicode letters, Unicode numbers (`Nd`, `Nl`, `No`), `_`.
const WORD_MEMBERS: &str = "\\p{L}\\p{N}_";
const WORD: &str = "[\\p{L}\\p{N}_]";
const NOT_WORD: &str = "[^\\p{L}\\p{N}_]";

/// Rewrites the character class starting at `start` (a `[`) into `out` and returns the index after
/// its closing `]` (or the end of the pattern when it is unterminated, which `regex` then reports).
///
/// CPython's rules (`sre_parse`): a `]` first after `[` / `[^` is a member; `[`, `&&`, `~~` and `||`
/// are plain members (a FutureWarning at most); `-` between two members is a range, a `-` that
/// STARTS an item or ENDS the class is a literal, and so `[a--b]` is the range `a`..`-` (an error)
/// and `[a-c--b]` is `a-c` plus `-`..`b`. `regex` reads `[` as a nested class and `&&`, `--`, `~~` as
/// set operations, so every member that is not syntax in Python is escaped, and only a real range
/// operator is written as `-`.
fn translate_class(chars: &[char], start: usize, python_word: bool, out: &mut String) -> usize {
    out.push('[');
    let mut i = start + 1;
    if chars.get(i) == Some(&'^') {
        out.push('^');
        i += 1;
    }
    let first = i;
    // True when the next char begins an item; false right after one, where a `-` is a range
    // operator.
    let mut at_item = true;
    while i < chars.len() {
        let c = chars[i];
        if c == ']' && i != first {
            out.push(']');
            return i + 1;
        }
        if !at_item && c == '-' {
            if chars.get(i + 1) == Some(&']') {
                // `[a-]`: a trailing literal `-`.
                out.push_str("\\-");
                i += 1;
                at_item = true;
                continue;
            }
            out.push('-');
            // The range END is one item.
            i = push_class_item(chars, i + 1, python_word, out);
            at_item = true;
            continue;
        }
        i = push_class_item(chars, i, python_word, out);
        at_item = false;
    }
    chars.len()
}

/// One class member (an escape or a single char) at `i`; returns the next index.
fn push_class_item(chars: &[char], i: usize, python_word: bool, out: &mut String) -> usize {
    let Some(&c) = chars.get(i) else {
        return i;
    };
    match c {
        '\\' if i + 1 < chars.len() => {
            push_escape(chars[i + 1], true, python_word, out);
            i + 2
        }
        '[' | '&' | '~' | '-' | ']' => {
            out.push('\\');
            out.push(c);
            i + 1
        }
        _ => {
            out.push(c);
            i + 1
        }
    }
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

/// Index just past the character class starting at `start` (a `[`), or the pattern's end.
fn class_end(chars: &[char], start: usize) -> usize {
    let mut i = start + 1;
    if chars.get(i) == Some(&'^') {
        i += 1;
    }
    let first = i;
    while i < chars.len() {
        match chars[i] {
            '\\' => i += 2,
            ']' if i != first => return i + 1,
            _ => i += 1,
        }
    }
    chars.len()
}

/// What one pass over a pattern's top level (outside character classes and `(?#…)` comments) finds
/// before any token is rewritten.
struct Prescan {
    /// A GLOBAL inline flag group — `(?m)`, `(?im)` — enables multiline.
    global_multiline: bool,
    /// The pattern uses `\b` or `\B`.
    word_boundary: bool,
}

/// Scans for `(?<letters>)` groups rather than searching for the literal `"(?m)"`, so `(?im)` is
/// recognized too. A scoped `(?m:` is NOT global: [`translate`] tracks it per group (expert review
/// 2026-11-07 #88: this scan used to count it, and `[(?m)]$`, whose `(?m)` is class members, as
/// global, so `(?m:a$)b$` left both `$` un-rewritten).
fn prescan(chars: &[char], last_close: Option<usize>) -> Prescan {
    let mut found = Prescan {
        global_multiline: false,
        word_boundary: false,
    };
    let mut i = 0;

    while i < chars.len() {
        match chars[i] {
            '\\' => {
                found.word_boundary |= matches!(chars.get(i + 1), Some('b') | Some('B'));
                i += 2;
            }
            '[' => i = class_end(chars, i),
            '(' if chars.get(i + 1) == Some(&'?') => {
                if chars.get(i + 2) == Some(&'#') {
                    // Skip a comment (to its first `)`, linear overall as in `classify_group`).
                    i = if last_close.is_some_and(|l| l > i) {
                        chars[i..]
                            .iter()
                            .position(|&c| c == ')')
                            .map_or(i + 1, |p| p + i + 1)
                    } else {
                        i + 1
                    };
                    continue;
                }
                let mut j = i + 2;
                let mut seen_m = false;
                while chars.get(j).is_some_and(|c| c.is_ascii_alphabetic()) {
                    seen_m |= chars[j] == 'm';
                    j += 1;
                }
                // Only a global flag group if the letters are followed by `)`; `(?=`, `(?P<`,
                // `(?m:` and friends are not.
                found.global_multiline |= seen_m && chars.get(j) == Some(&')');
                i += 1;
            }
            _ => i += 1,
        }
    }

    found
}

/// Cap on the positive cache, in entries (expert review 2026-11-07 #75; the decision is in
/// docs/component-notes.md). The budget below charges COMPILE size, but every cached regex also
/// keeps one `regex-automata` lazy-DFA `Cache` per thread that has run it, ~2.6 MiB each once a
/// state-heavy pattern fills the default 2 MiB hybrid cache (forward + reverse), allocated by the
/// Rust allocator outside the BEAM's allocators, `soft_heap_limit` and the byte budget. fancy-regex
/// does not expose `hybrid_cache_capacity` and `delegate_dfa_size_limit` is the FULL-DFA limit, so
/// the only lever on that memory is how many regexes can be alive: worst case is
/// `CACHE_CAP x threads that can run a regexp x 2.6 MiB`. Every exqlite NIF is dirty-IO, and
/// `rel/vm.args.eex` ships `+SDio 10`, so 20 x 10 x 2.6 MiB = 520 MiB (0.51 GiB; it was 256 x 10 x
/// 2.6 = 6.5 GiB). 20 still holds a normal app's whole pattern set; a 21st distinct hot pattern
/// makes a round-robin over them recompile on every call (`twenty_realistic_patterns_do_not_thrash`
/// pins the 20, `patterns_past_the_cap_are_evicted_and_recompile_correctly` the cliff).
const CACHE_CAP: usize = 20;

/// Cap on the negative cache, in entries. Separate from [`CACHE_CAP`]: a failed compile holds no
/// DFA caches (only the pattern text and the error, bounded by [`FAILED_BYTE_BUDGET`]), and
/// re-running the four compile tiers costs 32-85 ms per call, so it keeps the larger bound.
const FAILED_CAP: usize = 256;

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
/// patterns run on the linear `regex` engine and have the larger [`MAX_PLAIN_SUBJECT_BYTES`] cap.
pub const MAX_FANCY_SUBJECT_BYTES: usize = 64 * 1024;

/// Every subject over this is refused, whatever the pattern (expert review 2026-11-07 #34).
///
/// The plain path is linear but not fast for every pattern: when the hybrid DFA cannot hold a
/// pattern's state space (a counted repetition after an unbounded one, `[ab]*a[ab]{200}[^ab]`,
/// `(?i)(?:\w|\s){1,100}a(?:\w|\s){100}\W`) the `regex` crate falls back to its PikeVM, and the
/// match runs at 0.1–12 MB/s MEASURED on random a/b text (release build, Apple silicon:
/// 12 / 6 / 3.6 / 1.9 MB/s for `{20}` / `{50}` / `{100}` / `{200}`, 0.83 for the `\w|\s` one,
/// 0.11 for `(a|b)*a(a|b){2000}c`). SQLite's own limit lets a subject reach 64 MiB, so one call
/// could run for minutes on a dirty-IO thread; nothing can interrupt it (the progress handler
/// runs between VDBE ops, the regex crate has no cancellation). A bigger lazy-DFA cache does not
/// help (4 / 32 / 256 MiB all measured identical, so it is not cache thrash that decides), and
/// the plain subject cannot be matched in windows (a match may span any window). So the work is
/// bounded by input size: 256 KiB bounds one call to ~0.03 s for a realistic pattern and to
/// ~2.3 s for the slowest measured one (0.11 MB/s; it was 1 MiB, ~9 s, until the maintainer
/// lowered it), and the `regexp` UDF checks the statement deadline before each call so a per-row
/// statement stops at its next row. A `__regex` over a larger value is not a realistic workload
/// (the fancy cap is 64 KiB for the same reason). Residual, accepted: a hostile pattern can still
/// hold one thread for up to that ~2.3 s once per row until the deadline. A truly interruptible
/// matcher needs a hand-driven lazy-DFA loop; that alternative was declined (component-notes).
pub const MAX_PLAIN_SUBJECT_BYTES: usize = 256 * 1024;

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
    /// Any pattern was asked to scan a subject over [`MAX_PLAIN_SUBJECT_BYTES`].
    PlainSubjectTooLong(usize),
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
            PyreError::PlainSubjectTooLong(n) => write!(
                f,
                "text is {n} bytes, over the {MAX_PLAIN_SUBJECT_BYTES}-byte limit for regular \
                 expressions"
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
    search_in(&CACHE, pattern, subject)
}

/// [`search`] against an explicit cache, so the cache tests need not share the process-global one
/// (a test filling it would evict another test's pattern at [`CACHE_CAP`] = 20).
fn search_in(
    cache: &Mutex<Option<Cache>>,
    pattern: &str,
    subject: &str,
) -> Result<bool, PyreError> {
    if pattern.len() > MAX_PATTERN_BYTES {
        return Err(PyreError::PatternTooLong(pattern.len()));
    }
    let (re, fancy) = match cache_get(cache, pattern)? {
        Some(hit) => hit,
        None => match compile(pattern) {
            Ok((re, cost)) => {
                let re = Arc::new(re);
                let fancy = needs_backtracking(pattern);
                cache_put(cache, pattern, &re, fancy, cost);
                (re, fancy)
            }
            Err(e) => {
                cache_put_failed(cache, pattern, &e);
                return Err(e.into());
            }
        },
    };
    if fancy && subject.len() > MAX_FANCY_SUBJECT_BYTES {
        return Err(PyreError::SubjectTooLong(subject.len()));
    }
    if subject.len() > MAX_PLAIN_SUBJECT_BYTES {
        return Err(PyreError::PlainSubjectTooLong(subject.len()));
    }
    Ok(re.is_match(subject)?)
}

/// A hit, `None` for a miss, or the remembered compile error.
fn cache_get(
    cache: &Mutex<Option<Cache>>,
    pattern: &str,
) -> Result<Option<(Arc<Regex>, bool)>, PyreError> {
    let mut guard = cache.lock().unwrap_or_else(|e| e.into_inner());
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

fn cache_put(
    cache: &Mutex<Option<Cache>>,
    pattern: &str,
    re: &Arc<Regex>,
    fancy: bool,
    cost: usize,
) {
    let mut guard = cache.lock().unwrap_or_else(|e| e.into_inner());
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

fn cache_put_failed(cache: &Mutex<Option<Cache>>, pattern: &str, err: &fancy_regex::Error) {
    let mut guard = cache.lock().unwrap_or_else(|e| e.into_inner());
    let cache = guard.get_or_insert_with(new_cache);
    while !cache.failed.is_empty()
        && (cache.failed.len() >= FAILED_CAP
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
fn cache_bytes(cache: &Mutex<Option<Cache>>) -> usize {
    let guard = cache.lock().unwrap_or_else(|e| e.into_inner());
    guard.as_ref().map_or(0, |c| c.bytes)
}

/// Whether a pattern is currently in the positive / negative cache (test hook).
#[cfg(test)]
fn cached(cache: &Mutex<Option<Cache>>, pattern: &str) -> (bool, bool) {
    let guard = cache.lock().unwrap_or_else(|e| e.into_inner());
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
        assert_eq!(translate("a$"), "a(?:\\n?\\z)");
        assert_eq!(translate("\\$"), "\\$");
        assert_eq!(translate("[a$]"), "[a$]");
        assert_eq!(translate("(?m)a$"), "(?m)a$");
        assert_eq!(translate("a\\Z"), "a\\z");
    }

    // expert review 2026-11-07 #74: a `$` after which nothing can match is spelled consuming
    // (`(?:\n?\z)`, no lookaround, so the pattern stays on the linear engine); one that something
    // can follow keeps the lookahead, where consuming the `\n` would change what the rest matches.
    #[test]
    fn a_terminal_dollar_is_consuming_and_the_rest_keep_the_lookahead() {
        const C: &str = "(?:\\n?\\z)";
        const L: &str = "(?=\\n?\\z)";
        // End of pattern, end of an alternative, end of a (nested) group that ends one.
        assert_eq!(translate("a$|b$"), format!("a{C}|b{C}"));
        assert_eq!(translate("^a$|^b$"), format!("^a{C}|^b{C}"));
        assert_eq!(translate("(a$)"), format!("(a{C})"));
        assert_eq!(translate("(?:x(a$))|y"), format!("(?:x(a{C}))|y"));
        assert_eq!(translate("^(?:[a-z]+$)"), format!("^(?:[a-z]+{C})"));
        assert_eq!(translate("(a|b$)"), format!("(a|b{C})"));
        assert_eq!(translate("a$|"), format!("a{C}|"));
        // Something can follow: a literal, an escape, a quantifier, a group that is followed.
        assert_eq!(translate("a$\\n"), format!("a{L}\\n"));
        assert_eq!(translate("a$\\b"), format!("a{L}\\b"));
        assert_eq!(translate("a$*"), format!("a{L}*"));
        assert_eq!(translate("(a$)b"), format!("(a{L})b"));
        assert_eq!(translate("(a$|b)c"), format!("(a{L}|b)c"));
        assert_eq!(translate("(a$)+"), format!("(a{L})+"));
        assert_eq!(translate("(a$)?"), format!("(a{L})?"));
        // Inside a lookaround, atomic or conditional group: never consuming.
        assert_eq!(translate("(?=a$)"), format!("(?=a{L})"));
        assert_eq!(translate("(?<=a$)"), format!("(?<=a{L})"));
        assert_eq!(translate("(?>a$)"), format!("(?>a{L})"));
    }

    // The measured failure (fancy-regex 0.19.2): a `$` that is not the root's last atom made the
    // pattern a lookaround pattern, ran it on the backtracking VM, and a ~1 MB subject raised
    // "Max limit for backtracking count exceeded" / "Max stack size exceeded" where Python answers.
    #[test]
    fn terminal_dollars_in_alternatives_and_groups_answer_on_big_subjects() {
        // 200 kB: over the 64 KiB fancy cap (a regression to the backtracking route errors with
        // `SubjectTooLong` here) and under the 256 KiB plain cap. The measured ~1 MB subjects are
        // over the plain cap since 2026-11-08, so the VM-limit failure itself is no longer reachable.
        let slug = "a".repeat(200_000);
        assert_eq!(search("^[a-z0-9-]+$|^x$", &slug).unwrap(), true);
        assert_eq!(search("^(?:[a-z0-9-]+$)", &slug).unwrap(), true);
        let words = "ab ".repeat(66_666);
        assert_eq!(search("[a-z]+$|[0-9]+$", &words).unwrap(), false);
        assert_eq!(search("\\w+$|zz", &words).unwrap(), false);
        // And the trailing-newline rule is intact on every spelling.
        assert!(m("^a$|^b$", "b\n"));
        assert!(!m("^a$|^b$", "b\n\n"));
        assert!(m("(a$)", "a\n"));
        assert!(!m("(a$)", "a\nb"));
        assert!(m("(?:^|,)a$", ",a\n"));
    }

    // expert review 2026-11-07 #88: `(?m:…)` is scoped, `(?m)` inside a class is not a flag.
    #[test]
    fn multiline_is_tracked_per_scope() {
        const C: &str = "(?:\\n?\\z)";
        assert_eq!(translate("(?m:a$)|b$"), format!("(?m:a$)|b{C}"));
        assert_eq!(translate("(?m:a$)(?-m:$)"), format!("(?m:a$)(?-m:{C})"));
        assert_eq!(translate("(?m:(?-m:a$))"), format!("(?m:(?-m:a{C}))"));
        assert_eq!(translate("[(?m)]$"), format!("[(?m)]{C}"));
        assert_eq!(translate("(?i)(?m)a$"), "(?i)(?m)a$");
        // The first `$` is end-of-line (matches before the `\n` that `\n` then consumes); the
        // second is the ordinary end-of-text-or-before-one-newline `$`.
        assert!(m("(?m:a$)\\nb$", "a\nb\n"));
        assert!(!m("(?m:a$)\\nb$", "a\nb\n\n"));
        // Pre-fix the global scan counted the scoped flag, so the trailing `$` was Rust's strict one.
        assert!(m("(?m:a$)|b$", "b\n"));
    }

    // expert review 2026-11-07 #88: Python reads these as members of a class; `regex` reads them
    // as nested classes and set operations.
    #[test]
    fn class_members_that_regex_reads_as_syntax_are_escaped() {
        assert_eq!(translate("[[]"), "[\\[]");
        assert_eq!(translate("[a&&b]"), "[a\\&\\&b]");
        assert_eq!(translate("[a~~b]"), "[a\\~\\~b]");
        assert_eq!(translate("[[:alpha:]]"), "[\\[:alpha:]]");
        assert_eq!(translate("[]a]"), "[\\]a]");
        assert_eq!(translate("[^]a]"), "[^\\]a]");
        assert_eq!(translate("[a-]"), "[a\\-]");
        assert_eq!(translate("[-a]"), "[\\-a]");
        // `-` between members stays a range; `--` is a range STARTING at `-` (`[a-c--b]` is `a-c`
        // plus `-`..`b`), and `a--` is the bad range `a`..`-`.
        assert_eq!(translate("[a-z0-9]"), "[a-z0-9]");
        assert_eq!(translate("[a-c--b]"), "[a-c\\--b]");
        assert!(search("[a--b]", "a").is_err());
        assert!(m("[a-c--b]", "5"));
        assert!(m("[[]", "["));
        assert!(m("[a&&b]", "&"));
        assert!(!m("[a&&b]", "c"));
        assert!(m("[[:alpha:]]", "a]"));
        assert!(!m("[[:alpha:]]", "a"));
        assert!(m("[\\w-]", "-"));
    }

    // expert review 2026-11-07 #88: Python's `\w` is letters + numbers (`isalnum() or "_"`), not
    // `regex`'s alphabetic + marks + `Nd`.
    #[test]
    fn word_class_is_pythons_unless_the_pattern_uses_a_word_boundary() {
        assert_eq!(translate("\\w+"), "[\\p{L}\\p{N}_]+");
        assert_eq!(translate("\\W"), "[^\\p{L}\\p{N}_]");
        assert_eq!(translate("[\\w.-]"), "[\\p{L}\\p{N}_.\\-]");
        assert_eq!(translate("[^\\W\\d]"), "[^[^\\p{L}\\p{N}_]\\d]");
        // A combining mark is not a word character in Python; `²` (category No) is.
        assert!(!m("^\\w+$", "e\u{301}"));
        assert!(m("^\\w+$", "caf\u{e9}"));
        assert!(m("^\\w$", "\u{b2}"));
        assert!(!m("^[\\w ]+$", "Jose\u{301} Garcia"));
        assert!(m("^\\W+$", "\u{301}"));
        // With `\b` the engine's own word definition rules both, so they agree with each other.
        assert_eq!(translate("\\b\\w+\\b"), "\\b\\w+\\b");
        assert_eq!(translate("[\\w]\\B"), "[\\w]\\B");
        assert!(m("\\b\\w+\\b", "e\u{301}"));
        // `\b` inside a class is a backspace, not a boundary.
        assert_eq!(translate("[\\b]\\w"), "[\\b][\\p{L}\\p{N}_]");
    }

    #[test]
    fn angle_escapes_are_literals_and_comments_vanish() {
        assert_eq!(translate("\\<a\\>"), "<a>");
        assert!(m("\\<a\\>", "<a>"));
        assert!(!m("\\<a\\>", "a"));
        assert_eq!(translate("a(?#note)b"), "ab");
        assert!(m("a(?#note)b", "ab"));
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
                cache_bytes(&CACHE) <= 64 * 1024 * 1024,
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

    // Plain patterns (including `$`, which `translate` rewrites into a lookahead) stay linear and
    // keep working on big subjects up to MAX_PLAIN_SUBJECT_BYTES, not the 64 KiB fancy cap.
    #[test]
    fn plain_patterns_have_the_larger_cap() {
        // 200 kB: over the 64 KiB fancy cap, under the 256 KiB plain cap.
        let big = format!("{}b", "a".repeat(200_000));
        assert!(big.len() > MAX_FANCY_SUBJECT_BYTES && big.len() < MAX_PLAIN_SUBJECT_BYTES);
        assert!(m("a*b", &big));
        assert!(m("a+b$", &big));
        assert!(!m("^c", &big));
    }

    // expert review 2026-11-07 #34: a plain pattern is not fast for every shape (the hybrid DFA
    // gives up and the PikeVM runs at 0.1–12 MB/s), and a call cannot be interrupted, so the
    // subject is bounded for EVERY pattern. Refused before any matching, so it is quick.
    #[test]
    fn plain_patterns_refuse_subjects_over_the_plain_cap_quickly() {
        use std::time::{Duration, Instant};
        // The maintainer's decision (2026-11-08) is 256 KiB, written as a literal so moving the
        // constant fails here: the slowest measured pattern costs ~2.3 s per call at this size
        // (it was ~9 s at the old 1 MiB).
        assert_eq!(MAX_PLAIN_SUBJECT_BYTES, 256 * 1024);
        // The measured slow shape: a counted repetition after an unbounded one, on text the lazy
        // DFA cannot summarise. Over the cap it must not even start.
        let slow = "[ab]*a[ab]{200}[^ab]";
        let over = "ab".repeat(MAX_PLAIN_SUBJECT_BYTES / 2 + 1);
        let t = Instant::now();
        let r = search(slow, &over);
        assert!(
            matches!(r, Err(PyreError::PlainSubjectTooLong(n)) if n == over.len()),
            "{r:?}"
        );
        assert!(
            t.elapsed() < Duration::from_millis(100),
            "{:?}",
            t.elapsed()
        );
        // Cached and uncached, trivial patterns too.
        assert!(matches!(
            search("^a", &over),
            Err(PyreError::PlainSubjectTooLong(_))
        ));
    }

    // The boundary itself: a subject AT the cap still matches, one byte over is the typed error.
    #[test]
    fn the_plain_cap_boundary_is_exact() {
        let at = "a".repeat(MAX_PLAIN_SUBJECT_BYTES);
        assert!(!m("^c", &at));
        assert!(m("^a+$", &at));
        let over = "a".repeat(MAX_PLAIN_SUBJECT_BYTES + 1);
        for p in ["^c", "^a+$", "a"] {
            let r = search(p, &over);
            assert!(
                matches!(r, Err(PyreError::PlainSubjectTooLong(n)) if n == MAX_PLAIN_SUBJECT_BYTES + 1),
                "{p}: {r:?}"
            );
        }
        assert_eq!(
            search("a", &over).unwrap_err().to_string(),
            "text is 262145 bytes, over the 262144-byte limit for regular expressions"
        );
    }

    // 20 distinct patterns of the kind a Django app sends (`__regex`/`__iregex` lookups, validators,
    // slug/phone/uuid shapes), including two Unicode `\w` repetitions that compile at the top size
    // tier and are charged ~10 MiB each against the 64 MiB byte budget.
    const REALISTIC: [&str; 20] = [
        r"^[A-Z]{2}\d{6}$",
        r"^\d{3}-\d{3}-\d{4}$",
        r"^[\w.@+-]{1,150}$",
        r"(?i)^(draft|pending|active)$",
        r"^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$",
        r"(?i)\bfoo\b",
        r"^\s*$",
        r"^[a-z0-9]+(?:-[a-z0-9]+)*$",
        r"(?i)^[^@\s]+@[^@\s]+\.[a-z]{2,}$",
        r"^\+?\d{7,15}$",
        r"^(\d{1,3}\.){3}\d{1,3}$",
        r"(?i)(error|warn|fatal)",
        r"^#[0-9a-fA-F]{6}$",
        r"^v\d+\.\d+\.\d+$",
        r"(?i)\w{20}",
        r"^[^/]+/[^/]+$",
        r"(?i)^\s*select\b",
        r"^\d{4}-\d{2}-\d{2}$",
        r"^.{3,40}$",
        r"(?i)^[a-z]+\d*$",
    ];

    fn cached_in(cache: &Mutex<Option<Cache>>, pattern: &str) -> bool {
        cached(cache, pattern).0
    }

    fn cache_len(cache: &Mutex<Option<Cache>>) -> usize {
        let guard = cache.lock().unwrap_or_else(|e| e.into_inner());
        guard.as_ref().map_or(0, |c| c.map.len())
    }

    // expert review 2026-11-07 #75 (decision 2026-11-08): the cap is what bounds the lazy-DFA
    // caches the byte budget cannot see. Written as arithmetic on literals (2.6 MiB per (regex,
    // thread) pair measured, 10 dirty-IO threads from rel/vm.args.eex) so moving the constant back
    // up (it was 256: 6.5 GiB) fails here and the component-notes number stays honest.
    #[test]
    fn the_cache_cap_bounds_the_worst_case_lazy_dfa_memory() {
        const DIRTY_IO_THREADS: usize = 10;
        let worst_mib_x10 = CACHE_CAP * DIRTY_IO_THREADS * 26;
        assert!(
            worst_mib_x10 <= 5200,
            "worst case {} MiB",
            worst_mib_x10 as f64 / 10.0
        );
        assert!(
            CACHE_CAP >= REALISTIC.len(),
            "a normal app's patterns must all fit"
        );
    }

    #[test]
    fn twenty_realistic_patterns_do_not_thrash() {
        let cache = Mutex::new(None);
        for p in REALISTIC {
            assert!(search_in(&cache, p, "x").is_ok(), "{p}");
        }
        let first: Vec<Arc<Regex>> = REALISTIC
            .iter()
            .map(|p| {
                cache_get(&cache, p)
                    .unwrap()
                    .expect("cached after first use")
                    .0
            })
            .collect();
        // Five more laps: every call must be a hit on the SAME compiled regex (a recompile would
        // be a different Arc), and answers stay correct.
        for _ in 0..5 {
            assert!(search_in(&cache, r"^\d{4}-\d{2}-\d{2}$", "2026-11-08").unwrap());
            for (p, was) in REALISTIC.iter().zip(&first) {
                assert!(search_in(&cache, p, "x").is_ok());
                let now = cache_get(&cache, p).unwrap().expect("still cached").0;
                assert!(Arc::ptr_eq(was, &now), "{p} was evicted and recompiled");
            }
        }
        assert_eq!(cache_len(&cache), REALISTIC.len());
    }

    // Past the cap the least-recently-used pattern goes, an evicted pattern recompiles on its next
    // use, and both its answers and every survivor's stay correct.
    #[test]
    fn patterns_past_the_cap_are_evicted_and_recompile_correctly() {
        let cache = Mutex::new(None);
        let pats: Vec<String> = (0..CACHE_CAP + 5)
            .map(|i| format!("^cap{i}x[0-9]+$"))
            .collect();
        for (i, p) in pats.iter().enumerate() {
            assert!(search_in(&cache, p, &format!("cap{i}x42")).unwrap());
            assert!(cache_len(&cache) <= CACHE_CAP);
        }
        assert_eq!(cache_len(&cache), CACHE_CAP);
        for p in &pats[..5] {
            assert!(!cached_in(&cache, p), "{p} should have been evicted");
        }
        for p in &pats[5..] {
            assert!(cached_in(&cache, p), "{p} should have survived");
        }
        // An evicted pattern recompiles and answers both ways; it displaces the next-oldest.
        assert!(search_in(&cache, &pats[0], "cap0x7").unwrap());
        assert!(!search_in(&cache, &pats[0], "cap1x7").unwrap());
        assert!(cached_in(&cache, &pats[0]));
        assert!(!cached_in(&cache, &pats[5]));
        assert_eq!(cache_len(&cache), CACHE_CAP);
        // Survivors still answer correctly (no compiled form leaked across patterns).
        for (i, p) in pats.iter().enumerate().skip(6) {
            assert!(search_in(&cache, p, &format!("cap{i}x42")).unwrap(), "{p}");
            assert!(!search_in(&cache, p, "cap999x1").unwrap(), "{p}");
        }
    }

    // expert review 2026-10-13 #32: a failing pattern re-ran every compile tier on each call.
    #[test]
    fn compile_failures_are_negative_cached() {
        use std::time::Instant;
        let bad = "(?i)\\w{1000}x_negcache"; // needs ~128 MiB: refused at the top tier
        assert_eq!(cached(&CACHE, bad), (false, false));
        let t1 = Instant::now();
        assert!(search(bad, "b").is_err());
        let first = t1.elapsed();
        assert_eq!(
            cached(&CACHE, bad),
            (false, true),
            "failure was not remembered"
        );
        let t2 = Instant::now();
        let again = search(bad, "b");
        let second = t2.elapsed();
        assert!(again.is_err());
        assert!(
            second * 5 < first,
            "second call {second:?} vs first {first:?}"
        );
    }

    // expert review 2026-11-07 #75: the positive cap is small (20) but a failed compile holds no
    // DFA caches, so the negative cache keeps its own, larger bound.
    #[test]
    fn the_negative_cache_is_not_squeezed_by_the_positive_cap() {
        let cache = Mutex::new(None);
        let n = CACHE_CAP + 10;
        for i in 0..n {
            assert!(search_in(&cache, &format!("(unclosed{i}"), "x").is_err());
        }
        let guard = cache.lock().unwrap_or_else(|e| e.into_inner());
        assert_eq!(guard.as_ref().unwrap().failed.len(), n);
        assert!(FAILED_CAP > CACHE_CAP);
    }

    // expert review 2026-10-13 #32: over budget the cache used to clear() EVERYTHING, so one tenant
    // cycling big patterns evicted every other tenant's small hot pattern. Local cache, not the
    // global one: at CACHE_CAP = 20 other tests' patterns would evict this one at random.
    #[test]
    fn a_hot_small_pattern_survives_a_fill_with_big_ones() {
        let cache = Mutex::new(None);
        let hot = "^hot_lru_survivor$";
        assert!(search_in(&cache, hot, "hot_lru_survivor").unwrap());
        for i in 0..40 {
            let p = format!("(?i)\\w{{100}}lru{i}");
            assert!(search_in(&cache, &p, "b").is_ok());
            // Check BEFORE touching it again: a call re-inserts a missing pattern.
            assert!(cached_in(&cache, hot), "hot pattern evicted at fill {i}");
            assert!(search_in(&cache, hot, "hot_lru_survivor").unwrap()); // keep it recently used
        }
        assert!(cache_bytes(&cache) <= 64 * 1024 * 1024);
    }
}
