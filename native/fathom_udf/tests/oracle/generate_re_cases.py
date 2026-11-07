#!/usr/bin/env python3
"""Generate `re_cases.jsonl` — every expectation produced by running CPython's own `re.search`.

Django's `__regex` / `__iregex` lookups are `re.search(pattern, string)`. Under fathom the pattern
crosses the wire and is matched by `native/fathom_udf`, so "exact compatibility" means this table
must replay with zero disagreements.

Same method as `generate_cases.py` and for the same reason: hand-written expectations encode what
I *believe* Python does, and the belief is wrong often enough to matter. Here it found the `$`
divergence — a pattern that compiles in both engines and returns the opposite answer with no error
anywhere.

    cd native/fathom_udf/tests/oracle && python3 generate_re_cases.py

Commit the result; `tests/re_oracle.rs` reads it and CI needs no Python.

Run it with the CPython a current Django runs on (3.12 here): the CORE grid below is identical
under 3.9 and 3.12, but the families added for expert review 2026-11-07 #74/#88 probe syntax whose
meaning moved (global flags not at the start, possessive quantifiers).

## Families and known divergences

`PATTERNS x SUBJECTS` is the core grid. `FAMILIES` are narrower grids, each aimed at ONE known
difference between CPython's `re` and fathom's engine (`regex` + `fancy-regex`). Where the two
engines still disagree and the disagreement is a deliberate, documented choice, the pattern is
listed in `KNOWN_DIVERGENCES` with the reason; `tests/re_oracle.rs` then asserts the divergence
still EXISTS (a fix that removes it must delete the entry) instead of tolerating any difference.
"""

import itertools
import json
import platform
import re
import sys

# Patterns grouped by the feature they exercise. Weighted toward what a real Django app sends
# (`__regex` on a CharField) plus every construct where the two engines are known or suspected to
# differ.
PATTERNS = [
    # --- ordinary, the bulk of real usage ---
    r"^a", r"a$", r"^abc$", r"abc", r"[A-Z]", r"[a-z]+", r"\d", r"\d+", r"\w+", r"\s",
    r"colou?r", r"a{2}", r"a{2,}", r"a{2,3}", r"(foo|bar)", r"^$", r"", r".", r".*", r".+",
    r"[^abc]", r"\bword\b", r"\Bword", r"(abc)+", r"a|b|c",
    # --- anchors, the divergence class ---
    r"abc$", r"c$", r"$", r"\n$", r"\Z", r"abc\Z", r"\A", r"\Aabc", r"^", r"a$|b",
    r"(?m)abc$", r"(?m)^def", r"(?m)$", r"(?im)ABC$", r"(?ms:abc$)",
    # --- $ that is NOT an anchor ---
    r"\$", r"[a$]", r"[$]", r"[]$]", r"[^$]", r"costs \$5", r"a\$b",
    # --- inline flags ---
    r"(?i)HELLO", r"(?i)[a-z]+", r"(?s).", r"(?s)a.c", r"(?x) a b", r"(?i)(?s)A.C",
    # --- groups ---
    r"(?P<y>\d{4})", r"(?P<y>\d{4})-(?P<m>\d{2})", r"(?:abc)+", r"(a)(b)(c)",
    r"(?P<x>a)(?P=x)", r"(?P<x>ab)(?P=x)",
    # --- lookaround (rejected by the regex crate) ---
    r"foo(?=bar)", r"foo(?!bar)", r"(?<=USD)\d+", r"(?<!USD)\d+",
    r"^(?=.*\d)(?=.*[a-z]).+$", r"\d+(?= dollars)", r"(?<=\$)\d+",
    # --- backreferences (rejected by the regex crate) ---
    r"(\w)\1", r"^(.+)\1$", r"(a)(b)\2\1", r"(\d)\1{2}",
    # --- unicode ---
    r"\w", r"\d", r"[é]", r"(?i)É", r"^\w+$", r"\S+",
    # --- realistic app patterns ---
    r"^[\w.+-]+@[\w-]+\.[\w.]+$", r"^\+?1?\d{9,15}$", r"^[a-z0-9-]+$",
    r"^\s*$", r"\s+$", r"^\d{3}-\d{4}$",
]

SUBJECTS = [
    "", "a", "abc", "ABC", "aabb", "abcabc", "a\n", "abc\n", "abc\n\n", "abc\ndef",
    "\n", "\n\n", "def", "foobar", "foobaz", "USD100", "EUR100", "$5", "costs $5",
    "a$b", "]", "2026-08", "aaa", "aa", "111", "abab", "word", "a word here",
    "é", "É", "٣", "  ", " a b ", "hello", "HELLO",
    "user@example.com", "+15551234567", "my-slug-1", "123-4567", "10 dollars",
    "Passw0rd", "x" * 40,
]

INT64_OK = True

# Narrow grids, one per difference family (expert review 2026-11-07 #74 / #88). Each entry is
# (family name, patterns, subjects); the grid is their product.
FAMILIES = [
    # `$` that is not the last atom of the pattern, or is last in only one alternative: the
    # translation of `$` (lookahead vs consuming) depends on what FOLLOWS it.
    (
        "terminal_dollar",
        [
            r"a$|b$", r"^a$|^b$", r"(a$|b)c", r"(a$)", r"(?:a$)", r"a$\n", r"a$\s", r"a$$", r"$$",
            r"a$\b", r"(?=a$)", r"(?<=a)$", r"^(a|b$)", r"(a|b$)*c", r"(a$)+", r"a($)", r"a$|",
            r"|a$", r"(?:a|b$)", r"^(?:[a-z]+$)", r"(a$|b$)", r"(^a$|^b$)", r"a(?:$|c)",
            r"a(?:$|c)d", r"a$(?!x)", r"(?:a$)?", r"(a$){1}", r"a$ ", r"^$|^a$", r"(?:^|,)a$",
            r"a\Z|b$", r"a$|b\Z",
        ],
        ["a", "a\n", "a\n\n", "b", "b\n", "ac", "ac\n", "a\nc", "c", "\n", "", "ad", "abd", "a\nd",
         "aa\n", "a,", ",a\n", "a ", "a \n"],
    ),
    # Scoped inline flags: `(?m:...)` applies to its group only, so `$` after it is NOT multiline.
    (
        "scoped_flags",
        [
            r"(?m:a$)b$", r"(?m:a$)\nb", r"(?m:^b)", r"a(?m:$)", r"a(?m:$)\n", r"(?-m:a$)",
            r"(?i:A)b$", r"(?s:.)$", r"(?m:a$)|b$", r"[(?m)]$", r"(?ms:a$)", r"x(?m:^a$)",
            r"(?m:(?-m:a$))", r"(?m)a$\nb$", r"(?m:a$)(?-m:$)", r"(?i)a(?-i:B)$", r"(?m:$)a",
        ],
        ["a\nb", "a\nb\n", "ab", "ab\n", "b\n", "xa\n", "\nb", " a", "a", "a\n", "xa\nb", "A\nb\n",
         "AB", "aB\n"],
    ),
    # `\w`, `\W`, `\s`, `\b` over text whose characters the two engines classify differently:
    # Python's `\w` is `str.isalnum()` or `_` (letters + all numbers, NOT combining marks); Rust's is
    # Alphabetic + marks + Nd + Pc + Join_Control. Python's `\s` also covers U+001C..U+001F.
    (
        "unicode_classes",
        [
            r"^\w+$", r"\w", r"^\W+$", r"\W", r"^[\w]+$", r"^[^\w]+$", r"[\W]", r"\bx", r"x\b",
            r"^\s+$", r"\s", r"\S", r"[\s]", r"^[\w\s]+$", r"(?i)^\w+$", r"\d", r"^\d+$", r"^\w{3}$",
            r"^[\w.-]+$", r"^[^\W\d]+$", r"[^\w]", r"^[\S]+$", r"\w+\s\w+", r"\b\w+\b",
        ],
        ["e\u0301", "é", "caf\u00e9", "cafe\u0301", "नमस्ते", "สวัสดี", "²", "½", "①", "Ⅷ", "ⅷ", "٣",
         "日本語", "한글", "😀", "_", "\u0301", "\u200d", "a\u200db", "\x1c", "\x1f", "\x85", "\xa0",
         "\u2003", "\u3000", "a b", "ǅ", "ª", "µ", "ǆ", "x", "x ", " x", "ab cd", "e\u0301 e",
         "مَرْحَبًا", "שָׁלוֹם", "ひらがな", "ｱｲｳ", "①②", "a-b", "a.b", "1٣", "٣1"],
    ),
    # Character-class syntax: Python treats `&&`, `--`, `~~` and a nested `[` as literals;
    # Rust/fancy-regex treat them as set operations / nested classes. Plus the very common
    # email-validator spellings (`[\w-\.]`: Python 3.7+ REFUSES it as a bad range).
    (
        "class_syntax",
        [
            r"[a&&b]", r"[a--b]", r"[a~~b]", r"[[]", r"[[a]", r"[a[b]", r"[a[b]c]", r"[[:alpha:]]",
            r"[[:digit:]]+", r"[\w&&\d]", r"[^&&]", r"[&&]", r"[--]", r"[a-]", r"[-a]", r"[a\-z]",
            r"[\w-\.]", r"[\w.-]", r"[\d-z]", r"[a-\d]", r"[z-a]", r"^[\w-\.]+@([\w-]+\.)+[\w-]{2,4}$",
            r"^[\w.+-]+@[\w-]+\.[\w.-]+$", r"[\]]", r"[]]", r"[^]]", r"[\[]", r"[|]", r"[a|b]",
            r"[(]", r"[$]", r"[\^]", r"[x^]", r"[\b]", r"[a-z&&[^b]]", r"[\w--\d]", r"[a-c--b]",
            r"[ab&&bc]", r"[\x41-\x43]", r"[\u0041-\u0043]", r"[^\n]", r"[\s\S]", r"[\d\D]",
        ],
        ["a", "b", "ab", "&", "-", "[", "]", "a-b", "x@y.com", "a&b", "1", "z", "^", "|", "(", "$",
         "\x08", "!!", "abc@d.co", "x.y-z@mail.example.org", "~", "c", "B"],
    ),
    # Quantifier syntax: `{,n}` is `{0,n}` in Python and a literal in Rust; `{ 2}`, `{}`, `a{` and
    # friends are literals in Python.
    (
        "quantifier_syntax",
        [
            r"a{,3}", r"a{ 2}", r"a{2, 3}", r"a{2,3}", r"a{}", r"a{", r"a{x}", r"a{1,x}", r"a{,}",
            r"a{2,}", r"a{0}", r"a{2}{3}", r"a**", r"a+*", r"a*?", r"a?+", r"a++", r"a{1}+",
            r"x{,2}y", r"\d{,3}", r"a{3,2}", r"^a{,3}$", r"^a{2, }$", r"a{ ,3}", r"a{1,2}{2}",
        ],
        ["", "a", "aa", "aaa", "aaaa", "a{,3}", "a{ 2}", "a{", "a{}", "a{2, 3}", "x{,2}y", "xy",
         "xxy", "a{,}", "a{1,x}"],
    ),
    # Escapes and group syntax one engine has and the other does not.
    (
        "escapes_and_groups",
        [
            r"\<a\>", r"\/", r"\-", r"\_", r"\"", r"\'", r"\ ", r"\#", r"\@", r"\%", r"\=", r"\!",
            r"\:", r"\;", r"\,", r"\~", r"\&", r"\z", r"\Z", r"\h", r"\G", r"\K", r"\R", r"\pL",
            r"\p{L}", r"\x41", r"\x{41}", r"\u0041", r"\U00000041", r"\101", r"\0", r"\N{LATIN SMALL LETTER A}",
            r"(?#c)a", r"a(?#c)", r"(?x) a b # c", r"(?x)[ a]", r"(?x)a\ b", r"(?x)a[#]b",
            r"(?<n>a)", r"(?P<n>a)", r"(?P<n>a)(?P=n)", r"(?<=a|bc)d", r"(?<=ab|cd)e",
            r"(?(1)a|b)", r"(a)?(?(1)b|c)", r"(?i)a(?-i)b", r"a(?i)b", r"(?|a|b)", r"(?>a)",
            r"(?:a)", r"(?i:a)", r"(?s:.)", r"(?a)\w", r"(?u)\w", r"(?L)a", r"\a", r"\e", r"\Qa\E",
            r"\1", r"(a)\1", r"(a)\2", r"(a)\g<1>", r"\8", r"(?P=n)", r"(?P<n>a)(?P=m)",
        ],
        ["a", "b", "A", "B", "aB", "Ab", "ab", "<a>", "/", "-", "_", "\"", "'", " ", "#", "@", "\n",
         "101", "\x00", "a b", "abc", "ac", "bc", "ab\n", "aa", "bb", "cd", "cde", "abe", "dd",
         "\x07", "\x1b", "aA", "a1", "a-"],
    ),
    # Case folding.
    (
        "casefold",
        [
            r"(?i)ß", r"(?i)ss", r"(?i)ǆ", r"(?i)ǅ", r"(?i)σ", r"(?i)ς", r"(?i)k", r"(?i)K",
            r"(?i)ſ", r"(?i)s", r"(?i)İ", r"(?i)i", r"(?i)ı", r"(?i)É", r"(?i)[a-z]", r"(?i)[^a]",
            r"(?i)\w", r"(?i)[ß]",
        ],
        ["ß", "SS", "ss", "ǆ", "Ǆ", "ǅ", "σ", "Σ", "ς", "k", "K", "\u212a", "ſ", "s", "S", "İ", "i",
         "I", "ı", "é", "É", "A", "a", "ẞ"],
    ),
    # `.` and line terminators.
    (
        "line_terminators",
        [
            r".", r"a.b", r"(?s)a.b", r"a.b$", r"^.$", r".\n", r"\r", r"a\rb", r"(?m)^b", r"(?m)a$",
            r"(?m)a$\rb", r"^b", r"a$", r"(?m)^$", r"(?s).$", r"a\Z", r"(?m)a\Z",
        ],
        ["a\nb", "a\rb", "a\r\nb", "a\r\n", "a\r", "\u2028", "a\u2028b", "a\x0bb", "a\x0cb", "\x85",
         "a\x85b", "a", "a\n", "\r\n", "b\n", "a\nb\n"],
    ),
    # Patterns a Django app really sends to `__regex` / `__iregex`.
    (
        "realistic",
        [
            r"^\d{4}-\d{2}-\d{2}$", r"^[A-Z]{2}\d{6}$", r"^(?:\+?\d{1,3})?[-. (]*\d{3}[-. )]*\d{3}[-. ]*\d{4}$",
            r"^#?[0-9a-fA-F]{6}$", r"^https?://[^\s/$.?#].[^\s]*$", r"^[^@\s]+@[^@\s]+\.[^@\s]+$",
            r"\b(foo|bar)\b", r"^\s*$", r"^(?!.*\.\.)[\w.]+$", r"(?i)^admin", r"^[A-Za-z0-9_.+-]+$",
            r"\.(jpg|png|gif)$", r"(?i)\.(JPG|png)$", r"^\w+(-\w+)*$", r"\d{3,}", r"^.{3,10}$",
            r"^[^<>]*$", r"^[\w ]+$", r"^(\d+)\.(\d+)$", r"(\w+)@(\w+)\.com$", r"^(a|b|c)+$",
            r"^[\w\s'-]+$", r"^\w+\s+\w+$", r"(?i)^[a-z][a-z0-9_]{2,}$", r"^(?:[01]\d|2[0-3]):[0-5]\d$",
        ],
        ["2026-11-07", "2026-11-07\n", "AB123456", "+1 (555) 123-4567", "#a1b2c3", "https://x.io/p?q=1",
         "a@b.co", "foo bar", "foobar", "a..b", "Admin", "admin\n", "photo.JPG", "photo.jpg\n", "my-slug-1",
         "abc", "a<b", "John O'Brien", "José García", "Jose\u0301 Garcia", "1.5", "bob@example.com",
         "abcabc", "12:59", "24:00", "x" * 12, "नमस्ते दुनिया", "Zoë", "  ", ""],
    ),
]

# A seeded random-pattern family (expert review 2026-11-07 #74 / #88). A grammar over the tokens
# `translate` rewrites — `$` in every position, scoped `(?m:` / `(?-m:` groups, alternation,
# classes whose members include `[`, `&`, `~`, `-`, `^`, `\w`, `\W` — keeps only the patterns CPython
# COMPILES, so every case asserts "fathom agrees with CPython" with no known-divergence escape. It
# found the cases the hand-written families did not think of (a 120,000-case run of this grammar went
# from 10,009 disagreements to 145, all of them `\b` next to a combining mark / `No` digit, or a
# quantified `$`-only group, both excluded here and documented). Not generated: `\b` (a documented
# divergence), quantifiers on groups (`(?:$){,2}` is refused by fancy-regex: a documented
# limitation), and anchors under a quantifier.
GRAMMAR_PATTERNS = 1400
GRAMMAR_SUBJECTS_PER_PATTERN = 6
GRAMMAR_SUBJECTS = [
    "", "a", "ab", "abc", "a\n", "b\n", "ab\n", "a\n\n", "\n", "a b", "1", "a1_", "é", "e\u0301",
    "²", "a\nb", "ab\nc\n", "x", "-", "]", "[", "&", "~", "^", "$", "(", ")", "|",
]


def grammar_cases():
    import random

    rng = random.Random(20261107)
    atoms = ["a", "b", "c", "1", "-", "_", " ", "\n", ".", "\\d", "\\w", "\\W", "\\s", "\\S", "\\D",
             "é", "e\u0301", "²", "x"]
    class_items = ["a", "b", "c", "-", "[", "]", "&", "~", "^", "$", "\\w", "\\W", "\\d", "\\s",
                   "a-c", "0-9", "\\-", "\\]", "\\[", "é", "|", "(", ")", "\\\\"]
    quantifiers = ["*", "+", "?", "{2}", "{1,2}", "{,2}", "*?"]

    def char_class():
        items = [rng.choice(class_items) for _ in range(rng.randint(1, 5))]
        return "[" + ("^" if rng.random() < 0.2 else "") + "".join(items) + "]"

    def quantified(atom):
        return atom + rng.choice(quantifiers) if rng.random() < 0.3 else atom

    def atom(depth):
        x = rng.random()
        if x < 0.35:
            return quantified(rng.choice(atoms))
        if x < 0.6:
            return quantified(char_class())
        if depth < 2 and x < 0.7:
            return "(" + alternation(depth + 1) + ")"
        if depth < 2 and x < 0.75:
            return "(?:" + alternation(depth + 1) + ")"
        if depth < 2 and x < 0.8:
            return "(?m:" + alternation(depth + 1) + ")"
        if depth < 2 and x < 0.83:
            return "(?-m:" + alternation(depth + 1) + ")"
        if x < 0.92:
            return "$"
        if x < 0.95:
            return "^"
        return rng.choice(["\\Z", "\\A"])

    def sequence(depth):
        return "".join(atom(depth) for _ in range(rng.randint(1, 4)))

    def alternation(depth=0):
        return "|".join(sequence(depth) for _ in range(rng.randint(1, 3)))

    seen = set()
    while len(seen) < GRAMMAR_PATTERNS:
        pattern = alternation()
        if pattern in seen:
            continue
        try:
            re.compile(pattern)
        except Exception:  # noqa: BLE001
            continue
        seen.add(pattern)
        for subject in rng.sample(GRAMMAR_SUBJECTS, GRAMMAR_SUBJECTS_PER_PATTERN):
            yield pattern, subject



# Disagreements with CPython that are accepted and documented: pattern -> (why, subjects).
# `subjects` is the exact list of subjects the two engines still disagree on, or None for "every
# subject" (the pattern is accepted/refused differently). `tests/re_oracle.rs` replays the corpus
# with these entries allowed to differ and then asserts every entry STILL differs on every listed
# subject, so fixing a divergence forces its entry to be deleted rather than going stale.
#
# Two kinds. (1) fathom answers where CPython raises: a pattern CPython rejects ("bad escape",
# "multiple repeat", a global flag mid-pattern since 3.11) that the engine gives a meaning to. An
# app that relies on Django erroring gets an answer instead; no query that WORKS on Django breaks.
# (2) CPython supports it and fathom does not, or reads it differently: the rows below the second
# comment, each with the reason it was not worth fixing (see docs/component-notes.md).
_MORE_PERMISSIVE = "CPython rejects this pattern; the engine accepts it"
KNOWN_DIVERGENCES = {
    # --- (1) CPython raises, fathom answers ---
    r"(?<=a|bc)d": ("CPython needs a fixed-width lookbehind; fancy-regex allows alternatives of different widths", None),
    r"(?<n>a)": ("CPython has no (?<name>…) group syntax, only (?P<name>…); the engine accepts both", None),
    r"(?(1)a|b)": ("a conditional on a group that does not exist is a CPython RuntimeError (a CPython bug); the engine accepts it", None),
    r"(a)\g<1>": ("\\g<…> is replacement-template syntax CPython refuses in a pattern; fancy-regex reads a subroutine call", None),
    r"a(?i)b": ("CPython 3.11+ refuses a global flag that is not at the start; the engine applies it from that point", None),
    r"(?i)a(?-i)b": ("CPython refuses (?-i) without a scope; the engine applies it from that point", None),
    r"\e": (_MORE_PERMISSIVE + " (\\e is not a Python escape)", None),
    r"\G": (_MORE_PERMISSIVE + " (\\G is not a Python escape)", None),
    r"\h": (_MORE_PERMISSIVE + " (\\h is not a Python escape)", None),
    r"\K": (_MORE_PERMISSIVE + " (\\K is not a Python escape)", None),
    r"\R": (_MORE_PERMISSIVE + " (\\R is not a Python escape)", None),
    r"\pL": (_MORE_PERMISSIVE + " (\\p is not a Python escape)", None),
    r"\p{L}": (_MORE_PERMISSIVE + " (\\p{…} is not a Python escape)", None),
    r"\x{41}": (_MORE_PERMISSIVE + " (\\x takes exactly two hex digits in Python)", None),
    r"\z": (_MORE_PERMISSIVE + " (Python spells it \\Z)", None),
    r"a{1,2}{2}": ("CPython: multiple repeat; the engine nests the repetitions", None),
    r"a{2}{3}": ("CPython: multiple repeat; the engine nests the repetitions", None),
    r"a{3,2}": ("CPython: min repeat greater than max repeat; the engine accepts it", None),
    # --- (2) CPython supports it, fathom does not / reads it differently ---
    r"\0": ("octal escapes (\\0, \\101) are not translated: a NUL / octal literal in a Django regex is not a realistic pattern", None),
    r"\101": ("octal escapes (\\0, \\101) are not translated: a NUL / octal literal in a Django regex is not a realistic pattern", None),
    r"\N{LATIN SMALL LETTER A}": ("\\N{NAME} needs the Unicode name table; not available", None),
    r"(?a)\w": ("the (?a) ASCII flag is not an engine flag; the pattern is refused", None),
    r"(?i)[a-z]": ("IGNORECASE: CPython equates i, ı (U+0131) and İ (U+0130); Unicode simple case folding does not (Turkish dotted/dotless i)", ["İ", "ı"]),
    r"(?i)i": ("IGNORECASE: CPython equates i, ı (U+0131) and İ (U+0130); Unicode simple case folding does not (Turkish dotted/dotless i)", ["İ", "ı"]),
    r"(?i)İ": ("IGNORECASE: CPython equates i, ı (U+0131) and İ (U+0130); Unicode simple case folding does not (Turkish dotted/dotless i)", ["i", "I", "ı"]),
    r"(?i)ı": ("IGNORECASE: CPython equates i, ı (U+0131) and İ (U+0130); Unicode simple case folding does not (Turkish dotted/dotless i)", ["İ", "i", "I"]),
    r"\s": ("CPython's \\s also matches U+001C..U+001F (str.isspace); the engine's White_Space does not", ["\x1c", "\x1f"]),
    r"\S": ("CPython's \\s also matches U+001C..U+001F (str.isspace); the engine's White_Space does not", ["\x1c", "\x1f"]),
    r"[\s]": ("CPython's \\s also matches U+001C..U+001F (str.isspace); the engine's White_Space does not", ["\x1c", "\x1f"]),
    r"^\s+$": ("CPython's \\s also matches U+001C..U+001F (str.isspace); the engine's White_Space does not", ["\x1c", "\x1f"]),
    r"^[\S]+$": ("CPython's \\s also matches U+001C..U+001F (str.isspace); the engine's White_Space does not", ["\x1c", "\x1f"]),
    r"^[\w\s]+$": ("CPython's \\s also matches U+001C..U+001F (str.isspace); the engine's White_Space does not", ["\x1c", "\x1f"]),
    r"\b\w+\b": ("\\b cannot be given CPython's definition without a lookaround (the backtracking VM), so a pattern that uses \\b keeps the engine's \\w too: both treat combining marks as word characters and `²`/`①` (category No) as not", ["²", "½", "①", "\u0301", "\u200d", "①②"]),
}


def classify(pattern, subject):
    try:
        return {"match": bool(re.search(pattern, subject))}
    except Exception:  # noqa: BLE001 - re.error, but also RuntimeError / OverflowError on some patterns
        # A pattern CPython itself rejects. Recorded so the Rust side is checked for erroring
        # too — accepting a pattern Python refuses is its own kind of divergence, and the
        # opposite (refusing one Python accepts) is the bug class this whole file exists for.
        return {"error": True}


def main():
    import warnings

    # `[[]`, `[a&&b]` and friends are FutureWarnings in CPython: they are exactly the cases under test.
    warnings.simplefilter("ignore")

    cases = []
    invalid = 0

    def add(family, pattern, subject):
        nonlocal invalid
        result = classify(pattern, subject)
        invalid += "error" in result
        case = {"pattern": pattern, "subject": subject, "result": result}
        if family:
            case["family"] = family
        cases.append(case)

    for pattern, subject in itertools.product(PATTERNS, SUBJECTS):
        add(None, pattern, subject)

    for family, patterns, subjects in FAMILIES:
        for pattern, subject in itertools.product(patterns, subjects):
            add(family, pattern, subject)

    grammar_count = len(cases)
    for pattern, subject in grammar_cases():
        add("grammar", pattern, subject)
    grammar_count = len(cases) - grammar_count

    meta = {
        "_meta": True,
        "generated_by": "native/fathom_udf/tests/oracle/generate_re_cases.py",
        "source": "CPython re.search",
        "python": platform.python_version(),
        "patterns": len(PATTERNS),
        "subjects": len(SUBJECTS),
        "families": {
            **{name: len(p) * len(s) for name, p, s in FAMILIES},
            "grammar": grammar_count,
        },
        "count": len(cases),
        "invalid_patterns": invalid,
        "known_divergences": [
            {"pattern": pat, "why": why, "subjects": subjects}
            for pat, (why, subjects) in sorted(KNOWN_DIVERGENCES.items())
        ],
    }

    with open("re_cases.jsonl", "w", encoding="utf-8") as fh:
        fh.write(json.dumps(meta, separators=(",", ":"), ensure_ascii=False) + "\n")
        for case in cases:
            fh.write(json.dumps(case, separators=(",", ":"), ensure_ascii=False) + "\n")

    matched = sum(1 for c in cases if c["result"].get("match"))
    print(
        f"wrote re_cases.jsonl: {len(cases)} cases "
        f"({len(PATTERNS)} patterns x {len(SUBJECTS)} subjects + {len(FAMILIES)} families), "
        f"{matched} matching, {invalid} invalid-pattern cases "
        f"(python {platform.python_version()})"
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
