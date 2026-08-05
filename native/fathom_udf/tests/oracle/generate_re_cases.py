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


def main():
    cases = []
    invalid = 0

    for pattern, subject in itertools.product(PATTERNS, SUBJECTS):
        try:
            result = {"match": bool(re.search(pattern, subject))}
        except re.error:
            # A pattern CPython itself rejects. Recorded so the Rust side is checked for erroring
            # too — accepting a pattern Python refuses is its own kind of divergence, and the
            # opposite (refusing one Python accepts) is the bug class this whole file exists for.
            result = {"error": True}
            invalid += 1

        cases.append({"pattern": pattern, "subject": subject, "result": result})

    meta = {
        "_meta": True,
        "generated_by": "native/fathom_udf/tests/oracle/generate_re_cases.py",
        "source": "CPython re.search",
        "python": platform.python_version(),
        "patterns": len(PATTERNS),
        "subjects": len(SUBJECTS),
        "count": len(cases),
        "invalid_patterns": invalid,
    }

    with open("re_cases.jsonl", "w", encoding="utf-8") as fh:
        fh.write(json.dumps(meta, separators=(",", ":"), ensure_ascii=False) + "\n")
        for case in cases:
            fh.write(json.dumps(case, separators=(",", ":"), ensure_ascii=False) + "\n")

    matched = sum(1 for c in cases if c["result"].get("match"))
    print(
        f"wrote re_cases.jsonl: {len(cases)} cases "
        f"({len(PATTERNS)} patterns x {len(SUBJECTS)} subjects), "
        f"{matched} matching, {invalid} invalid-pattern cases "
        f"(python {platform.python_version()})"
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
