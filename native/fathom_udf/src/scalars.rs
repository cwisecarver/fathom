//! The pure scalar UDFs: text padding/repetition/reversal, the hash family, `regexp`, and the two
//! math functions SQLite's `SQLITE_ENABLE_MATH_FUNCTIONS` build does not already provide.
//!
//! Everything string-shaped operates on **Unicode scalar values**, not bytes. Python's `len()`,
//! slicing and `[::-1]` are all code-point-based, so a byte-oriented implementation agrees with
//! Django for ASCII and silently disagrees the moment a tenant stores an accented character —
//! which is exactly the kind of divergence that never shows up in a test suite written in English.

use md5::Md5;
use sha1::Sha1;
use sha2::{Digest, Sha224, Sha256, Sha384, Sha512};

/// Python's `s[:n]`, including `n < 0` meaning "all but the last |n| characters".
fn py_prefix(s: &str, n: i64) -> String {
    let len = s.chars().count() as i64;
    let end = if n < 0 { (len + n).max(0) } else { n.min(len) };
    s.chars().take(end as usize).collect()
}

/// Python's `s * n` — a non-positive count yields the empty string.
fn py_repeat(s: &str, n: i64) -> String {
    if n <= 0 {
        return String::new();
    }
    s.repeat(n as usize)
}

/// `_sqlite_lpad(text, length, fill_text)`.
///
/// Note the asymmetry with `rpad`: when the text is already at least `length` long, lpad
/// **truncates from the left** (`text[:length]`), it does not return the text untouched.
pub fn lpad(text: &str, length: i64, fill: &str) -> String {
    let delta = length - text.chars().count() as i64;
    if delta <= 0 {
        return py_prefix(text, length);
    }
    // `(fill * length)[:delta]` — the fill is repeated `length` times, not `delta` times, so an
    // empty fill string yields an empty pad rather than looping forever.
    let padding = py_prefix(&py_repeat(fill, length), delta);
    format!("{padding}{text}")
}

/// `_sqlite_rpad(text, length, fill_text)` — `(text + fill_text * length)[:length]`.
pub fn rpad(text: &str, length: i64, fill: &str) -> String {
    let combined = format!("{text}{}", py_repeat(fill, length));
    py_prefix(&combined, length)
}

/// `_sqlite_repeat(text, count)`.
pub fn repeat(text: &str, count: i64) -> String {
    py_repeat(text, count)
}

/// `_sqlite_reverse(text)` — `text[::-1]`, reversing code points.
pub fn reverse(text: &str) -> String {
    text.chars().rev().collect()
}

/// Hex digest of the UTF-8 encoding, matching `hashlib.<algo>(text.encode()).hexdigest()`.
pub fn md5_hex(text: &str) -> String {
    hex(Md5::digest(text.as_bytes()).as_slice())
}

pub fn sha1_hex(text: &str) -> String {
    hex(Sha1::digest(text.as_bytes()).as_slice())
}

pub fn sha224_hex(text: &str) -> String {
    hex(Sha224::digest(text.as_bytes()).as_slice())
}

pub fn sha256_hex(text: &str) -> String {
    hex(Sha256::digest(text.as_bytes()).as_slice())
}

pub fn sha384_hex(text: &str) -> String {
    hex(Sha384::digest(text.as_bytes()).as_slice())
}

pub fn sha512_hex(text: &str) -> String {
    hex(Sha512::digest(text.as_bytes()).as_slice())
}

fn hex(bytes: &[u8]) -> String {
    let mut out = String::with_capacity(bytes.len() * 2);
    for b in bytes {
        out.push_str(&format!("{b:02x}"));
    }
    out
}

/// `1 / tan(x)`. SQLite's math build has no COT, so Django registers one unconditionally.
pub fn cot(x: f64) -> f64 {
    1.0 / x.tan()
}

/// `_sqlite_bitxor(x, y)`.
pub fn bitxor(x: i64, y: i64) -> i64 {
    x ^ y
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn lpad_pads_on_the_left() {
        assert_eq!(lpad("x", 3, "0"), "00x");
        assert_eq!(lpad("abc", 5, "-"), "--abc");
        // Multi-character fill is repeated then truncated to exactly the GAP (4 here), and the
        // text is appended after — so this is "abab" + "x", not the first 5 chars of "ababab…x".
        assert_eq!(lpad("x", 5, "ab"), "ababx");
    }

    #[test]
    fn lpad_truncates_when_the_text_is_already_long_enough() {
        // Django's lpad TRUNCATES rather than returning the input — the case an "if too short,
        // pad; else return text" implementation gets wrong.
        assert_eq!(lpad("abcdef", 3, "0"), "abc");
        assert_eq!(lpad("abc", 3, "0"), "abc");
    }

    #[test]
    fn rpad_pads_on_the_right_and_truncates() {
        assert_eq!(rpad("x", 3, "0"), "x00");
        assert_eq!(rpad("abc", 5, "-"), "abc--");
        assert_eq!(rpad("abcdef", 3, "0"), "abc");
        assert_eq!(rpad("x", 5, "ab"), "xabab");
    }

    #[test]
    fn pad_with_an_empty_fill_does_not_hang() {
        // `("" * n)[:delta]` is "" in Python, so the result is just the (possibly truncated) text.
        assert_eq!(lpad("x", 5, ""), "x");
        assert_eq!(rpad("x", 5, ""), "x");
    }

    #[test]
    fn pad_with_zero_or_negative_length() {
        assert_eq!(lpad("abc", 0, "0"), "");
        assert_eq!(rpad("abc", 0, "0"), "");
        // Python's negative slice: text[:-1] drops the last character.
        assert_eq!(lpad("abc", -1, "0"), "ab");
    }

    #[test]
    fn text_functions_count_code_points_not_bytes() {
        // "é" is two bytes in UTF-8 and one character. A byte-based implementation pads one short
        // and can split the sequence, producing invalid UTF-8.
        assert_eq!(lpad("é", 3, "0"), "00é");
        assert_eq!(rpad("é", 3, "0"), "é00");
        assert_eq!(reverse("héllo"), "olléh");
        // Non-BMP: an emoji is one code point and four bytes.
        assert_eq!(reverse("a🙂b"), "b🙂a");
        assert_eq!(lpad("🙂", 2, "-"), "-🙂");
    }

    #[test]
    fn repeat_and_reverse() {
        assert_eq!(repeat("x", 3), "xxx");
        assert_eq!(repeat("ab", 2), "abab");
        assert_eq!(repeat("x", 0), "");
        assert_eq!(repeat("x", -1), "");
        assert_eq!(reverse("abc"), "cba");
        assert_eq!(reverse(""), "");
    }

    #[test]
    fn hashes_match_the_known_digests_of_x() {
        // Fixed vectors — these are the digests of the single character "x", the value the
        // tracked list in django_udf_compat_test.exs probes with.
        assert_eq!(md5_hex("x"), "9dd4e461268c8034f5c8564e155c67a6");
        assert_eq!(sha1_hex("x"), "11f6ad8ec52a2984abaafd7c3b516503785c2072");
        assert_eq!(
            sha256_hex("x"),
            "2d711642b726b04401627ca9fbac32f5c8530fb1903cc4db02258717921a4881"
        );
    }

    #[test]
    fn hashes_of_the_empty_string() {
        assert_eq!(md5_hex(""), "d41d8cd98f00b204e9800998ecf8427e");
        assert_eq!(sha1_hex(""), "da39a3ee5e6b4b0d3255bfef95601890afd80709");
        assert_eq!(
            sha224_hex(""),
            "d14a028c2a3a2bc9476102bb288234c415a2b01f828ea62ac5b3e42f"
        );
        assert_eq!(
            sha256_hex(""),
            "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
        );
        assert_eq!(
            sha384_hex(""),
            "38b060a751ac96384cd9327eb1b1e36a21fdb71114be07434c0cc7bf63f6e1da274edebfe76f65fbd51ad2f14898b95b"
        );
        assert_eq!(
            sha512_hex(""),
            "cf83e1357eefb8bdf1542850d66d8007d620e4050b5715dc83f4a921d36ce9ce47d0d13c5d85f2b0ff8318d2877eec2f63b931bd47417a81a538327af927da3e"
        );
    }

    #[test]
    fn hashes_are_over_utf8_bytes() {
        // Python hashes `text.encode()`, i.e. UTF-8. Pinned so a future change to a
        // latin-1/byte-oriented path is caught.
        // Cross-checked against both `hashlib.sha256("é".encode()).hexdigest()` and
        // `printf 'é' | shasum -a 256` (2026-08-05) — "é" is the two bytes c3 a9 in UTF-8.
        assert_eq!(
            sha256_hex("é"),
            "4a99557e4033c3539de2eb65472017cad5f9557f7a0625a09f1c3f6e2ba69c4c"
        );
    }

    #[test]
    fn digest_lengths() {
        assert_eq!(md5_hex("x").len(), 32);
        assert_eq!(sha1_hex("x").len(), 40);
        assert_eq!(sha224_hex("x").len(), 56);
        assert_eq!(sha256_hex("x").len(), 64);
        assert_eq!(sha384_hex("x").len(), 96);
        assert_eq!(sha512_hex("x").len(), 128);
    }

    #[test]
    fn bitxor_and_cot() {
        assert_eq!(bitxor(1, 2), 3);
        assert_eq!(bitxor(5, 3), 6);
        assert_eq!(bitxor(-1, 0), -1);
        assert!((cot(1.0) - (1.0 / 1.0_f64.tan())).abs() < 1e-12);
        // cot(pi/4) == 1
        assert!((cot(std::f64::consts::FRAC_PI_4) - 1.0).abs() < 1e-12);
    }
}
