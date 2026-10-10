//! The ENGINE half of the tenant PRAGMA gate (expert review 2026-10-08 #1, tier b).
//!
//! `Fathom.ShardExecutor` refuses tenant PRAGMA assignments outside an allow-list by PARSING the
//! statement text. Five parser defects in that gate shipped as live bypasses (a 200-byte window,
//! `main . name`, a leading `;`, `EXPLAIN PRAGMA`, a `;` inside a comment), and each one let a
//! tenant switch off a protective pragma: `max_page_count` (the size cap), `synchronous` (per-commit
//! durability), `journal_mode`, `writable_schema`, and the process-global `hard_heap_limit`, which
//! failed every co-resident tenant's allocations until the BEAM restarted.
//!
//! SQLite's authorizer sees the pragma AFTER SQLite has parsed it, so no spelling reaches it in a
//! different shape: `SQLITE_PRAGMA` arrives with the unquoted name (arg 3), the value or NULL for a
//! read (arg 4), and the schema the statement named or NULL (arg 5). This module installs an
//! authorizer that DENIES an assignment (arg 4 non-NULL) whose name is not on the tenant allow-list.
//! A READ (arg 4 NULL) is always allowed — Django reads pragmas, and a read discloses only this
//! connection's own configuration. Verified 2026-10-09: `PRAGMA synchronous /*;*/ = OFF` (the
//! ae5171a bypass) and `EXPLAIN PRAGMA synchronous=OFF` both reach it as `synchronous` = `OFF`.
//!
//! ## One slot
//!
//! A connection has ONE authorizer. Before this module, tenant handles carried exqlite's, denying
//! `ATTACH`/`DETACH` (expert review 2026-08-01 #1: a cross-tenant read+write breach). Installing
//! this one REPLACES exqlite's, so it denies `ATTACH`/`DETACH` itself. When the extension is not
//! loaded, `Fathom.Shard.Connection` keeps exqlite's authorizer exactly as before.
//!
//! ## Tenant handles only
//!
//! SQLite runs `VACUUM INTO` as an internal ATTACH, so an authorizer that denies ATTACH also denies
//! `VACUUM INTO` — which is how the coordinator takes every durability snapshot. Nothing here is
//! installed when the extension loads; only `fathom_pragma_guard(...)` installs it, and only
//! `Fathom.Shard.Connection` calls that, only on tenant handles.
//!
//! ## Set once, by fathom
//!
//! `fathom_pragma_guard(allowed_names, pinned)` installs the authorizer with the policy it is given.
//! Only the FIRST call does anything; later calls return 0 and change nothing. Fathom calls it last
//! in the tenant open, before any tenant SQL, so a tenant cannot widen its own policy. The function
//! is `SQLITE_DIRECTONLY` and owns the per-connection state; its destructor frees it at close.
//!
//! * `allowed_names` — comma-separated pragma names a tenant may ASSIGN. Built by Elixir from the
//!   same lists the text gate uses (allow + introspect + operator `:tenant_pragma_allow`, minus the
//!   hard deny list), so the two cannot drift: there is no copy of the list in Rust.
//! * `pinned` — comma-separated `[schema.]name=value` assignments allowed with EXACTLY that value.
//!   These are the pragmas fathom itself re-applies on a tenant handle after open
//!   (`synchronous=FULL` and `cache_size` on pooled reuse, `query_only=ON` on a `:ro` reuse,
//!   `temp.max_page_count` before TEMP DDL). Each is fathom's own protective value, so a tenant
//!   sending the same assignment changes nothing.
//!
//! Matching is ASCII case-insensitive. SQLite hands the name and value dequoted, and the schema as
//! its canonical name (`main`, `temp`), so quoting cannot change the outcome. A schema-qualified
//! assignment of an ALLOWED name is allowed (`PRAGMA main.foreign_keys=ON`, as Django may send); a
//! pinned pair matches only with the schema it was pinned with.
//!
//! ## Cost
//!
//! An authorizer runs at PREPARE time, once per action the statement contains (each column read,
//! each table, ...). For every action but a PRAGMA assignment this is one integer comparison.

use std::collections::HashSet;
use std::ffi::{c_void, CStr, CString};
use std::os::raw::{c_char, c_int};
use std::sync::OnceLock;

use rusqlite::{ffi, Connection, Error, Result};

/// What a tenant may assign. Built once per connection, read on every PRAGMA prepare.
#[derive(Debug, Default)]
pub struct Policy {
    names: HashSet<String>,
    pinned: HashSet<String>,
}

fn split_list(s: &str) -> HashSet<String> {
    s.split(',')
        .map(|item| item.trim().to_ascii_lowercase())
        .filter(|item| !item.is_empty())
        .collect()
}

impl Policy {
    pub fn parse(names: &str, pinned: &str) -> Policy {
        Policy {
            names: split_list(names),
            pinned: split_list(pinned),
        }
    }
}

/// The pure decision, separate so it can be unit-tested without SQLite. Arguments are the
/// authorizer's action code and its arg 3/4/5 (name, value, schema for `SQLITE_PRAGMA`).
pub fn decide(
    action: c_int,
    name: Option<&[u8]>,
    value: Option<&[u8]>,
    schema: Option<&[u8]>,
    policy: &Policy,
) -> c_int {
    match action {
        ffi::SQLITE_ATTACH | ffi::SQLITE_DETACH => ffi::SQLITE_DENY,
        ffi::SQLITE_PRAGMA => match value {
            // A read: always allowed.
            None => ffi::SQLITE_OK,
            Some(value) => {
                if pragma_assignment_allowed(name, value, schema, policy) {
                    ffi::SQLITE_OK
                } else {
                    ffi::SQLITE_DENY
                }
            }
        },
        _ => ffi::SQLITE_OK,
    }
}

fn pragma_assignment_allowed(
    name: Option<&[u8]>,
    value: &[u8],
    schema: Option<&[u8]>,
    policy: &Policy,
) -> bool {
    // A name that is absent or not UTF-8 matches nothing: refuse.
    let Some(Ok(name)) = name.map(std::str::from_utf8) else {
        return false;
    };
    let name = name.to_ascii_lowercase();
    if policy.names.contains(&name) {
        return true;
    }
    if policy.pinned.is_empty() {
        return false;
    }
    let (Ok(value), Ok(schema)) = (
        std::str::from_utf8(value),
        schema.map(std::str::from_utf8).transpose(),
    ) else {
        return false;
    };
    let key = match schema {
        Some(schema) => format!("{schema}.{name}={value}"),
        None => format!("{name}={value}"),
    };
    policy.pinned.contains(&key.to_ascii_lowercase())
}

struct Guard {
    policy: OnceLock<Policy>,
}

unsafe fn opt_bytes<'a>(p: *const c_char) -> Option<&'a [u8]> {
    if p.is_null() {
        None
    } else {
        Some(CStr::from_ptr(p).to_bytes())
    }
}

unsafe extern "C" fn authorize(
    ctx: *mut c_void,
    action: c_int,
    arg3: *const c_char,
    arg4: *const c_char,
    arg5: *const c_char,
    _arg6: *const c_char,
) -> c_int {
    // Every action but these three is allowed without touching the state.
    if action != ffi::SQLITE_PRAGMA && action != ffi::SQLITE_ATTACH && action != ffi::SQLITE_DETACH
    {
        return ffi::SQLITE_OK;
    }
    let guard = &*(ctx as *const Guard);
    // Installed only after the policy is set, so this is always Some; deny if it somehow is not.
    let Some(policy) = guard.policy.get() else {
        return ffi::SQLITE_DENY;
    };
    decide(
        action,
        opt_bytes(arg3),
        opt_bytes(arg4),
        opt_bytes(arg5),
        policy,
    )
}

unsafe fn text_arg(argv: *mut *mut ffi::sqlite3_value, i: usize) -> Option<String> {
    let v = *argv.add(i);
    if ffi::sqlite3_value_type(v) != ffi::SQLITE_TEXT {
        return None;
    }
    let p = ffi::sqlite3_value_text(v);
    if p.is_null() {
        return Some(String::new());
    }
    let len = ffi::sqlite3_value_bytes(v).max(0) as usize;
    let bytes = std::slice::from_raw_parts(p, len);
    std::str::from_utf8(bytes).ok().map(str::to_owned)
}

unsafe fn result_error(ctx: *mut ffi::sqlite3_context, msg: &str) {
    let msg = CString::new(msg).expect("static message");
    ffi::sqlite3_result_error(ctx, msg.as_ptr(), -1);
}

/// `fathom_pragma_guard(allowed_names, pinned)`: install the authorizer. Returns 1 when this call
/// installed it, 0 when one was already installed (the call is then ignored — see the module docs).
unsafe extern "C" fn install_guard(
    ctx: *mut ffi::sqlite3_context,
    argc: c_int,
    argv: *mut *mut ffi::sqlite3_value,
) {
    let guard_ptr = ffi::sqlite3_user_data(ctx);
    let guard = &*(guard_ptr as *const Guard);

    if guard.policy.get().is_some() {
        ffi::sqlite3_result_int64(ctx, 0);
        return;
    }
    if argc != 2 {
        result_error(ctx, "fathom_pragma_guard: expected 2 arguments");
        return;
    }
    let (Some(names), Some(pinned)) = (text_arg(argv, 0), text_arg(argv, 1)) else {
        result_error(ctx, "fathom_pragma_guard: arguments must be UTF-8 text");
        return;
    };
    if guard.policy.set(Policy::parse(&names, &pinned)).is_err() {
        ffi::sqlite3_result_int64(ctx, 0);
        return;
    }

    let db = ffi::sqlite3_context_db_handle(ctx);
    let rc = ffi::sqlite3_set_authorizer(db, Some(authorize), guard_ptr);
    if rc != ffi::SQLITE_OK {
        result_error(ctx, "fathom_pragma_guard: sqlite3_set_authorizer failed");
        return;
    }
    apply_limits(db);
    ffi::sqlite3_result_int64(ctx, 1);
}

/// Per-connection SQLite size limits for a TENANT handle (expert review 2026-10-10 #2).
///
/// With no `sqlite3_limit` anywhere, `SQLITE_LIMIT_LENGTH` is its compile-time 1e9, so one
/// statement (`SELECT length(randomblob(999999999))`) allocated ~917 MB on the node: the deadline
/// bounds time and `soft_heap_limit` only sheds cache, so a single token, `:ro` included, could OOM
/// every co-resident tenant. `sqlite3_limit` is lowering-only past the compile-time maximum and is
/// per connection, so setting it here — in the same set-once, `SQLITE_DIRECTONLY` call that installs
/// the authorizer — leaves coordinator/migrator/`VACUUM INTO` handles (which never call
/// `fathom_pragma_guard`) on SQLite's defaults. A tenant cannot raise them: no SQL reaches
/// `sqlite3_limit`, and a second `fathom_pragma_guard` call is ignored.
pub const LIMIT_LENGTH: c_int = 64 * 1024 * 1024;
pub const LIMIT_SQL_LENGTH: c_int = 16 * 1024 * 1024;
pub const LIMIT_LIKE_PATTERN_LENGTH: c_int = 10_000;
// EXPR_DEPTH is deliberately NOT lowered: Django builds left-associative OR/AND chains (hundreds of
// Q objects), depth is not a memory lever, and this finding is about memory. SQLite's 1000 stays.

unsafe fn apply_limits(db: *mut ffi::sqlite3) {
    ffi::sqlite3_limit(db, ffi::SQLITE_LIMIT_LENGTH, LIMIT_LENGTH);
    ffi::sqlite3_limit(db, ffi::SQLITE_LIMIT_SQL_LENGTH, LIMIT_SQL_LENGTH);
    ffi::sqlite3_limit(
        db,
        ffi::SQLITE_LIMIT_LIKE_PATTERN_LENGTH,
        LIMIT_LIKE_PATTERN_LENGTH,
    );
}

unsafe extern "C" fn destroy(p: *mut c_void) {
    drop(Box::from_raw(p as *mut Guard));
}

/// Register `fathom_pragma_guard`. Installs NO authorizer by itself — see the module docs.
pub fn install(db: &Connection) -> Result<()> {
    let state = Box::into_raw(Box::new(Guard {
        policy: OnceLock::new(),
    }));
    let name = CString::new("fathom_pragma_guard").expect("static name");

    unsafe {
        // The function OWNS the state; SQLite calls `destroy` itself if registration fails.
        let rc = ffi::sqlite3_create_function_v2(
            db.handle(),
            name.as_ptr(),
            2,
            ffi::SQLITE_UTF8 | ffi::SQLITE_DIRECTONLY,
            state as *mut c_void,
            Some(install_guard),
            None,
            None,
            Some(destroy),
        );
        if rc != ffi::SQLITE_OK {
            return Err(Error::SqliteFailure(ffi::Error::new(rc), None));
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn policy() -> Policy {
        Policy::parse(
            "foreign_keys, legacy_alter_table,table_info,USER_VERSION",
            "synchronous=FULL,temp.max_page_count=1048576,cache_size=-2000",
        )
    }

    fn pragma(name: &str, value: Option<&str>, schema: Option<&str>) -> c_int {
        decide(
            ffi::SQLITE_PRAGMA,
            Some(name.as_bytes()),
            value.map(str::as_bytes),
            schema.map(str::as_bytes),
            &policy(),
        )
    }

    #[test]
    fn attach_and_detach_are_always_denied() {
        let p = policy();
        assert_eq!(
            decide(ffi::SQLITE_ATTACH, Some(b"/x.db"), None, None, &p),
            ffi::SQLITE_DENY
        );
        assert_eq!(
            decide(ffi::SQLITE_DETACH, Some(b"v"), None, None, &p),
            ffi::SQLITE_DENY
        );
    }

    #[test]
    fn other_actions_are_allowed() {
        let p = policy();
        assert_eq!(
            decide(ffi::SQLITE_READ, Some(b"t"), Some(b"c"), Some(b"main"), &p),
            ffi::SQLITE_OK
        );
        assert_eq!(
            decide(ffi::SQLITE_INSERT, Some(b"t"), None, Some(b"main"), &p),
            ffi::SQLITE_OK
        );
    }

    #[test]
    fn every_pragma_read_is_allowed() {
        assert_eq!(pragma("max_page_count", None, None), ffi::SQLITE_OK);
        assert_eq!(
            pragma("writable_schema", None, Some("main")),
            ffi::SQLITE_OK
        );
    }

    #[test]
    fn allowed_names_may_be_assigned_any_value_and_schema() {
        assert_eq!(pragma("foreign_keys", Some("OFF"), None), ffi::SQLITE_OK);
        assert_eq!(
            pragma("Foreign_Keys", Some("1"), Some("main")),
            ffi::SQLITE_OK
        );
        assert_eq!(pragma("user_version", Some("7"), None), ffi::SQLITE_OK);
        assert_eq!(pragma("table_info", Some("t"), None), ffi::SQLITE_OK);
    }

    #[test]
    fn unlisted_assignments_are_denied() {
        for (name, value) in [
            ("max_page_count", "999999999"),
            ("synchronous", "OFF"),
            ("journal_mode", "DELETE"),
            ("writable_schema", "ON"),
            ("hard_heap_limit", "1"),
            ("query_only", "OFF"),
        ] {
            assert_eq!(pragma(name, Some(value), None), ffi::SQLITE_DENY, "{name}");
        }
    }

    #[test]
    fn pinned_pairs_match_exact_value_and_schema_only() {
        assert_eq!(pragma("synchronous", Some("full"), None), ffi::SQLITE_OK);
        assert_eq!(
            pragma("synchronous", Some("FULL"), Some("main")),
            ffi::SQLITE_DENY
        );
        assert_eq!(
            pragma("synchronous", Some("NORMAL"), None),
            ffi::SQLITE_DENY
        );
        assert_eq!(pragma("cache_size", Some("-2000"), None), ffi::SQLITE_OK);
        assert_eq!(
            pragma("cache_size", Some("-2000000"), None),
            ffi::SQLITE_DENY
        );
        assert_eq!(
            pragma("max_page_count", Some("1048576"), Some("temp")),
            ffi::SQLITE_OK
        );
        // The TEMP cap's value must not be usable to move MAIN's cap.
        assert_eq!(
            pragma("max_page_count", Some("1048576"), None),
            ffi::SQLITE_DENY
        );
        assert_eq!(
            pragma("max_page_count", Some("1048576"), Some("main")),
            ffi::SQLITE_DENY
        );
    }

    #[test]
    fn a_missing_or_non_utf8_name_is_denied() {
        let p = policy();
        assert_eq!(
            decide(ffi::SQLITE_PRAGMA, None, Some(b"1"), None, &p),
            ffi::SQLITE_DENY
        );
        assert_eq!(
            decide(ffi::SQLITE_PRAGMA, Some(b"\xff"), Some(b"1"), None, &p),
            ffi::SQLITE_DENY
        );
    }

    #[test]
    fn an_empty_policy_denies_every_assignment() {
        let p = Policy::parse("", "");
        assert_eq!(
            decide(
                ffi::SQLITE_PRAGMA,
                Some(b"foreign_keys"),
                Some(b"ON"),
                None,
                &p
            ),
            ffi::SQLITE_DENY
        );
        assert_eq!(
            decide(ffi::SQLITE_PRAGMA, Some(b"foreign_keys"), None, None, &p),
            ffi::SQLITE_OK
        );
    }

    // The limits themselves are applied through the loadable-extension API, which is only
    // initialised when SQLite loads the .so, so they are proved end to end in
    // test/fathom/shard/connection_limits_test.exs. Here: the caps must stay below SQLite's
    // defaults, or the guard call would RAISE a limit (expert review 2026-10-10 #2).
    #[test]
    fn limits_are_below_sqlite_defaults() {
        assert!(LIMIT_LENGTH < 1_000_000_000);
        assert!(LIMIT_SQL_LENGTH < 1_000_000_000);
        assert!(LIMIT_LIKE_PATTERN_LENGTH <= 50_000);
    }
}
