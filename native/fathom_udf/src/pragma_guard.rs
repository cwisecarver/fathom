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
//! ## The node-global policy (expert review 2026-10-22 r8 #18 step 3, user decision Q2=A)
//!
//! The policy is the same for every tenant handle on the node apart from two booleans, yet each
//! open used to rebuild a ~510-byte list in Elixir, bind it, and parse it here into two fresh
//! `HashSet<String>`s (~40 allocations, ~2-3 KB of system-allocator heap per connection, invisible
//! to the BEAM's memory accounting). Now `Fathom.Shard.Connection` pushes each of the four variants
//! (bit 0 = `user_version` blocked, bit 1 = deferred sync) ONCE, at first use and whenever its
//! inputs change, with `fathom_pragma_policy_set(variant, names, pinned)`; a tenant open installs
//! the guard with `fathom_pragma_guard_install(variant)`, which shares the variant's `Arc<Policy>`.
//!
//! NODE-GLOBAL MUTABLE STATE, so the setter is the dangerous one: a tenant able to call it would
//! widen the policy of every later tenant handle on the node. It is registered ONLY by the control
//! entry point (src/auto.rs, `sqlite3_fathomudf_control_init`), which needs `load_extension`, which
//! no tenant handle has; `extension_test.exs` asserts a tenant gets "no such function". The install
//! is set-once per connection exactly like `fathom_pragma_guard`, and FAILS (the open fails) when
//! no policy was pushed for the variant: fail closed, never an empty or default policy. A connection
//! keeps the `Arc` it installed with, so a later push changes only handles opened after it, the same
//! "read at open" rule the per-open parse had.
//!
//! ## Cost
//!
//! An authorizer runs at PREPARE time, once per action the statement contains (each column read,
//! each table, ...). For every action but a PRAGMA or a function reference this is one integer
//! comparison; a function reference adds one name comparison (`replication_denies`).

use std::collections::HashSet;
use std::ffi::{c_void, CStr, CString};
use std::os::raw::{c_char, c_int};
use std::sync::atomic::{AtomicPtr, Ordering};
use std::sync::{Arc, OnceLock, RwLock};

use rusqlite::functions::FunctionFlags;

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
        ffi::SQLITE_PRAGMA if !wal_checkpoint_mode_allowed(name, value) => ffi::SQLITE_DENY,
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

/// `wal_checkpoint` is allow-listed by NAME, but its argument is the mode: TRUNCATE/RESTART/FULL
/// block on (or wait for) other connections' readers and writers, so a tenant looping them stalls
/// every co-resident writer (expert review 2026-10-10 #33b). Only a bare `PRAGMA wal_checkpoint`
/// (value NULL) or PASSIVE passes. Probed 2026-10-10: `(TRUNCATE)`, `= truncate`, `main.` and quoted
/// spellings all reach the authorizer as name `wal_checkpoint` with the dequoted mode as arg 4.
/// Any other pragma is not this function's business.
fn wal_checkpoint_mode_allowed(name: Option<&[u8]>, value: Option<&[u8]>) -> bool {
    match (name, value) {
        (Some(n), Some(mode)) if n.eq_ignore_ascii_case(b"wal_checkpoint") => {
            mode.eq_ignore_ascii_case(b"passive")
        }
        _ => true,
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

/// The tenant-handle refusals that exist for A2 (expert review 2026-10-15 #1), as a pure decision.
///
/// * The registration function `fathom_wal_replicated` (see `wal.rs`): a tenant that could
///   unregister its own file would hand its WAL back to the unpinned 4000-frame hook checkpoint.
/// * ANY `wal_checkpoint` — PASSIVE and the bare form included, which both backfill — on a file
///   under replication. A tenant checkpoint is exactly the unpinned checkpoint that erases frames
///   the Session has not shipped; on a non-replicated file the existing PASSIVE-only rule stands.
pub fn replication_denies(
    action: c_int,
    name: Option<&[u8]>,
    func: Option<&[u8]>,
    replicated: impl FnOnce() -> bool,
) -> bool {
    match action {
        ffi::SQLITE_FUNCTION => {
            matches!(func, Some(f) if f.eq_ignore_ascii_case(crate::wal::REGISTER_FN.as_bytes()))
        }
        ffi::SQLITE_PRAGMA => {
            matches!(name, Some(n) if n.eq_ignore_ascii_case(b"wal_checkpoint")) && replicated()
        }
        _ => false,
    }
}

/// The four node-global policy variants (see the module docs). `None` = never pushed.
static POLICIES: RwLock<[Option<Arc<Policy>>; 4]> = RwLock::new([None, None, None, None]);

/// Store `policy` as variant `variant` (0..=3). Pure bookkeeping, unit-tested.
pub fn set_policy(variant: i64, policy: Policy) -> std::result::Result<(), &'static str> {
    let i = usize::try_from(variant)
        .ok()
        .filter(|i| *i < 4)
        .ok_or("variant must be 0..=3")?;
    let mut table = POLICIES.write().map_err(|_| "policy table poisoned")?;
    table[i] = Some(Arc::new(policy));
    Ok(())
}

/// The current policy of `variant`, if one was pushed.
pub fn variant_policy(variant: i64) -> Option<Arc<Policy>> {
    let i = usize::try_from(variant).ok().filter(|i| *i < 4)?;
    POLICIES.read().ok()?[i].clone()
}

struct Guard {
    policy: OnceLock<Arc<Policy>>,
    /// The connection this guard was installed on, for the replicated-file check. Set before the
    /// authorizer is installed, so `authorize` never sees it null.
    db: AtomicPtr<ffi::sqlite3>,
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
    // A function reference is checked for ONE name (one byte comparison per function in the
    // statement, at prepare time); see `replication_denies`.
    if action == ffi::SQLITE_FUNCTION {
        return if replication_denies(action, None, opt_bytes(arg4), || false) {
            ffi::SQLITE_DENY
        } else {
            ffi::SQLITE_OK
        };
    }
    // Every action but these three is allowed without touching the state.
    if action != ffi::SQLITE_PRAGMA && action != ffi::SQLITE_ATTACH && action != ffi::SQLITE_DETACH
    {
        return ffi::SQLITE_OK;
    }
    let guard = &*(ctx as *const Guard);
    if replication_denies(action, opt_bytes(arg3), None, || {
        let db = guard.db.load(Ordering::Acquire);
        !db.is_null() && crate::wal::db_is_replicated(db, c"main".as_ptr())
    }) {
        return ffi::SQLITE_DENY;
    }
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
    activate(ctx, guard_ptr, Arc::new(Policy::parse(&names, &pinned)));
}

/// `fathom_pragma_guard_install(variant)` (r8 #18 step 3): install the authorizer with the
/// node-global policy `variant`. Same contract as `fathom_pragma_guard`: 1 when this call installed
/// it, 0 when one already was; an error when no policy was pushed for the variant.
unsafe extern "C" fn install_guard_variant(
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
    if argc != 1 || ffi::sqlite3_value_type(*argv) != ffi::SQLITE_INTEGER {
        result_error(ctx, "fathom_pragma_guard_install: expected one integer variant");
        return;
    }
    match variant_policy(ffi::sqlite3_value_int64(*argv)) {
        Some(p) => activate(ctx, guard_ptr, p),
        None => result_error(ctx, "fathom_pragma_guard_install: no policy pushed for this variant"),
    }
}

/// Set the guard's policy (set-once), install the authorizer and the tenant limits.
unsafe fn activate(ctx: *mut ffi::sqlite3_context, guard_ptr: *mut c_void, policy: Arc<Policy>) {
    let guard = &*(guard_ptr as *const Guard);
    if guard.policy.set(policy).is_err() {
        ffi::sqlite3_result_int64(ctx, 0);
        return;
    }

    let db = ffi::sqlite3_context_db_handle(ctx);
    guard.db.store(db, Ordering::Release);
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
        db: AtomicPtr::new(std::ptr::null_mut()),
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

        // Shares the state; `fathom_pragma_guard`'s destructor frees it, so no destructor here.
        let name = CString::new("fathom_pragma_guard_install").expect("static name");
        let rc = ffi::sqlite3_create_function_v2(
            db.handle(),
            name.as_ptr(),
            1,
            ffi::SQLITE_UTF8 | ffi::SQLITE_DIRECTONLY,
            state as *mut c_void,
            Some(install_guard_variant),
            None,
            None,
            None,
        );
        if rc != ffi::SQLITE_OK {
            return Err(Error::SqliteFailure(ffi::Error::new(rc), None));
        }
    }
    Ok(())
}

/// Register `fathom_pragma_policy_set(variant, names, pinned)` on a CONTROL connection only
/// (src/auto.rs). Returns 1. See the module docs on why it must never reach a tenant handle.
pub fn register_control(db: &Connection) -> Result<()> {
    let flags = FunctionFlags::SQLITE_UTF8 | FunctionFlags::SQLITE_DIRECTONLY;
    db.create_scalar_function("fathom_pragma_policy_set", 3, flags, |ctx| {
        let variant: i64 = ctx.get(0)?;
        let names: String = ctx.get(1)?;
        let pinned: String = ctx.get(2)?;
        set_policy(variant, Policy::parse(&names, &pinned))
            .map(|()| 1i64)
            .map_err(|e| Error::UserFunctionError(e.into()))
    })
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

    // Expert review 2026-10-15 #1: on a REPLICATED file a tenant checkpoint is the unpinned
    // checkpoint that erases unshipped frames, so every form is refused there — PASSIVE and the
    // bare read included, since both backfill. A non-replicated file keeps the PASSIVE-only rule.
    #[test]
    fn a_replicated_file_refuses_every_tenant_checkpoint() {
        let name = Some(&b"wal_checkpoint"[..]);
        assert!(replication_denies(ffi::SQLITE_PRAGMA, name, None, || true));
        assert!(replication_denies(
            ffi::SQLITE_PRAGMA,
            Some(&b"WAL_CHECKPOINT"[..]),
            None,
            || true
        ));
        assert!(!replication_denies(ffi::SQLITE_PRAGMA, name, None, || {
            false
        }));
        // Other pragmas are not this rule's business, replicated or not.
        assert!(!replication_denies(
            ffi::SQLITE_PRAGMA,
            Some(&b"foreign_keys"[..]),
            None,
            || true
        ));
    }

    #[test]
    fn the_replication_registration_function_is_denied_to_tenants() {
        let f = Some(&b"fathom_wal_replicated"[..]);
        assert!(replication_denies(ffi::SQLITE_FUNCTION, None, f, || false));
        assert!(replication_denies(
            ffi::SQLITE_FUNCTION,
            None,
            Some(&b"FATHOM_WAL_REPLICATED"[..]),
            || false
        ));
        assert!(!replication_denies(
            ffi::SQLITE_FUNCTION,
            None,
            Some(&b"django_date_extract"[..]),
            || true
        ));
    }

    #[test]
    fn wal_checkpoint_only_passive_or_bare() {
        let p = Policy::parse("wal_checkpoint", "");
        let run = |name: &[u8], value: Option<&[u8]>, schema: Option<&[u8]>| {
            decide(ffi::SQLITE_PRAGMA, Some(name), value, schema, &p)
        };
        assert_eq!(run(b"wal_checkpoint", None, None), ffi::SQLITE_OK);
        assert_eq!(run(b"wal_checkpoint", None, Some(b"main")), ffi::SQLITE_OK);
        assert_eq!(
            run(b"wal_checkpoint", Some(b"PASSIVE"), None),
            ffi::SQLITE_OK
        );
        assert_eq!(
            run(b"WAL_Checkpoint", Some(b"passive"), Some(b"main")),
            ffi::SQLITE_OK
        );
        for mode in [
            &b"TRUNCATE"[..],
            b"restart",
            b"Full",
            b"noop",
            b"",
            b"bogus",
            b"passive ",
        ] {
            assert_eq!(
                run(b"wal_checkpoint", Some(mode), None),
                ffi::SQLITE_DENY,
                "{mode:?}"
            );
            assert_eq!(
                run(b"WAL_CHECKPOINT", Some(mode), Some(b"main")),
                ffi::SQLITE_DENY
            );
        }
        // Another pragma named with the word "passive" is unaffected.
        assert_eq!(
            pragma("foreign_keys", Some("passive"), None),
            ffi::SQLITE_OK
        );
        // A policy that never allowed wal_checkpoint still refuses even PASSIVE assignments.
        assert_eq!(
            pragma("wal_checkpoint", Some("passive"), None),
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
    fn the_node_global_policy_table_holds_four_variants_and_refuses_others() {
        assert!(set_policy(4, Policy::default()).is_err());
        assert!(set_policy(-1, Policy::default()).is_err());
        assert!(variant_policy(4).is_none());
        // Variant 3 is used by no other test here.
        set_policy(3, Policy::parse("foreign_keys", "synchronous=NORMAL")).unwrap();
        let p = variant_policy(3).expect("pushed");
        assert_eq!(
            decide(ffi::SQLITE_PRAGMA, Some(b"foreign_keys"), Some(b"OFF"), None, &p),
            ffi::SQLITE_OK
        );
        assert_eq!(
            decide(ffi::SQLITE_PRAGMA, Some(b"synchronous"), Some(b"FULL"), None, &p),
            ffi::SQLITE_DENY
        );
        // A later push replaces the variant for LATER installs; an Arc already taken is unchanged.
        set_policy(3, Policy::parse("", "")).unwrap();
        assert_eq!(
            decide(ffi::SQLITE_PRAGMA, Some(b"foreign_keys"), Some(b"OFF"), None, &p),
            ffi::SQLITE_OK
        );
        let q = variant_policy(3).unwrap();
        assert_eq!(
            decide(ffi::SQLITE_PRAGMA, Some(b"foreign_keys"), Some(b"OFF"), None, &q),
            ffi::SQLITE_DENY
        );
    }

    #[test]
    fn limits_are_below_sqlite_defaults() {
        assert!(LIMIT_LENGTH < 1_000_000_000);
        assert!(LIMIT_SQL_LENGTH < 1_000_000_000);
        assert!(LIMIT_LIKE_PATTERN_LENGTH <= 50_000);
    }
}
