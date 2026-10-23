//! Process-global registration of the UDFs as a SQLite AUTO-extension (expert review 2026-10-22
//! r8 #14, user decision Q1=A).
//!
//! ## Why
//!
//! Loading the extension per connection (`enable_load_extension` -> `SELECT load_extension(...)` ->
//! disable) costs five dirty-NIF dispatches plus a `dlopen`/`dlclose` of an image that is already
//! mapped, on EVERY `Fathom.Shard.Connection.open` (~45-50 us of a ~143 us open on macOS). Once
//! `auto_init` is registered with `sqlite3_auto_extension`, SQLite runs it inside every
//! `sqlite3_open` in the process instead: function registration only, no `dlopen`, no dispatch.
//!
//! ## What the auto init does NOT do
//!
//! It registers functions and per-connection state only. It installs NO hook or handler, because
//! it runs inside `sqlite3_open`, before anything that would overwrite them:
//!
//! * the WAL hook: `sqlite3_wal_hook` and `wal_autocheckpoint` are ONE slot (AGENTS.md loud
//!   warning), and SQLite arms its default autocheckpoint during open;
//! * the backstop's busy and progress handlers: exqlite installs its own right after
//!   `sqlite3_open` returns.
//!
//! `Fathom.Shard.Connection` installs all of them per open with `SELECT fathom_conn_init()` in its
//! open batch (lib.rs). A raw `Sqlite3.open` (a replica connection, a scratch `:memory:`) gets the
//! functions and nothing else.
//!
//! ## The security argument (why this is at least as strong as enable -> load -> disable)
//!
//! Before: every connection fathom opened, tenant ones included, had `sqlite3_enable_load_extension`
//! turned ON for the length of the load and OFF again, and the open failed if the disable failed.
//! The property that mattered was "no handle that ever runs tenant SQL has extension loading
//! enabled".
//!
//! Now: no `Connection.open` handle calls `sqlite3_enable_load_extension` at all, so the capability
//! is never turned on for it. A new SQLite connection starts with it OFF (SQLite's default; exqlite
//! does not compile `SQLITE_ENABLE_LOAD_EXTENSION` on by default for the SQL function), so the
//! property holds by construction rather than by a correctly ordered disable.
//!
//! The capability is now turned on in exactly one place: `Fathom.Shard.Extension`'s private
//! CONTROL connection, a `:memory:` handle it opens, loads with this file's
//! `sqlite3_fathomudf_control_init` entry point, uses for one or two statements, disables and
//! closes. It never runs tenant SQL and never leaves `Fathom.Shard.Extension`.
//!
//! The functions that change NODE-GLOBAL state (`fathom_auto_extension`, and since r8 #18 step 3
//! the pragma-guard policy setter `fathom_pragma_policy_set`) are registered ONLY by that control
//! entry point: never by `auto_init`, never by the per-connection `sqlite3_fathomudf_init`.
//! Reaching them requires `load_extension` with a chosen entry point, i.e. extension loading
//! enabled, which no tenant handle has. So a tenant cannot re-arm, disarm or re-point the
//! auto-extension, or widen the pragma policy, from SQL.
//!
//! What a tenant handle DOES get from `auto_init` is the same set of functions the per-open load
//! gave it, plus `fathom_conn_init()` (re-installs the same hook and handlers with the same state:
//! idempotent) and `fathom_pragma_guard_install` (set-once, like `fathom_pragma_guard`).
//! `test/fathom/shard/extension_test.exs` asserts from the tenant's side that `load_extension` is
//! refused and that the control functions do not exist.
//!
//! ## Pinning the library
//!
//! SQLite `dlclose`s a `load_extension`ed library when the connection that loaded it closes. The
//! auto-extension list holds a bare function pointer into this image, so once the control
//! connection closed, the next `sqlite3_open` would jump into an unmapped page. `pin_self` takes
//! one extra `dlopen` reference (`RTLD_NODELETE`, never closed) before registering, so the image
//! stays mapped for the life of the OS process.

use std::ffi::c_void;
use std::os::raw::{c_char, c_int};
use std::sync::OnceLock;

use rusqlite::functions::FunctionFlags;
use rusqlite::{ffi, Connection, Error, Result};

/// The auto-extension entry: SQLite calls it inside every `sqlite3_open` once registered.
///
/// Always returns `SQLITE_OK`: an error here would fail EVERY SQLite open in the BEAM, replica and
/// scratch connections included. A registration failure instead leaves functions missing, and
/// `Fathom.Shard.Connection`'s open batch then fails on `fathom_conn_init` / the pragma guard
/// ("no such function"), so a tenant handle can never come up without them (fail closed).
///
/// # Safety
///
/// Called by SQLite with a live handle and its API routine table.
unsafe extern "C" fn auto_init(
    db: *mut ffi::sqlite3,
    pz_err_msg: *mut *mut c_char,
    p_api: *const ffi::sqlite3_api_routines,
) -> c_int {
    let _ = Connection::extension_init2(
        db,
        pz_err_msg,
        p_api as *mut ffi::sqlite3_api_routines,
        crate::register_auto,
    );
    ffi::SQLITE_OK
}

fn entry() -> Option<unsafe extern "C" fn()> {
    // SAFETY: SQLite declares auto-extension entry points as `void (*)(void)` and calls them with
    // the `(db, pzErrMsg, pApi)` signature; this is the cast its own documentation prescribes.
    Some(unsafe {
        std::mem::transmute::<
            unsafe extern "C" fn(
                *mut ffi::sqlite3,
                *mut *mut c_char,
                *const ffi::sqlite3_api_routines,
            ) -> c_int,
            unsafe extern "C" fn(),
        >(auto_init)
    })
}

/// Take a never-released `dlopen` reference on this image (see the module docs). Once per process.
fn pin_self() -> bool {
    static PINNED: OnceLock<bool> = OnceLock::new();
    *PINNED.get_or_init(|| unsafe {
        let mut info: libc::Dl_info = std::mem::zeroed();
        if libc::dladdr(auto_init as *const c_void, &mut info) == 0 || info.dli_fname.is_null() {
            return false;
        }
        // Leaked on purpose: this reference is what keeps the image mapped.
        !libc::dlopen(info.dli_fname, libc::RTLD_NOW | libc::RTLD_NODELETE).is_null()
    })
}

/// `fathom_auto_extension(on)`: 1 registers `auto_init` process-wide (pinning the image first),
/// 0 cancels it. Returns 1 on success. Registered only by the control entry point.
fn auto_extension(on: i64) -> Result<i64> {
    unsafe {
        match on {
            1 => {
                if !pin_self() {
                    return Err(Error::UserFunctionError(
                        "fathom_auto_extension: could not pin the extension image".into(),
                    ));
                }
                // Idempotent: SQLite ignores a second registration of the same pointer.
                let rc = ffi::sqlite3_auto_extension(entry());
                if rc != ffi::SQLITE_OK {
                    return Err(Error::SqliteFailure(ffi::Error::new(rc), None));
                }
                Ok(1)
            }
            0 => Ok(i64::from(ffi::sqlite3_cancel_auto_extension(entry()))),
            _ => Err(Error::UserFunctionError(
                "fathom_auto_extension: expected 0 or 1".into(),
            )),
        }
    }
}

/// The CONTROL entry point (`load_extension(path, 'sqlite3_fathomudf_control_init')`): registers
/// the node-global control functions on this one connection, and nothing else.
///
/// # Safety
///
/// Called by SQLite through `dlopen`/`dlsym` with a live handle and the API table.
#[no_mangle]
pub unsafe extern "C" fn sqlite3_fathomudf_control_init(
    db: *mut ffi::sqlite3,
    pz_err_msg: *mut *mut c_char,
    p_api: *mut ffi::sqlite3_api_routines,
) -> c_int {
    Connection::extension_init2(db, pz_err_msg, p_api, register_control)
}

fn register_control(db: Connection) -> Result<bool> {
    // DIRECTONLY: never from a view, trigger or schema object.
    let flags = FunctionFlags::SQLITE_UTF8 | FunctionFlags::SQLITE_DIRECTONLY;
    db.create_scalar_function("fathom_auto_extension", 1, flags, |ctx| {
        auto_extension(ctx.get::<i64>(0)?)
    })?;
    // The node-global pragma-guard policy setter (r8 #18 step 3): control connections ONLY.
    crate::pragma_guard::register_control(&db)?;
    Ok(false)
}
