//! A statement deadline enforced on the QUERY'S OWN THREAD (2026-10-04).
//!
//! `:query_timeout_ms` is enforced by a BEAM watchdog process that cancels the connection when a
//! timer fires. Measured 2026-10-03: after the VM had been idle, no normal-scheduler timer fired
//! while one long query ran in a dirty NIF. The watchdog's 5 ms deadline fired at ~5,550 ms, as did
//! an unrelated `Process.sleep(1000)`, all at the moment the NIF returned. So a heavy query could
//! run with no deadline at all. The BEAM side cannot fix a timer that never fires.
//!
//! SQLite can. Its progress handler runs every N VDBE instructions on the thread executing the
//! statement, which is exactly the thread that is guaranteed to be running. This module:
//!
//!   * records when each statement starts (`SQLITE_TRACE_STMT`),
//!   * stops it from the progress handler once it has run longer than the connection's timeout.
//!
//! It is a BACKSTOP: the watchdog stays primary and still bounds lock waits (busy handler), which
//! this does not. `Fathom.Shard.Connection` classifies an interrupt past the deadline as
//! `:query_timeout` whichever side fired.
//!
//! ## Slots taken
//!
//! `sqlite3_progress_handler` and `sqlite3_trace_v2` each hold ONE callback per connection. The
//! trace slot was unused. The progress slot was exqlite's, which returned its `cancelled` flag;
//! `Exqlite.Sqlite3.cancel/1` also calls `sqlite3_interrupt`, which stops the VDBE without the
//! handler, so cancellation keeps working. Calling exqlite's `set_progress_handler_steps/2` would
//! put exqlite's handler back and silently remove this one; fathom never calls it.
//!
//! ## Set once, by fathom
//!
//! `fathom_backstop(ms)` sets the timeout. Only the FIRST positive value is accepted: fathom calls it
//! while opening the connection, before any tenant SQL, so a tenant's own `fathom_backstop(0)`
//! cannot switch its deadline off. exqlite's authorizer denies by action, not by function name, so
//! "first call wins" is the guard. The function is `SQLITE_DIRECTONLY` (not callable from schema
//! objects) and owns the per-connection state: its destructor frees it when the connection closes.

use std::ffi::{c_void, CString};
use std::os::raw::{c_char, c_int, c_uint};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::OnceLock;
use std::time::Instant;

use rusqlite::{ffi, Connection, Error, Result};

/// VDBE instructions between deadline checks. exqlite used 1000 for its own handler; the check is
/// one atomic load and one clock read, so this costs well under a microsecond per thousand ops.
const CHECK_EVERY_OPS: c_int = 1000;

struct Backstop {
    /// 0 = no deadline (not yet set, or set to 0).
    timeout_ns: AtomicU64,
    /// When the current statement started, as `now_ns()`. 0 = no statement seen yet.
    start_ns: AtomicU64,
}

fn now_ns() -> u64 {
    static BASE: OnceLock<Instant> = OnceLock::new();
    // +1 so a reading taken at the very first instant is never 0, which means "unset" above.
    BASE.get_or_init(Instant::now).elapsed().as_nanos() as u64 + 1
}

/// The pure decision, separate so it can be unit-tested without SQLite.
pub fn should_interrupt(timeout_ns: u64, start_ns: u64, now_ns: u64) -> bool {
    timeout_ns > 0 && start_ns > 0 && now_ns.saturating_sub(start_ns) > timeout_ns
}

/// The trace text SQLite passes for a TRIGGER subprogram starts with "--". Those fire mid-statement
/// and must not restart the clock, or a statement firing triggers would never time out.
pub fn is_trigger_trace(sql: &[u8]) -> bool {
    sql.starts_with(b"--")
}

unsafe extern "C" fn on_progress(ctx: *mut c_void) -> c_int {
    let state = &*(ctx as *const Backstop);
    let timeout = state.timeout_ns.load(Ordering::Relaxed);
    if timeout == 0 {
        return 0;
    }
    c_int::from(should_interrupt(
        timeout,
        state.start_ns.load(Ordering::Relaxed),
        now_ns(),
    ))
}

unsafe extern "C" fn on_trace(
    mask: c_uint,
    ctx: *mut c_void,
    _p: *mut c_void,
    x: *mut c_void,
) -> c_int {
    if mask == ffi::SQLITE_TRACE_STMT as c_uint {
        let state = &*(ctx as *const Backstop);
        let is_trigger = !x.is_null()
            && is_trigger_trace(std::ffi::CStr::from_ptr(x as *const c_char).to_bytes());
        if !is_trigger {
            state.start_ns.store(now_ns(), Ordering::Relaxed);
        }
    }
    0
}

unsafe extern "C" fn set_timeout(
    ctx: *mut ffi::sqlite3_context,
    argc: c_int,
    argv: *mut *mut ffi::sqlite3_value,
) {
    let state = &*(ffi::sqlite3_user_data(ctx) as *const Backstop);
    if argc == 1 {
        let ms = ffi::sqlite3_value_int64(*argv);
        if ms > 0 {
            // First positive value wins; see the module docs.
            let _ = state.timeout_ns.compare_exchange(
                0,
                (ms as u64).saturating_mul(1_000_000),
                Ordering::Relaxed,
                Ordering::Relaxed,
            );
        }
    }
    let ms = state.timeout_ns.load(Ordering::Relaxed) / 1_000_000;
    ffi::sqlite3_result_int64(ctx, ms as i64);
}

unsafe extern "C" fn destroy(p: *mut c_void) {
    drop(Box::from_raw(p as *mut Backstop));
}

/// Install the hooks and the setter on this connection.
pub fn install(db: &Connection) -> Result<()> {
    let state = Box::into_raw(Box::new(Backstop {
        timeout_ns: AtomicU64::new(0),
        start_ns: AtomicU64::new(0),
    }));
    let name = CString::new("fathom_backstop").expect("static name");

    unsafe {
        let handle = db.handle();

        // The function OWNS the state (destroy runs at close, after the last callback can fire),
        // so register it first: if that fails, nothing else references the state yet.
        let rc = ffi::sqlite3_create_function_v2(
            handle,
            name.as_ptr(),
            1,
            ffi::SQLITE_UTF8 | ffi::SQLITE_DIRECTONLY,
            state as *mut c_void,
            Some(set_timeout),
            None,
            None,
            Some(destroy),
        );
        if rc != ffi::SQLITE_OK {
            // SQLite calls the destructor itself when registration fails.
            return Err(Error::SqliteFailure(ffi::Error::new(rc), None));
        }

        ffi::sqlite3_progress_handler(
            handle,
            CHECK_EVERY_OPS,
            Some(on_progress),
            state as *mut c_void,
        );

        let rc = ffi::sqlite3_trace_v2(
            handle,
            ffi::SQLITE_TRACE_STMT as c_uint,
            Some(on_trace),
            state as *mut c_void,
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

    #[test]
    fn no_deadline_never_interrupts() {
        assert!(!should_interrupt(0, 1, u64::MAX));
    }

    #[test]
    fn no_statement_seen_never_interrupts() {
        assert!(!should_interrupt(5, 0, u64::MAX));
    }

    #[test]
    fn interrupts_only_past_the_deadline() {
        assert!(!should_interrupt(100, 1_000, 1_100));
        assert!(should_interrupt(100, 1_000, 1_101));
    }

    #[test]
    fn a_clock_reading_before_start_does_not_underflow() {
        assert!(!should_interrupt(100, 1_000, 900));
    }

    #[test]
    fn trigger_traces_are_recognized() {
        assert!(is_trigger_trace(b"-- TRIGGER t_after_insert"));
        assert!(!is_trigger_trace(b"SELECT 1"));
    }
}
