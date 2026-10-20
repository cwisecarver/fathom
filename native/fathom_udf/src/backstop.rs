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
//! It is a BACKSTOP: the watchdog stays primary. `Fathom.Shard.Connection` classifies an interrupt
//! (or a lock-wait `SQLITE_BUSY`) past the deadline as `:query_timeout` whichever side fired.
//!
//! ## Lock waits (2026-10-04)
//!
//! The progress handler does not run while a statement waits for another connection's lock;
//! SQLite calls the BUSY handler instead. CI showed the same stall there: `query_bounds_test:89`, a
//! write blocked behind another stream's lock under a 200 ms deadline, returned `SQLITE_BUSY` after
//! 5,012 ms — the full busy timeout, i.e. the watchdog never fired. So this module also installs
//! the busy handler, replacing exqlite's. It waits on SQLite's own delay ladder and gives up at the
//! deadline or at the busy timeout, whichever comes first.
//!
//! What exqlite's handler did that this cannot: stop early on `Exqlite.Sqlite3.cancel/1` and when
//! the calling process has died. Both are visible only through exqlite's private flag, or through
//! `sqlite3_is_interrupted`, which these 3.34 bindings predate. The watchdog's cancel fires AT the
//! deadline this handler already enforces, so a timeout behaves the same; a dead caller's wait now
//! ends at the deadline or the busy timeout rather than at the next delay step.
//!
//! The busy timeout lives here too (`fathom_busy_timeout(ms)`, default 5,000 ms, matching
//! `Fathom.Shard.Connection`). Exqlite's `set_busy_timeout/2` sets only exqlite's own field, so
//! `Connection.set_busy_timeout/2` calls both. A tenant can only LOWER it, never raise it.
//!
//! ## Yielding lock waits to the BEAM (expert review 2026-10-20 #2)
//!
//! Every exqlite NIF that steps a statement is `ERL_NIF_DIRTY_JOB_IO_BOUND`, so a lock wait spent
//! sleeping in this handler holds one of the node's `+SDio` (10) dirty-IO threads. Measured: with 11
//! tenant writers waiting on ONE shard's write lock, an unrelated shard's `SELECT` took ~2.1 s and
//! a plain `File.read!` ~2.1 s — every SQLite op and file I/O on the node queued behind sleepers,
//! including the lock holder's own COMMIT (a convoy).
//!
//! `fathom_busy_yield(1)` (set by `Fathom.Shard.Connection` on TENANT handles) makes this handler
//! give up AT ONCE, so the statement returns `SQLITE_BUSY` and the owning BEAM process retries it
//! from its own mailbox wait (`receive after`), holding no thread.
//!
//! Re-running the statement is valid exactly when THIS HANDLER was what gave up, because SQLite's
//! btree calls the busy handler only while the connection holds no transaction on the database
//! (`sqlite3BtreeBeginTrans` retries only while `inTransaction == TRANS_NONE`): no read snapshot to
//! go stale, no lock, and SQLite rolled back whatever the statement began. That covers autocommit,
//! `BEGIN IMMEDIATE`/`EXCLUSIVE` themselves, and an explicit transaction's FIRST statement (Django's
//! `transaction.atomic`: `BEGIN` then a write). A transaction that already READ and then fails to
//! upgrade to a writer (or `SQLITE_BUSY_SNAPSHOT`) returns BUSY WITHOUT calling the handler, and
//! re-stepping that can never succeed. exqlite reports both as the same `:busy` (no extended result
//! codes), so the handler records WHICH statement it yielded (`yielded_seq`, against `stmt_seq`
//! counted by the trace hook), and `fathom_busy_retry_ms()` grants a retry budget only when the
//! statement just before it was the one that yielded.
//!
//! Unlike `fathom_backstop`, the flag can be turned back OFF: the tenant `sequence` (script) path
//! runs several autocommit statements in one `sqlite3_exec`, where a mid-script retry is NOT valid,
//! so fathom switches yielding off around it. A tenant can do the same for its own statements, which
//! lets its lock waits hold a dirty-IO thread for up to the busy timeout (capped by its deadline).
//! That is deliberate self-harm with no more reach than a tenant already has: any heavy query holds
//! a dirty thread for up to the statement deadline. The yield protects tenants from ordinary
//! contention, not from a hostile tenant.
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
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::OnceLock;
use std::time::Instant;

use rusqlite::{ffi, Connection, Error, Result};

/// VDBE instructions between deadline checks. exqlite used 1000 for its own handler; the check is
/// one atomic load and one clock read, so this costs well under a microsecond per thousand ops.
const CHECK_EVERY_OPS: c_int = 1000;

/// Must match the busy timeout `Fathom.Shard.Connection` sets at open.
const DEFAULT_BUSY_TIMEOUT_MS: u64 = 5_000;

/// SQLite's own busy-wait ladder (`sqliteDefaultBusyCallback`), in ms, then 100 ms per retry.
const DELAYS_MS: [u64; 12] = [1, 2, 5, 10, 15, 20, 25, 25, 25, 50, 50, 100];

struct Backstop {
    /// 0 = no deadline (not yet set, or set to 0).
    timeout_ns: AtomicU64,
    /// When the current statement started, as `now_ns()`. 0 = no statement seen yet.
    start_ns: AtomicU64,
    /// How long one lock wait may last, in ms.
    busy_timeout_ms: AtomicU64,
    /// Give up a lock wait at once (see "Yielding lock waits to the BEAM").
    yield_on: AtomicBool,
    /// Top-level statements started on this connection (the trace hook; triggers excluded).
    stmt_seq: AtomicU64,
    /// `stmt_seq` of the statement the busy handler last gave up for. 0 = none.
    yielded_seq: AtomicU64,
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

/// Total ms already slept before busy-handler call number `count` (0-based).
pub fn waited_ms(count: u64) -> u64 {
    let n = DELAYS_MS.len() as u64;
    if count <= n {
        DELAYS_MS[..count as usize].iter().sum()
    } else {
        DELAYS_MS.iter().sum::<u64>() + (count - n) * DELAYS_MS[DELAYS_MS.len() - 1]
    }
}

/// What busy-handler call number `count` does: `None` = give up (SQLITE_BUSY), `Some(ms)` = sleep
/// that long and retry. Pure, for the unit tests.
pub fn busy_step(
    count: u64,
    busy_timeout_ms: u64,
    timeout_ns: u64,
    start_ns: u64,
    now_ns: u64,
) -> Option<u64> {
    if should_interrupt(timeout_ns, start_ns, now_ns) {
        return None;
    }
    let waited = waited_ms(count);
    if waited >= busy_timeout_ms {
        return None;
    }
    let step = DELAYS_MS[(count as usize).min(DELAYS_MS.len() - 1)];
    let mut sleep = step.min(busy_timeout_ms - waited);
    if timeout_ns > 0 && start_ns > 0 {
        let left_ms = timeout_ns.saturating_sub(now_ns.saturating_sub(start_ns)) / 1_000_000;
        sleep = sleep.min(left_ms.max(1));
    }
    Some(sleep)
}

/// Whether the statement running NOW (`current_seq`, the retry-budget probe) directly follows the
/// one the busy handler yielded for. Pure, for the unit tests.
pub fn retry_allowed(yield_on: bool, yielded_seq: u64, current_seq: u64) -> bool {
    yield_on && yielded_seq != 0 && yielded_seq.wrapping_add(1) == current_seq
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

// ROLLBACK-JOURNAL COMMIT (round-4 durability review 2026-10-20 F3, claim disproven). The review
// said that a COMMIT needing EXCLUSIVE reaches this handler with autoCommit already 1, so yielding
// returns an instant BUSY that nothing retries (`fathom_busy_retry_ms` -> 0), and prescribed
// "yield only when sqlite3_txn_state < WRITE". Measured: the retry DOES happen.
// `fathom_busy_retry_ms()` checks only that the previous statement is the one that yielded (it is
// not consulted for autocommit state), so the COMMIT is re-stepped from the BEAM, succeeds when
// the readers leave, and the transaction ends normally (pinned by busy_yield_test "a
// rollback-journal COMMIT waits for readers..."). Re-stepping COMMIT is valid: a BUSY COMMIT leaves
// the transaction open. The prescribed fix would make this wait happen INSIDE the handler again,
// holding a dirty-IO thread: it would undo #2 for this case. Do not add the txn_state check.
unsafe extern "C" fn on_busy(ctx: *mut c_void, count: c_int) -> c_int {
    let state = &*(ctx as *const Backstop);
    if state.yield_on.load(Ordering::Relaxed) {
        state
            .yielded_seq
            .store(state.stmt_seq.load(Ordering::Relaxed), Ordering::Relaxed);
        return 0;
    }
    match busy_step(
        count.max(0) as u64,
        state.busy_timeout_ms.load(Ordering::Relaxed),
        state.timeout_ns.load(Ordering::Relaxed),
        state.start_ns.load(Ordering::Relaxed),
        now_ns(),
    ) {
        None => 0,
        Some(ms) => {
            ffi::sqlite3_sleep(ms.min(c_int::MAX as u64) as c_int);
            1
        }
    }
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
            state.stmt_seq.fetch_add(1, Ordering::Relaxed);
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

/// `fathom_busy_timeout(ms)`: lower this connection's lock-wait limit. Never raises it, so a tenant
/// cannot make its own lock waits longer. Returns the limit in force.
unsafe extern "C" fn set_busy_timeout(
    ctx: *mut ffi::sqlite3_context,
    argc: c_int,
    argv: *mut *mut ffi::sqlite3_value,
) {
    let state = &*(ffi::sqlite3_user_data(ctx) as *const Backstop);
    if argc == 1 {
        let ms = ffi::sqlite3_value_int64(*argv);
        if ms >= 0 {
            state
                .busy_timeout_ms
                .fetch_min(ms as u64, Ordering::Relaxed);
        }
    }
    ffi::sqlite3_result_int64(ctx, state.busy_timeout_ms.load(Ordering::Relaxed) as i64);
}

/// `fathom_busy_yield(on)`: 1 = give up lock waits at once (the BEAM retries), 0 = wait
/// in the handler. Returns the setting in force (1/0). Any other argument only reads it.
unsafe extern "C" fn set_busy_yield(
    ctx: *mut ffi::sqlite3_context,
    argc: c_int,
    argv: *mut *mut ffi::sqlite3_value,
) {
    let state = &*(ffi::sqlite3_user_data(ctx) as *const Backstop);
    if argc == 1 {
        match ffi::sqlite3_value_int64(*argv) {
            0 => state.yield_on.store(false, Ordering::Relaxed),
            1 => state.yield_on.store(true, Ordering::Relaxed),
            _ => {}
        }
    }
    ffi::sqlite3_result_int64(
        ctx,
        i64::from(state.yield_on.load(Ordering::Relaxed)),
    );
}

/// `fathom_busy_retry_ms()`: how long the BEAM may keep retrying a statement that just returned
/// `SQLITE_BUSY` — the busy timeout when this handler yielded for the statement just before this
/// one, else 0 (do not retry: the handler already waited, or the BUSY never reached the handler and
/// re-stepping is not valid). Call it as the very next statement after the BUSY.
unsafe extern "C" fn busy_retry_ms(
    ctx: *mut ffi::sqlite3_context,
    _argc: c_int,
    _argv: *mut *mut ffi::sqlite3_value,
) {
    let state = &*(ffi::sqlite3_user_data(ctx) as *const Backstop);
    let ms = if retry_allowed(
        state.yield_on.load(Ordering::Relaxed),
        state.yielded_seq.load(Ordering::Relaxed),
        state.stmt_seq.load(Ordering::Relaxed),
    ) {
        state.busy_timeout_ms.load(Ordering::Relaxed)
    } else {
        0
    };
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
        busy_timeout_ms: AtomicU64::new(DEFAULT_BUSY_TIMEOUT_MS),
        yield_on: AtomicBool::new(false),
        stmt_seq: AtomicU64::new(0),
        yielded_seq: AtomicU64::new(0),
    }));
    let name = CString::new("fathom_backstop").expect("static name");
    let busy_name = CString::new("fathom_busy_timeout").expect("static name");
    let yield_name = CString::new("fathom_busy_yield").expect("static name");
    let retry_name = CString::new("fathom_busy_retry_ms").expect("static name");

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

        // Shares the state; `fathom_backstop`'s destructor frees it, so no destructor here.
        let rc = ffi::sqlite3_create_function_v2(
            handle,
            busy_name.as_ptr(),
            1,
            ffi::SQLITE_UTF8 | ffi::SQLITE_DIRECTONLY,
            state as *mut c_void,
            Some(set_busy_timeout),
            None,
            None,
            None,
        );
        if rc != ffi::SQLITE_OK {
            return Err(Error::SqliteFailure(ffi::Error::new(rc), None));
        }

        let rc = ffi::sqlite3_create_function_v2(
            handle,
            yield_name.as_ptr(),
            1,
            ffi::SQLITE_UTF8 | ffi::SQLITE_DIRECTONLY,
            state as *mut c_void,
            Some(set_busy_yield),
            None,
            None,
            None,
        );
        if rc != ffi::SQLITE_OK {
            return Err(Error::SqliteFailure(ffi::Error::new(rc), None));
        }

        let rc = ffi::sqlite3_create_function_v2(
            handle,
            retry_name.as_ptr(),
            0,
            ffi::SQLITE_UTF8 | ffi::SQLITE_DIRECTONLY,
            state as *mut c_void,
            Some(busy_retry_ms),
            None,
            None,
            None,
        );
        if rc != ffi::SQLITE_OK {
            return Err(Error::SqliteFailure(ffi::Error::new(rc), None));
        }

        ffi::sqlite3_progress_handler(
            handle,
            CHECK_EVERY_OPS,
            Some(on_progress),
            state as *mut c_void,
        );
        ffi::sqlite3_busy_handler(handle, Some(on_busy), state as *mut c_void);

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
    fn busy_waits_follow_the_ladder_until_the_busy_timeout() {
        assert_eq!(busy_step(0, 5_000, 0, 0, 0), Some(1));
        assert_eq!(busy_step(3, 5_000, 0, 0, 0), Some(10));
        assert_eq!(busy_step(50, 5_000, 0, 0, 0), Some(100));
        // The last sleep is trimmed so the total lands on the timeout, then it gives up.
        assert_eq!(waited_ms(12), 328);
        assert_eq!(busy_step(12, 330, 0, 0, 0), Some(2));
        assert_eq!(busy_step(13, 330, 0, 0, 0), None);
    }

    #[test]
    fn a_busy_wait_gives_up_at_the_deadline() {
        let ms = 1_000_000;
        // 200 ms deadline, statement started at t=1 s.
        assert_eq!(
            busy_step(5, 5_000, 200 * ms, 1_000 * ms, 1_100 * ms),
            Some(20)
        );
        assert_eq!(
            busy_step(5, 5_000, 200 * ms, 1_000 * ms, 1_195 * ms),
            Some(5)
        );
        assert_eq!(busy_step(5, 5_000, 200 * ms, 1_000 * ms, 1_201 * ms), None);
    }

    #[test]
    fn a_retry_is_granted_only_for_the_statement_the_handler_yielded() {
        // The probe (seq 8) directly follows the yielded statement (seq 7).
        assert!(retry_allowed(true, 7, 8));
        // The BUSY never reached the handler (a snapshot / upgrade BUSY): the last yield is older.
        assert!(!retry_allowed(true, 5, 8));
        // Nothing ever yielded.
        assert!(!retry_allowed(true, 0, 1));
        // Yield off (fathom's own handles, the tenant script path): the handler waited itself.
        assert!(!retry_allowed(false, 7, 8));
    }

    #[test]
    fn trigger_traces_are_recognized() {
        assert!(is_trigger_trace(b"-- TRIGGER t_after_insert"));
        assert!(!is_trigger_trace(b"SELECT 1"));
    }
}
