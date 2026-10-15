//! The WAL commit hook — the seam Phase 2 A2 (quorum replication) will ship frames from.
//!
//! `docs/a2-quorum-replication.md` records why this lives in the loadable extension rather than in
//! `exqlite`: exqlite 0.37.0 exposes no WAL surface beyond `PRAGMA wal_auto_check_point`, but an
//! extension receives a live `sqlite3*`, and `sqlite3_wal_hook` IS reachable through the
//! extension API pointer table. Same move as the Django UDFs — stop asking exqlite.
//!
//! # Why this module also checkpoints
//!
//! **`sqlite3_wal_hook` and `wal_autocheckpoint` are the same slot.** Auto-checkpointing *is* a
//! built-in WAL hook, so registering ours EVICTS it (rusqlite says so outright:
//! `hooks/mod.rs` — "the `sqlite3_wal_autocheckpoint()` interface and the `wal_autocheckpoint`
//! pragma both invoke `sqlite3_wal_hook()` and will overwrite any prior settings").
//!
//! `Fathom.Shard.Connection.configure_readwrite/3` sets the pragma first
//! (`lib/fathom/shard/connection.ex:196`) and loads this extension second (`:80`), so a hook that
//! merely observed would silently disable checkpointing on **every tenant connection** and grow
//! the WAL without bound — presenting as the disk-fill failure expert review #36 built
//! `FathomDiskFillingUp` for, with the diagnostic pointing at storage instead of here.
//!
//! So `on_commit` re-implements what it displaced: PASSIVE checkpoint at the same threshold. This
//! is deliberately behaviour-preserving, and `test/fathom/wal_hook_test.exs` asserts the negative
//! (the WAL still truncates) rather than only the positive (the hook fired) — the failure mode is
//! silent and slow, so a test that only proved the hook ran would pass either way.
//!
//! # What changed when A2 landed (expert review 2026-10-15 #1)
//!
//! A checkpoint backfills the WAL into the `.db`, and once EVERYTHING is backfilled the next writer
//! RESTARTS the log in place (new salt, offset 0) — destroying frames A2 may not have shipped yet.
//! A stream commits first and calls `Session.commit` second, so a checkpoint landing between the
//! two erased a commit that was then acked quorum-durable while no replica held it.
//!
//! For a WAL under replication the rule is now *checkpoint only what the write quorum has already
//! acked*, enforced in Elixir (`Fathom.Shard.Replication.Session`), which runs every checkpoint of
//! a replicated WAL behind a pinned read snapshot no newer than the quorum's extent. This hook's
//! part is to STEP ASIDE for such a WAL: `fathom_wal_replicated(path, delta)` keeps a refcounted
//! set of replicated database files, and `on_commit` skips its checkpoint for any file in it. Every
//! other database — replication off, a migrator's scratch file, a test path — keeps the 4000-frame
//! PASSIVE checkpoint exactly as before.
//!
//! The set is consulted only when a commit crosses the threshold, so a commit below it pays nothing
//! new. `pragma_guard` also reads it, to refuse a tenant's own `PRAGMA wal_checkpoint` on a
//! replicated file, and it denies tenant handles the registration function itself.

use std::collections::HashMap;
use std::ffi::{c_void, CStr};
use std::os::raw::{c_char, c_int};
use std::sync::atomic::{AtomicI64, Ordering};
use std::sync::{Mutex, OnceLock};

use rusqlite::functions::{Context, FunctionFlags};
use rusqlite::types::Value;
use rusqlite::{ffi, Connection, Error, Result};

/// Commits observed since process start, across every connection in this OS process.
///
/// Process-global rather than per-shard because the hook carries no per-connection state, and a
/// probe counter is all this is. A2 carries shard identity through its own channel.
static COMMITS: AtomicI64 = AtomicI64::new(0);

/// Page count reported by the most recent commit.
static LAST_PAGES: AtomicI64 = AtomicI64::new(0);

/// MUST track `PRAGMA wal_autocheckpoint` in `Fathom.Shard.Connection.configure/1`
/// (`lib/fathom/shard/connection.ex:196`). That 4000 is not SQLite's default of 1000 — expert
/// review 2026-07-24 #4 raised it deliberately, because an autocheckpoint runs INLINE inside the
/// committing tenant's query. Changing one without the other reintroduces that stall (too low) or
/// grows the WAL past what the coordinator's own checkpoint expects (too high).
/// `Fathom.Shard.Replication.Session`'s replicated-WAL threshold mirrors it too.
const AUTOCHECKPOINT_PAGES: c_int = 4000;

/// The registration function's name, shared with `pragma_guard`'s tenant denial.
pub const REGISTER_FN: &str = "fathom_wal_replicated";

/// Database files whose WAL is under A2 replication, by canonical path, refcounted (an outgoing
/// and an incoming Session of the same shard can briefly overlap).
fn replicated() -> &'static Mutex<HashMap<Vec<u8>, u32>> {
    static SET: OnceLock<Mutex<HashMap<Vec<u8>, u32>>> = OnceLock::new();
    SET.get_or_init(|| Mutex::new(HashMap::new()))
}

/// The key a path is registered under. SQLite names a database by its full path with symlinks
/// resolved (`unixFullPathname`), so registration resolves them the same way (`/tmp` is a symlink
/// on macOS).
///
/// A FILE THAT IS GONE still resolves through its directory (review follow-up 2026-10-15 M1): a
/// Session releases its registration in `terminate/2`, which can run after the drop path has
/// unlinked the `.db`. Falling back to the raw path there released a DIFFERENT key whenever a
/// directory component was a symlink, and the real key stayed registered forever — the hook then
/// skipped every checkpoint of that file for the life of the node. Callers also release with the
/// key registration returned (`fathom_wal_replicated(path, 1)` answers it), so this is the second
/// line of defence, for a directory that is still there.
fn canonical(path: &[u8]) -> Vec<u8> {
    use std::os::unix::ffi::{OsStrExt, OsStringExt};
    let p = std::path::Path::new(std::ffi::OsStr::from_bytes(path));
    if let Ok(c) = std::fs::canonicalize(p) {
        return c.into_os_string().into_vec();
    }
    match (p.parent(), p.file_name()) {
        (Some(dir), Some(name)) if !dir.as_os_str().is_empty() => {
            match std::fs::canonicalize(dir) {
                Ok(d) => d.join(name).into_os_string().into_vec(),
                Err(_) => path.to_vec(),
            }
        }
        _ => path.to_vec(),
    }
}

/// Whether `filename` (SQLite's own name for the database, `sqlite3_db_filename`) is replicated.
pub fn is_replicated(filename: &[u8]) -> bool {
    match replicated().lock() {
        Ok(set) => set.contains_key(filename),
        // A poisoned lock means a panic mid-update. Fail SAFE — treat it as replicated: the worst
        // case is a WAL the Session checkpoints instead of the hook.
        Err(_) => true,
    }
}

/// Whether database `schema` of connection `db` is replicated.
///
/// # Safety
/// `db` must be a live connection handle and `schema` a valid NUL-terminated name (or null).
pub unsafe fn db_is_replicated(db: *mut ffi::sqlite3, schema: *const c_char) -> bool {
    let name = ffi::sqlite3_db_filename(db, schema);
    if name.is_null() {
        return false;
    }
    let bytes = CStr::from_ptr(name).to_bytes();
    !bytes.is_empty() && is_replicated(bytes)
}

/// Adjust `path`'s refcount by `delta`; returns the key it is registered under and the new count
/// (0 removes it). Pure bookkeeping, separate so it is unit-testable without a connection.
pub fn adjust(path: &[u8], delta: i64) -> (Vec<u8>, i64) {
    let key = canonical(path);
    let Ok(mut set) = replicated().lock() else {
        return (key, -1);
    };
    let now = i64::from(set.get(&key).copied().unwrap_or(0)) + delta;
    if now <= 0 {
        set.remove(&key);
        (key, 0)
    } else {
        set.insert(key.clone(), u32::try_from(now).unwrap_or(u32::MAX));
        (key, now)
    }
}

/// Install the commit hook, the registration function, and (only when probing is enabled) the
/// read-back functions.
pub fn install(db: &Connection) -> Result<()> {
    // Registered through the raw API rather than `Connection::wal_hook`, because that wrapper hides
    // the `sqlite3*` and the hook needs it to name the database it is about to checkpoint.
    unsafe {
        ffi::sqlite3_wal_hook(db.handle(), Some(on_commit), std::ptr::null_mut());
    }

    // `fathom_wal_replicated(path, delta)`. DIRECTONLY: never from a view, trigger or schema
    // object. Tenant handles are additionally denied it by `pragma_guard`'s authorizer — a tenant
    // able to unregister its own file would re-enable the unpinned checkpoint.
    db.create_scalar_function(
        REGISTER_FN,
        2,
        FunctionFlags::SQLITE_UTF8 | FunctionFlags::SQLITE_DIRECTONLY,
        register,
    )?;

    if probe_enabled() {
        register_probe(db)?;
    }

    Ok(())
}

/// `fathom_wal_replicated(path, delta)`: delta 1 registers and -1 releases, both answering the KEY
/// the path is registered under (release with that key, see `canonical`); delta 0 answers the
/// current count without changing it.
fn register(ctx: &Context<'_>) -> Result<Value> {
    let path: String = ctx.get(0)?;
    let delta: i64 = ctx.get(1)?;
    if path.is_empty() || !(-1..=1).contains(&delta) {
        return Err(Error::UserFunctionError(
            "fathom_wal_replicated: expected (path, -1 | 0 | 1)".into(),
        ));
    }
    let (key, count) = adjust(path.as_bytes(), delta);
    Ok(if delta == 0 {
        Value::Integer(count)
    } else {
        Value::Text(String::from_utf8_lossy(&key).into_owned())
    })
}

/// Runs inside SQLite's commit path, on the committing tenant's thread.
///
/// Two hard rules. It must not block — every microsecond here is added to a tenant's COMMIT. And
/// it must not return an error: the return value propagates into the commit, so a failed checkpoint
/// must never fail the transaction that triggered it. SQLite's own autocheckpoint ignores
/// checkpoint errors for the same reason, so swallowing here is parity, not laziness.
///
/// A replicated database is skipped (see the module docs): its checkpoints belong to the Session,
/// which can prove the frames it backfills have shipped. The registry lookup runs only past the
/// threshold, so the common commit pays two atomic stores, as before.
unsafe extern "C" fn on_commit(
    _arg: *mut c_void,
    db: *mut ffi::sqlite3,
    db_name: *const c_char,
    pages: c_int,
) -> c_int {
    COMMITS.fetch_add(1, Ordering::Relaxed);
    LAST_PAGES.store(i64::from(pages), Ordering::Relaxed);

    if pages >= AUTOCHECKPOINT_PAGES && !db_is_replicated(db, db_name) {
        // PASSIVE matches what SQLite's built-in hook does: do as much as possible without
        // blocking readers or writers, and give up rather than wait.
        let _ = ffi::sqlite3_wal_checkpoint_v2(
            db,
            db_name,
            ffi::SQLITE_CHECKPOINT_PASSIVE,
            std::ptr::null_mut(),
            std::ptr::null_mut(),
        );
    }

    ffi::SQLITE_OK
}

/// `FATHOM_WAL_PROBE=1`.
///
/// Gated OFF by default because these functions are visible to **tenant** SQL, and a
/// process-global commit count is a co-tenancy side channel: it discloses no tenant content, but
/// it does reveal that other tenants on this node are writing. Enabling it is a debugging choice,
/// which is why it is not simply always on. A2 does not need it — Elixir receives frames over a
/// real channel, not by SELECTing a counter.
///
/// Read on EVERY install rather than cached in a `OnceLock`. The cache was the first version and
/// is untestable: `install` runs on every connection open, so the first open in the OS process
/// freezes the answer, and a test calling `System.put_env` afterwards could never turn the probe
/// on. `getenv` is an environ lookup, not a syscall — against the ~39 µs the extension already
/// costs per open, it is noise, and `bench_test`/the gate keep that honest.
fn probe_enabled() -> bool {
    matches!(std::env::var("FATHOM_WAL_PROBE").as_deref(), Ok("1"))
}

fn register_probe(db: &Connection) -> Result<()> {
    // Neither DETERMINISTIC nor INNOCUOUS, both truthfully: these read mutable process state, so
    // SQLite must refuse them in indexes, views and CHECK constraints.
    let flags = FunctionFlags::SQLITE_UTF8;

    db.create_scalar_function("fathom_wal_commits", 0, flags, |_| {
        Ok(COMMITS.load(Ordering::Relaxed))
    })?;

    db.create_scalar_function("fathom_wal_pages", 0, flags, |_| {
        Ok(LAST_PAGES.load(Ordering::Relaxed))
    })
}

// ---------------------------------------------------------------------------------------------
// Why the hook itself has no #[test] here
// ---------------------------------------------------------------------------------------------
//
// There CANNOT be one, and the first attempt failed in a way worth recording so nobody spends the
// afternoon again. This crate is built with rusqlite's `loadable_extension` feature, which routes
// every SQLite symbol through the `sqlite3_api_routines` pointer table that SQLite populates when
// it dlopens the extension. In a `cargo test` binary nothing ever populates it, so the moment a
// test touches a live connection it aborts with:
//
//     SQLite API not initialized or SQLite feature omitted
//
// That is why Cargo.toml says the `rlib` exists so "the in-crate unit tests can link the pure
// logic directly" — PURE logic. The registry bookkeeping below is pure and is tested here; the
// hook, which is connection-shaped, is out of reach by construction. Its tests are
// `test/fathom/wal_hook_test.exs` and `test/fathom/shard/replication_unshipped_checkpoint_test.exs`,
// driven through exqlite → dlopen → api routines → hook.

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn registration_is_refcounted_and_removal_is_total() {
        let p = b"/nonexistent/fathom_wal_test_refcount.db";
        assert!(!is_replicated(p));
        assert_eq!(adjust(p, 1).1, 1);
        assert_eq!(adjust(p, 1).1, 2);
        assert!(is_replicated(p));
        assert_eq!(adjust(p, -1).1, 1);
        assert!(
            is_replicated(p),
            "one holder remains, so the WAL is still replicated"
        );
        assert_eq!(adjust(p, -1).1, 0);
        assert!(!is_replicated(p));
        // An unbalanced release never goes negative and never resurrects the entry.
        assert_eq!(adjust(p, -1).1, 0);
        assert!(!is_replicated(p));
    }

    #[test]
    fn delta_zero_reads_the_count() {
        let p = b"/nonexistent/fathom_wal_test_read.db";
        assert_eq!(adjust(p, 0).1, 0);
        assert_eq!(adjust(p, 1).1, 1);
        assert_eq!(adjust(p, 0).1, 1);
        assert_eq!(adjust(p, -1).1, 0);
    }

    #[test]
    fn paths_are_keyed_by_their_resolved_form() {
        // The registry must agree with SQLite's own (symlink-resolved) name for the file.
        let dir = std::env::temp_dir().join(format!("fathom_wal_canon_{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let file = dir.join("x.db");
        std::fs::write(&file, b"").unwrap();
        let link = std::env::temp_dir().join(format!("fathom_wal_link_{}", std::process::id()));
        let _ = std::fs::remove_file(&link);
        std::os::unix::fs::symlink(&dir, &link).unwrap();

        use std::os::unix::ffi::OsStrExt;
        let via_link = link.join("x.db");
        let (key, n) = adjust(via_link.as_os_str().as_bytes(), 1);
        assert_eq!(n, 1);
        let resolved = std::fs::canonicalize(&file).unwrap();
        assert_eq!(key, resolved.as_os_str().as_bytes());
        assert!(is_replicated(resolved.as_os_str().as_bytes()));
        assert_eq!(adjust(resolved.as_os_str().as_bytes(), -1).1, 0);

        // THE LEAK (review follow-up 2026-10-15 M1): register through the symlink, DELETE the
        // file, then release through the symlink - what a Session's terminate/2 does after the
        // drop path unlinked the shard. The release must hit the registered key.
        assert_eq!(adjust(via_link.as_os_str().as_bytes(), 1).1, 1);
        std::fs::remove_file(&file).unwrap();
        assert_eq!(adjust(via_link.as_os_str().as_bytes(), -1).1, 0);
        assert!(
            !is_replicated(resolved.as_os_str().as_bytes()),
            "the registration leaked: the file stays replicated and is never checkpointed"
        );

        let _ = std::fs::remove_file(&link);
        let _ = std::fs::remove_dir_all(&dir);
    }
}
