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
//! # What changes when A2 lands
//!
//! A checkpoint **truncates the WAL**, which destroys frames that have not shipped yet. So the
//! A2 rule is *checkpoint only what the write quorum has already acked*, making truncation a
//! downstream consequence of replication progress rather than a page-count timer. The
//! `AUTOCHECKPOINT_PAGES` branch below is the pre-A2 stand-in for exactly that decision.

use std::os::raw::c_int;
use std::sync::atomic::{AtomicI64, Ordering};

use rusqlite::functions::FunctionFlags;
use rusqlite::hooks::{CheckpointMode, Wal};
use rusqlite::{Connection, Result};

/// Commits observed since process start, across every connection in this OS process.
///
/// Process-global rather than per-shard because `Connection::wal_hook` takes a bare `fn` pointer,
/// not a closure — there is nowhere to hang per-connection state, and `Wal` exposes only the
/// database *name* ("main"), never the path. Sufficient for the gate-1 proof; A2 will carry shard
/// identity through its own channel rather than through this counter.
static COMMITS: AtomicI64 = AtomicI64::new(0);

/// Page count reported by the most recent commit — the number A2's shipper will act on.
static LAST_PAGES: AtomicI64 = AtomicI64::new(0);

/// MUST track `PRAGMA wal_autocheckpoint` in `Fathom.Shard.Connection.configure/1`
/// (`lib/fathom/shard/connection.ex:196`). That 4000 is not SQLite's default of 1000 — expert
/// review 2026-07-24 #4 raised it deliberately, because an autocheckpoint runs INLINE inside the
/// committing tenant's query. Changing one without the other reintroduces that stall (too low) or
/// grows the WAL past what the coordinator's own checkpoint expects (too high).
const AUTOCHECKPOINT_PAGES: c_int = 4000;

/// Install the commit hook, and (only when probing is enabled) the read-back functions.
pub fn install(db: &Connection) -> Result<()> {
    db.wal_hook(Some(on_commit));

    if probe_enabled() {
        register_probe(db)?;
    }

    Ok(())
}

/// Runs inside SQLite's commit path, on the committing tenant's thread.
///
/// Two hard rules. It must not block — every microsecond here is added to a tenant's COMMIT. And
/// it must not return `Err`: the return value propagates into the commit, so a failed checkpoint
/// must never fail the transaction that triggered it. SQLite's own autocheckpoint ignores
/// checkpoint errors for the same reason, so swallowing here is parity, not laziness.
fn on_commit(wal: &Wal, pages: c_int) -> Result<()> {
    COMMITS.fetch_add(1, Ordering::Relaxed);
    LAST_PAGES.store(i64::from(pages), Ordering::Relaxed);

    if pages >= AUTOCHECKPOINT_PAGES {
        // PASSIVE matches what SQLite's built-in hook does: do as much as possible without
        // blocking readers or writers, and give up rather than wait.
        let _ = wal.checkpoint_v2(CheckpointMode::PASSIVE);
    }

    Ok(())
}

/// `FATHOM_WAL_PROBE=1`.
///
/// Gated OFF by default because these functions are visible to **tenant** SQL, and a
/// process-global commit count is a co-tenancy side channel: it discloses no tenant content, but
/// it does reveal that other tenants on this node are writing. Enabling it is a debugging choice,
/// which is why it is not simply always on. A2 will not need it — Elixir will receive frames over
/// a real channel, not by SELECTing a counter.
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
// Why there is no #[test] in this file
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
// logic directly" — PURE logic. `oracle.rs` and friends test formatting and parsing, which need no
// connection. Anything connection-shaped, including this hook, is out of reach here by
// construction.
//
// It is also the right outcome rather than a limitation to work around: a normal-linked Rust test
// would prove the hook fires under a DIFFERENT linkage than the one fathom ships, which is exactly
// the "the test double can't express the bug" trap AGENTS.md describes. The only test that proves
// anything is one driven through exqlite → dlopen → api routines → hook.
//
// That test is `test/fathom/wal_hook_test.exs`.
