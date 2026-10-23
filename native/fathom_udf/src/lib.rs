//! `fathom_udf` — the SQLite loadable extension that supplies the user-defined functions Django's
//! SQLite backend registers on its own client connections.
//!
//! ## Why this exists (expert review 2026-08-01 #19)
//!
//! `django/db/backends/sqlite3/_functions.py` registers ~35 Python functions on every connection
//! Django opens. Under `django-libsql` the SQL crosses the wire and is compiled by **fathom's**
//! SQLite, where those functions do not exist — so ordinary querysets (`__year`, `__date`,
//! `__regex`, `Trunc*`, `F()` arithmetic on a `DurationField`) raise `OperationalError` while
//! basic CRUD works. That asymmetry is why the working slice and both TPC harnesses never caught
//! it: TPC-B and TPC-C use only builtins.
//!
//! The finding recommended "implement the pure ones via exqlite's scalar-function registration".
//! That cannot be done as written — exqlite 0.37.0 exposes no user-defined-function API at all
//! (no `create_function`, no scalar or aggregate registration, in either `Exqlite.Sqlite3` or
//! `Exqlite.Sqlite3NIF`). What it *does* expose is `enable_load_extension/2`, so the mechanism is a
//! loadable extension. See `tasks/todo.md` for the three costed options and the decision.
//!
//! ## Why Rust rather than C
//!
//! Six of the missing functions take a `tzname` and need a timezone **database** server-side, and
//! they are the ones ordinary apps hit first (`__year`, `__date`, `TruncMonth`). `chrono-tz`
//! compiles the IANA database into this binary, so there is no `/usr/share/zoneinfo` dependency
//! and a slim runner image behaves like a dev box. In C that piece is the whole project.
//!
//! ## What is NOT registered here, on purpose
//!
//! The 18 math functions (`ACOS`…`TAN`) and `SIGN`. fathom's SQLite is built with
//! `SQLITE_ENABLE_MATH_FUNCTIONS` and already provides them — Django itself only registers those
//! when `sqlite_compileoption_used('ENABLE_MATH_FUNCTIONS')` is false. Registering them anyway
//! would silently REPLACE working builtins with our own, which is a strictly worse position: more
//! code on the hot path and a second implementation to keep correct. `COT` is the exception —
//! SQLite has no `COT`, and Django registers it unconditionally.
//!
//! ## Security posture
//!
//! Extension loading is a privileged capability on a multi-tenant engine, so `Fathom.Shard
//! .Connection` enables it, loads exactly this file, and **disables it again before any tenant SQL
//! runs**. A tenant calling `load_extension(...)` afterwards gets `not authorized`. Every function
//! here is pure: no filesystem, no network, no process state. They are registered
//! `SQLITE_INNOCUOUS` because that is true, which also lets them appear in views and expression
//! indexes the way Django expects.

use std::os::raw::{c_char, c_int};

use rusqlite::functions::{Aggregate, Context, FunctionFlags};
use rusqlite::types::{Type, Value, ValueRef};
use rusqlite::{ffi, Connection, Error, Result};

pub mod aggregates;
pub mod auto;
pub mod backstop;
pub mod datetime;
pub mod pragma_guard;
pub mod pyre;
pub mod pytypes;
pub mod scalars;
pub mod wal;

use aggregates::{BitAgg, BitOp, Variance};
use datetime::UdfError;

/// SQLite derives the entry-point symbol from the filename when `load_extension` is called with
/// one argument: `libfathom_udf.dylib` → strip `lib`, truncate at the first `.`, drop
/// non-alphanumerics, lowercase → `sqlite3_fathomudf_init`. `Fathom.Shard.Connection` passes the
/// name explicitly as the second argument anyway, so renaming the artifact cannot break loading.
///
/// # Safety
///
/// Called by SQLite through `dlopen`/`dlsym` with a live database handle and the extension API
/// table. `extension_init2` is rusqlite's wrapper for exactly this contract.
#[no_mangle]
pub unsafe extern "C" fn sqlite3_fathomudf_init(
    db: *mut ffi::sqlite3,
    pz_err_msg: *mut *mut c_char,
    p_api: *mut ffi::sqlite3_api_routines,
) -> c_int {
    Connection::extension_init2(db, pz_err_msg, p_api, register_all)
}

/// The explicit, per-connection load: every function, AND the WAL hook and the backstop's handlers
/// at once (exqlite's own handlers are already in place by the time anyone can call
/// `load_extension`). Used when the auto-extension is not armed (an artifact predating it), and by
/// callers that load it themselves (the A2 Session's registry connection).
///
/// Returning `Ok(false)` makes this a per-connection extension rather than a persistent one. The
/// PROCESS-GLOBAL registration is a separate, deliberate step: see src/auto.rs (expert review
/// 2026-10-22 r8 #14), whose module docs carry the security argument.
fn register_all(db: Connection) -> Result<bool> {
    let state = register_common(&db)?;
    unsafe {
        wal::install_hook(db.handle());
        backstop::attach(db.handle(), state)?;
    }
    Ok(false)
}

/// The auto-extension init (src/auto.rs), run inside every `sqlite3_open` once armed: every
/// function and the per-connection state, but NO hook or handler. `fathom_conn_init()` installs
/// those from `Fathom.Shard.Connection`'s open batch.
pub(crate) fn register_auto(db: Connection) -> Result<bool> {
    register_common(&db)?;
    Ok(false)
}

/// Everything both inits register. Returns the backstop state `fathom_conn_init` attaches.
fn register_common(db: &Connection) -> Result<backstop::State> {
    // Pure and deterministic: safe in views, expression indexes and CHECK constraints.
    let det = FunctionFlags::SQLITE_UTF8
        | FunctionFlags::SQLITE_DETERMINISTIC
        | FunctionFlags::SQLITE_INNOCUOUS;
    // Pure but NOT deterministic — SQLite will refuse to index these, which is correct.
    let nondet = FunctionFlags::SQLITE_UTF8 | FunctionFlags::SQLITE_INNOCUOUS;

    register_datetime(db, det)?;
    register_text(db, det)?;
    register_hashes(db, det)?;
    register_misc(db, det, nondet)?;
    register_aggregates(db)?;

    // The Phase 2 A2 frame seam's registration function. The commit hook itself is installed by
    // the caller (`register_all`) or by `fathom_conn_init()`, never here: `sqlite3_wal_hook` and
    // `wal_autocheckpoint` share one slot, so its placement is load-bearing. See src/wal.rs.
    wal::install_functions(db)?;

    // The statement-deadline backstop: a timeout enforced on the query's own thread, because a
    // BEAM timer can fail to fire while the query runs. See src/backstop.rs. Setters and state
    // only; the handlers go in with the hook.
    let state = backstop::register(db)?;

    // The engine half of the tenant PRAGMA gate. Registers only the set-once
    // `fathom_pragma_guard(...)` setter; the authorizer itself is installed when fathom calls it,
    // on tenant handles only (an authorizer denies ATTACH, and VACUUM INTO is an ATTACH). See
    // src/pragma_guard.rs.
    pragma_guard::install(db)?;

    register_conn_init(db, state)?;
    Ok(state)
}

/// `fathom_conn_init()` (expert review 2026-10-22 r8 #14): install this connection's WAL commit
/// hook and the backstop's progress, busy and trace handlers. Returns 1.
///
/// `Fathom.Shard.Connection` runs it inside its open batch on every handle it opens, in the place
/// `PRAGMA wal_autocheckpoint=4000` used to occupy (the hook re-implements that 4000-frame PASSIVE
/// checkpoint, src/wal.rs), so nothing that runs after it can take the shared slot back. Idempotent:
/// a second call re-installs the same callbacks with the same state, so a tenant calling it changes
/// nothing. DIRECTONLY.
///
/// The function's user data is the state `backstop::register` just returned on this connection;
/// both are registered together in `register_common`, so re-registering one re-registers the other
/// (see `backstop::register` on why that pairing matters).
fn register_conn_init(db: &Connection, state: backstop::State) -> Result<()> {
    let name = std::ffi::CString::new("fathom_conn_init").expect("static name");
    let rc = unsafe {
        ffi::sqlite3_create_function_v2(
            db.handle(),
            name.as_ptr(),
            0,
            ffi::SQLITE_UTF8 | ffi::SQLITE_DIRECTONLY,
            state,
            Some(conn_init),
            None,
            None,
            None,
        )
    };
    if rc != ffi::SQLITE_OK {
        return Err(Error::SqliteFailure(ffi::Error::new(rc), None));
    }
    Ok(())
}

unsafe extern "C" fn conn_init(
    ctx: *mut ffi::sqlite3_context,
    _argc: c_int,
    _argv: *mut *mut ffi::sqlite3_value,
) {
    let db = ffi::sqlite3_context_db_handle(ctx);
    wal::install_hook(db);
    match backstop::attach(db, ffi::sqlite3_user_data(ctx)) {
        Ok(()) => ffi::sqlite3_result_int64(ctx, 1),
        Err(_) => {
            let msg = c"fathom_conn_init: could not install the backstop handlers";
            ffi::sqlite3_result_error(ctx, msg.as_ptr(), -1);
        }
    }
}

// ---------------------------------------------------------------------------------------------
// argument decoding
// ---------------------------------------------------------------------------------------------

/// Read argument `i` as text, mapping SQL NULL to `None`.
///
/// Accepts an integer or real by rendering it, because SQLite is dynamically typed and a column
/// declared `date` can hold whatever was inserted; Django's Python functions would see whatever
/// `sqlite3` handed them and `typecast_timestamp` would raise → `None`. Returning `None` for a
/// non-text value keeps that NULL-on-garbage behaviour without erroring.
fn opt_text<'a>(ctx: &'a Context<'a>, i: usize) -> Result<Option<&'a str>> {
    match ctx.get_raw(i) {
        ValueRef::Null => Ok(None),
        ValueRef::Text(_) => ctx.get_raw(i).as_str().map(Some).map_err(Error::from),
        _ => Ok(None),
    }
}

fn opt_i64(ctx: &Context<'_>, i: usize) -> Option<i64> {
    match ctx.get_raw(i) {
        ValueRef::Integer(v) => Some(v),
        _ => None,
    }
}

fn opt_f64(ctx: &Context<'_>, i: usize) -> Option<f64> {
    match ctx.get_raw(i) {
        ValueRef::Real(v) => Some(v),
        ValueRef::Integer(v) => Some(v as f64),
        _ => None,
    }
}

/// Copy a borrowed SQLite value into an owned one.
///
/// `ValueRef` borrows from SQLite's own buffer, which is only valid for the duration of the step
/// callback — an aggregate that keeps a value across rows (`ANY_VALUE`) has to own it. rusqlite
/// has no `From<ValueRef> for Value`, so this is spelled out. Non-UTF-8 text is kept as a blob
/// rather than lossily replaced, so no byte is silently rewritten on the way through.
fn own(raw: ValueRef<'_>) -> Value {
    match raw {
        ValueRef::Null => Value::Null,
        ValueRef::Integer(i) => Value::Integer(i),
        ValueRef::Real(f) => Value::Real(f),
        ValueRef::Text(t) => match std::str::from_utf8(t) {
            Ok(s) => Value::Text(s.to_owned()),
            Err(_) => Value::Blob(t.to_vec()),
        },
        ValueRef::Blob(b) => Value::Blob(b.to_vec()),
    }
}

/// Turn a Django-equivalent exception into a SQLite error carrying the same message, so an
/// operator debugging a failing queryset sees `Unsupported lookup type: 'fortnight'` rather than
/// an opaque extension failure.
fn udf_err(e: UdfError) -> Error {
    Error::UserFunctionError(Box::new(e))
}

// ---------------------------------------------------------------------------------------------
// date / time
// ---------------------------------------------------------------------------------------------

fn register_datetime(db: &Connection, flags: FunctionFlags) -> Result<()> {
    // Arity 2, and it shares `_sqlite_datetime_extract` with django_datetime_extract — Django
    // registers the SAME function under both names. The two trailing timezone arguments are
    // simply absent here, which is the `tzname=None, conn_tzname=None` default.
    db.create_scalar_function("django_date_extract", 2, flags, |ctx| {
        let lookup = opt_text(ctx, 0)?;
        let dt = opt_text(ctx, 1)?;
        datetime::datetime_extract(lookup, dt, None, None).map_err(udf_err)
    })?;

    db.create_scalar_function("django_datetime_extract", 4, flags, |ctx| {
        let lookup = opt_text(ctx, 0)?;
        let dt = opt_text(ctx, 1)?;
        let tz = opt_text(ctx, 2)?;
        let conn_tz = opt_text(ctx, 3)?;
        datetime::datetime_extract(lookup, dt, tz, conn_tz).map_err(udf_err)
    })?;

    db.create_scalar_function("django_date_trunc", 4, flags, |ctx| {
        let lookup = opt_text(ctx, 0)?;
        let dt = opt_text(ctx, 1)?;
        let tz = opt_text(ctx, 2)?;
        let conn_tz = opt_text(ctx, 3)?;
        datetime::date_trunc(lookup, dt, tz, conn_tz).map_err(udf_err)
    })?;

    db.create_scalar_function("django_datetime_trunc", 4, flags, |ctx| {
        let lookup = opt_text(ctx, 0)?;
        let dt = opt_text(ctx, 1)?;
        let tz = opt_text(ctx, 2)?;
        let conn_tz = opt_text(ctx, 3)?;
        datetime::datetime_trunc(lookup, dt, tz, conn_tz).map_err(udf_err)
    })?;

    db.create_scalar_function("django_time_trunc", 4, flags, |ctx| {
        let lookup = opt_text(ctx, 0)?;
        let dt = opt_text(ctx, 1)?;
        let tz = opt_text(ctx, 2)?;
        let conn_tz = opt_text(ctx, 3)?;
        datetime::time_trunc(lookup, dt, tz, conn_tz).map_err(udf_err)
    })?;

    db.create_scalar_function("django_datetime_cast_date", 3, flags, |ctx| {
        let dt = opt_text(ctx, 0)?;
        let tz = opt_text(ctx, 1)?;
        let conn_tz = opt_text(ctx, 2)?;
        datetime::datetime_cast_date(dt, tz, conn_tz).map_err(udf_err)
    })?;

    db.create_scalar_function("django_datetime_cast_time", 3, flags, |ctx| {
        let dt = opt_text(ctx, 0)?;
        let tz = opt_text(ctx, 1)?;
        let conn_tz = opt_text(ctx, 2)?;
        datetime::datetime_cast_time(dt, tz, conn_tz).map_err(udf_err)
    })?;

    db.create_scalar_function("django_time_extract", 2, flags, |ctx| {
        let lookup = opt_text(ctx, 0)?;
        let dt = opt_text(ctx, 1)?;
        datetime::time_extract(lookup, dt).map_err(udf_err)
    })?;

    db.create_scalar_function("django_time_diff", 2, flags, |ctx| {
        datetime::time_diff(opt_text(ctx, 0)?, opt_text(ctx, 1)?).map_err(udf_err)
    })?;

    db.create_scalar_function("django_timestamp_diff", 2, flags, |ctx| {
        datetime::timestamp_diff(opt_text(ctx, 0)?, opt_text(ctx, 1)?).map_err(udf_err)
    })?;

    db.create_scalar_function("django_format_dtdelta", 3, flags, |ctx| {
        let connector = opt_text(ctx, 0)?;
        let Some(connector) = connector else {
            return Ok(Value::Null);
        };
        let lhs = datetime::prepare_dtdelta_param(
            connector,
            opt_i64(ctx, 1),
            opt_text(ctx, 1)?,
            opt_f64(ctx, 1),
        );
        let rhs = datetime::prepare_dtdelta_param(
            connector,
            opt_i64(ctx, 2),
            opt_text(ctx, 2)?,
            opt_f64(ctx, 2),
        );
        // The result type varies by connector — text for +/-, INTEGER or REAL for * and / —
        // because Django only `str()`s the first two. See `datetime::DtOut`.
        let out = datetime::format_dtdelta(Some(connector), lhs, rhs).map_err(udf_err)?;
        Ok(match out {
            None => Value::Null,
            Some(datetime::DtOut::Text(s)) => Value::Text(s),
            Some(datetime::DtOut::Int(i)) => Value::Integer(i),
            Some(datetime::DtOut::Float(f)) => Value::Real(f),
        })
    })?;

    Ok(())
}

// ---------------------------------------------------------------------------------------------
// text
// ---------------------------------------------------------------------------------------------

/// The largest string a text-building UDF may return, in bytes: the handle's current
/// `SQLITE_LIMIT_LENGTH`, capped at fathom's tenant limit (`pragma_guard::LIMIT_LENGTH`, 64 MiB).
///
/// SQLite enforces that limit on a function's RETURN value, i.e. after `REPEAT`/`LPAD`/`RPAD` have
/// already allocated the whole thing, so the builders need the number up front to refuse before
/// allocating (expert review 2026-10-12 #2). Falls back to the constant if the handle cannot be
/// read, which is the safe direction.
fn result_cap(ctx: &Context<'_>) -> usize {
    let fallback = pragma_guard::LIMIT_LENGTH as usize;
    // SAFETY: `get_connection` is unsafe only because the returned handle could be sent to another
    // thread; it is used here for one read of one limit on this thread and dropped.
    let limit = unsafe { ctx.get_connection() }
        .and_then(|c| c.limit(rusqlite::limits::Limit::SQLITE_LIMIT_LENGTH))
        .ok()
        .and_then(|l| usize::try_from(l).ok());
    limit.map_or(fallback, |l| l.min(fallback))
}

fn too_big() -> Error {
    udf_err(UdfError::TooBig)
}

fn register_text(db: &Connection, flags: FunctionFlags) -> Result<()> {
    db.create_scalar_function("LPAD", 3, flags, |ctx| {
        let (Some(text), Some(len), Some(fill)) =
            (opt_text(ctx, 0)?, opt_i64(ctx, 1), opt_text(ctx, 2)?)
        else {
            return Ok(None);
        };
        scalars::lpad(text, len, fill, result_cap(ctx))
            .map(Some)
            .map_err(|_| too_big())
    })?;

    db.create_scalar_function("RPAD", 3, flags, |ctx| {
        let (Some(text), Some(len), Some(fill)) =
            (opt_text(ctx, 0)?, opt_i64(ctx, 1), opt_text(ctx, 2)?)
        else {
            return Ok(None);
        };
        scalars::rpad(text, len, fill, result_cap(ctx))
            .map(Some)
            .map_err(|_| too_big())
    })?;

    db.create_scalar_function("REPEAT", 2, flags, |ctx| {
        let (Some(text), Some(count)) = (opt_text(ctx, 0)?, opt_i64(ctx, 1)) else {
            return Ok(None);
        };
        scalars::repeat(text, count, result_cap(ctx))
            .map(Some)
            .map_err(|_| too_big())
    })?;

    db.create_scalar_function("REVERSE", 1, flags, |ctx| {
        let Some(text) = opt_text(ctx, 0)? else {
            return Ok(None);
        };
        Ok(Some(scalars::reverse(text)))
    })?;

    Ok(())
}

// ---------------------------------------------------------------------------------------------
// hashes
// ---------------------------------------------------------------------------------------------

macro_rules! hash_fn {
    ($db:expr, $flags:expr, $name:literal, $f:path) => {
        $db.create_scalar_function($name, 1, $flags, |ctx| {
            let Some(text) = opt_text(ctx, 0)? else {
                return Ok(None);
            };
            Ok(Some($f(text)))
        })?;
    };
}

fn register_hashes(db: &Connection, flags: FunctionFlags) -> Result<()> {
    hash_fn!(db, flags, "MD5", scalars::md5_hex);
    hash_fn!(db, flags, "SHA1", scalars::sha1_hex);
    hash_fn!(db, flags, "SHA224", scalars::sha224_hex);
    hash_fn!(db, flags, "SHA256", scalars::sha256_hex);
    hash_fn!(db, flags, "SHA384", scalars::sha384_hex);
    hash_fn!(db, flags, "SHA512", scalars::sha512_hex);
    Ok(())
}

// ---------------------------------------------------------------------------------------------
// regexp, bit ops, uuids, rand
// ---------------------------------------------------------------------------------------------

fn register_misc(db: &Connection, det: FunctionFlags, nondet: FunctionFlags) -> Result<()> {
    // `X REGEXP Y` is SQLite syntax for `regexp(Y, X)` — pattern FIRST, which is also the order
    // Django registers. Getting these backwards produces a function that "works" on symmetric
    // test data and is wrong everywhere else.
    //
    // Matching goes through `pyre`, which reproduces Python's `re.search` — see that module for
    // the two classes of difference it closes, including Python's `$` also matching before a
    // trailing newline (which compiled fine under the `regex` crate and quietly returned the
    // opposite answer).
    //
    // Compiled patterns are cached inside `pyre` rather than via SQLite's auxdata: auxdata only
    // covers a CONSTANT argument, and a Django queryset binds the pattern as a PARAMETER
    // (`WHERE col REGEXP ?`), which would otherwise recompile per row.
    db.create_scalar_function("regexp", 2, det, |ctx| {
        let (Some(pattern), Some(text)) = (opt_text(ctx, 0)?, opt_text(ctx, 1)?) else {
            return Ok(None);
        };

        pyre::search(pattern, text)
            .map(Some)
            .map_err(|e| Error::UserFunctionError(Box::new(e)))
    })?;

    db.create_scalar_function("BITXOR", 2, det, |ctx| {
        let (Some(x), Some(y)) = (opt_i64(ctx, 0), opt_i64(ctx, 1)) else {
            return Ok(None);
        };
        Ok(Some(scalars::bitxor(x, y)))
    })?;

    db.create_scalar_function("COT", 1, det, |ctx| {
        let Some(x) = opt_f64(ctx, 0) else {
            return Ok(None);
        };
        Ok(Some(scalars::cot(x)))
    })?;

    // Django stores UUIDs as 32 hex characters with no dashes (`uuid4().hex`), which is what
    // UUIDField's converter expects to read back.
    db.create_scalar_function("UUIDV4", 0, nondet, |_ctx| {
        Ok(Some(uuid::Uuid::new_v4().simple().to_string()))
    })?;

    db.create_scalar_function("UUIDV7", 0, nondet, |_ctx| {
        Ok(Some(uuid::Uuid::now_v7().simple().to_string()))
    })?;

    // `random.random()` — a float in [0, 1). NOT SQLite's `random()`, which returns a 64-bit int.
    db.create_scalar_function("RAND", 0, nondet, |_ctx| {
        Ok(Some(rand::random::<f64>()))
    })?;

    // `fathom_conn_state()` (expert review 2026-10-17 #6): this connection's `changes`,
    // `last_insert_rowid` and autocommit flag in ONE statement step, so a write statement pays one
    // dirty-NIF dispatch for them instead of three (Fathom.Shard.Connection). Not deterministic
    // (it reads connection state). It only reports the CALLING connection's own counters, which
    // the tenant can already read via `changes()` / `last_insert_rowid()`; the autocommit bit is
    // the one extra fact, and is visible to the tenant through their own COMMIT/BEGIN anyway.
    db.create_scalar_function("fathom_conn_state", 0, nondet, |ctx| {
        // SAFETY: `get_connection` is unsafe only because the handle could be sent to another
        // thread; it is used for three reads on this thread and dropped.
        let conn = unsafe { ctx.get_connection() }?;
        // `sqlite3_db_filename(db, "temp")` is NULL until the temp database's btree exists (made
        // lazily by the first TEMP object or the first statement that touches the `temp` schema)
        // and an empty string afterwards. Asking does not open anything (expert review 2026-10-21
        // r7 #14).
        let temp_open = unsafe {
            !ffi::sqlite3_db_filename(conn.handle(), c"temp".as_ptr()).is_null()
        };
        Ok(scalars::conn_state_blob(
            conn.changes(),
            conn.last_insert_rowid(),
            conn.is_autocommit(),
            temp_open,
        ))
    })?;

    // `fathom_stmt_memused(sql)` (expert review 2026-10-20-2027 #7): the heap bytes SQLite holds
    // for the prepared statement on THIS connection whose text is exactly `sql`
    // (`sqlite3_stmt_status(SQLITE_STMTSTATUS_MEMUSED)`), or NULL if none is prepared. With a NULL
    // argument, the sum over every statement on the connection. `Fathom.Shard.Connection` bounds
    // its statement cache by these bytes: exqlite exposes no statement-status call, and a 29-column
    // Django SELECT costs ~14 KiB where a point read costs ~2, so a COUNT cap is blind to the
    // difference. It reads only the calling connection's own statements, whose text the tenant
    // supplied; the byte count is the one new fact and says nothing about any other tenant.
    db.create_scalar_function("fathom_stmt_memused", 1, nondet, |ctx| {
        let want: Option<Vec<u8>> = match ctx.get_raw(0) {
            ValueRef::Null => None,
            ValueRef::Text(t) => Some(t.to_vec()),
            _ => return Err(Error::InvalidFunctionParameterType(0, Type::Text)),
        };
        // SAFETY: the handle is the calling connection, used on this thread for read-only
        // statement-list walks while SQLite holds its mutex for the duration of this call.
        let conn = unsafe { ctx.get_connection() }?;
        let db = unsafe { conn.handle() };
        Ok(unsafe { stmt_status(db, ffi::SQLITE_STMTSTATUS_MEMUSED, want.as_deref()) })
    })?;

    // `fathom_stmt_reprepares(sql)` (expert review 2026-10-21 r7 #1): how many times SQLite has
    // transparently re-prepared the statement on THIS connection whose text is exactly `sql`
    // (`sqlite3_stmt_status(SQLITE_STMTSTATUS_REPREPARE)`: a schema-cookie change, or an
    // authorizer/pragma that invalidates it, makes the first step after it call
    // `sqlite3_prepare` again INSIDE `sqlite3_step`). NULL if none is prepared; with a NULL
    // argument, the sum over every statement. exqlite exposes no statement-status call and the
    // re-prepare happens inside the step dispatch, so no dispatch counter can see it. The counter
    // lives as long as the statement, so a pooled handle's carried cache shows its history. Same
    // lookup and the same scope as `fathom_stmt_memused`: the calling connection's own statements.
    db.create_scalar_function("fathom_stmt_reprepares", 1, nondet, |ctx| {
        let want: Option<Vec<u8>> = match ctx.get_raw(0) {
            ValueRef::Null => None,
            ValueRef::Text(t) => Some(t.to_vec()),
            _ => return Err(Error::InvalidFunctionParameterType(0, Type::Text)),
        };
        // SAFETY: as `fathom_stmt_memused` above.
        let conn = unsafe { ctx.get_connection() }?;
        let db = unsafe { conn.handle() };
        Ok(unsafe { stmt_status(db, ffi::SQLITE_STMTSTATUS_REPREPARE, want.as_deref()) })
    })?;

    // `fathom_fk_on()` (expert review 2026-10-21 r7 #13a): turn foreign-key enforcement ON through
    // `sqlite3_db_config(SQLITE_DBCONFIG_ENABLE_FKEY)`, which expires the connection's prepared
    // statements only when the flag actually CHANGES. `PRAGMA foreign_keys = ON` emits OP_Expire
    // unconditionally, so fathom's pooled-reuse batch (and Django's own per-connection pragma)
    // made every carried cached statement re-parse and re-plan at its next step (a 29-column read
    // 4.4 -> 21.5 us). Returns the enforcement state afterwards (1). A tenant calling it gets
    // what the allowed `PRAGMA foreign_keys = ON` gives them; it cannot turn enforcement off.
    db.create_scalar_function("fathom_fk_on", 0, nondet, |ctx| {
        // SAFETY: the calling connection, used on this thread while SQLite holds its mutex.
        let conn = unsafe { ctx.get_connection() }?;
        let db = unsafe { conn.handle() };
        let mut now: std::os::raw::c_int = 0;
        let rc = unsafe {
            ffi::sqlite3_db_config(
                db,
                ffi::SQLITE_DBCONFIG_ENABLE_FKEY,
                1 as std::os::raw::c_int,
                &mut now as *mut std::os::raw::c_int,
            )
        };
        if rc != ffi::SQLITE_OK {
            return Err(Error::UserFunctionError(
                format!("sqlite3_db_config(ENABLE_FKEY) failed: {rc}").into(),
            ));
        }
        Ok(now as i64)
    })?;

    // `fathom_require_query_only()` (r7 #13a): 1 when the connection cannot write: either
    // `PRAGMA query_only` is ON or the main database was opened READ-ONLY
    // (`sqlite3_db_readonly`), an error when neither holds. A `:ro` pooled reuse asserts
    // read-only-ness; `PRAGMA query_only=ON` expires every cached statement even when the value
    // does not change, so the batch only checks, and the caller falls back to the assignment when
    // this errors (a handle that is physically read-write and lost the pragma). A handle opened
    // `SQLITE_OPEN_READONLY` cannot write whatever the pragma says, so it passes. Reports only what
    // `PRAGMA query_only` / the open mode already tell the tenant.
    db.create_scalar_function("fathom_require_query_only", 0, nondet, |ctx| {
        let conn = unsafe { ctx.get_connection() }?;
        let ro = unsafe { ffi::sqlite3_db_readonly(conn.handle(), c"main".as_ptr()) } == 1;
        let on: i64 = if ro {
            1
        } else {
            conn.query_row("PRAGMA query_only", [], |r| r.get(0))?
        };
        if on == 1 {
            Ok(1i64)
        } else {
            Err(Error::UserFunctionError("connection is not read-only".into()))
        }
    })?;

    // `fathom_cache_used()` (expert review 2026-10-21 #15): the bytes of page cache THIS connection
    // holds (`sqlite3_db_status(SQLITE_DBSTATUS_CACHE_USED)`). exqlite exposes no db-status call;
    // the idle-handle `PRAGMA shrink_memory` (`Fathom.Shard.HandlePool`) is verified against it.
    // It reads only the calling connection's own counter.
    db.create_scalar_function("fathom_cache_used", 0, nondet, |ctx| {
        // SAFETY: the calling connection, used on this thread while SQLite holds its mutex.
        let conn = unsafe { ctx.get_connection() }?;
        let db = unsafe { conn.handle() };
        let (mut cur, mut hi) = (0i32, 0i32);
        unsafe { ffi::sqlite3_db_status(db, ffi::SQLITE_DBSTATUS_CACHE_USED, &mut cur, &mut hi, 0) };
        Ok(cur as i64)
    })?;

    Ok(())
}

/// Walk the connection's prepared-statement list (`sqlite3_next_stmt`) for `fathom_stmt_memused`
/// and `fathom_stmt_reprepares`; `op` is the `SQLITE_STMTSTATUS_*` counter to read (never reset).
/// `Some(sql)`: the counter of the first statement whose text is byte-equal to `sql`, else `None`.
/// `None`: the sum over every statement, including the one running this function.
///
/// # Safety
///
/// `db` must be the live handle of the connection executing the calling statement.
unsafe fn stmt_status(db: *mut ffi::sqlite3, op: i32, want: Option<&[u8]>) -> Option<i64> {
    let mut total: i64 = 0;
    let mut stmt = ffi::sqlite3_next_stmt(db, std::ptr::null_mut());
    while !stmt.is_null() {
        let used = ffi::sqlite3_stmt_status(stmt, op, 0) as i64;
        match want {
            None => total += used,
            Some(sql) => {
                let text = ffi::sqlite3_sql(stmt);
                if !text.is_null() && std::ffi::CStr::from_ptr(text).to_bytes() == sql {
                    return Some(used);
                }
            }
        }
        stmt = ffi::sqlite3_next_stmt(db, stmt);
    }
    want.map_or(Some(total), |_| None)
}

// ---------------------------------------------------------------------------------------------
// aggregates
// ---------------------------------------------------------------------------------------------

/// Which statistic a `Variance`-backed aggregate finalizes to.
#[derive(Clone, Copy)]
enum Stat {
    StdDevPop,
    StdDevSamp,
    VarPop,
    VarSamp,
}

struct StatAgg(Stat);

impl Aggregate<Variance, Option<f64>> for StatAgg {
    fn init(&self, _: &mut Context<'_>) -> Result<Variance> {
        Ok(Variance::default())
    }

    fn step(&self, ctx: &mut Context<'_>, acc: &mut Variance) -> Result<()> {
        // NULLs are skipped rather than accumulated — see the module docs on aggregates for why
        // this deliberately diverges from Django's `list.append` step.
        match ctx.get_raw(0) {
            ValueRef::Null => Ok(()),
            ValueRef::Integer(v) => {
                acc.step(v as f64);
                Ok(())
            }
            ValueRef::Real(v) => {
                acc.step(v);
                Ok(())
            }
            ValueRef::Text(_) | ValueRef::Blob(_) => Err(Error::InvalidFunctionParameterType(
                0,
                Type::Text,
            )),
        }
    }

    fn finalize(&self, _: &mut Context<'_>, acc: Option<Variance>) -> Result<Option<f64>> {
        // `None` here means the aggregate saw no rows at all.
        let acc = acc.unwrap_or_default();
        Ok(match self.0 {
            Stat::StdDevPop => acc.stddev_pop(),
            Stat::StdDevSamp => acc.stddev_samp(),
            Stat::VarPop => acc.var_pop(),
            Stat::VarSamp => acc.var_samp(),
        })
    }
}

struct BitAggregate(BitOp);

impl Aggregate<BitAgg, Option<i64>> for BitAggregate {
    fn init(&self, _: &mut Context<'_>) -> Result<BitAgg> {
        Ok(BitAgg::default())
    }

    fn step(&self, ctx: &mut Context<'_>, acc: &mut BitAgg) -> Result<()> {
        match ctx.get_raw(0) {
            ValueRef::Null => Ok(()),
            ValueRef::Integer(v) => {
                acc.step(self.0, v);
                Ok(())
            }
            ValueRef::Real(v) => {
                acc.step(self.0, v as i64);
                Ok(())
            }
            _ => Err(Error::InvalidFunctionParameterType(0, Type::Text)),
        }
    }

    fn finalize(&self, _: &mut Context<'_>, acc: Option<BitAgg>) -> Result<Option<i64>> {
        Ok(acc.and_then(|a| a.value()))
    }
}

/// `ANY_VALUE(x)` — an arbitrary value from the group.
///
/// Django's `AnyValue.finalize` returns `self[0]`, the first value appended, NULLs included. We
/// return the first **non-NULL** value instead, which is what MySQL and BigQuery's `ANY_VALUE` do
/// and is consistent with how the other aggregates here treat NULLs. Either is a legal choice for
/// an "arbitrary value" aggregate; being consistent within this extension is the tiebreaker.
struct AnyValue;

impl Aggregate<Option<Value>, Value> for AnyValue {
    fn init(&self, _: &mut Context<'_>) -> Result<Option<Value>> {
        Ok(None)
    }

    fn step(&self, ctx: &mut Context<'_>, acc: &mut Option<Value>) -> Result<()> {
        if acc.is_none() {
            let raw = ctx.get_raw(0);
            if !matches!(raw, ValueRef::Null) {
                *acc = Some(own(raw));
            }
        }
        Ok(())
    }

    fn finalize(&self, _: &mut Context<'_>, acc: Option<Option<Value>>) -> Result<Value> {
        Ok(acc.flatten().unwrap_or(Value::Null))
    }
}

fn register_aggregates(db: &Connection) -> Result<()> {
    let flags = FunctionFlags::SQLITE_UTF8 | FunctionFlags::SQLITE_INNOCUOUS;

    db.create_aggregate_function("STDDEV_POP", 1, flags, StatAgg(Stat::StdDevPop))?;
    db.create_aggregate_function("STDDEV_SAMP", 1, flags, StatAgg(Stat::StdDevSamp))?;
    db.create_aggregate_function("VAR_POP", 1, flags, StatAgg(Stat::VarPop))?;
    db.create_aggregate_function("VAR_SAMP", 1, flags, StatAgg(Stat::VarSamp))?;

    db.create_aggregate_function("BIT_AND", 1, flags, BitAggregate(BitOp::And))?;
    db.create_aggregate_function("BIT_OR", 1, flags, BitAggregate(BitOp::Or))?;
    db.create_aggregate_function("BIT_XOR", 1, flags, BitAggregate(BitOp::Xor))?;

    db.create_aggregate_function("ANY_VALUE", 1, flags, AnyValue)?;

    Ok(())
}
