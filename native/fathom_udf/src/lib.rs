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
pub mod datetime;
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

/// Returning `Ok(false)` makes this a per-connection extension rather than a persistent one:
/// fathom loads it explicitly on every connection it opens, and a persistent auto-extension would
/// also attach to connections fathom did not intend (and outlive the disable step above).
fn register_all(db: Connection) -> Result<bool> {
    // Pure and deterministic: safe in views, expression indexes and CHECK constraints.
    let det = FunctionFlags::SQLITE_UTF8
        | FunctionFlags::SQLITE_DETERMINISTIC
        | FunctionFlags::SQLITE_INNOCUOUS;
    // Pure but NOT deterministic — SQLite will refuse to index these, which is correct.
    let nondet = FunctionFlags::SQLITE_UTF8 | FunctionFlags::SQLITE_INNOCUOUS;

    register_datetime(&db, det)?;
    register_text(&db, det)?;
    register_hashes(&db, det)?;
    register_misc(&db, det, nondet)?;
    register_aggregates(&db)?;

    // The Phase 2 A2 frame seam. NOT a UDF — it installs a commit hook, and because
    // `sqlite3_wal_hook` and `wal_autocheckpoint` share one slot, it also takes over the
    // checkpointing it displaces. See src/wal.rs; removing this line silently disables BOTH the
    // hook and (harmlessly) restores SQLite's own autocheckpoint.
    wal::install(&db)?;

    Ok(false)
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

fn register_text(db: &Connection, flags: FunctionFlags) -> Result<()> {
    db.create_scalar_function("LPAD", 3, flags, |ctx| {
        let (Some(text), Some(len), Some(fill)) =
            (opt_text(ctx, 0)?, opt_i64(ctx, 1), opt_text(ctx, 2)?)
        else {
            return Ok(None);
        };
        Ok(Some(scalars::lpad(text, len, fill)))
    })?;

    db.create_scalar_function("RPAD", 3, flags, |ctx| {
        let (Some(text), Some(len), Some(fill)) =
            (opt_text(ctx, 0)?, opt_i64(ctx, 1), opt_text(ctx, 2)?)
        else {
            return Ok(None);
        };
        Ok(Some(scalars::rpad(text, len, fill)))
    })?;

    db.create_scalar_function("REPEAT", 2, flags, |ctx| {
        let (Some(text), Some(count)) = (opt_text(ctx, 0)?, opt_i64(ctx, 1)) else {
            return Ok(None);
        };
        Ok(Some(scalars::repeat(text, count)))
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

    Ok(())
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
