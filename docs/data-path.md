# Fathom — the shard data path (how a request becomes a served shard)

> Status: **BUILT.** The single-node lifecycle the distributed stories
> ([single-writer](single-writer.md), [warm-standby](warm-standby.md),
> [rebalancing](rebalancing.md), [migration](migration.md)) all sit on top of: how an unchanged
> libSQL client's query reaches a per-tenant SQLite file and how that file is opened, served, and
> put back.

## The problem it solves

A libSQL client (django-libsql over WebSocket, an SDK over HTTP) sends **opaque SQL** for **its**
tenant. Fathom has to bind that connection to the right per-tenant SQLite file, keep concurrent
tenants' transactions isolated, and manage the file's whole lifecycle — open it on demand, serve
it, flush it back, and drop it when nobody's using it — so that an idle tenant costs a node
essentially nothing and millions of shards can share it over time.

## The request flow, end to end

```
libSQL client ──Hrana (HTTP /v2/pipeline or WebSocket)──▶ Filo.Plug / Filo.Socket
      │  the wire protocol (framing, hello, streams) — the Filo library
      ▼
Fathom.ShardExecutor  (Filo's Executor callback)
   • open(shard_id)  — bind this stream to a shard (shard_id from the request; see admission.md)
   • Fathom.Shards.checkout(shard_id)  — find-or-start the coordinator, get a file path
   • Fathom.Shard.Connection.open(path)  — one exqlite connection for this stream
      ▼
   • execute(handle, stmt)  — run the client's SQL on that connection → rows back
```

Four modules, four jobs: **Filo** speaks the wire; **`ShardExecutor`** is the callback that maps a
stream to a shard and runs its SQL; **`Fathom.Shards`** is the find-or-start router; **`Fathom.Shard`**
is the per-shard coordinator that owns the file; **`Fathom.Shard.Connection`** is the one exqlite
connection that stream uses.

## One connection per stream — isolation for free

Each Hrana stream gets its **own** `Connection` (an exqlite connection to the shard file, opened at
stream start, closed at stream end). WAL gives every connection its own write transaction and read
snapshot, so two concurrent streams on the same shard are naturally isolated — and concurrent
writers serialize on `busy_timeout`, not on a fathom-level lock. Nothing is shared between streams
except the file.

## The coordinator owns the file lifecycle

`Fathom.Shard` is one GenServer per **active** shard (started on demand by `Fathom.Shards` via
`ShardRegistry` + `ShardSupervisor`). It owns the file:

- **Cold start** — it **pulls** the shard from `Fathom.Shard.Storage` (only on a genuine cold start;
  a **present local file is authoritative**, so a node with un-flushed writes never re-pulls over
  them). It also acquires the lease (see [single-writer](single-writer.md)).
- **Tracks its connections by monitoring the stream process.** The coordinator is the connection's
  **owner** (Filo's `Executor.owner/1` seam): each stream holding a handle monitors the coordinator
  and tears down — closing its exqlite connection — if the coordinator dies. This is what prevents
  an **orphaned writer** (finding #8): a stream whose coordinator vanished must not keep writing
  WAL frames that a successor's flush-and-drop would silently lose.
- **On idle** — when zero connections have been checked out for the idle window, it **checkpoints
  the WAL, flushes the file to storage, drops the local copy, and stops.** So an idle tenant leaves
  no coordinator, no connection, no fds — just a file in storage, cold-openable again on the next
  request.

## The dirty flag — flushes track writes, not opens

Every flush is **write-gated**. `ShardExecutor` classifies each statement (`wrote?/2` — a change
count on a non-SELECT) and bumps `Fathom.Shard.WriteCounter` (lock-free) on a write. A shard that
was only read (or never touched) is **clean** and **skips the upload entirely** — so durability
PUTs scale with *writes*, not with how many shards a node happened to open. (The classifier errs
toward marking dirty — an extra flush, never lost data.)

## How it connects

This lifecycle is the substrate the rest ride on: the **lease + fence** ([single-writer](single-writer.md))
guards the pull and the flush so only one node writes the file; the **flush** is the
[durability](durability.md) mechanism; a **survivor** promotes a [warm](warm-standby.md) copy on the
same cold-open path; and a [rebalance](rebalancing.md) handoff is just a *drain* of this coordinator
(flush + drop + release) on the source so a target can cold-open it. Which shard a request resolves
to, and whether a *novel* shard is admitted at all, is [admission](admission.md).

## One-line summary

Each Hrana stream binds to a shard via `ShardExecutor` → `Shards.checkout` → a dedicated exqlite
`Connection`, while a per-shard coordinator (`Fathom.Shard`) pulls the file on cold start, monitors
its streams so an orphaned writer can't be lost, marks the shard dirty only on a real write, and on
idle checkpoints + flushes + drops it — so a served shard is a live GenServer and an idle one is
just a file in storage.
