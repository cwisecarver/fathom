# Fathom — shard selection & admission control (the front door)

> Status: **BUILT.** How a request resolves to the *right* shard (multi-tenant isolation) and how a
> node decides whether to admit a *novel* shard at all (the fd cliff, abuse, and failing closed).
> This is the layer in front of the [data path](data-path.md); the safety findings it encodes are
> release blockers, not niceties.

## The problem it solves

Two dangers live at the front door. **Wrong routing** is a cross-tenant leak — a query for tenant A
must *never* resolve to tenant B's data — so shard resolution has to be single-sourced and
fail-closed. **Unbounded admission** is a self-inflicted outage — a node holds each open shard as a
connection (~3 fds) and a coordinator, so millions of tenants must not translate into millions of
open shards on one box, and a flood of never-before-seen ids must not be able to mint shards without
limit.

## Routing — Host subdomain, single-sourced, fail-closed

`ShardExecutor.shard_from_conn/1` resolves a request to a shard in one place (never ad-hoc string
surgery scattered around):

```
shard_from_host(conn.host)        # e.g. acme.fathom.example → "acme" — the prod path
  || maybe_override(conn)         # ?db= / x-fathom-shard — DEV-ONLY, gated :allow_shard_override
  || default_shard()              # the configured fallback
  || nil                          # → fail closed (a 400), never a shared shard
```

- **Host subdomain is the prod path.** Case-normalized via `Fathom.ShardId.cast` so `ACME` and
  `acme` are one shard (finding #19) — a case split would be a silent second tenant.
- **`?db=` / `x-fathom-shard` are dev-only.** They're an *unauthenticated* shard-selection
  primitive — a hostless request with `?db=<victim>` could open any shard — so they're gated behind
  `:allow_shard_override`, **off in prod** (finding #4). Off, a hostless request falls through to
  the configured default, never an attacker-named shard.
- **No default in prod ⇒ fail closed with a 400** (finding #26). Commingling unresolved requests
  into one shared shard is a cross-tenant leak; refusing is the safe answer. Dev/test set
  `:default_shard` to `"demo"`.

Any change to this resolution ships with a **cross-shard-isolation test** (the shard-isolation
gate) — a leak is a release blocker.

## Admission — the double gate on novel shards

A shard the node has never held is admitted only through **two independent gates**:

**1. `:max_open_shards` — cap how many shards a node holds open (the fd-cliff protection).** Off by
default (`:infinity`); an operator sets it from `kern.maxfilesperproc / ~3`. It's a **soft cap by
default**: at the limit a new open **evicts the least-recently-used *idle* shard** (`Fathom.Shards.Lru`
tracks recency lock-free; `drain/2` flushes + drops + releases the lease) rather than 503ing —
because a shard's LB home is *one* node, so a refused open means that tenant is **down**, while an
idle shard is bottomless-backed and just cold-re-opens elsewhere/later. A **busy** shard (checked-out
connections) is **never** evicted; if the LRU-coldest handful are all busy the node is genuinely
saturated and returns **503** (`[:fathom,:shards,:at_capacity]`). Set `:evict_idle_at_capacity` false
for a **hard cap** (503 at the limit). Either way the open count stays bounded by the cap.

**2. `Fathom.Shards.NovelLimiter` — rate-limit how fast *unseen* ids mint new shards.** Gated
`:novel_shard_rate` (off by default; `NOVEL_SHARD_RATE` in prod). Over budget ⇒ **429**, refused
**before** any novel work runs (the S3 lock PUT + the Postgres directory row). Crucially, **existing
shards are never limited** — a shard with a directory row, a local file, or a running coordinator is
known, so the limiter only throttles genuinely-*new* ids; and the directory check **fails open** on a
Postgres outage (an outage must not lock out known tenants).

## Template capture is a poisoning vector — two boot guards

`:template_shard_id` (off in prod by default) records a shard's SQL for fleet-wide replay, so if it
were reachable anonymously an attacker could poison the template every shard is built from.
`Fathom.Application` refuses two dangerous configs **at prod boot**:

- **`:default_shard` must not equal `:template_shard_id`** (`check_template_default!`, finding #17) —
  else an unresolved request would land on, and could write to, the template.
- **`:template_shard_id` set in prod with `:hrana_auth` disabled** is refused — a prod template must
  be auth-gated.

## How it connects

Once a request resolves and is admitted, it enters the [data path](data-path.md) (checkout → a
per-stream connection). The eviction path is the same **drain** (flush + drop + release the lease)
that a [rebalance](rebalancing.md) uses, and it's safe for the same reason — the
[lease](single-writer.md) means dropping an idle shard can't lose a live writer.

## One-line summary

A request resolves to a shard by Host subdomain through one fail-closed function (dev-only `?db=`
override, no-default ⇒ 400), and a novel shard is admitted only past a **double gate** — a soft
`:max_open_shards` cap that evicts the LRU *idle* shard (never a busy one; 503 only when genuinely
saturated) and a `NovelLimiter` that rate-limits *unseen* ids (429) without ever throttling known
tenants — with boot guards that keep the replay template from becoming a poisoning target.
