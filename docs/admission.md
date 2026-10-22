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
saturated and returns **503** (`[:fathom,:shards,:at_capacity]`). Among the LRU-coldest candidates a
**clean** one (nothing written since its last flush, an ETS-only check) is evicted before a dirty
one, so the admitting request rarely waits on someone else's flush + PUT (expert review 2026-10-17
#6); with no watermark data (`Fathom.Admin` off) it is plain LRU. Set `:evict_idle_at_capacity` false
for a **hard cap** (503 at the limit). Either way the open count stays bounded by the cap.

**Pre-eviction headroom** (perf review 2026-10-21 #14). That inline eviction makes the admitting
request wait for the victim's whole drop, ending in the lock release (an S3 DELETE round trip),
before its own cold open: measured in-process with 25 ms injected on the release and the pull,
an admitted open at the cap took p50 52.8 ms vs 26.9 ms uncapped. So an admission that leaves the
node within `:pre_evict_headroom` of the cap (default `:auto` = 1% of the cap, at least 16) starts
one background evictor that drops LRU idle shards, using the same candidates and the same
evict-if-idle primitive, until the node is back below `cap - headroom`. With it, the same probe
measured p50 26.3 ms. The inline path stays as the fallback for bursts that outrun it. The trade:
idle tenants drop slightly before strictly necessary. It is off for a cap that cannot hold twice
the headroom (≤ 32 with the minimum), or with `:pre_evict_headroom` 0. It reports
`[:fathom,:shards,:pre_evicted]`.

Under **concurrent** admissions that alone bought nothing (expert review 2026-10-21 r7 #7): with
K=16 admissions in flight the same probe measured p50 52-54 ms with the pre-evictor on, and half
the admissions still evicted inline. Three changes, measured together at p50 29-30 ms (uncapped
27-28) and 3-16 inline evictions of 160:
- **Slot hand-off.** A pre-evictor drain of a clean victim *offers* the slot it will free. An
  admission at the cap first *adopts* one offer (a CAS, so one slot serves one admission) and waits
  for that victim's exit within its 2 s budget, instead of draining a victim of its own. A slot
  that does not free (the victim took a connection) is reported at once and the admission walks
  the LRU as before. An admission at the cap also wakes the pre-evictor.
- **Parallelism scales with the deficit**: up to 32 victims per pass (8 before), of which at most 8
  dirty, and below the FlushGate cap, since each dirty victim flushes before it drops.
- **A claimed shard is not a candidate**: the LRU probe skips shards another process is already
  evicting without counting them against its 16 probes, and a walk that met only such claims
  adopts again instead of refusing `node_at_capacity` with idle shards right behind them.

**One admission per cold shard** (expert review 2026-10-21 r7 #6). Concurrent first requests for
the same cold shard share ONE admission (`Fathom.Shards.AdmissionFlight`): the first leads, the
rest wait for its result and take it as their own, refusal included. Before, each ran the whole
admission, so K requests for one cold tenant at the cap evicted K tenants (measured: 16 for K=16)
and spent K novel-rate tokens. A follower that waits past 15 s admits by itself. Victims are
claimed (`Lru.claim_eviction/1`, rv6 F10), so two admitters never count one freed slot twice.

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

Everything here guards the **data-path** front door (the Hrana port on `:8080`). The **control
plane** (`/admin` + the `/api` provisioning surface on `:4000`) has its own, separate abuse throttles
— an admin-auth brute-force lockout and an `/api` rate limit (`Fathom.RateLimiter`) — so
`:max_open_shards` / `NovelLimiter` are not what protects that surface. See [auth](auth.md).

## One-line summary

A request resolves to a shard by Host subdomain through one fail-closed function (dev-only `?db=`
override, no-default ⇒ 400), and a novel shard is admitted only past a **double gate** — a soft
`:max_open_shards` cap that evicts the LRU *idle* shard (never a busy one; 503 only when genuinely
saturated) and a `NovelLimiter` that rate-limits *unseen* ids (429) without ever throttling known
tenants — with boot guards that keep the replay template from becoming a poisoning target.
