# Plan — fixing audit #3: read-only token scope is unenforced over HTTP

**Status: IMPLEMENTED 2026-07-18 (Option B).** Shipped in filo `df685a9` (`Filo.Executor.open/2`
threading the authorize context through both transports) + fathom `cd62267` (`HranaAuth.authorize/2`
returns `{:ok, scope}`; `ShardExecutor.open/2` rides it; the `stash_scope`/`take_scope` process-dict
channel is deleted). Real-transport regression tests (HTTP pipeline + a 2-stream WS hello) close the
I3 gap. The options/analysis below are retained for reference.

## Decision

- **Option B** (Filo threads the verified auth context to `open`). A Filo change is **approved**
  (fathom + Filo are co-developed sibling repos).
- **No interim (C/D skipped):** the `ro`-scope feature is **not in use in prod yet** (opt-in, needs
  `HRANA_AUTH=required`), so there's nothing to fail-closed or WS-patch first — go straight to the
  root-cause fix.
- Open sub-questions for the implementer: verify-vs-trust at `open` (Q4 below — likely *trust*,
  since `authorize` already verified), and the exact Filo threading shape (new `open/2` vs a wrapped
  `{open_arg, context}` — prefer `open/2` for clarity). fathom is (as far as we know) the only
  `Filo.Executor` consumer, so the callback change's blast radius is contained.

## Build outline (Option B)

**Filo (`../filo`):**
1. Change the `authorize` callback contract: return `{:ok, context}` (host-opaque term) instead of
   `:ok`; treat a bare `:ok` as `{:ok, nil}` for back-compat.
2. Thread `context` into the executor's open — add `Filo.Executor.open/2` (preferred) and call it
   with the authorize context, falling back to `open/1` when a host doesn't define `/2`.
3. Wire it through **both** transports: `plug.ex` (the streamed path via `Streams.create` →
   `Filo.Stream.init` → `executor.open`, AND the direct `plug.ex:485` path) and `socket.ex` (store
   the hello-time context in socket state, pass it to every `open_stream`'s open).
4. Update Filo's own tests for the new contract.

**fathom:**
1. `HranaAuth.authorize/2`: return `{:ok, scope}` (the decoded `ro`/`rw`) instead of `:ok` +
   `Process.put`. Delete `stash_scope`/`take_scope`/`@scope_key` — the whole process-dict channel.
2. `ShardExecutor.open/2`: receive the scope context, ride it in the connection handle exactly where
   `take_scope` fed it today; enforce `FILO_READONLY` unchanged.
3. Update `application.ex` wiring if the callback shape shifts.

**Tests (the I3 gap — mandatory, real transport):**
1. HTTP: drive `Filo.Plug` with a `ro` token, assert a write → `403 FILO_READONLY`.
2. WS: open `open_stream(1)` + `open_stream(2)` on one `hello`-authorized `ro` connection, assert
   BOTH refuse writes.
3. Keep/adapt `read_only_scope_test.exs`; remove its same-process shortcut.

**Gates:** `mix precommit` on fathom + Filo's suite; a security read of the new contract; the
no-oracle / fenced posture is unchanged. Not a benched hot path, but the open path is adjacent —
sanity-check cold-open isn't affected.

---

*Options analysis (retained for reference):*

## The bug (confirmed in code + Filo source)

`Fathom.HranaAuth.verify/2` decodes the token's `ro`/`rw` scope and stashes it in the **calling
process's** dictionary (`Process.put(@scope_key, scope)`, `hrana_auth.ex:90-92`).
`Fathom.ShardExecutor.open/1` reads it back with `Process.delete(@scope_key) || :rw`
(`take_scope`, consuming). That only works if `authorize` and `open` run in the **same process**.

- **HTTP (`libsql-experimental` / HTTP SDKs) — BROKEN, Critical.** Filo runs `authorize(open_arg,
  token)` in the Plug request process, then opens a fresh stream via
  `DynamicSupervisor.start_child({Filo.Stream, ...})` whose `init/1` calls `executor.open(open_arg)`
  **in the new Stream GenServer process** (`../filo/lib/filo/stream.ex:98`). Different process ⇒ the
  process dict is empty ⇒ `take_scope` returns `:rw`. **A `ro` token gets full read-write on every
  HTTP stream. `403 FILO_READONLY` never fires.**
  (There is a second, non-streamed HTTP path — `../filo/lib/filo/plug.ex:485` — that opens in the
  plug process, where the scope *does* survive; but the streamed path is the primary one.)
- **WebSocket (`django-libsql`) — half-broken, High.** The socket keeps `authorize` and every
  `open_stream` in ONE process, but `authorize` runs **once at `hello`** (`socket.ex:139`) and
  `take_scope` **consumes** the value. So the *first* stream on the connection is correctly `ro`;
  every subsequent stream finds the dict empty and defaults `:rw` — an ro→rw escalation within one
  authenticated session.
- **Test gap (I3).** `read_only_scope_test.exs` calls `authorize` + `open` in the *same test
  process*, so it never exercises the HTTP cross-process path or a 2nd WS stream. The bug shipped
  and stayed shipped because the only coverage sidesteps the break.

## Why it's not a one-liner: the token-delivery asymmetry

Filo hands `open/1` exactly one thing — `open_arg` — and calls both `authorize(open_arg, token)` and
`open(open_arg)` with the **same** `open_arg`. `authorize` returns `:ok | {:error}`; it can't enrich
`open_arg`. fathom wires `open_arg: &ShardExecutor.shard_from_conn/1` (→ the shard id).

- **HTTP:** `open_arg` is computed **per request from `conn`**, which carries the bearer token in the
  `Authorization` header. So the token/scope **can** be baked into `open_arg`.
- **WebSocket:** ws clients send **no upgrade auth header** — the token arrives in the `hello`
  message. `open_arg` is computed **once from the upgrade conn** (no token) and is **static** for the
  whole connection. So the token **cannot** be baked into `open_arg`; the scope has to be captured at
  `authorize` (hello) time inside the socket process.

That asymmetry is the whole difficulty: HTTP wants scope-in-open_arg, WS wants scope-at-authorize.

## Options

### Option A — fathom-side, two-pronged (no Filo change)

- **HTTP:** change `open_arg` to a richer term that carries the token (or the pre-decoded scope)
  read from `conn`; `open/1` derives the scope from it. `authorize/2` must also accept the richer
  `open_arg` (it's called with the same value).
- **WS:** change `take_scope` from consuming (`Process.delete`) to non-consuming (`Process.get`) —
  WS keeps one process for the connection, so every stream reads the scope stashed at hello.
- **Distinguish the two at `open`:** HTTP `open_arg` carries an authoritative scope; WS `open_arg`
  (from a token-less upgrade conn) does not, so `open` must fall back to the process-dict for WS. A
  sentinel (`open_arg` scope = `:none` ⇒ use `take_scope`) tells them apart.

**Pluses**
- ✅ Contained to fathom; ships as one PR, no cross-repo coordination.
- ✅ The WS half is a genuinely tiny, obviously-correct change (non-consuming read).
- ✅ No change to Filo's public callback contract.

**Minuses**
- ❌ Intricate and security-critical: the sentinel/fallback split must be exactly right, or the
  bypass silently persists. Two mechanisms (open_arg for HTTP, process-dict for WS) to keep correct.
- ❌ The HTTP path decodes scope from the token **before** `authorize` verifies it. It's safe *because*
  `authorize` runs after and rejects an invalid token (so `open` only ever runs on a verified token),
  but that's a subtle ordering argument reviewers must re-derive.
- ❌ Keeps the process-dict side-channel for WS — correct within one process, but the same fragile
  pattern that caused this bug.

### Option B — Filo change: thread the verified token/scope to `open` (unified)

Make Filo pass `authorize`'s result to `open`. Concretely, `authorize` returns `{:ok, context}`
(instead of `:ok`) and Filo threads `context` into `open` (a new `open/2`, or `open({open_arg,
context})`), for **both** transports. `open/1` derives scope from the token/context uniformly. The
process dict disappears entirely.

**Pluses**
- ✅ Root-cause fix: authorize's authenticated output *flows* to open, which is the design the
  process-dict hack was faking. Eliminates the whole "scope via side-channel" class.
- ✅ **Unified** — no HTTP/WS asymmetry, no sentinel, no decode-before-verify ordering argument.
- ✅ Most robust long-term; a general, clean Filo capability (any host wanting per-connection auth
  context benefits, not just fathom).

**Minuses**
- ❌ Touches Filo (`{:filo, path: "../filo"}`) — a callback-contract change affecting Filo's plug,
  socket, tests, and any other host. Two-repo change.
- ❌ Bigger blast radius / more review surface than a fathom-only patch.
- ❌ Slightly slower to land (Filo API + fathom adoption + tests on both sides).

### Option C — WS fix now, flag HTTP

Ship only the non-consuming `take_scope` (closes the WS escalation, the lesser High) now; leave the
HTTP Critical for the A-vs-B decision.

**Pluses**
- ✅ A safe, tiny, correct win immediately; shrinks the exposed surface.
- ✅ Buys time to decide A vs B without leaving *everything* broken.

**Minuses**
- ❌ Leaves the **Critical** (HTTP) open — the bigger hole.
- ❌ A partial security fix can read as "handled" and lower urgency on the real one.

### Option D — fail closed on `ro` until the real fix lands (interim mitigation)

Until A or B ships, refuse rather than silently grant: if a `ro`-scoped token is presented on a path
where the scope can't be enforced, **deny the stream** (or stop minting ro tokens) instead of
granting `rw`.

**Pluses**
- ✅ Removes the **false** security guarantee immediately — no silent `rw` on an ro token.
- ✅ Small, reversible, honest posture while the real fix is designed.

**Minuses**
- ❌ Breaks the ro feature for legitimate users in the interim (they get denied, not read-only).
- ❌ A stopgap, not a fix; needs detection of "this is an ro token" at a deny-capable point.

## Cross-cutting (applies to whichever option)

- **Real-transport tests are mandatory (fixes I3).** Whatever we ship needs (a) an HTTP integration
  test driving `Filo.Plug` with a `ro` token that asserts a write is refused, and (b) a WS test
  opening `open_stream(1)` + `open_stream(2)` on one `hello`-authorized connection with a `ro` token,
  asserting the 2nd stream is *also* restricted. Building these is part of the work, not an add-on.
- **Verify-vs-trust at `open`.** Does `open` re-verify the token (belt-and-suspenders) or trust that
  `authorize` already did? Re-decoding the scope from an already-authorized token is safe; re-verifying
  the signature is defensive but costs a second verify per stream.

## Recommendation

**B, with the C/D interim** — do the root-cause Filo change (verified context flows to `open`), because
it removes the process-dict side-channel entirely and kills the HTTP/WS asymmetry that makes A
error-prone on a security path; and in the meantime take the WS one-liner (C) plus fail-closed on ro
over HTTP (D) so nothing silently grants `rw` while B is built. If a Filo change is off the table,
A is viable but demands very careful review of the sentinel/fallback and the decode-before-verify
ordering.

## Questions for you

1. **Is a Filo change acceptable?** fathom and Filo are sibling repos you control — is Filo meant to
   stay a clean general library (so a small, general `authorize → open` context feature is fine), or
   do you want to avoid touching it (⇒ Option A)? This is the A-vs-B fork.
2. **Is the ro-scope feature actually in use yet?** `ro` tokens are opt-in (review #24) and only
   matter with `HRANA_AUTH=required`. If nothing relies on them in prod today, urgency drops and B
   (do it right) is affordable with no interim. If they *are* relied on, we want C+D now.
3. **Interim posture:** ship the WS one-liner (C) + fail-closed on ro-over-HTTP (D) immediately, or
   hold everything for one complete PR?
4. **Verify-vs-trust at `open`:** re-verify the token in `open`, or trust `authorize` and only decode
   the scope claim?
5. **Any other Filo hosts?** Is fathom the only `Filo.Executor` consumer, or would a Filo callback
   change need to consider other callers?
