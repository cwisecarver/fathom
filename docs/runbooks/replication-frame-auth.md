# Runbook — enabling A2 replication frame authentication

Closes expert review 2026-08-20 #3 (tier 3). Ships `prev_extent` (#11a) in the same wire revision.

## What you are turning on, and what you are turning off

Before this rollout, **a reachable replication port is equivalent to write access to every shard on
the node**. The listener accepts any TCP connection and applies whatever WAL frames arrive. There
is no credential in the protocol and no peer allowlist; `REPLICATION_BIND_IP` — network
reachability — is the entire control.

After it, a peer that does not hold the fleet secret cannot construct a frame this node will
accept, wherever it connects from.

**What it does not cover.** The signature is over the frame HEADER, not the payload. It stops an
unauthenticated peer completely. It does not attest WAL bytes against an on-path attacker who can
rewrite an already-authenticated peer's traffic. Hashing up to `REPLICATION_MAX_PUSH_BYTES` (1 MiB)
per push on the commit path was judged too expensive for that, and there is no TLS on this port.
If you need that property, the control is a private network, not this flag.

## Prerequisites

- A shared secret every node can read. Either `REPLICATION_HMAC_SECRET`, or `HRANA_TOKEN_SECRET`
  which it derives from — if your fleet already distributes the latter, you need nothing new.
  The configured value is never used as the key directly; it is run through one HMAC with a fixed
  domain-separation label, so a leaked replication key is not the token-signing secret.
- The nodes must agree. A node with a different secret is indistinguishable from an attacker, which
  is the point, and it will be refused.

## The rollout — in this order or it is an outage

The ordering is the same shape as `REPLICATION_LISTEN` before `REPLICATION_ENABLED`, and it fails
the same way if reversed: every shard replicated across the boundary loses quorum, and losing
quorum is a **write outage**, not a degradation.

### Step 1 — deploy the code everywhere

Nothing changes on the wire. Both flags default off, frames go out in the legacy shape, and every
node accepts them. This step is a no-op you can take at any time.

**Verify before continuing:** every node is running the new build. This is the step people skip.

### Step 2 — `REPLICATION_SIGN_FRAMES=true` fleet-wide

Nodes now emit signed frames, which also carry `prev_extent`. Every node has understood that shape
since step 1, so there is no window in which a peer receives something it cannot parse.

Signing is a flag rather than unconditional for exactly this reason: a node that signed the moment
it restarted would be sending frames to peers that had not restarted yet.

**Verify before continuing:**

- No `replication follower closing connection: :unauthenticated` in any node's log.
- Commits still succeed — watch for `FILO_NO_QUORUM`, and remember it arrives as **HTTP 200** with
  the error inside the pipeline body, so a health check reading only the status code will score a
  refused write as a success.
- Every node has the flag. A node still unsigned is fine right now and fatal at step 3.

### Step 3 — `REPLICATION_HMAC_REQUIRED=true` fleet-wide

Unsigned frames are now refused. This is the step that turns the control on.

Roll it node by node and watch the same signals. A node here refuses any peer that skipped step 2.

## Rollback

Reverse order: clear `REPLICATION_HMAC_REQUIRED` everywhere first, then
`REPLICATION_SIGN_FRAMES`. Clearing signing first would leave a node requiring what nobody sends —
which is the boot guard's failure, and it will refuse to start.

## What the boot guard catches

`Fathom.Application.check_replication_frame_auth!/0` raises (not warns, and in every environment,
not just prod) on:

- either flag on with **no key configured** — the node would send frames unsigned while believing
  it signs them, which passes every "is signing on?" check an operator could run;
- `REPLICATION_HMAC_REQUIRED` on with `REPLICATION_SIGN_FRAMES` off — the rollout run backwards.

## There is no permissive mode, deliberately

No accept-if-absent-but-log value. A verification mode that accepts what it cannot verify is not a
control, and the ones that ship as temporary do not get turned off. The transition is carried by
the two booleans and the deploy order.

## `prev_extent` (#11a) rides along

Signed pushes carry how far the generation being discarded actually got. A follower short of that
number was behind at the generation boundary, so the WAL it absorbs at a reset is incomplete — and
it now stays `torn` (un-promotable) instead of clearing the flag and presenting a database that
opens cleanly, passes `quick_check`, and is missing writes.

Two things to know:

- **It only reports a gap it has evidence for.** The number is the high-water mark across followers
  in that generation, so a single-follower fleet never fires it. `prev_extent: 0` from an
  un-upgraded peer means "no statement" and clears `torn` as before — otherwise a rolling upgrade
  would mark every replica in the fleet un-promotable.
- **The primary-side half is NOT shipped.** Sending a full seed instead of a reset when a follower
  is known short is deferred until the rig's seed rate is measured. Seeds are the expensive
  operation A2 exists to avoid, and a rule that turns "behind at a boundary" into "ship the whole
  database" can convert a lag spike into a seed storm.

Expect `absorbed a SHORT WAL before a reset` warnings after step 2. They are the feature working;
each names a replica that will not be promoted until it is re-seeded.
