# Quorum under simultaneous multi-node loss — 2026-08-13

The last untested edge from the 2026-08-12 replication matrix
(`chaos-replication-matrix-2026-08-12.md`), and the top open item on the project's list at
the time.

**Result: the boundary holds on both sides, and the fast-fail is real.** Losing the largest
survivable number of followers at the same instant costs nothing measurable; losing one more fails
closed in tens of milliseconds rather than at the ship timeout.

## Why nothing had tested this

`chaos.sh soak` kills one node at a time on a 25 s timer. Each rig node ships to its four peers and
needs `REPLICATION_QUORUM` (2) acks, so one dead follower always left three. **The quorum was
reachable in every run this rig has ever done** — the boundary had never been crossed.

The in-process suite had an `:impossible` test, but it built its dead followers as shippers pointed
at `port: 1`. Nothing ever listened there, so `state.sock` is `nil` and `handle_cast` rejects
synchronously with no socket in the picture. A real node dies the other way round: connected and
healthy, with the rejection coming out of `Shipper.drop/2` on a `tcp_closed`. That path had no
coverage anywhere.

## What was built

**In process** — four tests in `test/fathom/shard/replication_transport_test.exs`, at n=4 q=2:
`n - q` dying at once still commits; `n - q + 1` is `:impossible` in under a second; dying while
*holding* the pushes rejects them through `drop/2`'s waiter loop; and a graceful shutdown refuses
rather than going quiet.

**On the rig** — `chaos.sh quorum-loss`. Both arms run every time and discriminate each other, the
`rpo` pattern: arm 1 must keep committing and arm 2 must fail closed, so a run where arm 2 also
commits reports NOT DISCRIMINATING instead of passing. The kill counts come from
`REPLICATION_QUORUM` rather than being hardcoded. All victims go down in one `docker kill`
invocation, because simultaneity is the whole property. The home node stays alive in both arms —
killing it too would fold failover in, and a failure could then be quorum, or failover, or the
interaction.

## The numbers

Two runs, 5 nodes, home `fathom4`, n=4 followers, q=2. Latency is the slowest of five writes
through the LB, so it includes the LB, the Hrana round trip, and the shell's `curl`.

| | baseline | arm 1 — 2 dead | arm 2 — 3 dead | recovery | destroyed |
|---|---|---|---|---|---|
| run 1 | 40 ms, 5/5 | 44 ms, **5/5 ok** | 46 ms, **0/5 ok** | 5/5 | 0 |
| run 2 | 41 ms, 5/5 | 44 ms, **5/5 ok** | 37 ms, **0/5 ok** | 5/5 | 0 |

Zero isolation leaks, zero acked rows destroyed, in both.

**A survivable simultaneous loss is invisible.** Arm 1 runs at 44 ms against a 40–41 ms baseline —
inside the noise. `Quorum.settle/1` returns at Q, so the two survivors carry the commit and nothing
waits on the corpses.

**A fatal one fails closed in tens of milliseconds**, against a 5 000 ms ship timeout. That is the
design claim `Quorum`'s `:impossible` branch exists for: a commit blocked on an unreachable quorum
is an outage wearing latency.

## The claim was checked, not assumed

A 503 after 5 s and a 503 after 40 ms are both "fail closed", and the scenario's own threshold
(4 000 ms) only separates them if the mechanism is what it looks like. So the ship timeout was
raised **6× to 30 000 ms** on the home node and the fatal arm re-run. If the failure came from the
timeout it would have moved with it:

```
--- ship timeout 30000, whole fleet ---
http=200  COMMITTED               0.006943s
--- ship timeout 30000, quorum unreachable (killed fathom1 fathom2 fathom3) ---
http=200  FILO_NO_QUORUM          0.005959s
http=200  FILO_NO_QUORUM          0.004581s
http=200  FILO_NO_QUORUM          0.004898s
```

**~5 ms at a 30 s timeout.** It is `:impossible`, confirmed by the node's own log
(`replication quorum IMPOSSIBLE for qldisc2: [disconnected: 0, disconnected: 0, disconnected: 0]`).

## Three things worth remembering

**1. The first scenario passed while proving less than it claimed.** Recovery was gated on a
successful write, and the run printed `writable again after 0s` — because with two survivors and
q=2 the shard commits happily while both victims are still booting. Arm 2 could therefore have
killed one live node and two corpses and reported it as a simultaneous three-node loss. The
precondition arm 2 actually needs is that the **home holds a live connection to every follower**,
which `ql_wait_connected` now polls (`Fleet.shippers/0` + `Shipper.connected?/1`). Run 2 is the
trustworthy one; run 1's arm-2 timing agrees, but its simultaneity is unverified.

**2. `FILO_NO_QUORUM` arrives as HTTP 200 with the error inside the pipeline body.** The first
discrimination probe read only `%{http_code}` and reported `200` for writes the server had plainly
refused — the mistake `chaos.sh`'s `hrana` helper checks both halves to prevent, reintroduced in a
throwaway script minutes after quoting the comment that warns about it. The timing conclusion was
unaffected, but **an operator health check that reads only the HTTP status will score a refused
write as a success.**

**3. The in-process fixture could not express node death, and passed anyway.** The first draft
killed followers with `stop_supervised!` and went green. A `Follower` hands every accepted socket to
an **unlinked `Task`** (`follower.ex:235`), so stopping the GenServer leaves the connection alive
and serving; it then answers `:internal` from `handle_push/2`'s rescue. Measured: `internal` rejects,
never a `disconnected` one. That is graceful shutdown — the opposite of a SIGKILLed container. Node
death needs a peer whose own process owns its sockets, which is what the black-hole listener is for.
The accidental discovery is kept as its own test so the two failure modes stay tellable apart. Same
shape as the `Storage.Local` vs S3 lock-etag gap AGENTS.md already records: **a fixture that cannot
express the failure exempts every bug in it.**

## Observed, benign: `already_in_flight` in the reject list

Three of four `IMPOSSIBLE` log lines carry one `already_in_flight` among the `disconnected`s. A
shipper allows one push in flight per shard; when the caller's `collect` gives up it does not tell
the shipper, so a waiter for a dead peer survives until `tcp_closed` clears it — and the next write
in the burst finds it. It is reported as a **reject**, so it subtracts from the quorum exactly like
a disconnect and `:impossible` still settles promptly; it clears on the drop or the reconnect. Not a
defect, but it is why the reject reasons in a burst are mixed rather than uniform.

## Not covered

* **Home + follower dying together** — deliberately out of scope here so a failure means "quorum"
  rather than "quorum, or failover, or their interaction". `failover` and `rpo` own that axis; the
  combination remains untested.
* **Loss during a sustained write load.** Both arms use five-write bursts against an idle shard.
* **Membership changes under loss** — `REPLICATION_MEMBERSHIP=roster` was not exercised.

## Reproduce

```
./chaos.sh build
REPLICATION_ENABLED=true ./chaos.sh up
REPLICATION_ENABLED=true ./chaos.sh quorum-loss
```

The image was verified to carry both 2026-08-12 A2 fixes before the run (`absorb_before_reset` in
`Follower`, `torn` in `FollowerLog`/`Promote`) — `chaos.sh build` was a full cache hit, which is
correct here because the code was on disk before those commits were made, but "the build said Built"
is not the same as "the image has the fix".
