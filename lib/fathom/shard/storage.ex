defmodule Fathom.Shard.Storage do
  @moduledoc """
  Durable, bottomless storage for shard database files. The `Fathom.Shard`
  coordinator **pulls** a shard's SQLite file from storage when it wakes and
  **flushes** it back (then drops the local copy) when the shard goes idle, so a
  node only holds the working set on local disk.

  This is a behaviour with a pluggable backend, selected by config:

      config :fathom, :shard_storage, Fathom.Shard.Storage.Local   # default

  `Fathom.Shard.Storage.Local` (a filesystem object store, the default) is what
  dev and tests use; `Fathom.Shard.Storage.S3` is the production backend.

  Both callbacks key by `shard_id`. `pull/2` writes the remote object to
  `local_path`; if the shard has no object yet (a brand-new shard) it returns
  `:ok` without creating a file. `flush/2` uploads `local_path` to the object for
  `shard_id`.

  ## Cross-node single-writer leasing

  A shard's file is one durable object, but SQLite/WAL only serializes writers on
  one shared file on one machine — nothing in `pull`/`flush` stops two nodes from
  each pulling their own copy, accepting writes, and flushing, with the last flush
  silently clobbering the other's. So storage also owns a per-shard **lease**: the
  `Fathom.Shard` coordinator acquires it before pulling, renews it for the shard's
  lifetime, and releases it on flush. A monotonic **epoch** stamped on the lease is
  the fencing token — a node that loses its lease (GC pause, partition) self-fences
  and never flushes over a newer owner.

  The lease is a `t:lease/0` (`owner`, `epoch`, `expires_at_ms`). `acquire_lease/3`
  returns `{:error, {:held, owner}}` when another owner holds a live lease;
  `renew_lease/3` returns `{:error, :superseded}` once the lease has been taken
  over. Backends MUST enforce mutual exclusion with a conditional/atomic write
  (the `Local` backend uses an `O_EXCL` lock file; `S3` uses conditional PUTs) and
  MUST **fail closed** on a transient lookup error — never fall back to an
  unconditional overwrite, which silently steals a live owner's lease.

  ## Node heartbeat (liveness, separate from ownership)

  Renewing every shard's lock individually is `active_shards / (ttl/3)` writes per
  second per node — a PUT storm at scale (millions of shards ⇒ ~100k PUT/s/node,
  see `mix fathom.scale --lease-rps`). So **liveness is a single per-node object**,
  not per-shard: a node renews one `t:heartbeat/0` at `heartbeat/<owner>` via
  `renew_heartbeat/2` (driven by `Fathom.Shard.Heartbeat`), and a shard lock no
  longer carries a meaningful per-lock TTL — its owner is *live* iff that owner's
  heartbeat is fresh. So `acquire_lease/3`'s steal decision reads the current
  owner's heartbeat (not `lock.expires_at_ms`): steal only if the heartbeat is
  missing or expired **past `steal_margin_ms/0`** (a clock-skew guard — see that
  function), and **fail closed** on a heartbeat read error (don't steal on
  uncertainty). `check_lease/2` is the read-only fence the coordinator uses before
  a flush to confirm it still owns the shard (it pairs with the holder's
  locally-confirmed heartbeat validity — see `Fathom.Shard.Heartbeat`). The lock's
  `expires_at_ms` is retained for wire/decode compatibility and debugging, but is
  no longer the liveness signal.
  """

  @typedoc """
  A shard lease. `owner` identifies the holding node, `epoch` is the monotonic
  fencing token (bumped each time an expired lease is stolen), and `expires_at_ms`
  is the wall-clock expiry in `System.system_time(:millisecond)`.

  `lock_etag` is **optional and backend-carried**: the S3 backend stamps every lease it returns
  with the etag of the lock object it just wrote (`put_lock_etag/2`), because release is a
  conditional `DELETE … If-Match: <that etag>` and `resolve_stale_release/4` — which lives here
  rather than in a backend precisely so every etag-carrying backend decides identically — reads it.
  `Storage.Local` does not set it.

  It was **missing from this type entirely** until 2026-08-14, which made the declaration a CLOSED
  three-key map that every lease from the production backend violated: dialyzer reported the call
  into `resolve_stale_release/4` as one that "breaks the contract" and `S3.resolve_412_release/2`
  as a function that can never return. Nothing was broken at runtime — the maps carry the key
  regardless — but the declared shape omitted the fencing token that the 2026-08-04 stuck-lease fix
  is built on, so the type was actively misleading about the one field that matters most on the
  release path.
  """
  @type lease :: %{
          :owner => String.t(),
          :epoch => non_neg_integer(),
          :expires_at_ms => integer(),
          optional(:lock_etag) => String.t()
        }

  @typedoc """
  How much of a shard's history a copy of it contains — Phase 2 A2.

  Ordered lexicographically by `{epoch, wal_gen, offset}`: `wal_gen` increases on every checkpoint
  and `offset` advances within a generation.

  Exists so a failover can order a node's local **replica** against the **stored object**, which
  nothing else can do — an etag is a content hash with no ordering, the lock carries the holder's
  epoch rather than the object's, and comparing wall-clock across nodes is unsound. The two are
  independent lineages of the same shard, and picking the older one silently loses acknowledged
  writes.

  ## `epoch` IS NOT MONOTONIC ACROSS A RELEASE, and this doc used to claim it was

  This said "the lease `epoch` increases monotonically per shard (it is already the fencing
  token)". That is false, and expert review 2026-08-20 #8 traced two live consequences to it. The
  epoch is a **lock generation**, meaningful only while the lock object exists: `release_lease`
  DELETES that object, and the next `acquire_lease` takes the optimistic `create_lock` path, which
  starts at **1**. So the epoch climbs on crash-steals and falls back to 1 on the next clean
  idle-drop, drain or rebalance handoff.

  What that breaks, given two consumers read it as a shard-history counter spanning a release:

    * `FollowerLog.decide/2` rejects `pushed < epoch` as `:stale_epoch` — permanently, because
      that reason is in `Session`'s `@settled_rejects` and `start_seeds/3` only seeds on
      `:unknown_shard`. Steal to epoch 2, idle out, re-open at epoch 1, and every follower refuses
      every push for that tenant until its `Follower` process restarts.
    * `Promote.fresher?/2` can pick a replica stranded at a HIGHER epoch from a previous ownership
      generation over an object flushed under a lower post-reset one — the older lineage.

  **Not yet fixed** — the candidate fixes trade against cold-open latency and the storage contract,
  so the decision is written up in the review's progress file rather than guessed at. Until then:
  treat `epoch` as ordering-within-one-ownership-generation only, and do not add a third consumer
  that assumes it counts a shard's history.
  """
  @type position :: %{
          :epoch => non_neg_integer(),
          :wal_gen => non_neg_integer(),
          :offset => non_neg_integer(),
          # The WAL's IDENTITY (expert review 2026-08-26 #2). Optional: absent on a stamp written
          # before that review, and on any stamp whose WAL could not be read. Absent means
          # "unknown", and `Promote.fresher?/2` refuses to rank against an unknown rather than
          # guessing — `wal_gen` alone is NOT an ordering, because it restarts at 0 whenever
          # SQLite recreates the `-wal`.
          optional(:salt1) => non_neg_integer()
        }

  @typedoc """
  A node's liveness heartbeat. `owner` identifies the node, `expires_at_ms` is the
  wall-clock expiry in `System.system_time(:millisecond)`. A shard whose lock names
  `owner` is live iff `owner`'s heartbeat is fresh.
  """
  @type heartbeat :: %{owner: String.t(), expires_at_ms: integer()}

  # Pull the stored object to `local_path`.
  #
  #   * `{:ok, etag}`     — BYTES WERE WRITTEN to `local_path`; `etag` fences the first flush.
  #   * `{:absent, etag}` — NOTHING was written (no object, or only a steal sentinel). `etag`
  #     is still the fence for a first flush (nil when there is no object at all).
  #
  # The two are separated because a caller cannot tell them apart from the return value
  # otherwise, and `{:ok, _}` invites `Connection.open/1` on a path that does not exist —
  # which CREATES a valid, quick_check-clean, EMPTY database and hands it back as if it were
  # the tenant's data (expert review 2026-08-01 #24).
  #
  # That is not theoretical: on the chaos rig it left three tenants serving an empty database
  # with `no such table`, their real rows preserved only in `.forked.*` quarantine files. The
  # S3 backend previously collapsed its steal sentinel to `{:ok, etag}` while writing no file,
  # so every "pull to a temp, then open it" consumer — reconcile, snapshots, export, the
  # restore drill, `mix fathom.shard` — fabricated an empty database from it.
  #
  # A backend must NEVER return `{:ok, _}` without having written bytes.
  @callback pull(shard_id :: String.t(), local_path :: Path.t()) ::
              {:ok, String.t()} | {:absent, String.t() | nil} | {:error, term()}

  # Unconditional write of `local_path` to the shard's object. For callers that don't fence
  # the live object (migration version copies, benchmarks); the coordinator uses `flush/3`.
  @callback flush(shard_id :: String.t(), local_path :: Path.t()) :: :ok | {:error, term()}

  # Fenced flush: write `local_path` only if the stored object still matches `expected_etag`
  # (`If-Match`), or — for a brand-new shard (`expected_etag == nil`) — only if no object
  # exists yet (`If-None-Match: *`). `{:error, :superseded}` (a 412) means the object changed
  # under us (a stealer flushed) → the caller must self-fence and NOT clobber. On success
  # returns the new object etag so the caller can fence its next flush. Puts the fencing token
  # on the data write itself, closing the check-then-PUT window (finding #15).
  @callback flush(
              shard_id :: String.t(),
              local_path :: Path.t(),
              expected_etag :: String.t() | nil
            ) ::
              {:ok, String.t()} | {:error, :superseded} | {:error, term()}

  # Fenced flush that also records HOW MUCH of the shard's history these bytes contain, so a
  # replica can be ordered against the stored object (Phase 2 A2 promote-on-open). `nil` writes no
  # stamp, which is what every caller outside the coordinator's flush paths wants.
  #
  # A backend must carry the stamp WITHOUT a second request — on S3 it is user metadata on the
  # same PUT. PUT *count* is the S3 bill (ingress bytes are free), so a stamp that cost an extra
  # request would make durability more expensive to buy a failover-only benefit.
  @callback flush(
              shard_id :: String.t(),
              local_path :: Path.t(),
              expected_etag :: String.t() | nil,
              position :: position() | nil
            ) ::
              {:ok, String.t()} | {:error, :superseded} | {:error, term()}

  # As `c:flush/4`, plus the shard's LINEAGE counter (expert review 2026-08-20 #8).
  #
  # The lineage is what the position stamp's `epoch` field carries. It exists because the LOCK
  # epoch — which used to fill that slot — is not monotonic: `release_lease` deletes the lock, so
  # the next `acquire_lease` takes the optimistic create path and starts again at 1. The number
  # therefore climbed on crash-steals and RESET on every clean idle-drop, drain and rebalance
  # handoff, while two consumers treated it as the high-order component of a total order.
  #
  # It is a SEPARATE metadata key rather than being read back out of the position stamp, and that
  # is load-bearing: `position_after_checkpoint/2` deliberately answers `nil` when the WAL is empty
  # both before and after a flush, which is the ordinary graceful-drop path. Deriving the lineage
  # from the stamp would therefore find nothing exactly where it is needed most and fall back to
  # the resetting lock epoch — the same bug, one layer down.
  #
  # `nil` writes no lineage, for the same reason `nil` writes no position: a caller outside the
  # coordinator's flush paths has no ownership history to claim.
  @callback flush(
              shard_id :: String.t(),
              local_path :: Path.t(),
              expected_etag :: String.t() | nil,
              position :: position() | nil,
              lineage :: non_neg_integer() | nil
            ) ::
              {:ok, String.t()} | {:error, :superseded} | {:error, term()}

  # The stored object's position stamp, or `nil` when it has none — an object flushed before
  # stamping existed, or by a caller that passed `nil`. `nil` must be read as "unknown", never as
  # "empty": an unknown stamp means the object can NEVER be overridden by a replica.
  @callback object_position(shard_id :: String.t()) :: {:ok, position() | nil} | {:error, term()}

  # The stored object's current etag WITHOUT transferring the body (an S3 HEAD; the Local
  # double hashes the file). `{:ok, nil}` when no object exists. Used on a warm restart — where
  # the coordinator kept its local copy and skipped the pull — to learn the etag its first
  # fenced flush must match.
  @callback object_etag(shard_id :: String.t()) :: {:ok, String.t() | nil} | {:error, term()}

  # BOTH of the above, from ONE read — the object's identity and its claim, as of a single moment.
  #
  # Not a convenience wrapper. A2's peer recovery compares a replica against the object's position
  # and then publishes fenced on the object's etag, and those two facts have to describe the SAME
  # version of the object or the comparison is not about the thing being overwritten. Read
  # separately they are two requests with a window in between, and the window is on the far side of
  # a multi-second peer query and a whole-database transfer.
  #
  # Note especially that an etag CANNOT stand in for the position on S3: the etag hashes the BODY
  # and the stamp is user metadata, so a re-flush of byte-identical bytes carrying an advanced
  # position keeps the same etag. Comparing etags alone would call that "unchanged".
  #
  # `{:ok, nil}` when no object exists — distinct from `{:ok, %{etag: nil, position: nil}}`, which
  # no backend returns; absence is one answer, not a head full of nils.
  @callback object_head(shard_id :: String.t()) ::
              {:ok,
               %{
                 etag: String.t() | nil,
                 position: position() | nil,
                 lineage: non_neg_integer() | nil
               }
               | nil}
              | {:error, term()}

  # Conditional pull, keyed on the caller's currently-held `etag` (an opaque store
  # value captured from a prior pull). The warm-standby freshness check: a warm cache
  # may lag the owner's latest flush, so before serving it we confirm it equals the
  # store's current object.
  #
  #   * `{:ok, :unchanged}` — the object's etag matches `etag`; nothing written, the
  #     caller's existing local copy is current (a store 304). No byte transfer.
  #   * `{:ok, {:written, new_etag}}` — the object differs (or `etag` was `nil`); fresh
  #     bytes were written to `local_path`, `new_etag` is the current object's etag
  #     (may be `nil` if the store returned none).
  #   * `{:ok, :absent}` — no object exists (a brand-new shard); nothing written.
  #
  # `nil` `etag` degrades to an unconditional pull (always `:written` or `:absent`),
  # which is how a follower captures the initial etag.
  @callback pull_if_changed(
              shard_id :: String.t(),
              local_path :: Path.t(),
              etag :: String.t() | nil
            ) ::
              {:ok, :unchanged}
              | {:ok, {:written, String.t() | nil}}
              | {:ok, :absent}
              | {:error, term()}

  @callback acquire_lease(shard_id :: String.t(), owner :: String.t(), ttl_ms :: pos_integer()) ::
              {:ok, lease()} | {:error, {:held, String.t()}} | {:error, term()}
  @callback renew_lease(shard_id :: String.t(), lease :: lease(), ttl_ms :: pos_integer()) ::
              {:ok, lease()} | {:error, :superseded} | {:error, term()}
  @callback release_lease(shard_id :: String.t(), lease :: lease()) :: :ok | {:error, term()}

  # Read-only fence: confirm `lease` is still the live lock for `shard_id` (owner +
  # epoch unchanged) without writing. `:superseded` once another owner/epoch holds it.
  @callback check_lease(shard_id :: String.t(), lease :: lease()) ::
              :ok | {:error, :superseded} | {:error, term()}

  # Read-only ownership probe: is `shard_id` currently owned by a LIVE node (its lock's
  # owner has a fresh heartbeat, per the same `owner_live?` rule `acquire_lease` uses)?
  # `{:held, owner}` = a live owner holds it; `:free` = no lock or the owner is dead
  # (stealable). Does NOT mutate anything (unlike `acquire_lease`). The rebalancer's
  # dead-node reconciler uses this as the authoritative data-plane liveness check before
  # unpinning — the reporter beat is only a cheap prefilter.
  @callback lease_holder(shard_id :: String.t()) ::
              {:held, String.t()} | :free | {:error, term()}

  # Same read as `lease_holder/1`, but returns WHEN the hold becomes stealable rather than only
  # whether it already is: `{:held, owner, stealable_at_ms}` on this caller's clock.
  #
  # Exists so a caller that wants to WAIT for an imminent steal can ask the backend that owns the
  # liveness rule instead of re-deriving it. `Shards.holder_stealable_soon?/2` did re-derive it,
  # from the heartbeat alone, and #12 then made `acquire_lease` require BOTH the heartbeat and the
  # lock TTL to lapse — so the predictor and the performer disagreed, and a checkout could hold and
  # retry its whole crash-failover budget waiting for a steal that could not happen yet. Both
  # backends derive this and `owner_live?/3` from one function, so they cannot drift apart again.
  @callback lease_stealable_at(shard_id :: String.t()) ::
              {:held, String.t(), integer()} | :free | {:error, term()}

  # Per-node liveness heartbeat (one object per node, not per shard).
  @callback renew_heartbeat(owner :: String.t(), ttl_ms :: pos_integer()) ::
              {:ok, heartbeat()} | {:error, term()}
  @callback read_heartbeat(owner :: String.t()) ::
              {:ok, heartbeat()} | :not_found | {:error, term()}
  @callback clear_heartbeat(owner :: String.t()) :: :ok | {:error, term()}

  # Versioned copies for blue/green migration: the live object stays
  # `<shard_id>`, and the migrator keeps prior versions under `<shard_id>@<version>`
  # for the retention window so a revert is a copy-back.
  @callback retain(shard_id :: String.t(), version :: non_neg_integer()) ::
              :ok | {:error, term()}
  @callback restore(shard_id :: String.t(), version :: non_neg_integer()) ::
              :ok | {:error, term()}

  # Fenced restore (expert review 2026-07-14 #4): copy the shard's `version` object back over
  # live only if the live object still matches `expected_etag` (`If-Match`) — the revert
  # counterpart of the fenced `flush/3`. A revert's read-only `check_lease` fence proves the lock
  # is ours only at THAT instant; the migrator can then stall while a coordinator steals + flushes
  # new bytes, so an UNCONDITIONAL restore would clobber those acknowledged writes with the
  # reverted lineage (and the stealer's next flush would 412 and self-fence away its own writes,
  # since `check_lease` still sees our lock — the restore touched the DATA object, not the lock).
  # A 412 / etag mismatch is `{:error, :superseded}` — the caller aborts and does NOT clobber.
  @callback restore(
              shard_id :: String.t(),
              version :: non_neg_integer(),
              expected_etag :: String.t() | nil
            ) :: :ok | {:error, :superseded} | {:error, term()}

  @callback drop_version(shard_id :: String.t(), version :: non_neg_integer()) ::
              :ok | {:error, term()}

  # Fork-from-template (finding #10): copy the retained snapshot at
  # `<template_id>@<version>` to the LIVE object of `dst_shard_id` — the new-tenant
  # bootstrap. The caller (`Fathom.Migrator.fork_from_template/1`) holds the dst
  # shard's lease and stamps the copied object's `user_version` afterward; this is
  # just the byte copy. A missing snapshot object is an ordinary `{:error, term}`
  # (Local: `:enoent`; S3: a 404 copy status) the caller classifies.
  @callback fork_from(
              template_id :: String.t(),
              version :: non_neg_integer(),
              dst_shard_id :: String.t()
            ) :: :ok | {:error, term()}

  # Delete `shard_id`'s LIVE object (idempotent). The fork path's cleanup: a fork
  # that fails before stamping must leave the dst with NO live object, so the shard
  # simply cold-opens empty next time instead of carrying an unstamped schema copy
  # a later rollout would corrupt (replaying DDL over already-present tables).
  @callback drop_live(shard_id :: String.t()) :: :ok | {:error, term()}

  # Fork a LIVE shard to a NEW shard id (expert review #14): copy `<src_id>`'s live object to
  # `<dst_id>`'s live object — the database-forking / tenant-clone kernel (a fork is one object
  # copy). Refuses `{:error, :dst_exists}` if `dst_id` already has a stored object (never clobber a
  # tenant) and `{:error, :no_source}` if `src_id` has none. Reflects `src`'s last durably-flushed
  # state (like a snapshot); the caller (`Fathom.Tenants.fork/2`) registers the dst directory row +
  # token. Distinct from `fork_from/3`, which copies a retained `@<version>` template snapshot.
  @callback fork_shard(src_id :: String.t(), dst_id :: String.t()) ::
              :ok | {:error, :dst_exists | :no_source | term()}

  @typedoc "A stored point-in-time snapshot: its `id` and the object's byte size."
  @type snapshot :: %{id: String.t(), bytes: non_neg_integer()}

  # Point-in-time snapshots (expert review 2026-07-14 #12): fathom-managed, time-labeled copies
  # of a shard's live object, kept under `<shard_id>@snap-<snapshot_id>` — a namespace distinct
  # from the integer-`@<version>` migration copies above (and excluded from `stored_usage`, which
  # already skips any `@`-keyed object). Backend-uniform (Local + S3), so the whole snapshot/
  # restore path is testable without a real object store; S3 bucket versioning
  # (`docs/durability.md`) layers underneath as defense-in-depth. `snapshot/2` copies live →
  # snapshot; `restore_snapshot/2` copies snapshot → live UNCONDITIONALLY (a low-level primitive,
  # exercised only by backend round-trip tests). Production restore goes through the FENCED
  # `restore_snapshot/3` — mirroring `restore/2` vs the fenced `restore/3` — which If-Match's the
  # live etag so a write racing in after `Fathom.Snapshots.restore/3`'s drain can't be silently
  # clobbered (expert review 2026-07-18 #2): a mismatch is `{:error, :superseded}`, no overwrite.
  @callback snapshot(shard_id :: String.t(), snapshot_id :: String.t()) :: :ok | {:error, term()}
  @callback list_snapshots(shard_id :: String.t()) :: {:ok, [snapshot()]} | {:error, term()}
  @callback restore_snapshot(shard_id :: String.t(), snapshot_id :: String.t()) ::
              :ok | {:error, term()}
  @callback restore_snapshot(
              shard_id :: String.t(),
              snapshot_id :: String.t(),
              expected_etag :: String.t() | nil
            ) :: :ok | {:error, :superseded} | {:error, term()}
  @callback drop_snapshot(shard_id :: String.t(), snapshot_id :: String.t()) ::
              :ok | {:error, term()}

  # Fenced restore of ALREADY-DOWNLOADED snapshot bytes (expert review 2026-08-26 #25). Same fence
  # and same failure shapes as `restore_snapshot/3`; the only difference is that the caller supplies
  # the bytes instead of the backend fetching them.
  #
  # This exists so `Fathom.Snapshots.restore/3` downloads a snapshot ONCE. It used to pull the
  # snapshot to a temp purely to read four bytes at file offset 60 (`PRAGMA user_version`, the
  # schema-boundary guard), delete that temp, and then have `restore_snapshot/3` download the
  # IDENTICAL key again to promote it. Beyond the doubled GET and the doubled shard-sized temp
  # write, the version that GATED the restore was read from download #1 while download #2's bytes
  # became the live object — a gap no code closed. One download makes the verified bytes and the
  # promoted bytes the same bytes.
  #
  # The caller owns `local_path` and is responsible for deleting it; a backend must not consume it.
  @callback restore_snapshot_from_file(
              shard_id :: String.t(),
              local_path :: Path.t(),
              expected_etag :: String.t() | nil
            ) :: :ok | {:error, :superseded} | {:error, term()}

  # Downloads a snapshot's bytes to `local_path` (`{:ok, etag}`, or `{:ok, nil}` if the snapshot is
  # absent) — the read counterpart of `pull` for the `@snap-<id>` namespace. `Fathom.Snapshots.restore`
  # uses it to read a snapshot's `PRAGMA user_version` and refuse a cross-schema-version restore
  # before the destructive copy-back (expert review #7).
  @callback pull_snapshot(
              shard_id :: String.t(),
              snapshot_id :: String.t(),
              local_path :: Path.t()
            ) ::
              {:ok, String.t() | nil} | {:absent, String.t() | nil} | {:error, term()}

  # Full tenant erasure (expert review 2026-07-14 #15): delete EVERY stored object
  # belonging to `shard_id` — the live `.db`, the `.lock`, every retained
  # `@<version>` copy, and every `@snap-<snapshot_id>` — in one sweep, since a
  # per-object caller (`drop_live`/`drop_version`/`drop_snapshot`) would first have
  # to enumerate versions/snapshots the deleting node may not know. Idempotent (a
  # shard with no objects is `:ok`). Matching is EXACT on the id delimiter — the
  # character after the id must be `.` (live/lock) or `@` (a version/snapshot copy)
  # — so purging `acme` can never touch `acme2`. The per-node `heartbeats/<owner>`
  # object is not a shard object and is never matched. Caller must have drained the
  # shard and confirmed no live node holds the lease (`Fathom.Tenants.DeleteJob`).
  @callback purge_shard(shard_id :: String.t()) :: :ok | {:error, term()}

  # Aggregate storage footprint for observability — `{object_count, total_bytes}` of the live
  # shard objects. Optional: a backend that can't cheaply enumerate simply omits it, and the
  # dispatcher returns `{:error, :unsupported}`. Potentially expensive (an S3 LIST), so callers
  # poll it slowly + cache; at fleet scale prefer S3 Inventory / CloudWatch over a live LIST.
  @callback stored_usage() :: {non_neg_integer(), non_neg_integer()} | {:error, term()}
  @optional_callbacks stored_usage: 0

  # The cross-store DR backstop (expert review #6): a deleted tenant's re-mint guard is the Postgres
  # directory (loaded into the Tombstones ETS), but a Postgres point-in-time restore rolls the
  # directory back and can resurrect a deleted tenant. Storage is NOT rolled back, so a tiny tombstone
  # marker here — under a `tombstones/<shard_id>` key that `purge_shard` never touches (a distinct
  # namespace, not a `<shard>@…` object) — survives the restore. `put_tombstone/1` is written on
  # delete; `tombstoned_ids/0` is the boot-time union scan that repopulates the ETS despite a rollback.
  @callback put_tombstone(shard_id :: String.t()) :: :ok | {:error, term()}
  @callback tombstoned_ids() :: {:ok, [String.t()]} | {:error, term()}

  # The durable token-revocation floor (expert review #6, DR backstop). Token revocation bumps a
  # per-shard `token_version` in the Postgres directory; a directory point-in-time restore rolls it
  # back and revoked tokens verify again. This mirrors the floor (a monotonic high-water mark) to
  # storage under a `tokenfloors/<shard_id>` key so `HranaAuth.Revocations` can union it on a cold
  # read and never drop below it — keeping a revocation durable across a directory restore.
  @callback put_token_floor(shard_id :: String.t(), version :: non_neg_integer()) ::
              :ok | {:error, term()}
  @callback read_token_floor(shard_id :: String.t()) ::
              {:ok, non_neg_integer() | nil} | {:error, term()}

  @doc "Pulls the shard's stored file to `local_path`, returning its etag (`nil` if absent)."
  # `{:absent, _}` was missing here while the `@callback` above has carried it since expert review
  # 2026-08-01 #24 — the delegating spec contradicted the contract it delegates to, and callers
  # matching the absent case (`Fathom.Shard.revalidate_takeover/2`, `mix fathom.shard`) therefore
  # read as dead code. That case is not incidental: it is how "no object, or a steal sentinel"
  # is told apart from "a real object", which #24 introduced precisely because a sentinel used to
  # read as a genuine snapshot of an empty database.
  @spec pull(String.t(), Path.t()) ::
          {:ok, String.t()} | {:absent, String.t() | nil} | {:error, term()}
  def pull(shard_id, local_path), do: backend().pull(shard_id, local_path)

  @doc "Unconditional flush of `local_path` (unfenced — see `flush/3` for the coordinator)."
  @spec flush(String.t(), Path.t()) :: :ok | {:error, term()}
  def flush(shard_id, local_path), do: backend().flush(shard_id, local_path)

  @doc "Fenced flush: writes only if the stored object still matches `expected_etag`. See callback."
  @spec flush(String.t(), Path.t(), String.t() | nil) ::
          {:ok, String.t()} | {:error, :superseded} | {:error, term()}
  def flush(shard_id, local_path, expected_etag),
    do: backend().flush(shard_id, local_path, expected_etag, nil)

  @doc "Fenced flush carrying a position stamp. See the `c:flush/4` callback."
  @spec flush(String.t(), Path.t(), String.t() | nil, position() | nil) ::
          {:ok, String.t()} | {:error, :superseded} | {:error, term()}
  def flush(shard_id, local_path, expected_etag, position),
    do: backend().flush(shard_id, local_path, expected_etag, position)

  @doc "Fenced flush carrying a position stamp and a lineage counter. See the `c:flush/5` callback."
  @spec flush(String.t(), Path.t(), String.t() | nil, position() | nil, non_neg_integer() | nil) ::
          {:ok, String.t()} | {:error, :superseded} | {:error, term()}
  def flush(shard_id, local_path, expected_etag, position, lineage),
    do: backend().flush(shard_id, local_path, expected_etag, position, lineage)

  @doc """
  The next lineage value for `shard_id`, given what the store currently holds.

  Strictly greater than anything previously stamped for this shard, which is the whole property
  the lock epoch failed to provide. 1 for a shard nothing has ever flushed.

  **The MAX of the two stored numbers, not a preference between them**, and that is the whole
  subtlety. During a rolling upgrade a shard's object is written alternately by nodes that stamp a
  lineage and nodes that still stamp the LOCK epoch, and `flush/5`'s `nil` deliberately leaves an
  existing lineage in place — so the two keys drift apart and EITHER can be the larger. Seeding
  from the lineage alone would hand out a number below an epoch a not-yet-upgraded peer had
  already shipped to its replicas, and that replica would then outrank the object: exactly the
  promotion this counter exists to prevent, reintroduced by the migration to it.
  """
  @spec next_lineage(%{lineage: non_neg_integer() | nil, position: position() | nil} | nil) ::
          pos_integer()
  def next_lineage(nil), do: 1

  def next_lineage(head) do
    max(
      non_neg(Map.get(head, :lineage)),
      non_neg(
        case Map.get(head, :position) do
          %{epoch: e} -> e
          _ -> nil
        end
      )
    ) + 1
  end

  defp non_neg(n) when is_integer(n) and n >= 0, do: n
  defp non_neg(_), do: 0

  @doc "The stored object's position stamp, or `nil` if it carries none. See the callback."
  @spec object_position(String.t()) :: {:ok, position() | nil} | {:error, term()}
  def object_position(shard_id), do: backend().object_position(shard_id)

  @doc """
  Serialize a position stamp for transport: `"<epoch>:<wal_gen>:<offset>"`, or
  `"<epoch>:<wal_gen>:<offset>:<salt1>"` when the WAL's identity is known.

  Deliberately a fixed integer shape rather than a map format: this crosses a network and comes
  back from an object a tenant's node wrote, so `parse_position/1` must be able to refuse anything
  unexpected without inventing a value. Shared here so every backend encodes it identically — a
  per-backend format would be a contract only the double ever tested.

  ## Why the fourth field exists (expert review 2026-08-26 #2)

  `wal_gen` is SQLite's `ckpt_seq`, which counts checkpoints WITHIN ONE WAL FILE and restarts at 0
  when SQLite deletes and recreates the `-wal`. That is not exotic — it happens on the last
  connection close, which `snapshot_and_upload/1` notes is frequently the periodic flush itself, so
  it is the QUIET-TENANT path. Measured on this codebase:

      stream 1 open   ckpt_seq=0  salt1=977542977   (wal exists)
      after close     wal UNLINKED
      stream 2 open   ckpt_seq=0  salt1=978380554   (wal exists)

  Same generation number, unrelated WAL. `salt1` is what distinguishes them, and the follower side
  has carried it all along (`FollowerLog.t()`); only the object stamp did not.

  ## Mixed-version fleets

  Both directions degrade INERT, which is why this needs no rollout flag. An old node reading a
  4-field stamp gets `nil` from its `parse_position/1` (unexpected shape) and treats the object as
  un-overridable. A new node reading a 3-field stamp parses it without a salt, and
  `Promote.fresher?/2` refuses to rank. Both answers are "never override the stored object" — the
  pre-A2 behaviour AGENTS.md already calls "never worse than off".
  """
  @spec encode_position(position()) :: String.t()
  def encode_position(%{epoch: e, wal_gen: g, offset: o, salt1: s}) when is_integer(s),
    do: "#{e}:#{g}:#{o}:#{s}"

  def encode_position(%{epoch: e, wal_gen: g, offset: o}), do: "#{e}:#{g}:#{o}"

  @doc """
  Parse a position stamp, or `nil` for anything that is not exactly one.

  Every failure returns `nil` — absent, malformed, negative, extra fields. `nil` means "unknown",
  and the only thing that consumes a position treats unknown as "never override the stored
  object", so garbage degrades to the safe answer instead of a fabricated ordering.
  """
  @spec parse_position(String.t() | nil) :: position() | nil
  def parse_position(nil), do: nil

  def parse_position(raw) when is_binary(raw) do
    case String.split(raw, ":") do
      [e, g, o] -> parse_position(e, g, o, nil)
      # The salt-bearing form (#2). A stamp written before that review has three fields and still
      # parses — it simply carries no salt, which `Promote.fresher?/2` treats as unknown.
      [e, g, o, s] -> parse_position(e, g, o, s)
      _ -> nil
    end
  end

  def parse_position(_), do: nil

  defp parse_position(e, g, o, s) do
    with {epoch, ""} when epoch >= 0 <- Integer.parse(e),
         {gen, ""} when gen >= 0 <- Integer.parse(g),
         {off, ""} when off >= 0 <- Integer.parse(o),
         {:ok, salt} <- parse_salt(s) do
      base = %{epoch: epoch, wal_gen: gen, offset: off}
      if salt, do: Map.put(base, :salt1, salt), else: base
    else
      _ -> nil
    end
  end

  defp parse_salt(nil), do: {:ok, nil}

  defp parse_salt(s) when is_binary(s) do
    case Integer.parse(s) do
      {salt, ""} when salt >= 0 -> {:ok, salt}
      # A malformed salt is NOT "no salt" — the whole stamp is refused, matching this function's
      # rule that anything unexpected returns nil rather than a partially-invented value.
      _ -> :error
    end
  end

  @doc "The stored object's current etag (`nil` if absent) without transferring the body."
  @spec object_etag(String.t()) :: {:ok, String.t() | nil} | {:error, term()}
  def object_etag(shard_id), do: backend().object_etag(shard_id)

  @doc "The object's etag and position stamp from ONE read. See the callback."
  @spec object_head(String.t()) ::
          {:ok, %{etag: String.t() | nil, position: position() | nil} | nil} | {:error, term()}
  def object_head(shard_id), do: backend().object_head(shard_id)

  @doc """
  Conditional pull keyed on the caller's held `etag` — the warm-standby freshness
  check. Returns `{:ok, :unchanged}` (store object matches `etag`; nothing written),
  `{:ok, {:written, new_etag}}` (fresh bytes written to `local_path`), or
  `{:ok, :absent}` (no object). A `nil` `etag` is an unconditional pull that captures
  the current etag. See the callback docs.
  """
  @spec pull_if_changed(String.t(), Path.t(), String.t() | nil) ::
          {:ok, :unchanged}
          | {:ok, {:written, String.t() | nil}}
          | {:ok, :absent}
          | {:error, term()}
  def pull_if_changed(shard_id, local_path, etag),
    do: backend().pull_if_changed(shard_id, local_path, etag)

  @doc """
  Acquires `shard_id`'s lease for `owner` with a `ttl_ms` window. Returns
  `{:error, {:held, owner}}` if another owner holds a live lease, or
  `{:error, reason}` on a transient store error (the caller must NOT proceed —
  see the module doc on failing closed).
  """
  @spec acquire_lease(String.t(), String.t(), pos_integer()) ::
          {:ok, lease()} | {:error, {:held, String.t()}} | {:error, term()}
  def acquire_lease(shard_id, owner, ttl_ms),
    do: backend().acquire_lease(shard_id, owner, ttl_ms)

  @doc """
  Renews `lease` on `shard_id`, extending its expiry by `ttl_ms`. Returns
  `{:error, :superseded}` once another owner has taken the lease (the holder must
  self-fence), or `{:error, reason}` on a transient store error (retry, don't
  fence — a transient blip is not loss of ownership).
  """
  @spec renew_lease(String.t(), lease(), pos_integer()) ::
          {:ok, lease()} | {:error, :superseded} | {:error, term()}
  def renew_lease(shard_id, lease, ttl_ms),
    do: backend().renew_lease(shard_id, lease, ttl_ms)

  @doc "Releases `lease` on `shard_id` (no-op if we no longer hold it)."
  @spec release_lease(String.t(), lease()) :: :ok | {:error, term()}
  def release_lease(shard_id, lease), do: backend().release_lease(shard_id, lease)

  @doc """
  The shared policy half of a **conditional** lease release that came back "not the object I
  wrote" (S3's `412 Precondition Failed`). Lives here, not in a backend, because it is a decision
  every etag-carrying backend has to make identically — and because putting it here is what lets
  the default test suite exercise it (the suite's backend is `Fathom.Test.FaultyStorage`, so a
  policy implemented inside each backend would be tested only against the double).

  A 412 is **two** situations, and reporting `:ok` for both is how a lock gets stranded silently:

    * the lock is now **someone else's** — a correct no-op. Deleting it would remove a live
      owner's lock, which is finding #22 and the reason the delete is conditional at all.

    * the lock is **still ours at a different etag** — a leak reported as success. Our own etag
      rotates under us: a same-owner reclaim rewrites the lock (`S3.acquire_existing/4`, same
      epoch, new etag) and so does a legacy-mode `renew_lease/3`. The stranded lock then names a
      LIVE node with no coordinator behind it, `owner_live?` reads that node's fresh heartbeat
      forever, and no peer, failover or migrator can ever take the shard — while it keeps serving,
      because its own node reclaims at the same incarnation. That combination is why this survived
      #9 and #11: both fixed CALLERS that released with a stale lease, so any rotation whose result
      the caller did not receive still leaked.

  `read_owner` returns `{:ours, handle}` / `:not_ours` / `{:error, reason}`, where `handle` is
  whatever the backend needs to condition the retry on what it just read (the fresh etag, for S3).
  `delete_current` receives that handle. Threading it through the return value rather than stashing
  it keeps the two closures independent — an earlier draft passed it via the process dictionary and
  the `Process.put(...) && :ours` idiom silently returned `nil`, because `Process.put/2` returns the
  PREVIOUS value.

  One extra round trip, only on the 412 path, which a healthy release never takes.
  """
  @spec resolve_stale_release(
          String.t(),
          lease(),
          (-> {:ours, term()} | :not_ours | {:error, term()}),
          (term() -> :ok | {:error, term()})
        ) :: :ok | {:error, term()}
  def resolve_stale_release(shard_id, lease, read_owner, delete_current) do
    case read_owner.() do
      {:ours, handle} ->
        :telemetry.execute(
          [:fathom, :shard, :lease, :release_retried],
          %{count: 1},
          %{shard_id: shard_id, owner: Map.get(lease, :owner)}
        )

        delete_current.(handle)

      :not_ours ->
        :ok

      {:error, reason} ->
        {:error, reason}
    end
  end

  @doc """
  Read-only fence: returns `:ok` if `lease` is still the live lock for `shard_id`
  (same owner + epoch), `{:error, :superseded}` if another owner/epoch has taken it,
  or `{:error, reason}` on a transient store error. Unlike `renew_lease/3` this does
  not write — liveness is the node heartbeat, so the flush fence only needs to
  confirm ownership hasn't changed.
  """
  @spec check_lease(String.t(), lease()) :: :ok | {:error, :superseded} | {:error, term()}
  def check_lease(shard_id, lease), do: backend().check_lease(shard_id, lease)

  @doc """
  Read-only ownership probe: `{:held, owner}` if `shard_id` is owned by a live node,
  `:free` if no lock exists or its owner is dead (stealable), `{:error, reason}` on a
  transient store error. Never mutates. The rebalancer reconciler uses this as the
  authoritative "is this shard's data plane alive?" check before unpinning a dead node.
  """
  @spec lease_holder(String.t()) :: {:held, String.t()} | :free | {:error, term()}
  def lease_holder(shard_id), do: backend().lease_holder(shard_id)

  @doc """
  When `shard_id`'s current hold becomes stealable, in `now_ms/0` terms. See the callback.
  """
  @spec lease_stealable_at(String.t()) ::
          {:held, String.t(), integer()} | :free | {:error, term()}
  def lease_stealable_at(shard_id), do: backend().lease_stealable_at(shard_id)

  @doc """
  Renews this node's liveness heartbeat (`owner`), extending its expiry by `ttl_ms`.
  One object per node — the cost is O(nodes), not O(shards). Driven by
  `Fathom.Shard.Heartbeat`.
  """
  @spec renew_heartbeat(String.t(), pos_integer()) :: {:ok, heartbeat()} | {:error, term()}
  def renew_heartbeat(owner, ttl_ms), do: backend().renew_heartbeat(owner, ttl_ms)

  @doc "Reads `owner`'s heartbeat (the liveness signal `acquire_lease/3` consults to steal)."
  @spec read_heartbeat(String.t()) :: {:ok, heartbeat()} | :not_found | {:error, term()}
  def read_heartbeat(owner), do: backend().read_heartbeat(owner)

  @doc "Clears `owner`'s heartbeat (clean node shutdown so its shards are immediately stealable)."
  @spec clear_heartbeat(String.t()) :: :ok | {:error, term()}
  def clear_heartbeat(owner), do: backend().clear_heartbeat(owner)

  @doc "Copies the shard's live object to its `version` (retains the old version for revert)."
  @spec retain(String.t(), non_neg_integer()) :: :ok | {:error, term()}
  def retain(shard_id, version), do: backend().retain(shard_id, version)

  @doc "Copies the shard's `version` object back to live (the revert step)."
  @spec restore(String.t(), non_neg_integer()) :: :ok | {:error, term()}
  def restore(shard_id, version), do: backend().restore(shard_id, version)

  @doc """
  Fenced restore: copies the shard's `version` object back over live only if the live object
  still matches `expected_etag` (`If-Match`). `{:error, :superseded}` on a mismatch — a steal
  changed the live object since the revert's fence, so the caller aborts instead of clobbering.
  See the callback docs.
  """
  @spec restore(String.t(), non_neg_integer(), String.t() | nil) ::
          :ok | {:error, :superseded} | {:error, term()}
  def restore(shard_id, version, expected_etag),
    do: backend().restore(shard_id, version, expected_etag)

  @doc "Deletes the shard's `version` object (idempotent; retirement)."
  @spec drop_version(String.t(), non_neg_integer()) :: :ok | {:error, term()}
  def drop_version(shard_id, version), do: backend().drop_version(shard_id, version)

  @doc """
  Copies the retained `<template_id>@<version>` snapshot to `dst_shard_id`'s LIVE
  object — the fork-from-template new-tenant bootstrap (finding #10). The caller must
  hold the dst shard's lease and stamp the copy's `user_version` afterward; see
  `Fathom.Migrator.fork_from_template/1`.
  """
  @spec fork_from(String.t(), non_neg_integer(), String.t()) :: :ok | {:error, term()}
  def fork_from(template_id, version, dst_shard_id),
    do: backend().fork_from(template_id, version, dst_shard_id)

  @doc """
  Deletes `shard_id`'s LIVE object (idempotent) — the fork path's failure cleanup, so
  an aborted fork leaves the shard to cold-open empty rather than carrying an
  unstamped schema copy. Callers must hold the shard's lease.
  """
  @spec drop_live(String.t()) :: :ok | {:error, term()}
  def drop_live(shard_id), do: backend().drop_live(shard_id)

  @doc """
  Forks a live shard to a new shard id — copies `src_id`'s live object to `dst_id`'s (the
  database-forking kernel, #14). `{:error, :dst_exists}` if the dst is already stored,
  `{:error, :no_source}` if the src isn't. See the callback.
  """
  @spec fork_shard(String.t(), String.t()) :: :ok | {:error, :dst_exists | :no_source | term()}
  def fork_shard(src_id, dst_id), do: backend().fork_shard(src_id, dst_id)

  @doc "Copies the shard's live object to a point-in-time snapshot `<shard>@snap-<snapshot_id>`."
  @spec snapshot(String.t(), String.t()) :: :ok | {:error, term()}
  def snapshot(shard_id, snapshot_id), do: backend().snapshot(shard_id, snapshot_id)

  @doc "Lists the shard's stored snapshots (`id` + byte size), newest id first."
  @spec list_snapshots(String.t()) :: {:ok, [snapshot()]} | {:error, term()}
  def list_snapshots(shard_id), do: backend().list_snapshots(shard_id)

  @doc """
  Copies a snapshot back over the shard's live object, UNCONDITIONALLY. Low-level primitive
  (backend round-trip tests); production restore uses the fenced `restore_snapshot/3`.
  """
  @spec restore_snapshot(String.t(), String.t()) :: :ok | {:error, term()}
  def restore_snapshot(shard_id, snapshot_id),
    do: backend().restore_snapshot(shard_id, snapshot_id)

  @doc """
  Fenced snapshot restore: copies the snapshot over live only if live still matches
  `expected_etag` (captured by the caller right after its `lease_holder` check). A write that
  raced in after `Fathom.Snapshots.restore/3`'s drain — a fresh checkout on this or any node that
  acquired the freed lease and flushed — moves live's etag, so this returns `{:error, :superseded}`
  and does NOT clobber it (expert review 2026-07-18 #2). Mirrors the fenced migration `restore/3`.
  """
  @spec restore_snapshot(String.t(), String.t(), String.t() | nil) ::
          :ok | {:error, :superseded} | {:error, term()}
  def restore_snapshot(shard_id, snapshot_id, expected_etag),
    do: backend().restore_snapshot(shard_id, snapshot_id, expected_etag)

  @doc """
  Fenced restore of snapshot bytes the caller ALREADY downloaded — same fence, same failure shapes
  as `restore_snapshot/3`, one fewer full-object GET (expert review 2026-08-26 #25).

  `Fathom.Snapshots.restore/3` reads the snapshot's `PRAGMA user_version` from a pulled temp to
  gate a cross-schema-version restore; passing that same temp here means the bytes that were
  version-checked are the bytes that get promoted. The caller owns `local_path` and deletes it.
  """
  @spec restore_snapshot_from_file(String.t(), Path.t(), String.t() | nil) ::
          :ok | {:error, :superseded} | {:error, term()}
  def restore_snapshot_from_file(shard_id, local_path, expected_etag),
    do: backend().restore_snapshot_from_file(shard_id, local_path, expected_etag)

  @doc "Deletes a stored snapshot (idempotent)."
  @spec drop_snapshot(String.t(), String.t()) :: :ok | {:error, term()}
  def drop_snapshot(shard_id, snapshot_id), do: backend().drop_snapshot(shard_id, snapshot_id)

  @doc "Downloads a snapshot's bytes to `local_path` (`{:ok, nil}` if absent). See the callback (#7)."
  # Both backends return `{:absent, _}` — S3 for a missing object AND for a steal sentinel,
  # Local for a missing file — and neither this spec nor the `@callback` said so.
  @spec pull_snapshot(String.t(), String.t(), Path.t()) ::
          {:ok, String.t() | nil} | {:absent, String.t() | nil} | {:error, term()}
  def pull_snapshot(shard_id, snapshot_id, local_path),
    do: backend().pull_snapshot(shard_id, snapshot_id, local_path)

  @doc """
  Deletes every stored object for `shard_id` — the live `.db`, the `.lock`, all retained
  `@<version>` copies, and all `@snap-<id>` snapshots — for full tenant erasure (finding #15).
  Idempotent and collision-safe (never touches a sibling id like `acme2`). See the callback.
  """
  @spec purge_shard(String.t()) :: :ok | {:error, term()}
  def purge_shard(shard_id), do: backend().purge_shard(shard_id)

  @doc """
  Writes the durable tombstone marker for `shard_id` (the DR backstop, #6) — a tiny object under a
  `tombstones/` namespace that survives a Postgres directory restore and `purge_shard`. Idempotent.
  """
  @spec put_tombstone(String.t()) :: :ok | {:error, term()}
  def put_tombstone(shard_id), do: backend().put_tombstone(shard_id)

  @doc """
  Lists every tombstoned (deleted) shard id from durable storage — the boot-time union scan that
  keeps the re-mint guard complete even after a Postgres point-in-time restore (#6).
  """
  @spec tombstoned_ids() :: {:ok, [String.t()]} | {:error, term()}
  def tombstoned_ids, do: backend().tombstoned_ids()

  @doc "Mirrors `shard_id`'s token-revocation floor to durable storage (the DR backstop, #6). Idempotent."
  @spec put_token_floor(String.t(), non_neg_integer()) :: :ok | {:error, term()}
  def put_token_floor(shard_id, version), do: backend().put_token_floor(shard_id, version)

  @doc "Reads `shard_id`'s durable token-revocation floor (`nil` if none) — the boot/cold-read union (#6)."
  @spec read_token_floor(String.t()) :: {:ok, non_neg_integer() | nil} | {:error, term()}
  def read_token_floor(shard_id), do: backend().read_token_floor(shard_id)

  @doc """
  Aggregate storage footprint — `{object_count, total_bytes}` of the live shard objects — for the
  admin dashboard's storage panel. Best-effort and potentially expensive (an S3 LIST): poll it
  slowly and cache. Returns `{:error, :unsupported}` if the backend doesn't implement it.

  > At fleet scale prefer S3 Inventory / CloudWatch (`BucketSizeBytes`, `NumberOfObjects`) over a
  > live LIST — this is a small-scale / dev stand-in.
  """
  @spec stored_usage() :: {non_neg_integer(), non_neg_integer()} | {:error, term()}
  def stored_usage do
    b = backend()
    if function_exported?(b, :stored_usage, 0), do: b.stored_usage(), else: {:error, :unsupported}
  end

  @doc false
  @spec encode_lease(lease()) :: binary()
  def encode_lease(%{owner: owner, epoch: epoch, expires_at_ms: exp}),
    do: Jason.encode!(%{"owner" => owner, "epoch" => epoch, "expires_at_ms" => exp})

  @doc false
  @spec decode_lease(binary()) :: {:ok, lease()} | :error
  def decode_lease(body) do
    case Jason.decode(body) do
      {:ok, %{"owner" => owner, "epoch" => epoch, "expires_at_ms" => exp}}
      when is_binary(owner) and is_integer(epoch) and is_integer(exp) ->
        {:ok, %{owner: owner, epoch: epoch, expires_at_ms: exp}}

      _ ->
        :error
    end
  end

  @doc false
  @spec encode_heartbeat(heartbeat()) :: binary()
  def encode_heartbeat(%{owner: owner, expires_at_ms: exp}),
    do: Jason.encode!(%{"owner" => owner, "expires_at_ms" => exp})

  @doc false
  @spec decode_heartbeat(binary()) :: {:ok, heartbeat()} | :error
  def decode_heartbeat(body) do
    case Jason.decode(body) do
      {:ok, %{"owner" => owner, "expires_at_ms" => exp}}
      when is_binary(owner) and is_integer(exp) ->
        {:ok, %{owner: owner, expires_at_ms: exp}}

      _ ->
        :error
    end
  end

  @doc false
  @spec now_ms() :: integer()
  def now_ms, do: System.system_time(:millisecond)

  @default_steal_margin_ms 5_000

  @doc """
  How long past a heartbeat's expiry an owner must stay silent before a peer may
  steal its shards (`config :fathom, :steal_margin_ms`, default
  #{@default_steal_margin_ms}ms).

  The steal decision compares two nodes' wall clocks — the reader's `now` against the
  owner's heartbeat `expires_at_ms` — so this margin absorbs inter-node clock skew: a
  peer steals only once the heartbeat is expired by MORE than the margin, so a wrongful
  steal requires skew greater than the heartbeat's remaining life PLUS this margin. It
  must exceed the fleet's max inter-node clock skew — run NTP and monitor skew (a
  sustained `fathom.shard.lease.superseded` rate is the tripwire). Cost: a hard crash
  (an owner that dies without clearing its heartbeat) delays failover by up to this
  margin beyond the lease TTL; a clean shutdown clears the heartbeat and is unaffected.
  """
  @spec steal_margin_ms() :: non_neg_integer()
  def steal_margin_ms,
    do: Application.get_env(:fathom, :steal_margin_ms, @default_steal_margin_ms)

  @doc """
  Write `body` to `path` atomically: write a sibling temp file, then `File.rename/2` it into
  place (atomic on POSIX within a filesystem — the temp sits in `path`'s dir). A crash or a
  concurrent reader (a warm-cache promotion mid-rewrite) therefore sees either the whole old
  file or the whole new one, never a torn/half-written object (findings #24/#28). The temp is
  cleaned up on a write or rename error.
  """
  @spec atomic_write(Path.t(), iodata()) :: :ok | {:error, term()}
  def atomic_write(path, body), do: with_atomic_temp(path, &File.write(&1, body))

  @doc "Like `atomic_write/2` but copies `src`'s bytes into `dst` (temp + rename)."
  @spec atomic_copy(Path.t(), Path.t()) :: :ok | {:error, term()}
  def atomic_copy(src, dst), do: with_atomic_temp(dst, &File.cp(src, &1))

  # Materialize into a sibling temp, then atomically rename into `dst`; drop the temp on error.
  #
  # The temp is fsynced BEFORE the rename (expert review #17): rename-without-data-fsync
  # is atomic against process crash but not power loss — after a power cut the name can
  # exist with zero-length/partial content (guaranteed on XFS, heuristic on ext4). A torn
  # local file would then be adopted as authoritative on reboot (`warm?` ⇒ dirty) and
  # flushed over the good stored object with a valid If-Match — the corruption becomes
  # the durable truth. With the data fsynced first, a power cut yields whole-old (rename
  # not yet persisted) or whole-new, never torn. Persisting the rename itself would need
  # a directory fsync, which the BEAM can't do portably (`:file.open` on a dir is
  # eisdir); losing the rename only re-exposes the old object, which is safe.
  defp with_atomic_temp(dst, produce) do
    File.mkdir_p!(Path.dirname(dst))
    tmp = "#{dst}.tmp.#{System.unique_integer([:positive])}"

    with :ok <- produce.(tmp),
         :ok <- sync_file(tmp),
         :ok <- File.rename(tmp, dst) do
      :ok
    else
      {:error, _} = err ->
        File.rm(tmp)
        err
    end
  end

  @doc """
  Fsync + atomically rename an already-materialized temp into `dst` — the promotion
  half of `atomic_write/2` for callers that STREAM their bytes into the temp
  themselves (the S3 backend's downloads, expert review #20) instead of buffering a
  whole object in memory. Same crash-consistency contract (expert review #17).
  """
  @spec promote_temp(Path.t(), Path.t()) :: :ok | {:error, term()}
  def promote_temp(tmp, dst) do
    File.mkdir_p!(Path.dirname(dst))

    with :ok <- sync_file(tmp),
         :ok <- File.rename(tmp, dst) do
      :ok
    else
      {:error, _} = err ->
        File.rm(tmp)
        err
    end
  end

  @doc """
  Record that `owner` — a previous incarnation of THIS node — was verified dead and
  its heartbeat cleared (expert review round-2 #34; the proof-of-death rules are
  #16's, in `Fathom.Shard.Heartbeat`). `incarnation_dead?/1` lets the backends'
  `owner_live?` treat that exact owner's locks as stealable immediately instead of
  waiting out the lock-TTL fallback (~TTL+margin of unavailability per
  recently-held shard on every fast restart — the fallback exists for foreign
  owners whose liveness we can't know, which a proven-dead predecessor is not).
  """
  @spec mark_incarnation_dead(String.t()) :: :ok
  def mark_incarnation_dead(owner) do
    # A SET of every proven-dead incarnation, not a single overwriting slot (expert review
    # 2026-07-14 #19): a node that restarts several times fast (incarnations X→Y→Z) proves each
    # predecessor dead in turn, but a lone slot remembered only the LATEST — locks still held by
    # the earlier, equally-dead incarnations then fell back to the slow lock-TTL liveness path
    # (up to TTL+margin of extra unavailability per recently-held shard on every fast restart),
    # the exact cost this fast-path exists to erase. The set is tiny (a handful of recent
    # incarnations of THIS node) so it stays bounded and simple. persistent_term writes trigger
    # a global GC, so write ONLY on a genuinely new discovery (membership-check first).
    dead = dead_incarnations()

    unless MapSet.member?(dead, owner) do
      :persistent_term.put({__MODULE__, :dead_incarnations}, MapSet.put(dead, owner))
    end

    :ok
  end

  @doc "Whether `owner` is a previous incarnation `mark_incarnation_dead/1` proved dead. Exact string match, so migrator/foreign owners can never match."
  @spec incarnation_dead?(String.t()) :: boolean()
  def incarnation_dead?(owner), do: MapSet.member?(dead_incarnations(), owner)

  # A heartbeat proof is NOT a proof that the process stopped (expert review 2026-08-20 #10).
  #
  # `mark_incarnation_dead/1` records that a predecessor's HEARTBEAT OBJECT stopped being renewed.
  # A node whose `Heartbeat` GenServer died but which keeps SERVING produces exactly that reading —
  # and it is precisely the case finding #11's `:not_found` branch exists to protect, because such a
  # node degrades to the legacy per-shard renew fence and goes on renewing every lock it holds while
  # its heartbeat sits frozen. Taking the fast path against it is a double-serve window.
  #
  # THE FINDING'S OWN FIX IS WRONG AND IS NOT IMPLEMENTED. It proposed gating on
  # `lock_expires_at_ms <= now`. `judge_previous/1` marks a predecessor dead the moment its
  # HEARTBEAT is stale past `steal_margin_ms` — and at that instant a genuinely crashed
  # predecessor's LOCKS are still in the FUTURE, because they were renewed shortly before the
  # crash. That condition is therefore false in exactly the case round-2 #34 optimizes, so it would
  # silently revert #34 and reintroduce a per-shard stall on every fast restart.
  #
  # The correct discriminator is "are the locks still being RENEWED", which is what actually
  # separates a frozen predecessor from a live legacy one. This is the same two-read protocol
  # `judge_previous/1` already applies to heartbeats, and `heartbeat.ex` calls that the protocol
  # that prevents fleet-wide split-brain.
  #
  # ONCE PER INCARNATION, NOT PER SHARD. The question is about the PROCESS — a renewal timer
  # renews all of its locks or none — and per-shard would cost a full probe window per shard at
  # restart, undoing #34's win on the very path it exists for. So the first shard to be asked twice,
  # far enough apart, settles it for every lock that incarnation holds.
  #
  # SAMPLED OPPORTUNISTICALLY, never blocking. The first call records the lock it was asked about
  # and answers "not proven" (so the caller falls back to the lock TTL — the safe pre-#34
  # behaviour). A later call about the SAME shard, past the probe window, compares: an unchanged
  # expiry means nobody is renewing. Nothing sleeps, nothing is scheduled, and a shard that is never
  # asked twice simply never gets the fast path.
  #
  # The probe window is derived from OUR OWN lease TTL, and that is sound here in a way it would not
  # be generally: this applies only to a previous incarnation of THIS node, so its renewal cadence
  # is our configuration. `renew_lease` runs every `ttl/3`, plus the steal margin for clock skew.
  @doc false
  @spec fast_steal_ok?(String.t(), String.t(), integer(), integer()) :: boolean()
  def fast_steal_ok?(owner, shard_id, lock_expires_at_ms, now_ms) do
    incarnation_dead?(owner) and quiescent?(owner, shard_id, lock_expires_at_ms, now_ms)
  end

  # The pure half, so the state machine can be tested without a store or a clock.
  #
  # Returns `{verdict, next_state}` where `verdict` is whether the fast path may be taken and
  # `next_state` is what to remember (`:keep` = unchanged).
  @doc false
  @spec judge_quiescence(term(), String.t(), integer(), integer(), pos_integer()) ::
          {boolean(), term()}
  def judge_quiescence(state, shard_id, expiry, now_ms, probe_ms)

  def judge_quiescence(:quiescent, _shard_id, _expiry, _now, _probe), do: {true, :keep}

  # Settled the other way. Sticky: a predecessor observed renewing is a LIVE process, and nothing
  # short of a fresh death proof should make it stealable again — `mark_incarnation_dead/1` writing
  # a new owner string is that fresh proof, and it gets its own entry.
  def judge_quiescence(:renewing, _shard_id, _expiry, _now, _probe), do: {false, :keep}

  def judge_quiescence(nil, shard_id, expiry, now, _probe),
    do: {false, {:sample, shard_id, expiry, now}}

  def judge_quiescence({:sample, shard_id, expiry, at}, shard_id, expiry, now, probe)
      when now - at >= probe do
    {true, :quiescent}
  end

  # Same shard, same window, but the expiry MOVED: something renewed it. That is a live process.
  def judge_quiescence({:sample, shard_id, was, _at}, shard_id, expiry, _now, _probe)
      when was != expiry do
    {false, :renewing}
  end

  # Same shard, unchanged, but not enough time has passed for silence to mean anything yet.
  def judge_quiescence({:sample, shard_id, _e, _at}, shard_id, _expiry, _now, _probe),
    do: {false, :keep}

  # A DIFFERENT shard. Its expiry says nothing about the sampled one, and replacing the sample
  # would restart the clock forever on a node opening many shards — the sample must be allowed to
  # age. Leave it alone.
  def judge_quiescence({:sample, _other, _e, _at}, _shard_id, _expiry, _now, _probe),
    do: {false, :keep}

  defp quiescent?(owner, shard_id, expiry, now_ms) do
    {verdict, next} =
      judge_quiescence(quiescence_state(owner), shard_id, expiry, now_ms, probe_window_ms())

    unless next == :keep, do: put_quiescence(owner, next)
    verdict
  end

  # ETS, not `:persistent_term`: this is written on a sampling path rather than once at boot, and a
  # persistent-term write schedules a literal-area cleanup that scans every process on the node —
  # the cost expert review #36 removed from the shipper budget for the same reason.
  @quiescence_table __MODULE__.Quiescence

  defp quiescence_state(owner) do
    case :ets.lookup(quiescence_table(), owner) do
      [{^owner, state}] -> state
      _ -> nil
    end
  rescue
    ArgumentError -> nil
  end

  defp put_quiescence(owner, state) do
    :ets.insert(quiescence_table(), {owner, state})
    :ok
  rescue
    ArgumentError -> :ok
  end

  # `:ets.whereis/1` answers `:undefined`, not nil, when the table is absent — the `||` idiom
  # dialyzer rejects. Created lazily and owned by whichever process asks first: this table is a
  # CACHE of a re-derivable observation, so losing it costs one extra probe window, never
  # correctness. That is why it does not need a supervised owner the way the shipper budget does.
  defp quiescence_table do
    case :ets.whereis(@quiescence_table) do
      :undefined ->
        :ets.new(@quiescence_table, [:named_table, :public, :set, read_concurrency: true])

      _ ->
        :ok
    end

    @quiescence_table
  end

  @doc false
  @spec reset_quiescence() :: :ok
  def reset_quiescence do
    :ets.delete_all_objects(quiescence_table())
    :ok
  rescue
    ArgumentError -> :ok
  end

  # How long silence has to last before it means nobody is renewing.
  #
  # `renew_lease` runs every `ttl/3`, so anything shorter can catch a live renewer BETWEEN
  # renewals and call it dead. `ttl/2` gives 50% slack over that cadence while staying well under
  # the `ttl + margin` fallback it replaces — which is the whole point, since a probe window at or
  # above the fallback would make the fast path worthless.
  #
  # NO `steal_margin_ms` TERM, deliberately. That margin exists for inter-node CLOCK SKEW, and no
  # clock comparison happens here: both observations are ours, of the same stored `expires_at_ms`
  # field, and the only thing our clock measures is the gap between our own two reads. Adding it
  # would be cargo-culting the neighbouring code and would halve the remaining win.
  defp probe_window_ms do
    ttl = Application.get_env(:fathom, :shard_lease_ttl_ms, 30_000)
    Application.get_env(:fathom, :lease_quiescence_probe_ms, max(div(ttl, 2), 1))
  end

  @doc false
  @spec dead_incarnations() :: MapSet.t(String.t())
  def dead_incarnations,
    do: :persistent_term.get({__MODULE__, :dead_incarnations}, MapSet.new())

  # THE OTHER HALF OF `reset_quiescence/0`, and its absence was a test-isolation hole.
  #
  # `fast_steal_ok?/4` reads TWO globals: the quiescence ETS table, which had a reset, and this
  # `:persistent_term` set, which did not. So a test that called `mark_incarnation_dead/1` left that
  # owner marked dead for every test that ran after it, in the same BEAM, for the rest of the suite —
  # and `mark_incarnation_dead/1` only ever ADDS, so the set could not shrink back on its own.
  #
  # That makes a liveness rule depend on TEST ORDER, which is the one thing a randomly-seeded suite
  # guarantees will vary. `two_stealer_test.exs` had already written the hazard down in a comment
  # ("a process-global :persistent_term that is NEVER cleared between tests and only grows") while
  # investigating a failure it could not attribute; it survives today only because the owner strings
  # in flight happen not to collide, which is luck rather than isolation.
  #
  # Test-only, hence `@doc false`: production WANTS this set to persist for the life of the node —
  # it is how a fast-restarting node remembers that each of its own dead predecessors is stealable
  # without waiting out the lock TTL.
  @doc false
  @spec reset_incarnation_deaths() :: :ok
  def reset_incarnation_deaths do
    :persistent_term.erase({__MODULE__, :dead_incarnations})
    :ok
  end

  @doc """
  Remove orphaned sibling temps of `base` older than `older_than_ms` (expert review
  round-2 #27): externally-killed downloads/snapshots (`Task.shutdown` brutal_kill,
  pull timeouts, the follower's `:kill_task`) strand uniquely-named `.dl.*` /
  `.snap.*` / `.tmp.*` / `.pull*` temps that no fixed-suffix sweeper ever matched —
  an unbounded disk leak that eats the warm-standby density budget. The age gate
  keeps any live sibling work (a concurrent pull's fresh temp) safe. `base` may
  contain wildcards itself (the follower reaps `<cache_dir>/*`). Returns the number
  reaped.
  """
  @spec reap_stale_temps(Path.t(), non_neg_integer()) :: non_neg_integer()
  def reap_stale_temps(base, older_than_ms) do
    cutoff = System.system_time(:second) - div(older_than_ms, 1000)

    # `z` is the compressed-upload temp (`Codec.compress_to_temp/1` writes
    # `<local_path>.z.<n>`) — expert review 2026-08-01 #45. Its own `after File.rm` covers an
    # exception but not an external kill, and the flush task IS killed externally on the
    # terminate path. For a periodic flush `local_path` is `<path>.snap.<n>`, so the temp was
    # already caught by the `snap` glob; for `upload_for_drop/1` it is the LIVE db path, so the
    # temp is `<path>.z.<n>` and matched nothing here — one shard-sized orphan per killed
    # drop-flush, forever.
    # `promote` is the replica-promotion staging temp (`<path>.promote.<n>`, written by both
    # `Fathom.Shard`'s promote-on-open and `Replication.Promote.stage/3`) — expert review
    # 2026-08-24 #27, the identical omission to `z` above and for the identical reason. Each site
    # has an `after` block that removes it, which covers an exception; but `promote_replica/7` runs
    # INLINE in the coordinator's `handle_continue(:open, …)`, and that process is brutally killed
    # on `:shard_shutdown_ms` expiry, on `DynamicSupervisor.terminate_child` from `Shards.stop/1`,
    # and on node death. One full shard-sized orphan (plus its `-wal`/`-shm`) per killed promotion,
    # on a volume nothing ever swept — it eats the density budget and, via
    # `Fathom.Admin.Measurements.disk/0`, the free-space floor the warm cache backs off against.
    for tmp <- Path.wildcard(base <> ".{dl,snap,tmp,pull,z,promote}*"),
        stale_file?(tmp, cutoff),
        reduce: 0 do
      acc ->
        case File.rm(tmp) do
          :ok -> acc + 1
          _ -> acc
        end
    end
  end

  @doc """
  Reap a KNOWN, fixed set of sibling temp `paths` older than `older_than_ms` by a
  direct `File.stat`/`File.rm` on each — **no directory scan**. The O(1) counterpart
  to `reap_stale_temps/2` for the DETERMINISTIC temp names (the coordinator's `.pull`
  family) a cold open must clear off its hot path (expert review 2026-07-14 #2).

  `reap_stale_temps/2`'s `Path.wildcard` cannot prefix-optimize a pattern whose
  filename component holds a `*`, so it full-`readdir`s the (fleet-sized) shard data
  dir on EVERY open — tens-to-hundreds of ms at 30k–105k shards. The uniquely-suffixed
  orphans (`.dl.*` / `.snap.*` / `.tmp.*`) still need that scan, but it now runs
  amortized in `Fathom.Shard.TempReaper`, not per open. Same age-gate semantics: a path
  younger than the cutoff (possibly a live sibling's fresh temp) is left alone. Returns
  the number reaped.
  """
  @spec reap_named_temps([Path.t()], non_neg_integer()) :: non_neg_integer()
  def reap_named_temps(paths, older_than_ms) do
    cutoff = System.system_time(:second) - div(older_than_ms, 1000)

    Enum.reduce(paths, 0, fn path, acc ->
      with true <- stale_file?(path, cutoff),
           :ok <- File.rm(path) do
        acc + 1
      else
        _ -> acc
      end
    end)
  end

  defp stale_file?(path, cutoff) do
    case File.stat(path, time: :posix) do
      {:ok, %File.Stat{mtime: mtime}} -> mtime < cutoff
      _ -> false
    end
  end

  defp sync_file(path) do
    case :file.open(path, [:read, :write, :raw, :binary]) do
      {:ok, fd} ->
        result = :file.sync(fd)
        :ok = :file.close(fd)
        result

      {:error, _} = err ->
        err
    end
  end

  defp backend, do: Application.get_env(:fathom, :shard_storage, Fathom.Shard.Storage.Local)
end
