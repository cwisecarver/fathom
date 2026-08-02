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
  """
  @type lease :: %{owner: String.t(), epoch: non_neg_integer(), expires_at_ms: integer()}

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

  # The stored object's current etag WITHOUT transferring the body (an S3 HEAD; the Local
  # double hashes the file). `{:ok, nil}` when no object exists. Used on a warm restart — where
  # the coordinator kept its local copy and skipped the pull — to learn the etag its first
  # fenced flush must match.
  @callback object_etag(shard_id :: String.t()) :: {:ok, String.t() | nil} | {:error, term()}

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

  # Downloads a snapshot's bytes to `local_path` (`{:ok, etag}`, or `{:ok, nil}` if the snapshot is
  # absent) — the read counterpart of `pull` for the `@snap-<id>` namespace. `Fathom.Snapshots.restore`
  # uses it to read a snapshot's `PRAGMA user_version` and refuse a cross-schema-version restore
  # before the destructive copy-back (expert review #7).
  @callback pull_snapshot(
              shard_id :: String.t(),
              snapshot_id :: String.t(),
              local_path :: Path.t()
            ) :: {:ok, String.t() | nil} | {:error, term()}

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
  @spec pull(String.t(), Path.t()) :: {:ok, String.t() | nil} | {:error, term()}
  def pull(shard_id, local_path), do: backend().pull(shard_id, local_path)

  @doc "Unconditional flush of `local_path` (unfenced — see `flush/3` for the coordinator)."
  @spec flush(String.t(), Path.t()) :: :ok | {:error, term()}
  def flush(shard_id, local_path), do: backend().flush(shard_id, local_path)

  @doc "Fenced flush: writes only if the stored object still matches `expected_etag`. See callback."
  @spec flush(String.t(), Path.t(), String.t() | nil) ::
          {:ok, String.t()} | {:error, :superseded} | {:error, term()}
  def flush(shard_id, local_path, expected_etag),
    do: backend().flush(shard_id, local_path, expected_etag)

  @doc "The stored object's current etag (`nil` if absent) without transferring the body."
  @spec object_etag(String.t()) :: {:ok, String.t() | nil} | {:error, term()}
  def object_etag(shard_id), do: backend().object_etag(shard_id)

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

  @doc "Deletes a stored snapshot (idempotent)."
  @spec drop_snapshot(String.t(), String.t()) :: :ok | {:error, term()}
  def drop_snapshot(shard_id, snapshot_id), do: backend().drop_snapshot(shard_id, snapshot_id)

  @doc "Downloads a snapshot's bytes to `local_path` (`{:ok, nil}` if absent). See the callback (#7)."
  @spec pull_snapshot(String.t(), String.t(), Path.t()) ::
          {:ok, String.t() | nil} | {:error, term()}
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

  defp dead_incarnations,
    do: :persistent_term.get({__MODULE__, :dead_incarnations}, MapSet.new())

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
    for tmp <- Path.wildcard(base <> ".{dl,snap,tmp,pull,z}*"),
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
