# Runbook — cross-store disaster recovery (Postgres restore coherence)

Fathom's correctness state is split across **two stores that can be restored independently**:

- **Object storage** (S3 / R2 / … or `Local`): shard files, the `{owner,epoch}` lease, snapshots,
  and the DR backstops — `tombstones/<id>` (deleted tenants) and `tokenfloors/<id>` (revocation
  floors).
- **Postgres** (`Fathom.Repo` — the directory / control plane): each shard's `schema_version`,
  `status` (incl. `deleted`), `token_version`, migration registry, and Oban jobs.

The everyday failure mode — **Postgres is *down*** — is safe and covered in
[`operations.md`](operations.md): the data path fails open (no directory resolve on the hot path),
shard *data* durability is unaffected (that's storage), and the control plane simply stalls until
Postgres returns. Nothing here changes for an outage.

This runbook is for the **rarer, more dangerous** event: **Postgres is *restored to an earlier
point in time*** (an RDS PITR after a directory-side incident, a bad migration rolled back, a
snapshot restore). Storage is **not** rolled back with it, so the directory can come back
*desynced* from the durable truth in storage — and, unmitigated, that desync is a correctness
incident, not just lost control-plane state.

## What a Postgres PITR rewinds — and what protects you

| Directory fact rolled back | Unmitigated consequence | Backstop |
|---|---|---|
| a tenant's `deleted` status | the erased subdomain re-mints an **empty shard** on next request (a GDPR erasure failure) | **Storage tombstone** (`tombstones/<id>`, audit #6a): written on delete, never swept by `purge_shard`, and **unioned into the admission ETS at each node's boot**. A node already running keeps its append-only tombstone set; a node booting after the restore re-reads storage. |
| `token_version` (revocation floor) | a **revoked/leaked token verifies again** | **Storage token floor** (`tokenfloors/<id>`, audit #6b): written on revoke/rotate. `HranaAuth.Revocations` is monotonic in memory (a running node never lowers its floor) and unions the storage floor on a cold cache read (a node booting after the restore). |
| `schema_version` | the laggard sweep sees `schema_version < head`, **replays a migration onto an already-migrated file**, hits "already exists", and **quarantines** the tenant | **The shard file's `PRAGMA user_version`** lives in storage, not Postgres, so it is *not* rolled back and is the source of truth. `mix fathom.directory reconcile` realigns the directory to it. |

The through-line, same as everywhere in fathom: **tenant data is never at risk** (it lives in
storage, arbitrated by the lease). The exposure is the directory *disagreeing* with storage about
erasure, revocation, and schema version.

## Recovery procedure

Run this after any Postgres restore that moved the directory backward in time.

1. **Keep the Hrana data-plane port drained / the LB pointed away** while you reconcile — you do not
   want the laggard sweep or novel-shard admission acting on a stale directory. (The Oban crons —
   reconcile, rebalancer, retirement — are the risk; if you can, keep `:rebalancer_enabled` and the
   migration queue paused until step 3 completes.)

2. **Let the boot backstops load.** Restart the fathom nodes (or confirm they were restarted after
   the DB came back). On boot, `Fathom.Tenants.Tombstones` unions the storage tombstones and
   `HranaAuth.Revocations` will union storage token floors on first use — so deletes and revocations
   are already re-enforced at admission/auth even before the directory is fixed.

3. **Reconcile the directory to storage — the completion step:**

   ```bash
   mix fathom.directory reconcile           # dry run: report every drift, change nothing
   mix fathom.directory reconcile --fix      # apply: align the directory to storage
   ```

   `--fix` realigns three facts to their durable-storage truth: `schema_version` ← the file's
   `user_version` (this is what prevents the laggard-quarantine), `token_version` ← the storage
   revocation floor (raised, never lowered), and re-tombstones any storage-`deleted` id the directory
   forgot. It also reports **dangling rows** (a directory row with no stored object) for you to
   triage. It pulls each shard's object, so on a large fleet scope it with `--limit N` and sweep in
   batches, or run it against the recently-active set first.

4. **Verify, then reopen traffic.** Re-run `mix fathom.directory reconcile` (no `--fix`) and confirm
   it reports "coherent — nothing to fix" (modulo dangling rows you've triaged). Un-pause the crons
   and point the LB back.

## Notes and residuals

- **Do the reconcile before the laggard sweep runs.** The one time-sensitive hazard is `ReconcileJob`
  (the hourly migration cron) firing against a rewound `schema_version` and quarantining tenants.
  Pausing the migration queue (step 1) until reconcile (step 3) closes the window; a tenant that does
  get quarantined is recoverable (the quarantine preserves the file — see `docs/migration.md`), but
  it is avoidable.
- **Grace windows are not restored.** A rotation's grace window (`token_version_bumped_at`) is
  transient (default 1h) and directory-sourced; after a restore, treat any in-flight rotations as
  needing a fresh rotate. The hard revocation floor is what the storage backstop guarantees.
- **Forward restores are fine.** If Postgres is restored *forward* (to a later point than storage),
  the monotonic/`max` logic means the directory simply leads storage; reconcile is a no-op for
  token/schema (storage never exceeds it) and the next flush/revoke re-syncs storage.
- **To truly reuse a deleted id** (the documented escape hatch): hard-delete the directory row, delete
  the `tombstones/<id>` storage object, and restart — otherwise the storage backstop re-tombstones it.
