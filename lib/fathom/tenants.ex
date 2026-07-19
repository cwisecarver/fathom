defmodule Fathom.Tenants do
  @moduledoc """
  Tenant lifecycle orchestration — the whole-shard operations the migration engine deliberately
  left out of scope: **delete** (GDPR Article 17 erasure / offboarding, #15), **export** (data
  portability, #15), **provision** (explicit create, #21), and **suspend/resume** (administrative
  offline, #20). The admission gates for the last two are `Fathom.Tenants.Tombstones` (permanent)
  and `Fathom.Tenants.Suspensions` (reversible) — ETS sets checked O(1) in `Fathom.Shards.ensure/1`
  off the Postgres hot path.

  A "tenant" is one shard — one SQLite file. Deletion is the hard case: a full erase
  must reach the live stored object + lock, every retained migration version, every
  snapshot, the owner node's local file, every warm-follower cache copy fleet-wide, the
  directory row, and any pending per-shard Oban jobs — and it must leave a **tombstone**
  so novel-shard admission can't silently re-mint the subdomain as an empty shard.

  ## Ordering (why it's safe)

  `delete/1` sets the re-mint guard **first and synchronously** — tombstone the directory
  row, then broadcast so every node's `Fathom.Tenants.Tombstones` ETS set refuses the id
  and drops its warm-follower copy — and only then enqueues `Fathom.Tenants.DeleteJob` for
  the durable, retryable physical erase. So from the instant a delete starts, no new
  stream can open the shard.

  Purging the stored objects is safe even if a coordinator on another node still holds the
  lease: every coordinator flush is **fenced** (`Storage.flush/3` uses `If-Match`), so once
  the live object is gone the next flush 412s and the coordinator self-fences (dropping its
  buffered writes — exactly the erase we want) instead of re-creating the object. The home
  coordinator is drained first so the common single-home case never needs the self-fence.
  """
  require Logger

  import Ecto.Query, only: [from: 2]

  alias Fathom.{Directory, HranaAuth, Shards}
  alias Fathom.Shard.Storage
  alias Fathom.ShardId
  alias Fathom.Tenants.{DeleteJob, Suspensions, Tombstones}

  # Graceful-drain budget when suspending a tenant — in-flight streams finish, then the
  # coordinator flushes + stops; a busy one keeps serving current streams (new opens are denied
  # by the admission gate regardless).
  @suspend_drain_ms 5_000

  # How long a fork waits to drain any LOCAL coordinator for the dst before acquiring its lease (#14),
  # so this node isn't the one holding the lease it's about to acquire. Short — a novel dst usually
  # has no coordinator, and a busy one means dst is genuinely in use (refuse rather than clobber).
  @fork_drain_ms 5_000

  # Per-shard Oban workers a delete must cancel so a queued migration/revert/retirement/
  # handoff can't run against (or resurrect) a shard being erased. The DeleteJob itself is
  # excluded — it must not cancel its own kind.
  @per_shard_workers ~w(
    Fathom.Migrator.ShardMigrationJob
    Fathom.Migrator.RevertJob
    Fathom.Migrator.RetirementJob
    Fathom.Rebalancer.HandoffJob
  )

  @doc """
  Deletes a tenant: tombstones the directory row and broadcasts the tombstone fleet-wide
  (both synchronous, so re-mint is blocked immediately), then enqueues `DeleteJob` for the
  physical erase. Returns `{:ok, :scheduled}`, `{:error, :invalid_shard_id}`, or the
  directory changeset error.
  """
  @spec delete(String.t()) :: {:ok, :scheduled} | {:error, term()}
  def delete(shard_id) do
    with {:ok, id} <- cast(shard_id),
         {:ok, _row} <- Directory.tombstone(id) do
      broadcast_deleted(id)
      enqueue_delete(id)
      {:ok, :scheduled}
    end
  end

  @doc """
  Provisions a tenant explicitly (control-plane #21) instead of waiting for traffic to mint it
  on first request. Registers the directory row (`active`), forks its schema from the template at
  HEAD when `:fork_from_template` is enabled (otherwise it's born empty on first connect, today's
  behavior), and mints a bearer token. Returns
  `%{shard_id, url, auth_token, auth_required}` — `url` is the `libsql://<id>.<base>` endpoint a
  Django/libSQL client points at, `auth_required` reflects whether `:hrana_auth` is on (with it
  off the token is informational — the trust boundary is the network).

  Refuses `{:error, :already_exists}` (a live row), `{:error, :tombstoned}` (a deleted id — the
  resurrection guard), or `{:error, :invalid_shard_id}`.
  """
  @spec provision(String.t()) :: {:ok, map()} | {:error, term()}
  def provision(shard_id) do
    with {:ok, id} <- cast(shard_id),
         :ok <- refuse_if_taken(id),
         {:ok, _row} <- Directory.resolve(id) do
      maybe_fork(id)

      {:ok,
       %{
         shard_id: id,
         url: tenant_url(id),
         auth_token: mint_token(id),
         auth_required: auth_required?()
       }}
    end
  end

  @doc """
  Forks a live tenant to a NEW shard id (#14, the database-forking / tenant-clone kernel): copies
  `src_id`'s stored object to `dst_id`, registers the dst directory row at `src`'s schema version
  (so the laggard sweep never tries to replay a migration onto the already-migrated fork), and mints
  a token. Returns `%{shard_id, url, auth_token, auth_required}` for the dst, like `provision/1`.

  Reflects `src`'s **last durably-flushed** state — pass `flush_source: true` to first force-flush
  `src`'s live coordinator so the fork carries its very latest writes (the keystone-fork case: fork
  a just-migrated template without waiting for the idle flush). The flush is non-disruptive (`src`
  keeps serving) but **local** — see `Fathom.Shards.flush/1`; on a multi-node fleet flush `src` on its
  home node first. Refuses `{:error, :already_exists}`/`{:error, :tombstoned}` (dst taken in the
  directory), `{:error, :dst_exists}` (dst already has a stored object), `{:error, :dst_busy}` (dst is
  being opened/forked concurrently — its lease is held), `{:error, :no_source}` (src has no stored
  object / directory row), the flush error if `flush_source` fails, or `{:error, :invalid_shard_id}`.
  """
  @spec fork(String.t(), String.t(), keyword()) :: {:ok, map()} | {:error, term()}
  def fork(src_id, dst_id, opts \\ []) do
    with {:ok, src} <- cast(src_id),
         {:ok, dst} <- cast(dst_id),
         :ok <- refuse_if_taken(dst),
         {:ok, %{schema_version: schema_version}} <- fetch_src(src),
         :ok <- maybe_flush_source(src, opts),
         :ok <- fork_into_leased_dst(src, dst) do
      register_fork(dst, schema_version)

      {:ok,
       %{
         shard_id: dst,
         url: tenant_url(dst),
         auth_token: mint_token(dst),
         auth_required: auth_required?()
       }}
    end
  end

  # Copy src → dst while HOLDING the dst lease (expert review 2026-07-18 #14). Without it,
  # `refuse_if_taken` (a directory check) then an unconditional `Storage.fork_shard` copy is a TOCTOU:
  # a dst minted organically or by a second fork between the check and the copy is clobbered, and an
  # organic coordinator that already holds the dst lease (with acked writes still in its WAL, not yet
  # flushed) has its first flush 412 and self-fence — silently losing those writes. Mirror
  # `ShardMigration.do_fork`'s guard: drain any local coordinator so THIS node isn't the holder, then
  # `acquire_lease` — an atomic `If-None-Match:*` create on the `.lock` object, so a concurrent
  # open/fork loses the acquire (`{:held, _}`) and is refused instead of racing at the `.db`. A fork
  # is one object copy, well within the lease TTL, so no renewer is needed (unlike the migration copy).
  defp fork_into_leased_dst(src, dst) do
    case Shards.drain(dst, @fork_drain_ms) do
      :ok ->
        owner = fork_owner()

        case Storage.acquire_lease(dst, owner, fork_lease_ttl()) do
          {:ok, lease} ->
            try do
              Storage.fork_shard(src, dst)
            after
              Storage.release_lease(dst, lease)
            end

          # Another node/coordinator holds the dst lease — refuse rather than clobber it.
          {:error, {:held, _holder}} ->
            {:error, :dst_busy}

          {:error, reason} ->
            {:error, {:lease_unavailable, reason}}
        end

      # A local coordinator for dst is busy/undrainable — treat as dst-in-use, don't clobber.
      {:error, :busy} ->
        {:error, :dst_busy}

      {:error, reason} ->
        {:error, {:drain_failed, reason}}
    end
  end

  defp fork_owner, do: "tenant-fork@#{node()}@#{System.unique_integer([:positive])}"
  defp fork_lease_ttl, do: Application.get_env(:fathom, :shard_lease_ttl_ms, 30_000)

  @doc """
  Force-flushes a tenant's live coordinator so its current state is durable in storage, without
  disrupting it (the shard keeps serving) — the flush-before-fork affordance (#10). Returns `:ok`
  (flushed, or already clean, or no coordinator running locally), `{:error, reason}` on a flush
  failure, or `{:error, :invalid_shard_id}`. **Local**: flushes the coordinator on THIS node; see
  `Fathom.Shards.flush/1` for the multi-node caveat.
  """
  @spec flush(String.t()) :: :ok | {:error, term()}
  def flush(shard_id) do
    with {:ok, id} <- cast(shard_id) do
      Shards.flush(id)
    end
  end

  @doc """
  The physical erase, run by `DeleteJob` (and directly callable/testable). Cancels pending
  per-shard jobs, drains the home coordinator, and purges every stored object. Idempotent —
  every step tolerates already-gone state. Returns `:ok`, or `{:error, reason}` on a storage
  failure so the job retries.
  """
  @spec purge(String.t()) :: :ok | {:error, term()}
  def purge(shard_id) do
    with {:ok, id} <- cast(shard_id) do
      cancel_pending_jobs(id)
      # Stop the home coordinator BEFORE deleting storage: while its lease is still valid the
      # shutdown flushes/releases cleanly and never self-fences, so no `.fenced.<ts>` copy of
      # the tenant's data is left behind (the erase's whole point). Then note whether another
      # LIVE node still owns it (it will self-fence on its next fenced flush against the
      # now-gone object — a documented cross-node residual).
      Shards.stop(id)
      note_remote_holder(id)

      case Storage.purge_shard(id) do
        :ok ->
          rm_local(id)

          :telemetry.execute([:fathom, :tenants, :deleted], %{count: 1}, %{shard_id: id})
          :ok

        {:error, reason} ->
          Logger.error("tenant #{id}: storage purge failed (#{inspect(reason)}) — will retry")
          {:error, reason}
      end
    end
  end

  @doc """
  Exports a tenant's data for GDPR portability / offboarding: pulls the shard's current durable
  stored object to a temp file and returns its path + a suggested download filename. Because a
  tenant *is* one SQLite file, this is just a copy of that object — no export format to build.

  Reflects the **last flush**: an active shard may have newer writes still buffered on its
  coordinator (drain or let it idle for the very latest, same caveat as a snapshot). The CALLER
  owns the returned temp file and must delete it after use (the admin download does, so an
  exported copy never lingers on disk). Returns `{:error, :not_stored}` if the shard has no
  stored object (never flushed, or already deleted), or `{:error, :invalid_shard_id}`.
  """
  @spec export(String.t()) :: {:ok, %{path: Path.t(), filename: String.t()}} | {:error, term()}
  def export(shard_id) do
    with {:ok, id} <- cast(shard_id) do
      tmp =
        Path.join(
          System.tmp_dir!(),
          "fathom_export_#{id}_#{System.unique_integer([:positive])}.db"
        )

      case Storage.pull(id, tmp) do
        {:ok, nil} ->
          File.rm(tmp)
          {:error, :not_stored}

        {:ok, _etag} ->
          {:ok, %{path: tmp, filename: "#{id}.db"}}

        {:error, reason} ->
          File.rm(tmp)
          {:error, reason}
      end
    end
  end

  @doc """
  Suspends a tenant (#20): flips the directory row to `suspended`, broadcasts the suspension
  fleet-wide (so every node's admission gate denies new streams immediately), and best-effort
  graceful-drains the home coordinator so it stops serving (in-flight transactions finish; a busy
  coordinator keeps its current streams but all new opens are denied). Reversible via `resume/1`.
  Returns `:ok`, `{:error, :invalid_shard_id}`, `{:error, :not_found}`, or `{:error, :deleted}`
  (a tombstoned tenant is gone, not suspendable).
  """
  @spec suspend(String.t()) :: :ok | {:error, term()}
  def suspend(shard_id) do
    with {:ok, id} <- cast(shard_id),
         {:ok, _row} <- Directory.suspend(id) do
      broadcast_suspension(id, true)
      Shards.drain(id, @suspend_drain_ms)
      :ok
    end
  end

  @doc """
  Resumes a suspended tenant (#20): flips the directory row back to `active` and broadcasts the
  resume so every node's gate stops denying it. Next request cold-opens fresh. Returns `:ok`,
  `{:error, :invalid_shard_id}`, `{:error, :not_found}`, or `{:error, :deleted}`.
  """
  @spec resume(String.t()) :: :ok | {:error, term()}
  def resume(shard_id) do
    with {:ok, id} <- cast(shard_id),
         {:ok, _row} <- Directory.resume(id) do
      broadcast_suspension(id, false)
      :ok
    end
  end

  @doc """
  True if `shard_id` has been deleted (tombstoned). Checked O(1) on the admission path so a
  request for an erased subdomain is refused instead of re-minting an empty shard.
  """
  @spec tombstoned?(String.t()) :: boolean()
  def tombstoned?(shard_id), do: Tombstones.tombstoned?(shard_id)

  @doc "True if `shard_id` is administratively suspended (#20). Checked O(1) on the admission path."
  @spec suspended?(String.t()) :: boolean()
  def suspended?(shard_id), do: Suspensions.suspended?(shard_id)

  @doc """
  Broadcasts a suspend (`suspended: true`) or resume (`false`) fleet-wide: updates THIS node's
  suspension gate immediately, then pushes over Oban's notifier so every other node converges (a
  down node catches up on the periodic reconcile). Best-effort. See `Fathom.Tenants.Suspensions`.
  """
  @spec broadcast_suspension(String.t(), boolean()) :: :ok
  def broadcast_suspension(shard_id, suspended?) do
    if suspended?, do: Suspensions.put(shard_id), else: Suspensions.remove(shard_id)

    Oban.Notifier.notify(Oban, Suspensions.channel(), %{shard_id: shard_id, suspended: suspended?})

    :ok
  rescue
    _ -> :ok
  catch
    :exit, _ -> :ok
  end

  @doc """
  Announces `shard_id`'s deletion fleet-wide: records it in THIS node's tombstone set
  immediately (so re-mint is blocked the instant a delete starts) and pushes it over Oban's
  LISTEN/NOTIFY so every other node tombstones it and purges its warm-follower copy. Best-effort
  — a node that's down misses the push and converges on the periodic tombstone refresh. See
  `Fathom.Tenants.Tombstones`.
  """
  @spec broadcast_deleted(String.t()) :: :ok
  def broadcast_deleted(shard_id) do
    Tombstones.put(shard_id)
    Oban.Notifier.notify(Oban, Tombstones.channel(), %{shard_id: shard_id})
    :ok
  rescue
    _ -> :ok
  catch
    :exit, _ -> :ok
  end

  defp cast(shard_id) do
    case ShardId.cast(shard_id) do
      {:ok, id} -> {:ok, id}
      :error -> {:error, :invalid_shard_id}
    end
  end

  defp refuse_if_taken(id) do
    cond do
      tombstoned?(id) ->
        {:error, :tombstoned}

      true ->
        case Directory.get(id) do
          {:ok, %{status: "deleted"}} -> {:error, :tombstoned}
          {:ok, _row} -> {:error, :already_exists}
          :error -> :ok
        end
    end
  end

  # Fork the new tenant's schema from the template at HEAD when the fork path is enabled (#10);
  # otherwise it's born empty on first connect (today's default). Best-effort — a provision is
  # never failed for a fork miss (the shard just cold-opens empty).
  defp maybe_fork(id) do
    if Application.get_env(:fathom, :fork_from_template, false),
      do: _ = Fathom.Migrator.fork_from_template(id)

    :ok
  rescue
    _ -> :ok
  catch
    :exit, _ -> :ok
  end

  defp mint_token(id) do
    case HranaAuth.token_for(id) do
      {:ok, token} -> token
      _ -> nil
    end
  end

  # Flush src's live coordinator before a fork when `flush_source: true`, so the copy reflects the
  # very latest writes (not just the last idle/periodic flush). A flush error fails the fork — better
  # than silently forking stale bytes.
  defp maybe_flush_source(src, opts) do
    if Keyword.get(opts, :flush_source, false), do: Shards.flush(src), else: :ok
  end

  defp fetch_src(src) do
    case Directory.get(src) do
      {:ok, row} -> {:ok, row}
      :error -> {:error, :no_source}
    end
  end

  # Register the fork's directory row AT the source's schema version — critical so the laggard sweep
  # doesn't see the fork at v0 and replay a migration onto its already-vN copy (the #7/#8 quarantine
  # trap). resolve inserts active@0; cutover stamps the real version (a no-op skip when src is at v0).
  defp register_fork(dst, schema_version) do
    Directory.resolve(dst)
    if schema_version > 0, do: Directory.cutover(dst, schema_version)
    :ok
  end

  defp tenant_url(id), do: "libsql://#{id}.#{base_domain()}"

  defp base_domain do
    case Application.get_env(:fathom, :shard_base_domain) do
      d when is_binary(d) and d != "" -> d
      _ -> "local"
    end
  end

  # `:disabled` is the only "no token needed" mode; unknown values fail closed to required, matching
  # HranaAuth's own normalization.
  defp auth_required?, do: Application.get_env(:fathom, :hrana_auth, :disabled) != :disabled

  defp enqueue_delete(id) do
    %{shard_id: id}
    |> DeleteJob.new()
    |> Oban.insert()
    |> case do
      {:ok, _job} ->
        :ok

      {:error, reason} ->
        # The tombstone already blocks new traffic; a failed enqueue leaves a recoverable
        # half-deleted state that re-running delete/1 (or a manual purge) completes.
        Logger.error("tenant #{id}: failed to enqueue DeleteJob (#{inspect(reason)})")
        :ok
    end
  end

  defp cancel_pending_jobs(id) do
    Oban.cancel_all_jobs(
      from(j in Oban.Job,
        where: j.worker in ^@per_shard_workers,
        # Cancel the safely-cancellable states; an EXECUTING job holds the lease and is
        # handled by the drain + fenced-flush self-fence, not a cancel.
        where: j.state in ["scheduled", "available", "retryable"],
        where: fragment("?->>'shard_id' = ?", j.args, ^id)
      )
    )
  rescue
    _ -> {:ok, 0}
  catch
    :exit, _ -> {:ok, 0}
  end

  # Remove every local working file for the shard after the coordinator is stopped: the live
  # db + its WAL/SHM AND any quarantine copies (`.fenced.<ts>` / `.forked.<ts>` / `.corrupt.<ts>`)
  # a prior partial run or self-fence could have left — otherwise an erased tenant's data
  # lingers on local disk. The glob is anchored at `<id>.db` so it can't match a sibling id
  # (`<id>9.db` doesn't start with `<id>.db`).
  defp rm_local(id) do
    base = Fathom.Shard.db_path(id)
    for path <- Path.wildcard(base <> "*"), do: File.rm(path)
    :ok
  rescue
    _ -> :ok
  end

  defp note_remote_holder(id) do
    case Storage.lease_holder(id) do
      {:held, owner} when owner != nil ->
        # Only a DIFFERENT live node is the cross-node case worth surfacing. Our own
        # just-stopped coordinator can leave the lock a beat before purge_shard deletes it —
        # that's not a remote holder, so don't cry wolf.
        if owner == Fathom.Shard.Heartbeat.owner(), do: :ok, else: warn_remote_holder(id, owner)

      _ ->
        :ok
    end
  rescue
    _ -> :ok
  catch
    :exit, _ -> :ok
  end

  defp warn_remote_holder(id, owner) do
    # A LIVE node still owns the lease — the purge deletes the live object out from under it
    # and that node self-fences on its next fenced flush. Surface it so a cross-node erase
    # relying on the self-fence is observable, not silent.
    Logger.warning(
      "tenant #{id}: purging while #{owner} still holds the lease — it will self-fence " <>
        "on its next flush (cross-node delete)"
    )

    :telemetry.execute([:fathom, :tenants, :purge_while_held], %{count: 1}, %{
      shard_id: id,
      owner: owner
    })
  end
end
