defmodule Fathom.Migrator.ShardMigration do
  @moduledoc """
  The per-shard blue/green migration, as a plain function (the Oban worker in
  `Fathom.Migrator.ShardMigrationJob` is a thin wrapper over this).

  `run/2` brings one shard to `target`:

    1. stand the shard down (`Fathom.Shards.drain/2`) so its latest data is durable
       in storage and its lease is free;
    2. hold a migration lease (`migrator@<node>`) so no node re-opens it mid-copy;
    3. retain the current version (`<shard>@<prev>`) for revert;
    4. pull the live file, build the new version (`Fathom.Migrator.Copy` — copy +
       replay the version's captured SQL + stamp `user_version`);
    5. flush the new file to live and cut the directory over to `target`.

  On any error the lease is released and the live object is left at `prev` (cutover
  never ran), so the shard is unharmed and the job can retry. **Crash-forward:** if
  a prior run flushed the new version but died before cutover, the live file's
  `user_version` already equals `target`, so the re-run skips the copy and just
  cuts over.

  `revert/2` restores a retained version over live and cuts the directory back.
  Returns `:ok` / `{:ok, %{from, to}}`, `{:retry, reason}` (shard busy / lease
  held — try later), or `{:error, reason}`.
  """
  require Logger

  alias Fathom.Migrator.Copy
  alias Fathom.Shard.{Connection, Storage}
  alias Fathom.{Directory, Migrator, Shards}

  @doc """
  Brings `shard_id` to `target` (a no-op if it is already there). `token` uniquely identifies
  this migration OPERATION so its lease owner (`migrator@<node>@<token>`) can't be merged with a
  concurrent revert's via the same-owner reclaim path (finding #9); callers pass the Oban
  `job.id` (stable across a job's retries/snoozes) — the default is a fresh per-call token.
  """
  @spec run(String.t(), non_neg_integer(), term()) ::
          :ok | {:ok, map()} | {:retry, term()} | {:error, term()}
  def run(shard_id, target, token \\ make_token()) do
    # Directory.get, NOT resolve (expert review #40): resolve upserts last_active_at
    # and registers unknown ids, so every rollout/reconcile sweep attempt — including
    # ones that merely snooze — phantom-bumped recency on shards no client touched
    # (corrupting warm-follower targeting, laggard ordering, and over-refusing the
    # revert write-age guard), and a mistyped id minted a bogus active v0 row.
    # Registering genuinely-new shards is the checkout path's job.
    case Directory.get(shard_id) do
      {:ok, %{schema_version: v}} when v >= target -> :ok
      {:ok, _} -> with_lease(shard_id, token, fn lease -> do_run(shard_id, target, lease) end)
      :error -> {:error, :unknown_shard}
    end
  end

  @doc """
  Reverts `shard_id` to `to_version` by restoring its retained copy. Backs up the current live
  version first so the revert is recoverable (finding #13); returns `{:ok, %{from, to}}` where
  `from` is the backed-up version. See `run/3` re `token`.

  **Force-guard (finding #13):** a revert discards every write made on the live version since
  its cutover. If the directory shows activity after `cutover_at` — or the cutover age is
  unknown (`cutover_at` nil) — the revert refuses with
  `{:error, {:writes_since_cutover | :unknown_write_age, details}}` unless `opts` carries
  `force: true`. The refusal is deterministic (retrying won't change it): callers should
  surface it to the operator, not retry.

  Returns plain `:ok` when the revert has already fully completed (directory AND live
  file both at `to_version` — a crashed-after-cutover retry, round-2 #30): re-running
  the destructive path would clobber the retained backup with the restored bytes.
  """
  @spec revert(String.t(), non_neg_integer(), term(), keyword()) ::
          :ok | {:ok, map()} | {:retry, term()} | {:error, term()}
  def revert(shard_id, to_version, token \\ make_token(), opts \\ []) do
    force? = Keyword.get(opts, :force, false)

    with_lease(shard_id, token, fn lease ->
      # Push this node's buffered directory touches down before reading the guard's
      # inputs (expert review #11): a checkout sitting in the Recorder's ≤1s buffer is
      # otherwise invisible to the write-age guard, letting an unforced revert discard
      # real post-cutover activity. Best-effort and node-local — another node's buffer
      # can still lag; the guard remains the safe-direction approximation it documents.
      flush_recorder()

      {current, last_active, cutover_at} = current_state(shard_id)
      tmp = temp_path(shard_id, "revert")

      try do
        # Capture the live object's etag at pull time so the destructive restore can If-Match
        # on it (expert review 2026-07-14 #4): the revert's read-only fence/2 only proves the
        # LOCK is ours at that instant, but the migrator can then stall and a coordinator steal +
        # flush new bytes before the copy-back lands. Threading this etag into restore/3 turns a
        # mid-window steal into :superseded instead of an unconditional clobber (the forward
        # flush/3 already does the same — round-2 #5).
        with {:ok, etag} <- pull_live(shard_id, tmp) do
          # Round-2 #30 (crash-forward, the OTHER half of #10 below): the cutover
          # already completed and the job died before acking — the retry must be a
          # NO-OP. Re-entering the destructive path would retain(current==to_version),
          # copying live over the retained @to_version backup (destroying the recovery
          # copy the next revert restores from), or spuriously log "revert REFUSED".
          if live_version(tmp) == to_version and to_version == current do
            :ok
          else
            do_revert(
              shard_id,
              to_version,
              current,
              last_active,
              cutover_at,
              tmp,
              lease,
              force?,
              etag
            )
          end
        end
      after
        drop_temp(tmp)
      end
    end)
  end

  defp do_revert(
         shard_id,
         to_version,
         current,
         last_active,
         cutover_at,
         tmp,
         lease,
         force?,
         expected_etag
       ) do
    if live_version(tmp) == to_version and to_version != current do
      # Crash-forward (expert review #10): a prior attempt already restored
      # to_version over live but died before the cutover, so the directory still
      # says `current`. Re-running retain(current) here would copy the RESTORED
      # bytes now in live over the <shard>@<current> backup — destroying the only
      # copy of the post-cutover writes that backup exists to preserve (and the
      # write-age guard passes identically on a retry, so nothing else stops it).
      # The live file's own user_version is the truth: just finish the cutover.
      with {:ok, _} <- Directory.cutover(shard_id, to_version) do
        warn_revert(shard_id, current, to_version, last_active)
        {:ok, %{from: current, to: to_version}}
      end
    else
      # Self-fence before the clobbering restore: a steal since acquire means a newer
      # owner is authoritative, so abort rather than overwrite it (finding #7).
      with :ok <- write_age_guard(shard_id, last_active, cutover_at, force?),
           :ok <- fence(shard_id, lease),
           # Back up the live vN object BEFORE overwriting it with vN-1, so the discarded
           # post-cutover writes survive at <shard>@<current> for the retention window
           # instead of being destroyed unrecoverably (finding #13). RevertJob schedules
           # its retirement.
           :ok <- Storage.retain(shard_id, current),
           # If-Match-fence the restore on the live object's pull-time etag (expert review
           # 2026-07-14 #4): the read-only fence/2 above only proves the LOCK is ours at THIS
           # instant, but the migrator can then stall and a coordinator steal + flush new bytes
           # before this copy-back lands. An UNCONDITIONAL restore would clobber the stealer's
           # acknowledged writes with the reverted lineage, and the stealer's next flush would
           # 412 and self-fence away its own writes (check_lease still sees our lock — the restore
           # touched the DATA object, not the lock). A 412 → :superseded aborts here, no clobber.
           :ok <- Storage.restore(shard_id, to_version, expected_etag),
           {:ok, _} <- Directory.cutover(shard_id, to_version) do
        warn_revert(shard_id, current, to_version, last_active)
        {:ok, %{from: current, to: to_version}}
      end
    end
  end

  # The hard guard in front of the destructive restore (finding #13). Directory.cutover stamps
  # cutover_at and last_active_at with the same instant, so strictly-greater last_active_at
  # means the shard was USED since it cut over to the live version — a revert would discard
  # those writes. Approximation, in the safe direction: last_active_at is bumped by checkouts
  # (reads too, via the async Recorder), so this can over-refuse (a read-only shard, or a
  # just-flushed touch) but only under-refuses for writes that bypassed checkout entirely.
  # Runs before retain/restore so a refused revert leaves storage completely untouched.
  defp write_age_guard(_shard_id, _last_active, _cutover_at, true), do: :ok

  defp write_age_guard(shard_id, last_active, cutover_at, false) do
    details = %{shard_id: shard_id, last_active_at: last_active, cutover_at: cutover_at}

    cond do
      # No cutover stamp (pre-column row, or a shard the directory doesn't know): the write-age
      # is unknowable, so fail closed — the operator confirms with force rather than the revert
      # silently assuming "no writes".
      is_nil(cutover_at) or is_nil(last_active) ->
        {:error, {:unknown_write_age, details}}

      DateTime.compare(last_active, cutover_at) == :gt ->
        {:error, {:writes_since_cutover, details}}

      true ->
        :ok
    end
  end

  # A revert throws away every write made on the live version since its cutover. That data is
  # unrecoverable from live once restored, so make the discard LOUD (the review's "surface
  # write-age"): the operator can still recover from the retained <shard>@<from> object.
  defp warn_revert(shard_id, from, to, last_active) do
    Logger.warning(
      "shard #{shard_id}: reverted v#{from} -> v#{to}; writes on v#{from} since cutover are " <>
        "DISCARDED from live but recoverable at #{shard_id}@#{from} for the retention window " <>
        "(last_active_at #{inspect(last_active)})"
    )

    :telemetry.execute(
      [:fathom, :migrator, :revert],
      %{count: 1},
      %{shard_id: shard_id, from: from, to: to, last_active_at: last_active}
    )
  end

  # --- fork-from-template (finding #10) ---

  @doc """
  Births `dst_shard_id` AT `head` from the retained `<template_id>@<head>` snapshot —
  the new-tenant bootstrap. Single-writer-safe (holds the dst shard's lease for the
  whole operation, exactly like `run/3`) and stamps the same version places a real
  migration does: `PRAGMA user_version` in the file (via `Fathom.Migrator.Copy`) and
  the directory row (`Fathom.Directory.cutover/2`). The snapshot carries the
  template's schema + `django_migrations` rows only (the template is schema-only by
  design), so a forked tenant inherits ZERO tenant data.

  Steps: lease the dst → server-side copy the snapshot to the dst live object
  (`Storage.fork_from/3`) → pull it, stamp `user_version = head` (an empty-statement
  `Copy.migrate/4`), fence, and flush it back If-Matched → register + cut the
  directory row over to `head` → release the lease.

  Idempotent / crash-forward: a dst live object already stamped at `head` just
  re-registers the directory row. A dst object at any OTHER version is refused
  (`{:error, :dst_exists}`) — it isn't ours to overwrite. On a failure before the
  stamp lands, the dst live object is dropped (`Storage.drop_live/1`) so the shard
  cold-opens EMPTY next time — never carrying an unstamped schema copy a later
  rollout would corrupt. Never dropped on `:superseded`/fence uncertainty (the
  object may be a new owner's). Returns `{:ok, %{version: head}}`,
  `{:error, :no_template_snapshot}` (no retained snapshot — the caller births
  empty), `{:retry, reason}` (lease contention), or `{:error, reason}`.
  """
  @spec fork(String.t(), String.t(), pos_integer(), term()) ::
          {:ok, map()} | {:retry, term()} | {:error, term()}
  def fork(dst_shard_id, template_id, head, token \\ make_token()) do
    with_lease(dst_shard_id, token, fn lease ->
      do_fork(dst_shard_id, template_id, head, lease)
    end)
  end

  defp do_fork(dst, template, head, lease) do
    case Storage.object_etag(dst) do
      # Genuinely novel: no live object yet — copy the snapshot in, then stamp.
      {:ok, nil} ->
        case Storage.fork_from(template, head, dst) do
          :ok -> stamp_fork(dst, head, lease)
          {:error, reason} -> classify_fork_error(reason)
        end

      # An object already exists. Crash-forward: a prior fork that flushed the
      # stamped copy but died before the directory flip re-registers here; anything
      # else is NOT ours to overwrite — refuse, and the normal cold-open serves it.
      {:ok, _etag} ->
        tmp = temp_path(dst, "fork-check")

        try do
          with {:ok, _etag} <- pull_live(dst, tmp) do
            if live_version(tmp) == head do
              register_fork(dst, head)
            else
              {:error, :dst_exists}
            end
          end
        after
          drop_temp(tmp)
        end

      {:error, reason} ->
        {:error, reason}
    end
  end

  # Pull the freshly-copied object, stamp user_version = head (Copy.migrate with an
  # empty statement chain — the same stamp + wal_checkpoint a real migration runs),
  # fence, and flush it back If-Matched on the pull etag.
  defp stamp_fork(dst, head, lease) do
    old = temp_path(dst, "fork-old")
    new = temp_path(dst, "fork-new")

    stamped =
      try do
        with {:ok, etag} <- pull_live(dst, old),
             :ok <- Copy.migrate(old, new, head, []),
             :ok <- fence(dst, lease),
             {:ok, _new_etag} <- flush_fenced(dst, new, etag) do
          :ok
        end
      after
        drop_temp(old)
        drop_temp(new)
      end

    case stamped do
      :ok ->
        register_fork(dst, head)

      # A steal/supersede means a newer owner may own the object — never drop it.
      {:error, :superseded} = error ->
        error

      # Ownership unconfirmed (transient fence error): don't write (don't drop).
      {:error, {:fence_failed, _}} = error ->
        error

      # The fork failed while the live object is still our unstamped snapshot copy
      # (we hold the lease): remove it so the shard cold-opens EMPTY next time.
      {:error, _} = error ->
        _ = Storage.drop_live(dst)
        error
    end
  end

  # Register the forked shard in the directory AT head. resolve/1 registers a
  # first-sight row (birth IS tenant activity); cutover/2 stamps schema_version +
  # cutover_at — the same directory flip a completed migration performs. If this
  # fails after the stamped flush, the shard self-heals: its file already reads
  # `user_version == head`, so the next lazy-migrate/rollout run crash-forwards
  # straight to the cutover (see run/3's `current == target` branch).
  defp register_fork(dst, head) do
    with {:ok, _} <- Directory.resolve(dst),
         {:ok, _} <- Directory.cutover(dst, head) do
      Logger.info("shard #{dst}: born at v#{head} via fork-from-template")
      {:ok, %{version: head}}
    end
  end

  # A missing snapshot object surfaces backend-specifically: Local's atomic_copy is
  # {:error, :enoent}; S3's CopyObject is a 404 copy status. Both mean "no retained
  # template@head snapshot" — the caller births the shard empty.
  defp classify_fork_error(:enoent), do: {:error, :no_template_snapshot}
  defp classify_fork_error({:s3_copy_status, 404}), do: {:error, :no_template_snapshot}
  defp classify_fork_error(reason), do: {:error, reason}

  # --- lease orchestration ---

  defp with_lease(shard_id, token, fun) do
    case Shards.drain(shard_id) do
      :ok -> hold_lease(shard_id, token, fun)
      {:error, :busy} -> {:retry, :shard_busy}
      {:error, reason} -> {:error, {:drain_failed, reason}}
    end
  end

  defp hold_lease(shard_id, token, fun) do
    case Storage.acquire_lease(shard_id, owner(token), ttl()) do
      {:ok, lease} ->
        # Keep the lock's TTL fresh for the whole (unbounded) copy so the #11 lock-TTL liveness
        # fallback keeps this no-heartbeat owner `:live` and a client checkout is refused instead
        # of stealing the shard mid-copy (finding #7). The pre-flush `fence/2` is the safety net if
        # a steal happens anyway.
        renewer = start_renewer(shard_id, lease)

        try do
          fun.(lease)
        after
          stop_renewer(renewer)
          Storage.release_lease(shard_id, lease)
        end

      {:error, {:held, holder}} ->
        {:retry, {:held, holder}}

      {:error, reason} ->
        {:error, {:lease_unavailable, reason}}
    end
  end

  # A linked process that renews the migrator lease every ttl/3. Unlinked + stopped in the
  # caller's `after`; if the migrator crashes the link tears the renewer down (no orphan). Only
  # `:superseded` stops the loop — the pre-flush `fence/2` is the last-resort fence.
  defp start_renewer(shard_id, lease) do
    ttl = ttl()
    interval = max(div(ttl, 3), 1)
    spawn_link(fn -> renew_loop(shard_id, lease, ttl, interval) end)
  end

  # Expert review 2026-07-14 #9: the loop must distinguish a TRANSIENT store blip
  # (`{:error, reason}` — a store hiccup: retry, don't fence) from real ownership loss
  # (`{:error, :superseded}` — a newer owner took over). It previously stopped on ANY
  # non-`{:ok,_}`, so a single S3 hiccup during a long (unbounded) copy silently ended
  # renewal — the lock's TTL then lapses and a client steals the shard MID-MIGRATION, the
  # exact failure the renewer exists to prevent. Mirror the coordinator's own renewal
  # (`Fathom.Shard`'s `:renew_lease` handler): continue on `{:ok, _}` AND on a transient
  # error, stop only on `:superseded`. The storage behaviour contract documents the same
  # "retry, don't fence" split (`Fathom.Shard.Storage.renew_lease/3`).
  defp renew_loop(shard_id, lease, ttl, interval) do
    receive do
      :stop -> :ok
    after
      interval ->
        result =
          try do
            Storage.renew_lease(shard_id, lease, ttl)
          rescue
            # A raised store error is transient too — don't fence on an exception.
            e -> {:error, e}
          catch
            kind, reason -> {:error, {kind, reason}}
          end

        if renew_continue?(result) do
          # Thread the fresh lease on success; keep the prior lease on a transient retry
          # (its owner/epoch are unchanged — only the expiry lapsed, which the next renew
          # extends). Same as the coordinator's `%{state | lease: lease}` threading.
          next_lease =
            case result do
              {:ok, fresh} -> fresh
              {:error, _transient} -> lease
            end

          renew_loop(shard_id, next_lease, ttl, interval)
        else
          :ok
        end
    end
  end

  # The renew loop's continue/stop decision, split out so the transient-vs-superseded
  # distinction is testable without the loop's timing (expert review 2026-07-14 #9). Only a
  # `:superseded` (real ownership loss) stops renewal; `{:ok, _}` and every transient
  # `{:error, _}` keep it alive so a store blip can't lapse the migration lock.
  @doc false
  @spec renew_continue?({:ok, term()} | {:error, term()}) :: boolean()
  def renew_continue?({:error, :superseded}), do: false
  def renew_continue?({:ok, _}), do: true
  def renew_continue?({:error, _transient}), do: true

  defp stop_renewer(pid) do
    Process.unlink(pid)
    send(pid, :stop)
    :ok
  end

  # Confirm we still own the shard right before a clobbering write (flush / restore / cutover).
  # `:superseded` ⇒ a newer owner took over ⇒ abort so we don't overwrite them (finding #7).
  defp fence(shard_id, lease) do
    case Storage.check_lease(shard_id, lease) do
      :ok -> :ok
      {:error, :superseded} -> {:error, :superseded}
      {:error, reason} -> {:error, {:fence_failed, reason}}
    end
  end

  # --- the migration ---

  defp do_run(shard_id, target, lease) do
    old = temp_path(shard_id, "old")
    new = temp_path(shard_id, "new")

    try do
      with {:ok, etag} <- pull_live(shard_id, old) do
        current = live_version(old)

        cond do
          # Crash-forward: the new version is already live; just finish the cutover.
          current == target ->
            finalize(shard_id, target)

          # The live file is AHEAD of the target (a stale sweep against a shard
          # something else already moved past): replaying target's statements over
          # a newer schema would corrupt it. Error out; nothing was touched.
          current > target ->
            {:error, {:ahead_of_target, current}}

          true ->
            # Round-2 #9: replay EVERY version from current+1 through target, in
            # order — capture records one fleet version per Django migration
            # transaction, so a cold-tail shard 2+ behind is routine, and jumping
            # straight to target applied only target's DDL: silent per-shard schema
            # corruption with all three version stamps agreeing. A missing/yanked
            # intermediate makes the chain unbuildable — error (the shard stays
            # untouched at its old version) rather than half-apply.
            with {:ok, chain} <- statement_chain(current, target) do
              # Marked only once the chain is buildable: an unknown/yanked target
              # must leave the shard's status untouched (#23), and the copy window
              # is what "migrating" pauses anyway.
              Directory.mark_migrating(shard_id)
              forward(shard_id, target, chain, old, new, lease, etag)
            end
        end
      end
    after
      drop_temp(old)
      drop_temp(new)
    end
  end

  defp statement_chain(current, target) do
    Enum.reduce_while((current + 1)..target//1, {:ok, []}, fn v, {:ok, acc} ->
      case Migrator.statements(v) do
        nil -> {:halt, {:error, {:unknown_version, v}}}
        statements -> {:cont, {:ok, [{v, statements} | acc]}}
      end
    end)
    |> case do
      {:ok, acc} -> {:ok, Enum.reverse(acc)}
      {:error, _} = error -> error
    end
  end

  defp forward(shard_id, target, chain, old, new, lease, expected_etag) do
    prev = current_version(shard_id)

    with :ok <- Storage.retain(shard_id, prev),
         :ok <- Copy.migrate_chain(old, new, chain),
         # Self-fence right before the clobbering flush: if we were superseded during the (long)
         # copy, abort instead of overwriting the new owner's object (finding #7).
         :ok <- fence(shard_id, lease),
         # The data flush itself is If-Match-fenced on the live object's etag at pull
         # time (round-2 expert review #5): the read-only `fence/2` above only proves the
         # LOCK is ours at THIS instant, but the migrator can then stall (the renewer loop
         # merely stops on failure) and a coordinator can steal + flush new bytes before
         # this PUT lands. An unconditional PUT would clobber the new owner's object with
         # a migrated copy of the OLD lineage, with zero error signal. If-Match turns that
         # into a 412 → :superseded, and the job retries from a fresh pull.
         {:ok, _new_etag} <- flush_fenced(shard_id, new, expected_etag),
         {:ok, _} <- Directory.cutover(shard_id, target) do
      Logger.info("shard #{shard_id}: migrated v#{prev} -> v#{target}")
      {:ok, %{from: prev, to: target}}
    end
  end

  # Storage.flush/3 returns {:ok, etag} | {:error, :superseded} | {:error, reason};
  # normalize into the with-chain (a bare :superseded aborts the migration cleanly).
  defp flush_fenced(shard_id, path, expected_etag) do
    Storage.flush(shard_id, path, expected_etag)
  end

  defp finalize(shard_id, target) do
    prev = current_version(shard_id)

    with {:ok, _} <- Directory.cutover(shard_id, target) do
      {:ok, %{from: prev, to: target}}
    end
  end

  # --- helpers ---

  defp pull_live(shard_id, path) do
    # Keep the object etag (round-2 expert review #5): the forward flush If-Matches it so
    # a steal that lands between pull and flush is caught instead of clobbered.
    case Storage.pull(shard_id, path) do
      {:ok, etag} -> if File.exists?(path), do: {:ok, etag}, else: {:error, :no_live_object}
      error -> error
    end
  end

  defp current_version(shard_id) do
    case Directory.get(shard_id) do
      {:ok, %{schema_version: v}} -> v
      :error -> 0
    end
  end

  # The live version plus its last-active/cutover timestamps (captured before a revert's
  # cutover overwrites them), for the revert backup, force-guard, and write-age warning.
  defp current_state(shard_id) do
    case Directory.get(shard_id) do
      {:ok, %{schema_version: v, last_active_at: la, cutover_at: co}} -> {v, la, co}
      :error -> {0, nil, nil}
    end
  end

  defp live_version(path) do
    {:ok, conn} = Connection.open(path)

    version =
      case Connection.query(conn, "PRAGMA user_version", []) do
        {:ok, %{rows: [[n]]}} -> n
        _ -> 0
      end

    Connection.close(conn)
    version
  end

  # Synchronous recorder flush, tolerant of the recorder being down (a control-plane
  # blip must not turn a revert into a crash — the guard then just sees the directory
  # as-is, no worse than before).
  defp flush_recorder do
    Fathom.Directory.Recorder.flush()
  catch
    :exit, _ -> 0
  end

  # Per-operation lease owner so a forward and a revert on the same shard can't be merged by the
  # same-owner reclaim branch (finding #9) — the loser sees a foreign owner, gets {:held}, and
  # snoozes/retries after the winner releases.
  defp owner(token), do: "migrator@#{node()}@#{token}"
  defp ttl, do: Application.get_env(:fathom, :shard_lease_ttl_ms, 30_000)

  # A fresh operation token when the caller has no stable id (the direct lazy-migrate path); Oban
  # workers pass `job.id` instead so a snoozed/retried job reclaims its own lock.
  defp make_token, do: System.unique_integer([:positive])

  defp temp_path(shard_id, suffix) do
    dir = Path.join(System.tmp_dir!(), "fathom_migrate")
    File.mkdir_p!(dir)
    Path.join(dir, "#{shard_id}-#{suffix}-#{System.unique_integer([:positive])}.db")
  end

  defp drop_temp(path), do: Enum.each(["", "-wal", "-shm"], &File.rm(path <> &1))
end
