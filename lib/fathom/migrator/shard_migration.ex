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
  """
  @spec revert(String.t(), non_neg_integer(), term(), keyword()) ::
          {:ok, map()} | {:retry, term()} | {:error, term()}
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
        with :ok <- pull_live(shard_id, tmp) do
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
                 :ok <- Storage.restore(shard_id, to_version),
                 {:ok, _} <- Directory.cutover(shard_id, to_version) do
              warn_revert(shard_id, current, to_version, last_active)
              {:ok, %{from: current, to: to_version}}
            end
          end
        end
      after
        drop_temp(tmp)
      end
    end)
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
  # caller's `after`; if the migrator crashes the link tears the renewer down (no orphan). A
  # renew returning :superseded / error just stops the loop — the pre-flush `fence/2` is the fence.
  defp start_renewer(shard_id, lease) do
    ttl = ttl()
    interval = max(div(ttl, 3), 1)
    spawn_link(fn -> renew_loop(shard_id, lease, ttl, interval) end)
  end

  defp renew_loop(shard_id, lease, ttl, interval) do
    receive do
      :stop -> :ok
    after
      interval ->
        renewed? =
          try do
            match?({:ok, _}, Storage.renew_lease(shard_id, lease, ttl))
          rescue
            _ -> false
          catch
            _, _ -> false
          end

        if renewed?, do: renew_loop(shard_id, lease, ttl, interval), else: :ok
    end
  end

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
    case Migrator.statements(target) do
      nil -> {:error, {:unknown_version, target}}
      statements -> migrate(shard_id, target, statements, lease)
    end
  end

  defp migrate(shard_id, target, statements, lease) do
    Directory.mark_migrating(shard_id)
    old = temp_path(shard_id, "old")
    new = temp_path(shard_id, "new")

    try do
      with :ok <- pull_live(shard_id, old) do
        if live_version(old) == target do
          # Crash-forward: the new version is already live; just finish the cutover.
          finalize(shard_id, target)
        else
          forward(shard_id, target, statements, old, new, lease)
        end
      end
    after
      drop_temp(old)
      drop_temp(new)
    end
  end

  defp forward(shard_id, target, statements, old, new, lease) do
    prev = current_version(shard_id)

    with :ok <- Storage.retain(shard_id, prev),
         :ok <- Copy.migrate(old, new, target, statements),
         # Self-fence right before the clobbering flush: if we were superseded during the (long)
         # copy, abort instead of overwriting the new owner's object (finding #7).
         :ok <- fence(shard_id, lease),
         :ok <- Storage.flush(shard_id, new),
         {:ok, _} <- Directory.cutover(shard_id, target) do
      Logger.info("shard #{shard_id}: migrated v#{prev} -> v#{target}")
      {:ok, %{from: prev, to: target}}
    end
  end

  defp finalize(shard_id, target) do
    prev = current_version(shard_id)

    with {:ok, _} <- Directory.cutover(shard_id, target) do
      {:ok, %{from: prev, to: target}}
    end
  end

  # --- helpers ---

  defp pull_live(shard_id, path) do
    # pull/2 now returns the object etag ({:ok, nil} when absent); the migrator doesn't fence
    # its version copies, so it only cares that the live file materialized.
    case Storage.pull(shard_id, path) do
      {:ok, _etag} -> if File.exists?(path), do: :ok, else: {:error, :no_live_object}
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
