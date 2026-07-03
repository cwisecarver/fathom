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
    case Directory.resolve(shard_id) do
      {:ok, %{schema_version: v}} when v >= target -> :ok
      {:ok, _} -> with_lease(shard_id, token, fn lease -> do_run(shard_id, target, lease) end)
      {:error, _} = error -> error
    end
  end

  @doc """
  Reverts `shard_id` to `to_version` by restoring its retained copy. Backs up the current live
  version first so the revert is recoverable (finding #13); returns `{:ok, %{from, to}}` where
  `from` is the backed-up version. See `run/3` re `token`.
  """
  @spec revert(String.t(), non_neg_integer(), term()) ::
          {:ok, map()} | {:retry, term()} | {:error, term()}
  def revert(shard_id, to_version, token \\ make_token()) do
    with_lease(shard_id, token, fn lease ->
      {current, last_active} = current_state(shard_id)

      # Self-fence before the clobbering restore: a steal since acquire means a newer owner is
      # authoritative, so abort rather than overwrite it (finding #7).
      with :ok <- fence(shard_id, lease),
           # Back up the live vN object BEFORE overwriting it with vN-1, so the discarded
           # post-cutover writes survive at <shard>@<current> for the retention window instead of
           # being destroyed unrecoverably (finding #13). RevertJob schedules its retirement.
           :ok <- Storage.retain(shard_id, current),
           :ok <- Storage.restore(shard_id, to_version),
           {:ok, _} <- Directory.cutover(shard_id, to_version) do
        warn_revert(shard_id, current, to_version, last_active)
        {:ok, %{from: current, to: to_version}}
      end
    end)
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

  # The live version plus its last-active timestamp (captured before a revert's cutover
  # overwrites it), for the revert backup + write-age warning.
  defp current_state(shard_id) do
    case Directory.get(shard_id) do
      {:ok, %{schema_version: v, last_active_at: la}} -> {v, la}
      :error -> {0, nil}
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
