defmodule Fathom.Shard.Materializer do
  @moduledoc """
  Makes a shard's bytes locally available — the pull / warm-promotion / open-materialization path,
  extracted from `Fathom.Shard` (2026-09-13, Phase 4 of the coordinator decomposition).

  This is the one concern that BLOCKS ON THE NETWORK (the S3 GET), which is why it already ran in a
  `Task` inside the coordinator and is the natural module to lift out — the same reasoning that put
  the replication ship path in its own `Session` process. It touches only the shard id, the on-disk
  path, and the provenance sidecar; its outputs into coordinator state are the object `etag` and the
  materialized file. It holds no coordinator state.

  Flow, all overlapped with the lease acquire and promoted to the live path only after the lease
  confirms (fence-first — we serve only once we own the shard):

    * `start_pull/2` spawns a crash-isolated `Task` that fills `<path>.pull`.
    * `warm_or_cold_pull/2` uses the warm-follower cache when it can validate freshness (a 304
      copies with no full transfer), else a cold `Storage.pull`.
    * `await_pull/3` resolves the object etag for the flush fence — from the speculative pull's
      temp (`promote_pull/2` lands it), or, on a warm restart with no pull, from the local copy's
      PROVENANCE sidecar (never "whatever the store holds now", which let a stale fork clobber a
      newer lineage).
    * `abandon_pull/2` kills the speculative pull and drops its temp when the lease is lost.

  The `@pull_timeout` here is duplicated in `Fathom.Shard` (which needs it as a compile-time
  constant for its open/checkout budget math); keep the two in sync.
  """

  require Logger

  alias Fathom.Shard.Fork
  alias Fathom.Shard.Provenance
  alias Fathom.Shard.Storage
  alias Fathom.Shard.WarmFollower

  @pull_timeout 60_000

  @doc "Temp path the speculative pull fills before promotion. Also used by the lease re-pull."
  @spec pull_temp(Path.t()) :: Path.t()
  def pull_temp(path), do: path <> ".pull"

  # Pull into a temp file, off the init process. Wrapped so the task can never
  # crash the coordinator — it always returns `:ok | {:error, reason}`.
  @spec start_pull(String.t(), Path.t()) :: Task.t()
  def start_pull(shard_id, path) do
    temp = pull_temp(path)

    Task.async(fn ->
      try do
        warm_or_cold_pull(shard_id, temp)
      rescue
        e -> {:error, {:pull_exception, e}}
      catch
        :exit, reason -> {:error, {:pull_exit, reason}}
      end
    end)
  end

  # Fill `temp` with the shard's current bytes. When the warm-follower holds a cached
  # copy for this shard, validate its freshness against storage before promoting it — a
  # warm cache may lag the owner's latest flush, so a stale copy must NEVER be served:
  #
  #   * `:unchanged` (304) — the cache equals storage's current object: copy it into
  #     `temp` with no full transfer (the warm-standby fast path). If the follower
  #     evicted it in the gap, fall back to a cold pull so we never promote a gone file.
  #   * `{:written, _}` (200) — the cache was stale: `temp` already holds the fresh bytes.
  #   * `:absent` (404) — no object (brand-new shard): leave `temp` absent.
  #
  # With no validatable warm copy (no cache / no recorded etag) it's an ordinary cold
  # pull — unchanged from before the warm follower existed. This runs speculatively,
  # overlapped with the lease acquire; `temp` is promoted to the live path only after
  # the lease confirms, so fence-first still holds (we only serve once we own the shard).
  # Returns `{:ok, etag_or_nil}` (the current object etag, threaded into the coordinator's
  # flush fence — finding #15) or `{:error, reason}`.
  defp warm_or_cold_pull(shard_id, temp) do
    case WarmFollower.cached_etag(shard_id) do
      nil ->
        Storage.pull(shard_id, temp)

      etag ->
        # Identity of the cache file the sidecar `etag` describes, captured BEFORE the
        # freshness check — the promotion re-checks it after the copy (expert review #13).
        pre_stat = warm_cache_stat(shard_id)

        case Storage.pull_if_changed(shard_id, temp, etag) do
          {:ok, :unchanged} ->
            emit_warm(shard_id, :hit)
            # 304 ⇒ the cache equals storage's current object, so `etag` IS the current etag.
            case promote_warm_cache(shard_id, temp, etag, pre_stat) do
              :ok ->
                {:ok, etag}

              {:ok, _} = ok ->
                ok

              # THE COLD-PULL FALLBACK CAN LEGITIMATELY FIND NO OBJECT (expert review 2026-08-20
              # #34). `promote_warm_cache/4` falls back to `Storage.pull/2` on any doubt about the
              # cache, and that function's documented return set includes `{:absent, etag_or_nil}`
              # — the shape review 2026-08-01 #24 introduced SPECIFICALLY because collapsing it
              # into `{:ok, _}` fabricated empty databases. This case matched only `:ok`,
              # `{:ok, _}` and `{:error, _}`, so `{:absent, nil}` raised `CaseClauseError`, was
              # caught by `start_pull/2`'s rescue as `{:error, {:pull_exception, _}}`, and
              # `open_with_lease/8` released the lease and failed the checkout — turning a benign
              # brand-new-shard state into an error and a spurious `[:fathom, :shard, :open,
              # :failed]`.
              #
              # PASSED THROUGH rather than folded to `{:ok, nil}`: `await_pull/3` has its own
              # `{:absent, etag}` clause, and folding would hide the distinction from
              # `promote_pull/2`, which uses it to stamp the "derived from no object" sentinel
              # sidecar so the first flush is a conditional CREATE rather than a blind overwrite.
              #
              # Dialyzer cannot see this class: `Storage.pull/2` dispatches through
              # `backend().pull(...)`, so its success typing is `term()` — the "wrappers over
              # dynamic dispatch are not checked" case AGENTS.md § Typing records.
              {:absent, _} = absent ->
                absent

              {:error, _} = error ->
                error
            end

          {:ok, {:written, new_etag}} ->
            emit_warm(shard_id, :stale)
            {:ok, new_etag}

          {:ok, :absent} ->
            {:ok, nil}

          {:error, _} = error ->
            error
        end
    end
  end

  # A 304 says the cached bytes equal storage's current object — copy them into `temp`
  # (the warm promotion). If the cache vanished under us (the follower evicted it between our
  # etag read and now), fall back to a fresh cold pull (which returns `{:ok, etag}`) rather
  # than promote nothing.
  #
  # TOCTOU (expert review #13): between our 304 and this copy, the follower's poll can
  # atomically swap FRESHER bytes into the cache path (a dying old owner's final flush,
  # landed after our freshness check). The cp would then copy the newer bytes while the
  # coordinator records the OLDER etag as provenance — its first fenced flush 412s
  # against an object that is effectively its own lineage, discarding acknowledged
  # post-open writes with no competing owner. So after the copy, require the cache file
  # to still be the very inode we validated (the follower's atomic_write promotion
  # always replaces the inode) AND its sidecar to still name `etag`; any doubt falls
  # back to a fresh cold pull (spurious transfer, never wrong provenance).
  # `Storage.atomic_copy/2`, NOT a bare `File.cp/2` (expert review 2026-08-24 #9). Every other
  # path that materializes a shard file fsyncs before the rename, and `Storage.with_atomic_temp/2`
  # states why: rename-without-data-fsync is atomic against a process crash but NOT against power
  # loss — afterwards the name can exist with zero-length or partial content (guaranteed on XFS,
  # heuristic on ext4). This 304 fast path was the one exception, and it is the WORST place for it:
  # `promote_pull/2` then writes the provenance sidecar with the store's current etag and renames
  # the temp onto `<id>.db`, so a power cut in that window leaves a torn file whose sidecar MATCHES
  # the stored object. The next open reads `:match`, opens warm, and seeds dirty. A zero-length
  # file is a valid empty SQLite database and `PRAGMA quick_check` returns `ok`, so
  # `verify_and_snapshot/2`'s integrity gate never fires — the tenant is served an empty database and
  # the next periodic flush PUTs it over the good stored object with a valid If-Match.
  #
  # The cold-pull fallback on the `else` branch was already fsynced (`Storage.pull/2` promotes
  # through `promote_temp/2`), so only the warm path — the failover path the follower exists to
  # accelerate — was exposed. The inode/etag TOCTOU re-check below is unaffected and stays.
  defp promote_warm_cache(shard_id, temp, etag, pre_stat) do
    with :ok <- Storage.atomic_copy(WarmFollower.cache_path(shard_id), temp),
         true <- pre_stat != nil and warm_cache_stat(shard_id) == pre_stat,
         ^etag <- WarmFollower.cached_etag(shard_id) do
      :ok
    else
      _ -> Storage.pull(shard_id, temp)
    end
  end

  # Identity triple for the follower's cache file. The inode is the load-bearing part:
  # a swapped-in fresh pull is a rename, which always changes it (mtime/size guard the
  # exotic in-place rewrite).
  defp warm_cache_stat(shard_id) do
    case File.stat(WarmFollower.cache_path(shard_id), time: :posix) do
      {:ok, %File.Stat{inode: inode, mtime: mtime, size: size}} -> {inode, mtime, size}
      {:error, _} -> nil
    end
  end

  # Resolve the shard's current object etag for the flush fence (#15), returning
  # `{:ok, etag_or_nil}` or `{:error, reason}`. `nil` task ⇒ a local copy already existed
  # (warm restart): no pull ran, so fetch the etag with a HEAD so the first fenced flush can
  # If-Match it. Otherwise await the speculative pull and promote its temp into place.
  @spec await_pull(Task.t() | nil, Path.t(), String.t()) ::
          {:ok, String.t() | nil} | {:error, term()}
  def await_pull(nil, path, shard_id) do
    # Warm restart: fence with the local copy's PROVENANCE etag (the sidecar written
    # at pull/flush time), never "whatever the store holds now" (expert review #1) —
    # adopting the current etag let a stale fork flush over a newer lineage with a
    # valid If-Match. Fork.evidence/2 + Fork.resolve/4 already re-pulled a mismatched copy when the
    # store was reachable, so this normally equals the current etag; when the store
    # was unreachable at open, fencing with the provenance etag makes a forked flush
    # 412 (self-fence) instead of clobbering. A missing sidecar is a legacy warm file
    # from before provenance tracking: fall back to adopting the current etag, once.
    case Provenance.read(path) do
      {:ok, etag} ->
        {:ok, etag}

      # Normally unreachable — Fork.resolve/4 already quarantined a corrupt
      # sidecar before warm? could hold — but if it ever surfaces (the quarantine's
      # sidecar rm failed), NEVER adopt-current off unknown provenance (expert
      # review #12); fail the open instead.
      :corrupt ->
        {:error, :sidecar_corrupt}

      # Born here against no stored object (2026-08-01 #2). Fence with nil, which the storage
      # layer renders as `If-None-Match: *`: the first flush can only CREATE. If a peer created
      # the object in the meantime, that flush 412s and self-fences instead of overwriting it.
      # Fork.resolve/4 has already quarantined this case when the store was reachable; this is
      # the fence for when it was not.
      :no_object ->
        {:ok, nil}

      # No sidecar ⇒ unknown provenance. Fork.resolve/4 normally quarantined this and the open
      # came through the COLD path, so reaching here means the quarantine rename FAILED (or
      # :adopt_unprovenanced_warm is on). Adopting the store's current etag is what let a
      # planted or stale file flush over a live lineage with a valid If-Match, so only do it
      # when explicitly configured; otherwise fail the open and leave the copy untouched for
      # an operator.
      :missing ->
        if Fork.adopt_unprovenanced_warm?() do
          Logger.warning(
            "shard #{shard_id}: warm file has no provenance sidecar; adopting current etag " <>
              "(:adopt_unprovenanced_warm)"
          )

          case Storage.object_etag(shard_id) do
            {:ok, etag} -> {:ok, etag}
            {:error, reason} -> {:error, {:etag_unavailable, reason}}
          end
        else
          Logger.error(
            "shard #{shard_id}: warm file has no provenance sidecar and could not be " <>
              "quarantined; refusing to open rather than adopt an unknown lineage"
          )

          {:error, :no_provenance}
        end
    end
  end

  def await_pull(task, path, _shard_id) do
    case Task.yield(task, @pull_timeout) || Task.shutdown(task) do
      {:ok, {:ok, etag}} ->
        promote_pull(path, etag)

      # No bytes written — a brand-new shard, or a steal sentinel (expert review 2026-08-01
      # #24). `promote_pull/2`'s else branch is exactly this case: it stamps the "derived from
      # no object" sentinel sidecar and carries the fence etag, so the first flush is a
      # conditional create rather than a blind overwrite.
      {:ok, {:absent, etag}} ->
        promote_pull(path, etag)

      {:ok, {:error, _} = error} ->
        rm_pull_temp(path)
        error

      nil ->
        rm_pull_temp(path)
        {:error, :pull_timeout}

      {:exit, reason} ->
        rm_pull_temp(path)
        {:error, {:pull_crashed, reason}}
    end
  end

  # A new shard has no object, so the pull wrote no temp — leave the path absent (the first
  # connection creates it empty). Otherwise move the temp into place. Carries the object etag
  # through for the flush fence.
  @spec promote_pull(Path.t(), String.t() | nil) :: {:ok, String.t() | nil} | {:error, term()}
  def promote_pull(path, etag) do
    temp = pull_temp(path)

    if File.exists?(temp) do
      # Remove any stale sidecars BEFORE the pulled file lands (expert review #18): a
      # crash between drop_local's two File.rm calls (db deleted, -wal not yet) leaves
      # an orphan WAL, and SQLite's first open would run WAL recovery against it —
      # replaying a different generation's frames into the freshly pulled database
      # (resurrected deletes, torn pages, or a malformed db, then flushed back as the
      # durable object). A pulled object is always a self-contained checkpointed
      # image, so any sidecars next to it are by definition stale.
      File.rm(path <> "-wal")
      File.rm(path <> "-shm")

      # Establish provenance BEFORE the pulled file becomes authoritative (expert
      # review 2026-07-14 #5). Writing the sidecar AFTER the rename left a crash
      # window: an authoritative warm `.db` with NO sidecar. The next warm open reads
      # `:missing` (Fork.evidence → :no_sidecar ⇒ warm, await_pull(nil) → adopt the store's
      # CURRENT etag), so if another node stole+wrote+released the shard in between
      # (reachable on a persisted/remounted `:shard_data_dir`), the stale local copy
      # is served and fenced with the NEW lineage's etag — its first flush If-Matches
      # and clobbers the newer owner's writes (the #1 clobber, through the promote's
      # crash hole). Sidecar-first inverts the residue to an orphan `<path>.etag` with
      # no `.db`, which is harmless: warm detection gates on File.exists?(path) (the
      # `.db`, see handle_continue's `warm?`), and the brand-new-open branch below
      # File.rm's a stale sidecar before landing. The sidecar path is `<path>.etag`,
      # derived from the FINAL path, so it's writable before the rename.
      Provenance.write(path, etag)

      case File.rename(temp, path) do
        :ok ->
          {:ok, etag}

        {:error, _} = err ->
          err
      end
    else
      # Brand-new shard: no object, so the pull wrote no temp. This used to `File.rm` the
      # sidecar, which left every born-empty shard with NO PROVENANCE BY CONSTRUCTION — and
      # "absent sidecar" was then read as "legacy file, adopt whatever etag the store holds
      # now". That is the clobber the sidecar exists to prevent, reachable with no attacker at
      # all (expert review 2026-08-01 #2):
      #
      #   node A opens a new shard and takes writes, dying before its first flush. The LB
      #   reroutes to B, which serves, flushes (the object now EXISTS), idles and releases
      #   cleanly. A comes back on a persisted :shard_data_dir, sees its local `.db`, reads
      #   `:missing`, opens WARM, adopts the store's current etag — and its first flush
      #   If-Matches successfully, destroying every write B acknowledged.
      #
      # So record the absence EXPLICITLY. `Provenance.no_object_sentinel/0` means "this file was
      # created locally against no stored object", which is a provenance claim the open path can
      # check (see Fork.evidence/2) rather than an absence it has to guess about.
      Provenance.write_no_object(path)
      {:ok, etag}
    end
  end

  # Lease lost / errored: kill the speculative pull and drop its temp file.
  @spec abandon_pull(Task.t() | nil, Path.t()) :: :ok
  def abandon_pull(nil, _path), do: :ok

  def abandon_pull(task, path) do
    Task.shutdown(task, :brutal_kill)
    rm_pull_temp(path)
    :ok
  end

  defp rm_pull_temp(path), do: Enum.each(["", "-wal", "-shm"], &File.rm(pull_temp(path) <> &1))

  # Warm-promotion outcome at cold-open: `:hit` = the follower cache was
  # storage-current and promoted without a full transfer (the warm-standby win);
  # `:stale` = it lagged the owner's latest flush and was re-pulled fresh. Only fires
  # when a validatable warm copy existed.
  defp emit_warm(shard_id, result) do
    :telemetry.execute(
      [:fathom, :shard, :warm, :promoted],
      %{count: 1},
      %{shard_id: shard_id, result: result}
    )
  end
end
