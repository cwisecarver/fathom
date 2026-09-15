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

    * `start_pull/2` spawns a crash-isolated `Task` that cold-pulls the object into `<path>.pull`
      via `Storage.pull/2`. (The warm-follower cache fast path was removed 2026-09-14 when
      WarmFollower was retired in favour of A2 — see the warm-follower-removal history.)
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
        Storage.pull(shard_id, temp)
      rescue
        e -> {:error, {:pull_exception, e}}
      catch
        :exit, reason -> {:error, {:pull_exit, reason}}
      end
    end)
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
end
