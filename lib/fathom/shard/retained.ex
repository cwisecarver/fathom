defmodule Fathom.Shard.Retained do
  @moduledoc """
  Keep a shard's local `.db` + provenance sidecar after a CLEAN, DURABLE drop, under a node disk
  budget, so the next open on this node is a warm open instead of a full pull (expert review
  2026-10-20-2027 #22).

  ## Why

  The LB consistent-hashes a tenant back to the same node, so an idle-dropped shard usually reopens
  HERE. A drop used to delete the local file, so every reopen paid the full GET + decode + MD5 +
  fsync + renames. A warm open with `sidecar == object etag` skips all of that: the lease acquire
  and the fork-evidence HEAD overlap, and nothing is transferred.

  ## The files

    * `<db>.retained` — the CLEAN-RETAINED marker. Written by the dropping coordinator only after the
      object is durable and the local file equals it (`drop_local_unless_serving/1`'s clean branch in
      `Fathom.Shard`). It says "no acked write lives only in this file", which is what makes it safe
      to evict without an upload and to discard instead of quarantining on a mismatch.
    * `<db>.retained.open` — the claimed marker. The next open renames `.retained` to this BEFORE it
      looks at the file (`claim/2`), and removes it before the coordinator serves (`settle_claim!/1`).
      While it exists, `Fathom.Shard.Fork.quarantine!/3` DELETES the local copy and the open pulls,
      instead of moving it to `.forked.<ts>`: a mismatch on a clean copy is "the lineage moved on
      elsewhere", not a fork holding acked writes, so a quarantine would only leak disk.

  ## Serialization (the one hazard)

  Eviction deletes files a concurrent open may be about to use. Both the claim and every eviction
  run inside THIS process, so they cannot interleave: an open either claims before the eviction
  (the eviction then finds no marker and skips) or after it (the claim finds no marker and the
  open sees no file). An open only calls in when the marker exists (one `stat`), so the common
  open path — no retained file — never touches this process. If this process is down, a claim
  fails and the open fails retryably rather than racing an eviction that might be in flight.

  The marker on disk is the truth and the in-memory LRU index is a hint: an entry whose marker is
  gone (claimed, erased) is dropped when it is reached. `Fathom.Shard.TempReaper`'s periodic
  directory scan feeds `reconcile/2`, which rebuilds the index after a restart and enforces the
  budget, so the existing reaper is what evicts files this process has never seen.

  ## Budget

  `:shard_retain_local_bytes` (env `SHARD_RETAIN_LOCAL_BYTES`), default `0` = OFF: nothing is
  retained and any retained files found are evicted. LRU by bytes, oldest retain first. A file
  larger than the whole budget is never retained.

  ## What still sees a retained file

  A retained file is NOT an unflushed local copy, so `unflushed_local?/1` is what `Fathom.Shards`
  asks instead of a bare `File.exists?` (drain, flush, export/fork's flush-before). A tenant erase
  removes `<id>.db*`, which includes both markers.
  """
  use GenServer

  require Logger

  @doc "The clean-retained marker for a shard's db path."
  @spec marker(Path.t()) :: Path.t()
  def marker(path), do: path <> ".retained"

  @doc "The claimed marker an open holds until it serves."
  @spec claimed_marker(Path.t()) :: Path.t()
  def claimed_marker(path), do: path <> ".retained.open"

  @doc "The node retention budget in bytes (0 = retention off)."
  @spec budget_bytes() :: non_neg_integer()
  def budget_bytes, do: Application.get_env(:fathom, :shard_retain_local_bytes, 0)

  @doc """
  The smallest file worth retaining (expert review 2026-10-22 r8 #26). `:shard_retain_min_bytes`
  (env `SHARD_RETAIN_MIN_BYTES`) wins when set. Unset, it follows the request arithmetic:

    * replication ON: a retained reopen is lock PUT then one HEAD (2 RTT, no transfer); a cold
      reopen is GET ∥ PUT then the same HEAD (2 RTT + transfer + decode + MD5 + fsync). Retention
      wins at every size, so the floor is 0.
    * replication OFF: a cold reopen is GET ∥ PUT (1 RTT + transfer); a retained one is PUT then
      the post-lease etag HEAD (2 RTT). Retention only wins once the transfer and its local costs
      exceed an RTT, about 1 MiB at real-S3 latency (unmeasured; the BENCH retained-reopen arm is
      what measures it), so the floor is 1 MiB.
  """
  @spec min_bytes() :: non_neg_integer()
  def min_bytes do
    case Application.get_env(:fathom, :shard_retain_min_bytes) do
      n when is_integer(n) and n >= 0 -> n
      _ -> if Fathom.Shard.Replication.Fleet.replicating?(), do: 0, else: 1_048_576
    end
  end

  @doc """
  True iff a local file exists that may hold writes the stored object lacks — i.e. it exists and is
  not marked clean-retained (claimed or not).
  """
  @spec unflushed_local?(Path.t()) :: boolean()
  def unflushed_local?(path) do
    Fathom.FS.exists?(path) and not Fathom.FS.exists?(marker(path)) and
      not Fathom.FS.exists?(claimed_marker(path))
  end

  # --- the dropping coordinator ---

  @doc """
  Called by a coordinator at a clean, durable drop, with no connection open. Retains the file when
  retention is on and the file is self-contained (no WAL frames), else runs `drop.(path)`.
  """
  @spec retain_or_drop(String.t(), Path.t(), (Path.t() -> any())) :: :retained | :dropped
  def retain_or_drop(shard_id, path, drop) do
    case retainable(path) do
      {:ok, bytes} ->
        # No WAL content, so the -wal/-shm are just SQLite scratch: remove them so the retained set
        # is the `.db` + sidecar + marker only.
        Fathom.FS.rm(path <> "-wal")
        Fathom.FS.rm(path <> "-shm")

        case Fathom.FS.write(marker(path), "") do
          :ok ->
            GenServer.cast(__MODULE__, {:retained, shard_id, path, bytes, now_ms()})
            :retained

          {:error, _} ->
            drop.(path)
            :dropped
        end

      :no ->
        drop.(path)
        :dropped
    end
  end

  # Only a file whose content is entirely in the `.db`: a non-empty `-wal` may hold frames the db
  # lacks (a periodic flush uploads a VACUUM INTO of db+wal), and keeping the db without them would
  # serve stale content under a sidecar that names the newer object. A provenance sidecar is
  # required too, or the next open could not tell what the file is.
  defp retainable(path) do
    budget = budget_bytes()
    floor = min_bytes()

    with true <- budget > 0,
         {:ok, %File.Stat{size: bytes}} when bytes <= budget and bytes >= floor <-
           Fathom.FS.stat(path),
         true <- wal_empty?(path),
         true <- Fathom.FS.exists?(path <> ".etag") do
      {:ok, bytes}
    else
      _ -> :no
    end
  end

  defp wal_empty?(path) do
    case Fathom.FS.stat(path <> "-wal") do
      {:error, :enoent} -> true
      {:ok, %File.Stat{size: 0}} -> true
      _ -> false
    end
  end

  # --- the opening coordinator ---

  @doc """
  Claim a retained file before an open looks at it. A no-op (no process call) when there is no
  marker. `{:error, _}` only when a marker exists and the claim could not be serialized — the open
  must then fail (retryably) rather than race an eviction.
  """
  @spec claim(String.t(), Path.t()) :: :ok | {:error, term()}
  def claim(shard_id, path) do
    if Fathom.FS.exists?(marker(path)) do
      try do
        GenServer.call(__MODULE__, {:claim, shard_id, path}, 30_000)
      catch
        :exit, reason -> {:error, {:retained_claim_unavailable, reason}}
      end
    else
      :ok
    end
  end

  @doc "Is this open working on a claimed (clean) retained copy?"
  @spec claimed?(Path.t()) :: boolean()
  def claimed?(path), do: Fathom.FS.exists?(claimed_marker(path))

  @doc """
  Called by the open right before it serves: removes the claimed marker and answers whether there
  was one. RAISES if it cannot be removed — once the coordinator serves, the file takes writes, and
  a surviving marker would let a later mismatch DELETE them.
  """
  @spec settle_claim!(Path.t()) :: boolean()
  def settle_claim!(path) do
    case Fathom.FS.rm(claimed_marker(path)) do
      :ok -> true
      {:error, :enoent} -> false
      {:error, reason} -> raise "retained claim marker not removable: #{inspect(reason)}"
    end
  end

  @doc """
  A pre-open stop (lease held elsewhere, open failed) leaves a claimed, still-clean file behind.
  Put it back under the budget instead of leaving it unevictable.
  """
  @spec restore_claim(String.t(), Path.t()) :: :ok
  def restore_claim(shard_id, path) do
    claimed = claimed_marker(path)

    if Fathom.FS.exists?(claimed) do
      case Fathom.FS.stat(path) do
        {:ok, %File.Stat{size: bytes}} ->
          if Fathom.FS.rename(claimed, marker(path)) == :ok,
            do: GenServer.cast(__MODULE__, {:retained, shard_id, path, bytes, now_ms()})

        _ ->
          Fathom.FS.rm(claimed)
      end
    end

    :ok
  end

  @doc """
  The mismatch branch of a claimed copy: delete it (it holds nothing the store lacks) so the open
  pulls. Same contract as `Fork.quarantine!/3`: `:ok` iff the main file is gone.
  """
  @spec discard(String.t(), Path.t(), atom()) :: :ok | {:error, term()}
  def discard(shard_id, path, reason) do
    Fathom.FS.rm(path <> "-wal")
    Fathom.FS.rm(path <> "-shm")

    case Fathom.FS.rm(path) do
      result when result in [:ok, {:error, :enoent}] ->
        Fathom.FS.rm(path <> ".etag")

        Logger.info(
          "shard #{shard_id}: retained clean copy no longer matches the stored lineage " <>
            "(#{reason}); discarded it and pulling (expert review 2026-10-20-2027 #22)"
        )

        :telemetry.execute([:fathom, :shard, :retained, :discarded], %{count: 1}, %{
          shard_id: shard_id,
          reason: reason
        })

        :ok

      {:error, _} = error ->
        error
    end
  end

  # --- the reaper ---

  @doc """
  Feed one data-dir listing (bare names) from `Fathom.Shard.TempReaper`: index every retained
  file, forget entries whose marker is gone, and evict down to the budget.
  """
  @spec reconcile([String.t()], Path.t()) :: :ok
  def reconcile(names, dir) do
    found =
      for name <- names, String.ends_with?(name, ".db.retained") do
        id = String.replace_suffix(name, ".db.retained", "")
        {id, Path.join(dir, id <> ".db")}
      end

    GenServer.cast(__MODULE__, {:reconcile, found})
  end

  @doc "Synchronously evict down to the budget; returns the bytes still retained. Test seam."
  @spec enforce() :: non_neg_integer()
  def enforce, do: GenServer.call(__MODULE__, :enforce, 30_000)

  @doc "`%{count, bytes}` currently indexed."
  @spec stats() :: %{count: non_neg_integer(), bytes: non_neg_integer()}
  def stats, do: GenServer.call(__MODULE__, :stats)

  @doc false
  def start_link(opts \\ []), do: GenServer.start_link(__MODULE__, opts, name: __MODULE__)

  @impl true
  def init(_opts), do: {:ok, %{entries: %{}, order: :gb_sets.empty(), total: 0}}

  @impl true
  def handle_cast({:retained, id, path, bytes, at}, state) do
    {:noreply, state |> put_entry(id, path, bytes, at) |> evict_over_budget()}
  end

  def handle_cast({:reconcile, found}, state) do
    seen = MapSet.new(found, &elem(&1, 0))

    state =
      Enum.reduce(Map.keys(state.entries), state, fn id, acc ->
        if MapSet.member?(seen, id), do: acc, else: delete_entry(acc, id)
      end)

    state =
      Enum.reduce(found, state, fn {id, path}, acc ->
        if Map.has_key?(acc.entries, id), do: acc, else: index_found(acc, id, path)
      end)

    {:noreply, evict_over_budget(state)}
  end

  @impl true
  def handle_call({:claim, id, path}, _from, state) do
    state = delete_entry(state, id)

    reply =
      case Fathom.FS.rename(marker(path), claimed_marker(path)) do
        :ok -> :ok
        # Evicted (or erased) before we got here: the files are gone, the open sees no file.
        {:error, :enoent} -> :ok
        {:error, reason} -> {:error, {:retained_claim_failed, reason}}
      end

    {:reply, reply, state}
  end

  def handle_call(:enforce, _from, state) do
    state = evict_over_budget(state)
    {:reply, state.total, state}
  end

  def handle_call(:stats, _from, state),
    do: {:reply, %{count: map_size(state.entries), bytes: state.total}, state}

  defp index_found(state, id, path) do
    with {:ok, %File.Stat{size: bytes}} <- Fathom.FS.stat(path),
         {:ok, %File.Stat{mtime: mtime}} <- Fathom.FS.stat(marker(path), time: :posix) do
      put_entry(state, id, path, bytes, mtime * 1000)
    else
      # A marker without its db: a half-finished eviction. Finish it.
      _ ->
        _ = evict_files(path)
        state
    end
  end

  defp put_entry(state, id, path, bytes, at) do
    state = delete_entry(state, id)

    %{
      state
      | entries: Map.put(state.entries, id, {at, bytes, path}),
        order: :gb_sets.add({at, id}, state.order),
        total: state.total + bytes
    }
  end

  defp delete_entry(state, id) do
    case Map.pop(state.entries, id) do
      {nil, _} ->
        state

      {{at, bytes, _path}, entries} ->
        %{
          state
          | entries: entries,
            order: :gb_sets.delete_any({at, id}, state.order),
            total: state.total - bytes
        }
    end
  end

  defp evict_over_budget(state) do
    budget = budget_bytes()

    if state.total > budget and not :gb_sets.is_empty(state.order) do
      {{_at, id}, _} = :gb_sets.take_smallest(state.order)
      {_at, bytes, path} = Map.fetch!(state.entries, id)
      state = delete_entry(state, id)

      case evict_files(path) do
        :evicted ->
          :telemetry.execute([:fathom, :shard, :retained, :evicted], %{bytes: bytes}, %{
            shard_id: id
          })

        _ ->
          :ok
      end

      evict_over_budget(state)
    else
      state
    end
  end

  # Only while the marker still exists: no marker means an open claimed it (or an erase removed
  # it), and the file is not ours to touch. The marker goes LAST, so a crash mid-eviction leaves a
  # marked set that the next claim (cold: no db) or reconcile (finishes the job) handles.
  defp evict_files(path) do
    if Fathom.FS.exists?(marker(path)) do
      Enum.each(["-wal", "-shm"], &Fathom.FS.rm(path <> &1))

      case Fathom.FS.rm(path) do
        result when result in [:ok, {:error, :enoent}] ->
          Fathom.FS.rm(path <> ".etag")
          Fathom.FS.rm(marker(path))
          :evicted

        {:error, reason} ->
          Logger.warning("retained shard file #{path} not evictable: #{inspect(reason)}")
          :kept
      end
    else
      :gone
    end
  end

  defp now_ms, do: System.system_time(:millisecond)
end
