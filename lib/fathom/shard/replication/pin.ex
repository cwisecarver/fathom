defmodule Fathom.Shard.Replication.Pin do
  @moduledoc """
  A read snapshot held on a shard's `.db`, so SQLite's own read marks bound what a checkpoint may do.

  The pin is a read transaction on its own handle: `BEGIN` alone takes no snapshot (it is DEFERRED), the
  read is what acquires the WAL read mark. While it is held a PASSIVE checkpoint never backfills past
  the oldest reader's mark and the log cannot restart, which is the mechanism "Checkpoints of a
  replicated WAL" in `Fathom.Shard.Replication.Session` rests on, and what freezes the main file's
  bytes for the folded-WAL flush route and for a seed's snapshot.

  One primitive, used by every holder (cleanliness audit 2026-11-13 #30; it was written three times,
  in `Session` twice and in `SeedPin`):

    * `open/1` + `release/1`: a `:ro` pin; the Session's threshold checkpoint (`maybe_pin/2`), a seed's
      snapshot, and `SeedPin`'s shared batch pin.
    * `with_pin/2`: a `:ro` pin around a function, for the flush's quorum-gated checkpoint.
    * `with_flush_pin/3`: the flush's own READ-WRITE pin with an early `release` (see its doc for why
      it is not `:ro`).

  Release always rolls the transaction back before closing, and the caller must keep the handle
  referenced for the whole hold: a NIF resource nothing references is finalized at the next GC, which
  releases the snapshot mid-stream (probed in `seed_pinned_snapshot_test.exs`).
  """

  alias Fathom.Shard.Connection

  @doc """
  Take the snapshot on `conn`. `BEGIN` alone takes none (it is DEFERRED); the read is what acquires
  the WAL read mark.
  """
  @spec snapshot(reference()) :: :ok | {:error, term()}
  def snapshot(conn) do
    with :ok <- Connection.exec(conn, "BEGIN") do
      Connection.exec(conn, "SELECT 1 FROM sqlite_schema LIMIT 1")
    end
  end

  @doc """
  End a pin: roll back its transaction and close the handle.
  """
  @spec release(reference()) :: term()
  def release(conn) do
    _ = Connection.rollback_if_open(conn)
    Connection.close(conn)
  end

  @doc """
  Open a `:ro` pin on `db_path`: `{:ok, conn}`, or `{:error, reason}`. A snapshot that could not be
  taken is `{:error, {:snapshot_failed, reason}}` (the handle is closed); a missing file or a failed
  open is that error as-is.

  The file is stat'ed first because a `:ro` `Connection.open/2` MATERIALIZES an absent file, and a
  seed of a shard with no `.db` must fail as it always did, not ship an empty database. A trusted
  handle (no `tenant?`): no authorizer, no statement deadline.
  """
  @spec open(Path.t()) :: {:ok, reference()} | {:error, term()}
  def open(db_path) do
    with {:ok, _} <- Fathom.FS.stat(db_path),
         {:ok, conn} <- Connection.open(db_path, scope: :ro) do
      case snapshot(conn) do
        :ok ->
          {:ok, conn}

        {:error, reason} ->
          Connection.close(conn)
          {:error, {:snapshot_failed, reason}}
      end
    end
  end

  @doc """
  Hold a `:ro` read snapshot on `db_path` around `fun`; `:skipped` if the pin could not be opened.
  The pin's connection is closed only after `fun` returns, and every caller holds another connection to
  the same file across it (a stream's, or the flush's), so this close is never the LAST one: the close
  that runs SQLite's close-time checkpoint and unlinks the WAL.
  """
  @spec with_pin(Path.t(), (-> result)) :: result | :skipped when result: var
  def with_pin(db_path, fun) do
    case open(db_path) do
      {:ok, pin} ->
        try do
          fun.()
        after
          release(pin)
        end

      _ ->
        :skipped
    end
  end

  @doc """
  Run `fun.(pin_conn, release)` while a read snapshot is pinned on `db_path` (expert review
  2026-10-20 #28).

  The pin is a read transaction on its own handle, so no checkpoint can backfill into the
  `.db` while it is held (the checkpointer takes read-lock 0 exclusively to write pages, and
  limits itself to the oldest read mark): the main file's bytes are frozen for the duration, and
  `pin_conn` reads exactly them. `:skipped` if the pin could not be opened.

  Unlike the `:ro` pins of ``FlushCheckpoint`'s pinned checkpoint, this handle is opened READ-WRITE (and only
  ever reads): the flush's caller may hold no other connection, so this close can be the LAST one,
  and the last close must run SQLite's close-time checkpoint and unlink `-wal`/`-shm` exactly as
  the flush's old snapshot connection did. A `:ro` handle leaves a zero-length `-wal` and a stale
  `-shm` behind (`PoolGraceTest` pins the unlink).

  `release` (expert review 2026-10-21 r7 #23) ends the pin EARLY, once `fun` no longer needs the
  main file frozen: an encoding backend has captured the body in its own temp before the PUT, and
  holding the pin through the PUT only stops every checkpoint for the PUT's duration. Idempotent;
  the pin is released on return regardless.
  """
  @spec with_flush_pin(Path.t(), (reference(), (-> :ok) -> result), keyword()) ::
          result | :skipped
        when result: var
  def with_flush_pin(db_path, fun, opts \\ []) when is_function(fun, 2) do
    with {:ok, _} <- Fathom.FS.stat(db_path),
         {:ok, pin} <- Connection.open(db_path) do
      released = :atomics.new(1, [])

      release = fn ->
        if :atomics.compare_exchange(released, 1, 0, 1) == :ok, do: release(pin)
        :ok
      end

      try do
        # `:before_pin` runs on this handle BEFORE its read transaction begins (expert review
        # 2026-10-21 #11: the replication-off flush's PASSIVE, moved ahead of the pin so the pin
        # can find the WAL fully backfilled).
        if before = opts[:before_pin], do: before.(pin)

        case snapshot(pin) do
          :ok -> fun.(pin, release)
          {:error, _} -> :skipped
        end
      after
        release.()
      end
    else
      _ -> :skipped
    end
  end
end
