defmodule Fathom.Shard.Replication.WalSync do
  @moduledoc """
  DEFERRED WAL SYNC for replicated shards (expert review 2026-10-21 #7). **Off by default**
  (`:replication_deferred_wal_sync`, env `REPLICATION_DEFERRED_WAL_SYNC`), and it stays off until
  the rig arm and the crash test the commit that added it names have passed.

  ## What it changes

  With the flag off, every tenant `:rw` handle runs `PRAGMA synchronous=FULL`: SQLite fsyncs the
  `-wal` INSIDE the commit, while it still holds the WAL write lock, and only then does the
  executor call `Session.commit/4` for the quorum round. So every writer on a shard is serialized at
  fsync speed, and a replicated commit pays the fsync and the network round trip in sequence.

  With the flag on AND replication on, a tenant `:rw` handle is opened `synchronous=NORMAL`: the
  COMMIT writes its frames to the page cache and returns without a sync. The Session then issues
  ONE `fdatasync` of the `-wal` per commit round, started right after it has gathered the round's
  callers and run concurrently with the push, and it replies to every caller in the round only when
  BOTH the quorum and that fdatasync have succeeded.

  ## The ack-ordering argument (what a reviewer should check)

  1. A tenant write is acked only through `Fathom.ShardExecutor`, and on a deferred handle only
     after `Session.commit(..., sync: true)` (replication on) or `sync/1` in the stream process
     (replication turned off at runtime under a live deferred handle) returned `:ok`.
  2. A caller sends its `Session.commit` request only AFTER its SQLite COMMIT returned, so its
     frames are in the page cache of the `-wal` inode that is current at that moment.
  3. The round's fdatasync task is started only after `coalesce_queued/1` has collected every
     request in the round, i.e. after every one of those COMMITs returned.
  4. `sync/1` opens the `-wal` BY PATH when it runs. The inode it reaches is the one current at
     that time. It is either the inode holding the round's frames, or a successor. SQLite only
     replaces or restarts a WAL after a checkpoint has backfilled every frame and synced the
     database file, and `synchronous=NORMAL` keeps the checkpoint syncs (`walSyncFlags` keeps the
     CKPT bits). So a successor implies the frames are already durable in the `.db`.
  5. The reply is `:ok` only if the quorum said `:ok` AND the task returned `:ok`. Any sync error,
     a sync still running at the deadline, or an earlier round's sync whose result is still unknown
     turns the reply into `{:error, {:wal_sync_failed, _}}`. That reply is never an ack.

  ## Fail-safe on an fsync error

  A failed `fdatasync` POISONS the Session: every later tenant round on it fails with
  `{:error, {:wal_sync_failed, {:poisoned, reason}}}` until the coordinator (and with it the Session)
  is replaced. Failing only the one round is not enough. After an fsync error, Linux may mark the
  dirty pages clean without having written them, so a LATER fdatasync can succeed while the earlier
  frames never reached the disk. SQLite's WAL recovery stops at the first frame whose cumulative
  checksum breaks, so a power loss would then drop the later, acked commits too.

  An OPEN failure (`emfile`, `enoent`) does not poison. No sync ran, the pages are still dirty, and
  the next round's sync covers them. That round still fails, because nothing was synced for it.

  ## What it does NOT change

  * A non-replicating node: `deferred?/0` is false, so handles stay FULL.
  * Fathom's own connections (the coordinator's snapshot, the Session's checkpoint, the migration
    engine): they never pass `:deferred_sync?` and stay FULL.
  * Checkpoints: they still sync the WAL before backfill and the `.db` after, under NORMAL.

  ## Visibility before durability (accepted, 2026-10-21)

  Under FULL a commit becomes visible to other connections only after its WAL fsync. Under this
  mode, another reader can see a commit before it is locally durable. A power loss in that window
  can drop a write a reader saw, though never one a WRITER was acked for. A2 already exposes commits
  before the quorum ack, so the class is not new. The user accepted it as option (a).
  """

  @doc "Whether the flag is set. Says nothing about replication; see `deferred?/0`."
  @spec configured?() :: boolean()
  def configured?, do: Application.get_env(:fathom, :replication_deferred_wal_sync, false) == true

  @doc """
  Whether a tenant `:rw` handle opened NOW should run `synchronous=NORMAL` with the deferred sync.
  Both the flag and replication must be on. With replication off there is no Session round to
  carry the sync, so the handle stays FULL.
  """
  @spec deferred?() :: boolean()
  def deferred?, do: configured?() and Fathom.Shard.Replication.Session.enabled?()

  @doc """
  `fdatasync` the WAL at `wal_path`, opened by path now (see point 4 of the moduledoc).

  `{:error, {:open, reason}}` means no sync ran. `{:error, {:datasync, reason}}` means the kernel
  reported a sync failure, and the caller must poison.

  `:file.datasync/1`, i.e. `fdatasync` on Linux, as the user decision specifies. exqlite builds
  SQLite without `HAVE_FDATASYNC`, so SQLite's own FULL commit calls `fsync` there. `fdatasync`
  still flushes the data and the file size, which is all WAL recovery reads back; it skips only
  timestamps.

  DO NOT JUDGE THIS PATH ON macOS. Measured on the dev Mac (2026-10-21, OTP 29, a real shard WAL
  dirtied by one 500-byte insert): SQLite's whole FULL insert-and-commit p50 46-48 us, while
  `:file.datasync/1` alone p50 166-175 us (`:file.sync/1` 165-171 us; open 14 us, close 2 us).
  The two primitives do not cost the same there, so a macOS A/B measures the primitive, not the
  overlap. The decision is the Linux rig's.
  """
  @spec sync(Path.t()) :: :ok | {:error, {:open | :datasync, term()}}
  def sync(wal_path) do
    case Application.get_env(:fathom, :replication_wal_sync_fun) do
      # Test seam: delay or fail the sync deterministically. Never set in config.
      fun when is_function(fun, 1) -> fun.(wal_path)
      _ -> do_sync(wal_path)
    end
  end

  defp do_sync(wal_path) do
    case :file.open(wal_path, [:read, :raw, :binary]) do
      {:ok, fd} ->
        try do
          case :file.datasync(fd) do
            :ok -> :ok
            {:error, reason} -> {:error, {:datasync, reason}}
          end
        after
          :file.close(fd)
        end

      {:error, reason} ->
        {:error, {:open, reason}}
    end
  end
end
