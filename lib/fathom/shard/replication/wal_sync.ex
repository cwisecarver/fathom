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
  4. The round resolves its fd with `hold/2` when it starts: the long-lived fd if the path still
     names the inode it was opened on, else a new one opened BY PATH (rv6 F1: a fresh fd per round
     missed write-back errors another fd had consumed). Either way the inode is the one the path
     names at that moment. It is either the inode holding the round's frames, or a successor. SQLite only
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
  `fdatasync` the WAL at `wal_path` through a FRESH fd, opened by path now. Only the
  replication-off fallback in `Fathom.ShardExecutor` uses it; the Session syncs through its
  long-lived fd (`hold/2`, `sync_held/2`), because a fresh fd misses a write-back error another fd
  already consumed (rv6 F1).

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

  # ---------------------------------------------------------------------------------------------
  # The Session's long-lived sync fd (expert review 2026-10-21 rv6 F1)
  # ---------------------------------------------------------------------------------------------
  #
  # WHY NOT A FRESH FD PER ROUND. Linux reports a write-back error once PER OPEN FILE (errseq_t):
  # an fd samples the inode's error sequence at open, and its fsync/fdatasync reports any error
  # newer than that sample, once. A fresh open samples "unseen" errors as 0, so it does see an error
  # NOBODY has reported yet, but it misses one ANOTHER fd already consumed. The Session's threshold
  # checkpoint and the coordinator's flush checkpoint both fsync this `-wal` through their own
  # SQLite handles. If write-back of a deferred commit's frames failed and one of those checkpoints
  # consumed the EIO, a sync on a fresh fd returns :ok, the poison never fires, and an acked commit
  # is lost on power loss. So the Session opens ONE fd when it first sees a WAL inode and syncs
  # through it every round: an fd that was open when the error happened always gets its own report.
  #
  # Not `:raw`: a raw fd belongs to the process that opened it, and the sync runs in a task so it
  # overlaps the push. A non-raw fd is an io-server process linked to the Session, usable from the
  # task and closed with the Session.
  #
  # The checkpoints' own errors are wired into the poison too (`io_error?/1`): a checkpoint that
  # consumed an EIO from this WAL fails with SQLITE_IOERR, and that is the other half of the cover.

  @typedoc "The Session's held sync fd and the WAL inode it was opened on, or nil."
  @type held :: %{fd: pid(), inode: non_neg_integer()} | nil

  @doc """
  The held sync fd for the `-wal` at `wal_path` as it is NOW, opening one if none is held or the
  path's inode changed. Runs in the Session process, at the start of a sync round.

  An inode change RETIRES the old fd first (`retire/1`): one last fdatasync, then close. Its error
  means the old inode's write-back failed and is returned as `{:error, {:datasync, _}, nil}`, which
  the caller must treat as a poison. `{:error, {:open, _}, held}` means no sync can run this round.

  No open-then-stat race: the caller runs inside a commit round, and the committing stream holds a
  checkout across it, so the coordinator cannot unlink the files and SQLite does not delete the
  `-wal` (that needs the last connection to close). A WAL restart reuses the inode.
  """
  @spec hold(Path.t(), held) :: {:ok, held} | {:error, {:open | :datasync, term()}, held}
  def hold(wal_path, held) do
    case Fathom.FS.stat(wal_path) do
      {:ok, %{inode: inode}} when is_map(held) and held.inode == inode ->
        {:ok, held}

      {:ok, _} ->
        case retire(held) do
          :ok -> open_held(wal_path)
          {:error, reason} -> {:error, reason, nil}
        end

      {:error, reason} ->
        {:error, {:open, reason}, held}
    end
  end

  defp open_held(wal_path) do
    with {:ok, fd} <- :file.open(wal_path, [:read, :binary]),
         {:ok, %{inode: inode}} <- Fathom.FS.stat(wal_path) do
      {:ok, %{fd: fd, inode: inode}}
    else
      {:error, reason} -> {:error, {:open, reason}, nil}
    end
  end

  @doc """
  `fdatasync` through the held fd. Runs in the round's sync task. The `:replication_wal_sync_fun`
  test seam, when set, replaces the syscall exactly as it does in `sync/1`.
  """
  @spec sync_held(Path.t(), %{fd: pid()}) :: :ok | {:error, {:open | :datasync, term()}}
  def sync_held(wal_path, %{fd: fd}) do
    case Application.get_env(:fathom, :replication_wal_sync_fun) do
      fun when is_function(fun, 1) -> fun.(wal_path)
      _ -> datasync(fd)
    end
  end

  @doc """
  Stop holding a sync fd: one last `fdatasync`, then close. Called on a WAL inode change, and by
  the Session's quiet-shard fd release.

  WHY A LAST SYNC MAKES THE RELEASE SAFE. Holding the fd for the Session's whole life would pin the
  unlinked `-wal` of every quiet shard (the leak the quiet-shard release exists to stop). Closing it
  silently would lose the errseq cover for every frame written while it was held. One sync on the
  held fd before the close reports any error for those frames (poison), or proves them on disk. A
  frame written after the close belongs to a later commit round, which reopens the fd before its
  own sync. Between that COMMIT and that round the stream holds a checkout, so the close-time
  checkpoint cannot run; the Session's and the coordinator's checkpoints are covered by
  `io_error?/1`.
  """
  @spec retire(held) :: :ok | {:error, {:datasync, term()}}
  def retire(nil), do: :ok

  def retire(%{fd: fd}) do
    result = datasync(fd)
    _ = :file.close(fd)
    result
  end

  defp datasync(fd) do
    case :file.datasync(fd) do
      :ok -> :ok
      {:error, reason} -> {:error, {:datasync, reason}}
    end
  end

  @doc """
  Whether a checkpoint result is an I/O error, i.e. SQLITE_IOERR (any extended code: SQLite's
  message for all of them is "disk I/O error"). A checkpoint fsyncs the WAL before backfilling, so
  an IOERR there may be the write-back error of a deferred commit's frames, consumed by the
  checkpoint's fd instead of the Session's (expert review 2026-10-21 rv6 F1).
  """
  @spec io_error?(term()) :: boolean()
  def io_error?({:error, msg}) when is_binary(msg),
    do: String.contains?(String.downcase(msg), "disk i/o error")

  def io_error?(_), do: false

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
