defmodule Fathom.Shard.SqliteHeader do
  @moduledoc """
  Read fields straight from a SQLite file's 100-byte header, WITHOUT opening the
  database (expert review 2026-08-31 #23).

  `Fathom.Shard.Connection.open/1` runs `journal_mode=WAL` + the Django extension
  load + a close-time checkpoint, so opening a `VACUUM INTO` snapshot — which is in
  rollback-journal header mode — WRITES to the file and spawns `-wal`/`-shm`. When
  that same file is the one a restore/drill then promotes, the mutation makes the
  promoted object differ byte-for-byte from the verified snapshot, and pays an
  extension load + checkpoint to read four bytes. Reading the header leaves the file
  untouched.

  `Fathom.Snapshots` and `Fathom.RestoreDrillJob` parsed these bytes identically;
  this is their shared home (self-review 2026-08-31 #2).
  """

  @doc """
  The file's `user_version` (what `PRAGMA user_version` returns), read from header
  offset 60 as a 32-bit big-endian int
  (https://www.sqlite.org/fileformat.html#user_version_number).

  `{:error, _}` if the file is unreadable or is not a SQLite database — the 16-byte
  magic string is matched, so a non-database file is rejected the same way a SQLite
  open would have rejected it.
  """
  @spec user_version(Path.t()) :: {:ok, integer()} | {:error, term()}
  def user_version(path) do
    case File.open(path, [:read, :binary], fn io -> :file.pread(io, 0, 64) end) do
      {:ok, {:ok, <<"SQLite format 3\0", _::binary-size(44), v::signed-big-32>>}} -> {:ok, v}
      {:ok, other} -> {:error, {:user_version_unreadable, other}}
      {:error, reason} -> {:error, reason}
    end
  end
end
