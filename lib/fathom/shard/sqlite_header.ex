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

  @doc """
  Page size, page count and freelist page total from the header (offsets 16, 28 and 36;
  expert review 2026-10-20-2027 #9). The page count is only authoritative when the file is not
  ahead of its WAL, so callers must read it with an empty/folded WAL (the drop route after a
  TRUNCATE checkpoint, the pinned-live route under its empty-WAL check).
  """
  @spec free_pages(Path.t()) ::
          {:ok,
           %{page_size: pos_integer(), page_count: non_neg_integer(), freelist: non_neg_integer()}}
          | {:error, term()}
  def free_pages(path) do
    case File.open(path, [:read, :binary], fn io -> :file.pread(io, 0, 40) end) do
      {:ok,
       {:ok,
        <<"SQLite format 3\0", ps::big-16, _::binary-size(10), count::big-32, _::binary-size(4),
          free::big-32>>}} ->
        {:ok, %{page_size: if(ps == 1, do: 65_536, else: ps), page_count: count, freelist: free}}

      {:ok, other} ->
        {:error, {:free_pages_unreadable, other}}

      {:error, reason} ->
        {:error, reason}
    end
  end
end
