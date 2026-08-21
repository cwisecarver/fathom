defmodule Fathom.Shard.Replication.Wal do
  @moduledoc """
  Reading a SQLite WAL header, so A2 can tell "new frames were appended" from "the WAL was reset".
  See `docs/a2-quorum-replication.md`.

  ## Why file size is not enough

  The obvious delta detector — "the `-wal` file grew, ship the new bytes" — is **wrong**, and wrong
  in the silent direction. A checkpoint does not necessarily shrink the file: SQLite **reuses** it,
  rewriting frames from the beginning with fresh salts while the size stays the same or even grows
  again. A primary that shipped "bytes 5000..9000" after a reset would be shipping frames from a
  *new* generation at offsets a follower interprets as belonging to the old one, splicing two
  unrelated WALs together. The result passes `quick_check` and holds wrong data.

  So the generation is read from the header rather than inferred:

  | offset | bytes | field |
  |---|---|---|
  | 0 | 4 | magic (`0x377f0682` or `0x377f0683`, the two byte-order variants) |
  | 4 | 4 | format version |
  | 8 | 4 | page size |
  | 12 | 4 | **checkpoint sequence number** |
  | 16 | 4 | **salt-1** |
  | 20 | 4 | salt-2 |
  | 24 | 8 | checksum |

  The **checkpoint sequence number** increments every time the WAL is reset, which is exactly the
  `wal_gen` the protocol carries. Salt-1 is read alongside it as a corroborating signal: SQLite
  also changes the salt on reset, so a generation that moved without the salt moving (or the
  reverse) means the header is not what we think it is, and the safe response is to treat the WAL as
  a new generation rather than to append into it.
  """

  @header_bytes 32
  # Frame header: pgno(4) nTruncate(4) salt1(4) salt2(4) checksum1(4) checksum2(4). All big-endian.
  @frame_header_bytes 24
  # Both byte-order variants of the WAL magic. Anything else is not a WAL header we understand.
  @magics [0x377F0682, 0x377F0683]

  @type header :: %{
          ckpt_seq: non_neg_integer(),
          salt1: non_neg_integer(),
          size: non_neg_integer(),
          commit_extent: non_neg_integer()
        }

  @doc """
  Read the header and current size of a WAL file.

  Returns `{:ok, :empty}` when the file is absent or too short to hold a header — a shard that has
  been opened but not yet written. That is deliberately distinct from an error: it is the normal
  state of a quiet tenant, and treating it as a failure would make every idle shard look broken.
  """
  @spec read(Path.t()) :: {:ok, header()} | {:ok, :empty} | {:error, term()}
  def read(path) do
    case File.stat(path) do
      {:ok, %{size: size}} when size >= @header_bytes ->
        read_header(path, size)

      {:ok, _} ->
        {:ok, :empty}

      {:error, :enoent} ->
        {:ok, :empty}

      {:error, reason} ->
        {:error, reason}
    end
  end

  defp read_header(path, size) do
    with {:ok, fd} <- :file.open(path, [:read, :raw, :binary]),
         {:ok, bin} <- :file.pread(fd, 0, @header_bytes) do
      result =
        case bin do
          <<magic::32, _fmt::32, page::32, seq::32, salt1::32, salt2::32, _ck::binary>>
          when magic in @magics ->
            {:ok,
             %{
               ckpt_seq: seq,
               salt1: salt1,
               size: size,
               commit_extent: commit_extent(fd, size, page, salt1, salt2)
             }}

          _ ->
            # Not a WAL header. Refusing is the only safe answer: shipping bytes we cannot identify
            # the generation of is precisely the splice this module exists to prevent.
            {:error, :not_a_wal}
        end

      :file.close(fd)
      result
    else
      {:error, reason} -> {:error, reason}
      :eof -> {:ok, :empty}
    end
  end

  # THE COMMITTED EXTENT, which is NOT the file size (expert review 2026-08-20 #5).
  #
  # SQLite does not shorten the WAL on `ROLLBACK`; it rewinds `mxFrame` in the wal-index and lets
  # the next transaction overwrite the abandoned frame slots in place. So the file length is a
  # HIGH-WATER MARK. Measured on this project's own exqlite: a rolled-back bulk insert took the WAL
  # from 12,392 to 6,909,272 bytes, and the next real COMMIT left it at 6,909,272 — unchanged. A
  # primary keyed on file size therefore ships megabytes of abandoned frames once, records the
  # high-water mark as the follower's position, and then plans `:nothing` for every subsequent
  # commit while replying `:ok` to the tenant. Acknowledged writes that no follower holds.
  #
  # The discriminator is cheap and needs no checksumming: **an aborted transaction never writes a
  # commit frame.** SQLite sets a frame's `nTruncate` to the post-commit database size in pages
  # only on the final frame of a COMMITTED transaction, and zero on every other frame. So the last
  # frame with a non-zero `nTruncate` and this generation's salts is the last commit, and the byte
  # just past it is the committed extent.
  #
  # Walked BACKWARD from the end for cost. In the ordinary case — no rollback in flight — the very
  # last frame is a commit frame, so this is ONE 24-byte read on the commit path. Only after a
  # rollback does it step back over the abandoned frames, and it stops at the first commit it
  # finds. That is why this does not verify frame checksums: doing so would mean reading every page
  # of the WAL on every commit, and the `nTruncate` rule already separates committed from abandoned.
  defp commit_extent(fd, size, page, salt1, salt2) when page > 0 do
    frame_bytes = @frame_header_bytes + page
    frames = div(size - @header_bytes, frame_bytes)
    scan_back(fd, frames, frame_bytes, salt1, salt2)
  end

  # A zero page size is a malformed header. Claim nothing committed rather than guess an extent.
  defp commit_extent(_fd, _size, _page, _s1, _s2), do: @header_bytes

  defp scan_back(_fd, 0, _frame_bytes, _s1, _s2), do: @header_bytes

  defp scan_back(fd, n, frame_bytes, salt1, salt2) do
    at = @header_bytes + (n - 1) * frame_bytes

    case :file.pread(fd, at, @frame_header_bytes) do
      {:ok, <<_pgno::32, truncate::32, fs1::32, fs2::32, _ck::binary>>}
      when truncate != 0 and fs1 == salt1 and fs2 == salt2 ->
        # A commit frame from THIS generation. Everything through it is committed.
        at + frame_bytes

      {:ok, <<_::binary-size(@frame_header_bytes)>>} ->
        scan_back(fd, n - 1, frame_bytes, salt1, salt2)

      # Unreadable tail: keep walking back rather than trusting a partial frame.
      _ ->
        scan_back(fd, n - 1, frame_bytes, salt1, salt2)
    end
  end

  @doc """
  Read `len` bytes starting at `offset` — the frame delta itself.

  `pread` rather than `File.read!` + `binary_part`: a WAL under a busy tenant can be tens of
  megabytes, and reading all of it to slice off the tail would allocate the whole file on every
  commit.
  """
  @spec read_delta(Path.t(), non_neg_integer(), non_neg_integer()) ::
          {:ok, binary()} | {:error, term()}
  def read_delta(_path, _offset, 0), do: {:ok, <<>>}

  def read_delta(path, offset, len) do
    with {:ok, fd} <- :file.open(path, [:read, :raw, :binary]),
         {:ok, bin} <- :file.pread(fd, offset, len),
         :ok <- :file.close(fd) do
      if byte_size(bin) == len do
        {:ok, bin}
      else
        # A short read means the file shrank under us — a checkpoint raced this read. Refuse; the
        # next commit re-reads the header and takes the reset path.
        {:error, :short_read}
      end
    else
      :eof -> {:error, :short_read}
      {:error, reason} -> {:error, reason}
    end
  end

  @doc "Size of the WAL header, which is where generation N's frames begin."
  @spec header_bytes() :: pos_integer()
  def header_bytes, do: @header_bytes
end
