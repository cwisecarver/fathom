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
  # Both byte-order variants of the WAL magic. Anything else is not a WAL header we understand.
  @magics [0x377F0682, 0x377F0683]

  @type header :: %{
          ckpt_seq: non_neg_integer(),
          salt1: non_neg_integer(),
          size: non_neg_integer()
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
         {:ok, bin} <- :file.pread(fd, 0, @header_bytes),
         :ok <- :file.close(fd) do
      case bin do
        <<magic::32, _fmt::32, _page::32, seq::32, salt1::32, _rest::binary>>
        when magic in @magics ->
          {:ok, %{ckpt_seq: seq, salt1: salt1, size: size}}

        _ ->
          # Not a WAL header. Refusing is the only safe answer: shipping bytes we cannot identify
          # the generation of is precisely the splice this module exists to prevent.
          {:error, :not_a_wal}
      end
    else
      {:error, reason} -> {:error, reason}
      :eof -> {:ok, :empty}
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
