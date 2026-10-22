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

  require Record
  Record.defrecordp(:file_info, Record.extract(:file_info, from_lib: "kernel/include/file.hrl"))

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

  ## Options

    * `index: true` — derive the commit extent from the `-shm` wal-index header (see "THE COMMIT
      EXTENT FROM THE WAL-INDEX" below) instead of walking frame headers, falling back to the walk
      when the index is absent or unusable. **PRIMARY ONLY**: it is correct only for a `-wal` that
      SQLite itself writes, because SQLite publishes `mxFrame` there on every commit. A follower's
      replica WAL is written by fathom directly, so its `-shm` (if a connection ever mapped one)
      does not track it and would under-report the extent. Default `false`.
  """
  @spec read(Path.t(), keyword()) :: {:ok, header()} | {:ok, :empty} | {:error, term()}
  def read(path, opts \\ []) do
    case stat(path) do
      {:ok, %{size: size}} when size >= @header_bytes ->
        read_header(path, size, Keyword.get(opts, :index, false))

      {:ok, _} ->
        {:ok, :empty}

      {:error, :enoent} ->
        {:ok, :empty}

      {:error, reason} ->
        {:error, reason}
    end
  end

  defp read_header(path, size, index?) do
    with {:ok, fd} <- :file.open(path, [:read, :raw, :binary]),
         {:ok, bin} <- :file.pread(fd, 0, @header_bytes) do
      result =
        case bin do
          <<magic::32, _fmt::32, page::32, seq::32, salt1::32, salt2::32, _ck::binary>>
          when magic in @magics ->
            # The index is read AFTER the WAL header, the order the agreement check needs: SQLite
            # publishes a restart's new salts in the index BEFORE it rewrites the WAL header, so an
            # index read after a header of generation G shows G or a later one, never an earlier.
            extent =
              (index? && one_shot_index_extent(path, page, salt1, salt2)) ||
                commit_extent(fd, size, page, salt1, salt2)

            {:ok, %{ckpt_seq: seq, salt1: salt1, size: max(size, extent), commit_extent: extent}}

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

  # --- held-fd reads (expert review 2026-08-26 #32) --------------------------------------------
  #
  # `read/1` and `read_delta/3` above each do their own `open`+`pread`+`close`. One
  # `Session.commit/3` calls them three times, so a replicated commit paid THREE open/close pairs
  # and TWO stats, all on dirty-IO schedulers, on the tenant's synchronous write path.
  #
  # MEASURED before building, because the finding is explicit that it is a hypothesis: 2 000 reps
  # of the real shapes on a 64 KiB file — today 123.31 µs/commit, a held fd 19.62 µs/commit, a
  # **103.7 µs** saving against the 288 µs `replication_cost_test` attributes to replication. The
  # finding's own threshold for reverting was "under ~10 µs".
  #
  # THE INODE CHECK IS NOT AN OPTIMIZATION DETAIL, IT IS THE SAFETY PROPERTY. A cached fd survives
  # an unlink+recreate: SQLite deletes and recreates `-wal`, and reads on the old fd would keep
  # returning the OLD generation's bytes. `stable?/2` would then compare old-against-old and pass,
  # which is exactly the cross-generation splice this module exists to prevent. Every read
  # revalidates by inode first and reopens on a mismatch.
  #
  # It is also STRICTLY STRONGER than what exists today: nothing currently detects an
  # unlink+recreate between `ship/5`'s `read/1` and the re-check `read/1`, except the salt
  # happening to differ — which is not guaranteed.

  @typedoc """
  What the last header read through a handle found: the generation's identity, its page size, and
  the commit extent. The index fast path reuses `ckpt_seq` while the index still shows these salts.
  """
  @type cursor :: %{
          ckpt_seq: non_neg_integer(),
          salt1: non_neg_integer(),
          salt2: non_neg_integer(),
          page: pos_integer(),
          extent: non_neg_integer(),
          src: :index | :walk,
          blind: non_neg_integer()
        }

  @typedoc """
  A revalidated read handle: the path, the open fd, the inode it was opened on, and (primary
  `-wal` paths only) a read fd on the `-shm` wal-index. `nil` means "no fd held", which is the
  starting state and the state after any error.
  """
  @type handle ::
          %{
            path: Path.t(),
            fd: :file.fd(),
            inode: non_neg_integer(),
            shm: :file.fd() | nil,
            cursor: cursor() | nil
          }
          | nil

  @doc """
  Header + size, read through `handle`, reopening if the file was recreated.

  Returns `{:ok, header, handle}` / `{:ok, :empty, handle}` / `{:error, reason, handle}`. The
  handle comes back in every case so the caller always has somewhere to put it — including on
  error, where it comes back `nil` and closed, so the next commit starts clean.
  """
  @spec read_held(handle(), Path.t()) ::
          {:ok, header(), handle()} | {:ok, :empty, handle()} | {:error, term(), handle()}
  def read_held(handle, path) do
    case stat(path) do
      {:ok, %{size: size, inode: inode}} when size >= @header_bytes ->
        case ensure_fd(handle, path, inode) do
          {:ok, h} -> h |> ensure_shm() |> header_from(size)
          {:error, reason} -> {:error, reason, close_held(handle)}
        end

      {:ok, _} ->
        # Too short to hold a header: a shard opened but not yet written. Release the fd — the file
        # it points at is not the one a later commit will read.
        {:ok, :empty, close_held(handle)}

      {:error, :enoent} ->
        {:ok, :empty, close_held(handle)}

      {:error, reason} ->
        {:error, reason, close_held(handle)}
    end
  end

  @typedoc "What a stability recheck needs: the generation identity, not the commit extent."
  @type identity :: %{ckpt_seq: non_neg_integer(), salt1: non_neg_integer()}

  @doc """
  The cheap trailing recheck of a commit (expert review 2026-10-19 #17): is this still the same WAL
  generation the commit planned against?

  `Session.stable?/3` compares only `ckpt_seq` and `salt1`, but the recheck used to call
  `read_held/2` again, which also walked for the commit extent (one more pread, on a dirty-IO
  scheduler, on the ack path) and then threw it away. This is a path stat plus ONE header pread —
  or, with the `-shm` index held, ONE index pread and no stat (expert review 2026-10-20-2027 #16,
  see the comment above the first clause).

  Everything that makes `read_held/2` safe is kept: the path is stat'd and its inode compared with
  the handle's, so an unlink+recreate reopens the fd and reports the NEW file's identity (never the
  deleted inode's), and an absent or too-short file is `:empty` and releases the fd. The cursor is
  left alone: the next commit's `read_held/2` refreshes it, and a stale cursor only lengthens its
  forward walk, never misreports the extent.

  (The finding also proposed fstat on the held fd for the FIRST read. Measured by counting, that is
  job-neutral — an fstat is one dirty-IO job exactly like the path stat — and it would lose the
  first read's recreate detection, turning every normal WAL recreate into a refused round.)
  """
  @spec recheck_held(handle(), Path.t()) ::
          {:ok, identity(), handle()} | {:ok, :empty, handle()} | {:error, term(), handle()}
  # THE RECHECK FROM THE HELD WAL-INDEX (expert review 2026-10-20-2027 #16): ONE pread, no stat.
  #
  # When the handle holds the `-shm` fd and a cursor from this commit's `read_held/2`, a valid
  # wal-index header (the same two-copy + checksum read `header_from/2` uses) still showing the
  # cursor's salts and page size means the WAL header has not changed since: `ckpt_seq` and the
  # salts are written once per generation, and an in-place restart (`walRestartHdr`) publishes the
  # NEW salts in the index BEFORE it rewrites the WAL header or overwrites any frame. So a payload
  # read between `read_held/2` and this call that straddled a restart always sees the index move,
  # and the round falls back to the stat + header read below — today's path, which then reports the
  # new generation (or, in the window before the header is rewritten, the old one, whose frames are
  # not yet overwritten either) and `Session.stable?/3` decides exactly as before. An unusable index
  # (torn, short, rebuilding) falls back too.
  #
  # WHAT THE DROPPED STAT USED TO CATCH, and why it is not needed here: an unlink+recreate of `-wal`
  # between `read_held/2`'s stat and this recheck. SQLite unlinks `-wal` and `-shm` only in the
  # last connection's close, AFTER checkpointing every frame into the `.db`; nothing ever writes the
  # old inodes again. So the held fds still describe a complete, frozen generation: the index read
  # through the old `-shm` matches the cursor, and the payload read through the old `-wal` is that
  # generation's committed bytes under that generation's own header — consistent, not a splice.
  # Shipping it is the same outcome as a recreate landing just AFTER the old stat-based recheck,
  # which that recheck could never prevent either; the next commit's `read_held/2` stats, sees the
  # new inode and takes the reset path. (Same argument as `read_delta_held/4`'s dropped stat,
  # expert review 2026-10-17 #10a.) Pinned by the collapse test's unlink+recreate variant of #9.
  def recheck_held(%{path: path, shm: shm, cursor: %{} = c} = handle, path) when shm != nil do
    case read_index(shm) do
      {:ok, %{salt1: s1, salt2: s2, page: page}}
      when s1 == c.salt1 and s2 == c.salt2 and page == c.page ->
        {:ok, %{ckpt_seq: c.ckpt_seq, salt1: s1}, handle}

      _ ->
        recheck_stat(handle, path)
    end
  end

  def recheck_held(handle, path), do: recheck_stat(handle, path)

  defp recheck_stat(handle, path) do
    case stat(path) do
      {:ok, %{size: size, inode: inode}} when size >= @header_bytes ->
        case ensure_fd(handle, path, inode) do
          {:ok, h} -> identity_from(h)
          {:error, reason} -> {:error, reason, close_held(handle)}
        end

      {:ok, _} ->
        {:ok, :empty, close_held(handle)}

      {:error, :enoent} ->
        {:ok, :empty, close_held(handle)}

      {:error, reason} ->
        {:error, reason, close_held(handle)}
    end
  end

  @doc """
  `read_delta/3` through `handle`. Same `{:ok, bin, handle}` / `{:error, reason, handle}` shape,
  and the same short-read guard: a shrunken file means a checkpoint raced us, so refuse.
  """
  @spec read_delta_held(handle(), Path.t(), non_neg_integer(), non_neg_integer()) ::
          {:ok, binary(), handle()} | {:error, term(), handle()}
  def read_delta_held(handle, _path, _offset, 0), do: {:ok, <<>>, handle}

  # NO STAT HERE WHEN THE HANDLE IS FOR THIS PATH (expert review 2026-10-17 #10a). The handle came
  # from `read_held/2` microseconds earlier in the same commit, which already stat'd the path and
  # pinned the inode; a second stat per range was one more syscall per commit (17 us single-caller
  # on macOS APFS) to re-confirm it. The recreate-in-the-window case it used to narrow is caught by
  # the caller's trailing `read_held/2`: that stats again, sees a different inode, reopens, and
  # `Session.stable?/3` rejects the round because the new generation's salt differs from the header
  # the payloads were planned against. A pread on the stale fd returns the OLD generation's bytes,
  # which are consistent with that old header, and the recheck still refuses to ship them. A shrunk
  # file (checkpoint truncate) is still caught here: same inode, short pread.
  def read_delta_held(%{path: path, fd: fd} = handle, path, offset, len) do
    case :file.pread(fd, offset, len) do
      {:ok, bin} when byte_size(bin) == len -> {:ok, bin, handle}
      {:ok, _short} -> {:error, :short_read, handle}
      :eof -> {:error, :short_read, handle}
      {:error, reason} -> {:error, reason, close_held(handle)}
    end
  end

  # No held fd (or one for another path): the stat-and-open path, which also pins the inode.
  def read_delta_held(handle, path, offset, len) do
    case stat(path) do
      {:ok, %{inode: inode}} ->
        with {:ok, h} <- ensure_fd(handle, path, inode),
             {:ok, bin} when byte_size(bin) == len <- :file.pread(h.fd, offset, len) do
          {:ok, bin, h}
        else
          {:ok, _short} -> {:error, :short_read, handle}
          :eof -> {:error, :short_read, handle}
          {:error, reason} -> {:error, reason, close_held(handle)}
        end

      {:error, _} ->
        {:error, :short_read, close_held(handle)}
    end
  end

  # RAW STAT (expert review 2026-10-17 #8). Plain `File.stat/1` is a non-raw `:file` call, which is
  # routed through the single `file_server_2` process; `[:raw]` does the syscall in the caller.
  # Measured on OTP 29, macOS APFS: 16.9 us vs 12.8 us single-caller, and 16 concurrent callers
  # (the shape of N tenants committing at once, 2-3 stats per commit) 15.6 us vs 6.4 us per call
  # throughput, i.e. the non-raw path serialises on that one process. Same option the follower
  # already uses for its own stats.
  # (`File.stat/2`'s spec does not admit `:raw`, and passing it made dialyzer drop `read/1`'s
  # return type and cascade through 100+ callers, so this reads the `file_info` record directly.)
  # POSIX TIMES (expert review 2026-10-21 r7 #25): the default `time: :local` converts atime, mtime
  # and ctime to local time on every stat (3 `universaltime_to_localtime` calls, which take the libc
  # tz lock), and only size and inode are read here.
  @spec stat(Path.t()) ::
          {:ok, %{size: non_neg_integer(), inode: non_neg_integer()}} | {:error, :file.posix()}
  defp stat(path) do
    case :file.read_file_info(path, [:raw, {:time, :posix}]) do
      {:ok, info} -> {:ok, %{size: file_info(info, :size), inode: file_info(info, :inode)}}
      {:error, _} = error -> error
    end
  end

  @doc "Closes any held fd. Always returns `nil`, so callers can assign the result directly."
  @spec close_held(handle()) :: nil
  def close_held(nil), do: nil

  def close_held(%{fd: fd} = handle) do
    :file.close(fd)
    if shm = Map.get(handle, :shm), do: :file.close(shm)
    nil
  end

  # Reuse the fd only when BOTH the path and the inode match. A different inode is an
  # unlink+recreate, and reading the old one would splice generations.
  #
  # The `-shm` fd is dropped with the WAL fd and reopened by `ensure_shm/1` on the next
  # `read_held/2`, so it is always opened AFTER the WAL inode it is paired with was observed. That
  # pairing is what makes holding it safe: SQLite unlinks `-shm` only in the same close that
  # deletes `-wal` (`sqlite3WalClose` → `walIndexClose(isDelete)`, shm first, both under an
  # EXCLUSIVE lock on the database), and fathom's own removals (`Materializer.promote_pull/2`,
  # `drop_local/1`) remove both with no connection open. So a `-shm` replaced under a held fd
  # implies a new `-wal` inode, which reopens both here. (Inode-number REUSE defeats this exactly as
  # it already defeats the WAL fd's own check — no new exposure.)
  defp ensure_fd(%{path: path, inode: inode} = handle, path, inode), do: {:ok, handle}

  defp ensure_fd(handle, path, inode) do
    _ = close_held(handle)

    case :file.open(path, [:read, :raw, :binary]) do
      {:ok, fd} -> {:ok, %{path: path, fd: fd, inode: inode, shm: nil, cursor: nil}}
      {:error, reason} -> {:error, reason}
    end
  end

  # Open the wal-index once per WAL fd. Only for a `<db>-wal` path: the handle is the primary's
  # (Session), whose WAL SQLite writes. Absent (no connection has mapped it yet, or a synthetic
  # WAL in a test) is not an error — the extent falls back to the frame walk and the open is
  # retried on the next read. In production a `-wal` holding a commit always has its `-shm`.
  defp ensure_shm(%{shm: nil, path: path} = h) do
    with true <- String.ends_with?(path, "-wal"),
         {:ok, shm} <- :file.open(shm_path(path), [:read, :raw, :binary]) do
      %{h | shm: shm}
    else
      _ -> h
    end
  end

  defp ensure_shm(h), do: h

  defp shm_path(wal_path), do: String.replace_suffix(wal_path, "-wal", "-shm")

  # THE COMMITTED EXTENT THROUGH A HELD FD, WITHOUT THE BACKWARD WALK (expert review 2026-10-01
  # perf #8). After a checkpoint SQLite restarts the WAL IN PLACE: the file keeps its high-water
  # mark and the new generation overwrites it from frame 0, so everything past the extent is stale
  # frames from the old generation. `scan_back/5` starts at EOF and steps back one 24-byte pread
  # per stale frame until it reaches this generation's last commit. Measured on this repo's exqlite
  # with one small commit after a checkpoint: 20 us with no stale tail, 2.1 ms at 1,000 stale
  # frames, 7.6 ms at 4,000 (the default autocheckpoint size) and 49 ms at 25,000, and a commit
  # reads the header at least twice.
  #
  # The held fd remembers the extent it last found and the salts it belongs to. In order:
  #   1. the frame at EOF is a commit of this generation: that is the extent (one pread, the
  #      ordinary no-stale-tail case, same as the first step of `scan_back/5`);
  #   2. the cursor is from THIS generation: walk FORWARD from it. Committed frames of a generation
  #      are never rewritten before the next reset, so the extent only grows from there;
  #   3. the cursor is from an EARLIER generation: a restart happened since, so this generation is
  #      young and its frames start at the header. Walk forward from there;
  #   4. no cursor (a freshly opened fd): fall back to `scan_back/5`.
  # The forward walk stops at the first frame whose salts are not this generation's, which is
  # SQLite's own recovery rule: nothing after an invalid frame is valid.
  #
  # THIS IS NOW THE FALLBACK (expert review 2026-10-20 #1/#17): the wal-index read below answers
  # first, and the walk runs only when the index is absent or unusable. The walk trusts a frame
  # HEADER, which SQLite writes before the frame's page — see "THE COMMIT EXTENT FROM THE
  # WAL-INDEX" below for why that is a torn-frame hazard over a stale tail, and why the index is
  # not.
  defp held_extent(handle, size, page, salt1, salt2) when page > 0 do
    frame_bytes = @frame_header_bytes + page
    frames = div(size - @header_bytes, frame_bytes)
    limit = @header_bytes + frames * frame_bytes

    cond do
      frames > 0 and commit_frame?(handle.fd, limit - frame_bytes, salt1, salt2) ->
        limit

      match?(%{salt1: ^salt1, salt2: ^salt2, extent: ext} when ext <= limit, handle.cursor) ->
        %{extent: ext} = handle.cursor
        scan_forward(handle.fd, ext, limit, frame_bytes, salt1, salt2, ext)

      handle.cursor != nil ->
        scan_forward(handle.fd, @header_bytes, limit, frame_bytes, salt1, salt2, @header_bytes)

      true ->
        scan_back(handle.fd, frames, frame_bytes, salt1, salt2)
    end
  end

  defp held_extent(_handle, _size, _page, _s1, _s2), do: @header_bytes

  defp commit_frame?(fd, at, salt1, salt2) do
    case :file.pread(fd, at, @frame_header_bytes) do
      {:ok, <<_pgno::32, truncate::32, ^salt1::32, ^salt2::32, _ck::binary>>} -> truncate != 0
      _ -> false
    end
  end

  # `extent` is the end of the last commit frame seen so far; `at` the next frame to read.
  defp scan_forward(_fd, at, limit, frame_bytes, _s1, _s2, extent) when at + frame_bytes > limit,
    do: extent

  defp scan_forward(fd, at, limit, frame_bytes, salt1, salt2, extent) do
    case :file.pread(fd, at, @frame_header_bytes) do
      {:ok, <<_pgno::32, truncate::32, ^salt1::32, ^salt2::32, _ck::binary>>} ->
        next = at + frame_bytes

        scan_forward(
          fd,
          next,
          limit,
          frame_bytes,
          salt1,
          salt2,
          if(truncate != 0, do: next, else: extent)
        )

      _ ->
        extent
    end
  end

  defp identity_from(handle) do
    case :file.pread(handle.fd, 0, 20) do
      {:ok, <<magic::32, _fmt::32, _page::32, seq::32, salt1::32>>} when magic in @magics ->
        {:ok, %{ckpt_seq: seq, salt1: salt1}, handle}

      {:ok, _} ->
        {:error, :not_a_wal, close_held(handle)}

      :eof ->
        {:ok, :empty, close_held(handle)}

      {:error, reason} ->
        {:error, reason, close_held(handle)}
    end
  end

  # The held header read, index first (expert review 2026-10-20 #1/#17/#23).
  #
  # STEADY STATE IS ONE PREAD: when the index still shows the cursor's generation (same salts, same
  # page size), the WAL header cannot have changed — `ckpt_seq` and the salts are written once per
  # generation, and a restart publishes NEW salts in the index before it rewrites the WAL header —
  # so `ckpt_seq` comes from the cursor and the WAL header is not read at all. That replaces the
  # old header pread + EOF-frame pread pair, and in a stale-tail cycle the per-frame forward walk
  # (panel 3: 6.0 / 9.2 / 21.6 preads per read at 1 / 4 / 16-row commits; now 1). A replicated
  # commit's raw file jobs go 6 → 5 (stat, index, payload, recheck stat, recheck header), which is
  # what #23 asked of the EOF-probe/payload fusion — with the index there is no EOF probe left
  # to fuse, so #23 is subsumed rather than built. The recheck now reads the index too, 5 → 4
  # (stat, index, payload, index; expert review 2026-10-20-2027 #16).
  #
  # Inode recreation is still caught by the stat in `read_held/2` before this runs; a WAL restart
  # after this read is still caught by the commit's trailing `recheck_held/2`: a restart publishes
  # new salts in the index first, so the recheck's index read misses and it reads the WAL header
  # itself (SQLite rewrites it before overwriting any frame, so a payload that straddles a restart
  # always fails `Session.stable?/3`).
  defp header_from(handle, size) do
    index = read_index(handle.shm)

    case {index, handle.cursor} do
      {{:ok, %{salt1: s1, salt2: s2, page: page} = ix}, %{salt1: s1, salt2: s2, page: page} = c} ->
        extent = index_extent_over(frame_extent(ix.mx, page), c)
        header_reply(handle, c.ckpt_seq, s1, s2, page, size, {extent, :index, 0})

      _ ->
        header_from_wal(handle, size, index)
    end
  end

  defp header_from_wal(handle, size, index) do
    case :file.pread(handle.fd, 0, @header_bytes) do
      {:ok, <<magic::32, _fmt::32, page::32, seq::32, salt1::32, salt2::32, _ck::binary>>}
      when magic in @magics ->
        same =
          case handle.cursor do
            %{salt1: ^salt1, salt2: ^salt2} = c -> c
            _ -> nil
          end

        result =
          case agreeing_extent(handle.shm, index, page, salt1, salt2) do
            nil ->
              walk_or_hold(handle, same, size, page, salt1, salt2)

            ext ->
              {if(same, do: index_extent_over(ext, same), else: ext), :index, 0}
          end

        header_reply(handle, seq, salt1, salt2, page, size, result)

      {:ok, _} ->
        # Same refusal as `read_header/2`: bytes we cannot identify the generation of are the
        # splice this module exists to prevent.
        {:error, :not_a_wal, close_held(handle)}

      :eof ->
        {:ok, :empty, close_held(handle)}

      {:error, reason} ->
        {:error, reason, close_held(handle)}
    end
  end

  # THE INDEX IS UNUSABLE FOR THIS READ (round-4 durability review 2026-10-20 F2). With an `-shm` fd
  # held the index is unusable only transiently (SQLite truncates it while the first connection
  # rebuilds it; three torn reads under heavy commit), and the walk it would fall back to trusts a
  # frame HEADER — the torn-frame hazard of #1. Worse, the cursor keeps the greater extent for the
  # rest of the generation, so one over-count stuck. So: same generation as the cursor and an fd
  # held — report the cursor's extent unchanged (ship nothing new this round; an older extent is a
  # safe under-claim). Bounded by `@max_blind` consecutive rounds so a permanently unusable index
  # (never expected) degrades to the walk instead of stalling replication. Everything else walks,
  # as before, and tags the cursor `:walk` so the next usable index read REPLACES (not max-es
  # against) the walk's possibly over-counted value.
  @max_blind 8
  defp walk_or_hold(%{shm: shm}, %{blind: blind} = same, _size, _page, _s1, _s2)
       when shm != nil and blind < @max_blind,
       do: {same.extent, same.src, blind + 1}

  defp walk_or_hold(handle, same, size, page, salt1, salt2) do
    walked = held_extent(handle, size, page, salt1, salt2)
    # Within one generation the reported extent never moves backward: a cursor taken from the
    # walk can be ahead of the index for the microseconds between SQLite finishing a commit's
    # frames and publishing it, and a smaller extent than a follower already holds would make
    # `Primary.plan/3` order a full reset.
    {if(same, do: max(walked, same.extent), else: walked), :walk, 0}
  end

  # Index extents are monotone within a generation, so they max against an index-derived cursor;
  # a walk-derived cursor extent is unverified and is replaced.
  defp index_extent_over(ext, %{src: :index, extent: c}), do: max(ext, c)
  defp index_extent_over(ext, _walked_cursor), do: ext

  defp header_reply(handle, seq, salt1, salt2, page, size, {extent, src, blind}) do
    cursor = %{
      ckpt_seq: seq,
      salt1: salt1,
      salt2: salt2,
      page: page,
      extent: extent,
      src: src,
      blind: blind
    }

    # `size` is the stat taken before the index read; a commit published in between can put the
    # extent past it. Its frames are on disk (published ⇒ written), so report the larger.
    {:ok, %{ckpt_seq: seq, salt1: salt1, size: max(size, extent), commit_extent: extent},
     %{handle | cursor: cursor}}
  end

  # The index extent for a WAL header just read, or nil to fall back to the walk. `index` was read
  # BEFORE that header (the held fast path tried it first); if it shows a different generation, the
  # header may simply be newer than it, so read the index once more — now AFTER the header, the
  # order in which a match is guaranteed for the current generation (see `read_header/3`).
  defp agreeing_extent(shm, index, page, salt1, salt2) do
    index_extent(index, page, salt1, salt2) ||
      (match?({:ok, _}, index) && index_extent(read_index(shm), page, salt1, salt2)) || nil
  end

  defp one_shot_index_extent(wal_path, page, salt1, salt2) do
    with true <- String.ends_with?(wal_path, "-wal"),
         {:ok, shm} <- :file.open(shm_path(wal_path), [:read, :raw, :binary]) do
      index = read_index(shm)
      :file.close(shm)
      index_extent(index, page, salt1, salt2)
    else
      _ -> nil
    end
  end

  # --- THE COMMIT EXTENT FROM THE WAL-INDEX (expert review 2026-10-20 #1 / #17) ----------------
  #
  # WHY: the frame walk decides "committed" from a frame's 24-byte HEADER (salts + nTruncate), but
  # SQLite's `walWriteOneFrame` writes the header and the page in two separate `write`s
  # (sqlite3.c, `walWriteOneFrame`). In append mode a header-only frame is invisible (the file is
  # not yet long enough, so `div(size - 32, frame_bytes)` floors it away), but over a STALE TAIL —
  # every cycle after an in-place restart, since prod's `journal_size_limit` is 64 MiB — or over a
  # rolled-back transaction's abandoned slots, the file is already long. A new commit header lands
  # on an old slot, the walk counts it, and the payload pread returns the OLD page under it: a torn
  # frame on the replica, whose checksum chain breaks there, so SQLite on the follower silently
  # drops it and everything after while the primary counts the follower as holding them.
  #
  # `mxFrame` in the wal-index header is the authority SQLite's own readers use, and `walFrames`
  # publishes it (`walIndexWriteHdr`) only AFTER every frame of the commit is written — and synced,
  # when synchronous calls for it. Frames ≤ mxFrame are never rewritten until the next restart,
  # which changes the salts. So `32 + mxFrame * (24 + page)` under matching salts is a committed,
  # fully written extent by construction, with no frame walk.
  #
  # LAYOUT (`struct WalIndexHdr`, 48 bytes, NATIVE byte order — the index is never shared across
  # machines), two copies at offsets 0 and 48:
  #   0 iVersion u32 (3007000) · 4 unused · 8 iChange · 12 isInit u8 · 13 bigEndCksum u8 ·
  #   14 szPage u16 (65536 encoded as 1) · 16 mxFrame u32 · 20 nPage · 24 aFrameCksum[2] ·
  #   32 aSalt[2] (the WAL header's salt bytes, copied verbatim, so read big-endian like the WAL) ·
  #   40 aCksum[2] = walChecksumBytes(native) over bytes 0..40.
  #
  # THE LOCK-FREE READ, and why a pread is as good as SQLite's mmap read here. SQLite's writer
  # (`walIndexWriteHdr`) writes copy [1], a memory barrier, then copy [0]; its reader
  # (`walIndexTryHdr`) copies [0], barrier, [1], and accepts only if the copies are byte-identical,
  # isInit is set and the checksum matches — otherwise it retries. We do the same over one 96-byte
  # pread:
  #   - COHERENCE. SQLite maps `-shm` MAP_SHARED; `pread` copies out of the same page-cache page
  #     (Linux page cache, macOS unified buffer cache), so it sees the writer's stores exactly as a
  #     second mapping would. Freshness is causal: the tenant's COMMIT returned (after
  #     `walIndexWriteHdr`) before the Session was asked to ship it, through BEAM message passing,
  #     so the read observes at least that commit's header.
  #   - ORDERING. A pread is one kernel memcpy of both copies with no barrier between them, so on a
  #     weakly ordered CPU (arm64) its loads may be reordered. A torn read nevertheless has to pass
  #     three independent checks: (1) both copies identical — each field of each copy is written
  #     once per publish, copy [1] wholly before copy [0], so an identical-but-mixed pair needs the
  #     loads of BOTH copies to straddle the writer at the same field boundary; (2) the checksum —
  #     a mix of two headers' fields (iChange, mxFrame, nPage, the 64-bit running frame checksum)
  #     must also reproduce a valid aCksum; (3) salts + page size equal to the WAL header's. The
  #     overwhelmingly common torn case (copies differ) is retried, as SQLite does.
  #   - STALENESS IS SAFE. An older consistent header is a smaller, still fully written extent; it
  #     only ships less this round. Within a generation `header_from/2` never reports less than the
  #     cursor already did.
  #   - GENERATIONS. A restart (`walRestartHdr`) publishes the new salts with mxFrame = 0 BEFORE the
  #     new WAL header is written. So a WAL header read FIRST and an index read SECOND agree for the
  #     current generation (or the index has moved on, and the walk with the header's salts sees only
  #     that finished generation's complete frames, the newer frames carrying other salts).
  #
  # FALLBACK to the walk (today's behaviour, unchanged) when the index is absent, short, never
  # initialised, still mid-write after retries, of another generation, or of another page size.
  #
  # POSIX-LOCK CAVEAT, recorded because it is not obvious: SQLite holds fcntl byte-range locks on
  # `-shm`, and closing ANY fd a process has on a file drops ALL of that process's fcntl locks on
  # it. In-process SQLite does not depend on them (connections in one OS process share one
  # `unixShmNode` and arbitrate in memory), so this is invisible to fathom — but a SECOND OS process
  # opening a live shard's database could then believe nothing has the index mapped. Running a
  # second process against a live shard was already unsupported; this makes it more so. The held
  # fd is closed only with the WAL fd (a recreate, after which the old `-shm` inode is dead) or at
  # Session exit; the one-shot `read(path, index: true)` opens and closes it per call.
  @index_hdr_bytes 48
  @index_version 3_007_000
  # A copy mismatch means a writer is mid-publish (two ~48-byte memcpys); an immediate re-read is
  # what SQLite does. Bounded, then the walk.
  @index_attempts 3

  defp read_index(nil), do: :none
  defp read_index(fd), do: read_index(fd, @index_attempts)

  defp read_index(_fd, 0), do: :none

  defp read_index(fd, n) do
    case :file.pread(fd, 0, 2 * @index_hdr_bytes) do
      {:ok, <<a::binary-size(@index_hdr_bytes), b::binary-size(@index_hdr_bytes)>>} when a == b ->
        parse_index(a)

      {:ok, <<_::binary-size(@index_hdr_bytes), _::binary-size(@index_hdr_bytes)>>} ->
        read_index(fd, n - 1)

      # Short (SQLite truncates `-shm` to 3 bytes while the first connection rebuilds it), absent,
      # or unreadable.
      _ ->
        :none
    end
  end

  defp parse_index(
         <<@index_version::32-native, _unused::32, _change::32, init::8, _bigend::8,
           szpage::16-native, mx::32-native, _npage::32, _frame_ck::binary-size(8), salt1::32,
           salt2::32, c1::32-native, c2::32-native>> = hdr
       )
       when init != 0 do
    if index_checksum(binary_part(hdr, 0, 40), 0, 0) == {c1, c2},
      do: {:ok, %{mx: mx, salt1: salt1, salt2: salt2, page: decode_page(szpage)}},
      else: :none
  end

  defp parse_index(_), do: :none

  # `WalCkptInfo` follows the two header copies: `nBackfill` is its first u32 (native order), at 96.
  @backfill_offset 2 * @index_hdr_bytes

  @doc """
  Whether the PRIMARY WAL at `wal_path` is FULLY BACKFILLED per its `-shm` wal-index:
  `nBackfill == mxFrame`, i.e. every committed frame is already in the main database file (expert
  review 2026-10-21 #11). `false` whenever that cannot be established (no `-shm`, a short or
  uninitialised index, a header still mid-publish after retries, or two reads that disagree).

  Read twice; both reads must be byte-identical. One pread is one memcpy with no ordering promise,
  so a `nBackfill` copied while a checkpointer stores it could in principle tear into a value it
  never held; requiring the same 100 bytes twice in a row rules that out.

  **Correct only for a SQLite-written primary `-wal`** (the same caveat as `read(path, index:
  true)`), and only meaningful to the caller holding a read transaction on the database: see
  `Fathom.Shard`'s pinned-live flush route for why "fully backfilled, read under a pin" means the
  main file is frozen at exactly the pin's state.
  """
  @spec fully_backfilled?(Path.t()) :: boolean()
  def fully_backfilled?(wal_path) do
    with true <- String.ends_with?(wal_path, "-wal"),
         {:ok, shm} <- :file.open(shm_path(wal_path), [:read, :raw, :binary]) do
      try do
        backfilled_read(shm, @index_attempts)
      after
        :file.close(shm)
      end
    else
      _ -> false
    end
  end

  defp backfilled_read(_fd, 0), do: false

  defp backfilled_read(fd, n) do
    with {:ok, first} <- :file.pread(fd, 0, @backfill_offset + 4),
         {:ok, ^first} <- :file.pread(fd, 0, @backfill_offset + 4),
         <<a::binary-size(@index_hdr_bytes), b::binary-size(@index_hdr_bytes),
           backfill::32-native>>
         when a == b <- first do
      match?({:ok, %{mx: ^backfill}}, parse_index(a))
    else
      # A shm shorter than the read (SQLite truncates it while the first connection rebuilds it).
      {:ok, short} when byte_size(short) < @backfill_offset + 4 -> false
      :eof -> false
      {:error, _} -> false
      # Copies differ, or the two reads differ: a writer or checkpointer is mid-store. Retry.
      _ -> backfilled_read(fd, n - 1)
    end
  end

  # `szPage` is a u16, so 65536 is stored as 1: `(sz & 0xfe00) + ((sz & 1) << 16)` in SQLite.
  defp decode_page(sz), do: Bitwise.band(sz, 0xFE00) + Bitwise.bsl(Bitwise.band(sz, 1), 16)

  # `walChecksumBytes(nativeCksum = 1, …)`: pairs of native u32s, 32-bit wrapping.
  defp index_checksum(<<a::32-native, b::32-native, rest::binary>>, s1, s2) do
    s1 = Bitwise.band(s1 + a + s2, 0xFFFFFFFF)
    index_checksum(rest, s1, Bitwise.band(s2 + b + s1, 0xFFFFFFFF))
  end

  defp index_checksum(<<>>, s1, s2), do: {s1, s2}

  defp index_extent({:ok, %{salt1: salt1, salt2: salt2, page: page, mx: mx}}, page, salt1, salt2),
    do: frame_extent(mx, page)

  defp index_extent(_index, _page, _salt1, _salt2), do: nil

  defp frame_extent(mx, page), do: @header_bytes + mx * (@frame_header_bytes + page)
end
