defmodule Fathom.Shard.Provenance do
  @moduledoc """
  The etag provenance sidecar (expert review #1) — pure `<path>.etag` file I/O.

  Extracted verbatim from `Fathom.Shard` (2026-09-13, Phase 1 of the coordinator decomposition).

  `<path>.etag` records which stored-object version the live local file derives from — written on
  every pull promotion and successful flush, removed with the local copy. A warm restart compares
  it to the store's current etag: equal ⇒ the local file continues the stored lineage (and may hold
  newer un-flushed writes); different ⇒ the lineages FORKED (another node wrote and released while
  we were down) and serving or flushing our copy would clobber acknowledged writes.

  Two write paths, deliberately different (the reason each exists is on the function):

    * `write/2` — a plain `File.write`, for the OPEN paths (single writer, read only at open). Made
      durable by `make_durable/1` on the shard's first write (expert review 2026-09-29 #14).
    * `write_durable/2` — fsync+rename, for the FLUSH path (the sidecar then names the object the
      tenant's fsynced commits descend from, and a torn value there is not recoverable).

  `read/1` returns `{:ok, etag} | :no_object | :missing | :corrupt` — a zero-length or unreadable
  sidecar is `:corrupt` (the safe direction: a spurious but recoverable quarantine), an absent one
  is `:missing` (unknown provenance), and the `"-"` sentinel is `:no_object` (born locally against
  no stored object — a POSITIVE claim, distinct from an absent sidecar).

  ## The fixed-width record and the PUT intent (expert review 2026-10-10 #6)

  The durable flush used to stamp the sidecar with temp+rename, and a rename is not durable until
  its directory is fsynced (the BEAM cannot portably do that). A lost rename reverts the sidecar to
  the PREVIOUS etag while the object (already PUT) and the local `.db` hold newer, acked writes —
  `Fork` then read `{:diverged, old, new}` and quarantined a good copy. So the flush stamp is now a
  FIXED-WIDTH record overwritten IN PLACE (`pwrite` at offset 0 + `fsync`): no rename, nothing for a
  directory fsync to be missing, and a record far smaller than a sector is written whole or not at
  all. The first write over a legacy (variable-width) file converts it with temp+rename once.

      "FPV1 " <> etag (96) <> " " <> intent md5 (32) <> " " <> previous intent md5 (32) <> " " <> crc32 (8 hex) <> "\n"

  (every field but the crc space-padded to its width; the CRC32 covers all bytes before it, so a
  write torn between old and new bytes reads as `:corrupt` instead of parsing as a chimera —
  expert review 2026-10-10 R2-5). TWO intent slots, because a retry flush records its own
  intent BEFORE its PUT: a first flush whose PUT landed but whose result was never stamped, then a
  retry that overwrites the single slot and 412s (the object is still the first one's), then a
  crash — the surviving intent matched nothing and a good copy was quarantined (expert review
  2026-10-10 #R2-1 of the fix-review). Recording a NEW intent demotes the current one to "previous"
  (an unresolved intent is never dropped by a retry); `Fork` adopts on a match with EITHER.

  The same record carries the PUT INTENT: the plaintext md5 of the bytes about to be PUT, recorded
  durably BEFORE the PUT. If the process dies after the object landed but before the sidecar learned
  the new etag (the coordinator handles the task result later, in its mailbox), the next open finds
  the stored object's `fathom-md5` equal to the recorded intent and ADOPTS it instead of
  quarantining (`Fathom.Shard.Fork`). Equal md5 means the stored bytes are exactly a snapshot of
  this node's own lineage, so adopting can never serve or flush over a peer's different data.

  `read/1` still reads the legacy format (a bare etag), so files written before this change keep
  working across an upgrade.
  """

  require Logger

  alias Fathom.Shard.Storage

  @record_magic "FPV1 "
  @etag_width 96
  @intent_width 32
  # magic + etag + " " + intent + " " + previous intent
  @crc_width 8
  @body_size 5 + 96 + 1 + 32 + 1 + 32
  # ... + " " + crc32 of everything before it (8 lowercase hex) + "\n"
  @record_size byte_size(@record_magic) + @etag_width + 1 + @intent_width + 1 + @intent_width + 1 +
                 @crc_width + 1

  @spec sidecar_path(String.t()) :: String.t()
  def sidecar_path(path), do: path <> ".etag"

  # "Derived from: no stored object." A real etag is a hex content hash, so this can never
  # collide with one. Written when a brand-new shard is born locally, so that "no object" is a
  # POSITIVE provenance claim rather than an absent sidecar (expert review 2026-08-01 #2).
  @no_object_sentinel "-"

  @spec write_no_object(String.t()) :: :ok
  def write_no_object(path), do: write(path, @no_object_sentinel)

  @spec write(String.t(), String.t() | nil) :: :ok
  def write(_path, nil), do: :ok

  def write(path, etag) do
    # A plain write, deliberately NOT atomic_write ON THE PULL PATH: the sidecar has a single writer
    # (this coordinator) and is read only at open, before any writer exists, so no torn CONCURRENT
    # read is possible — and a torn PULL-path value after a crash merely reads as a mismatch ⇒ a
    # spurious, recoverable quarantine of an object with NO un-flushed local writes (the safe
    # direction). The temp+rename pattern costs ~5× more (two APFS-journaled metadata ops) on the
    # timed cold-open path. The FLUSH path uses write_durable/2 instead — see there for
    # why the same torn value is NOT recoverable once the local .db holds acked writes (#15).
    case File.write(sidecar_path(path), etag) do
      :ok -> :ok
      {:error, reason} -> Logger.warning("etag sidecar write failed: #{inspect(reason)}")
    end
  end

  # The DURABLE sidecar write, for the FLUSH path (expert review 2026-09-05 #15). The plain write
  # above is correct for the pull path, but after a periodic/drop flush PUT the sidecar names the
  # object the tenant's fsynced commits descend from. If it is still in the page cache when an OS
  # crash (kernel panic, power loss, hard VM reset) hits, the on-disk sidecar reverts to the OLD etag
  # (or empty) while the local .db holds NEWER acked writes — fork_evidence then reads a divergence
  # and quarantines the acked tail, serving the pre-PUT object. That is the RPO of node loss on the
  # one path durability.md documents as loss-free (disk intact). storage.ex applies this same
  # fsync+rename reasoning to every OBJECT file, but not to the file that decides whether the object
  # is trusted. Cost: two metadata ops and one fsync on a ~40-byte file, beside a full-object PUT.
  @spec write_durable(String.t(), String.t() | nil) :: :ok
  def write_durable(_path, nil), do: :ok

  def write_durable(path, etag) do
    case put_record(path, etag, nil) do
      :ok -> :ok
      {:error, reason} -> Logger.warning("durable etag sidecar write failed: #{inspect(reason)}")
    end
  end

  @doc """
  Durably record the plaintext md5 (hex) of the bytes about to be PUT, keeping the current etag
  (expert review 2026-10-10 #6). Called by the storage backend right before the PUT. A sidecar that
  is absent or corrupt is left alone — that open already fails closed, and an intent cannot make an
  unknown provenance known. Cleared by the next `write_durable/2`. An existing, different intent
  is kept as the "previous" slot (see the moduledoc: a retry must not erase an unresolved intent).
  """
  @spec record_intent(String.t(), String.t()) :: :ok
  def record_intent(path, md5_hex) when byte_size(md5_hex) == @intent_width do
    sidecar = sidecar_path(path)

    # Steady state (expert review 2026-10-10 #P5): the sidecar already holds a valid record, so ONE
    # read yields the etag, both intent slots and proof that the in-place pwrite is safe. The
    # general path below costs `read` + `read_intents` + `put_record`'s stat and read before the
    # write, on every flush. The pre-PUT fsync (pwrite_sync) and the demotion rule are unchanged,
    # and `encode_record` recomputes the CRC.
    case File.read(sidecar) do
      {:ok, @record_magic <> _ = bin} when byte_size(bin) == @record_size ->
        record_intent_in_place(sidecar, bin, md5_hex)

      _ ->
        record_intent_general(path, md5_hex)
    end
  end

  defp record_intent_in_place(sidecar, bin, md5_hex) do
    with {:ok, etag, intent, prev} <- parse_record(bin),
         demoted = demote(Enum.reject([intent, prev], &is_nil/1), md5_hex),
         {:ok, record} <- encode_record(etag, md5_hex, demoted),
         :ok <- pwrite_sync(sidecar, record) do
      :ok
    else
      # A record that does not parse (torn, bad CRC) is left alone, like an absent sidecar.
      :error ->
        :ok

      {:error, reason} ->
        Logger.warning("etag sidecar intent write failed: #{inspect(reason)}")
    end
  end

  # Legacy bare-format, absent, or otherwise non-record sidecar: the original read-then-put path.
  defp record_intent_general(path, md5_hex) do
    etag =
      case read(path) do
        {:ok, etag} -> etag
        :no_object -> @no_object_sentinel
        _ -> nil
      end

    with etag when is_binary(etag) <- etag,
         :ok <- put_record(path, etag, md5_hex, demoted_intent(path, md5_hex)) do
      :ok
    else
      nil ->
        :ok

      {:error, reason} ->
        Logger.warning("etag sidecar intent write failed: #{inspect(reason)}")
    end
  end

  # The intent a NEW intent displaces: the current one if it differs (it becomes "previous"), else
  # the existing previous (re-recording the same md5 must not lose an older unresolved one).
  defp demoted_intent(path, new_md5), do: demote(read_intents(path), new_md5)

  defp demote(intents, new_md5) do
    case intents do
      [^new_md5, prev] -> prev
      [^new_md5] -> nil
      [current | _] -> current
      [] -> nil
    end
  end

  @doc "The CURRENT recorded PUT intent (plaintext md5, hex), or `nil` — none recorded or a legacy sidecar."
  @spec read_intent(String.t()) :: String.t() | nil
  def read_intent(path), do: path |> read_intents() |> List.first()

  @doc "Every unresolved PUT intent, newest first (at most two); `[]` for none or a legacy sidecar."
  @spec read_intents(String.t()) :: [String.t()]
  def read_intents(path) do
    case File.read(sidecar_path(path)) do
      {:ok, @record_magic <> _ = bin} ->
        case parse_record(bin) do
          {:ok, _etag, intent, prev} -> Enum.reject([intent, prev], &is_nil/1)
          :error -> []
        end

      _ ->
        []
    end
  end

  # Writes the fixed-width record. In place when the file already holds one (pwrite at 0 + fsync:
  # no rename to lose); otherwise temp+rename once, which converts a legacy sidecar. An etag that
  # does not fit the record (or has whitespace) falls back to the legacy bare format, which `read/1`
  # still understands — no intent can ride on it, so the gap stays open for such an etag only.
  defp put_record(path, etag, intent, prev_intent \\ nil) do
    case encode_record(etag, intent, prev_intent) do
      {:ok, record} ->
        sidecar = sidecar_path(path)

        if in_place_ok?(sidecar) do
          pwrite_sync(sidecar, record)
        else
          Storage.atomic_write(sidecar, record)
        end

      :error ->
        Storage.atomic_write(sidecar_path(path), etag)
    end
  end

  defp in_place_ok?(sidecar) do
    case File.stat(sidecar) do
      {:ok, %{size: @record_size}} -> match?({:ok, @record_magic <> _}, File.read(sidecar))
      _ -> false
    end
  end

  defp pwrite_sync(sidecar, record) do
    case :file.open(sidecar, [:read, :write, :raw, :binary]) do
      {:ok, fd} ->
        try do
          with :ok <- :file.pwrite(fd, 0, record), do: :file.sync(fd)
        after
          :file.close(fd)
        end

      {:error, _} = error ->
        error
    end
  end

  defp encode_record(etag, intent, prev_intent) do
    if byte_size(etag) in 1..@etag_width and etag =~ ~r/\A[^\s]+\z/ do
      {:ok,
       @record_magic <>
         String.pad_trailing(etag, @etag_width) <>
         " " <>
         String.pad_trailing(intent || "", @intent_width) <>
         " " <> String.pad_trailing(prev_intent || "", @intent_width)}
      |> with_crc()
    else
      :error
    end
  end

  defp with_crc({:ok, body}), do: {:ok, body <> " " <> crc_hex(body) <> "\n"}

  defp crc_hex(body) do
    body
    |> :erlang.crc32()
    |> Integer.to_string(16)
    |> String.downcase()
    |> String.pad_leading(@crc_width, "0")
  end

  defp parse_record(bin) when byte_size(bin) == @record_size do
    <<body::binary-size(@body_size), " ", crc::binary-size(@crc_width), "\n">> = bin

    if crc_hex(body) == crc, do: parse_body(body), else: :error
  end

  defp parse_record(_), do: :error

  defp parse_body(body) do
    @record_magic <> rest = body

    <<etag::binary-size(@etag_width), " ", intent::binary-size(@intent_width), " ",
      prev::binary-size(@intent_width)>> = rest

    case String.trim_trailing(etag) do
      "" -> :error
      etag -> {:ok, etag, blank_nil(intent), blank_nil(prev)}
    end
  end

  defp blank_nil(field) do
    case String.trim_trailing(field) do
      "" -> nil
      value -> value
    end
  end

  # A record that does not parse is a torn/garbled sidecar: same safe direction as an empty one.
  defp read_record(bin) do
    case parse_record(bin) do
      {:ok, @no_object_sentinel, _intent, _prev} -> :no_object
      {:ok, etag, _intent, _prev} -> {:ok, etag}
      :error -> :corrupt
    end
  end

  @doc """
  Re-write the sidecar DURABLY with whatever it currently says (expert review 2026-09-29 #14).

  The open paths write the sidecar with plain `write/2` — making every open pay the fsync was
  measured at +24–28% cold_open_p50. That plain write is safe only while the local `.db` holds
  nothing newer than the object it names; the coordinator calls this on the shard's FIRST write
  after open, so the sidecar is on disk before acked writes can depend on it. An absent/corrupt
  sidecar is left alone (the open path already decided what that means).
  """
  @spec make_durable(String.t()) :: :ok
  def make_durable(path) do
    case read(path) do
      {:ok, etag} -> write_durable(path, etag)
      :no_object -> write_durable(path, @no_object_sentinel)
      _ -> :ok
    end
  end

  @spec read(String.t()) :: {:ok, String.t()} | :no_object | :missing | :corrupt
  def read(path) do
    case File.read(sidecar_path(path)) do
      # A zero-length sidecar is a TORN WRITE, not "no provenance" (expert review
      # #12): File.write is O_TRUNC and power loss classically leaves the truncated
      # empty file. Mapping it to :missing fell through to the legacy adopt-current
      # branch — the exact clobber the sidecar exists to prevent. Corrupt routes to
      # quarantine instead (spurious but recoverable, the safe direction).
      {:ok, ""} -> :corrupt
      # An explicit "born locally against no stored object" claim (2026-08-01 #2) — distinct
      # from an absent sidecar, which means unknown provenance.
      {:ok, @no_object_sentinel} -> :no_object
      {:ok, @record_magic <> _ = bin} -> read_record(bin)
      {:ok, etag} -> {:ok, etag}
      # A truly ABSENT sidecar is now UNKNOWN provenance, not "legacy, trust it". Every file
      # this node creates carries a sidecar — a pulled object gets the object's etag, a
      # born-empty shard gets the sentinel — so an absent one is a file fathom did not write
      # here: a pre-provenance legacy copy, or a planted one.
      {:error, :enoent} -> :missing
      # Unreadable (eacces/eio/...) is unknown provenance, same safe direction.
      {:error, _} -> :corrupt
    end
  end
end
