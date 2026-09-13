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

    * `write/2` — a plain `File.write`, for the PULL path (single writer, read only at open).
    * `write_durable/2` — fsync+rename, for the FLUSH path (the sidecar then names the object the
      tenant's fsynced commits descend from, and a torn value there is not recoverable).

  `read/1` returns `{:ok, etag} | :no_object | :missing | :corrupt` — a zero-length or unreadable
  sidecar is `:corrupt` (the safe direction: a spurious but recoverable quarantine), an absent one
  is `:missing` (unknown provenance), and the `"-"` sentinel is `:no_object` (born locally against
  no stored object — a POSITIVE claim, distinct from an absent sidecar).
  """

  require Logger

  alias Fathom.Shard.Storage

  @spec sidecar_path(String.t()) :: String.t()
  def sidecar_path(path), do: path <> ".etag"

  # "Derived from: no stored object." A real etag is a hex content hash, so this can never
  # collide with one. Written when a brand-new shard is born locally, so that "no object" is a
  # POSITIVE provenance claim rather than an absent sidecar (expert review 2026-08-01 #2).
  @no_object_sentinel "-"

  @spec no_object_sentinel() :: String.t()
  def no_object_sentinel, do: @no_object_sentinel

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
    case Storage.atomic_write(sidecar_path(path), etag) do
      :ok -> :ok
      {:error, reason} -> Logger.warning("durable etag sidecar write failed: #{inspect(reason)}")
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
