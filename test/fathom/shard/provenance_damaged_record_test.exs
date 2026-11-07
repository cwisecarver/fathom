defmodule Fathom.Shard.ProvenanceDamagedRecordTest do
  @moduledoc """
  Expert review 2026-11-07 #70: a damaged sidecar record is `:corrupt`, never a crash.

  The record is 177 bytes: a 167-byte body, a space at byte 168, an 8-hex CRC, a newline at byte
  177. The CRC covers only the body, so the separator and the newline are the two bytes OUTSIDE its
  protection, and `parse_record/1` matched them with an unguarded `<<body, " ", crc, "\\n">> = bin`.
  Damage to exactly one of them raised `MatchError` out of `Provenance.read/1`, which
  crash-looped the open (`{:open_failed, %MatchError{}}` on every retry until an operator deleted
  `<id>.db.etag`) and killed the flush task in `record_intent/2` every interval — the RPO growing
  silently, the failure permanent rather than self-healing.

  ## What `:corrupt` means to each consumer (all checked, all pinned below)

    * `Fork.evidence/2` → `:corrupt` → `Fork.resolve/4` QUARANTINES the copy (`.forked.<ms>`) and the
      open re-pulls: provenance is unknown, so the copy is not trusted to continue the stored lineage.
      Spurious but recoverable — the acked writes live in the quarantine file — the established safe
      direction (the zero-length / torn / bad-CRC cases already go this way).
    * `Materializer.await_pull(nil, ..)` (a warm open that reached the fence with a corrupt sidecar)
      → `{:error, :sidecar_corrupt}`: fails the open, never adopts the store's current etag.
    * `Provenance.record_intent/2` leaves the record alone (it cannot make unknown provenance known)
      and returns `:ok`: the flush proceeds, and its post-PUT stamp rewrites the whole record, so a
      damaged sidecar HEALS on the next successful flush.
    * `Provenance.read_intents/1` → `[]`, `make_durable/1` → no-op.
  """
  use ExUnit.Case, async: true

  import ExUnit.CaptureLog

  alias Fathom.Shard.{Fork, Materializer, Provenance}

  @record_size 177
  # 0-indexed offsets of the two bytes outside the CRC's coverage (bytes 168 and 177, 1-indexed).
  @separator_offset 167
  @newline_offset 176

  @moduletag :tmp_dir
  setup %{tmp_dir: dir} do
    path = Path.join(dir, "damaged_#{System.unique_integer([:positive])}.db")
    :ok = Provenance.write(path, "\"abc123\"")
    sidecar = Provenance.sidecar_path(path)

    assert byte_size(File.read!(sidecar)) == @record_size,
           "fixture: the sidecar must be a fixed-width record"

    assert Provenance.read(path) == {:ok, "\"abc123\""}
    %{path: path, sidecar: sidecar}
  end

  defp damage!(sidecar, offset) do
    bin = File.read!(sidecar)
    head = binary_part(bin, 0, offset)
    <<byte>> = binary_part(bin, offset, 1)
    tail = binary_part(bin, offset + 1, byte_size(bin) - offset - 1)
    File.write!(sidecar, <<head::binary, Bitwise.bxor(byte, 0x55), tail::binary>>)
  end

  for {name, offset} <- [
        {"separator (byte 168)", @separator_offset},
        {"newline (byte 177)", @newline_offset}
      ] do
    describe "damage to the #{name}" do
      setup %{path: path, sidecar: sidecar} do
        damage!(sidecar, unquote(offset))
        assert byte_size(File.read!(sidecar)) == @record_size
        %{damaged: File.read!(sidecar), path: path}
      end

      test "read/1 answers :corrupt instead of raising", %{path: path} do
        assert Provenance.read(path) == :corrupt
      end

      test "record_intent/2 returns :ok without raising and leaves the record alone",
           %{path: path, sidecar: sidecar, damaged: damaged} do
        assert Provenance.record_intent(path, String.duplicate("a", 32)) == :ok
        assert File.read!(sidecar) == damaged
        assert Provenance.read_intents(path) == []
        assert Provenance.make_durable(path) == :ok
        assert File.read!(sidecar) == damaged
      end

      test "the next successful flush stamp rewrites the whole record (it heals)", %{path: path} do
        assert Provenance.stamp_after_put(path, "\"e2\"") == :ok
        assert Provenance.read(path) == {:ok, "\"e2\""}
      end

      test "a warm open that reaches the fence fails closed, never adopting the store's etag",
           %{path: path} do
        assert Materializer.await_pull(nil, path, "damaged") == {:error, :sidecar_corrupt}
      end

      test "Fork treats it as unknown provenance: quarantine and re-pull", %{path: path} do
        File.write!(path, "acked-unflushed local bytes")
        assert Fork.evidence("damaged", path) == :corrupt

        capture_log(fn -> assert Fork.resolve(:corrupt, "damaged", path, %{}) end)

        refute File.exists?(path)
        refute File.exists?(Provenance.sidecar_path(path))
        assert [aside] = Path.wildcard(path <> ".forked.*")
        assert File.read!(aside) == "acked-unflushed local bytes"
      end
    end
  end
end
