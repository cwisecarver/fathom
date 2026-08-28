defmodule Fathom.Shard.WalOrdinalTest do
  @moduledoc """
  The monotone per-lineage WAL ordinal — step one of expert review 2026-08-26 #2 (Critical, data
  loss).

  ## The defect it exists to fix

  `Promote.fresher?/2` ranks `{lineage, wal_gen, offset}`, and `wal_gen` is SQLite's `ckpt_seq`:
  it counts checkpoints WITHIN one WAL file and **restarts at 0 when SQLite deletes and recreates
  the `-wal`**, which `docs/a2-quorum-replication.md` says happens "after every Hrana stream on a
  quiet shard". Measured on this codebase: two consecutive streams both read `ckpt_seq = 0`, with
  salts 977542977 then 978380554.

  So a flush that checkpoints stamps `{L, G+1, 0}`, the replica then sits at `{L, 0, Y}` in the
  recreated WAL, and `0 < G+1` means `fresher?` says no. `Recovery.choose/3` returns `:none`, the
  survivor cold-opens the stale object, and the acked tail is gone — silently, and
  indistinguishably from A2 being switched off.

  ## Why a coordinator-local counter is sound

  This is the property the whole design rests on: `open_lineage/1` takes its lineage from
  `Storage.next_lineage/1`, which is **strictly greater than anything previously stamped** for the
  shard. A lineage therefore never repeats across coordinator lifetimes, one lineage has exactly
  one coordinator, and so exactly one counter. Restarting at 0 for a new coordinator is correct
  because `lineage` is the higher-order component.

  ## Why the coordinator and not ETS

  Two readers must AGREE: the coordinator stamps the object's position, and
  `Replication.Session` stamps the replicas'. If they disagreed about a WAL's ordinal, the object
  and its replicas would be on different scales — which is the bug, not the fix. Serializing
  through the shard's single writer is what makes them agree.

  **This step is inert.** Nothing stamps the ordinal yet; the wire and the ordering come next, and
  they are gated. That is the same shape the `salt1` groundwork landed in.
  """
  use ExUnit.Case, async: false

  alias Fathom.{Shard, Shards}

  setup do
    id = "walord_#{System.unique_integer([:positive])}"
    {:ok, pid, ref, _path} = Shards.checkout(id)
    on_exit(fn -> Shards.drain(id, 5_000) end)
    %{id: id, pid: pid, ref: ref}
  end

  test "the same WAL identity always gets the same ordinal", %{pid: pid} do
    # STABILITY, which is half the contract. The object's stamp and the replicas' positions are
    # produced by two different processes; if asking twice about one WAL could give two answers,
    # they would be on different scales and the ordering would be meaningless.
    first = Shard.wal_ordinal(pid, 111)

    assert Shard.wal_ordinal(pid, 111) == first
    assert Shard.wal_ordinal(pid, 111) == first
  end

  test "a new WAL identity always sorts ABOVE the one it replaced", %{pid: pid} do
    # MONOTONICITY, the other half — and the actual fix. `ckpt_seq` would go 5 -> 0 here.
    a = Shard.wal_ordinal(pid, 111)
    b = Shard.wal_ordinal(pid, 222)
    c = Shard.wal_ordinal(pid, 333)

    assert b > a
    assert c > b
  end

  test "returning to a PREVIOUS salt still moves forward, never back", %{pid: pid} do
    # SQLite picks salts randomly, so a repeat is possible. Treating a repeat as "we are back at
    # the old ordinal" would let a newer WAL rank below an older one — the exact inversion this
    # counter exists to prevent. The counter tracks succession, not identity.
    a = Shard.wal_ordinal(pid, 111)
    _b = Shard.wal_ordinal(pid, 222)
    again = Shard.wal_ordinal(pid, 111)

    assert again > a,
           "a salt seen before was given its OLD ordinal (#{again} vs #{a}); the counter must " <>
             "order succession, not identity, or a recreated WAL can rank below the one it replaced"
  end

  test "an unreadable WAL does not advance the counter", %{pid: pid} do
    # `nil` means "could not read the WAL". That is not evidence of a NEW one, and inventing a
    # higher ordinal for it would let an unknown state outrank a known one — the opposite of the
    # "unknown ⇒ object wins" posture this module's siblings take.
    a = Shard.wal_ordinal(pid, 111)

    assert Shard.wal_ordinal(pid, nil) == a
    assert Shard.wal_ordinal(pid, nil) == a
    assert Shard.wal_ordinal(pid, 111) == a, "the nil read consumed the ordinal for salt 111"
  end

  test "a fresh coordinator starts over, which is safe because the LINEAGE moved", %{
    id: id,
    pid: pid0,
    ref: ref0
  } do
    # The design's load-bearing assumption, asserted rather than assumed: restarting the ordinal is
    # only sound because `Storage.next_lineage/1` guarantees a coordinator never reuses a lineage.
    # If that ever changes, this counter silently stops being an order.
    _ = Shard.wal_ordinal(pid0, 111)
    second = Shard.wal_ordinal(pid0, 222)
    assert second > 0

    # Release the setup's checkout so the coordinator can actually stop.
    Shard.checkin(pid0, ref0)
    :ok = Shards.drain(id, 5_000)
    {:ok, pid2, ref2, _path} = Shards.checkout(id)
    on_exit(fn -> Shard.checkin(pid2, ref2) end)

    assert Shard.wal_ordinal(pid2, 999) == 1,
           "a new coordinator did not restart its ordinal; that is only correct if lineages are " <>
             "never reused, and it is the restart that keeps the counter cheap"
  end
end
