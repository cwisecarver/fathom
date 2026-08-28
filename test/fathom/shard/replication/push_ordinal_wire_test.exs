defmodule Fathom.Shard.Replication.PushOrdinalWireTest do
  @moduledoc """
  The ordinal-carrying push frame — step two of expert review 2026-08-26 #2 (Critical, data loss).

  ## Why the ordinal has to be on the wire at all

  `salt1` already crosses the wire (added at `@version 2`, 2026-08-11), so a follower can SEE that
  its WAL changed. What it cannot see is which WAL is **newer** — and that is the whole gap.
  `Promote.fresher?/2` ranks `{lineage, wal_gen, offset}` lexicographically, `wal_gen` is SQLite's
  `ckpt_seq`, and `ckpt_seq` restarts at 0 whenever SQLite recreates the `-wal` (after every Hrana
  stream on a quiet shard). So a replica holding a recreated WAL at gen 0 loses to an object
  stamped gen G+1, `Recovery.choose/3` returns `:none`, and the survivor cold-opens the stale
  object with the acked tail gone.

  ## Why it cannot ride the seed, unlike the lineage before it

  `@seed_begin_lin`'s own comment records that a push carrying a stale LINEAGE always routes
  through `FollowerLog.decide_fresh/2`, which sets `torn: true`, and a torn replica is refused
  everywhere — so the seed was enough for that field. The ordinal changes **within** a lineage, on
  every WAL recreate, and that recreate ships as a `{:reset, 0, Y}` on a live session with no
  re-seed. A push is exactly where it changes.

  ## Why it gets its OWN gate

  `lineage_wire?/0` defaults to TRUE because it guards a window that closes once. This adds a type
  code that every currently-deployed peer answers `{:error, :malformed}` to, so emitting it under
  an already-on gate would close sockets fleet-wide — the exact break the gate pattern exists to
  prevent. `ordinal_wire?/0` defaults to FALSE and follows `FrameAuth.signing?/0`: opt-in, turned
  on once the code is everywhere.
  """
  use ExUnit.Case, async: false

  alias Fathom.Shard.Replication.FrameAuth
  alias Fathom.Shard.Replication.Protocol
  alias Fathom.Shard.Replication.Protocol.Push

  setup do
    prev_ord = Application.get_env(:fathom, :replication_ordinal_wire)
    prev_sign = Application.get_env(:fathom, :replication_sign_frames)
    prev_secret = Application.get_env(:fathom, :replication_hmac_secret)

    on_exit(fn ->
      restore(:replication_ordinal_wire, prev_ord)
      restore(:replication_sign_frames, prev_sign)
      restore(:replication_hmac_secret, prev_secret)
      FrameAuth.forget_key()
    end)

    :ok
  end

  defp restore(k, nil), do: Application.delete_env(:fathom, k)
  defp restore(k, v), do: Application.put_env(:fathom, k, v)

  # `FrameAuth.key/0` memoizes the DERIVED key in `:persistent_term` under a constant cache key,
  # so setting `:replication_hmac_secret` alone does nothing once anything in the run has already
  # asked for the key. In a full-suite run something always has, and it cached `nil` — no secret
  # was configured at the time. `sign/1` then returns nil, `encode_push/1` emits an UNSIGNED
  # frame, and `decode/1` accepts a tampered one: measured, the tamper rewrote `epoch` from 7 to
  # 280375465082887 and returned `{:ok, …}`.
  #
  # That is why "a signed ordinal frame is REJECTED if tampered with" passed alone and failed in
  # the suite, and it silently made "it composes with frame signing" vacuous too — both were
  # exercising the unsigned path. `forget_key/0` exists for exactly this; `protocol_signing_test`
  # has always called it.
  defp enable_signing do
    Application.put_env(:fathom, :replication_hmac_secret, :crypto.strong_rand_bytes(32))
    Application.put_env(:fathom, :replication_sign_frames, true)
    FrameAuth.forget_key()

    assert FrameAuth.key_configured?(),
           "fixture: no signing key is in force, so these frames would ship UNSIGNED and the " <>
             "tamper assertion below would be measuring nothing"
  end

  defp push(opts \\ []) do
    %Push{
      shard_id: "acme",
      epoch: 7,
      wal_gen: 3,
      salt1: 977_542_977,
      offset: 4096,
      payload: "frames",
      prev_extent: 2048,
      wal_ordinal: Keyword.get(opts, :wal_ordinal, 11)
    }
  end

  defp roundtrip(p), do: p |> Protocol.encode_push() |> IO.iodata_to_binary() |> Protocol.decode()

  describe "with the gate OFF (the default)" do
    test "the frame is byte-identical to what shipped before" do
      Application.put_env(:fathom, :replication_ordinal_wire, false)

      # The whole safety argument for adding a type code rather than bumping the version: a node
      # with the gate off emits exactly what it emitted before, so a peer one deploy behind is
      # unaffected.
      with_ordinal = Protocol.encode_push(push()) |> IO.iodata_to_binary()
      without = Protocol.encode_push(%{push() | wal_ordinal: nil}) |> IO.iodata_to_binary()

      assert with_ordinal == without,
             "the ordinal leaked into the frame while its gate was off — a peer one deploy " <>
               "behind would answer :malformed and close the socket"
    end

    test "a decoded push states no ordinal" do
      Application.put_env(:fathom, :replication_ordinal_wire, false)

      assert {:ok, %Push{wal_ordinal: nil}} = roundtrip(push()),
             "an ordinal appeared with the gate off"
    end
  end

  describe "with the gate ON" do
    setup do
      Application.put_env(:fathom, :replication_ordinal_wire, true)
      :ok
    end

    test "the ordinal survives the round trip with every other field intact" do
      assert {:ok, decoded} = roundtrip(push())

      assert decoded.wal_ordinal == 11
      assert decoded.shard_id == "acme"
      assert decoded.epoch == 7
      assert decoded.wal_gen == 3
      assert decoded.salt1 == 977_542_977
      assert decoded.offset == 4096
      assert decoded.payload == "frames"

      assert decoded.prev_extent == 2048,
             "prev_extent was dropped; the ordinal frame extends @push_ext and must carry it"
    end

    test "an unstated ordinal encodes as 0, not as a missing field" do
      # 0 means "not stated", exactly as `lineage: 0` does on the seed. It reaches here when the
      # gate is on but the primary could not read its WAL — the unknown case, NOT a zeroth
      # generation. `Promote.fresher?/2` must refuse to rank against it.
      assert {:ok, %Push{wal_ordinal: 0}} = roundtrip(push(wal_ordinal: nil))
    end

    test "large ordinals survive — the field is 64-bit" do
      big = 18_446_744_073_709_551_615
      assert {:ok, %Push{wal_ordinal: ^big}} = roundtrip(push(wal_ordinal: big))
    end

    test "it composes with frame signing rather than competing with it" do
      # The two gates are independent because they guard different things: a rolling-upgrade
      # window versus a distributed key. A signed ordinal frame must both verify and decode.
      enable_signing()

      assert {:ok, decoded} = roundtrip(push())
      assert decoded.wal_ordinal == 11
      assert decoded.prev_extent == 2048
    end

    test "a signed ordinal frame is REJECTED if tampered with" do
      # Proves the new code is actually inside the signed envelope rather than merely surviving
      # alongside it — a frame the signature does not cover would decode happily after tampering.
      enable_signing()

      <<head::binary-size(6), rest::binary>> =
        Protocol.encode_push(push()) |> IO.iodata_to_binary()

      tampered = head <> <<0xFF>> <> binary_part(rest, 1, byte_size(rest) - 1)

      assert {:error, _} = Protocol.decode(tampered),
             "a tampered signed ordinal frame decoded; the new type code is outside the signature"
    end
  end

  # THE PEER OFFER CARRIES IT TOO, and without that the fix is only half applied (step 3b).
  # `Recovery.choose/3` filters peer offers through `Promote.fresher?/2`, which refuses a replica
  # whose ordinal is unstated — so an offer that cannot state one is never chosen and CROSS-FLEET
  # recovery stays inert even once the local path works. That is exactly the case a real failover
  # hits: the survivor holds nothing, a peer holds the acked tail, the object is behind both.
  describe "the peer position offer" do
    defp offer do
      %{lineage: 9, epoch: 1, wal_gen: 5, salt1: 7, next_offset: 8_240, wal_ordinal: 12}
    end

    defp offer_roundtrip(pos),
      do: "acme" |> Protocol.encode_position(pos) |> IO.iodata_to_binary() |> Protocol.decode()

    test "with the gate OFF it is byte-identical to what shipped before" do
      Application.put_env(:fathom, :replication_ordinal_wire, false)

      with_ord = Protocol.encode_position("acme", offer()) |> IO.iodata_to_binary()
      without = Protocol.encode_position("acme", Map.delete(offer(), :wal_ordinal))
      without = IO.iodata_to_binary(without)

      assert with_ord == without,
             "the ordinal leaked into the offer frame while its gate was off — a peer one deploy " <>
               "behind would answer :malformed and close the socket"

      assert {:ok, {:position, "acme", decoded}} = offer_roundtrip(offer())
      refute Map.has_key?(decoded, :wal_ordinal), "an ordinal appeared with the gate off"
    end

    test "with the gate ON the ordinal survives with every other field intact" do
      Application.put_env(:fathom, :replication_ordinal_wire, true)

      assert {:ok, {:position, "acme", decoded}} = offer_roundtrip(offer())
      assert decoded.wal_ordinal == 12
      assert decoded.lineage == 9
      assert decoded.epoch == 9, "both keys still come from the one ordering-key field"
      assert decoded.wal_gen == 5
      assert decoded.salt1 == 7
      assert decoded.next_offset == 8_240
    end

    test "an offer with no ordinal states 0, which fresher?/2 refuses to rank" do
      Application.put_env(:fathom, :replication_ordinal_wire, true)

      assert {:ok, {:position, "acme", decoded}} =
               offer_roundtrip(Map.delete(offer(), :wal_ordinal))

      assert decoded.wal_ordinal == 0
    end

    # "I hold nothing" keeps the OLD type code: there is no ordinal to state, so a new code would
    # buy nothing and cost a rolling upgrade.
    test "the empty offer is unchanged under either gate" do
      Application.put_env(:fathom, :replication_ordinal_wire, true)
      on = Protocol.encode_position("acme", nil) |> IO.iodata_to_binary()
      Application.put_env(:fathom, :replication_ordinal_wire, false)
      off = Protocol.encode_position("acme", nil) |> IO.iodata_to_binary()

      assert on == off
      assert {:ok, {:position, "acme", nil}} = Protocol.decode(on)
    end

    test "it composes with frame signing, and a tampered offer is REJECTED" do
      Application.put_env(:fathom, :replication_ordinal_wire, true)
      enable_signing()

      frame = Protocol.encode_position("acme", offer()) |> IO.iodata_to_binary()
      assert {:ok, {:position, "acme", %{wal_ordinal: 12}}} = Protocol.decode(frame)

      <<head::binary-size(6), rest::binary>> = frame
      tampered = head <> <<0xFF>> <> binary_part(rest, 1, byte_size(rest) - 1)

      assert {:error, _} = Protocol.decode(tampered),
             "a tampered signed offer decoded; the new type code is outside the signature"
    end
  end

  test "the version is NOT bumped — that is what keeps a rolling upgrade alive" do
    Application.put_env(:fathom, :replication_ordinal_wire, true)
    <<version::8, type::8, _::binary>> = Protocol.encode_push(push()) |> IO.iodata_to_binary()

    assert version == 2,
           "the wire version moved. `decode/1` rejects a version mismatch outright, so this takes " <>
             "the commit path down fleet-wide for the length of a rolling upgrade — the cost the " <>
             "protocol's own comments record for exactly this decision. Add a TYPE CODE instead."

    assert type == 15, "the ordinal push should be its own type code"
  end
end
