defmodule Fathom.Shard.ProtocolSigningTest do
  @moduledoc """
  A2 frame authentication and `prev_extent` (expert review 2026-08-20 #3 tier 3 and #11a) — the
  wire layer, which is where both of them actually live.

  ## What #3 closes

  A reachable replication port was equivalent to write access to every shard on the node: the
  listener accepts any TCP connection and applies whatever WAL frames arrive, with
  `REPLICATION_BIND_IP` as the only control. These tests pin that an unauthenticated peer cannot
  construct an acceptable frame once enforcement is on, and — just as important — that a fleet
  mid-rollout keeps working.

  ## The rollout property is the fragile one

  Both flags default OFF and the legacy frame shape is unchanged, so step 1 of the rollout (deploy
  everywhere) is a no-op on the wire. That is asserted directly, because getting it wrong does not
  look like a test failure, it looks like a fleet-wide loss of quorum during a deploy.
  """
  use ExUnit.Case, async: false

  alias Fathom.Shard.Replication.FrameAuth
  alias Fathom.Shard.Replication.Protocol

  @secret "a-fleet-shared-secret-long-enough-to-be-real"

  setup do
    prev = %{
      sign: Application.get_env(:fathom, :replication_sign_frames),
      required: Application.get_env(:fathom, :replication_hmac_required),
      secret: Application.get_env(:fathom, :replication_hmac_secret)
    }

    on_exit(fn ->
      for {k, v} <- [
            replication_sign_frames: prev.sign,
            replication_hmac_required: prev.required,
            replication_hmac_secret: prev.secret
          ] do
        if is_nil(v),
          do: Application.delete_env(:fathom, k),
          else: Application.put_env(:fathom, k, v)
      end

      FrameAuth.forget_key()
    end)

    Application.put_env(:fathom, :replication_hmac_secret, @secret)
    FrameAuth.forget_key()
    :ok
  end

  defp sign!(on?), do: Application.put_env(:fathom, :replication_sign_frames, on?)
  defp require!(on?), do: Application.put_env(:fathom, :replication_hmac_required, on?)

  defp push(overrides \\ []) do
    struct!(
      %Protocol.Push{
        shard_id: "acme",
        epoch: 3,
        wal_gen: 7,
        salt1: 123_456,
        offset: 4096,
        payload: :binary.copy("w", 512)
      },
      overrides
    )
  end

  # Every frame the protocol can emit, so the signing and round-trip properties below are asserted
  # across all of them rather than on `push` alone — an envelope that silently failed to wrap a
  # seed_begin would leave the seed path unauthenticated while the tests looked green.
  defp all_frames do
    [
      {:push, Protocol.encode_push(push())},
      {:seed_begin,
       Protocol.encode_seed_begin(%Protocol.SeedBegin{
         shard_id: "acme",
         epoch: 1,
         wal_gen: 2,
         salt1: 3,
         wal_offset: 4,
         db_size: 5,
         wal_size: 6
       })},
      {:seed_chunk, Protocol.encode_seed_chunk("acme", :db, 0, :binary.copy("d", 64))},
      {:seed_end, Protocol.encode_seed_end("acme")},
      {:seed_abort, Protocol.encode_seed_abort("acme")},
      {:ack, Protocol.encode_ack("acme", 8192)},
      {:reject, Protocol.encode_reject("acme", :offset_mismatch, 4096)},
      {:position_query, Protocol.encode_position_query("acme")},
      {:position,
       Protocol.encode_position("acme", %{epoch: 1, wal_gen: 2, salt1: 3, next_offset: 4})},
      {:position_nil, Protocol.encode_position("acme", nil)},
      {:replica_request, Protocol.encode_replica_request("acme")}
    ]
  end

  describe "the rollout is safe at every step" do
    test "step 1 — both flags off leaves the wire BYTE-IDENTICAL to before" do
      # THE PROPERTY THAT MAKES A ROLLING DEPLOY SURVIVABLE. If merely landing the code changed
      # what goes on the wire, every not-yet-restarted peer would refuse it and every shard
      # replicated across the deploy boundary would lose quorum. That failure would not look like
      # a test failure, so it is asserted here explicitly.
      sign!(false)
      require!(false)

      for {name, frame} <- all_frames() do
        bin = IO.iodata_to_binary(frame)
        <<version::8, type::8, _::binary>> = bin

        assert version == Protocol.version(),
               "#{name} left @version #{Protocol.version()} with signing off"

        refute type == 12, "#{name} emitted a signed envelope with signing off"
        assert {:ok, _} = Protocol.decode(bin), "#{name} did not round-trip with both flags off"
      end
    end

    test "step 2 — signing on, requiring off: a node accepts BOTH shapes" do
      # The mixed state that exists for as long as it takes to roll the flag out. A node that has
      # flipped signing must still accept an un-flipped peer's frames, or step 2 is itself an
      # outage and there is no safe path from step 1 to step 3.
      sign!(false)
      unsigned = IO.iodata_to_binary(Protocol.encode_push(push()))

      sign!(true)
      signed = IO.iodata_to_binary(Protocol.encode_push(push()))

      require!(false)
      assert {:ok, %Protocol.Push{}} = Protocol.decode(unsigned)
      assert {:ok, %Protocol.Push{}} = Protocol.decode(signed)
    end

    test "step 3 — requiring on: unsigned is refused, signed is accepted" do
      sign!(false)
      unsigned = IO.iodata_to_binary(Protocol.encode_push(push()))
      sign!(true)
      signed = IO.iodata_to_binary(Protocol.encode_push(push()))

      require!(true)
      assert {:error, :unauthenticated} = Protocol.decode(unsigned)
      assert {:ok, %Protocol.Push{}} = Protocol.decode(signed)
    end

    test "requiring refuses EVERY frame type unsigned, not just pushes" do
      # A seed_begin accepted unsigned would let an unauthenticated peer install a whole forged
      # database, which is a worse outcome than a forged delta.
      sign!(false)
      unsigned = for {name, f} <- all_frames(), do: {name, IO.iodata_to_binary(f)}

      require!(true)

      for {name, bin} <- unsigned do
        assert {:error, :unauthenticated} = Protocol.decode(bin),
               "#{name} was accepted unsigned while :replication_hmac_required was on"
      end
    end
  end

  describe "the signature actually authenticates" do
    setup do
      sign!(true)
      require!(true)
      :ok
    end

    test "every frame type round-trips through the envelope" do
      for {name, frame} <- all_frames() do
        assert {:ok, _} = Protocol.decode(IO.iodata_to_binary(frame)),
               "#{name} did not survive its own signed envelope"
      end
    end

    test "a peer with a DIFFERENT key is refused" do
      signed = IO.iodata_to_binary(Protocol.encode_push(push()))

      Application.put_env(:fathom, :replication_hmac_secret, "some-other-fleets-secret")
      FrameAuth.forget_key()

      assert {:error, :unauthenticated} = Protocol.decode(signed)
    end

    test "a peer with NO key is refused" do
      signed = IO.iodata_to_binary(Protocol.encode_push(push()))

      Application.delete_env(:fathom, :replication_hmac_secret)
      Application.delete_env(:fathom, :hrana_token_secret)
      FrameAuth.forget_key()

      assert {:error, :unauthenticated} = Protocol.decode(signed)
    end

    test "flipping any byte of the signed header is refused" do
      signed = IO.iodata_to_binary(Protocol.encode_push(push()))
      header_end = 3 + FrameAuth.tag_bytes() + 36 + byte_size("acme")

      for i <- (3 + FrameAuth.tag_bytes())..(header_end - 1) do
        <<pre::binary-size(^i), byte::8, post::binary>> = signed
        tampered = <<pre::binary, Bitwise.bxor(byte, 1)::8, post::binary>>

        assert match?({:error, _}, Protocol.decode(tampered)),
               "flipping byte #{i} of the signed header was accepted"
      end
    end

    test "flipping a byte of the TAG is refused" do
      signed = IO.iodata_to_binary(Protocol.encode_push(push()))

      for i <- 3..(3 + FrameAuth.tag_bytes() - 1) do
        <<pre::binary-size(^i), byte::8, post::binary>> = signed
        tampered = <<pre::binary, Bitwise.bxor(byte, 1)::8, post::binary>>
        assert {:error, :unauthenticated} = Protocol.decode(tampered)
      end
    end

    test "the PAYLOAD is deliberately NOT covered — asserted so the limit is not forgotten" do
      # This is the documented trade (see FrameAuth's moduledoc): hashing up to
      # REPLICATION_MAX_PUSH_BYTES of WAL on the commit path was judged too expensive, and the
      # threat this is sized for is an unauthenticated peer opening a socket — which it stops
      # completely, because such a peer cannot produce a valid header at all.
      #
      # It is asserted rather than left implicit so that nobody reads "frames are authenticated"
      # and concludes the bytes are attested. If payload coverage is ever added, this test SHOULD
      # fail, and its failure is the signal to update the moduledoc and the runbook with it.
      signed = IO.iodata_to_binary(Protocol.encode_push(push()))
      last = byte_size(signed) - 1
      <<pre::binary-size(^last), byte::8>> = signed
      tampered = <<pre::binary, Bitwise.bxor(byte, 1)::8>>

      assert {:ok, %Protocol.Push{}} = Protocol.decode(tampered),
             "the payload is now covered by the signature — good, but update FrameAuth's " <>
               "moduledoc and docs/runbooks/replication-frame-auth.md, which both say it is not"
    end

    test "an envelope cannot nest — a signed envelope inside one is malformed, not recursed" do
      inner = IO.iodata_to_binary(Protocol.encode_push(push()))

      assert {:error, :malformed} =
               Protocol.decode(IO.iodata_to_binary(Protocol.seal([inner, ""])))
    end
  end

  describe "signable/1 — the send and receive sides must agree exactly" do
    test "the signed prefix matches what seal/1 signs, for every frame type" do
      # THE DRIFT GUARD. `seal/1` signs the first two elements of an iolist it just built;
      # `signable/1` takes a byte prefix of a flat binary off a socket. They are different code
      # computing the same answer, and if they ever disagree every frame fails verification —
      # which presents as a total replication outage with no obvious cause.
      sign!(false)

      for {name, frame} <- all_frames() do
        [header, shard | _payload] = frame
        expected = IO.iodata_to_binary([header, shard])
        actual = Protocol.signable(IO.iodata_to_binary(frame))

        assert actual == expected,
               "#{name}: signable/1 covered #{byte_size(actual)} bytes but seal/1 signs " <>
                 "#{byte_size(expected)} — the two sides would never agree on a tag"
      end
    end

    test "a truncated frame does not raise" do
      # Bytes off a socket are the one input never under our control; `signable/1` runs BEFORE
      # `decode/1` has validated anything, so it sees garbage first.
      for {_name, frame} <- all_frames() do
        bin = IO.iodata_to_binary(frame)

        for take <- 0..min(byte_size(bin), 12) do
          assert is_binary(Protocol.signable(binary_part(bin, 0, take)))
        end
      end
    end
  end

  describe "the boot guard" do
    setup do
      prev_hrana = Application.get_env(:fathom, :hrana_token_secret)

      on_exit(fn ->
        if is_nil(prev_hrana),
          do: Application.delete_env(:fathom, :hrana_token_secret),
          else: Application.put_env(:fathom, :hrana_token_secret, prev_hrana)

        FrameAuth.forget_key()
      end)

      :ok
    end

    test "signing with NO key refuses to boot" do
      # The quiet failure this exists for: `sign/1` answers nil with no key and `seal/1` then sends
      # the frame UNSIGNED. The node believes it is authenticating and is not, which is the worst
      # of the three states — it would pass every "is signing on?" check an operator could run.
      Application.delete_env(:fathom, :replication_hmac_secret)
      Application.delete_env(:fathom, :hrana_token_secret)
      FrameAuth.forget_key()
      sign!(true)
      require!(false)

      assert_raise RuntimeError, ~r/no key is configured/, fn ->
        Fathom.Application.check_replication_frame_auth!()
      end
    end

    test "requiring with NO key refuses to boot" do
      Application.delete_env(:fathom, :replication_hmac_secret)
      Application.delete_env(:fathom, :hrana_token_secret)
      FrameAuth.forget_key()
      sign!(true)
      require!(true)

      assert_raise RuntimeError, ~r/no key is configured/, fn ->
        Fathom.Application.check_replication_frame_auth!()
      end
    end

    test "REQUIRING without SIGNING refuses to boot — the rollout run backwards" do
      # Step 3 before step 2. This node refuses every peer that has not enabled signing, while
      # itself sending frames those peers will refuse once they do. Every shard replicated across
      # that boundary loses quorum, which is a write outage, not a degradation.
      sign!(false)
      require!(true)

      assert_raise RuntimeError,
                   ~r/must be enabled fleet-wide BEFORE|before any node requires/i,
                   fn ->
                     Fathom.Application.check_replication_frame_auth!()
                   end
    end

    test "the HRANA secret alone satisfies the key requirement" do
      # The documented fallback: a fleet already distributing one shared secret should not have to
      # distribute a second before it can turn this on.
      Application.delete_env(:fathom, :replication_hmac_secret)
      Application.put_env(:fathom, :hrana_token_secret, "an-existing-fleet-wide-hrana-secret")
      FrameAuth.forget_key()
      sign!(true)
      require!(true)

      assert Fathom.Application.check_replication_frame_auth!() == nil
    end

    test "both flags off is always fine, key or no key" do
      Application.delete_env(:fathom, :replication_hmac_secret)
      Application.delete_env(:fathom, :hrana_token_secret)
      FrameAuth.forget_key()
      sign!(false)
      require!(false)

      assert Fathom.Application.check_replication_frame_auth!() == nil
    end
  end

  describe "key derivation" do
    test "the configured secret is NEVER the key" do
      # Domain separation. If the raw secret were the HMAC key, a leaked replication key would BE
      # the Hrana token-signing secret, and anyone holding it could mint a tenant credential for
      # any shard in the fleet.
      Application.put_env(:fathom, :replication_hmac_secret, @secret)
      FrameAuth.forget_key()

      tag = FrameAuth.sign("some frame header")

      raw =
        binary_part(:crypto.mac(:hmac, :sha256, @secret, "some frame header"), 0, byte_size(tag))

      refute tag == raw,
             "the configured secret is being used directly as the HMAC key — a leaked " <>
               "replication key is then the Hrana token secret"
    end

    test "REPLICATION_HMAC_SECRET wins over the HRANA fallback" do
      Application.put_env(:fathom, :hrana_token_secret, "the-hrana-one")
      Application.put_env(:fathom, :replication_hmac_secret, "the-replication-one")
      FrameAuth.forget_key()
      dedicated = FrameAuth.sign("m")

      Application.delete_env(:fathom, :replication_hmac_secret)
      FrameAuth.forget_key()
      fallback = FrameAuth.sign("m")

      refute dedicated == fallback,
             "the dedicated secret did not take precedence, so the two cannot be rotated apart"

      Application.delete_env(:fathom, :hrana_token_secret)
      FrameAuth.forget_key()
      assert FrameAuth.sign("m") == nil
    end
  end

  describe "prev_extent (#11a)" do
    test "rides the signed shape and survives the round trip" do
      sign!(true)
      require!(true)
      frame = IO.iodata_to_binary(Protocol.encode_push(push(prev_extent: 8192)))
      assert {:ok, %Protocol.Push{prev_extent: 8192}} = Protocol.decode(frame)
    end

    test "the legacy shape reports 0 — 'no statement', never a fabricated gap" do
      # An un-upgraded peer sets no field. Reading absence as a gap would mark every replica in the
      # fleet torn for the length of a rolling upgrade.
      sign!(false)
      frame = IO.iodata_to_binary(Protocol.encode_push(push(prev_extent: 8192)))
      assert {:ok, %Protocol.Push{prev_extent: 0}} = Protocol.decode(frame)
    end

    test "defaults to 0 when never set" do
      assert %Protocol.Push{prev_extent: 0} = push()
    end
  end
end
