defmodule Fathom.Shard.Replication.PushCompressTest do
  @moduledoc """
  The zstd-compressed push frame, `@push_ord_lin_z` (perf review 2026-10-01 #32).

  Measured on 1,000 TPC-B commits (17 KB of WAL frames each): zstd-1 shrinks a push 4.5x at 12 µs
  with random row data and 13.6x at 8 µs with pgbench's padding. These pin that it is lossless,
  that it only appears where it should, and — because pushes sign the HEADER only — that the
  unsigned payload cannot be used to make a follower inflate more than the signed length.
  """
  use ExUnit.Case, async: false

  alias Fathom.Shard.Replication.FrameAuth
  alias Fathom.Shard.Replication.Protocol
  alias Fathom.Shard.Replication.Protocol.Push

  @push_ord_lin 17
  @push_ord_lin_z 19

  setup do
    prev =
      for k <- [
            :replication_ordinal_wire,
            :replication_sign_frames,
            :replication_hmac_secret,
            :replication_hmac_required
          ],
          into: %{},
          do: {k, Application.get_env(:fathom, k)}

    Application.put_env(:fathom, :replication_ordinal_wire, true)

    on_exit(fn ->
      for {k, v} <- prev,
          do:
            if(is_nil(v),
              do: Application.delete_env(:fathom, k),
              else: Application.put_env(:fathom, k, v)
            )

      FrameAuth.forget_key()
    end)

    :ok
  end

  # See push_ordinal_wire_test: the derived key is memoized, so forget it or the frames ship unsigned.
  defp enable_signing do
    Application.put_env(:fathom, :replication_hmac_secret, :crypto.strong_rand_bytes(32))
    Application.put_env(:fathom, :replication_sign_frames, true)
    FrameAuth.forget_key()

    assert FrameAuth.key_configured?(),
           "fixture: no signing key in force; frames would be unsigned"
  end

  # A WAL-frame-like payload: 4 KiB pages, mostly unchanged bytes.
  defp wal_payload,
    do: :binary.copy(<<0::size(4000)-unit(8), "row-change", 0::size(86)-unit(8)>>, 4)

  defp push(payload, zpayload) do
    %Push{
      shard_id: "acme",
      epoch: 7,
      wal_gen: 3,
      salt1: 977_542_977,
      offset: 4096,
      payload: payload,
      prev_extent: 2048,
      wal_ordinal: 11,
      lineage: 5,
      zpayload: zpayload
    }
  end

  defp compressed_push do
    payload = wal_payload()
    z = Protocol.compress_payload(payload)
    assert is_binary(z) and byte_size(z) < byte_size(payload), "fixture did not compress"
    push(payload, z)
  end

  defp frame_type(iodata) do
    case IO.iodata_to_binary(iodata) do
      # A signed envelope wraps the inner frame after its tag.
      <<_v::8, 12::8, tlen::8, _tag::binary-size(tlen), _v2::8, type::8, _::binary>> -> type
      <<_v::8, type::8, _::binary>> -> type
    end
  end

  # A hand-built compressed frame, so the decoder can be fed lies the encoder would never emit.
  defp raw_frame(raw_len, zpayload, shard \\ "acme") do
    <<Protocol.version()::8, @push_ord_lin_z::8, byte_size(shard)::16, 7::64, 3::64, 1::64,
      4096::64, 0::64, 11::64, 5::64, raw_len::64, shard::binary, zpayload::binary>>
  end

  describe "the encoder" do
    test "a push carrying a compressed payload goes out as @push_ord_lin_z and round-trips exactly" do
      p = compressed_push()
      frame = Protocol.encode_push(p)
      assert frame_type(frame) == @push_ord_lin_z

      assert IO.iodata_length(frame) < byte_size(p.payload),
             "the compressed frame is not smaller than the raw payload"

      assert {:ok, decoded} = frame |> IO.iodata_to_binary() |> Protocol.decode()
      assert decoded.payload == p.payload

      assert {decoded.shard_id, decoded.epoch, decoded.wal_gen, decoded.salt1} ==
               {"acme", 7, 3, 977_542_977}

      assert {decoded.offset, decoded.prev_extent, decoded.wal_ordinal, decoded.lineage} ==
               {4096, 2048, 11, 5}

      assert decoded.zpayload == nil,
             "zpayload is a send-side field and must never come off the wire"
    end

    test "without a compressed payload the frame is the plain @push_ord_lin, unchanged" do
      assert frame_type(Protocol.encode_push(push(wal_payload(), nil))) == @push_ord_lin
    end

    test "with the ordinal gate off a compressed payload is ignored (older shapes never carry it)" do
      Application.put_env(:fathom, :replication_ordinal_wire, false)
      p = compressed_push()
      refute frame_type(Protocol.encode_push(p)) == @push_ord_lin_z

      assert {:ok, %Push{payload: payload}} =
               p |> Protocol.encode_push() |> IO.iodata_to_binary() |> Protocol.decode()

      assert payload == p.payload
    end

    test "compress_payload/1 declines what it cannot shrink" do
      assert Protocol.compress_payload(:crypto.strong_rand_bytes(17_000)) == nil
      assert Protocol.compress_payload("") == nil
    end

    test "compression is on by default" do
      Application.delete_env(:fathom, :replication_compress)
      assert Protocol.compress?()
    end
  end

  describe "signing" do
    test "a signed compressed push round-trips" do
      enable_signing()
      p = compressed_push()

      assert {:ok, %Push{payload: payload}} =
               p |> Protocol.encode_push() |> IO.iodata_to_binary() |> Protocol.decode()

      assert payload == p.payload
    end

    # raw_len is what bounds decompression of the UNSIGNED payload, so it must be inside the MAC.
    test "the uncompressed length is signed: tampering with it is refused" do
      enable_signing()
      bin = compressed_push() |> Protocol.encode_push() |> IO.iodata_to_binary()
      <<v::8, 12::8, tlen::8, tag::binary-size(tlen), inner::binary>> = bin

      # raw_len is the 8 bytes at offset 60 of the inner frame (after the 60-byte @push_ord_lin header).
      <<head::binary-size(60), raw_len::64, tail::binary>> = inner

      tampered =
        <<v::8, 12::8, tlen::8, tag::binary, head::binary, raw_len + 1::64, tail::binary>>

      assert Protocol.decode(tampered) == {:error, :unauthenticated}
    end

    test "the signed prefix is the 68-byte header plus the shard id" do
      frame = raw_frame(10, "x")
      assert Protocol.signable(frame) == binary_part(frame, 0, 68 + byte_size("acme"))
    end
  end

  describe "the decoder never trusts the unsigned payload" do
    # THE BOMB. A payload that inflates to 512 MiB under a signed length of 100. Decoding must stop
    # at the bound rather than allocate what the payload claims — checked by decoding in a process
    # whose heap (shared binaries included) is capped far below 512 MiB: without the bound it is
    # killed, and the result is the length check's :malformed only after the damage is done.
    test "a payload that inflates past its signed length is refused without inflating it" do
      bomb = IO.iodata_to_binary(:zstd.compress(:binary.copy(<<0>>, 512 * 1024 * 1024)))
      assert byte_size(bomb) < 64 * 1024, "fixture: the bomb should be small on the wire"
      frame = raw_frame(100, bomb)
      parent = self()

      {pid, ref} =
        spawn_monitor(fn ->
          Process.flag(:max_heap_size, %{
            size: div(32 * 1024 * 1024, :erlang.system_info(:wordsize)),
            kill: true,
            error_logger: false,
            include_shared_binaries: true
          })

          send(parent, {:decoded, Protocol.decode(frame)})
        end)

      assert_receive {:DOWN, ^ref, :process, ^pid, reason}, 30_000

      assert reason == :normal,
             "the decoder was killed for inflating the bomb (#{inspect(reason)})"

      assert_received {:decoded, {:error, :malformed}}
    end

    test "a payload that inflates SHORT of its signed length is refused" do
      z = IO.iodata_to_binary(:zstd.compress("only these bytes"))
      assert Protocol.decode(raw_frame(1_000, z)) == {:error, :malformed}
    end

    test "a signed length beyond the frame cap is refused before any decompression" do
      z = IO.iodata_to_binary(:zstd.compress("x"))
      assert Protocol.decode(raw_frame(Protocol.max_frame_bytes() + 1, z)) == {:error, :malformed}
    end

    test "a payload that is not zstd is refused, not raised" do
      assert Protocol.decode(raw_frame(16, "definitely not zstd")) == {:error, :malformed}
    end

    test "an honest hand-built frame decodes (the cases above fail for their stated reason)" do
      body = "sixteen bytes!!!"
      z = IO.iodata_to_binary(:zstd.compress(body))
      assert {:ok, %Push{payload: ^body}} = Protocol.decode(raw_frame(byte_size(body), z))
    end
  end
end
