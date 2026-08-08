defmodule Fathom.Shard.ReplicationTest do
  @moduledoc """
  The A2 replication core — Phase 2, see `docs/a2-quorum-replication.md`.

  These three modules are pure on purpose, because every way A2 can corrupt a tenant's database is
  a decision one of them makes: accepting frames from a deposed primary, appending across a
  checkpoint seam, writing a delta at the wrong offset, or satisfying a commit from a follower that
  is not actually in step.

  None of that needs a socket, and the failure mode is the quiet kind — a follower that appends the
  wrong bytes does not raise, it produces a SQLite file that looks fine until it is promoted and the
  primary that could have corrected it is gone.
  """
  use ExUnit.Case, async: true

  alias Fathom.Shard.Replication.FollowerLog
  alias Fathom.Shard.Replication.Protocol
  alias Fathom.Shard.Replication.Protocol.Push
  alias Fathom.Shard.Replication.Quorum

  defp push(opts \\ []) do
    %Push{
      shard_id: Keyword.get(opts, :shard_id, "acme"),
      epoch: Keyword.get(opts, :epoch, 7),
      wal_gen: Keyword.get(opts, :wal_gen, 3),
      offset: Keyword.get(opts, :offset, 4120),
      payload: Keyword.get(opts, :payload, :binary.copy(<<0xAB>>, 4120))
    }
  end

  describe "Protocol" do
    test "a push round-trips with the payload byte-identical" do
      p = push(payload: :crypto.strong_rand_bytes(9000))

      assert {:ok, decoded} =
               p |> Protocol.encode_push() |> IO.iodata_to_binary() |> Protocol.decode()

      assert decoded == p
    end

    test "a shard id containing the delimiterless header bytes still decodes" do
      # The header is fixed-width with an explicit length, so no byte in the id is special. Worth
      # pinning: a future "optimisation" to a delimiter would break exactly this.
      p = push(shard_id: <<0, 1, 2, 255>>, payload: "x")
      assert {:ok, ^p} = p |> Protocol.encode_push() |> IO.iodata_to_binary() |> Protocol.decode()
    end

    test "ack and reject round-trip, and both name their shard" do
      # The shard id on the REPLY is what lets one socket per follower node serve every shard:
      # without it the primary cannot tell which push an ack belongs to, forcing a connection per
      # shard per follower — millions of sockets at fathom's stated scale.
      assert {:ok, {:ack, "acme", 8240}} =
               "acme" |> Protocol.encode_ack(8240) |> IO.iodata_to_binary() |> Protocol.decode()

      assert {:ok, {:reject, "beta", :offset_mismatch, 4120}} =
               "beta"
               |> Protocol.encode_reject(:offset_mismatch, 4120)
               |> IO.iodata_to_binary()
               |> Protocol.decode()
    end

    test "garbage and truncated frames are errors, never crashes" do
      assert {:error, :malformed} = Protocol.decode(<<>>)
      assert {:error, :malformed} = Protocol.decode(<<1, 1, 0>>)

      # Correct version byte, garbage after it. NOT `strong_rand_bytes(7)` on its own — a random
      # first byte is almost never a valid version, so that lands in :unsupported_version and the
      # assertion would be testing the version clause while claiming to test framing.
      assert {:error, :malformed} =
               Protocol.decode(<<Protocol.version()::8>> <> :crypto.strong_rand_bytes(6))

      # A push claiming a longer shard id than it carries must not decode.
      assert {:error, :malformed} =
               Protocol.decode(<<1, 1, 99::16, 0::64, 0::64, 0::64, "short">>)
    end

    test "a different protocol version is distinguishable from garbage" do
      bad = <<Protocol.version() + 1::8, 1::8, 0::16, 0::64, 0::64, 0::64>>
      assert {:error, :unsupported_version} = Protocol.decode(bad)
    end
  end

  describe "FollowerLog — the corruption guards" do
    test "a shard that was never seeded is refused" do
      assert {:reject, :unknown_shard, 0} = FollowerLog.decide(nil, push())
    end

    test "a deposed primary cannot land writes on the new owner" do
      state = FollowerLog.seeded(7, 3, 4120)
      assert {:reject, :stale_epoch, 0} = FollowerLog.decide(state, push(epoch: 6))
    end

    test "a new epoch resets, and only from the start of its WAL" do
      state = FollowerLog.seeded(7, 3, 4120)

      assert {:reset_then_append, new} =
               FollowerLog.decide(state, push(epoch: 8, wal_gen: 1, offset: 0, payload: "abc"))

      assert new.epoch == 8 and new.wal_gen == 1 and new.next_offset == 3

      # Mid-stream from a new primary is meaningless — our offsets do not carry across ownership.
      assert {:reject, :offset_mismatch, 0} =
               FollowerLog.decide(state, push(epoch: 8, wal_gen: 1, offset: 4120))
    end

    test "frames from before a checkpoint we already applied are dropped" do
      state = FollowerLog.seeded(7, 3, 4120)
      assert {:reject, :stale_wal_gen, 0} = FollowerLog.decide(state, push(wal_gen: 2))
    end

    test "a checkpoint seam forces a reset instead of appending across it" do
      state = FollowerLog.seeded(7, 3, 4120)

      # THE corruption case. After the primary checkpoints, its WAL restarts with fresh salts, so
      # byte offset 4120 in generation 4 is unrelated to byte offset 4120 in generation 3.
      # Appending across the seam produces a file that passes quick_check and holds wrong data.
      assert {:reset_then_append, new} =
               FollowerLog.decide(state, push(wal_gen: 4, offset: 0, payload: "xy"))

      assert new.wal_gen == 4 and new.next_offset == 2

      assert {:reject, :offset_mismatch, 0} =
               FollowerLog.decide(state, push(wal_gen: 4, offset: 4120))
    end

    test "an in-order delta appends and advances by exactly the payload size" do
      state = FollowerLog.seeded(7, 3, 4120)

      assert {:append, new} =
               FollowerLog.decide(state, push(offset: 4120, payload: :binary.copy(<<1>>, 500)))

      assert new.next_offset == 4620
      assert new.epoch == 7 and new.wal_gen == 3
    end

    test "a gap is retryable — the reject says where the follower actually is" do
      state = FollowerLog.seeded(7, 3, 4120)

      # A push that skips ahead. Silently accepting this is the corruption; the expected offset is
      # what lets the primary rewind instead of re-seeding the whole shard from S3.
      assert {:reject, :offset_mismatch, 4120} = FollowerLog.decide(state, push(offset: 8240))

      # And a duplicate/reordered push that arrives late.
      assert {:reject, :offset_mismatch, 4120} = FollowerLog.decide(state, push(offset: 0))
    end
  end

  describe "Quorum" do
    test "Q = N is impossible to construct" do
      # The measured finding the design rests on: Q=N tolerates zero follower failures and inherits
      # the slowest replica (32-82x worse). It must not be reachable by config.
      assert_raise ArgumentError, ~r/must be < 4 followers/, fn -> Quorum.new(4, 4, 0) end
      assert_raise ArgumentError, ~r/must be < 4 followers/, fn -> Quorum.new(4, 5, 0) end
      assert_raise ArgumentError, ~r/at least 1/, fn -> Quorum.new(4, 0, 0) end
    end

    test "reaches at exactly Q acks, not before" do
      q = Quorum.new(4, 2, 100)

      assert {:pending, q} = Quorum.ack(q, :f1, 100)
      assert Quorum.remaining(q) == 1
      assert {:reached, q} = Quorum.ack(q, :f2, 100)
      assert Quorum.remaining(q) == 0
    end

    test "one chatty follower cannot satisfy a quorum by itself" do
      q = Quorum.new(4, 2, 100)

      assert {:pending, q} = Quorum.ack(q, :f1, 100)
      assert {:pending, q} = Quorum.ack(q, :f1, 100)
      assert {:pending, _} = Quorum.ack(q, :f1, 100)
    end

    test "an ack for the wrong offset counts AGAINST the quorum, not toward it" do
      # A follower that is writing at a different position is diverged, not in step. Counting it
      # would let a follower with the wrong bytes satisfy a commit.
      q = Quorum.new(4, 2, 100)

      assert {:pending, q} = Quorum.ack(q, :f1, 999)
      assert Quorum.remaining(q) == 2
      assert {:pending, q} = Quorum.ack(q, :f2, 100)
      assert {:reached, _} = Quorum.ack(q, :f3, 100)
    end

    test "fails fast once the quorum has become unreachable" do
      # 4 followers, need 2: after 3 rejections only one can still ack, so waiting is an outage
      # dressed up as latency.
      q = Quorum.new(4, 2, 100)

      assert {:pending, q} = Quorum.reject(q, :f1, :offset_mismatch, 0)
      assert {:pending, q} = Quorum.reject(q, :f2, :stale_epoch, 0)
      assert {:impossible, _} = Quorum.reject(q, :f3, :internal, 0)
    end

    test "a reached quorum is not undone by a later rejection" do
      # The stragglers' fate does not change a commit that has already been acknowledged.
      q = Quorum.new(4, 2, 100)
      assert {:pending, q} = Quorum.ack(q, :f1, 100)
      assert {:reached, q} = Quorum.ack(q, :f2, 100)
      assert {:reached, _} = Quorum.reject(q, :f3, :internal, 0)
    end
  end
end
