defmodule Fathom.Shard.Replication.FollowerOrdinalTest do
  @moduledoc """
  The replica's half of the monotone WAL ordinal — expert review 2026-08-26 #2, step 3.

  `Promote.fresher?/2` can only rank a replica against the stored object if BOTH carry a number on
  the same scale. `Fathom.Shard.wal_ordinal/2` is the one that assigns it, `Replication.Session`
  pushes it, and `FollowerLog` is where the replica records it. These pin the three rules that make
  the recorded value trustworthy rather than merely present:

    * a seed states NOTHING, because `SeedBegin` carries no ordinal and inventing one would put the
      replica on a scale the primary never assigned;
    * an APPEND may learn one it did not have (the gate was flipped on mid-session) but must never
      unlearn one, because 0 is "unknown" and moving a rankable replica back to unknown costs a
      promotion for no reason;
    * a RESET takes the push's value outright, because a different WAL's ordinal is exactly the
      unsound comparison #2 exists to remove.

  Pure — `FollowerLog` is a decision function over maps, which is why it can be tested without a
  socket, a shard, or a primary.
  """
  use ExUnit.Case, async: true

  alias Fathom.Shard.Replication.FollowerLog
  alias Fathom.Shard.Replication.Protocol.Push

  defp push(opts) do
    %Push{
      shard_id: "acme",
      epoch: Keyword.get(opts, :epoch, 1),
      wal_gen: Keyword.get(opts, :wal_gen, 0),
      salt1: Keyword.get(opts, :salt1, 977_542_977),
      offset: Keyword.get(opts, :offset, 0),
      payload: Keyword.get(opts, :payload, "abcd"),
      wal_ordinal: Keyword.get(opts, :wal_ordinal)
    }
  end

  test "a seed states no ordinal" do
    state = FollowerLog.seeded(1, 0, 977_542_977, 4096, 3)

    assert state.wal_ordinal == 0,
           "a seed invented an ordinal. SeedBegin carries none, so any value here is on a scale " <>
             "the primary never assigned"
  end

  test "an append records an ordinal the follower did not have" do
    state = FollowerLog.seeded(1, 0, 977_542_977, 4096, 3)

    assert {:append, next} =
             FollowerLog.decide(state, push(offset: 4096, wal_ordinal: 7, payload: "xy"))

    assert next.wal_ordinal == 7
    assert next.next_offset == 4098, "the append itself stopped working"
  end

  # The gate-off and old-peer case. 0 on the wire means "not stated", and writing it over a known
  # ordinal would move a rankable replica back to unknown — losing a promotion for no reason, since
  # an APPEND is by definition the same WAL and therefore the same ordinal.
  test "an append with no stated ordinal keeps the one already recorded" do
    state = %{FollowerLog.seeded(1, 0, 977_542_977, 4096, 3) | wal_ordinal: 7}

    assert {:append, next} =
             FollowerLog.decide(state, push(offset: 4096, wal_ordinal: nil, payload: "xy"))

    assert next.wal_ordinal == 7, "an unstated ordinal unlearned a known one"

    # An explicit 0 is the same statement as `nil` — the wire has no other way to say "unknown".
    assert {:append, next} =
             FollowerLog.decide(state, push(offset: 4096, wal_ordinal: 0, payload: "xy"))

    assert next.wal_ordinal == 7
  end

  # THE ONE THAT MATTERS. A salt change means SQLite recreated the `-wal`, so the bytes are from a
  # different file entirely. Carrying the previous WAL's ordinal across that seam is precisely the
  # comparison #2 exists to remove: the replica would claim a place in an order it is not in.
  test "a reset takes the push's ordinal outright rather than carrying the old one" do
    state = %{FollowerLog.seeded(1, 0, 977_542_977, 4096, 3) | wal_ordinal: 7}

    assert {:reset_then_append, next} =
             FollowerLog.decide(state, push(salt1: 978_380_554, offset: 0, wal_ordinal: 8))

    assert next.wal_ordinal == 8
    assert next.salt1 == 978_380_554
    assert next.torn, "a reset must still mark the replica torn — see decide_fresh/2"
  end

  test "a reset with no stated ordinal CLEARS the old one to unknown" do
    state = %{FollowerLog.seeded(1, 0, 977_542_977, 4096, 3) | wal_ordinal: 7}

    assert {:reset_then_append, next} =
             FollowerLog.decide(state, push(salt1: 978_380_554, offset: 0, wal_ordinal: nil))

    assert next.wal_ordinal == 0,
           "a stale ordinal survived onto a DIFFERENT WAL. Unknown is the honest answer here and " <>
             "it costs only a promotion; a wrong ordinal costs the acked tail"
  end

  # A new primary took over: its WAL is its own, so this routes through the same reset path and the
  # ordinal must come from the new owner, not from the old one.
  test "an epoch takeover takes the new primary's ordinal" do
    state = %{FollowerLog.seeded(1, 0, 977_542_977, 4096, 3) | wal_ordinal: 7}

    assert {:reset_then_append, next} =
             FollowerLog.decide(state, push(epoch: 2, offset: 0, wal_ordinal: 1))

    assert next.epoch == 2
    assert next.wal_ordinal == 1
  end
end
