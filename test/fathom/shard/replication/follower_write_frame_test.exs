defmodule Fathom.Shard.Replication.FollowerWriteFrameTest do
  @moduledoc """
  Expert review 2026-08-31 #1 — the follower's WAL write must be POSITIONAL, not append.

  A frame is written at its own `push.offset`. The previous `[:append]` open wrote at EOF and only
  stayed correct because `FollowerLog.decide/2` guarantees `push.offset == next_offset` — an ETS
  value never checked against the file's actual size. When a write's bytes landed but the
  follower's state did not advance (a failed close/put_state), the primary re-sent from the
  un-advanced offset and the append laid a SECOND copy of the frame after the first. A duplicate
  frame breaks SQLite's cumulative WAL checksum chain: on promote/recover SQLite treats the
  duplicate as end-of-WAL and silently drops every acked frame after it.

  These pin idempotency-under-re-send, the invariant `pwrite` restores and `[:append]` violated.
  """
  use ExUnit.Case, async: true

  alias Fathom.Shard.Replication.Follower

  setup do
    path = Path.join(System.tmp_dir!(), "wframe_#{System.unique_integer([:positive])}.wal")
    on_exit(fn -> File.rm(path) end)
    %{path: path}
  end

  test "a re-sent append frame is idempotent — it does not duplicate at EOF", %{path: path} do
    base = :binary.copy(<<0xAB>>, 4096)
    File.write!(path, base)

    # First application of the frame at its offset.
    assert :ok = Follower.write_frame(path, 4096, "XY", :append)
    assert File.read!(path) == base <> "XY"

    # The re-send: the primary did not see our ack (our state failed to advance), so it ships the
    # SAME frame at the SAME offset again. Positional write leaves the file byte-identical; the old
    # `[:append]` path appended a SECOND copy, corrupting the WAL checksum chain.
    assert :ok = Follower.write_frame(path, 4096, "XY", :append)

    assert File.read!(path) == base <> "XY",
           "a re-sent frame duplicated at EOF instead of overwriting at its offset — the WAL " <>
             "checksum chain is now broken and the acked tail is lost at promotion"
  end

  test "an append frame lands at its offset even when the file is shorter than expected", %{
    path: path
  } do
    # The file-EOF == next_offset invariant having drifted is exactly the silent case. A positional
    # write puts the frame where the primary says it belongs regardless of the current file size.
    File.write!(path, :binary.copy(<<0>>, 16))

    assert :ok = Follower.write_frame(path, 16, "tail", :append)
    assert File.read!(path) == :binary.copy(<<0>>, 16) <> "tail"
  end

  test "a truncate frame replaces the file from position 0", %{path: path} do
    File.write!(path, :binary.copy(<<0xFF>>, 8192))

    assert :ok = Follower.write_frame(path, 0, "new-generation", :truncate)

    assert File.read!(path) == "new-generation",
           "a reset must truncate the old generation, not leave its tail behind"
  end
end
