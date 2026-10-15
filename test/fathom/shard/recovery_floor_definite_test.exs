defmodule Fathom.Shard.RecoveryFloorDefiniteTest do
  @moduledoc """
  The recovery responder floor counts only answers that could hold the acked tail
  (expert review 2026-10-15 #4).

  ## The bug

  `Recovery` refuses to decide a failover until `N - Q + 1` DEFINITE answers are in, because any
  that many intersect the `Q` followers a write was acked at. Two kinds of answer were counted that
  cannot carry the tail:

    1. **An unrankable replica.** After a follower restart, recovery rebuilt every replica at
       `wal_ordinal: 0` ("not stated"), which `Promote.fresher?/2` refuses to rank. The floor was
       "met" by that answer while the only copy of the tail was invisible to `choose/3`: acked
       writes lost on a failover that follows a rolling deploy.
    2. **Self, when self is the previous owner.** A node that held the lease was never its own
       follower, yet `answered/2` always added `1 +` for itself, so a same-node reopen after a steal
       decided on `N - Q` real followers.

  ## The fix (default D2)

  The follower persists the ordinal beside the WAL it describes, keyed by that WAL's salt, so a
  restarted replica stays rankable. `Recovery.answered/3` then excludes unrankable positions (which
  now means "no persisted ordinal"), a torn local replica, and self when the previous owner's
  `node()` is this node. Peer previous-owner exclusion by NODE_KEY is expert review 2026-10-15 #17.
  """
  use ExUnit.Case, async: false

  alias Exqlite.Sqlite3
  alias Fathom.Shard.Connection
  alias Fathom.Shard.Replication.Follower
  alias Fathom.Shard.Replication.Promote
  alias Fathom.Shard.Replication.Recovery
  alias Fathom.Shard.Replication.Wal

  defp ep(key), do: {key, ~c"127.0.0.1", 1}
  defp pos(ordinal, lineage \\ 3), do: %{lineage: lineage, wal_ordinal: ordinal, torn: false}

  describe "Recovery.answered/3 — what counts toward the floor" do
    test "a rankable position and a definite 'nothing' both count, plus self" do
      answers = [{ep("b"), {:answered, pos(4)}}, {ep("c"), {:answered, nil}}]
      assert Recovery.answered(answers, nil, self_node: "a@h") == 3
    end

    test "an UNRANKABLE position (ordinal 0) does not count — the tail it holds is invisible" do
      answers = [{ep("b"), {:answered, pos(0)}}, {ep("c"), {:answered, nil}}]
      assert Recovery.answered(answers, nil, self_node: "a@h") == 2
    end

    test "a position with no lineage does not count either" do
      answers = [{ep("b"), {:answered, pos(4, 0)}}]
      assert Recovery.answered(answers, nil, self_node: "a@h") == 1
    end

    test "silent peers never count" do
      assert Recovery.answered([{ep("b"), :silent}], nil, self_node: "a@h") == 1
    end

    test "this node does not count itself when IT is the previous owner" do
      answers = [{ep("b"), {:answered, pos(4)}}]
      opts = [self_node: "a@h", previous_owner: "a@h#oldincarnation"]
      assert Recovery.answered(answers, nil, opts) == 1

      # A different node's previous ownership leaves self counted.
      assert Recovery.answered(answers, nil, self_node: "a@h", previous_owner: "z@h#1") == 2
      # A node name that merely shares a prefix is not self.
      assert Recovery.answered(answers, nil, self_node: "a@h", previous_owner: "a@hx#1") == 2
    end

    test "self's own replica must be definite too: unrankable or torn local does not count" do
      assert Recovery.answered([], pos(0), self_node: "a@h") == 0
      assert Recovery.answered([], %{pos(4) | torn: true}, self_node: "a@h") == 0
      assert Recovery.answered([], pos(4), self_node: "a@h") == 1
    end
  end

  describe "the follower's ordinal survives a restart" do
    setup do
      id = "floordef_#{System.unique_integer([:positive])}"
      root = Path.join(System.tmp_dir!(), "floordef_#{System.unique_integer([:positive])}")
      File.mkdir_p!(root)
      on_exit(fn -> File.rm_rf(root) end)
      %{id: id, root: root}
    end

    # A real SQLite `.db` + `-wal` pair copied into the follower's directory, as a seed leaves it.
    defp plant_replica!(root, name, id) do
      src = Path.join(root, "src.db")
      {:ok, conn} = Connection.open(src)
      :ok = Sqlite3.execute(conn, "PRAGMA wal_autocheckpoint=0")
      :ok = Sqlite3.execute(conn, "CREATE TABLE t (a INTEGER PRIMARY KEY)")
      for i <- 1..5, do: :ok = Sqlite3.execute(conn, "INSERT INTO t VALUES (#{i})")
      File.cp!(src, Follower.db_path(name, id))
      File.cp!(src <> "-wal", Follower.wal_path(name, id))
      :ok = Connection.close(conn)
      {:ok, h} = Wal.read(Follower.wal_path(name, id))
      h
    end

    defp start!(root, name, tag) do
      pid =
        start_supervised!({Follower, name: name, port: 0, dir: Path.join(root, "f")},
          id: :"#{name}_#{tag}"
        )

      _ = :sys.get_state(pid)
      pid
    end

    test "a ranked replica is still ranked after its follower restarts", %{id: id, root: root} do
      name = :"floordef_f_#{System.unique_integer([:positive])}"
      start!(root, name, :first)
      h = plant_replica!(root, name, id)

      :ok = Follower.seed(name, id, 1, h.ckpt_seq, h.salt1, h.commit_extent, 3)
      :ok = Follower.note_ordinal(name, id, 5)
      before = Follower.state_of(name, id)
      assert before.wal_ordinal == 5

      # The object is one generation behind: the replica holds the acked tail.
      stamp = %{epoch: 3, wal_ordinal: 4, offset: h.commit_extent}
      assert Promote.fresher?(before, stamp), "fixture: the replica is not ahead before restart"

      stop_supervised!(:"#{name}_first")
      start!(root, name, :second)
      after_restart = Follower.state_of(name, id)

      assert after_restart.wal_ordinal == 5,
             "the restart forgot the replica's ordinal; it is unrankable until its next push, " <>
               "so a failover now loses the acked tail it holds"

      assert Promote.fresher?(after_restart, stamp)
      assert Recovery.answered([], after_restart, self_node: "a@h") == 1
    end

    test "an ordinal persisted for a DIFFERENT WAL is not adopted", %{id: id, root: root} do
      name = :"floordef_f_#{System.unique_integer([:positive])}"
      start!(root, name, :first)
      h = plant_replica!(root, name, id)
      :ok = Follower.seed(name, id, 1, h.ckpt_seq, h.salt1, h.commit_extent, 3)
      :ok = Follower.note_ordinal(name, id, 5)
      ordinal_file = Follower.ordinal_path(name, id)
      assert File.exists?(ordinal_file)
      stop_supervised!(:"#{name}_first")

      # A crash between the WAL bytes and the ordinal file: they name different salts.
      File.write!(ordinal_file, "5 #{h.salt1 + 1}")

      start!(root, name, :second)
      assert Follower.state_of(name, id).wal_ordinal == 0
      # And so it does not count toward the floor: unknown, not "nothing".
      assert Recovery.answered([], Follower.state_of(name, id), self_node: "a@h") == 0
    end

    test "forget/2 removes the ordinal file", %{id: id, root: root} do
      name = :"floordef_f_#{System.unique_integer([:positive])}"
      start!(root, name, :first)
      h = plant_replica!(root, name, id)
      :ok = Follower.seed(name, id, 1, h.ckpt_seq, h.salt1, h.commit_extent, 3)
      :ok = Follower.note_ordinal(name, id, 5)
      assert File.exists?(Follower.ordinal_path(name, id))
      :ok = Follower.forget(name, id)
      refute File.exists?(Follower.ordinal_path(name, id))
    end
  end
end
