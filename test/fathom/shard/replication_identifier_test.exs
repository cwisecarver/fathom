defmodule Fathom.Shard.ReplicationIdentifierTest do
  @moduledoc """
  THE SHARD-ISOLATION GATE, applied to the replication RECEIVE path (expert review 2026-08-20 #37).

  AGENTS.md requires a cross-shard-isolation test for any change to shard resolution or shard-path
  construction. `Follower` builds filesystem paths out of a `shard_id` that arrives **from the
  network**, and had no such test.

  The suite already had the exact shape one directory over, for the strictly SAFER path
  (`warm_follower_test.exs`: `for bad <- ["../etc/passwd", "a/b", "acme.evil", "has space"]`).
  Meanwhile the twelve replication test files exercised offsets, generations, salts, epochs, torn
  replicas and quorum boundaries — every PROTOCOL decision — and **zero IDENTIFIER decisions**.
  That is why #1 survived a review pass that explicitly hardened this path.

  ## Why the assertions are shaped this way

  The check is `File.ls!` of the follower directory's **parent**, not the absence of one predicted
  path. A traversal test that asserts `refute File.exists?(<the one path I thought of>)` passes for
  every payload that escapes somewhere else, which is the failure mode the test exists to catch.
  """
  use ExUnit.Case, async: false

  alias Fathom.Shard.Replication.Follower
  alias Fathom.Shard.Replication.Promote
  alias Fathom.Shard.Replication.Protocol

  # Every one of these is a real class, not a variation on one:
  #   * traversal, both relative and into fathom's OWN live shard directory;
  #   * a separator that would silently create a subdirectory (or fail mid-write);
  #   * `..` alone, which resolves to the parent itself;
  #   * a dotted id, which is how a Host-routed shard id would be smuggled;
  #   * an embedded NUL, which truncates in the C layer beneath `:file`;
  #   * an id long enough to exceed NAME_MAX, where the failure is a raise rather than a write;
  #   * empty, which joins to the directory itself.
  @hostile [
    "../fathom_shards/victim",
    "../../etc/passwd",
    "a/b",
    "..",
    "acme.evil",
    <<"x", 0, "y">>,
    String.duplicate("a", 5_000),
    ""
  ]

  setup do
    root = Path.join(System.tmp_dir!(), "replid_#{System.unique_integer([:positive])}")
    dir = Path.join(root, "replica")
    File.mkdir_p!(dir)

    # A sibling directory holding something a traversal would want, in the place a relative
    # `../fathom_shards/...` from `dir` actually lands.
    victim_dir = Path.join(root, "fathom_shards")
    File.mkdir_p!(victim_dir)
    victim = Path.join(victim_dir, "victim.db")
    File.write!(victim, "VICTIM-BYTES")

    name = :"replid_f#{System.unique_integer([:positive])}"
    pid = start_supervised!({Follower, name: name, port: 0, dir: dir}, id: name)
    {:ok, port} = Follower.port(pid)

    on_exit(fn -> File.rm_rf(root) end)

    %{root: root, dir: dir, victim: victim, name: name, port: port, pid: pid}
  end

  defp send_frame(port, frame) do
    {:ok, sock} =
      :gen_tcp.connect(~c"127.0.0.1", port, [:binary, packet: 4, active: false], 2_000)

    :ok = :gen_tcp.send(sock, frame)
    reply = :gen_tcp.recv(sock, 0, 500)
    :gen_tcp.close(sock)
    reply
  end

  # Everything under `root`, as a sorted list of relative paths. The whole tree, so an escape into
  # ANY sibling is visible rather than only the one path a `refute File.exists?` would name.
  defp tree(root) do
    root
    |> Path.join("**")
    |> Path.wildcard(match_dot: true)
    |> Enum.map(&Path.relative_to(&1, root))
    |> Enum.sort()
  end

  describe "a hostile shard_id on the wire never becomes a path" do
    test "a push for each hostile id creates nothing and disturbs nothing", ctx do
      %{root: root, port: port, victim: victim} = ctx
      before = tree(root)

      for bad <- @hostile do
        _ =
          send_frame(
            port,
            Protocol.encode_push(%Protocol.Push{
              shard_id: bad,
              epoch: 1,
              wal_gen: 0,
              salt1: 0,
              offset: 0,
              payload: :binary.copy(<<0xAB>>, 64)
            })
          )
      end

      assert tree(root) == before,
             "a shard_id from the network created or removed a file. `Follower` builds paths " <>
               "straight out of wire input, and this is the isolation gate AGENTS.md requires " <>
               "for any change to shard-path construction."

      assert File.read!(victim) == "VICTIM-BYTES"
    end

    test "a seed for each hostile id creates nothing and disturbs nothing", ctx do
      %{root: root, port: port, victim: victim} = ctx
      before = tree(root)

      for bad <- @hostile do
        _ =
          send_frame(
            port,
            Protocol.encode_seed_begin(%Protocol.SeedBegin{
              shard_id: bad,
              epoch: 1,
              wal_gen: 0,
              salt1: 0,
              wal_offset: 0,
              db_size: 4096,
              wal_size: 0
            })
          )
      end

      assert tree(root) == before, "a seed named by a hostile shard_id touched the filesystem"
      assert File.read!(victim) == "VICTIM-BYTES"
    end

    # THE FRAME BOUNDARY IS THE GATE, and this is the assertion that says so.
    #
    # `db_path/2` and friends also `assert_valid_shard_id!`, and that backstop alone is enough to
    # keep a hostile id off the filesystem — so the "creates nothing" assertions above pass even
    # with `decode_validated/1`'s check removed. Measured, not assumed: with it removed the
    # follower ANSWERS each hostile push with a reject frame instead of closing.
    #
    # That difference is the invariant worth pinning. An id that failed validation must never
    # reach a handler at all, because a handler that answers it has already used it as a map key
    # and a log field, and every future handler added below the boundary would have to remember
    # to re-validate.
    test "a hostile id is refused AT THE FRAME BOUNDARY — the connection closes, never answers",
         ctx do
      %{port: port} = ctx

      for bad <- @hostile do
        assert {:error, :closed} =
                 send_frame(
                   port,
                   Protocol.encode_push(%Protocol.Push{
                     shard_id: bad,
                     epoch: 1,
                     wal_gen: 0,
                     salt1: 0,
                     offset: 0,
                     payload: "x"
                   })
                 ),
               "the follower ANSWERED a frame whose shard_id is #{inspect(bad)}. It must be " <>
                 "refused at `decode_validated/1`, before any handler sees the id — a handler " <>
                 "that replies has already used it as a map key and a log field."
      end
    end

    test "no hostile id records replication state either", ctx do
      %{port: port, name: name} = ctx

      for bad <- @hostile do
        _ =
          send_frame(
            port,
            Protocol.encode_push(%Protocol.Push{
              shard_id: bad,
              epoch: 1,
              wal_gen: 0,
              salt1: 0,
              offset: 0,
              payload: "x"
            })
          )

        # A refused frame must leave no trace in the ETS log either: a recorded position for an id
        # that can never have a file is a phantom the promote path would later have to reason about.
        refute Follower.state_of(name, bad)
      end
    end

    test "the listener survives all of it and still serves a legitimate shard", ctx do
      %{port: port, name: name, pid: pid} = ctx

      for bad <- @hostile do
        _ =
          send_frame(
            port,
            Protocol.encode_push(%Protocol.Push{
              shard_id: bad,
              epoch: 1,
              wal_gen: 0,
              salt1: 0,
              offset: 0,
              payload: "x"
            })
          )
      end

      # Failing CLOSED must not mean failing DEAD: a peer that can crash the listener with one
      # malformed id is a denial of service on every shard this node follows.
      assert {:ok, ^port} = Follower.port(pid)

      Follower.seed(name, "goodshard", 1, 0, 0, 0)

      assert {:ok, _} =
               send_frame(
                 port,
                 Protocol.encode_push(%Protocol.Push{
                   shard_id: "goodshard",
                   epoch: 1,
                   wal_gen: 0,
                   salt1: 0,
                   offset: 0,
                   payload: :binary.copy(<<0xCD>>, 32)
                 })
               )
    end
  end

  # CHARACTERIZATION, NOT A GUARANTEE — read this before trusting it (expert review #37 asked for a
  # `fresher?/2` case here; this is the honest version of it).
  #
  # `Promote.fresher?/2` compares `{epoch, wal_gen, offset}` tuples, and a follower's side of that
  # tuple is written from values a PEER SUPPLIED ON THE WIRE. So a peer that can reach the
  # replication port can hand this node a replica that outranks the stored object and have it
  # promoted over real data. That is not a defect in `fresher?/2`: ordering is exactly its job, and
  # it has no way to know which peer said what.
  #
  # The control is authenticating the PEER, which is #3 tier 3 and is PARKED pending a decision on
  # the wire-format change. Until it lands, the honest posture statement is the one AGENTS.md
  # already makes: a reachable replication port is equivalent to write access to every shard on the
  # node, and `REPLICATION_BIND_IP` is a security control.
  #
  # Pinned here so the next reader does not mistake `fresher?/2` for the boundary.
  describe "what fresher?/2 does NOT protect against" do
    # The RANKING FIELD is now `lineage`, not `epoch` (expert review 2026-08-24 #12) — the two are
    # different counters and the comparison used to mix them. The point of this test is unchanged
    # and so is its answer: a peer supplies the lineage on the wire exactly as it supplied the
    # epoch, so `fresher?/2` is no more a boundary than it was.
    test "a wire-supplied lineage outranks a stamped object, by design" do
      stamp = %{epoch: 5, wal_gen: 9, offset: 1_000_000}
      forged = %{lineage: 9_999, epoch: 1, wal_gen: 0, next_offset: 0, torn: false}

      assert Promote.fresher?(forged, stamp),
             "if this ever becomes false, the ordering rule changed — re-read the note above " <>
               "rather than assuming peer authentication landed"
    end

    test "but a TORN replica is refused however far ahead it claims to be" do
      stamp = %{epoch: 5, wal_gen: 9, offset: 1_000_000}
      torn = %{epoch: 9_999, wal_gen: 9_999, next_offset: 9_999_999, torn: true}

      refute Promote.fresher?(torn, stamp)
    end
  end
end
