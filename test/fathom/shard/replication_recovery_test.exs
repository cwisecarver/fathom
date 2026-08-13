defmodule Fathom.Shard.ReplicationRecoveryTest do
  @moduledoc """
  Survivor selection — Phase 2 A2. See `docs/a2-quorum-replication.md`.

  The gap this closes is not a bug in replication; replication worked. It is that **nothing
  connected "which node holds a current replica" to "which node the LB failed over to."** The LB
  picks a survivor by consistent hash on the Host subdomain, so the node that takes a shard over
  routinely holds no replica while its peers hold a current one — and it cold-opened from S3 and
  lost the tail. Measured on the rig 2026-08-11: an acked, quorum-replicated write lost while three
  other nodes held it.

  Two layers here, matching the two ways this can go wrong.

  **`choose/3` is pure**, like `Promote.fresher?/2` and `FollowerLog.decide/2`, because it picks one
  lineage of a tenant's database and discards the others. Every test below that names a *safety*
  property lives here, and every one of them was checked against a broken implementation (drop the
  `fresher?` filter, or use `>=` where the code uses `>`) and observed to fail.

  **The pull is driven over a real socket**, against a real `Follower` on the other end, because the
  property that matters there — that the bytes which arrive are installed as a replica
  indistinguishable from a pushed one — is a claim about the wire and the filesystem, not about a
  decision.

  The cross-shard test is the shard-isolation gate (AGENTS.md): this is a path where bytes arriving
  over an **unauthenticated** socket get written under a shard id, so "our own peers would not do
  that" is not a property we are entitled to assume.
  """
  use ExUnit.Case, async: false

  alias Fathom.Shard.Replication.Follower
  alias Fathom.Shard.Replication.Protocol
  alias Fathom.Shard.Replication.Protocol.SeedBegin
  alias Fathom.Shard.Replication.Recovery

  # A position as a peer reports it (the `FollowerLog.t()` shape), and as the stored object stamps
  # it (`Storage.position/0`) — deliberately different key names, which is exactly the mismatch
  # `Promote.fresher?/2` exists to bridge.
  defp at(epoch, gen, off), do: %{epoch: epoch, wal_gen: gen, salt1: 0, next_offset: off}
  defp stamp(epoch, gen, off), do: %{epoch: epoch, wal_gen: gen, offset: off}

  defp peer(key, port \\ 1), do: {key, "127.0.0.1", port}

  describe "choose/3 — which lineage gets served" do
    test "a peer strictly ahead of the stored object is pulled" do
      offers = [{peer("n2"), at(9, 3, 8_240)}]

      assert {:pull, {"n2", _, _}, %{next_offset: 8_240}} =
               Recovery.choose(nil, stamp(9, 3, 4_120), offers)
    end

    test "the FURTHEST peer wins, not the first to answer" do
      offers = [
        {peer("n2"), at(9, 3, 4_200)},
        {peer("n3"), at(9, 3, 9_000)},
        {peer("n4"), at(9, 3, 6_000)}
      ]

      assert {:pull, {"n3", _, _}, %{next_offset: 9_000}} =
               Recovery.choose(nil, stamp(9, 3, 4_120), offers)
    end

    test "a later epoch beats a longer WAL" do
      # The ordering is lexicographic on {epoch, wal_gen, offset} and that is not cosmetic: after a
      # takeover the new primary's WAL restarts, so a node following it sits at a SMALL offset while
      # holding strictly newer history than a node still holding the deposed primary's long WAL.
      offers = [
        {peer("stale"), at(9, 3, 900_000)},
        {peer("current"), at(10, 0, 120)}
      ]

      assert {:pull, {"current", _, _}, _} = Recovery.choose(nil, stamp(9, 3, 4_120), offers)
    end

    test "NOTHING is pulled when no peer is ahead of the stored object" do
      # The degrade-safely property, and the one that makes this feature never worse than leaving it
      # off: the object is the answer whenever nobody can prove they beat it.
      offers = [{peer("n2"), at(9, 3, 4_120)}, {peer("n3"), at(9, 2, 999_999)}]
      assert :none = Recovery.choose(nil, stamp(9, 3, 4_120), offers)
    end

    test "an UNSTAMPED object is never overridden, however far ahead a peer claims to be" do
      # An object written before position stamping existed, or by a node that has not been upgraded.
      # "Unknown" is not "empty" — treating it as position zero would let any replica win against an
      # object nobody can order, which is the one comparison guaranteed to be uninformed.
      offers = [{peer("n2"), at(99, 99, 99_999_999)}]
      assert :none = Recovery.choose(nil, nil, offers)
      assert :none = Recovery.choose(at(99, 99, 99_999_999), nil, offers)
    end

    test "peers holding nothing are skipped rather than counted as being at the beginning" do
      offers = [{peer("n2"), nil}, {peer("n3"), nil}]
      assert :none = Recovery.choose(nil, stamp(1, 0, 0), offers)

      # And a peer that genuinely IS at the beginning still cannot beat an object there.
      assert :none = Recovery.choose(nil, stamp(0, 0, 0), [{peer("n2"), at(0, 0, 0)}])
    end

    test "an unreachable fleet is silence, not agreement" do
      # Every peer failing to answer must read as "no offer", never as "no peer is ahead, so promote
      # whatever is local". Losing contact with the fleet is MOST likely during exactly the failover
      # this runs in.
      assert :none = Recovery.choose(nil, stamp(9, 3, 4_120), [])
    end

    test "the local replica wins ties, so an equal peer costs no transfer" do
      offers = [{peer("n2"), at(9, 3, 8_240)}]
      assert :local = Recovery.choose(at(9, 3, 8_240), stamp(9, 3, 4_120), offers)
      assert :local = Recovery.choose(at(9, 3, 9_000), stamp(9, 3, 4_120), offers)

      assert {:pull, {"n2", _, _}, _} =
               Recovery.choose(at(9, 3, 8_000), stamp(9, 3, 4_120), offers)
    end

    test "a local replica that does NOT beat the object loses to a peer that does" do
      # The literal failover shape: the survivor holds a stale replica (or a stale-but-present one
      # from before the shard moved) while a peer holds the current lineage.
      offers = [{peer("n2"), at(9, 3, 8_240)}]
      assert {:pull, {"n2", _, _}, _} = Recovery.choose(at(9, 3, 100), stamp(9, 3, 4_120), offers)
    end

    # The 2026-08-12 rig bug, at the decision layer. A torn replica's offset is a true statement
    # about a WAL and a false one about a copy of the shard: its `.db` is a generation behind, so
    # the pair does not compose into a database. It read as strictly ahead of the object and was
    # promoted, and the tenant got an EMPTY database over a working one.
    # See docs/reviews/a2-checkpoint-torn-replica-2026-08-12.md.
    test "a TORN replica is never chosen — not locally, not from a peer" do
      torn = Map.put(at(99, 99, 99_999_999), :torn, true)

      # however far ahead it claims to be
      assert :none = Recovery.choose(torn, stamp(9, 3, 4_120), [])
      assert :none = Recovery.choose(nil, stamp(9, 3, 4_120), [{peer("n2"), torn}])
      assert :none = Recovery.choose(torn, stamp(9, 3, 4_120), [{peer("n2"), torn}])

      # and a whole peer holding one must not beat a WHOLE local replica that is behind it
      assert :local =
               Recovery.choose(at(9, 3, 8_000), stamp(9, 3, 4_120), [{peer("n2"), torn}])
    end

    test "a whole replica is still chosen alongside a torn one" do
      # Keeps the flag from being a way to never recover: the torn peer is skipped, the whole one
      # still wins.
      torn = Map.put(at(99, 99, 99_999_999), :torn, true)
      whole = at(9, 3, 8_240)

      assert {:pull, {"n3", _, _}, %{next_offset: 8_240}} =
               Recovery.choose(nil, stamp(9, 3, 4_120), [{peer("n2"), torn}, {peer("n3"), whole}])
    end

    test "ties between peers break deterministically, so two survivors choose alike" do
      offers = [{peer("n2"), at(9, 3, 8_240)}, {peer("n3"), at(9, 3, 8_240)}]
      assert {:pull, {"n3", _, _}, _} = Recovery.choose(nil, stamp(9, 3, 0), offers)
      assert {:pull, {"n3", _, _}, _} = Recovery.choose(nil, stamp(9, 3, 0), Enum.reverse(offers))
    end
  end

  # ------------------------------------------------------------------------------------------
  # recheck/3 — the decision is made BEFORE the transfer and acted on AFTER it
  # ------------------------------------------------------------------------------------------

  describe "recheck/3 — is the choice still true once the pull is done" do
    defp head(etag, position), do: %{etag: etag, position: position}

    test "an unchanged object leaves the promotion standing" do
      before = head("v1", stamp(9, 3, 4_120))
      assert :ok = Recovery.recheck(before, before, at(9, 3, 8_240))
    end

    test "an object that moved is refused, and says what it was and is" do
      # The ordinary race: another node flushed while we were querying peers and transferring a
      # database. The etag we would fence the publish with is stale, so the publish could not land
      # anyway — the point of catching it here is to not have done the snapshot, and to not log
      # "the stored object was behind it" about an object that has since moved ahead.
      before = head("v1", stamp(9, 3, 4_120))
      now = head("v2", stamp(9, 3, 9_000))

      assert {:error, {:object_moved, "v1", "v2"}} =
               Recovery.recheck(before, now, at(9, 3, 8_240))
    end

    test "an object DELETED mid-recovery is refused rather than read as unstamped" do
      before = head("v1", stamp(9, 3, 4_120))
      assert {:error, {:object_moved, "v1", nil}} = Recovery.recheck(before, nil, at(9, 3, 8_240))
    end

    # THE CASE ETAGS ALONE CANNOT SEE, and the reason `recheck/3` compares the position rather than
    # trusting etag equality to imply it. On S3 the etag hashes the BODY while the position is user
    # metadata, so a re-flush of byte-identical bytes carrying an advanced position keeps the etag.
    # An etag-only check would call this "unchanged" and promote a replica over a newer claim.
    test "a same-etag object whose STAMP advanced past the replica is refused" do
      before = head("v1", stamp(9, 3, 4_120))
      now = head("v1", stamp(9, 3, 9_999))

      assert {:error, {:object_advanced, %{offset: 9_999}}} =
               Recovery.recheck(before, now, at(9, 3, 8_240))
    end

    test "a stamp that advanced but is STILL behind the replica keeps the promotion" do
      # The other half of the above: refusing on any movement at all would abandon transfers that
      # are still correct wins, which is the same over-caution as never recovering.
      before = head("v1", stamp(9, 3, 4_120))
      now = head("v1", stamp(9, 3, 6_000))

      assert :ok = Recovery.recheck(before, now, at(9, 3, 8_240))
    end

    test "an object that became UNSTAMPED is refused, because unknown is never overridable" do
      # Same rule `choose/3` follows: a stamp we cannot read is not a stamp at position zero.
      before = head("v1", stamp(9, 3, 4_120))

      assert {:error, {:object_advanced, nil}} =
               Recovery.recheck(before, head("v1", nil), at(9, 3, 8_240))
    end
  end

  # ------------------------------------------------------------------------------------------
  # the wire
  # ------------------------------------------------------------------------------------------

  # A real WAL header, because `Follower`'s pull path reads generation, salt and size from the FILE
  # rather than from ETS — the same rule `recover/3` follows. A fixture with random bytes in the
  # first 32 would make `Wal.read/1` return `:not_a_wal` and the source would offer nothing, so the
  # test would pass while proving the wrong thing.
  defp wal_bytes(ckpt_seq, salt1, payload) do
    <<0x377F0682::32, 3_007_000::32, 4096::32, ckpt_seq::32, salt1::32, 0::32, 0::64>> <> payload
  end

  defp start_follower(prefix) do
    root = Path.join(System.tmp_dir!(), "#{prefix}_#{System.unique_integer([:positive])}")
    File.mkdir_p!(root)
    on_exit(fn -> File.rm_rf(root) end)

    name = :"#{prefix}_#{System.unique_integer([:positive])}"
    pid = start_supervised!({Follower, name: name, port: 0, dir: root}, id: name)
    {:ok, port} = Follower.port(pid)
    {name, port}
  end

  # Give `name` a replica of `id` the way a completed seed would leave it: files on disk and a
  # matching ETS row.
  defp plant_replica(name, id, epoch, gen, salt, db, wal) do
    File.write!(Follower.db_path(name, id), db)
    File.write!(Follower.wal_path(name, id), wal)
    Follower.seed(name, id, epoch, gen, salt, byte_size(wal))
  end

  setup do
    id = "recov_#{System.unique_integer([:positive])}"
    %{id: id}
  end

  test "a survivor holding NOTHING pulls the freshest peer's replica and can promote it", %{
    id: id
  } do
    {source, source_port} = start_follower("recov_src")
    {survivor, _} = start_follower("recov_surv")

    db = :crypto.strong_rand_bytes(12_288)
    wal = wal_bytes(5, 0xDEADBEEF, :crypto.strong_rand_bytes(8_192))
    plant_replica(source, id, 9, 5, 0xDEADBEEF, db, wal)

    # The survivor holds no replica at all — the case that lost the write on the rig.
    assert is_nil(Follower.state_of(survivor, id))

    assert {:ok, installed} =
             Recovery.best_replica(id, %{epoch: 9, wal_gen: 5, offset: 0},
               follower: survivor,
               peers: [{"src", "127.0.0.1", source_port}]
             )

    # Byte-identical, both halves. A `.db` without its matching `-wal` is the corrupt-looking pair
    # every install path in this subsystem is ordered to avoid.
    assert File.read!(Follower.db_path(survivor, id)) == db
    assert File.read!(Follower.wal_path(survivor, id)) == wal

    # And the replication state a PUSHED seed would have left, so `Promote` cannot tell the
    # difference and needs no new case.
    assert installed == Follower.state_of(survivor, id)

    assert %{epoch: 9, wal_gen: 5, salt1: 0xDEADBEEF, next_offset: byte_size(wal)} ==
             Map.take(installed, [:epoch, :wal_gen, :salt1, :next_offset])
  end

  test "a survivor already holding the freshest copy asks nobody", %{id: id} do
    {survivor, _} = start_follower("recov_surv")

    db = :crypto.strong_rand_bytes(4_096)
    wal = wal_bytes(5, 7, :crypto.strong_rand_bytes(2_048))
    plant_replica(survivor, id, 9, 5, 7, db, wal)

    # Every peer address is a port nothing is listening on. If the local short-circuit were removed
    # this would still return the local replica, but only after paying connect timeouts — so the
    # assertion is on the CLOCK as much as the value.
    started = System.monotonic_time(:millisecond)

    assert {:ok, %{next_offset: _}} =
             Recovery.best_replica(id, %{epoch: 9, wal_gen: 5, offset: 0},
               follower: survivor,
               peers: [{"gone", "127.0.0.1", 1}, {"also_gone", "127.0.0.1", 2}]
             )

    assert System.monotonic_time(:millisecond) - started < 500
  end

  test "a fleet that cannot be reached falls back to the stored object", %{id: id} do
    {survivor, _} = start_follower("recov_surv")

    assert :none =
             Recovery.best_replica(id, %{epoch: 9, wal_gen: 5, offset: 0},
               follower: survivor,
               peers: [{"gone", "127.0.0.1", 1}],
               query_timeout_ms: 200
             )

    assert is_nil(Follower.state_of(survivor, id))
  end

  test "a peer holding no replica answers so, and nothing is pulled", %{id: id} do
    {source, source_port} = start_follower("recov_src")
    {survivor, _} = start_follower("recov_surv")

    # The source is a healthy, reachable follower that simply does not follow this shard.
    assert is_nil(Follower.state_of(source, id))

    assert :none =
             Recovery.best_replica(id, %{epoch: 9, wal_gen: 5, offset: 0},
               follower: survivor,
               peers: [{"src", "127.0.0.1", source_port}]
             )

    assert is_nil(Follower.state_of(survivor, id))
  end

  test "a peer BEHIND the stored object is not pulled from", %{id: id} do
    {source, source_port} = start_follower("recov_src")
    {survivor, _} = start_follower("recov_surv")

    wal = wal_bytes(5, 7, :crypto.strong_rand_bytes(1_024))
    plant_replica(source, id, 9, 5, 7, :crypto.strong_rand_bytes(4_096), wal)

    # The object was flushed at a later generation, so the replica is an older lineage. Pulling it
    # would DELETE acknowledged writes — the failure this whole comparison exists to prevent, and
    # the one direction where being wrong is unrecoverable.
    assert :none =
             Recovery.best_replica(id, %{epoch: 9, wal_gen: 6, offset: 0},
               follower: survivor,
               peers: [{"src", "127.0.0.1", source_port}]
             )

    assert is_nil(Follower.state_of(survivor, id))
  end

  # ------------------------------------------------------------------------------------------
  # shard isolation on the pull path
  # ------------------------------------------------------------------------------------------

  defp start_fake_peer(handler) do
    {:ok, lsock} =
      :gen_tcp.listen(0, [:binary, packet: 4, active: false, reuseaddr: true, backlog: 8])

    {:ok, port} = :inet.port(lsock)
    on_exit(fn -> :gen_tcp.close(lsock) end)

    # `spawn`, not `spawn_link`: this process dies with the listening socket at the end of the
    # test, and linking would turn that ordinary shutdown into a test failure.
    spawn(fn -> fake_accept(lsock, handler) end)
    port
  end

  defp fake_accept(lsock, handler) do
    case :gen_tcp.accept(lsock) do
      {:ok, sock} ->
        spawn(fn -> fake_serve(sock, handler) end)
        fake_accept(lsock, handler)

      {:error, _} ->
        :ok
    end
  end

  defp fake_serve(sock, handler) do
    with {:ok, bytes} <- :gen_tcp.recv(sock, 0, 5_000),
         {:ok, msg} <- Protocol.decode(bytes) do
      handler.(sock, msg)
      fake_serve(sock, handler)
    else
      _ -> :gen_tcp.close(sock)
    end
  end

  test "a peer answering with ANOTHER tenant's bytes installs nothing", %{id: id} do
    {survivor, _} = start_follower("recov_surv")
    victim = "#{id}_victim"
    db = :crypto.strong_rand_bytes(2_048)

    port =
      start_fake_peer(fn
        sock, {:position_query, asked} ->
          # Answers honestly about the shard we asked for, so it wins the choice and we proceed to
          # the pull. The substitution happens only once bytes start moving.
          :gen_tcp.send(
            sock,
            Protocol.encode_position(asked, %{
              epoch: 9,
              wal_gen: 5,
              salt1: 7,
              next_offset: 99_999
            })
          )

        sock, {:replica_request, _asked} ->
          :gen_tcp.send(
            sock,
            Protocol.encode_seed_begin(%SeedBegin{
              shard_id: victim,
              epoch: 9,
              wal_gen: 5,
              salt1: 7,
              wal_offset: 0,
              db_size: byte_size(db),
              wal_size: 0
            })
          )

          :gen_tcp.send(sock, Protocol.encode_seed_chunk(victim, :db, 0, db))
          :gen_tcp.send(sock, Protocol.encode_seed_end(victim))

        _sock, _other ->
          :ok
      end)

    assert :none =
             Recovery.best_replica(id, %{epoch: 9, wal_gen: 5, offset: 0},
               follower: survivor,
               peers: [{"liar", "127.0.0.1", port}]
             )

    # Neither the shard we asked for nor the one the bytes named. A frame naming a different shard
    # is refused outright rather than filed under its own id, because accepting it would let any
    # reachable host write into any tenant this node follows.
    assert is_nil(Follower.state_of(survivor, id))
    assert is_nil(Follower.state_of(survivor, victim))
    refute File.exists?(Follower.db_path(survivor, id))
    refute File.exists?(Follower.db_path(survivor, victim))
  end

  test "a truncated stream installs nothing and leaves no temp files", %{id: id} do
    {survivor, _} = start_follower("recov_surv")
    db = :crypto.strong_rand_bytes(4_096)

    port =
      start_fake_peer(fn
        sock, {:position_query, asked} ->
          :gen_tcp.send(
            sock,
            Protocol.encode_position(asked, %{
              epoch: 9,
              wal_gen: 5,
              salt1: 7,
              next_offset: 99_999
            })
          )

        sock, {:replica_request, asked} ->
          # Declares 4096 bytes of database and sends 1024, then commits. A short install would be
          # a database missing pages — it opens cleanly and reads wrong.
          :gen_tcp.send(
            sock,
            Protocol.encode_seed_begin(%SeedBegin{
              shard_id: asked,
              epoch: 9,
              wal_gen: 5,
              salt1: 7,
              wal_offset: 0,
              db_size: byte_size(db),
              wal_size: 0
            })
          )

          :gen_tcp.send(
            sock,
            Protocol.encode_seed_chunk(asked, :db, 0, binary_part(db, 0, 1_024))
          )

          :gen_tcp.send(sock, Protocol.encode_seed_end(asked))

        _sock, _other ->
          :ok
      end)

    assert :none =
             Recovery.best_replica(id, %{epoch: 9, wal_gen: 5, offset: 0},
               follower: survivor,
               peers: [{"short", "127.0.0.1", port}]
             )

    assert is_nil(Follower.state_of(survivor, id))

    assert Follower.dir(survivor) |> File.ls!() |> Enum.filter(&String.ends_with?(&1, ".seeding")) ==
             []
  end

  test "a source that aborts mid-stream leaves the survivor on the stored object", %{id: id} do
    {survivor, _} = start_follower("recov_surv")

    port =
      start_fake_peer(fn
        sock, {:position_query, asked} ->
          :gen_tcp.send(
            sock,
            Protocol.encode_position(asked, %{
              epoch: 9,
              wal_gen: 5,
              salt1: 7,
              next_offset: 99_999
            })
          )

        sock, {:replica_request, asked} ->
          :gen_tcp.send(
            sock,
            Protocol.encode_seed_begin(%SeedBegin{
              shard_id: asked,
              epoch: 9,
              wal_gen: 5,
              salt1: 7,
              wal_offset: 0,
              db_size: 4_096,
              wal_size: 0
            })
          )

          # The source found its `.db` and `-wal` no longer belong together — a checkpoint or a
          # re-seed landed underneath it while it was reading.
          :gen_tcp.send(sock, Protocol.encode_seed_abort(asked))

        _sock, _other ->
          :ok
      end)

    assert :none =
             Recovery.best_replica(id, %{epoch: 9, wal_gen: 5, offset: 0},
               follower: survivor,
               peers: [{"aborter", "127.0.0.1", port}]
             )

    assert is_nil(Follower.state_of(survivor, id))
  end

  test "a real follower serves a position query without touching the database", %{id: id} do
    {source, port} = start_follower("recov_src")
    wal = wal_bytes(5, 7, :crypto.strong_rand_bytes(512))
    plant_replica(source, id, 9, 5, 7, :crypto.strong_rand_bytes(1_024), wal)

    {:ok, sock} =
      :gen_tcp.connect(~c"127.0.0.1", port, [:binary, packet: 4, active: false, nodelay: true])

    on_exit(fn -> :gen_tcp.close(sock) end)

    :ok = :gen_tcp.send(sock, Protocol.encode_position_query(id))
    {:ok, bytes} = :gen_tcp.recv(sock, 0, 5_000)

    assert {:ok, {:position, ^id, %{epoch: 9, wal_gen: 5, salt1: 7}}} = Protocol.decode(bytes)

    # And an unknown shard answers "nothing", not "the beginning".
    :ok = :gen_tcp.send(sock, Protocol.encode_position_query("never_seen"))
    {:ok, bytes2} = :gen_tcp.recv(sock, 0, 5_000)
    assert {:ok, {:position, "never_seen", nil}} = Protocol.decode(bytes2)
  end
end
