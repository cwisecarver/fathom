defmodule Fathom.Shard.RecoveryPullFailureTest do
  @moduledoc """
  A failed or refused recovery PULL must not degrade to the stale stored object (expert review
  2026-11-07 #4).

  `Recovery.choose/4` says a peer is provably ahead of the object and the responder floor is met, so
  what that peer holds is quorum-acked history the object lacks. The pull used to answer `:none` on
  every failure — connect refused or closed by the accept cap, the source aborting, a timeout, THIS
  node's own sink refusing the seed on its disk floor — which is the same value as "no peer is
  ahead", so the open was served from the stale object and its first flush re-stamped over the only
  copies of the tail. A failed PROMOTION already refuses for the under-quorum window (Jepsen
  2026-10-24); a failed pull is that same decision one step earlier.

  Pinned here:

    * a failed pull REFUSES the open inside the window (`{:refuse, %UnderQuorum{}}`) and falls open
      to `:none` only after it, and the window really expires even though the floor is met on every
      retry (the per-shard row must not be cleared by the settle that precedes the failing pull);
    * a refusal by this node's own sink is a typed reply the puller sees, and ends the attempt;
    * the next candidate is a peer ranked EXACTLY equal to the winner, never a lower one (a lower
      peer may lack the acked tail the winner holds, and succeeding from it would lose it);
    * ties spread across peers by `phash2({shard_id, node_key})`, deterministically per shard.
  """
  use ExUnit.Case, async: false

  @moduletag :capture_log

  alias Fathom.Shard.Replication.Follower
  alias Fathom.Shard.Replication.Protocol
  alias Fathom.Shard.Replication.Recovery
  alias Fathom.Shard.Replication.Recovery.UnderQuorum
  alias Fathom.Test.Await

  @salt 0xDEADBEEF

  @env [
    :replication_lineage_wire,
    :replication_ordinal_wire,
    :replication_recovery_underquorum_window_ms,
    :replication_disk_free_floor_bytes
  ]

  setup do
    prev = Map.new(@env, &{&1, Application.get_env(:fathom, &1)})

    on_exit(fn ->
      for {k, v} <- prev do
        if is_nil(v),
          do: Application.delete_env(:fathom, k),
          else: Application.put_env(:fathom, k, v)
      end
    end)

    # The cross-fleet path ranks only when lineage and ordinal travel on the wire.
    Application.put_env(:fathom, :replication_lineage_wire, true)
    Application.put_env(:fathom, :replication_ordinal_wire, true)
    Application.put_env(:fathom, :replication_recovery_underquorum_window_ms, 60_000)

    # The under-quorum window table, normally owned by `Fleet`'s supervisor. Without it a refusal
    # fails OPEN (`underquorum_step/1` rescues the missing table), which would hide the behaviour.
    Recovery.init_table()

    %{id: "pullfail_#{System.unique_integer([:positive])}"}
  end

  # -- fixtures --------------------------------------------------------------------------------

  defp at(lineage, gen, off),
    do: %{
      lineage: lineage,
      epoch: 1,
      wal_gen: gen,
      wal_ordinal: gen + 1,
      salt1: 0,
      next_offset: off
    }

  defp stamp(epoch, gen, off),
    do: %{epoch: epoch, wal_gen: gen, wal_ordinal: gen + 1, offset: off}

  defp peer(key, port \\ 1), do: {key, "127.0.0.1", port}

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

  # A replica the way a completed seed plus one push leaves it (see `ReplicationRecoveryTest`).
  defp plant_replica(name, id, lineage, gen, salt, db, wal) do
    File.write!(Follower.db_path(name, id), db)
    File.write!(Follower.wal_path(name, id), wal)
    Follower.seed(name, id, 1, gen, salt, byte_size(wal), lineage)
    :ok = Follower.note_ordinal(name, id, gen + 1)
  end

  # A peer that offers `offer` and then fails the pull `how` (`:reject` or `:close`). Tells the test
  # it was asked, so a result can be tied to the pull really having been attempted against it.
  defp failing_peer(offer, how) do
    test = self()

    start_fake_peer(fn
      sock, {:position_query, asked} ->
        :gen_tcp.send(sock, Protocol.encode_position(asked, offer))

      sock, {:replica_request, asked} ->
        send(test, {:pull_attempted, asked})

        case how do
          :reject -> :gen_tcp.send(sock, Protocol.encode_reject(asked, :internal, 0))
          :close -> :gen_tcp.close(sock)
        end

      _sock, _other ->
        :ok
    end)
  end

  defp start_fake_peer(handler) do
    {:ok, lsock} =
      :gen_tcp.listen(0, [:binary, packet: 4, active: false, reuseaddr: true, backlog: 8])

    {:ok, port} = :inet.port(lsock)
    on_exit(fn -> :gen_tcp.close(lsock) end)
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

  # -- the failure itself ----------------------------------------------------------------------

  describe "a failed pull refuses the open" do
    for how <- [:reject, :close] do
      test "a source that #{how}s the pull (the accept cap's signature is :close)", %{id: id} do
        {survivor, _} = start_follower("pf_surv")
        offer = %{at(9, 5, 99_999) | salt1: @salt}
        port = failing_peer(offer, unquote(how))

        assert {:refuse, %UnderQuorum{shard_id: ^id}} =
                 Recovery.best_replica(id, stamp(9, 5, 0),
                   follower: survivor,
                   peers: [peer("failing", port)]
                 )

        assert_received {:pull_attempted, ^id}, "the pull was never attempted, so nothing failed"
        assert is_nil(Follower.state_of(survivor, id))
      end
    end

    test "this node's own sink refusing on the disk floor is a typed refusal, not silence", %{
      id: id
    } do
      {source, source_port} = start_follower("pf_src")
      {survivor, _} = start_follower("pf_surv")

      db = :crypto.strong_rand_bytes(8_192)
      wal = wal_bytes(5, @salt, :crypto.strong_rand_bytes(4_096))
      plant_replica(source, id, 9, 5, @salt, db, wal)

      # Control: with room the same pull installs, so the refusal below is the floor and nothing
      # else (a pull that fails for any other reason would pass the assertion on its own).
      control_id = id <> "_control"
      plant_replica(source, control_id, 9, 5, @salt, db, wal)

      assert {:ok, _} =
               Recovery.best_replica(control_id, stamp(9, 5, 0),
                 follower: survivor,
                 peers: [peer("src", source_port)]
               )

      # No real volume clears this floor, whatever the cached reading says.
      Application.put_env(:fathom, :replication_disk_free_floor_bytes, Bitwise.bsl(1, 62))

      begin = %Protocol.SeedBegin{
        shard_id: id,
        epoch: 1,
        wal_gen: 5,
        salt1: @salt,
        wal_offset: byte_size(wal),
        db_size: byte_size(db),
        wal_size: byte_size(wal),
        lineage: 9
      }

      assert Follower.begin_seed(survivor, %{}, begin) == %{},
             "a primary's refused seed must stay silent (its RPO simply stays at the object)"

      assert %{^id => %{failed: true, refusal: :disk_floor}} =
               Follower.begin_seed(survivor, %{}, begin, pull: true)

      assert {:refuse, %UnderQuorum{shard_id: ^id}} =
               Recovery.best_replica(id, stamp(9, 5, 0),
                 follower: survivor,
                 peers: [peer("src", source_port)]
               )

      assert is_nil(Follower.state_of(survivor, id))
      refute File.exists?(Follower.db_path(survivor, id))
    end

    test "the window is bounded: it expires even though the floor is met on every retry", %{
      id: id
    } do
      # Two peers at Q = 1 need N - Q + 1 = 2 definite answers: this node's own "nothing" plus the
      # failing peer's offer, so every retry reaches `settle/7`, which used to CLEAR the per-shard
      # window row before the pull failed. Each retry then started a fresh first refusal and the
      # refusal never ended ("at most one window" became "indefinitely").
      {survivor, _} = start_follower("pf_surv")
      Application.put_env(:fathom, :replication_recovery_underquorum_window_ms, 200)

      offer = %{at(9, 5, 99_999) | salt1: @salt}
      port = failing_peer(offer, :reject)
      # The second peer is dead (an instant refused connect), so the floor is met only WITH the
      # failing peer's answer — an answering empty peer could meet it first and the early exit
      # would never consult the failing one.
      peers = [peer("failing", port), peer("dead", 1)]
      opts = [follower: survivor, peers: peers, quorum: 1]

      assert Recovery.answers_needed(peers, stamp(9, 5, 0), quorum: 1) == 2,
             "fixture: the floor must exceed one answer or settle's clear is never reached"

      assert {:refuse, %UnderQuorum{}} = Recovery.best_replica(id, stamp(9, 5, 0), opts)

      Await.until(
        fn -> Recovery.best_replica(id, stamp(9, 5, 0), opts) == :none end,
        "the refusal never expired: a failing pull re-armed the window on every retry"
      )

      # Past the window it is best-effort: the stored object, never a stuck shard.
      assert :none = Recovery.best_replica(id, stamp(9, 5, 0), opts)
    end
  end

  # -- the next candidate ----------------------------------------------------------------------

  describe "the fallback candidate" do
    test "a failed winner falls to a peer ranked EXACTLY equal, which installs", %{id: id} do
      {source, source_port} = start_follower("pf_src")
      {survivor, _} = start_follower("pf_surv")

      db = :crypto.strong_rand_bytes(8_192)
      wal = wal_bytes(5, @salt, :crypto.strong_rand_bytes(4_096))
      plant_replica(source, id, 9, 5, @salt, db, wal)

      offer = %{at(9, 5, byte_size(wal)) | salt1: @salt}
      fake_port = failing_peer(offer, :reject)

      # Which of two EQUAL peers wins is the per-shard tie-break, so ask `choose/4` and make the
      # winner the failing one.
      {:pull, {winner, _, _}, _} =
        Recovery.choose(nil, stamp(9, 5, 0), [{peer("pf_a"), offer}, {peer("pf_b"), offer}], id)

      loser = Enum.find(["pf_a", "pf_b"], &(&1 != winner))

      assert {:ok, installed} =
               Recovery.best_replica(id, stamp(9, 5, 0),
                 follower: survivor,
                 peers: [
                   {winner, "127.0.0.1", fake_port},
                   {loser, "127.0.0.1", source_port}
                 ]
               )

      assert_received {:pull_attempted, ^id}, "the failing winner was never tried first"
      assert File.read!(Follower.db_path(survivor, id)) == db
      assert File.read!(Follower.wal_path(survivor, id)) == wal
      assert installed == Follower.state_of(survivor, id)
    end

    test "a LOWER-ranked peer is never the fallback: that would serve a tail-less copy", %{id: id} do
      {source, source_port} = start_follower("pf_src")
      {survivor, _} = start_follower("pf_surv")

      # The source holds a replica that is ahead of the object but BEHIND the failing winner.
      db = :crypto.strong_rand_bytes(8_192)
      wal = wal_bytes(5, @salt, :crypto.strong_rand_bytes(1_024))
      plant_replica(source, id, 9, 5, @salt, db, wal)

      winner_offer = %{at(9, 5, byte_size(wal) + 50_000) | salt1: @salt}
      fake_port = failing_peer(winner_offer, :reject)

      assert {:refuse, %UnderQuorum{shard_id: ^id}} =
               Recovery.best_replica(id, stamp(9, 5, 0),
                 follower: survivor,
                 peers: [peer("winner", fake_port), peer("lower", source_port)],
                 quorum: 2
               )

      assert_received {:pull_attempted, ^id}
      assert is_nil(Follower.state_of(survivor, id)), "a lower-ranked copy was installed"
    end

    test "pull_alternates/5 lists only offers ranked exactly equal to the winner" do
      equal = at(9, 3, 8_240)

      offers = [
        {peer("n2"), equal},
        {peer("n3"), equal},
        {peer("n4"), at(9, 3, 8_000)},
        {peer("n5"), Map.put(equal, :torn, true)},
        {peer("n6"), nil}
      ]

      {:pull, winner, pos} = Recovery.choose(nil, stamp(9, 3, 4_120), offers, "alt_shard")
      alternates = Recovery.pull_alternates(stamp(9, 3, 4_120), offers, "alt_shard", winner, pos)

      assert [{other, ^equal}] = alternates
      assert other != winner
      assert elem(other, 0) in ["n2", "n3"] and elem(winner, 0) in ["n2", "n3"]
    end
  end

  # -- spreading ties --------------------------------------------------------------------------

  describe "tied peers spread across shards" do
    test "the winner varies per shard and is the same for the same shard in any order" do
      offers = [{peer("n2"), at(9, 3, 8_240)}, {peer("n3"), at(9, 3, 8_240)}]

      winners =
        for i <- 1..40 do
          shard = "spread_#{i}"
          {:pull, {key, _, _}, _} = Recovery.choose(nil, stamp(9, 3, 0), offers, shard)

          assert {:pull, {^key, _, _}, _} =
                   Recovery.choose(nil, stamp(9, 3, 0), Enum.reverse(offers), shard),
                 "two nodes recovering the same shard must agree"

          key
        end

      assert Enum.uniq(winners) |> Enum.sort() == ["n2", "n3"],
             "every tied pull went to one peer: #{inspect(Enum.frequencies(winners))}"
    end

    test "a strictly higher rank still wins whatever the spread" do
      offers = [{peer("n2"), at(9, 3, 8_240)}, {peer("n3"), at(9, 3, 8_000)}]

      for i <- 1..20 do
        assert {:pull, {"n2", _, _}, _} =
                 Recovery.choose(nil, stamp(9, 3, 4_120), offers, "rank_#{i}")
      end
    end
  end
end
