defmodule Fathom.Shard.RecoveryEarlyExitTest do
  @moduledoc """
  Peer recovery's ask stops waiting once the responder floor is decided (perf review 2026-10-14
  #8(a)), and an unrankable stamp neither dials a peer nor writes the recovery marker (#8(b)).

  ## What changed and what must NOT

  `Recovery.ask/5` used to consume `Task.async_stream` until the LAST peer answered or timed out:
  one slow-but-live peer (2 s connect + 2 s query) stalled every replicating cold open although the
  floor's verdict (`answered/2 >= needed`) was long fixed. It now halts the stream at the first
  moment the floor's own count reaches `needed`. The recovery POLICY is the user's and is untouched:
  `N - Q + 1` definite answers, a 60 s 503 window below that, best-effort after. So the guards here
  are the ways an early return could change a floor decision:

    * below the floor it still waits the whole timeout and refuses;
    * a PREVIOUS OWNER's answer does not count toward the floor, so it does not end the wait;
    * a peer holding a fresher replica that is among the answers that met the floor is still chosen;
    * `needed <= 1` (met by this node alone) keeps the full wait, because exiting would drop offers.

  The slow peer is a listener that accepts the connection and never replies, so the query runs to
  its full `query_timeout_ms` (1.5 s here, the number the review quoted).
  """
  use ExUnit.Case, async: false

  @moduletag :capture_log

  alias Fathom.Shard.PromoteOnOpen
  alias Fathom.Shard.Replication.{Fleet, Follower, Recovery}
  alias Fathom.Shard.Replication.Recovery.UnderQuorum

  @slow_ms 1_500

  setup do
    id = "early_#{System.unique_integer([:positive])}"
    prev_wire = Application.get_env(:fathom, :replication_lineage_wire)
    prev_ord = Application.get_env(:fathom, :replication_ordinal_wire)
    Application.put_env(:fathom, :replication_lineage_wire, true)
    Application.put_env(:fathom, :replication_ordinal_wire, true)
    Recovery.init_table()

    on_exit(fn ->
      for {k, v} <- [replication_lineage_wire: prev_wire, replication_ordinal_wire: prev_ord] do
        if is_nil(v),
          do: Application.delete_env(:fathom, k),
          else: Application.put_env(:fathom, k, v)
      end
    end)

    %{id: id, survivor: start_follower("early_surv")}
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

  defp live(prefix) do
    {name, port} = start_follower(prefix)
    {name, {"#{name}", "127.0.0.1", port}}
  end

  # Accepts and never answers; reports every accepted connection to the test.
  defp slow_peer(key) do
    test = self()
    {:ok, listen} = :gen_tcp.listen(0, [:binary, active: false, reuseaddr: true])
    {:ok, port} = :inet.port(listen)

    pid =
      spawn_link(fn ->
        accept_loop = fn loop, socks ->
          case :gen_tcp.accept(listen, 5_000) do
            {:ok, s} ->
              send(test, {:slow_peer_accepted, key})
              loop.(loop, [s | socks])

            _ ->
              loop.(loop, socks)
          end
        end

        accept_loop.(accept_loop, [])
      end)

    on_exit(fn ->
      Process.unlink(pid)
      Process.exit(pid, :kill)
      :gen_tcp.close(listen)
    end)

    {key, "127.0.0.1", port}
  end

  defp stamp(epoch, gen, off),
    do: %{epoch: epoch, wal_gen: gen, wal_ordinal: gen + 1, offset: off}

  defp plant_replica(name, id, lineage, gen, salt) do
    wal = <<0x377F0682::32, 3_007_000::32, 4096::32, gen::32, salt::32, 0::32, 0::64>> <> "xxxx"
    File.write!(Follower.db_path(name, id), :crypto.strong_rand_bytes(4096))
    File.write!(Follower.wal_path(name, id), wal)
    Follower.seed(name, id, 1, gen, salt, byte_size(wal), lineage)
    :ok = Follower.note_ordinal(name, id, gen + 1)
  end

  defp timed(fun) do
    t0 = System.monotonic_time(:millisecond)
    result = fun.()
    {System.monotonic_time(:millisecond) - t0, result}
  end

  test "one slow live peer no longer stalls a decided floor", %{id: id, survivor: {surv, _}} do
    {_, peer_a} = live("early_a")

    # N = 3, Q = 2 -> 2 definite answers needed: this node + one peer. The slow peer is never needed.
    peers = [peer_a, slow_peer("slow1"), slow_peer("slow2")]

    {ms, result} =
      timed(fn ->
        Recovery.best_replica(id, stamp(9, 5, 0),
          follower: surv,
          peers: peers,
          quorum: 2,
          query_timeout_ms: @slow_ms
        )
      end)

    # The peer holds nothing: a definite "nothing" answer, so the decision is :none.
    assert result == :none
    # Before the fix this waited the slow peers' full query timeout (~1.5 s); the answer was in
    # milliseconds.
    assert ms < 400, "ask waited #{ms} ms for peers the floor did not need (was ~#{@slow_ms})"
  end

  test "an offer inside the answers that met the floor is still chosen", %{
    id: id,
    survivor: {surv, _}
  } do
    {src, src_peer} = live("early_src")
    plant_replica(src, id, 9, 5, 0xDEADBEEF)
    peers = [src_peer, slow_peer("slow1"), slow_peer("slow2")]

    {ms, result} =
      timed(fn ->
        Recovery.best_replica(id, stamp(9, 3, 0),
          follower: surv,
          peers: peers,
          quorum: 2,
          query_timeout_ms: @slow_ms
        )
      end)

    assert {:ok, %{lineage: 9}} = result, "the fresher replica that answered must be promoted"
    assert ms < 1_000
  end

  test "below the floor it still waits the whole timeout, then refuses", %{
    id: id,
    survivor: {surv, _}
  } do
    {_, peer_a} = live("early_a")
    # N = 4, Q = 2 -> 3 needed. This node + one live peer = 2 < 3: the slow peers can still change
    # the verdict, so the early exit must NOT fire.
    peers = [peer_a, slow_peer("s1"), slow_peer("s2"), slow_peer("s3")]

    {ms, result} =
      timed(fn ->
        Recovery.best_replica(id, stamp(9, 5, 0),
          follower: surv,
          peers: peers,
          quorum: 2,
          query_timeout_ms: 400
        )
      end)

    assert {:refuse, %UnderQuorum{answered: 2, needed: 3}} = result
    assert ms >= 400, "returned after #{ms} ms: it stopped waiting below the floor"
  end

  test "a previous owner's answer does not count toward the floor, so it does not end the wait",
       %{
         id: id,
         survivor: {surv, _}
       } do
    {_, {key, _, _} = prev_peer} = live("early_prev")
    # Needed 2 (N = 3, Q = 2); the only fast answer is the PREVIOUS OWNER, which is excluded from
    # the count (expert review 2026-10-13 #14) - so this is still 1 of 2 and the open must refuse.
    peers = [prev_peer, slow_peer("s1"), slow_peer("s2")]

    {ms, result} =
      timed(fn ->
        Recovery.best_replica(id, stamp(9, 5, 0),
          follower: surv,
          peers: peers,
          quorum: 2,
          query_timeout_ms: 400,
          previous_owner: key <> "#boot"
        )
      end)

    assert {:refuse, %UnderQuorum{answered: 1, needed: 2}} = result
    assert ms >= 400, "the excluded previous owner ended the wait after #{ms} ms"
  end

  test "an unrankable stamp makes zero peer connects", %{id: id, survivor: {surv, _}} do
    peers = [slow_peer("p1"), slow_peer("p2"), slow_peer("p3")]

    for unrankable <- [nil, %{epoch: 0, wal_ordinal: 0}, %{epoch: 9, wal_ordinal: 0}] do
      {ms, result} =
        timed(fn ->
          Recovery.best_replica(id, unrankable, follower: surv, peers: peers, quorum: 2)
        end)

      assert result == :none
      assert ms < 100
    end

    refute_receive {:slow_peer_accepted, _}, 200, "an unrankable stamp dialed a peer"

    # The fixture can count: the same peers ARE dialed for a rankable stamp.
    Recovery.best_replica(id, stamp(9, 5, 0),
      follower: surv,
      peers: peers,
      quorum: 2,
      query_timeout_ms: 200
    )

    assert_receive {:slow_peer_accepted, _}, 500
  end

  describe "the recovery marker" do
    setup do
      prev =
        Map.new(
          [
            :replication_promote_on_open,
            :replication_recover_from_peers,
            :replication_followers,
            :recovery_ask_crash_hook,
            :replication_quorum
          ],
          &{&1, Application.get_env(:fathom, &1)}
        )

      prev_running = Fleet.running()
      Fleet.publish([])

      on_exit(fn ->
        Fleet.publish(prev_running)

        for {k, v} <- prev do
          if is_nil(v),
            do: Application.delete_env(:fathom, k),
            else: Application.put_env(:fathom, k, v)
        end
      end)

      Application.put_env(:fathom, :replication_promote_on_open, true)
      Application.put_env(:fathom, :replication_recover_from_peers, true)
      Application.put_env(:fathom, :replication_quorum, 2)

      Application.put_env(
        :fathom,
        :replication_followers,
        for(n <- 1..4, do: {"dead#{n}", "127.0.0.1", n})
      )

      test = self()

      Application.put_env(:fathom, :recovery_ask_crash_hook, fn path ->
        send(test, {:asked, path})
      end)

      :ok
    end

    @tag :marker
    test "an unrankable prefetched head skips the marker and the ask; a rankable one writes it",
         %{id: id} do
      # The default-named follower is what a coordinator looks for.
      dir = Path.join(System.tmp_dir!(), "early_def_#{System.unique_integer([:positive])}")
      File.mkdir_p!(dir)
      on_exit(fn -> File.rm_rf(dir) end)
      start_supervised!({Follower, name: Follower, port: 0, dir: dir}, id: :default_follower)

      path = Path.join(dir, "#{id}.db")
      File.write!(path, "")
      lease = %{owner: "me", epoch: 1}

      for head <- [{:ok, nil}, {:ok, %{etag: "e", position: nil, lineage: nil, md5: nil}}] do
        assert "e" ==
                 PromoteOnOpen.maybe_promote_replica(id, path, lease, "e", 1, false, head)

        refute_received {:asked, _}, "the marker/ask ran for an unrankable object"
        refute File.exists?(PromoteOnOpen.recovering_path(path))
      end

      rankable = {:ok, %{etag: "e", position: stamp(9, 5, 0), lineage: 9, md5: nil}}

      assert_raise UnderQuorum, fn ->
        PromoteOnOpen.maybe_promote_replica(id, path, lease, "e", 1, false, rankable)
      end

      assert_received {:asked, ^path}
    end
  end
end
