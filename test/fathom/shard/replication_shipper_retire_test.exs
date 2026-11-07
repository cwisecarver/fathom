defmodule Fathom.Shard.ReplicationShipperRetireTest do
  @moduledoc """
  Expert review 2026-11-07 #44 ("same class"): a shipper stopped by a membership swap left its
  outstanding waiters unanswered.

  A `Shipper` does not trap exits, so `DynamicSupervisor.terminate_child/2` kills it without
  running `terminate/2`. A push still waiting on its follower's reply was never answered: the
  Session that sent it kept an `inflight` entry for a process that no longer existed, and a
  catch-up cycle then settled on that entry until its deadline. `Fleet.stop_shipper/1` now
  `Shipper.retire/1`s it first, so every waiter hears `:disconnected` — the answer a dead link
  gives, and one `Session.reconcile/2` already treats as settled.

  The follower's reply is held by a `PausablePeer`, so the waiter really is outstanding when the
  shipper is stopped.
  """
  use ExUnit.Case, async: false

  alias Fathom.Shard.Replication.{Fleet, Follower, Shipper}
  alias Fathom.Shard.Replication.Protocol.Push
  alias Fathom.Test.PausablePeer
  import Fathom.Test.ReplicationCase, only: [start_followers!: 3, enable_replication!: 2]

  test "stopping a shipper answers a push still waiting on it :disconnected" do
    root = Path.join(System.tmp_dir!(), "shipretire_#{System.unique_integer([:positive])}")
    File.mkdir_p!(root)
    on_exit(fn -> File.rm_rf(root) end)

    [{f1, p1}, f2, f3] = start_followers!(root, 3, "sr")
    Follower.seed(f1, "acme", 1, 1, 0, 0)
    peer = start_supervised!({PausablePeer, upstream_port: p1, notify: self()})

    enable_replication!([{f1, PausablePeer.port(peer)}, f2, f3], connected: :all)

    # The shipper that dials the peer is the one published for the peer's port.
    peer_port = PausablePeer.port(peer)
    {_, _, ^peer_port, name} = Enum.find(Fleet.running(), &(elem(&1, 2) == peer_port))

    :ok = PausablePeer.pause(peer)

    Shipper.push(name, %Push{
      shard_id: "acme",
      epoch: 1,
      wal_gen: 1,
      salt1: 1,
      offset: 0,
      payload: :binary.copy(<<0xCD>>, 512)
    })

    assert_receive {:peer_frame, :to_primary, _}, 2_000
    assert PausablePeer.held(peer) == 1, "fixture: the follower's reply was not withheld"
    refute_received {:repl_reply, _, _}

    :ok = Fleet.stop_shipper(name)

    assert_receive {:repl_reply, ^name, {:reject, "acme", :disconnected, 0}},
                   2_000,
                   "a stopped shipper left its waiter unanswered"
  end
end
