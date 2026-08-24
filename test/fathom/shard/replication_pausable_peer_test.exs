defmodule Fathom.Shard.ReplicationPausablePeerTest do
  @moduledoc """
  Tests for the TEST FIXTURE, `Fathom.Test.PausablePeer`, because five bugs in it have now
  presented as product bugs.

  Every one of the five looked identical from the outside — "the proxied follower simply never
  converged", intermittently — which is indistinguishable from the straggler behaviour the fixture
  exists to study. Two of them made CI red on 2 of 3 OTP legs. AGENTS.md's standing lesson is that
  a gap between a double and the real thing silently exempts every bug in that contract, and that
  closing the gap is worth more than the single fix that exposed it; a fixture that has been wrong
  five times has earned tests of its own.

  ## The one pinned here: a frame that arrives before its `{:accepted, _}` on a RECONNECT

  The acceptor calls `:gen_tcp.controlling_process/2` and THEN sends `{:accepted, sock}`. Those two
  are not ordered relative to each other, so the kernel can deliver `{:tcp, sock, frame}` to the
  peer first. A clause was added for that case guarded on `%{up: nil}` — which covers the FIRST
  connection and only the first. On a reconnect (a `Shipper` reconnects 500 ms after any socket
  error) `state.up` is still the previous upstream socket, so the frame matched no clause, fell
  through the catch-all, and was dropped. Routing on socket identity instead covers both.

  ## Why this is driven through the mailbox and not over a real reconnect

  The failure is a MESSAGE ORDER, and no black-box test can command one: from outside, a client
  reconnecting and immediately writing produces the two messages in whichever order the VM chooses,
  which is the whole reason the bug is intermittent in the first place. So the test hands the peer a
  genuinely connected socket it has not been told about, sends the frame, and only then sends
  `{:accepted, _}` — the exact interleaving, every run.
  """
  use ExUnit.Case, async: false

  alias Fathom.Test.PausablePeer

  @sockopts [:binary, packet: 4, active: true, reuseaddr: true, nodelay: true]

  # A listener that accepts REPEATEDLY and hands every accepted socket to the test process.
  #
  # Repeatedly is load-bearing, and the first draft was one-shot — the same mistake the fixture
  # under test made in its own second bug, reached from the other side. `{:accepted, _}` makes the
  # peer dial a FRESH upstream connection, so a one-shot acceptor leaves that dial sitting in the
  # backlog with nobody reading it. The test then failed with the fix correctly in place, which
  # reads as "the fix does not work" rather than "the harness cannot express the case".
  defp start_acceptor!(tag) do
    {:ok, lsock} = :gen_tcp.listen(0, @sockopts)
    {:ok, port} = :inet.port(lsock)
    test = self()

    spawn_link(fn -> accept_forever(lsock, test, tag) end)
    on_exit(fn -> :gen_tcp.close(lsock) end)

    {lsock, port}
  end

  defp accept_forever(lsock, test, tag) do
    case :gen_tcp.accept(lsock) do
      {:ok, sock} ->
        :ok = :gen_tcp.controlling_process(sock, test)
        send(test, {tag, sock})
        accept_forever(lsock, test, tag)

      # The listener closed during teardown. Exit NORMALLY: this is spawn_link'd, and an abnormal
      # exit here would take the test process down with it and look like a transport failure.
      _ ->
        :ok
    end
  end

  # A real, connected socket owned by `peer` that `peer` has NOT been told about — exactly what its
  # own acceptor produces in the window between `controlling_process/2` and `{:accepted, _}`.
  defp unannounced_socket!(port, peer) do
    {:ok, client} = :gen_tcp.connect(~c"127.0.0.1", port, @sockopts, 5_000)
    assert_receive {:handover, server}, 2_000
    :ok = :gen_tcp.controlling_process(server, peer)
    {client, server}
  end

  defp connect_and_prime!(peer_port) do
    {:ok, client} = :gen_tcp.connect(~c"127.0.0.1", peer_port, @sockopts, 5_000)
    assert_receive {:upstream, _sock}, 2_000
    :ok = :gen_tcp.send(client, "hello")

    assert_receive {:tcp, _, "hello"},
                   2_000,
                   "the ORDINARY path is broken, so nothing below this line proves anything"

    client
  end

  test "a frame that beats its accept on a RECONNECT still reaches the follower" do
    {_up_lsock, upstream_port} = start_acceptor!(:upstream)
    peer = start_supervised!({PausablePeer, upstream_port: upstream_port, notify: self()})

    # The first connection, established normally. This is what makes the bug reachable: `state.up`
    # is now non-nil, so the old `%{up: nil}` buffering clause no longer matches anything.
    _first = connect_and_prime!(PausablePeer.port(peer))

    # We cannot reach inside the peer's own `accept_next/2`, so its acceptor is rebuilt by hand: a
    # connected socket transferred to the peer, a frame delivered from it, and `{:accepted, _}`
    # arriving afterwards.
    {_handover_lsock, handover_port} = start_acceptor!(:handover)
    {_client2, server2} = unannounced_socket!(handover_port, peer)

    # Frame FIRST, accept notification SECOND. Pre-fix this matched neither leg (the socket is
    # neither `down` nor `up`) and the `%{up: nil}` clause was inapplicable, so the catch-all ate it.
    send(peer, {:tcp, server2, "after-reconnect"})
    send(peer, {:accepted, server2})

    assert_receive {:tcp, _, "after-reconnect"},
                   2_000,
                   "the first frame of a reconnected connection was dropped by the fixture. To a " <>
                     "test this is the proxied follower silently missing a push — the same " <>
                     "presentation as the four fixture bugs before it, and the same presentation " <>
                     "as the product straggler bug this fixture exists to study."
  end

  # THIS ONE DOES NOT DISCRIMINATE THE BUG, and saying so is the point of the comment.
  #
  # It passes against the old fixture too, for the wrong reason: there the frame was DROPPED
  # outright, so of course it was not replayed. Measured, not assumed — reverting
  # `pausable_peer.ex` leaves the test above failing and this one green.
  #
  # It is kept as an invariant guard on the new socket-tagged `pending` list, which is where a
  # replay could now be introduced: filtering by socket is one comprehension filter away from being
  # "simplified" back into forwarding everything. It is NOT a regression test for the fifth bug.
  test "a frame buffered from an ABANDONED socket is not replayed onto the new connection" do
    {_up_lsock, upstream_port} = start_acceptor!(:upstream)
    peer = start_supervised!({PausablePeer, upstream_port: upstream_port, notify: self()})

    _first = connect_and_prime!(PausablePeer.port(peer))

    {_handover_lsock, handover_port} = start_acceptor!(:handover)
    {_c_old, old} = unannounced_socket!(handover_port, peer)
    {_c_new, new} = unannounced_socket!(handover_port, peer)

    # A frame from a connection the primary then abandoned, followed by the accept of a DIFFERENT
    # one. Forwarding the stale frame would be the fixture inventing traffic — the mirror of the
    # rule that already drops HELD replies across a reconnect.
    send(peer, {:tcp, old, "abandoned"})
    send(peer, {:accepted, new})

    refute_receive {:tcp, _, "abandoned"},
                   500,
                   "a frame from a socket the primary abandoned was replayed onto the new " <>
                     "connection, so the follower saw a push nobody re-sent to it"
  end
end
