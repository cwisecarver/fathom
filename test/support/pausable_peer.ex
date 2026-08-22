defmodule Fathom.Test.PausablePeer do
  @moduledoc """
  A transparent TCP proxy in front of a real `Fathom.Shard.Replication.Follower`, whose replies can
  be **held and released on demand**.

  ## Why this exists

  A2's most persistent bug class is the straggler: `ship_quorum/4` returns at the Q-th ack, so a
  follower that has not answered still holds its shipper's single per-shard waiter, and the next
  push for that shard is refused `:already_in_flight` before it reaches the socket. Reproducing that
  needs a follower that **accepts a push and withholds its reply until told otherwise** — and until
  this module there was no way to build one:

    * `Follower` hands every accepted socket to an **unlinked `Task`** (`follower.ex:235`) and
      `handle_push/2` is a plain function, so `:sys.suspend/1` on the `Follower` GenServer does not
      stop it answering.
    * `replication_transport_test.exs`'s `black_hole!/1` never answers *at all*, which models node
      death, not slowness. A straggler is a peer that is late, not one that is gone.

  This is the third time this repository has been bitten by a fixture that could not express the
  failure it needed to catch (`Storage.Local` vs S3 lock etags; the follower `Task` vs node death),
  and AGENTS.md's standing lesson is that **closing the fixture gap is worth more than the single
  fix that exposed it**. Hence a reusable module rather than a helper inside one test file.

  ## Why a PROXY and not a fake follower

  A hand-written fake would have to reproduce `FollowerLog`'s accept/reject decision, the WAL writes
  and the exact acked offsets — i.e. re-implement the thing under test, and drift from it. Proxying
  a real `Follower` keeps every one of those semantics real and controls only **when the reply is
  delivered**, which is the single variable a straggler test needs.

  ## Everything is owned by one process, deliberately

  Both sockets are owned by this GenServer, so stopping it provably closes them. That is the exact
  mistake `Follower` makes (an unlinked `Task` survives `stop_supervised!` and keeps serving), and
  it is the reason this fixture had to be written at all — so it is not repeated here.

      {:ok, peer} = PausablePeer.start_link(upstream_port: follower_port, notify: self())
      port = PausablePeer.port(peer)          # point the shipper at THIS, not the follower

      PausablePeer.pause(peer)                # replies now queue instead of being delivered
      # ... the primary now sees a follower that received its bytes and never answered ...
      PausablePeer.release(peer)              # deliver everything held, resume passthrough
  """
  use GenServer

  # `packet: 4` on both legs, matching `Shipper` and `Follower`. Forwarding whole FRAMES rather than
  # raw bytes is what makes "hold the replies" well defined — a byte-level proxy could split a frame
  # across the pause boundary and deliver half a reply.
  @sockopts [:binary, packet: 4, active: true, reuseaddr: true, nodelay: true]

  defstruct [:lsock, :down, :up, :upstream_port, :notify, paused: false, held: [], pending: []]

  @spec start_link(keyword()) :: GenServer.on_start()
  def start_link(opts), do: GenServer.start_link(__MODULE__, opts, Keyword.take(opts, [:name]))

  @doc "The port a shipper should connect to. Bound before `start_link/1` returns."
  @spec port(GenServer.server()) :: :inet.port_number()
  def port(peer), do: GenServer.call(peer, :port)

  @doc """
  Stop delivering the follower's replies. Frames from the primary still flow THROUGH, so the
  follower really does receive and apply them — this models a slow answer, not a dead peer.
  """
  @spec pause(GenServer.server()) :: :ok
  def pause(peer), do: GenServer.call(peer, :pause)

  @doc "Deliver every held reply in order, then resume passthrough. Returns how many were held."
  @spec release(GenServer.server()) :: {:ok, non_neg_integer()}
  def release(peer), do: GenServer.call(peer, :release)

  @doc "How many replies are currently held. For assertions that avoid sleeping."
  @spec held(GenServer.server()) :: non_neg_integer()
  def held(peer), do: GenServer.call(peer, :held)

  @impl true
  def init(opts) do
    {:ok, lsock} = :gen_tcp.listen(0, @sockopts)
    accept_next(lsock, self())

    {:ok,
     %__MODULE__{
       lsock: lsock,
       upstream_port: Keyword.fetch!(opts, :upstream_port),
       notify: Keyword.get(opts, :notify)
     }}
  end

  @impl true
  def handle_call(:port, _from, state) do
    {:ok, port} = :inet.port(state.lsock)
    {:reply, port, state}
  end

  def handle_call(:pause, _from, state), do: {:reply, :ok, %{state | paused: true}}
  def handle_call(:held, _from, state), do: {:reply, length(state.held), state}

  def handle_call(:release, _from, state) do
    # Oldest first: the primary correlates replies per shard, and delivering them out of order would
    # be a fault this fixture invented rather than one it observed.
    for frame <- Enum.reverse(state.held), do: send_down(state, frame)
    {:reply, {:ok, length(state.held)}, %{state | paused: false, held: []}}
  end

  # DATA CAN BEAT THE ACCEPT NOTIFICATION. The acceptor calls `controlling_process/2` and THEN sends
  # `{:accepted, sock}`, but once ownership transfers the kernel may deliver buffered `{:tcp, _, _}`
  # straight to this process — and those two messages are not ordered relative to each other. Before
  # this clause such a frame matched neither the `down:` nor the `up:` clause, fell through the
  # catch-all and was silently DROPPED, which presented as the first push after connect vanishing.
  #
  # Buffer instead: `handle_info({:accepted, _}, _)` flushes these the moment the upstream leg is up.
  @impl true
  def handle_info({:tcp, sock, frame}, %{up: nil} = state) do
    {:noreply, %{state | down: sock, pending: [frame | state.pending]}}
  end

  def handle_info({:accepted, sock}, state) do
    # ACCEPT REPEATEDLY, not once. A `Shipper` reconnects 500 ms after ANY socket error, and the
    # first version of this module accepted exactly one connection — so a reconnect completed the
    # TCP handshake against the listen backlog and then sat there with nobody accepting it, and
    # every frame after that vanished. It presented as the proxied follower simply never converging,
    # intermittently, which is indistinguishable from the product bug this fixture exists to study.
    #
    # A fixture that cannot survive an ordinary reconnect cannot be trusted to report one, which is
    # the same lesson that motivated the module in the first place — arrived at the hard way, from
    # the inside.
    # `old != sock` IS A FOURTH FIXTURE BUG (2026-08-22), and it is the SAME
    # data-beats-the-notification race the `up: nil` clause above exists for — reached from the
    # other end. Captured live rather than reasoned about:
    #
    #     +0.00ms   buffer     sock=274, up==nil  ->  down := 274
    #     +0.01ms   accepted   sock=274, old_down=274      <- closed the socket being accepted
    #     +0.80ms   from_up    16 bytes, paused -> held
    #     +501.4ms  accepted   sock=278                    <- the Shipper's 500 ms reconnect
    #
    # When a frame arrives before `{:accepted, sock}`, that clause has ALREADY stored the very
    # socket being accepted as `state.down`, because it needs somewhere to put the bytes. This loop
    # then closed it and installed the corpse as the new `down`. The `Shipper` sees the close,
    # reconnects 500 ms later, and a reconnect deliberately clears `held: []` — so the withheld
    # reply is DROPPED and `release/1` delivers nothing. A test awaiting that ack gets
    # `{:reject, "acme", :disconnected, 0}` instead.
    #
    # Same presentation as the three bugs above: the proxied follower simply never converging,
    # which is indistinguishable from the product bug this fixture exists to study. It is what made
    # CI red on 2 of 3 OTP legs twice running on 2026-08-22, and it is the cause of the flake the
    # comment on the describe block below predicted.
    #
    # A genuine reconnect is unaffected: there `state.down` is the PREVIOUS socket, so it still
    # closes.
    #
    # TWO THINGS ABOUT DIAGNOSING THIS, both learned the expensive way:
    #
    #   * It reproduces at roughly 1 in 30 runs of `replication_transport_test.exs` TOGETHER WITH
    #     `replication_commit_test.exs` under CPU load, and essentially never with the transport
    #     file alone. A negative from the wrong combination is not evidence — this fix was
    #     correctly diagnosed, then WRONGLY retracted on 0 hits from a 12-run single-file probe.
    #   * Any per-event `IO.puts` here SUPPRESSES it completely (0 failures in 40 runs). The window
    #     is a few microseconds wide. Accumulate diagnostics in the struct and dump them once, and
    #     note that `terminate/2` will not run to do that dumping — this GenServer does not trap
    #     exits, so a supervisor shutdown kills it outright and the cleanup in `terminate/2` below
    #     has never executed.
    for old <- [state.down, state.up], old != nil, old != sock, do: :gen_tcp.close(old)
    accept_next(state.lsock, self())

    # Connect upstream only once a client has arrived, so a peer that is never used opens nothing.
    {:ok, up} = :gen_tcp.connect(~c"127.0.0.1", state.upstream_port, @sockopts, 5_000)

    # A reconnect means the primary abandoned whatever it was waiting for, so held frames belong to
    # a socket that no longer exists. Dropping them is correct; delivering them down the NEW socket
    # would be the fixture inventing a reply.
    # Oldest first — these are frames that arrived before the upstream leg existed.
    for frame <- Enum.reverse(state.pending), do: :gen_tcp.send(up, frame)
    for _ <- state.pending, do: notify(state, :to_follower)

    {:noreply, %{state | down: sock, up: up, held: [], pending: []}}
  end

  # Primary -> follower. ALWAYS forwarded, even while paused: the follower must genuinely receive and
  # apply the bytes, or this would be a black hole with extra steps.
  def handle_info({:tcp, sock, frame}, %{down: sock} = state) do
    :gen_tcp.send(state.up, frame)
    notify(state, :to_follower)
    {:noreply, state}
  end

  # Follower -> primary. This is the only direction the pause affects.
  def handle_info({:tcp, sock, frame}, %{up: sock} = state) do
    notify(state, :to_primary)

    if state.paused do
      {:noreply, %{state | held: [frame | state.held]}}
    else
      send_down(state, frame)
      {:noreply, state}
    end
  end

  # The FOLLOWER closed. Mirror it onto the client, which is what a direct connection would show,
  # rather than leaving the primary talking into a proxy whose far end is gone. Surfaced through
  # `notify/2` so a test can tell this apart from a frame simply being slow.
  def handle_info({:tcp_closed, sock}, %{up: sock} = state) do
    if state.down, do: :gen_tcp.close(state.down)
    notify(state, :upstream_closed)
    {:noreply, %{state | down: nil, up: nil, held: [], pending: []}}
  end

  def handle_info({:tcp_closed, _}, state), do: {:noreply, state}
  def handle_info({:tcp_error, _, _}, state), do: {:noreply, state}
  def handle_info(_, state), do: {:noreply, state}

  @impl true
  def terminate(_reason, state) do
    for s <- [state.down, state.up, state.lsock], s != nil, do: :gen_tcp.close(s)
    :ok
  end

  defp send_down(%{down: down}, frame) when down != nil, do: :gen_tcp.send(down, frame)
  defp send_down(_state, _frame), do: :ok

  # One acceptor at a time, handing the socket to the GenServer so every `{:tcp, _, _}` lands in
  # `handle_info/2` and dies with it. Re-armed on each accept, so reconnects are served.
  defp accept_next(lsock, owner) do
    # `spawn_link`, so the acceptor dies with the peer — but that link cuts BOTH ways, and an
    # abnormal exit here takes the peer down with it, closing both sockets. The primary then sees
    # `:disconnected` out of nowhere, which is indistinguishable from the transport failure this
    # fixture is used to study. It was diagnosed exactly that way: a test asserting an ack got
    # `{:reject, "acme", :disconnected, 0}` instead.
    #
    # So every failure path here exits NORMALLY. `accept/1` can return errors other than `:closed`
    # during teardown, and `controlling_process/2` fails outright if the socket died between accept
    # and transfer — as a bare `:ok = ...` match that was a `MatchError`, i.e. an abnormal exit.
    spawn_link(fn ->
      with {:ok, sock} <- :gen_tcp.accept(lsock),
           :ok <- :gen_tcp.controlling_process(sock, owner) do
        send(owner, {:accepted, sock})
      else
        # Includes the ordinary teardown case (the listener closed), which must not look like a
        # crash: a linked process doing exactly what it should should not log one.
        _ -> :ok
      end
    end)
  end

  # Lets a test synchronise on frames actually crossing the wire instead of sleeping — the same
  # reason `black_hole!/1` reports `{:black_hole_push, _}`.
  defp notify(%{notify: nil}, _dir), do: :ok
  defp notify(%{notify: pid}, dir), do: send(pid, {:peer_frame, dir, self()})
end
