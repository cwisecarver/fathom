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

  defstruct [:lsock, :down, :up, :upstream_port, :notify, paused: false, held: []]

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
    me = self()

    # Accept in a linked helper, then hand the socket to THIS process so every subsequent
    # `{:tcp, _, _}` lands in `handle_info/2` and dies with the GenServer.
    spawn_link(fn ->
      {:ok, sock} = :gen_tcp.accept(lsock)
      :ok = :gen_tcp.controlling_process(sock, me)
      send(me, {:accepted, sock})
    end)

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

  @impl true
  def handle_info({:accepted, sock}, state) do
    # Connect upstream only once a client has arrived, so a peer that is never used opens nothing.
    {:ok, up} = :gen_tcp.connect(~c"127.0.0.1", state.upstream_port, @sockopts, 5_000)
    {:noreply, %{state | down: sock, up: up}}
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

  # Lets a test synchronise on frames actually crossing the wire instead of sleeping — the same
  # reason `black_hole!/1` reports `{:black_hole_push, _}`.
  defp notify(%{notify: nil}, _dir), do: :ok
  defp notify(%{notify: pid}, dir), do: send(pid, {:peer_frame, dir, self()})
end
