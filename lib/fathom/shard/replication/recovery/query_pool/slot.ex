defmodule Fathom.Shard.Replication.Recovery.QueryPool.Slot do
  @moduledoc false
  # One connection to one peer, and one ask at a time on it. Started and linked by `QueryPool`,
  # whose moduledoc states the rules this process keeps (expert review 2026-10-21 #9).

  use GenServer

  require Logger

  alias Fathom.Shard.Replication.{Protocol, Recovery}
  alias Fathom.Shard.Replication.Recovery.QueryPool

  @impl true
  def init(endpoint), do: {:ok, %{endpoint: endpoint, sock: nil}}

  @impl true
  def handle_call({:ask, key, shard_id, timeout_ms, connect_ms, deadline, honor?}, from, st) do
    {caller, _tag} = from

    {answer, st} =
      if remaining(deadline) == 0 or not Process.alive?(caller) do
        # The caller has already given up, or was killed by `Recovery`'s early exit (rv6 F2). Do
        # not spend the network on an answer nobody reads.
        {:silent, st}
      else
        run(st, shard_id, timeout_ms, connect_ms, deadline, honor?)
      end

    # Free the slot before replying, so the next ask finds it idle rather than overflowing to a
    # fresh dial while this reply is in flight.
    QueryPool.release(key, deadline)
    {:reply, answer, st, QueryPool.idle_ms()}
  end

  @impl true
  def handle_info(:timeout, st), do: {:noreply, drop(st)}
  def handle_info(_msg, st), do: {:noreply, st, QueryPool.idle_ms()}

  @impl true
  def terminate(_reason, st) do
    _ = drop(st)
    :ok
  end

  defp run(%{sock: nil} = st, shard_id, timeout_ms, connect_ms, deadline, honor?),
    do: fresh(st, shard_id, timeout_ms, connect_ms, deadline, honor?)

  defp run(%{sock: sock} = st, shard_id, timeout_ms, connect_ms, deadline, honor?) do
    if idle_clean?(sock) do
      case Recovery.exchange(sock, shard_id, min(timeout_ms, remaining(deadline))) do
        {answer, :keep} ->
          {answer, st}

        {_answer, :dead} ->
          # Closed or reset before any reply byte arrived: most likely a peer restart since the
          # last ask. Redial once inside the same deadline.
          fresh(drop(st), shard_id, timeout_ms, connect_ms, deadline, honor?)

        {answer, :close} ->
          {answer, drop(st)}
      end
    else
      fresh(drop(st), shard_id, timeout_ms, connect_ms, deadline, honor?)
    end
  end

  defp fresh(st, shard_id, timeout_ms, connect_ms, deadline, honor?) do
    {_key, host, port} = endpoint = st.endpoint

    cond do
      remaining(deadline) == 0 ->
        {:silent, st}

      # Queued behind an ask that just found this peer blackholed: skip it as the caller would.
      honor? and Recovery.unreachable?(endpoint) ->
        {:silent, st}

      true ->
        connect_and_ask(st, endpoint, host, port, shard_id, timeout_ms, connect_ms, deadline)
    end
  end

  # Dial and handshake are separate steps here, unlike the fresh path's `connect/2`, because only a
  # DIAL that ran (about) its full connect bound may mark the peer unreachable. Both are clipped to
  # the caller's deadline. A dial cut well short by that clipping proves nothing about the peer, and
  # marking it would make every open skip a healthy peer for 10 s. A handshake timeout marks nothing:
  # with the default timeouts the fresh path's 5 s handshake could not finish inside the caller's
  # 4 s budget, so it never marked there either.
  defp connect_and_ask(st, endpoint, host, port, shard_id, timeout_ms, connect_ms, deadline) do
    left = remaining(deadline)
    bound = min(connect_ms, left)

    with {:dial, {:ok, sock}} <- {:dial, Recovery.dial_peer(host, port, bound)},
         hello_timeout = min(Protocol.handshake_timeout_ms(), left),
         {:hello, _sock, :ok} <- {:hello, sock, Protocol.handshake_connect(sock, hello_timeout)} do
      Recovery.reachable(endpoint)

      case Recovery.exchange(sock, shard_id, min(timeout_ms, remaining(deadline))) do
        {answer, :keep} ->
          {answer, %{st | sock: sock}}

        {answer, _close_or_dead} ->
          :gen_tcp.close(sock)
          {answer, st}
      end
    else
      {:dial, {:error, reason}} ->
        if full_dial?(bound, connect_ms), do: Recovery.note_unreachable(endpoint, reason)
        {:silent, st}

      {:hello, sock, {:error, _reason}} ->
        :gen_tcp.close(sock)
        {:silent, st}
    end
  end

  @doc false
  # Whether a dial bounded by `bound` ran long enough that its timeout says something about the
  # peer (expert review 2026-10-21 rv6 F8). It used to require `bound == connect_ms` exactly. A peer
  # partitioned AFTER the handshake shows up first as a recv timeout on a reused socket, which marks
  # nothing; the asks that then dialed it had lost part of their budget (before rv6 F2, to the slot
  # queue; now, to a millisecond of message passing at most), so their dial timeouts marked nothing
  # either, and every open kept paying the connect timeout. 90% of the bound is a timeout on the
  # peer, not on our own clock.
  @spec full_dial?(non_neg_integer(), non_neg_integer()) :: boolean()
  def full_dial?(bound, connect_ms), do: bound * 10 >= connect_ms * 9

  # Nothing may be waiting on an idle socket: this slot never has a query in flight between
  # asks. A frame here is one nobody asked for, and `{:error, :closed}` is a peer that hung up
  # (a restart, or the follower's own close). Either way the socket is not reused.
  defp idle_clean?(sock) do
    case :gen_tcp.recv(sock, 0, 0) do
      {:error, :timeout} ->
        true

      {:ok, _stray} ->
        Logger.debug("replication recovery: discarding a pooled connection with a stray frame")
        false

      {:error, _} ->
        false
    end
  end

  defp drop(%{sock: nil} = st), do: st

  defp drop(%{sock: sock} = st) do
    :gen_tcp.close(sock)
    %{st | sock: nil}
  end

  defp remaining(deadline), do: max(deadline - System.monotonic_time(:millisecond), 0)
end
