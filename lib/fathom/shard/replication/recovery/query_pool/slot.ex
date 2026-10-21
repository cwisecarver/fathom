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
  def handle_call({:ask, shard_id, timeout_ms, connect_ms, deadline, honor?}, _from, st) do
    if remaining(deadline) == 0 do
      # The caller has already given up. Do not spend the network on an answer nobody reads.
      {:reply, :silent, st, QueryPool.idle_ms()}
    else
      {answer, st} = run(st, shard_id, timeout_ms, connect_ms, deadline, honor?)
      {:reply, answer, st, QueryPool.idle_ms()}
    end
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
  # DIAL that ran its full connect bound may mark the peer unreachable. Both are clipped to the
  # caller's deadline, so a slot never holds the asks queued behind it past their budget. But a dial
  # cut short by that clipping (an ask that waited in the queue) proves nothing about the peer, and
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
        if bound == connect_ms, do: Recovery.note_unreachable(endpoint, reason)
        {:silent, st}

      {:hello, sock, {:error, _reason}} ->
        :gen_tcp.close(sock)
        {:silent, st}
    end
  end

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
