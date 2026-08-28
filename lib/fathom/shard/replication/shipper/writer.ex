defmodule Fathom.Shard.Replication.Shipper.Writer do
  @moduledoc """
  The process that owns one link's blocking `:gen_tcp.send/2` calls — expert review 2026-08-26 #19.

  ## Why it exists

  `Shipper` is both the writer and the READER of its socket. `:gen_tcp.send/2` blocks until the
  peer's receive window drains — up to `send_timeout`, 5 000 ms — so a send inside `handle_cast/2`
  left incoming acks **for every other shard on that link** sitting unread in the shipper's mailbox.
  A shard whose ack had already arrived could not complete its quorum until an unrelated shard's
  send unblocked, and `Budget.release/2` was delayed with it, so the node-wide byte budget stayed
  claimed for the whole stall.

  The defect is absorbed at N=3 / Q=2 and bites at N=2 / Q=1 — fathom's documented three-node
  development topology — or with one link congested and one peer down.

  ## Why this shape

  **`:gen_tcp.send/2` may be called from any process; only the CONTROLLING process receives
  `{:tcp, …}`.** So the shipper stays the controlling process and the reader, and this owns the
  sends. Three properties fall out:

    * **Ordering is free.** The shipper is this process's only sender, and BEAM guarantees message
      order between one pair of processes, so frames leave in the order they were cast.
    * **The bound already exists.** Moving the block here moves the queue here, which would be
      unbounded — except one push per shard is enforced by the shipper's `waiters`, and the
      node-wide `Budget` caps total bytes in flight.
    * **A drop still fails every waiter.** A failed send is reported back and the shipper runs its
      ordinary `drop/2`.

  ## Every send goes through here, including the seeds

  Seed frames are sent with `send_now/2`, a synchronous call, so the shipper blocks exactly as it
  did before — the seed state machine's ordering and abort semantics are untouched, which is the
  half of #19 that was deliberately NOT made asynchronous.

  Routing them through here anyway makes this socket **single-sender**, which is worth more than it
  looks: `send_timeout` and `send_timeout_close` are per-socket settings whose behaviour with two
  processes sending concurrently is not something to have to reason about on a quorum path. One
  sender removes the question rather than answering it.

  ## The stale-report guard

  `stop/1` cannot interrupt a send already in progress, so a writer whose socket was just closed can
  still report a failure after its replacement exists. Every report carries this process's pid and
  the shipper matches on it, so a straggler is ignored rather than tearing down the socket that
  replaced it. The socket-level twin of that guard is on `{:tcp_closed, sock}` in `Shipper`.
  """

  # `send_timeout` defaults to 5 s and is operator-tunable; this only has to be comfortably above
  # whatever it is, because the socket is what actually bounds the send.
  @call_timeout_ms 30_000

  @doc """
  Start a writer for `sock`, reporting failures to `shipper`.

  Linked deliberately, and NOT trapping: this process does nothing but call `:gen_tcp.send/2`, which
  returns errors rather than raising, so a crash here means something genuinely unexpected and the
  loud answer — take the shipper down with it and let the supervisor reconnect — is the right one. A
  normal exit does not propagate, which is what makes `stop/1` safe.
  """
  @spec start_link(:gen_tcp.socket(), pid()) :: pid()
  def start_link(sock, shipper), do: spawn_link(fn -> loop(sock, shipper) end)

  @doc """
  Queue `iodata` for sending. Returns immediately; a failure is reported to the shipper.

  This is the whole point of the module: the caller does not block, so it keeps reading acks.
  """
  @spec frame(pid() | nil, iodata()) :: :ok
  def frame(nil, _iodata), do: :ok

  def frame(writer, iodata) do
    send(writer, {:frame, iodata})
    :ok
  end

  @doc """
  Send `iodata` and wait for the result — the seed path's shape, unchanged from an inline send.

  `{:error, :no_writer}` for a link with no socket, which is what an inline
  `:gen_tcp.send(nil, …)` could never express and the caller's `sock: nil` clauses already handle.
  """
  @spec send_now(pid() | nil, iodata()) :: :ok | {:error, term()}
  def send_now(nil, _iodata), do: {:error, :no_writer}

  def send_now(writer, iodata) do
    ref = make_ref()
    send(writer, {:send_now, self(), ref, iodata})

    receive do
      {^ref, result} -> result
    after
      # Bounded by the socket's own `send_timeout` plus slack. A writer that misses this deadline is
      # wedged in a way `send_timeout` was supposed to prevent, and reporting it as a send error
      # routes into `drop/2` — the same place every other link failure goes.
      @call_timeout_ms -> {:error, :writer_timeout}
    end
  end

  defp loop(sock, shipper) do
    receive do
      {:frame, iodata} ->
        case :gen_tcp.send(sock, iodata) do
          :ok ->
            loop(sock, shipper)

          {:error, reason} ->
            # Report once and STOP. The shipper's `drop/2` closes the socket and starts a fresh
            # link with a fresh writer, so anything still queued here belongs to a link that no
            # longer exists — draining it would send frames onto a closed socket and report N more
            # failures for one event.
            send(shipper, {:send_failed, self(), reason})
            exit(:normal)
        end

      {:send_now, from, ref, iodata} ->
        result = :gen_tcp.send(sock, iodata)
        send(from, {ref, result})

        case result do
          :ok ->
            loop(sock, shipper)

          {:error, reason} ->
            # The caller already has the error and handles it, but the link is still broken and the
            # shipper's `drop/2` is what fails the other waiters. Reporting is not redundant: the
            # seed path's own `{:error, _}` branch drops too, and a second `drop/2` on an already
            # dropped link is a no-op because `writer` no longer matches.
            send(shipper, {:send_failed, self(), reason})
            exit(:normal)
        end

      :stop ->
        exit(:normal)
    end
  end

  @doc """
  Ask a writer to stop. Best-effort by construction — see the stale-report guard in the moduledoc.
  """
  @spec stop(pid() | nil) :: :ok
  def stop(nil), do: :ok

  def stop(writer) do
    send(writer, :stop)
    :ok
  end
end
