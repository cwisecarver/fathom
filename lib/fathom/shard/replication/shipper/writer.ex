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
      node-wide `Budget` caps total bytes in flight. That second half was FALSE until 2026-10-01
      #17: the shipper released each reservation at hand-off, so bytes waiting here were
      uncounted. Each frame now carries its reservation and is released after its send returns.
    * **A drop still fails every waiter.** A failed send is reported back and the shipper runs its
      ordinary `drop/2`.

  ## Every send goes through here, including the seeds

  Seed frames used to be sent with a synchronous `send_now/2` (now removed), so the shipper
  blocked for a whole seed chunk's wire time (4 MiB is ~34 ms at 1 Gbit, seconds on a congested link) with every
  other shard's acks unread behind it — the #19 hazard, back on the seed path (expert review
  2026-10-20-2027 #14). Now a chunk is handed over with `frame_reply/3`: this process sends it and
  reports `{:sent, writer, from, result}` back to the shipper, which answers the seeder. The seeder
  still waits for each chunk's send (the 2026-10-01 #6 one-chunk-in-flight bound) and frames still
  leave in cast order; only the shipper no longer waits. `seed_begin`/`end`/`abort` ride `frame/4`.

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

  alias Fathom.Shard.Replication.Budget

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

  `budget_ref` and `reserved` are the push's byte reservation, already moved onto this writer's
  counter by `Budget.hand_off/3`; it is released here, after the send returns, so the node budget
  still counts a frame while it waits behind a blocked send (expert review 2026-10-01 #17).
  """
  @spec frame(pid() | nil, iodata(), :counters.counters_ref() | nil, non_neg_integer()) :: :ok
  def frame(nil, _iodata, budget_ref, reserved), do: Budget.sent(budget_ref, reserved)

  def frame(writer, iodata, budget_ref, reserved) do
    send(writer, {:frame, iodata, budget_ref, reserved})
    :ok
  end

  @doc """
  Queue a seed chunk and have the result reported back to the shipper as
  `{:sent, writer, from, :ok | {:error, reason}}`, so the shipper can answer the seeder's call
  (`from`) without blocking on the send (expert review 2026-10-20-2027 #14). On a failure the
  `{:sent, …}` report precedes the `{:send_failed, …}` one (same sender, so in order); a request
  still queued here when the writer stops is answered by the shipper's `drop/2`, never lost.
  """
  @spec frame_reply(pid(), iodata(), GenServer.from()) :: :ok
  def frame_reply(writer, iodata, from) do
    send(writer, {:frame_reply, iodata, from})
    :ok
  end

  defp loop(sock, shipper) do
    receive do
      {:frame_reply, iodata, from} ->
        result = :gen_tcp.send(sock, iodata)
        send(shipper, {:sent, self(), from, result})

        case result do
          :ok ->
            loop(sock, shipper)

          {:error, reason} ->
            send(shipper, {:send_failed, self(), reason})
            exit(:normal)
        end

      {:frame, iodata, budget_ref, reserved} ->
        case :gen_tcp.send(sock, iodata) do
          :ok ->
            Budget.sent(budget_ref, reserved)
            loop(sock, shipper)

          {:error, reason} ->
            # Report once and STOP. The shipper's `drop/2` closes the socket and starts a fresh
            # link with a fresh writer, so anything still queued here belongs to a link that no
            # longer exists — draining it would send frames onto a closed socket and report N more
            # failures for one event.
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
