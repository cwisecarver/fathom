defmodule Fathom.Test.ReplicationWire do
  @moduledoc """
  Speak one A2 frame to a follower's replication listener over a real socket and decode its reply.

  Six follower/replication tests each carried a private `send_frame!/2` in two variants (strict, and
  one that hands a transport error back instead of crashing); this is the one copy of each
  (cleanliness audit 2026-11-13 #37).
  """

  alias Fathom.Shard.Replication.Protocol

  @connect_opts [:binary, packet: 4, active: false]

  @doc """
  Send `frame` to `port` on loopback, read one reply, decode it. A connect/send/recv failure
  crashes the test with a match error: use when the exchange must succeed.
  """
  @spec send_frame!(:inet.port_number(), iodata()) :: term()
  def send_frame!(port, frame) do
    {:ok, sock} = :gen_tcp.connect(~c"127.0.0.1", port, @connect_opts, 5_000)

    try do
      :ok = :gen_tcp.send(sock, frame)
      {:ok, bytes} = :gen_tcp.recv(sock, 0, 5_000)
      Protocol.decode(bytes)
    after
      :gen_tcp.close(sock)
    end
  end

  @doc """
  Like `send_frame!/2`, but a receive failure (the listener closed the socket without answering,
  e.g. on a refused frame) is returned as `{:error, reason}` instead of raising.
  """
  @spec send_frame(:inet.port_number(), iodata()) :: term() | {:error, term()}
  def send_frame(port, frame) do
    {:ok, sock} = :gen_tcp.connect(~c"127.0.0.1", port, @connect_opts, 5_000)

    try do
      :ok = :gen_tcp.send(sock, frame)

      case :gen_tcp.recv(sock, 0, 5_000) do
        {:ok, bytes} -> Protocol.decode(bytes)
        other -> other
      end
    after
      :gen_tcp.close(sock)
    end
  end
end
