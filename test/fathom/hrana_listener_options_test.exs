defmodule Fathom.HranaListenerOptionsTest do
  @moduledoc """
  Expert review 2026-07-24 #6: the Hrana listener shipped with no transport options, so all 100
  acceptors contended on ONE listen socket with ONE 1024-slot kernel accept queue — the structural
  version of the ListenOverflows the 4096-shed report measured.

  These options are only meaningful if the listener actually STARTS with them: ThousandIsland fails
  startup on an unsupported option, and `reuseport` is exactly the kind of thing a kernel can refuse.
  The listener is disabled in test (`hrana_server: false`), so this boots a real Bandit listener with
  the same options on an ephemeral port and connects to it.
  """
  use ExUnit.Case, async: false

  defmodule EchoPlug do
    @behaviour Plug
    def init(o), do: o
    def call(conn, _), do: Plug.Conn.send_resp(conn, 200, "ok")
  end

  test "the configured transport options boot a real listener and accept a connection" do
    opts = Fathom.Application.hrana_transport_options()

    assert Keyword.fetch!(opts, :num_listen_sockets) >= 1
    assert Keyword.fetch!(opts, :transport_options)[:backlog] == 4096

    # Bind a concrete port: with several listen sockets, port 0 would give each a different
    # ephemeral port and there would be nothing single to connect to.
    port = 49_000 + :erlang.phash2(self(), 500)

    start_supervised!(
      {Bandit,
       [
         plug: EchoPlug,
         scheme: :http,
         port: port,
         ip: {127, 0, 0, 1},
         thousand_island_options: opts
       ]}
    )

    assert {:ok, sock} = :gen_tcp.connect(~c"127.0.0.1", port, [:binary, active: false], 2_000)
    :ok = :gen_tcp.send(sock, "GET / HTTP/1.1\r\nHost: localhost\r\n\r\n")
    assert {:ok, resp} = :gen_tcp.recv(sock, 0, 2_000)
    assert resp =~ "200"
    :gen_tcp.close(sock)
  end

  # Expert review 2026-07-24 #16: Bandit defaults `compress: true`, so any client advertising
  # accept-encoding (reqwest, undici/fetch, requests/httpx — all by default) made every Hrana
  # pipeline response pay a full zlib context cycle, init-dominated at kilobyte JSON sizes, for what
  # is usually a datacenter LAN hop to the LB.
  #
  # This cost is absent from every measurement in the repo: Filo.Client sends only `content-type`,
  # so neither chaos driver ever advertised an encoding and the whole tpc-fleet / hotspots corpus
  # measured the UNCOMPRESSED path. Asking for gzip explicitly is the only way to see it.
  test "the listener does not compress, even when the client asks for it" do
    port = 49_600 + :erlang.phash2(self(), 300)

    start_supervised!(
      {Bandit,
       [
         plug: EchoPlug,
         scheme: :http,
         port: port,
         ip: {127, 0, 0, 1},
         thousand_island_options: Fathom.Application.hrana_transport_options(),
         http_options: Fathom.Application.hrana_http_options()
       ]}
    )

    assert {:ok, sock} = :gen_tcp.connect(~c"127.0.0.1", port, [:binary, active: false], 2_000)

    :ok =
      :gen_tcp.send(
        sock,
        "GET / HTTP/1.1\r\nHost: localhost\r\naccept-encoding: gzip\r\n\r\n"
      )

    assert {:ok, resp} = :gen_tcp.recv(sock, 0, 2_000)
    :gen_tcp.close(sock)

    refute String.downcase(resp) =~ "content-encoding: gzip",
           "the node compressed a response despite compress: false — compression belongs on the " <>
             "LB, where gzip_min_length can skip the small replies Bandit has no knob for"
  end

  test "setting :hrana_listen_sockets to 1 drops reuseport for platforms that refuse it" do
    prev = Application.get_env(:fathom, :hrana_listen_sockets)
    Application.put_env(:fathom, :hrana_listen_sockets, 1)

    on_exit(fn ->
      if is_nil(prev),
        do: Application.delete_env(:fathom, :hrana_listen_sockets),
        else: Application.put_env(:fathom, :hrana_listen_sockets, prev)
    end)

    opts = Fathom.Application.hrana_transport_options()

    assert Keyword.fetch!(opts, :num_listen_sockets) == 1

    refute Keyword.has_key?(opts[:transport_options], :reuseport),
           "the single-socket fallback must not pass reuseport — ThousandIsland FAILS STARTUP " <>
             "when the platform refuses it, so this is the escape hatch from a boot loop"
  end
end
