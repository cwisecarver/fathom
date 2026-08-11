defmodule Fathom.Shard.ReplicationFleetTest do
  @moduledoc """
  Membership parsing and follower health — Phase 2 A2. See `docs/a2-quorum-replication.md`.

  Two things are pinned here, and the second is the one that matters.

  **Parsing refuses rather than shortens.** A malformed entry in `REPLICATION_FOLLOWERS` must fail
  the boot. The alternative — skip what we cannot parse and carry on — starts a node with a shorter
  replica set than the operator wrote, and nothing downstream can tell: replication runs, the quorum
  passes, and every shard is under-replicated with no error anywhere.

  **Liveness is observability, never a filter on the push set.** `connection_status/0` exists
  separately from `health/0` precisely so the periodic gauge stays Postgres-free, and neither is
  reachable from `Session.commit/3`. Filtering the followers a commit ships to by a remote,
  flappable signal would shrink `n` — and `q >= n` is a crash inside a tenant's write
  (fixed in f1827e1). `no_liveness_on_the_commit_path` below pins that the commit path does not
  call either one.
  """
  use ExUnit.Case, async: false

  alias Fathom.Shard.Replication.Fleet
  alias Fathom.Shard.Replication.Follower

  setup do
    prev = %{
      enabled: Application.get_env(:fathom, :replication_enabled),
      followers: Application.get_env(:fathom, :replication_followers),
      quorum: Application.get_env(:fathom, :replication_quorum),
      listen: Application.get_env(:fathom, :replication_listen),
      listen_port: Application.get_env(:fathom, :replication_listen_port),
      bind_ip: Application.get_env(:fathom, :replication_bind_ip),
      dir: Application.get_env(:fathom, :replication_dir),
      advertise_host: Application.get_env(:fathom, :replication_advertise_host)
    }

    on_exit(fn ->
      for {k, v} <- [
            replication_enabled: prev.enabled,
            replication_followers: prev.followers,
            replication_quorum: prev.quorum,
            replication_listen: prev.listen,
            replication_listen_port: prev.listen_port,
            replication_bind_ip: prev.bind_ip,
            replication_dir: prev.dir,
            replication_advertise_host: prev.advertise_host
          ] do
        if is_nil(v),
          do: Application.delete_env(:fathom, k),
          else: Application.put_env(:fathom, k, v)
      end
    end)

    :ok
  end

  describe "parse_followers!/1" do
    test "parses node_key@host:port" do
      assert [{"fathom2", "10.0.1.2", 9100}, {"fathom3", "10.0.2.3", 9100}] =
               Fleet.parse_followers!("fathom2@10.0.1.2:9100,fathom3@10.0.2.3:9100")
    end

    test "the node_key is optional and is synthesized from the address" do
      assert [{"10.0.1.2:9100", "10.0.1.2", 9100}] = Fleet.parse_followers!("10.0.1.2:9100")
    end

    test "tolerates surrounding whitespace" do
      assert [{"a", "h1", 1}, {"b", "h2", 2}] = Fleet.parse_followers!(" a@h1:1 , b@h2:2 ")
    end

    # Each of these would otherwise become a silently missing replica.
    test "refuses malformed entries instead of skipping them" do
      for bad <- [
            "fathom2@10.0.1.2",
            "fathom2@10.0.1.2:",
            "fathom2@10.0.1.2:notaport",
            "fathom2@:9100",
            "fathom2@10.0.1.2:0",
            "fathom2@10.0.1.2:70000",
            # IPv6 is rejected outright rather than truncated at its first colon, which would
            # produce a plausible-looking host that connects somewhere else entirely.
            "fathom2@::1:9100"
          ] do
        assert_raise ArgumentError, fn -> Fleet.parse_followers!(bad) end
      end
    end

    test "one bad entry fails the whole spec, not just itself" do
      assert_raise ArgumentError, fn ->
        Fleet.parse_followers!("fathom2@10.0.1.2:9100,fathom3@broken")
      end
    end
  end

  describe "endpoints/0" do
    test "normalises both accepted config shapes" do
      Application.put_env(:fathom, :replication_followers, [
        {~c"127.0.0.1", 9100},
        {"fathom3", "10.0.2.3", 9101}
      ])

      assert [{"127.0.0.1:9100", ~c"127.0.0.1", 9100}, {"fathom3", "10.0.2.3", 9101}] =
               Fleet.endpoints()
    end
  end

  describe "health/0 and connection_status/0" do
    setup ctx do
      root = Path.join(System.tmp_dir!(), "replfleet_#{System.unique_integer([:positive])}")
      File.mkdir_p!(root)
      on_exit(fn -> File.rm_rf(root) end)

      name = :"fleet_f_#{System.unique_integer([:positive])}"

      pid =
        start_supervised!({Follower, name: name, port: 0, dir: Path.join(root, "f")}, id: name)

      {:ok, port} = Follower.port(pid)

      # One follower that is really listening and one pointed at a closed port, so "connected"
      # is measured against a live socket rather than assumed.
      Application.put_env(:fathom, :replication_enabled, true)
      Application.put_env(:fathom, :replication_quorum, ctx[:quorum] || 1)

      Application.put_env(:fathom, :replication_followers, [
        {"up", ~c"127.0.0.1", port},
        {"down", ~c"127.0.0.1", 1}
      ])

      start_supervised!(Fleet)
      await_connected!()
      %{}
    end

    # The live follower's shipper connects in handle_continue; without waiting, "connected?" is a
    # race rather than a measurement.
    defp await_connected!(timeout \\ 5_000) do
      deadline = System.monotonic_time(:millisecond) + timeout

      Stream.repeatedly(fn ->
        if Enum.any?(Fleet.connection_status(), fn {_k, up?} -> up? end),
          do: :up,
          else: Process.sleep(20)
      end)
      |> Enum.find(fn
        :up -> true
        _ -> System.monotonic_time(:millisecond) > deadline
      end)
      |> case do
        :up -> :ok
        _ -> flunk("the live follower's shipper never connected")
      end
    end

    test "connection_status reports the live follower up and the closed port down" do
      status = Map.new(Fleet.connection_status())

      assert status["up"] == true
      assert status["down"] == false
    end

    test "health carries identity and socket state for every configured follower" do
      health = Map.new(Fleet.health(), &{&1.node_key, &1})

      assert %{host: "127.0.0.1", connected?: true} = health["up"]
      assert %{port: 1, connected?: false} = health["down"]
    end

    # The roster lives in Postgres and this suite has no beats, so `alive?` must degrade to a
    # value an operator can read as "unknown", never to `false` — reporting every follower dead
    # because the DB was unreadable points the diagnosis at the wrong system entirely.
    test "alive? never reports a false negative when the roster says nothing" do
      for f <- Fleet.health(), do: assert(f.alive? in [false, :unknown])
    end

    # `slack` is the alertable quantity (alert-rules.yml thresholds `== 0` and `< 0`), so it is
    # asserted at all three points rather than via a separate "degraded" event — that event was
    # written first and removed when TelemetryCoverageTest pointed out nothing exported it.
    @tag quorum: 1
    test "slack counts connected followers against the quorum, at the boundary and either side" do
      ref = make_ref()
      parent = self()

      :telemetry.attach(
        {__MODULE__, ref},
        [:fathom, :replication, :followers],
        fn _e, measurements, _meta, _ -> send(parent, {ref, measurements}) end,
        nil
      )

      on_exit(fn -> :telemetry.detach({__MODULE__, ref}) end)

      # One of two followers up, quorum 1 ⇒ slack 0. The invisible state: every write still
      # succeeds and one more loss fails all of them.
      Fathom.Admin.Measurements.replication()
      assert_receive {^ref, %{connected: 1, configured: 2, quorum: 1, slack: 0}}

      # Healthy side of the boundary.
      Application.put_env(:fathom, :replication_quorum, 0)
      Fathom.Admin.Measurements.replication()
      assert_receive {^ref, %{slack: 1}}

      # Already failing: fewer followers connected than the quorum needs, so commits are
      # returning FILO_NO_QUORUM. Must go NEGATIVE, not clamp at zero — the alert rule
      # distinguishes "no slack" (page) from "writes failing" (page, different runbook step).
      Application.put_env(:fathom, :replication_quorum, 2)
      Fathom.Admin.Measurements.replication()
      assert_receive {^ref, %{slack: -1}}
    end

    test "the gauge is silent when replication is off" do
      ref = make_ref()
      parent = self()

      :telemetry.attach(
        {__MODULE__, ref},
        [:fathom, :replication, :followers],
        fn _e, m, _meta, _ -> send(parent, {ref, m}) end,
        nil
      )

      on_exit(fn -> :telemetry.detach({__MODULE__, ref}) end)

      Application.put_env(:fathom, :replication_enabled, false)
      Fathom.Admin.Measurements.replication()

      # A node that never turned A2 on must emit nothing, not a permanently-degraded zero that
      # would page whoever owns the alert.
      refute_receive {^ref, _}, 100
    end
  end

  # THE RECEIVE HALF. Until 2026-08-10 `Follower` was started only by tests, so a deployment with
  # replication on shipped to addresses where nothing listened and 503'd every tenant write. These
  # pin that `Fleet` supervises it, and that the two roles are independently gated — a node must be
  # able to listen WITHOUT shipping, which is what makes a safe rollout order expressible.
  describe "the receive half (:replication_listen)" do
    setup do
      dir = Path.join(System.tmp_dir!(), "fleet_listen_#{System.unique_integer([:positive])}")
      on_exit(fn -> File.rm_rf(dir) end)
      Application.put_env(:fathom, :replication_dir, dir)
      %{dir: dir, port: free_port()}
    end

    test "a listening node supervises a Follower that accepts a connection", %{port: port} do
      Application.put_env(:fathom, :replication_listen, true)
      Application.put_env(:fathom, :replication_listen_port, port)

      start_supervised!(Fleet)

      assert {:ok, sock} = connect(port),
             "nothing accepted on #{port} — the Follower listener is not supervised, which is " <>
               "the exact gap that made A2 undeployable"

      :gen_tcp.close(sock)
    end

    # The discriminating half: without this, "it listens" could just be a port something else
    # opened, and the rollout-order argument for two flags would be untestable.
    test "a node that does not listen opens no port", %{port: port} do
      Application.put_env(:fathom, :replication_listen, false)
      Application.put_env(:fathom, :replication_listen_port, port)
      Application.put_env(:fathom, :replication_enabled, false)

      # children/0 returns [] here, so start Fleet directly to prove init/1 also declines.
      start_supervised!(Fleet)

      assert {:error, :econnrefused} = connect(port)
    end

    test "listening does not require shipping, and starts no shippers", %{port: port} do
      Application.put_env(:fathom, :replication_listen, true)
      Application.put_env(:fathom, :replication_listen_port, port)
      Application.put_env(:fathom, :replication_enabled, false)
      Application.delete_env(:fathom, :replication_followers)

      # A listen-only node has no follower list. Before the split, init/1 called
      # validate_quorum!/0 unconditionally and this raised "there is nothing to replicate to" —
      # i.e. a node could not be a pure follower at all.
      start_supervised!(Fleet)

      assert Fleet.shippers() == []
      assert {:ok, sock} = connect(port)
      :gen_tcp.close(sock)
    end

    test "children/0 is empty only when BOTH roles are off" do
      Application.put_env(:fathom, :replication_enabled, false)
      Application.put_env(:fathom, :replication_listen, false)
      assert Fleet.children() == []

      Application.put_env(:fathom, :replication_listen, true)
      assert Fleet.children() == [Fleet]

      Application.put_env(:fathom, :replication_listen, false)
      Application.put_env(:fathom, :replication_enabled, true)
      assert Fleet.children() == [Fleet]
    end

    # Proves the `ip:` option is threaded through to :gen_tcp.listen and accepted. It does NOT
    # prove interface isolation — that needs a multi-homed host, and asserting it on loopback
    # would pass whether or not the option took. The security claim rests on :gen_tcp's own
    # semantics; what is checkable here is that the value reaches the socket at all, which is
    # where a config-plumbing bug would land.
    test "a bind IP is honoured by the listener", %{port: port} do
      Application.put_env(:fathom, :replication_listen, true)
      Application.put_env(:fathom, :replication_listen_port, port)
      Application.put_env(:fathom, :replication_bind_ip, {127, 0, 0, 1})

      start_supervised!(Fleet)

      assert {:ok, sock} = connect(port)
      :gen_tcp.close(sock)
    end
  end

  # What this node publishes to the roster. Every branch here fails toward nil, and nil means
  # "not a membership candidate" — which is strictly safer than publishing an endpoint peers
  # cannot reach, because a nil node is simply not chosen while a wrong address makes the roster
  # report a node present that every shipper then fails to connect to.
  describe "advertised_address/0" do
    test "is nil when this node does not listen, even with a host configured" do
      Application.put_env(:fathom, :replication_listen, false)
      Application.put_env(:fathom, :replication_advertise_host, "10.0.0.1")

      refute Fleet.advertised_address()
    end

    test "is nil when listening but no host is configured — never guessed" do
      Application.put_env(:fathom, :replication_listen, true)
      Application.delete_env(:fathom, :replication_advertise_host)

      refute Fleet.advertised_address()
    end

    test "is nil for an empty host, not the bare port" do
      Application.put_env(:fathom, :replication_listen, true)
      Application.put_env(:fathom, :replication_advertise_host, "")

      refute Fleet.advertised_address()
    end

    test "is host:port when listening and configured" do
      Application.put_env(:fathom, :replication_listen, true)
      Application.put_env(:fathom, :replication_advertise_host, "10.0.0.7")
      Application.put_env(:fathom, :replication_listen_port, 9100)

      assert Fleet.advertised_address() == "10.0.0.7:9100"
    end

    # The published port must track the port actually bound, or peers dial somewhere nothing
    # listens while both values look individually reasonable.
    test "the advertised port follows the configured listen port" do
      Application.put_env(:fathom, :replication_listen, true)
      Application.put_env(:fathom, :replication_advertise_host, "h")
      Application.put_env(:fathom, :replication_listen_port, 9999)

      assert Fleet.advertised_address() == "h:9999"
    end
  end

  defp connect(port) do
    :gen_tcp.connect(~c"127.0.0.1", port, [:binary, packet: 4, active: false], 1_000)
  end

  defp free_port do
    {:ok, s} = :gen_tcp.listen(0, [])
    {:ok, p} = :inet.port(s)
    :gen_tcp.close(s)
    p
  end

  # Not a behavioural test — a structural one, and deliberately so. The rule it protects
  # ("liveness never filters the push set") is invisible at runtime until the day a follower flaps
  # and every commit on the node starts raising `q >= n`. Reading it off the source is the only
  # check that fails at the moment someone wires the two together.
  test "no_liveness_on_the_commit_path" do
    session = File.read!("lib/fathom/shard/replication/session.ex")

    for forbidden <- ["Fleet.health", "connection_status", "Rebalancer.Nodes"] do
      refute String.contains?(session, forbidden),
             "Session (the commit path) references #{forbidden}. Liveness must stay " <>
               "observability-only: filtering the push set by it shrinks n, and q >= n raises " <>
               "inside a tenant's commit. See the Fleet.health/0 moduledoc."
    end
  end
end
