defmodule Fathom.Shard.WalQuorumBenchTest do
  @moduledoc """
  **Gate 2** for Phase 2 A2 — what a quorum ack costs the write path.
  See `docs/a2-quorum-replication.md`.

  Gate 1 proved the mechanism works in both directions. This one asks whether it is *affordable*,
  because a quorum ack puts a network hop on the commit path that does not exist today. The number
  it must be judged against is fathom's measured per-request round trip, **`hrana_rt_us` ≈ 127 µs**
  (`scripts/perf_history.jsonl`, commit 5358099, MIX_ENV=prod).

  ## What is measured, and what is NOT

  Measured here, on loopback:

    * **Follower apply cost** — appending a frame-sized delta to a follower's `-wal`, with and
      without `fdatasync`. That is a genuine design fork: Waterpark acks from **RAM** (it touches
      no disk at all), so a durable follower ack is fathom being *stricter* than the reference
      architecture, and the cost of that choice should be a number rather than a preference.
    * **Quorum wait shape** — 2-of-4 against 4-of-4 over real TCP. This quantifies the
      `Q < N` argument in the design doc rather than leaving it as reasoning.

  **NOT measured: real inter-node latency.** Loopback RTT is tens of microseconds; a same-AZ hop is
  ~0.1–0.25 ms and cross-AZ ~0.5–1.5 ms. So the numbers below are a **floor**, isolating the
  transport, syscall and coordination costs from the network. Deployment cost is
  `floor + one RTT to the 2nd-fastest follower`. Sweeping the RTT term is a rig job with toxiproxy,
  exactly as `deploy/chaos/chaos.sh latency-cost` already does for the S3 paths — do not guess it
  here, and do not read these numbers as a deployment estimate.

  Tagged `:bench`, excluded by default. Run with:

      mix test --include bench test/fathom/shard/wal_quorum_bench_test.exs
  """
  use ExUnit.Case, async: false

  @moduletag :bench

  # One WAL frame: 24-byte frame header + a 4 KiB page. The realistic unit A2 ships.
  @frame_bytes 24 + 4096
  @samples 200
  @followers 4
  # A straggler's extra delay. Stands in for one follower on a worse network path, a GC pause, or a
  # node under load — the situation a quorum exists to absorb.
  @lag_ms 5

  # ---------------------------------------------------------------------------------------------
  # a follower: accept a framed delta, append it, optionally fsync, ack
  # ---------------------------------------------------------------------------------------------

  defp start_follower(path, fsync?, lag_ms) do
    {:ok, lsock} =
      :gen_tcp.listen(0, [:binary, packet: 4, active: false, reuseaddr: true, nodelay: true])

    {:ok, port} = :inet.port(lsock)
    test = self()

    pid =
      spawn(fn ->
        {:ok, sock} = :gen_tcp.accept(lsock)
        send(test, :accepted)
        {:ok, fd} = :file.open(path, [:append, :raw, :binary])
        serve(sock, fd, fsync?, lag_ms)
        :file.close(fd)
        :gen_tcp.close(sock)
        :gen_tcp.close(lsock)
      end)

    {port, pid}
  end

  defp serve(sock, fd, fsync?, lag_ms) do
    case :gen_tcp.recv(sock, 0) do
      {:ok, delta} ->
        :ok = :file.write(fd, delta)
        # The durability fork: an ack after fdatasync survives the follower's node dying; an ack
        # before it is Waterpark's model, where durability is replica COUNT rather than disk.
        if fsync?, do: :ok = :file.datasync(fd)

        # A straggler. This is the ONLY thing that makes a quorum measurable — see the moduledoc.
        if lag_ms > 0, do: Process.sleep(lag_ms)
        :ok = :gen_tcp.send(sock, "a")
        serve(sock, fd, fsync?, lag_ms)

      {:error, _closed} ->
        :ok
    end
  end

  defp connect!(port) do
    {:ok, sock} =
      :gen_tcp.connect(~c"127.0.0.1", port, [:binary, packet: 4, active: true, nodelay: true])

    assert_receive :accepted, 1000
    sock
  end

  # Send to every follower, return once `quorum` of them have acked. The remaining acks arrive
  # later and are drained before the next sample so they cannot be miscounted.
  defp ship_and_wait(socks, payload, quorum) do
    for s <- socks, do: :ok = :gen_tcp.send(s, payload)
    await(quorum)
  end

  defp await(0), do: :ok

  defp await(n) do
    receive do
      {:tcp, _, _} -> await(n - 1)
    after
      5000 -> flunk("a follower never acked — this measured nothing")
    end
  end

  defp drain(0), do: :ok

  defp drain(n) do
    receive do
      {:tcp, _, _} -> drain(n - 1)
    after
      5000 -> flunk("stragglers never arrived; the next sample would miscount")
    end
  end

  defp p50(list) do
    sorted = Enum.sort(list)
    Enum.at(sorted, div(length(sorted), 2))
  end

  # `stragglers` — how many of the @followers ack `@lag_ms` late. Everything else is identical.
  defp measure(quorum, fsync?, stragglers \\ 0) do
    dir = Path.join(System.tmp_dir!(), "fathom_q_#{System.unique_integer([:positive])}")
    File.mkdir_p!(dir)

    followers =
      for i <- 1..@followers do
        lag = if i > @followers - stragglers, do: @lag_ms, else: 0
        start_follower(Path.join(dir, "f#{i}.db-wal"), fsync?, lag)
      end

    socks = for {port, _} <- followers, do: connect!(port)
    payload = :crypto.strong_rand_bytes(@frame_bytes)

    # Warm the sockets and the page cache so the first sample isn't the outlier.
    for _ <- 1..20 do
      ship_and_wait(socks, payload, @followers)
    end

    samples =
      for _ <- 1..@samples do
        t0 = System.monotonic_time(:microsecond)
        ship_and_wait(socks, payload, quorum)
        us = System.monotonic_time(:microsecond) - t0
        drain(@followers - quorum)
        us
      end

    for s <- socks, do: :gen_tcp.close(s)
    File.rm_rf(dir)

    p50(samples)
  end

  test "a quorum ack's local floor, against the 127µs hrana_rt_us baseline" do
    q2_ram = measure(2, false)
    q4_ram = measure(4, false)
    q2_sync = measure(2, true)
    q4_sync = measure(4, true)

    # The scenario a quorum EXISTS for: two of the four followers are slow.
    q2_lag = measure(2, false, 2)
    q4_lag = measure(4, false, 2)

    IO.puts("""

    === A2 gate 2 — quorum ack floor (loopback; NO inter-node latency) ===
      baseline hrana_rt_us (measured, prod)     127 µs

      all four followers healthy:
        2-of-4, ack from RAM (Waterpark parity) #{q2_ram} µs
        4-of-4, ack from RAM                    #{q4_ram} µs
        2-of-4, ack after fdatasync             #{q2_sync} µs
        4-of-4, ack after fdatasync             #{q4_sync} µs

      TWO of four stragglers, +#{@lag_ms}ms each (what quorum is FOR):
        2-of-4, ack from RAM                    #{q2_lag} µs
        4-of-4, ack from RAM                    #{q4_lag} µs

      Deployment cost = these + ONE RTT to the 2nd-fastest follower.
      Same-AZ ~0.1-0.25ms, cross-AZ ~0.5-1.5ms — sweep it on the rig, do not guess it.
    """)

    # Loose ceilings only. These exist so the file fails loudly if the transport or the fsync path
    # regresses by an order of magnitude, not to police noise on a contended dev box.
    assert q2_ram < 20_000, "2-of-4 RAM ack floor #{q2_ram}µs exceeded the 20ms ceiling"
    assert q2_sync < 100_000, "2-of-4 durable ack floor #{q2_sync}µs exceeded the 100ms ceiling"

    # THE design assertion, measured rather than argued. With four healthy loopback followers the
    # acks land within noise of each other and 2-of-4 vs 4-of-4 is unresolvable — an earlier version
    # of this file asserted an ordering there and failed, because there was no straggler to absorb.
    # Quorum buys nothing when every replica is equally fast; it buys everything when one is not.
    assert q2_lag < q4_lag / 2,
           "with two stragglers, 2-of-4 (#{q2_lag}µs) should be far below 4-of-4 (#{q4_lag}µs) — " <>
             "if it is not, the harness is not measuring the order statistic it claims to"

    # And the quorum should barely notice them, since two healthy followers still answer fast.
    assert q2_lag < @lag_ms * 1000,
           "2-of-4 with two stragglers took #{q2_lag}µs, i.e. it waited for a slow follower " <>
             "instead of returning on the two fast ones"
  end
end
