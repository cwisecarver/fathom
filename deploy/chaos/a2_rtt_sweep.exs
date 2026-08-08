# A2 gate 2, RTT sweep. Puts REAL injected network latency on the loopback floor measured by
# test/fathom/shard/wal_quorum_bench_test.exs.
#
# Topology: four follower listeners run here on the host (ports 21200..21203). Toxiproxy runs in a
# container with published ports 21100..21103, each proxying to one follower and carrying a
# `latency` toxic on BOTH streams — so a toxic of L ms means one-way L, round trip 2L. The
# primary connects to the proxies, ships a frame-sized payload to all four, and waits for a quorum.
#
# The fathom NODES are deliberately not involved: A2 has no transport yet, so there is nothing on
# them to measure. What the rig contributes here is toxiproxy, which is the same latency-injection
# tool `chaos.sh latency-cost` uses for the S3 paths.

frame_bytes = 24 + 4096
samples = 60
followers = 4
base_follower_port = 21200
base_proxy_port = 21100
toxi = "http://localhost:8475"
sweep = [0, 10, 30, 60]

defmodule Follower do
  def start(port, fsync?, dir) do
    {:ok, ls} =
      :gen_tcp.listen(port, [:binary, packet: 4, active: false, reuseaddr: true, nodelay: true])

    spawn(fn -> accept_loop(ls, fsync?, Path.join(dir, "f#{port}.wal")) end)
    ls
  end

  defp accept_loop(ls, fsync?, path) do
    case :gen_tcp.accept(ls) do
      {:ok, sock} ->
        spawn(fn ->
          {:ok, fd} = :file.open(path, [:append, :raw, :binary])
          serve(sock, fd, fsync?)
          :file.close(fd)
        end)

        accept_loop(ls, fsync?, path)

      {:error, :closed} ->
        :ok
    end
  end

  defp serve(sock, fd, fsync?) do
    case :gen_tcp.recv(sock, 0) do
      {:ok, delta} ->
        :ok = :file.write(fd, delta)
        if fsync?, do: :ok = :file.datasync(fd)
        :ok = :gen_tcp.send(sock, "a")
        serve(sock, fd, fsync?)

      {:error, _} ->
        :ok
    end
  end
end

defmodule Sweep do
  def reset_proxies(toxi, n, base_proxy, base_follower, latency_ms) do
    for i <- 0..(n - 1) do
      name = "a2f#{i}"
      Req.delete(url: "#{toxi}/proxies/#{name}", retry: false)

      {:ok, %{status: st}} =
        Req.post(
          url: "#{toxi}/proxies",
          json: %{
            name: name,
            listen: "0.0.0.0:#{base_proxy + i}",
            upstream: "host.docker.internal:#{base_follower + i}",
            enabled: true
          },
          retry: false
        )

      if st not in [200, 201], do: raise("proxy create failed: #{st}")

      if latency_ms > 0 do
        for stream <- ["upstream", "downstream"] do
          {:ok, %{status: st}} =
            Req.post(
              url: "#{toxi}/proxies/#{name}/toxics",
              json: %{
                name: "lat_#{stream}",
                type: "latency",
                stream: stream,
                attributes: %{latency: latency_ms, jitter: 0}
              },
              retry: false
            )

          if st not in [200, 201], do: raise("toxic create failed: #{st}")
        end
      end
    end
  end

  def connect_all(base_proxy, n) do
    for i <- 0..(n - 1) do
      {:ok, s} =
        :gen_tcp.connect(~c"127.0.0.1", base_proxy + i, [
          :binary,
          packet: 4,
          active: true,
          nodelay: true
        ])

      s
    end
  end

  def ship(socks, payload, quorum) do
    for s <- socks, do: :ok = :gen_tcp.send(s, payload)
    await(quorum)
  end

  defp await(0), do: :ok

  defp await(n) do
    receive do
      {:tcp, _, _} -> await(n - 1)
    after
      30_000 -> raise "a follower never acked — measured nothing"
    end
  end

  def drain(0), do: :ok

  def drain(n) do
    receive do
      {:tcp, _, _} -> drain(n - 1)
    after
      30_000 -> raise "stragglers never arrived"
    end
  end

  def p50(l) do
    s = Enum.sort(l)
    Enum.at(s, div(length(s), 2))
  end
end

dir = Path.join(System.tmp_dir!(), "a2rtt_#{System.unique_integer([:positive])}")
File.mkdir_p!(dir)
for i <- 0..(followers - 1), do: Follower.start(base_follower_port + i, false, dir)
payload = :crypto.strong_rand_bytes(frame_bytes)

IO.puts("\n=== A2 gate 2 — RTT sweep (toxiproxy, real TCP) ===")
IO.puts("baseline hrana_rt_us (measured, prod) = 127 µs\n")

IO.puts(
  String.pad_trailing("one-way", 10) <>
    String.pad_trailing("RTT", 10) <>
    String.pad_trailing("2-of-4", 14) <> String.pad_trailing("4-of-4", 14) <> "4-of-4 penalty"
)

results =
  for lat <- sweep do
    Sweep.reset_proxies(toxi, followers, base_proxy_port, base_follower_port, lat)
    socks = Sweep.connect_all(base_proxy_port, followers)

    # Warm: also proves the proxy path works before anything is timed.
    for _ <- 1..5, do: Sweep.ship(socks, payload, followers)

    measure = fn quorum ->
      Sweep.p50(
        for _ <- 1..samples do
          t0 = System.monotonic_time(:microsecond)
          Sweep.ship(socks, payload, quorum)
          us = System.monotonic_time(:microsecond) - t0
          Sweep.drain(followers - quorum)
          us
        end
      )
    end

    q2 = measure.(2)
    q4 = measure.(4)
    for s <- socks, do: :gen_tcp.close(s)

    IO.puts(
      String.pad_trailing("#{lat} ms", 10) <>
        String.pad_trailing("#{lat * 2} ms", 10) <>
        String.pad_trailing("#{q2} µs", 14) <>
        String.pad_trailing("#{q4} µs", 14) <>
        "#{Float.round(q4 / max(q2, 1), 2)}x"
    )

    {lat, q2, q4}
  end

File.rm_rf(dir)
IO.puts("\nsamples per cell: #{samples}; payload: #{frame_bytes} B (one WAL frame)")
IO.inspect(results, label: "raw", limit: :infinity)
