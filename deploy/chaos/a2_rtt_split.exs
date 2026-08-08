# A2 gate 2, follow-up: ASYMMETRIC follower placement.
#
# The uniform sweep found 2-of-4 ≈ 4-of-4 at every injected latency — because all four followers
# were given the SAME latency, so there is no straggler and a quorum has nothing to skip. That
# matches the loopback finding: quorum buys nothing when replicas are uniform.
#
# The deployment question that actually follows is placement: if TWO followers are near and TWO are
# far, does 2-of-4 ack at the NEAR latency while still holding four copies? If yes, the placement
# rule is "keep a quorum close, put the rest far for failure-domain spread" rather than "spread all
# four evenly", and Waterpark's 4-DC layout is doing exactly that relative to a primary's own DC.

frame_bytes = 24 + 4096
samples = 60
base_follower_port = 21200
base_proxy_port = 21100
toxi = "http://localhost:8475"

# {near_ms, far_ms} — followers 0,1 near; followers 2,3 far.
scenarios = [{0, 0}, {0, 30}, {0, 60}, {10, 60}]

defmodule F do
  def start(port, dir) do
    {:ok, ls} =
      :gen_tcp.listen(port, [:binary, packet: 4, active: false, reuseaddr: true, nodelay: true])

    spawn(fn -> loop(ls, Path.join(dir, "f#{port}.wal")) end)
    ls
  end

  defp loop(ls, path) do
    case :gen_tcp.accept(ls) do
      {:ok, s} ->
        spawn(fn ->
          {:ok, fd} = :file.open(path, [:append, :raw, :binary])
          serve(s, fd)
          :file.close(fd)
        end)

        loop(ls, path)

      {:error, :closed} ->
        :ok
    end
  end

  defp serve(s, fd) do
    case :gen_tcp.recv(s, 0) do
      {:ok, d} ->
        :ok = :file.write(fd, d)
        :ok = :gen_tcp.send(s, "a")
        serve(s, fd)

      {:error, _} ->
        :ok
    end
  end
end

defmodule S do
  def setup(toxi, lats, bp, bf) do
    lats
    |> Enum.with_index()
    |> Enum.each(fn {lat, i} ->
      name = "a2f#{i}"
      Req.delete(url: "#{toxi}/proxies/#{name}", retry: false)

      {:ok, %{status: st}} =
        Req.post(
          url: "#{toxi}/proxies",
          json: %{
            name: name,
            listen: "0.0.0.0:#{bp + i}",
            upstream: "host.docker.internal:#{bf + i}",
            enabled: true
          },
          retry: false
        )

      if st not in [200, 201], do: raise("proxy create failed: #{st}")

      if lat > 0 do
        for stream <- ["upstream", "downstream"] do
          {:ok, %{status: st2}} =
            Req.post(
              url: "#{toxi}/proxies/#{name}/toxics",
              json: %{
                name: "lat_#{stream}",
                type: "latency",
                stream: stream,
                attributes: %{latency: lat, jitter: 0}
              },
              retry: false
            )

          if st2 not in [200, 201], do: raise("toxic create failed: #{st2}")
        end
      end
    end)
  end

  def conns(bp, n) do
    for i <- 0..(n - 1) do
      {:ok, s} =
        :gen_tcp.connect(~c"127.0.0.1", bp + i, [:binary, packet: 4, active: true, nodelay: true])

      s
    end
  end

  def ship(socks, payload, q) do
    for s <- socks, do: :ok = :gen_tcp.send(s, payload)
    await(q)
  end

  defp await(0), do: :ok

  defp await(n) do
    receive do
      {:tcp, _, _} -> await(n - 1)
    after
      30_000 -> raise "no ack"
    end
  end

  def drain(0), do: :ok

  def drain(n) do
    receive do
      {:tcp, _, _} -> drain(n - 1)
    after
      30_000 -> raise "no straggler"
    end
  end

  def p50(l), do: Enum.at(Enum.sort(l), div(length(l), 2))
end

dir = Path.join(System.tmp_dir!(), "a2split_#{System.unique_integer([:positive])}")
File.mkdir_p!(dir)
for i <- 0..3, do: F.start(base_follower_port + i, dir)
payload = :crypto.strong_rand_bytes(frame_bytes)

IO.puts("\n=== A2 gate 2 — ASYMMETRIC placement (2 near / 2 far) ===")
IO.puts("baseline hrana_rt_us (measured, prod) = 127 µs\n")

IO.puts(
  String.pad_trailing("near", 10) <>
    String.pad_trailing("far", 10) <>
    String.pad_trailing("2-of-4", 14) <> String.pad_trailing("4-of-4", 14) <> "cost of Q=N"
)

for {near, far} <- scenarios do
  S.setup(toxi, [near, near, far, far], base_proxy_port, base_follower_port)
  socks = S.conns(base_proxy_port, 4)
  for _ <- 1..5, do: S.ship(socks, payload, 4)

  m = fn q ->
    S.p50(
      for _ <- 1..samples do
        t0 = System.monotonic_time(:microsecond)
        S.ship(socks, payload, q)
        us = System.monotonic_time(:microsecond) - t0
        S.drain(4 - q)
        us
      end
    )
  end

  q2 = m.(2)
  q4 = m.(4)
  for s <- socks, do: :gen_tcp.close(s)

  IO.puts(
    String.pad_trailing("#{near} ms", 10) <>
      String.pad_trailing("#{far} ms", 10) <>
      String.pad_trailing("#{q2} µs", 14) <>
      String.pad_trailing("#{q4} µs", 14) <>
      "#{Float.round(q4 / max(q2, 1), 2)}x"
  )
end

File.rm_rf(dir)
IO.puts("\nsamples per cell: #{samples}; payload: #{frame_bytes} B; latencies are ONE-WAY")
