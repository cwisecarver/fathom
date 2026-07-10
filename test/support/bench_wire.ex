defmodule Fathom.Bench.Wire do
  @moduledoc """
  The **wire benches** (Phase 1, docs/tpc-benchmark-plan.md): metrics measured through the
  full Hrana WebSocket path — a real `Fathom.Bench.HranaClient` (Mint.WebSocket) → Filo's
  `Filo.Socket` → `Fathom.ShardExecutor` → the shard → back. This is what a real client
  (django-libsql) actually pays, vs the in-process `Fathom.Bench` metrics which stop at
  `ShardExecutor.execute`.

  Test-env only (it uses the dev/test `mint_web_socket` client), so it never runs in the
  prod per-commit gate; it runs via `MIX_ENV=test mix fathom.wire_bench`. The numbers are the
  wire *software* cost (localhost — µs link, no bandwidth-delay/TLS/LB hop), not a
  cross-network RTT (the chaos rig gives that).

  Assumes the shard tree + Local storage are already up — true under `mix test` (the app is
  started) and under the wire-bench task (which runs `app.start` first).
  """

  alias Fathom.Bench.HranaClient

  @hrana_rt_samples 200

  @doc """
  `hrana_rt_us` — median µs of a warm-stream `SELECT 1` round-trip over the wire. A read that
  issues no fsync, so it isolates the software wire cost (WS framing + `Filo.Value` decode,
  `Filo.Socket` routing, `Request.handle`, `ShardExecutor.execute`, response encode) with no
  storage noise — stable enough to gate.
  """
  @spec hrana_rt(keyword()) :: float()
  def hrana_rt(opts \\ []) do
    samples = Keyword.get(opts, :hrana_rt_samples, @hrana_rt_samples)

    with_listener(fn port ->
      shard = uniq("wire_rt")

      try do
        {:ok, c} = HranaClient.connect(port, shard)
        # Warm-up (opens the shard + primes the stream) — not timed.
        {:ok, c, _} = HranaClient.execute(c, "SELECT 1")

        {c, us} =
          Enum.reduce(1..samples, {c, []}, fn _, {c, acc} ->
            {t, {:ok, c, _}} = :timer.tc(fn -> HranaClient.execute(c, "SELECT 1") end)
            {c, [t | acc]}
          end)

        HranaClient.close(c)
        p50(us)
      after
        rm_shard(shard)
      end
    end)
  end

  # --- shared harness ------------------------------------------------------

  defp with_listener(fun) do
    {:ok, sup, port} = HranaClient.start_listener()

    try do
      fun.(port)
    after
      HranaClient.stop_listener(sup)
    end
  end

  defp uniq(prefix), do: "#{prefix}_#{System.unique_integer([:positive])}"

  defp rm_shard(id) do
    for dir <- ["fathom_shards", "fathom_remote_test"], s <- ["", "-wal", "-shm"] do
      File.rm(Path.join([System.tmp_dir!(), dir, "#{id}.db"]) <> s)
    end
  end

  # Interpolating p50 (matches the in-process bench's method; kept tiny + local to avoid a
  # cross-module dependency for a one-liner).
  defp p50([]), do: 0.0

  defp p50(samples) do
    sorted = Enum.sort(samples)
    n = length(sorted)
    rank = 0.5 * (n - 1)
    lo = trunc(rank)
    hi = min(lo + 1, n - 1)
    frac = rank - lo
    (Enum.at(sorted, lo) * (1 - frac) + Enum.at(sorted, hi) * frac) / 1.0
  end
end
