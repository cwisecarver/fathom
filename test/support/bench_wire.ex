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
  alias Fathom.Shard.{Connection, Storage}
  alias Fathom.Shards

  @hrana_rt_samples 200
  @cold_open_wire_samples 30

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

  @doc """
  `cold_open_wire_p50_us` — median µs for a client to first-query a **cold** shard over the
  wire: a fresh WS connect whose `open_stream` triggers the cold `Shards.checkout` (pull from
  storage + coordinator start), then the first `execute`. The wire parallel to the in-process
  `Fathom.Bench.cold_open/1`; it adds the WS upgrade/hello + framing a real client pays on a
  new connection. Each sample seeds a fresh shard into storage and drops its local copy, so
  every open is genuinely cold.
  """
  @spec cold_open_wire(keyword()) :: float()
  def cold_open_wire(opts \\ []) do
    samples = Keyword.get(opts, :cold_open_wire_samples, @cold_open_wire_samples)

    with_listener(fn port ->
      # Warm code paths once (module/NIF/WS load), not timed.
      warm = uniq("wire_co_warm")
      seed_cold_shard(warm)
      cold_first_query(port, warm)
      teardown_cold(warm)

      1..samples
      |> Enum.map(fn _ ->
        id = uniq("wire_co")
        seed_cold_shard(id)
        {us, :ok} = :timer.tc(fn -> cold_first_query(port, id) end)
        teardown_cold(id)
        us
      end)
      |> p50()
    end)
  end

  # A fresh connection to a cold shard: connect (upgrade + hello + open_stream → cold checkout)
  # then the first query. Closes the stream; returns :ok.
  defp cold_first_query(port, shard) do
    {:ok, c} = HranaClient.connect(port, shard)
    {:ok, c, _} = HranaClient.execute(c, "SELECT 1")
    HranaClient.close(c)
    :ok
  end

  # Put a shard into storage but leave no local copy or coordinator, so the next open is a
  # genuine cold pull (mirrors Fathom.Bench.seed_storage_shard/1). Uses a `.seed` temp so the
  # coordinator's own `<id>.db` path stays absent.
  defp seed_cold_shard(id) do
    # The .seed is a throwaway local source for the Storage.flush PUT (the coordinator later
    # pulls object `id` to its own data dir), so any writable path works — don't depend on
    # :shard_data_dir, which is nil in test (the coordinator defaults it internally).
    tmp = Path.join(System.tmp_dir!(), "#{id}.seed")
    drop_local(tmp)
    {:ok, conn} = Connection.open(tmp)
    :ok = Connection.exec(conn, "CREATE TABLE t (id INTEGER PRIMARY KEY)")
    :ok = Connection.exec(conn, "INSERT INTO t DEFAULT VALUES")
    :ok = Connection.exec(conn, "PRAGMA wal_checkpoint(TRUNCATE)")
    Connection.close(conn)
    :ok = Storage.flush(id, tmp)
    drop_local(tmp)
  end

  # Stop the coordinator (flush + drop local + release lease) so nothing lingers between
  # samples, then best-effort remove the storage-side seed.
  defp teardown_cold(id) do
    Shards.drain(id, 5_000)
    rm_shard(id)
  end

  defp drop_local(path), do: Enum.each(["", "-wal", "-shm"], &File.rm(path <> &1))

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
