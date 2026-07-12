# Continuous Zipf-skewed hotspot load against THIS node's shard data path, for demoing the admin
# dashboard. Runs inside a serving node so the load, the ShardLoad/collector ETS, and the endpoint
# all share one BEAM — the collector sees the load and the dashboard reflects it live.
#
#   PHX_SERVER=1 SHARD_LOAD=true elixir -S mix run --no-halt scripts/hotspot_demo.exs
#
# Prints the collector snapshot (what the dashboard receives) every few seconds. Ctrl-C to stop;
# the ~200 demo shards live under System.tmp_dir!/fathom_shards + fathom_remote(_test).
alias Fathom.Admin.MetricsCollector
alias Fathom.ShardExecutor
alias Filo.Stmt

n = 200
s = 1.2
workers = 8

# Zipf(s) CDF over ranks 1..n, so rank 1 ("hot_1") is the hottest.
weights = for r <- 1..n, do: 1.0 / :math.pow(r, s)
total = Enum.sum(weights)
cdf = weights |> Enum.scan(&+/2) |> Enum.map(&(&1 / total)) |> List.to_tuple()

pick = fn ->
  u = :rand.uniform()
  Enum.find(1..n, n, &(elem(cdf, &1 - 1) >= u))
end

create = %Stmt{sql: "CREATE TABLE IF NOT EXISTS kv (k INTEGER PRIMARY KEY, v TEXT)", args: []}
insert = %Stmt{sql: "INSERT INTO kv (v) VALUES (?)", args: ["x"]}
select = %Stmt{sql: "SELECT count(*) FROM kv", args: []}

IO.puts("hotspot demo: #{workers} workers, Zipf(s=#{s}) over #{n} shards → /admin")

for _ <- 1..workers do
  spawn(fn ->
    Stream.repeatedly(fn ->
      shard = "hot_#{pick.()}"

      case ShardExecutor.open(shard) do
        {:ok, h} ->
          ShardExecutor.execute(h, create)
          # Bias writes to the head so the dirty/RPO panel also moves.
          if :rand.uniform() < 0.3, do: ShardExecutor.execute(h, insert)
          ShardExecutor.execute(h, select)
          ShardExecutor.close(h)

        _ ->
          :ok
      end

      Process.sleep(2)
    end)
    |> Stream.run()
  end)
end

# Print what the collector publishes to the dashboard, every 3s.
spawn(fn ->
  Stream.interval(3000)
  |> Stream.each(fn _ ->
    case MetricsCollector.snapshot().current do
      nil ->
        IO.puts("collector: warming up…")

      m ->
        hot =
          m.hot_shards
          |> Enum.take(5)
          |> Enum.map_join(" ", fn h -> "#{h.shard_id}=#{round(h.q_per_s)}" end)

        IO.puts(
          "QPS=#{round(m.node_qps)} p50=#{Float.round(m.query_p50_ms, 2)}ms " <>
            "p99=#{Float.round(m.query_p99_ms, 2)}ms open=#{m.open_shards} " <>
            "dirty=#{m.dirty_shards} rpo=#{round(m.oldest_rpo_ms)}ms | hot: #{hot}"
        )
    end
  end)
  |> Stream.run()
end)
