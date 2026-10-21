defmodule Fathom.Shard.RetainedBenchTest do
  @moduledoc """
  Reopen latency of a RETAINED copy vs a PULLED one (expert review 2026-10-20-2027 #22), in-process,
  against `Storage.Local` and the MD5-etag S3 stub (`Fathom.Test.S3EtagStore`, served over Req's
  in-process plug — no network RTT, so the S3 figure is the protocol + body cost only, the floor of
  what retention saves; a real S3 adds ~1 RTT + transfer per pull).

  Measures the coordinator's own `[:fathom, :shard, :cold_open]` duration, p50 over a few reopens of
  a ~4 MiB shard. `@tag :bench`: excluded from the default suite; run with `--only bench`.
  """
  use ExUnit.Case, async: false

  alias Fathom.{Shard, ShardExecutor, Shards}
  alias Fathom.Shard.{Retained, Storage}
  alias Fathom.Shard.Storage.S3
  alias Fathom.Test.S3EtagStore
  alias Filo.Stmt

  @moduletag :bench
  @rounds 9

  setup do
    prev =
      for k <- [:shard_retain_local_bytes, :shard_idle_ms, :shard_storage, S3],
          do: {k, Application.get_env(:fathom, k)}

    Application.put_env(:fathom, :shard_idle_ms, 600_000)

    on_exit(fn ->
      for {k, v} <- prev,
          do:
            if(is_nil(v),
              do: Application.delete_env(:fathom, k),
              else: Application.put_env(:fathom, k, v)
            )
    end)

    :ok
  end

  defp stmt(sql), do: %Stmt{sql: sql, args: []}

  defp seed!(id) do
    {:ok, c} = ShardExecutor.open(id)
    {:ok, _} = ShardExecutor.execute(c, stmt("CREATE TABLE t (v BLOB)"))

    for _ <- 1..String.to_integer(System.get_env("RETAIN_BENCH_BLOBS", "64")),
        do: {:ok, _} = ShardExecutor.execute(c, stmt("INSERT INTO t VALUES (randomblob(65536))"))

    :ok = ShardExecutor.close(c)
  end

  defp drop!(id) do
    {:ok, pid} = Shards.ensure(id)
    ref = Process.monitor(pid)
    :ok = Shards.drain(id, 10_000)
    assert_receive {:DOWN, ^ref, :process, ^pid, _}, 10_000
    flush_opens()
  end

  # `Shards.ensure/1` above may itself have opened the shard; that open is not the one measured.
  defp flush_opens do
    receive do
      {:cold_open, _, _} -> flush_opens()
    after
      0 -> :ok
    end
  end

  defp timed_open!(id) do
    {:ok, c} = ShardExecutor.open(id)
    {:ok, _} = ShardExecutor.execute(c, stmt("SELECT count(*) FROM t"))
    :ok = ShardExecutor.close(c)
    assert_receive {:cold_open, us, warm}, 5_000
    {us, warm}
  end

  defp p50(xs), do: xs |> Enum.sort() |> Enum.at(div(length(xs), 2))

  defp measure(id) do
    test = self()
    name = "retained-bench-#{id}"

    :telemetry.attach(
      name,
      [:fathom, :shard, :cold_open],
      fn _, %{duration: d}, %{shard_id: ^id, warm: w}, _ ->
        send(test, {:cold_open, System.convert_time_unit(d, :native, :microsecond), w})
      end,
      nil
    )

    try do
      seed!(id)
      drop!(id)

      retained =
        for _ <- 1..@rounds do
          Application.put_env(:fathom, :shard_retain_local_bytes, 1_073_741_824)
          drop!(id)
          assert File.exists?(Retained.marker(Shard.db_path(id)))
          {us, true} = timed_open!(id)
          us
        end

      pulled =
        for _ <- 1..@rounds do
          Application.put_env(:fathom, :shard_retain_local_bytes, 0)
          drop!(id)
          refute File.exists?(Shard.db_path(id))
          {us, false} = timed_open!(id)
          us
        end

      {p50(retained), p50(pulled)}
    after
      :telemetry.detach(name)
      drop!(id)
      for f <- Path.wildcard(Shard.db_path(id) <> "*"), do: File.rm(f)
      Storage.purge_shard(id)
    end
  end

  test "Storage.Local: a retained reopen beats a pulled one" do
    {r, p} = measure("retbenchl#{System.unique_integer([:positive])}")
    IO.puts("\n[retained bench] Local 4 MiB: retained p50 #{r} us, pulled p50 #{p} us")
    assert r - p < 0
  end

  test "S3 stub: a retained reopen beats a pulled one" do
    store = start_supervised!({Agent, fn -> S3EtagStore.initial(%{}) end})

    Application.put_env(:fathom, S3,
      bucket: "b",
      region: "us-east-1",
      access_key_id: "k",
      secret_access_key: "s",
      endpoint: "https://s3.example",
      path_style: true,
      req_plug: fn conn -> S3EtagStore.serve(conn, store) end
    )

    Application.put_env(:fathom, :shard_storage, S3)
    {r, p} = measure("retbenchs#{System.unique_integer([:positive])}")
    IO.puts("\n[retained bench] S3 stub 4 MiB: retained p50 #{r} us, pulled p50 #{p} us")
    assert r - p < 0
  end
end
