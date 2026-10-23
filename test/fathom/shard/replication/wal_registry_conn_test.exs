defmodule Fathom.Shard.Replication.WalRegistryConnTest do
  @moduledoc """
  The WAL registry is reached through ONE long-lived `:memory:` connection per node (expert review
  2026-10-22 r8 #17), not a fresh connection + extension load per Session start and stop.

  The risk of sharing a handle is cross-talk: bind and step are separate NIF calls, so two Sessions
  sharing one prepared statement could run one's path with the other's delta. Each call prepares its
  own statement; this pins that concurrent registrations stay independent, plus a `:bench` ceiling
  on the per-pair cost the change exists to remove (~178 µs p50 measured before it).
  """
  use ExUnit.Case, async: false

  alias Fathom.Shard.Replication.Session

  setup do
    dir = Path.join(System.tmp_dir!(), "walreg_#{System.unique_integer([:positive])}")
    File.mkdir_p!(dir)
    on_exit(fn -> File.rm_rf(dir) end)

    unless match?({:ok, _}, Session.wal_registry(Path.join(dir, "probe.db"), 0)),
      do: flunk("precondition: the fathom_udf extension is not loadable in this build")

    %{dir: dir}
  end

  test "concurrent registrations through the shared handle stay independent", %{dir: dir} do
    paths =
      for i <- 1..64 do
        p = Path.join(dir, "s#{i}.db")
        File.write!(p, "")
        p
      end

    keys =
      paths
      |> Task.async_stream(
        fn p ->
          {:ok, key} = Session.wal_registry(p, 1)
          {:ok, 1} = Session.wal_registry(key, 0)
          key
        end,
        max_concurrency: 64,
        timeout: 10_000
      )
      |> Enum.map(fn {:ok, key} -> key end)

    assert length(Enum.uniq(keys)) == 64

    for key <- keys,
        do: assert({:ok, 1} = Session.wal_registry(key, 0), "#{key} lost or gained a count")

    keys
    |> Task.async_stream(fn key -> Session.wal_registry(key, -1) end,
      max_concurrency: 64,
      timeout: 10_000
    )
    |> Stream.run()

    for key <- keys, do: assert({:ok, 0} = Session.wal_registry(key, 0))
  end

  @tag :bench
  test "a register + release pair costs microseconds, not an extension load", %{dir: dir} do
    p = Path.join(dir, "bench.db")
    File.write!(p, "")
    {:ok, _} = Session.wal_registry(p, 0)

    times =
      for _ <- 1..200 do
        {us, _} =
          :timer.tc(fn ->
            {:ok, key} = Session.wal_registry(p, 1)
            {:ok, _} = Session.wal_registry(key, -1)
          end)

        us
      end

    p50 = times |> Enum.sort() |> Enum.at(100)
    IO.puts("wal registry register+release p50 #{p50} us (was ~178 us per fresh connection)")
    assert p50 < 60, "p50 #{p50} us: is the registry reopening a connection per call?"
  end
end
