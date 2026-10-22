defmodule Fathom.Shard.Replication.PushStatCostTest do
  @moduledoc """
  The per-push stat micro-costs (expert review 2026-10-21 r7 #25), pinned by tracing the process
  that does the work.

    * `:file.read_file_info/2` defaults to `time: :local`, which converts atime, mtime and ctime to
      local time on every stat (3 `erlang:universaltime_to_localtime/1` calls, each taking the libc
      tz lock). The replication stats read only size and inode: one per Session commit
      (`Wal.read/1`'s stat) and one or two per follower append push. Traced at 15 conversions per
      replicated commit fleet-wide.
    * The follower built and validated the same WAL path twice per append push (`wal_path/2`: an
      ETS directory lookup, the shard-id assertion, a `Path.join`).

  A hygiene guard rather than a correctness test: the cost is ~2-4 us per commit, below what the
  bench can resolve, so these count calls instead of timing them.
  """
  use ExUnit.Case, async: false

  alias Fathom.Shard.Replication.{Follower, Protocol, Wal}

  setup do
    name = :"pushstat_#{System.unique_integer([:positive])}"
    dir = Path.join(System.tmp_dir!(), to_string(name))
    start_supervised!({Follower, name: name, port: 0, dir: dir}, id: name)
    on_exit(fn -> File.rm_rf(dir) end)
    %{name: name, dir: dir, id: "pushstat-#{System.unique_integer([:positive])}"}
  end

  test "an append push converts no timestamps and builds its WAL path once", ctx do
    %{name: name, id: id} = ctx
    File.write!(Follower.db_path(name, id), "the existing db")
    File.write!(Follower.wal_path(name, id), "")
    :ok = Follower.seed(name, id, 1, 0, 0, 0)

    {:ok, l} = :gen_tcp.listen(0, [:binary, packet: 4, active: false])
    {:ok, port} = :inet.port(l)
    {:ok, client} = :gen_tcp.connect(~c"127.0.0.1", port, [:binary, packet: 4, active: false])
    {:ok, server} = :gen_tcp.accept(l)
    :gen_tcp.close(l)

    worker = Follower.spawn_worker(server, name, nil, self(), :atomics.new(1, []))
    Process.unlink(worker)
    on_exit(fn -> Process.exit(worker, :kill) end)

    push = fn offset, payload ->
      %Protocol.Push{
        shard_id: id,
        epoch: 1,
        wal_gen: 0,
        salt1: 0,
        offset: offset,
        payload: payload
      }
    end

    # Warm the held fd first, so the traced push is the steady-state hit path.
    send(worker, {:frame, push.(0, "frames"), 1})
    assert_receive {:frame_done, 1}, 5_000
    assert {:ok, {:ack, ^id, 6}} = recv(client)

    calls =
      trace_calls(
        worker,
        [{:erlang, :universaltime_to_localtime, 1}, {Follower, :wal_path, 2}],
        fn ->
          send(worker, {:frame, push.(6, "more"), 2})
          assert_receive {:frame_done, 2}, 5_000
        end
      )

    assert {:ok, {:ack, ^id, 10}} = recv(client), "fixture: the traced push was not applied"

    assert count(calls, :universaltime_to_localtime) == 0,
           "an append push converted file timestamps to local time: #{inspect(calls)}"

    assert count(calls, :wal_path) == 1,
           "an append push built its WAL path #{count(calls, :wal_path)} times"
  end

  test "Wal.read/1's stat converts no timestamps", %{dir: dir} do
    path = Path.join(dir, "short.db-wal")
    File.write!(path, "short")
    test = self()

    reader =
      spawn_link(fn ->
        receive do
          :go -> send(test, {:read, Wal.read(path)})
        end

        # Alive until untraced: `:erlang.trace/3` on a dead pid raises.
        receive do
          :stop -> :ok
        end
      end)

    on_exit(fn -> send(reader, :stop) end)

    calls =
      trace_calls(reader, [{:erlang, :universaltime_to_localtime, 1}], fn ->
        send(reader, :go)
        assert_receive {:read, {:ok, :empty}}, 5_000
      end)

    assert calls == [], "Wal.read/1 converted file timestamps to local time: #{inspect(calls)}"
  end

  defp recv(client) do
    {:ok, bytes} = :gen_tcp.recv(client, 0, 5_000)
    Protocol.decode(bytes)
  end

  defp count(calls, fun), do: Enum.count(calls, &match?({_, ^fun, _}, &1))

  # The calls `pid` makes to `mfas` while `fun` runs, as `{module, function, args}`.
  defp trace_calls(pid, mfas, fun) do
    for mfa <- mfas, do: :erlang.trace_pattern(mfa, true, [:local])
    :erlang.trace(pid, true, [:call])

    try do
      fun.()
    after
      :erlang.trace(pid, false, [:call])
      for mfa <- mfas, do: :erlang.trace_pattern(mfa, false, [:local])
    end

    ref = :erlang.trace_delivered(pid)
    assert_receive {:trace_delivered, ^pid, ^ref}, 5_000

    # Trace patterns are VM-global: a pattern another test left set (e.g. on `:file`) also reports
    # this process's calls to it, so keep only the functions asked for.
    wanted = MapSet.new(mfas)
    Enum.filter(collect([]), fn {m, f, args} -> MapSet.member?(wanted, {m, f, length(args)}) end)
  end

  defp collect(acc) do
    receive do
      {:trace, _pid, :call, mfa} -> collect([mfa | acc])
    after
      0 -> Enum.reverse(acc)
    end
  end
end
