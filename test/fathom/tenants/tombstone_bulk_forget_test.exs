defmodule Fathom.Tenants.TombstoneBulkForgetTest do
  @moduledoc """
  A bulk tombstone load costs nothing per id that has no replica here (expert review 2026-10-21
  r7 #22).

  `Tombstones` learns every LIFETIME deletion at boot (directory full load plus the storage union),
  and each id is new to the boot, so its bulk hook ran for all of them. Since the lazy follower
  boot (2026-10-21 #5) no replica row is resident at boot, and since rv6 F5 the hook forgets
  unconditionally, so whenever the Follower was up first every historical tombstone cost a shard
  lock plus 6 unlink syscalls on files that almost never exist: 100k ids measured 13.5 s on macOS,
  inside the `Tombstones` GenServer, every boot on every node. The same 6 unlinks ran on every
  `state_of/2` of a tombstoned id with no files (`recover_shard/3`).

  The invariant pinned: with no row and no `.db`/`-wal`, neither path calls `Follower.forget/2` or
  unlinks anything; a replica that IS on disk is still erased by the bulk load (the hook is not a
  no-op, because the Follower's post-listen pass may run before the id is loaded).
  """
  use ExUnit.Case, async: false

  alias Fathom.Shard.Replication.Follower
  alias Fathom.Tenants.Tombstones

  setup do
    dir = Path.join(System.tmp_dir!(), "tombbulk_#{System.unique_integer([:positive])}")
    File.mkdir_p!(dir)
    on_exit(fn -> File.rm_rf(dir) end)

    pid = start_supervised!({Follower, name: Follower, port: 0, dir: dir}, id: Follower)
    _ = :sys.get_state(pid)
    %{dir: dir}
  end

  test "a bulk load of tombstones with no replicas forgets nothing; a replica on disk is erased" do
    ids = for i <- 1..50, do: "tombbulk-#{System.unique_integer([:positive])}-#{i}"
    [present | _] = ids
    File.write!(Follower.db_path(Follower, present), "db")
    File.write!(Follower.wal_path(Follower, present), "wal")

    test = self()

    loader = fn ->
      # Hand the test this process before the hook runs, so it can trace exactly the load.
      send(test, {:loading, self()})

      receive do
        :go -> ids
      end
    end

    name = :"tombbulk_#{System.unique_integer([:positive])}"

    start_supervised!(
      {Tombstones,
       name: name,
       table: name,
       loader: loader,
       since_loader: fn _ -> [] end,
       storage_loader: fn -> {:ok, []} end,
       sweeper: fn -> :ok end},
      id: name
    )

    assert_receive {:loading, tomb}, 5_000

    calls =
      trace_calls(tomb, [{Follower, :forget, 2}, {Fathom.FS, :rm, 1}], fn ->
        send(tomb, :go)
        _ = :sys.get_state(tomb)
      end)

    forgotten = for {Follower, :forget, [_, id]} <- calls, do: id
    unlinks = for {Fathom.FS, :rm, [_]} <- calls, do: :rm

    assert forgotten == [present],
           "the bulk load forgot #{length(forgotten)} ids for 1 replica on disk"

    assert length(unlinks) == 6, "expected one forget's 6 unlinks, got #{length(unlinks)}"

    refute File.exists?(Follower.db_path(Follower, present)),
           "a deleted tenant's replica on disk survived the bulk load"

    refute File.exists?(Follower.wal_path(Follower, present))
  end

  test "state_of/2 on a tombstoned id with no files unlinks nothing" do
    id = "tombbulk-#{System.unique_integer([:positive])}"
    :ets.insert(Tombstones, {id})
    on_exit(fn -> :ets.delete(Tombstones, id) end)

    # A process cannot usefully trace itself, so the reads run in a helper the test traces.
    test = self()

    reader =
      spawn_link(fn ->
        receive do
          :go -> send(test, {:read, for(_ <- 1..3, do: Follower.state_of(Follower, id))})
        end

        # Alive until untraced: `:erlang.trace/3` on a dead pid raises.
        receive do
          :stop -> :ok
        end
      end)

    on_exit(fn -> send(reader, :stop) end)

    calls =
      trace_calls(reader, [{Follower, :forget, 2}, {Fathom.FS, :rm, 1}], fn ->
        send(reader, :go)
        assert_receive {:read, [nil, nil, nil]}, 5_000
      end)

    assert calls == [], "state_of/2 of a tombstoned id with no files ran #{inspect(calls)}"
  end

  test "state_of/2 on a tombstoned id with files still erases them" do
    id = "tombbulk-#{System.unique_integer([:positive])}"
    File.write!(Follower.db_path(Follower, id), "db")
    :ets.insert(Tombstones, {id})
    on_exit(fn -> :ets.delete(Tombstones, id) end)

    assert Follower.state_of(Follower, id) == nil
    refute File.exists?(Follower.db_path(Follower, id))
  end

  # The calls `pid` makes to `mfas` while `fun` runs, in order, as `{module, function, args}`.
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
