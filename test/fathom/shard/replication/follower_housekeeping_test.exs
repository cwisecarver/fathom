defmodule Fathom.Shard.Replication.FollowerHousekeepingTest do
  @moduledoc """
  Expert review 2026-10-10 #11, #19 and #22 — the follower's small lifecycle fixes.

    * #11 the replication listener and dial set TCP keepalive (a half-open peer is noticed in
      ~60 s, not 2 h);
    * #19 seed temps left by a killed worker are reaped by a periodic sweep, not only at boot;
    * #22 `forget/2` and `note_ordinal/3` mutate follower state under the per-shard lock, and a
      pulled seed's ordinal is stamped by the install itself.
  """
  use ExUnit.Case, async: false

  alias Fathom.Shard.Replication.{Follower, Keepalive, Protocol}

  setup do
    name = :"housekeeping_#{System.unique_integer([:positive])}"
    dir = Path.join(System.tmp_dir!(), to_string(name))
    start_supervised!({Follower, name: name, port: 0, dir: dir}, id: name)
    on_exit(fn -> File.rm_rf(dir) end)

    %{name: name, dir: dir, id: "housekeeping-#{System.unique_integer([:positive])}"}
  end

  defp put_env(key, value) do
    prev = Application.get_env(:fathom, key)
    Application.put_env(:fathom, key, value)

    on_exit(fn ->
      if is_nil(prev),
        do: Application.delete_env(:fathom, key),
        else: Application.put_env(:fathom, key, prev)
    end)
  end

  # A process that takes `id`'s replica lock and keeps it until told to release.
  defp hold_lock(name, id) do
    parent = self()

    pid =
      spawn_link(fn ->
        {:ok, token} = Follower.lock_shard(name, id, 1_000)
        send(parent, :locked)

        receive do
          :release -> Follower.unlock_shard(name, token)
        end
      end)

    assert_receive :locked, 2_000
    pid
  end

  describe "keepalive (#11)" do
    test "the follower's listener and the sockets it accepts have keepalive on", %{name: name} do
      {:ok, port} = Follower.port(name)

      {:ok, client} =
        :gen_tcp.connect(~c"127.0.0.1", port, [:binary, active: false] ++ Keepalive.opts())

      # The follower's own listener options are what an accepted socket inherits; ask the listener.
      state = :sys.get_state(name)
      assert {:ok, [keepalive: true]} = :inet.getopts(state.lsock, [:keepalive])
      assert {:ok, [keepalive: true]} = :inet.getopts(client, [:keepalive])
      :gen_tcp.close(client)
    end

    test "the tuned idle time is applied where the OS supports it", %{name: name} do
      case Keepalive.option_numbers(:os.type()) do
        nil ->
          # Portable fallback: plain keepalive with the OS's own timers.
          assert Keepalive.opts() == [keepalive: true]

        %{idle: idle_opt, interval: intvl_opt, count: cnt_opt} ->
          state = :sys.get_state(name)

          for {opt, want} <- [{idle_opt, 30}, {intvl_opt, 10}, {cnt_opt, 3}] do
            assert {:ok, [{:raw, 6, ^opt, <<got::native-32>>}]} =
                     :inet.getopts(state.lsock, [{:raw, 6, opt, 4}])

            assert got == want
          end
      end
    end
  end

  describe "periodic seed-temp sweep (#19)" do
    test "a stale seed temp left by a killed worker is reaped without a restart", ctx do
      %{name: name, dir: dir, id: id} = ctx
      stale = Path.join(dir, "#{id}.db.seeding.12345")
      fresh = Path.join(dir, "#{id}.db.seeding.67890")
      File.write!(stale, "half a seed")
      File.write!(fresh, "a live seed")
      :ok = File.touch(stale, System.os_time(:second) - 3_600)

      send(Process.whereis(name), :reap_seed_temps)
      # Synchronise on the follower having handled the message.
      _ = :sys.get_state(name)

      refute File.exists?(stale), "a dead seed's temp survived the periodic sweep"
      assert File.exists?(fresh), "the sweep removed a seed still being written"
    end
  end

  describe "per-shard lock (#22)" do
    test "forget waits for a holder instead of deleting underneath it", ctx do
      %{name: name, id: id} = ctx
      :ok = Follower.seed(name, id, 1, 0, 0, 0, 3)
      holder = hold_lock(name, id)

      forget = Task.async(fn -> Follower.forget(name, id) end)

      assert Task.yield(forget, 200) == nil,
             "forget removed the replica while another process held its lock"

      assert Follower.state_of(name, id) != nil

      send(holder, :release)
      assert Task.await(forget, 5_000) == :ok
      assert Follower.state_of(name, id) == nil
    end

    test "forget still completes when the lock stays busy (tombstone erase is not blockable)",
         ctx do
      %{name: name, id: id} = ctx
      put_env(:replication_forget_lock_wait_ms, 50)
      :ok = Follower.seed(name, id, 1, 0, 0, 0, 3)
      holder = hold_lock(name, id)

      assert Follower.forget(name, id) == :ok
      assert Follower.state_of(name, id) == nil
      send(holder, :release)
    end

    test "note_ordinal does not read-modify-write while another process holds the lock", ctx do
      %{name: name, id: id} = ctx
      :ok = Follower.seed(name, id, 1, 0, 0, 0, 3)
      holder = hold_lock(name, id)

      assert Follower.note_ordinal(name, id, 9) == :ok
      assert Follower.state_of(name, id).wal_ordinal == 0

      send(holder, :release)
      # Released: it applies.
      assert Follower.note_ordinal(name, id, 9) == :ok
      assert Follower.state_of(name, id).wal_ordinal == 9
    end

    test "a pulled seed's ordinal is stamped by the install itself", ctx do
      %{name: name, id: id} = ctx

      begin = %Protocol.SeedBegin{
        shard_id: id,
        epoch: 1,
        wal_gen: 0,
        salt1: 0,
        wal_offset: 0,
        db_size: 0,
        wal_size: 0,
        lineage: 3
      }

      seeds = Follower.begin_seed(name, %{}, begin)
      seeds = Follower.stamp_seed_ordinal(seeds, id, 7)
      assert {{:ok, 0}, %{}} = Follower.finish_seed(name, seeds, id)

      assert %{wal_ordinal: 7} = Follower.state_of(name, id)
    end
  end
end
