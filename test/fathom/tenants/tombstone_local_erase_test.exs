defmodule Fathom.Tenants.TombstoneLocalEraseTest do
  @moduledoc """
  A tenant delete erases the node-LOCAL files on EVERY node, not only the one that ran the
  DeleteJob (expert review 2026-10-15 #3).

  Symptom: `Tenants.purge/1` stops the coordinator and removes `<id>.db*` on the node Oban picked for
  the DeleteJob. The shard's LB home is usually a different node, where the coordinator stayed up
  (its terminate for a tombstoned id deliberately skips the flush AND the drop) and the complete
  tenant database stayed on disk. `Tombstones.on_new` runs on every node but only forgot the A2
  replica.

  The multi-node shape is simulated by delivering the Oban NOTIFY to this node's `Tombstones` while
  NO purge runs: this node plays the home that never saw `purge/1`. Parameterised over both
  liveness modes (legacy renew PUTs vs node heartbeat), asserting the mode actually took.
  """
  use ExUnit.Case, async: false

  alias Fathom.{Shard, ShardExecutor, Shards}
  alias Fathom.Shard.{Heartbeat, Storage}
  alias Fathom.Tenants.Tombstones
  alias Filo.Stmt

  @modes [:legacy, :heartbeat]

  setup do
    id = "tomblocal_#{System.unique_integer([:positive])}"
    prev_idle = Application.get_env(:fathom, :shard_idle_ms)

    # Pin the idle timer high: an idle-stop racing the assertions would answer for the coordinator.
    Application.put_env(:fathom, :shard_idle_ms, 600_000)

    on_exit(fn ->
      if prev_idle,
        do: Application.put_env(:fathom, :shard_idle_ms, prev_idle),
        else: Application.delete_env(:fathom, :shard_idle_ms)

      :ets.delete(Tombstones, id)
      Shards.drain(id, 2_000)
      Storage.purge_shard(id)
      File.rm_rf(Path.join(Shard.data_dir(), id <> ".db"))

      for f <- Path.wildcard(Path.join(Shard.data_dir(), "#{id}*")), do: File.rm(f)
    end)

    %{id: id}
  end

  defp set_mode!(:legacy), do: refute(Heartbeat.running?())

  defp set_mode!(:heartbeat) do
    hb = start_supervised!({Heartbeat, ttl_ms: 30_000})
    _ = :sys.get_state(hb)
    assert Heartbeat.running?()
  end

  defp assert_mode!(pid, :heartbeat), do: assert(is_integer(:sys.get_state(pid).acquire_gen))
  defp assert_mode!(pid, :legacy), do: assert(is_nil(:sys.get_state(pid).acquire_gen))

  defp local_files(id), do: Path.wildcard(Path.join(Shard.data_dir(), id <> ".db*"))

  defp seed!(id) do
    {:ok, conn} = ShardExecutor.open(id)
    {:ok, _} = ShardExecutor.execute(conn, %Stmt{sql: "CREATE TABLE t (v TEXT)", args: []})

    {:ok, _} =
      ShardExecutor.execute(conn, %Stmt{sql: "INSERT INTO t VALUES ('secret')", args: []})

    :ok = ShardExecutor.close(conn)
  end

  for mode <- @modes do
    test "[#{mode}] a NOTIFY on a node that never ran purge stops the coordinator and erases every local file",
         %{id: id} do
      set_mode!(unquote(mode))
      seed!(id)
      {:ok, pid} = Shards.ensure(id)
      assert_mode!(pid, unquote(mode))
      ref = Process.monitor(pid)

      # Forensic copies a prior self-fence/quarantine left (default D1: erasure may destroy them),
      # plus a SIBLING id's file that must survive (the glob is anchored at `<id>.db`).
      File.write!(Path.join(Shard.data_dir(), id <> ".db.fenced.123"), "old tenant data")
      File.write!(Path.join(Shard.data_dir(), id <> ".db.corrupt.123"), "old tenant data")
      sibling = Path.join(Shard.data_dir(), id <> "9.db")
      File.write!(sibling, "another tenant")

      assert [_ | _] = local_files(id), "precondition: the tenant has local files on this node"

      :telemetry.attach(
        "tomb-local-erase-#{id}",
        [:fathom, :tenants, :local_erased],
        fn _, _, meta, test -> send(test, {:erased, meta.shard_id}) end,
        self()
      )

      on_exit(fn -> :telemetry.detach("tomb-local-erase-#{id}") end)

      # The fleet-wide delete NOTIFY, as every node's Tombstones receives it. No purge/1 here.
      tomb = Process.whereis(Tombstones)
      send(tomb, {:notification, Tombstones.channel(), %{"shard_id" => id}})

      assert_receive {:erased, ^id}, 5_000
      assert_receive {:DOWN, ^ref, :process, ^pid, _}, 5_000

      assert local_files(id) == [],
             "a deleted tenant's data is still on this node's disk: #{inspect(local_files(id))}"

      assert File.exists?(sibling), "the erase reached a sibling tenant's file"
      File.rm(sibling)
    end
  end

  test "erase_local refuses an id that is not tombstoned (destructive, unfenced)", %{id: id} do
    seed!(id)
    {:ok, pid} = Shards.ensure(id)
    assert :ok = Fathom.Tenants.erase_local(id)
    assert Process.alive?(pid), "a live, un-tombstoned tenant's coordinator was stopped"
    assert [_ | _] = local_files(id)
  end

  test "the boot sweep erases a tombstoned id found on disk, and only that id", %{id: id} do
    live = id <> "x"
    File.mkdir_p!(Shard.data_dir())

    for suffix <- [".db", ".db-wal", ".db.etag", ".db.fenced.1"],
        do: File.write!(Path.join(Shard.data_dir(), id <> suffix), "data")

    File.write!(Path.join(Shard.data_dir(), live <> ".db"), "live tenant")
    :ets.insert(Tombstones, {id})

    assert :ok = Tombstones.sweep_local()

    assert local_files(id) == [], "the sweep left a tombstoned tenant's files"
    assert File.exists?(Path.join(Shard.data_dir(), live <> ".db"))
    File.rm(Path.join(Shard.data_dir(), live <> ".db"))
  end

  test "a successful boot load runs the sweeper once per newly loaded source", %{id: id} do
    test_pid = self()
    table = :"tomb_sweep_#{System.unique_integer([:positive])}"
    name = :"tomb_sweep_srv_#{System.unique_integer([:positive])}"

    start_supervised!(
      {Tombstones,
       name: name,
       table: table,
       loader: fn -> [id] end,
       since_loader: fn _ -> [] end,
       storage_loader: fn -> {:ok, []} end,
       sweeper: fn -> send(test_pid, :swept) end}
    )

    assert_receive :swept, 2_000
    # directory + storage loaded in the same first attempt: one sweep, not one per source.
    refute_receive :swept, 100
  end
end
