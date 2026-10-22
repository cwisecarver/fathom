defmodule Fathom.Shard.ReplicationPriorityTest do
  @moduledoc """
  Expert review 2026-10-21 r7 #12: `REPLICATION_PROCESS_PRIORITY` = `normal | hubs | all`.

  Pins the plumbing, not the latency win (that is the rig A/B's job, `tpc-tail 1024` alternating
  modes): under each mode, exactly the intended processes run at `:high` — the Shipper, its Writer,
  the follower's per-connection reader and its workers for `hubs`; those plus the Session for `all`
  — and the default `normal` changes nothing. The deferred-sync task's priority is covered by
  `Priority.for_role/2` (it is spawned with `for_role(:session)`), since it lives for one fdatasync.
  """
  use ExUnit.Case, async: false

  alias Fathom.Shard.Connection
  alias Fathom.Shard.Replication.{Fleet, Follower, Priority, Session, Shipper}
  alias Fathom.Shards

  @registry Fathom.Shard.Replication.SessionRegistry
  @keys [
    :replication_enabled,
    :replication_followers,
    :replication_quorum,
    :replication_process_priority
  ]

  setup do
    prev = Map.new(@keys, &{&1, Application.get_env(:fathom, &1)})

    on_exit(fn ->
      Enum.each(prev, fn
        {k, nil} -> Application.delete_env(:fathom, k)
        {k, v} -> Application.put_env(:fathom, k, v)
      end)
    end)

    :ok
  end

  test "the mode table: hubs raises the hubs, all adds the Session, normal raises nothing" do
    assert Priority.for_role(:hub, :normal) == :normal
    assert Priority.for_role(:session, :normal) == :normal
    assert Priority.for_role(:hub, :hubs) == :high
    assert Priority.for_role(:session, :hubs) == :normal
    assert Priority.for_role(:hub, :all) == :high
    assert Priority.for_role(:session, :all) == :high
  end

  test "parse accepts exactly the three modes" do
    assert Priority.parse("normal") == {:ok, :normal}
    assert Priority.parse("hubs") == {:ok, :hubs}
    assert Priority.parse("all") == {:ok, :all}
    assert Priority.parse("high") == :error
    assert Priority.parse("") == :error
  end

  test "the default is normal" do
    Application.delete_env(:fathom, :replication_process_priority)
    assert Priority.mode() == :normal
    assert Priority.for_role(:hub) == :normal
    assert Priority.for_role(:session) == :normal
  end

  for {mode, hub, session} <- [
        {:normal, :normal, :normal},
        {:hubs, :high, :normal},
        {:all, :high, :high}
      ] do
    @mode mode
    @hub hub
    @session session

    test "mode #{mode}: hubs run at #{hub}, the Session at #{session}" do
      Application.put_env(:fathom, :replication_process_priority, @mode)
      assert_priorities!(@hub, @session)
    end
  end

  defp assert_priorities!(hub, session_prio) do
    id = "repl_prio_#{System.unique_integer([:positive])}"
    root = Path.join(System.tmp_dir!(), "replprio_#{System.unique_integer([:positive])}")
    File.mkdir_p!(root)

    on_exit(fn ->
      Session.stop(id)
      File.rm_rf(root)
      for s <- ["", "-wal", "-shm"], do: File.rm(Fathom.Shard.db_path(id) <> s)
    end)

    workers_before = MapSet.new(follower_workers())

    followers =
      for i <- 1..3 do
        name = :"prio_f#{i}_#{System.unique_integer([:positive])}"

        pid =
          start_supervised!(
            {Follower, name: name, port: 0, dir: Path.join(root, to_string(name))},
            id: name
          )

        {:ok, port} = Follower.port(pid)
        {name, port}
      end

    Application.put_env(:fathom, :replication_enabled, true)
    Application.put_env(:fathom, :replication_quorum, 2)

    Application.put_env(
      :fathom,
      :replication_followers,
      for({_n, p} <- followers, do: {~c"127.0.0.1", p})
    )

    start_supervised!(Fleet)
    await!(fn -> Enum.all?(Fleet.shippers(), &Shipper.connected?/1) end)

    {:ok, coordinator, ref, path} = Shards.checkout(id)
    on_exit(fn -> Fathom.Shard.checkin(coordinator, ref) end)
    {:ok, conn} = Connection.open(path)
    on_exit(fn -> Connection.close(conn) end)
    {:ok, _} = Connection.query(conn, "CREATE TABLE t (a)", [])
    {:ok, epoch} = Fathom.Shard.epoch(coordinator)
    lineage = Fathom.Shard.lineage(coordinator)
    for {n, _} <- followers, do: Follower.seed(n, id, epoch, 0, 0, 0, lineage)
    assert :ok = Session.commit(id, path <> "-wal", coordinator)

    [{session, _}] = Registry.lookup(@registry, id)
    assert prio(session) == session_prio

    for shipper <- Fleet.shippers() do
      pid = GenServer.whereis(shipper)
      assert prio(pid) == hub, "shipper #{inspect(shipper)}"
      assert prio(:sys.get_state(pid).writer) == hub, "writer of #{inspect(shipper)}"
    end

    # Every follower worker this test's commit created, and the per-connection reader each is
    # linked to (`spawn_worker/5` links the worker to its reader).
    workers = Enum.reject(follower_workers(), &MapSet.member?(workers_before, &1))
    assert workers != [], "the commit reached no follower worker; the test exercised nothing"

    for w <- workers do
      assert prio(w) == hub, "follower worker"
      {:links, links} = Process.info(w, :links)
      readers = Enum.filter(links, &is_pid/1)
      assert readers != []
      for r <- readers, do: assert(prio(r) == hub, "follower reader")
    end
  end

  defp follower_workers do
    for pid <- Process.list(), :proc_lib.get_label(pid) == :replication_follower_worker, do: pid
  end

  defp prio(pid) do
    {:priority, p} = Process.info(pid, :priority)
    p
  end

  defp await!(fun, timeout \\ 5_000) do
    deadline = System.monotonic_time(:millisecond) + timeout

    Stream.repeatedly(fn ->
      cond do
        fun.() -> true
        System.monotonic_time(:millisecond) > deadline -> false
        true -> Process.sleep(10) && nil
      end
    end)
    |> Enum.find(&is_boolean/1)
    |> assert("never: shippers connect")
  end
end
