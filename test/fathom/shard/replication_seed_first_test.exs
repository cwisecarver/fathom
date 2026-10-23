defmodule Fathom.Shard.ReplicationSeedFirstTest do
  @moduledoc """
  Expert review 2026-10-22 r8 #16: the first tenant commit of a coordinator life that took a fresh
  lineage ships a push whose every answer is already known (`:unknown_shard`), waits a quorum round
  for N refusals, logs `quorum IMPOSSIBLE`, and only then seeds and re-ships.

  The Session now seeds first when `Fathom.Shard.epoch_first_session/1` says so. The observable is
  the number of quorum COLLECT rounds the first commit runs (`commit_stage` stage `:collect`,
  emitted by the Session process only when a push round actually ran). Before the change the first
  commit runs one: the doomed round. After it, none: the seeds carry every frame, so the post-seed
  retry plans `:nothing` and acks through `ack_if_owned/1`. A second Session under the same
  coordinator is told `false` and keeps today's path.
  """
  use ExUnit.Case, async: false

  alias Fathom.Shard.Connection
  alias Fathom.Shard.Replication.Fleet
  alias Fathom.Shard.Replication.Follower
  alias Fathom.Shard.Replication.Session
  alias Fathom.Shard.Replication.Shipper
  alias Fathom.Shards

  setup do
    id = "repl_seedfirst_#{System.unique_integer([:positive])}"
    root = Path.join(System.tmp_dir!(), "replseedfirst_#{System.unique_integer([:positive])}")
    File.mkdir_p!(root)

    keys = [
      :replication_enabled,
      :replication_followers,
      :replication_quorum,
      :replication_reseed
    ]

    prev = for k <- keys, do: {k, Application.get_env(:fathom, k)}

    on_exit(fn ->
      Session.stop(id)

      for {k, v} <- prev do
        if is_nil(v),
          do: Application.delete_env(:fathom, k),
          else: Application.put_env(:fathom, k, v)
      end

      File.rm_rf(root)
      for s <- ["", "-wal", "-shm"], do: File.rm(Fathom.Shard.db_path(id) <> s)
    end)

    followers =
      for i <- 1..3 do
        name = :"sf_f#{i}_#{System.unique_integer([:positive])}"
        dir = Path.join(root, to_string(name))
        pid = start_supervised!({Follower, name: name, port: 0, dir: dir}, id: name)
        {:ok, port} = Follower.port(pid)
        {name, port}
      end

    Application.put_env(:fathom, :replication_enabled, true)
    Application.put_env(:fathom, :replication_quorum, 2)

    Application.put_env(
      :fathom,
      :replication_followers,
      for({_, p} <- followers, do: {~c"127.0.0.1", p})
    )

    start_supervised!(Fleet)
    await_connected!(3)

    handler = "seedfirst-#{System.unique_integer([:positive])}"
    test_pid = self()

    :telemetry.attach(
      handler,
      [:fathom, :replication, :commit_stage],
      fn _e, _m, %{stage: stage}, _ -> send(test_pid, {:stage, self(), stage}) end,
      nil
    )

    on_exit(fn -> :telemetry.detach(handler) end)

    %{id: id, followers: Enum.map(followers, &elem(&1, 0))}
  end

  defp await_connected!(n, timeout \\ 5_000) do
    deadline = System.monotonic_time(:millisecond) + timeout

    Stream.repeatedly(fn ->
      cond do
        Enum.count(Fleet.shippers(), &Shipper.connected?/1) >= n -> true
        System.monotonic_time(:millisecond) > deadline -> false
        true -> Process.sleep(20) && nil
      end
    end)
    |> Enum.find(&is_boolean/1)
    |> assert("shippers never connected")
  end

  defp session!(id) do
    [{pid, _}] = Registry.lookup(Fathom.Shard.Replication.SessionRegistry, id)
    pid
  end

  defp collects(session) do
    receive do
      {:stage, ^session, :collect} -> 1 + collects(session)
      {:stage, _other, _stage} -> collects(session)
    after
      0 -> 0
    end
  end

  test "the first commit of a fresh-lineage life seeds first: one quorum round, not two", ctx do
    %{id: id, followers: names} = ctx
    {:ok, coordinator, ref, path} = Shards.checkout(id)
    on_exit(fn -> Fathom.Shard.checkin(coordinator, ref) end)
    {:ok, conn} = Connection.open(path)
    on_exit(fn -> Connection.close(conn) end)
    wal = path <> "-wal"

    {:ok, _} = Connection.query(conn, "CREATE TABLE t (a)", [])
    assert :ok = Session.commit(id, wal, coordinator)
    session = session!(id)

    assert collects(session) == 0,
           "the first commit shipped a push before seeding (a doomed :unknown_shard round)"

    # Correctness is the old path's: the followers were seeded and hold the shard.
    assert Enum.count(names, &match?(%{}, Follower.state_of(&1, id))) >= 2

    # Steady state is untouched: the next commit is one ordinary round.
    {:ok, _} = Connection.query(conn, "INSERT INTO t VALUES (1)", [])
    assert :ok = Session.commit(id, wal, coordinator)
    assert collects(session) == 1
  end

  test "a Session restarted under the same coordinator is told false and ships first", %{
    id: id
  } do
    {:ok, coordinator, ref, path} = Shards.checkout(id)
    on_exit(fn -> Fathom.Shard.checkin(coordinator, ref) end)
    {:ok, conn} = Connection.open(path)
    on_exit(fn -> Connection.close(conn) end)
    wal = path <> "-wal"

    {:ok, _} = Connection.query(conn, "CREATE TABLE t (a)", [])
    assert :ok = Session.commit(id, wal, coordinator)
    _ = collects(session!(id))

    Session.stop(id)
    {:ok, _} = Connection.query(conn, "INSERT INTO t VALUES (1)", [])
    assert :ok = Session.commit(id, wal, coordinator)

    # Told false: the push goes out first, as before.
    assert collects(session!(id)) >= 1
    assert :sys.get_state(session!(id)).fresh_lineage == false
  end

  test "reseed off: the first commit keeps the ship-first path", %{id: id} do
    Application.put_env(:fathom, :replication_reseed, false)
    {:ok, coordinator, ref, path} = Shards.checkout(id)
    on_exit(fn -> Fathom.Shard.checkin(coordinator, ref) end)
    {:ok, conn} = Connection.open(path)
    on_exit(fn -> Connection.close(conn) end)
    wal = path <> "-wal"

    {:ok, _} = Connection.query(conn, "CREATE TABLE t (a)", [])
    assert :ok = Session.commit(id, wal, coordinator)

    assert collects(session!(id)) >= 1,
           "with reseed off the Session must not guess every follower refuses"
  end
end
