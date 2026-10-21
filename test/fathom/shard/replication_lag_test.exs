defmodule Fathom.Shard.ReplicationLagTest do
  @moduledoc """
  Admin observability 2026-10-15 P1/P2: the per-commit `[:fathom, :replication, :commit]` and
  `[:fathom, :replication, :catchup]` events, `Session.lag_info/2`, `Fleet.lag_report/1` and the
  `LagGauge`. Uses the same real-shard, real-follower harness as `replication_commit_test.exs`.
  """
  use ExUnit.Case, async: false

  alias Fathom.Shard.Connection
  alias Fathom.Shard.Replication.{Fleet, Follower, LagGauge, Session}
  alias Fathom.Shards

  @registry Fathom.Shard.Replication.SessionRegistry

  setup do
    id = "repl_lag_#{System.unique_integer([:positive])}"
    root = Path.join(System.tmp_dir!(), "repllag_#{System.unique_integer([:positive])}")
    File.mkdir_p!(root)

    keys = [
      :replication_enabled,
      :replication_followers,
      :replication_quorum,
      :replication_catchup_ms
    ]

    prev = for k <- keys, do: {k, Application.get_env(:fathom, k)}

    test_pid = self()
    hid = "lag-test-#{System.unique_integer([:positive])}"

    :telemetry.attach_many(
      hid,
      [
        [:fathom, :replication, :commit],
        [:fathom, :replication, :commit_total],
        [:fathom, :replication, :commit_stage],
        [:fathom, :replication, :catchup],
        [:fathom, :replication, :lag]
      ],
      fn ev, m, meta, _ -> send(test_pid, {:tel, ev, m, meta}) end,
      nil
    )

    on_exit(fn ->
      :telemetry.detach(hid)
      Session.stop(id)

      for {k, v} <- prev do
        if is_nil(v),
          do: Application.delete_env(:fathom, k),
          else: Application.put_env(:fathom, k, v)
      end

      File.rm_rf(root)
      for s <- ["", "-wal", "-shm"], do: File.rm(Fathom.Shard.db_path(id) <> s)
    end)

    %{id: id, root: root}
  end

  defp start_followers!(root, n) do
    for i <- 1..n do
      name = :"rl_f#{i}_#{System.unique_integer([:positive])}"

      pid =
        start_supervised!({Follower, name: name, port: 0, dir: Path.join(root, to_string(name))},
          id: name
        )

      {:ok, port} = Follower.port(pid)
      {name, port}
    end
  end

  defp configure!(followers, quorum) do
    Application.put_env(:fathom, :replication_enabled, true)
    Application.put_env(:fathom, :replication_quorum, quorum)

    Application.put_env(
      :fathom,
      :replication_followers,
      for({_n, p} <- followers, do: {~c"127.0.0.1", p})
    )

    start_supervised!(Fleet)
  end

  defp open_shard!(id) do
    {:ok, coordinator, ref, path} = Shards.checkout(id)
    on_exit(fn -> Fathom.Shard.checkin(coordinator, ref) end)
    {:ok, conn} = Connection.open(path)
    on_exit(fn -> Connection.close(conn) end)
    {:ok, _} = Connection.query(conn, "CREATE TABLE t (a)", [])
    {coordinator, path}
  end

  test "commit_outcome/1 maps replies onto the four bounded outcomes" do
    assert Session.commit_outcome(:ok) == :ok
    assert Session.commit_outcome({:error, {:no_quorum, :impossible}}) == :no_quorum
    assert Session.commit_outcome({:error, {:no_quorum, :catching_up}}) == :no_quorum
    assert Session.commit_outcome({:error, {:no_quorum, :timeout}}) == :timeout
    assert Session.commit_outcome({:error, :timeout}) == :timeout
    assert Session.commit_outcome({:error, :stale_session}) == :other
  end

  test "an acked commit emits one commit event (ok, native duration) and lag_info answers", ctx do
    %{id: id, root: root} = ctx
    followers = start_followers!(root, 3)
    configure!(followers, 2)
    {coordinator, path} = open_shard!(id)
    for {n, _} <- followers, do: Follower.seed(n, id, 0, 0, 0, 0)

    assert :ok = Session.commit(id, path <> "-wal", coordinator)

    assert_receive {:tel, [:fathom, :replication, :commit], %{duration: d}, %{outcome: :ok}}
    assert is_integer(d) and d > 0
    refute_receive {:tel, [:fathom, :replication, :commit], _, _}, 50

    # Expert review 2026-10-17 #10: the caller-side total covers the in-handler span (it adds the
    # mailbox wait + ensure_started), and the two stages are bounded-tag events.
    assert_receive {:tel, [:fathom, :replication, :commit_total], %{duration: total},
                    %{outcome: :ok}}

    assert total >= d

    stages =
      for _ <- 1..2 do
        assert_receive {:tel, [:fathom, :replication, :commit_stage], %{duration: sd},
                        %{stage: stage}}

        assert is_integer(sd) and sd >= 0
        stage
      end

    assert Enum.sort(stages) == [:build, :collect]

    info = Session.lag_info(id)
    assert %{shard_id: ^id, catching_up?: false, catchup_fails: 0, unacked_bytes: ub} = info
    assert is_integer(ub)

    report = Fleet.lag_report()
    assert report.sessions >= 1 and report.timed_out == 0
    # Only flagged Sessions are asked (expert review 2026-10-21 #24); a current one is not.
    assert report.answered == report.queried
    refute Enum.any?(report.worst, &(&1.shard_id == id))
  end

  # Expert review 2026-10-19 #9 (measure only): the commit_stage events carry a bounded
  # `reset: true | false` tag, so the first commit after a checkpoint (a planned `:reset`, which waits
  # on the follower's absorb) can be separated from a plain append commit in the rig's histogram.
  test "commit_stage events are tagged reset: true for a reset round and false for an append",
       ctx do
    %{id: id, root: root} = ctx
    followers = start_followers!(root, 3)
    configure!(followers, 2)
    {coordinator, path} = open_shard!(id)
    {:ok, conn} = Connection.open(path)
    on_exit(fn -> Connection.close(conn) end)

    drain = fn drain, acc ->
      receive do
        {:tel, [:fathom, :replication, :commit_stage], _, %{stage: st, reset: r}} ->
          drain.(drain, [{st, r} | acc])
      after
        100 -> acc
      end
    end

    # Never shipped before: Primary.plan(nil, ...) is {:reset, 0, _}.
    assert :ok = Session.commit(id, path <> "-wal", coordinator)
    first = drain.(drain, [])
    assert {:collect, true} in first and {:build, true} in first

    # Followers are now at the WAL's extent in the same generation: the next commit is an append.
    {:ok, _} = Connection.query(conn, "INSERT INTO t VALUES (1)", [])
    assert :ok = Session.commit(id, path <> "-wal", coordinator)
    second = drain.(drain, [])
    assert {:collect, false} in second and {:build, false} in second
    refute Enum.any?(second, fn {_, r} -> r end)
  end

  # Expert review 2026-10-20-2027 #5: the catch-up (and idle-close) cycles ran through the same
  # `[:fathom, :replication, :commit]` execute as a tenant's commit with no way to tell them apart,
  # so the tenant-visible histogram counted cycles nobody waited on. Each cycle now carries its origin.
  test "commit and commit_stage events carry the cycle's mode; stages carry skipped", ctx do
    %{id: id, root: root} = ctx
    followers = start_followers!(root, 3)
    configure!(followers, 2)
    {coordinator, path} = open_shard!(id)
    for {n, _} <- followers, do: Follower.seed(n, id, 0, 0, 0, 0)

    assert :ok = Session.commit(id, path <> "-wal", coordinator)
    assert_receive {:tel, [:fathom, :replication, :commit], _, %{outcome: :ok, mode: :tenant}}

    assert_receive {:tel, [:fathom, :replication, :commit_stage], _,
                    %{stage: :collect, mode: :tenant, skipped: skipped}}

    assert is_boolean(skipped)

    # A cycle in the catch-up's shape (`:no_checkpoint`, no caller deadline).
    [{session, _}] = Registry.lookup(@registry, id)
    _ = GenServer.call(session, {:commit, path <> "-wal", :no_checkpoint})

    assert_receive {:tel, [:fathom, :replication, :commit], _, %{mode: :catch_up}}
    refute_receive {:tel, [:fathom, :replication, :commit], _, %{mode: :tenant}}, 50
  end

  test "a failed quorum emits outcome no_quorum", ctx do
    %{id: id, root: root} = ctx
    [{name, port}] = start_followers!(root, 1)
    Application.put_env(:fathom, :replication_enabled, true)
    Application.put_env(:fathom, :replication_quorum, 2)

    Application.put_env(:fathom, :replication_followers, [
      {~c"127.0.0.1", port},
      {~c"127.0.0.1", 1},
      {~c"127.0.0.1", 2}
    ])

    start_supervised!(Fleet)
    {coordinator, path} = open_shard!(id)
    Follower.seed(name, id, 0, 0, 0, 0)

    assert {:error, {:no_quorum, _}} = Session.commit(id, path <> "-wal", coordinator)
    assert_receive {:tel, [:fathom, :replication, :commit], _, %{outcome: :no_quorum}}
  end

  test "a reject arms catch-up: catchup event carries fails; lag_info/lag_report/gauge see it",
       ctx do
    %{id: id, root: root} = ctx
    # Long retry so the timer stays armed while we read it.
    Application.put_env(:fathom, :replication_catchup_ms, 60_000)
    followers = start_followers!(root, 3)
    configure!(followers, 1)
    {coordinator, path} = open_shard!(id)

    {:ok, epoch} = Fathom.Shard.epoch(coordinator)
    lineage = Fathom.Shard.lineage(coordinator)

    for {n, _} <- Enum.take(followers, 2),
        do: Follower.seed(n, id, epoch, 0, 0, 0, lineage)

    _ = Session.commit(id, path <> "-wal", coordinator)

    assert_receive {:tel, [:fathom, :replication, :catchup], %{fails: f}, _} when f >= 1, 2_000

    [{session, _}] = Registry.lookup(@registry, id)
    info = Session.lag_info(session)
    assert info.catching_up?
    assert info.catchup_fails >= 1

    report = Fleet.lag_report()
    assert report.catching_up >= 1
    assert Enum.any?(report.worst, &(&1.shard_id == id))

    # Expert review 2026-10-17 #2: the gauge must not call Sessions. A SUSPENDED Session answers no
    # call, so a gauge that still did `GenServer.call(:lag_info)` would block here until timeout (or
    # miscount); reading the node-level flag returns the right count immediately.
    :ok = :sys.suspend(session)
    {us, :ok} = :timer.tc(&LagGauge.measure/0)
    :ok = :sys.resume(session)
    assert us < 100_000
    assert_receive {:tel, [:fathom, :replication, :lag], %{catching_up_shards: n}, _}
    assert n >= 1
    assert LagGauge.count() >= 1
  end

  test "LagGauge flag follows arm / clear / kill without leaking" do
    start_supervised!(Fleet)
    base = LagGauge.count()

    LagGauge.mark(self(), true)
    assert LagGauge.count() == base + 1
    LagGauge.mark(self(), true)
    assert LagGauge.count() == base + 1, "re-arming must not double count"
    LagGauge.mark(self(), false)
    assert LagGauge.count() == base

    # A process that dies while flagged (killed: no terminate/2 ran) must not leak +1.
    pid = spawn(fn -> Process.sleep(:infinity) end)
    LagGauge.mark(pid, true)
    assert LagGauge.count() == base + 1
    ref = Process.monitor(pid)
    Process.exit(pid, :kill)
    assert_receive {:DOWN, ^ref, :process, ^pid, :killed}
    assert LagGauge.count() == base
  end

  test "a catching-up Session's flag is cleared by terminate", ctx do
    %{id: id, root: root} = ctx
    Application.put_env(:fathom, :replication_catchup_ms, 60_000)
    followers = start_followers!(root, 3)
    configure!(followers, 1)
    {coordinator, path} = open_shard!(id)
    {:ok, epoch} = Fathom.Shard.epoch(coordinator)
    lineage = Fathom.Shard.lineage(coordinator)
    for {n, _} <- Enum.take(followers, 2), do: Follower.seed(n, id, epoch, 0, 0, 0, lineage)
    _ = Session.commit(id, path <> "-wal", coordinator)
    assert_receive {:tel, [:fathom, :replication, :catchup], %{fails: f}, _} when f >= 1, 2_000

    before = LagGauge.count()
    assert before >= 1
    Session.stop(id)
    assert LagGauge.count() == before - 1
  end

  test "lag_report excludes a Session that does not answer instead of crashing" do
    start_supervised!(Fleet)
    parent = self()

    hung =
      spawn_link(fn ->
        {:ok, _} =
          Registry.register(@registry, "hung_#{System.unique_integer([:positive])}", self())

        # The report only asks flagged Sessions (expert review 2026-10-21 #24).
        LagGauge.mark(self(), true)
        send(parent, :registered)

        receive do
          :never -> :ok
        end
      end)

    assert_receive :registered
    assert Session.lag_info(hung, 50) == nil

    report = Fleet.lag_report(timeout: 100)
    assert report.timed_out >= 1
    assert report.answered == report.queried - report.timed_out
  end

  # Expert review 2026-10-21 #24. SYMPTOM: the Durability page's lag report called EVERY replication
  # Session every 10 s while a viewer was mounted — 30k shards, 30k calls, each waking a hibernated
  # Session and queueing behind its commits — to rank the handful that were lagging. INVARIANT: the
  # report calls only the Sessions flagged in `LagGauge` (a real laggard, #25), while still counting
  # every registered Session.
  test "lag_report calls only the Sessions flagged in LagGauge, and still counts them all" do
    start_supervised!(Fleet)
    parent = self()

    fake = fn armed? ->
      pid =
        spawn(fn ->
          id = "fake_#{System.unique_integer([:positive])}"
          {:ok, _} = Registry.register(@registry, id, self())
          if armed?, do: LagGauge.mark(self(), true)
          send(parent, {:registered, self()})
          fake_session_loop(parent, id, armed?)
        end)

      assert_receive {:registered, ^pid}
      pid
    end

    idle = for _ <- 1..1_000, do: fake.(false)
    armed = for _ <- 1..3, do: fake.(true)
    on_exit(fn -> for p <- idle ++ armed, do: Process.exit(p, :kill) end)

    report = Fleet.lag_report(timeout: 1_000)
    calls = drain_lag_calls([])

    assert length(calls) == 3,
           "lag_report called #{length(calls)} Sessions to find 3 laggards (1,000 idle)"

    assert Enum.sort(calls) == Enum.sort(armed)
    assert report.sessions == 1_003, "the report stopped counting the Sessions it did not call"
    assert report.catching_up == 3
    assert length(report.worst) == 3
    assert report.timed_out == 0
  end

  defp fake_session_loop(parent, id, armed?) do
    receive do
      {:"$gen_call", from, :lag_info} ->
        send(parent, {:lag_info_called, self()})

        GenServer.reply(from, %{
          shard_id: id,
          catchup_fails: if(armed?, do: 1, else: 0),
          catching_up?: armed?,
          inflight_pushes: 0,
          seeds_in_flight: 0,
          behind_followers: if(armed?, do: 1, else: 0),
          unacked_bytes: if(armed?, do: 4096, else: 0)
        })

        fake_session_loop(parent, id, armed?)
    end
  end

  defp drain_lag_calls(acc) do
    receive do
      {:lag_info_called, pid} -> drain_lag_calls([pid | acc])
    after
      0 -> acc
    end
  end

  test "lag_report with no registry (replication off) is an empty report, not a crash" do
    assert %{sessions: 0, queried: 0, answered: 0, timed_out: 0, catching_up: 0, worst: []} =
             Fleet.lag_report()

    assert Session.lag_info("nope") == nil
    assert :ok = LagGauge.measure()
  end
end
