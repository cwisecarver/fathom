defmodule Fathom.Shard.RefusedOpenUnderQuorumTest do
  @moduledoc """
  The `abandon_open/5` half of expert review 2026-10-13 #2 (+ the logging half of #11): an
  under-quorum floor refusal (`%UnderQuorum{}`) is taken AFTER the lease steal and its object
  touch, so the marker it leaves must carry the touch's provenance, and the refusal is a routine
  partition outcome that must log as a warning, not an error with a stacktrace.

  See `refused_open_provenance_test.exs` for the pull-failure half and the full description of the
  defect (the previous holder A's acked local copy read as a fork). Here the refusal comes from
  peer recovery: four configured peers nobody listens on, Q = 2, against a stored object whose stamp
  is rankable, so `Recovery.best_replica/3` cannot reach the 3 definite answers it needs.

  A refusal that happens AFTER an object write must NOT forgive its touch; that is why
  `refusal_release_lease/2` is a whitelist on `%UnderQuorum{}` rather than every raise. The refusal
  deletes the cold-pulled copy first and has written nothing, so it is a clean visit.
  """
  use ExUnit.Case, async: false

  import ExUnit.CaptureLog

  alias Fathom.Shard
  alias Fathom.Shard.{Connection, Heartbeat, Storage}
  alias Fathom.Shard.Replication.{Fleet, Follower, Recovery}
  alias Fathom.Shard.Storage.S3
  alias Fathom.ShardExecutor
  alias Fathom.Shards
  alias Fathom.Test.S3EtagStore
  alias Filo.{Stmt, StmtResult}

  @modes [:legacy, :heartbeat]

  @env [
    :shard_storage,
    S3,
    :shard_idle_ms,
    :replication_enabled,
    :replication_promote_on_open,
    :replication_recover_from_peers,
    :replication_followers,
    :replication_quorum,
    :replication_recovery_underquorum_window_ms,
    :replication_recovery_refusal_ttl_ms,
    :replication_lineage_wire,
    :replication_ordinal_wire
  ]

  setup do
    shard = "refusedq_#{System.unique_integer([:positive])}"
    dir = Path.join(System.tmp_dir!(), "refusedq_#{System.unique_integer([:positive])}")
    File.mkdir_p!(dir)

    prev = Map.new(@env, &{&1, Application.get_env(:fathom, &1)})
    prev_running = Fleet.running()

    on_exit(fn ->
      Shards.drain(shard, 2_000)
      Fleet.publish(prev_running)

      for {k, v} <- prev do
        if is_nil(v),
          do: Application.delete_env(:fathom, k),
          else: Application.put_env(:fathom, k, v)
      end

      File.rm_rf(dir)
      for f <- Path.wildcard(Path.join(Shard.data_dir(), "#{shard}*")), do: File.rm_rf(f)
    end)

    Recovery.init_table()
    start_supervised!({Follower, name: Follower, port: 0, dir: dir})

    Application.put_env(:fathom, :shard_idle_ms, 600_000)
    Application.put_env(:fathom, :replication_enabled, true)
    Application.put_env(:fathom, :replication_promote_on_open, true)
    Application.put_env(:fathom, :replication_recover_from_peers, true)
    Application.put_env(:fathom, :replication_quorum, 2)
    Application.put_env(:fathom, :replication_recovery_underquorum_window_ms, 600_000)
    # These tests retry a refusal to watch each attempt reach the floor, so the checkout negative
    # cache (expert review 2026-10-13 #11, `Recovery.recent_refusal/2`) is off here; it is covered
    # in `open_crash_safety_test.exs`.
    Application.put_env(:fathom, :replication_recovery_refusal_ttl_ms, 0)
    Application.put_env(:fathom, :replication_lineage_wire, true)
    Application.put_env(:fathom, :replication_ordinal_wire, true)
    # Four endpoints nothing listens on: an instant refused connect, so silent and never definite.
    Application.put_env(
      :fathom,
      :replication_followers,
      for(n <- 1..4, do: {"dead#{n}", "127.0.0.1", n})
    )

    Fleet.publish([])

    %{shard: shard}
  end

  defp build_db!(rows) do
    path = Path.join(System.tmp_dir!(), "rq_#{System.unique_integer([:positive])}.db")
    {:ok, c} = Connection.open(path)
    :ok = Connection.exec(c, "CREATE TABLE kv (v TEXT)")
    for r <- rows, do: :ok = Connection.exec(c, "INSERT INTO kv VALUES ('#{r}')")
    :ok = Connection.exec(c, "PRAGMA wal_checkpoint(TRUNCATE)")
    Connection.close(c)
    bytes = File.read!(path)
    for s <- ["", "-wal", "-shm"], do: File.rm(path <> s)
    bytes
  end

  defp use_s3!(store) do
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
  end

  defp set_mode!(:legacy) do
    refute Heartbeat.running?()
    :ok
  end

  defp set_mode!(:heartbeat) do
    hb = start_supervised!({Heartbeat, ttl_ms: 30_000})
    _ = :sys.get_state(hb)
    :ok
  end

  defp lock(store, shard),
    do: store |> S3EtagStore.body_of("#{shard}.lock") |> Jason.decode!()

  defp stmt(sql), do: %Stmt{sql: sql, args: []}

  defp refused_open!(shard) do
    {:ok, pid} = Shards.ensure(shard)
    ref = Process.monitor(pid)
    assert_receive {:DOWN, ^ref, :process, ^pid, reason}, 10_000
    assert {:shutdown, {:open_failed, %Recovery.UnderQuorum{}}} = reason
  end

  for mode <- @modes do
    describe "#{mode} mode" do
      setup do: set_mode!(unquote(mode))

      test "a floor refusal keeps the steal-touch provenance, and logs a warning", %{shard: shard} do
        base = build_db!(["base"])
        marker = Storage.encode_lease(Storage.released_marker(%{epoch: 3}, 5))

        store =
          start_supervised!(
            {Agent,
             fn -> S3EtagStore.initial(%{"#{shard}.db" => base, "#{shard}.lock" => marker}) end}
          )

        # A rankable stamp on the stored object, so the responder floor applies.
        Agent.update(
          store,
          &put_in(
            &1,
            [:objects, "#{shard}.db", :meta],
            %{"x-amz-meta-fathom-pos" => "2:1:100:5"}
          )
        )

        use_s3!(store)
        e0 = S3EtagStore.etag_of(store, "#{shard}.db")

        a_bytes = Path.join(System.tmp_dir!(), "rq_a_#{System.unique_integer([:positive])}.db")
        File.write!(a_bytes, base)
        {:ok, ac} = Connection.open(a_bytes)
        :ok = Connection.exec(ac, "INSERT INTO kv VALUES ('a-acked')")
        :ok = Connection.exec(ac, "PRAGMA wal_checkpoint(TRUNCATE)")
        Connection.close(ac)
        on_exit(fn -> for s <- ["", "-wal", "-shm"], do: File.rm(a_bytes <> s) end)

        # Two consecutive refusals (Y, then Z): the second must chain the first's provenance.
        log = ExUnit.CaptureLog.capture_log(fn -> refused_open!(shard) end)

        assert log =~ "open REFUSED after the lease was acquired",
               "an UnderQuorum refusal must take the demoted warning path"

        refute log =~ "open FAILED", "a routine floor refusal must not log at error"
        refute log =~ "(fathom ", "no stacktrace for a routine floor refusal"

        e1 = S3EtagStore.etag_of(store, "#{shard}.db")
        assert e1 != e0, "the fixture needs the refused open's steal to have touched"
        body = lock(store, shard)
        assert body["owner"] == Storage.released_owner()
        assert body["touched_from"] == e0 and body["touched_to"] == e1

        capture_log(fn -> refused_open!(shard) end)
        e2 = S3EtagStore.etag_of(store, "#{shard}.db")
        body = lock(store, shard)
        assert body["touched_from"] == e0 and body["touched_to"] == e2

        # A reopens with its acked local copy: warm, so no recovery is attempted.
        local = Path.join(Shard.data_dir(), "#{shard}.db")
        File.mkdir_p!(Shard.data_dir())
        File.cp!(a_bytes, local)
        File.write!(local <> ".etag", e0)

        capture_log(fn ->
          {:ok, conn} = ShardExecutor.open(shard)
          [{pid, _}] = Registry.lookup(Fathom.ShardRegistry, shard)

          if unquote(mode) == :heartbeat,
            do: assert(is_integer(:sys.get_state(pid).acquire_gen)),
            else: assert(is_nil(:sys.get_state(pid).acquire_gen))

          assert {:ok, %StmtResult{rows: [["a-acked"]]}} =
                   ShardExecutor.execute(conn, stmt("SELECT v FROM kv WHERE v = 'a-acked'")),
                 "A's acked local write must be served, not quarantined behind the older object"

          :ok = ShardExecutor.close(conn)
          Shards.drain(shard, 5_000)
        end)

        assert Path.wildcard(local <> ".forked.*") == []
      end
    end
  end
end
