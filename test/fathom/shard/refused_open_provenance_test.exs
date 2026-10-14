defmodule Fathom.Shard.RefusedOpenProvenanceTest do
  @moduledoc """
  A steal-touch made by an open that then FAILED before serving is recorded in the marker it leaves,
  so the previous holder's acked local copy is not read as a fork (expert review 2026-10-13 #2,
  completing 975b80c7 / expert review 2026-10-12 #10).

  ## The defect

  975b80c7 attaches touch provenance (`touched_from`/`touched_to`) only in `release_clean/1`. Holder A
  failed its drop flush and left a released marker with sidecar E0 on its disk. Peer Y steals the
  marker (touch E0 -> E1) and its open then FAILS after the acquire: the cold pull errors (the
  `pull_failed` branch of `open_with_lease/8`) or promote-on-open refuses with `UnderQuorum`
  (`abandon_open/5`, routine since eb3d226a: every floor refusal, repeated per client retry for up to
  the 60 s window). Both released with the raw lease, so the marker carried no provenance and the
  next stealer's `touch_origin/2` chain restarted. A's reopen then saw sidecar E0 against store
  E1/E2 with nothing explaining E0: `Fork` said `:diverged`, A's acked copy went to `.forked.<ts>`
  and the OLDER stored object served.

  ## What is pinned

  The refusing open wrote and served nothing, so forgiving its touches is exactly as safe as in
  `release_clean/1`. After one refusal AND after a chain of two, A's acked local copy is served warm
  and nothing is quarantined. Both liveness modes (mode asserted). Driven over the real S3 backend
  against the MD5-faithful `S3EtagStore`.
  """
  use ExUnit.Case, async: false

  import ExUnit.CaptureLog

  alias Fathom.{Shard, ShardExecutor, Shards}
  alias Fathom.Shard.{Connection, Heartbeat, Storage}
  alias Fathom.Shard.Storage.S3
  alias Fathom.Test.S3EtagStore
  alias Filo.{Stmt, StmtResult}

  @modes [:legacy, :heartbeat]

  setup do
    shard = "refused_#{System.unique_integer([:positive])}"

    for k <- [:shard_storage, S3, :shard_idle_ms, :replication_enabled] do
      prev = Application.get_env(:fathom, k)

      on_exit(fn ->
        if is_nil(prev),
          do: Application.delete_env(:fathom, k),
          else: Application.put_env(:fathom, k, prev)
      end)
    end

    Application.put_env(:fathom, :shard_idle_ms, 600_000)

    # The marker and its claim exist only while replicating (d80e554, expert review 2026-10-12 #3).
    Application.put_env(:fathom, :replication_enabled, true)

    on_exit(fn ->
      Shards.drain(shard, 2_000)
      for f <- Path.wildcard(Path.join(Shard.data_dir(), "#{shard}*")), do: File.rm_rf(f)
    end)

    %{shard: shard}
  end

  defp build_db!(rows) do
    path = Path.join(System.tmp_dir!(), "rf_#{System.unique_integer([:positive])}.db")
    {:ok, c} = Connection.open(path)
    :ok = Connection.exec(c, "CREATE TABLE kv (v TEXT)")
    for r <- rows, do: :ok = Connection.exec(c, "INSERT INTO kv VALUES ('#{r}')")
    :ok = Connection.exec(c, "PRAGMA wal_checkpoint(TRUNCATE)")
    Connection.close(c)
    bytes = File.read!(path)
    for s <- ["", "-wal", "-shm"], do: File.rm(path <> s)
    bytes
  end

  # `fail_pull` is an Agent flag: while true, the pull task's GET of the shard's data object answers 403
  # (not retried by Req), which fails the open's cold pull AFTER the acquire has already touched.
  defp use_s3!(store, fail_pull) do
    Application.put_env(:fathom, S3,
      bucket: "b",
      region: "us-east-1",
      access_key_id: "k",
      secret_access_key: "s",
      endpoint: "https://s3.example",
      path_style: true,
      req_plug: fn conn ->
        # Only the pull TASK: the steal-touch's own nonce-touch GET runs in the coordinator process
        # (the one registered under the shard id) and must succeed.
        if conn.method == "GET" and String.ends_with?(conn.request_path, ".db") and
             Agent.get(fail_pull, & &1) and Registry.keys(Fathom.ShardRegistry, self()) == [] do
          Plug.Conn.send_resp(conn, 403, "denied")
        else
          S3EtagStore.serve(conn, store)
        end
      end
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

  # One open that steals the marker and then fails its pull. Returns the stop reason.
  defp refused_open!(shard) do
    {:ok, pid} = Shards.ensure(shard)
    ref = Process.monitor(pid)
    assert_receive {:DOWN, ^ref, :process, ^pid, reason}, 10_000
    assert {:shutdown, {:pull_failed, _}} = reason
    reason
  end

  for mode <- @modes do
    describe "#{mode} mode" do
      setup do: set_mode!(unquote(mode))

      test "after one and after TWO refused opens, A's acked local copy is not a fork", %{
        shard: shard
      } do
        base = build_db!(["base"])
        # A failed its drop flush: a released marker carrying A's claim.
        marker = Storage.encode_lease(Storage.released_marker(%{epoch: 3}, 5))

        store =
          start_supervised!(
            {Agent,
             fn -> S3EtagStore.initial(%{"#{shard}.db" => base, "#{shard}.lock" => marker}) end}
          )

        fail_pull = start_supervised!({Agent, fn -> true end}, id: :fail_pull)
        use_s3!(store, fail_pull)
        e0 = S3EtagStore.etag_of(store, "#{shard}.db")

        # A's local copy: the stored bytes plus an acked write that never became durable (sidecar E0).
        a_bytes = Path.join(System.tmp_dir!(), "rf_a_#{System.unique_integer([:positive])}.db")
        File.write!(a_bytes, base)
        {:ok, ac} = Connection.open(a_bytes)
        :ok = Connection.exec(ac, "INSERT INTO kv VALUES ('a-acked')")
        :ok = Connection.exec(ac, "PRAGMA wal_checkpoint(TRUNCATE)")
        Connection.close(ac)
        on_exit(fn -> for s <- ["", "-wal", "-shm"], do: File.rm(a_bytes <> s) end)

        # Y refuses (steal-touch E0 -> E1, pull fails), then Z refuses (E1 -> E2).
        capture_log(fn -> refused_open!(shard) end)
        e1 = S3EtagStore.etag_of(store, "#{shard}.db")
        assert e1 != e0, "the fixture needs the refused open's steal to have touched"
        body = lock(store, shard)
        assert body["owner"] == Storage.released_owner(), "the pull failure must leave a marker"
        assert body["touched_from"] == e0 and body["touched_to"] == e1

        capture_log(fn -> refused_open!(shard) end)
        e2 = S3EtagStore.etag_of(store, "#{shard}.db")
        assert e2 not in [e0, e1]
        body = lock(store, shard)

        assert body["touched_from"] == e0 and body["touched_to"] == e2,
               "the second refusal must CHAIN the first's provenance (touch_origin/2), not restart it"

        # A reopens with its acked local copy.
        Agent.update(fail_pull, fn _ -> false end)
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

        assert Path.wildcard(local <> ".forked.*") == [],
               "only content-identical touches happened since A's sidecar: that is not a fork"
      end
    end
  end
end
