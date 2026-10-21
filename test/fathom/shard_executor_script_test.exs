defmodule Fathom.ShardExecutorScriptTest do
  @moduledoc """
  Expert review 2026-10-12 #4. A Hrana `sequence` (`executescript`) was acked as soon as SQLite
  committed it locally: `run_script` never reached `maybe_replicate`/`Session.commit`, so with A2
  replication on (the prod default) the script was neither shipped to a follower quorum nor gated by
  `ack_if_owned`. Scripts now take the same path as `execute/2`: an autocommit script ships before
  the ack, a script that leaves a transaction open ships at its COMMIT, and a failed quorum or an
  unconfirmed owner is a 503 FILO_NO_QUORUM.

  Also pins expert review 2026-10-12 #13: retryable open failures are 503, not 500.
  """
  use ExUnit.Case, async: false
  import ExUnit.CaptureLog

  alias Fathom.{Shard, ShardExecutor, Shards}
  alias Fathom.Shard.Replication.{Fleet, Follower, Session}
  alias Filo.{Error, Stmt}

  setup do
    id = "script_#{System.unique_integer([:positive])}"
    root = Path.join(System.tmp_dir!(), "scriptt_#{System.unique_integer([:positive])}")
    File.mkdir_p!(root)

    keys = [
      :replication_enabled,
      :replication_followers,
      :replication_quorum,
      :replication_deferred_wal_sync,
      :replication_wal_sync_fun
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
      for s <- ["", "-wal", "-shm"], do: File.rm(Shard.db_path(id) <> s)
    end)

    followers =
      for i <- 1..3 do
        name = :"script_f#{i}_#{System.unique_integer([:positive])}"

        pid =
          start_supervised!({Follower, name: name, port: 0, dir: Path.join(root, "f#{i}")},
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
      for({_n, port} <- followers, do: {~c"127.0.0.1", port})
    )

    start_supervised!(Fleet)
    %{id: id, followers: followers}
  end

  defp run(conn, sql), do: ShardExecutor.execute(conn, %Stmt{sql: sql, args: []})

  defp quorum_holds?(followers, id) do
    primary = File.read!(Shard.db_path(id) <> "-wal")
    assert byte_size(primary) > 0, "the primary wrote no WAL -- this test measured nothing"

    Enum.count(followers, fn {n, _} -> File.read(Follower.wal_path(n, id)) == {:ok, primary} end) >=
      2
  end

  defp await_quorum(followers, id, tries \\ 100) do
    cond do
      quorum_holds?(followers, id) ->
        true

      tries == 0 ->
        false

      true ->
        Process.sleep(20)
        await_quorum(followers, id, tries - 1)
    end
  end

  defp open!(id, followers) do
    {:ok, conn} = ShardExecutor.open(id)
    {:ok, _coordinator} = Shards.ensure(id)
    for {name, _} <- followers, do: Follower.seed(name, id, 0, 0, 0, 0)
    {:ok, _} = run(conn, "CREATE TABLE t (k INTEGER PRIMARY KEY, v TEXT)")
    on_exit(fn -> ShardExecutor.close(conn) end)
    conn
  end

  describe "a sequence is quorum-shipped (#4)" do
    test "an autocommit script is on a follower quorum when it is acked", %{id: id, followers: f} do
      conn = open!(id, f)
      assert :ok = ShardExecutor.execute_sequence(conn, "INSERT INTO t VALUES (1, 'a');")
      assert await_quorum(f, id), "a script was acked while no follower quorum held its frames"
    end

    test "a script that leaves a transaction open ships at the COMMIT", %{id: id, followers: f} do
      conn = open!(id, f)

      assert :ok = ShardExecutor.execute_sequence(conn, "BEGIN; INSERT INTO t VALUES (1, 'a');")
      {:ok, _} = run(conn, "COMMIT")
      assert await_quorum(f, id), "the COMMIT closing a script's transaction was acked unshipped"
    end

    # Expert review 2026-10-21 rv6 F4. The ship decision read SQLite's autocommit flag only at
    # the END of the script, so a script that COMMITTED a write and then opened a transaction
    # (`INSERT ...; BEGIN;`) was acked with no quorum ship. The later ROLLBACK is not a commit
    # boundary, so nothing ever shipped it: an acked write no follower held. Checked with NO wait:
    # the invariant is "on a quorum when acked", not "eventually".
    test "a script that commits and then opens a transaction ships before the ack", %{
      id: id,
      followers: f
    } do
      conn = open!(id, f)

      assert :ok = ShardExecutor.execute_sequence(conn, "INSERT INTO t VALUES (1, 'a'); BEGIN;")

      assert quorum_holds?(f, id),
             "a committed script write was acked while no follower quorum held its frames"

      {:ok, _} = run(conn, "ROLLBACK")
    end

    # The F4 fix ships while the script's transaction is still OPEN and holding the write lock.
    # That is safe only because the Session ships the COMMITTED extent, never frames an open
    # transaction spilled to the WAL. Pinned here: a transaction big enough to spill its cache
    # writes uncommitted frames past the commit, the script still returns (no deadlock against
    # the open writer), and no follower received those frames.
    test "a script ending in an open, spilled transaction ships only committed frames", %{
      id: id,
      followers: f
    } do
      conn = open!(id, f)

      sql =
        "INSERT INTO t VALUES (1, 'a'); BEGIN; " <>
          "WITH RECURSIVE n(i) AS (SELECT 2 UNION ALL SELECT i + 1 FROM n WHERE i < 3000) " <>
          "INSERT INTO t SELECT i, hex(randomblob(2000)) FROM n;"

      assert :ok = ShardExecutor.execute_sequence(conn, sql)

      primary = File.stat!(Shard.db_path(id) <> "-wal").size
      shipped = for {n, _} <- f, {:ok, st} <- [File.stat(Follower.wal_path(n, id))], do: st.size
      assert length(shipped) >= 2, "no quorum received the committed INSERT"
      # A precondition, or the test measures nothing: the open transaction DID spill.
      assert primary > Enum.max(shipped) + 1_000_000,
             "the open transaction did not spill frames into the WAL"

      {:ok, _} = run(conn, "ROLLBACK")
    end

    # Same bug, deferred-sync handle (rv6 F4): the commit ran at synchronous=NORMAL and was acked
    # without the fdatasync the mode promises. A failing sync must turn the ack into a 503.
    test "a deferred-sync script that ends in BEGIN is not acked without its fdatasync", %{
      id: id,
      followers: f
    } do
      Application.put_env(:fathom, :replication_deferred_wal_sync, true)
      conn = open!(id, f)
      test = self()

      Application.put_env(:fathom, :replication_wal_sync_fun, fn p ->
        send(test, {:sync, p})
        {:error, {:datasync, :eio}}
      end)

      capture_log(fn ->
        assert {:error, %Error{code: "FILO_WAL_SYNC_FAILED", status: 503}} =
                 ShardExecutor.execute_sequence(conn, "INSERT INTO t VALUES (1, 'a'); BEGIN;"),
               "a NORMAL-mode script commit was acked with no fdatasync"
      end)

      assert_received {:sync, _}
      {:ok, _} = run(conn, "ROLLBACK")
    end

    test "heartbeat mode with an unconfirmed owner refuses the script ack", %{
      id: id,
      followers: f
    } do
      owner = "n_#{System.unique_integer([:positive])}@test"
      hb = start_supervised!({Fathom.Shard.Heartbeat, ttl_ms: 30_000, owner: owner})
      _ = :sys.get_state(hb)

      conn = open!(id, f)
      {:ok, coordinator} = Shards.ensure(id)
      gen = Shard.acquire_gen(coordinator)
      assert is_integer(gen), "the coordinator opened in LEGACY mode; this tests nothing"

      assert :ok = ShardExecutor.execute_sequence(conn, "INSERT INTO t VALUES (1, 'a');")

      # The zombie: past its deadline at the SAME generation (`:not_valid`, not `:revalidate`).
      :sys.replace_state(hb, fn s ->
        Fathom.Shard.Heartbeat.publish_status(%{
          s
          | mono_deadline_ms: System.monotonic_time(:millisecond) - 1
        })
      end)

      assert Fathom.Shard.Heartbeat.valid_for_write?(gen) == :not_valid

      assert {:error, %Error{code: "FILO_NO_QUORUM", status: 503}} =
               ShardExecutor.execute_sequence(conn, "INSERT INTO t VALUES (2, 'b');"),
             "a script on a node that cannot confirm ownership must not be acked"
    end
  end

  # Expert review 2026-10-12 #13: these fell into the 500 FILO_OPEN_FAILED catch-all, so clients
  # did not retry transient storage / lease failures.
  describe "retryable open failures are 503 (#13)" do
    for {label, reason} <- [
          lease_unavailable: {:lease_unavailable, :s3_down},
          timeout: :timeout,
          normal: :normal,
          open_failed_exit: {:open_failed, {:exit, {:timeout, :storage}}}
        ] do
      test "#{label}" do
        reason = unquote(Macro.escape(reason))

        log =
          capture_log([level: :warning], fn ->
            assert %Error{code: "FILO_UNAVAILABLE", status: 503} =
                     ShardExecutor.open_error(reason)
          end)

        assert log =~ inspect(reason), "the reason must be logged server-side"
      end
    end

    test "a raw exception during open stays a permanent 500" do
      capture_log(fn ->
        assert %Error{code: "FILO_OPEN_FAILED", status: 500} =
                 ShardExecutor.open_error({:open_failed, %RuntimeError{message: "boom"}})
      end)
    end
  end
end
