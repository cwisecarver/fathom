defmodule Fathom.ConnectionWatchdogTest do
  @moduledoc """
  The statement-deadline watchdog must not leak `{:timed_out, ref}` into its owner's mailbox
  (expert review 2026-08-01 #46).

  `with_deadline/3` peeks for `{:timed_out, ref}` with `after 0` once the statement returns.
  The watchdog used to `Sqlite3.cancel(conn)` and THEN send that message, so a query finishing
  naturally in the gap between those two statements let the owner reach the peek first, find
  nothing, and return — after which the message landed in a mailbox where nothing would ever
  match it again (the next call uses a fresh `make_ref/0`).

  A stream holds its connection for the stream's life, and the watchdog is per-connection, so
  the residue accumulates in exactly the longest-lived processes.

  ## The reorder narrowed the race; it did not close it — CI proved that

  #46's fix (send before cancel) makes the message provably present before the cancel can let
  the statement return, which covers a query the watchdog actually interrupts. It does NOT cover
  a query that finishes on its own in the instant after the watchdog committed to firing: the
  `after 0` peek runs first, finds nothing, and the message lands afterwards in a mailbox where
  no later peek can match it (every call uses a fresh `make_ref/0`).

  That is not theoretical. `"a long run of fast queries under a deadline"` below drives 400
  queries at a **1 ms** deadline precisely to sit on that boundary, and it FAILED on CI (OTP 29,
  2026-08-03) — a 2–4 core runner widens the window enough to hit it, while an 18-core dev box
  does not. The original version of this moduledoc said these tests pass against the unfixed
  code; on a slow enough machine, one of them does not.

  `with_deadline/3` now sweeps stale `{:timed_out, _}` at ARM, before the fresh ref exists, so
  anything found is necessarily residue. `"a stale timeout message is swept"` pins that
  deterministically — it injects the residue rather than racing for it, and fails without the
  sweep.

  Not async: drives real SQLite connections and flips `:query_timeout_ms`.
  """
  use ExUnit.Case, async: false

  alias Fathom.Shard.Connection

  setup do
    prev = Application.get_env(:fathom, :query_timeout_ms)
    on_exit(fn -> restore(:query_timeout_ms, prev) end)

    path = Path.join(System.tmp_dir!(), "watchdog_#{System.unique_integer([:positive])}.db")
    on_exit(fn -> for s <- ["", "-wal", "-shm"], do: File.rm(path <> s) end)

    {:ok, conn} = Connection.open(path)
    on_exit(fn -> Connection.close(conn) end)

    %{conn: conn}
  end

  defp restore(key, nil), do: Application.delete_env(:fathom, key)
  defp restore(key, value), do: Application.put_env(:fathom, key, value)

  defp mailbox_size do
    {:messages, msgs} = Process.info(self(), :messages)
    Enum.count(msgs, &match?({:timed_out, _}, &1))
  end

  test "a long run of fast queries under a deadline leaves no timeout residue", %{conn: conn} do
    # A deadline short enough that the watchdog is armed and firing near the query's own
    # duration is what opens the race; the queries themselves all succeed.
    Application.put_env(:fathom, :query_timeout_ms, 1)

    :ok = Connection.exec(conn, "CREATE TABLE t (a INTEGER)")

    for i <- 1..200 do
      _ = Connection.query(conn, "INSERT INTO t VALUES (?)", [i])
      _ = Connection.query(conn, "SELECT count(*) FROM t", [])
    end

    assert mailbox_size() == 0,
           "the watchdog leaked {:timed_out, ref} into the owner's mailbox"
  end

  test "a stale timeout message is swept by the next guarded query", %{conn: conn} do
    # Deterministic where the race is not: inject exactly the residue the race produces, then run
    # one ordinary guarded query and assert the mailbox came back clean. Fails without the sweep.
    Application.put_env(:fathom, :query_timeout_ms, 5_000)
    :ok = Connection.exec(conn, "CREATE TABLE t (a INTEGER)")

    send(self(), {:timed_out, make_ref()})
    send(self(), {:timed_out, make_ref()})
    assert mailbox_size() == 2, "the fixture did not plant the residue it is about to check for"

    assert {:ok, _} = Connection.query(conn, "SELECT 1", [])

    assert mailbox_size() == 0,
           "residue from an earlier deadline survives into a long-lived stream's mailbox, one " <>
             "message per near-miss, with nothing that can ever match it"
  end

  test "the sweep does not eat the CURRENT query's timeout", %{conn: conn} do
    # The sweep runs before `ref` exists, so it cannot consume this call's own message — but that
    # is the obvious way to break it, so pin it: a genuine timeout must still be reported.
    Application.put_env(:fathom, :query_timeout_ms, 5)
    :ok = Connection.exec(conn, "CREATE TABLE big (a INTEGER)")
    send(self(), {:timed_out, make_ref()})

    result =
      Connection.query(
        conn,
        "WITH RECURSIVE c(x) AS (SELECT 1 UNION ALL SELECT x+1 FROM c WHERE x < 90000000) " <>
          "SELECT count(*) FROM c",
        []
      )

    assert {:error, :query_timeout} = result
    assert mailbox_size() == 0
  end

  test "a genuine timeout is still reported", %{conn: conn} do
    # The reorder must not break detection: the message has to arrive before the peek.
    Application.put_env(:fathom, :query_timeout_ms, 5)
    :ok = Connection.exec(conn, "CREATE TABLE big (a INTEGER)")

    # A recursive CTE that runs well past the deadline.
    result =
      Connection.query(
        conn,
        "WITH RECURSIVE c(x) AS (SELECT 1 UNION ALL SELECT x+1 FROM c WHERE x < 5000000) " <>
          "SELECT count(*) FROM c",
        []
      )

    assert {:error, :query_timeout} = result
    # And the timeout's own message was consumed by the peek, not left behind.
    assert mailbox_size() == 0
  end

  test "queries keep working after a timeout", %{conn: conn} do
    Application.put_env(:fathom, :query_timeout_ms, 5)
    :ok = Connection.exec(conn, "CREATE TABLE t (a INTEGER)")

    _ =
      Connection.query(
        conn,
        "WITH RECURSIVE c(x) AS (SELECT 1 UNION ALL SELECT x+1 FROM c WHERE x < 5000000) " <>
          "SELECT count(*) FROM c",
        []
      )

    Application.put_env(:fathom, :query_timeout_ms, 5_000)
    assert {:ok, %{rows: [[1]]}} = Connection.query(conn, "SELECT 1", [])
    assert mailbox_size() == 0
  end
end
