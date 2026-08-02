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

  ## These tests do NOT reproduce the race, and that is stated deliberately

  The window is the few microseconds between `Sqlite3.cancel/1` and the `send/2` that followed
  it, and hitting it requires a query to complete inside that gap. These tests pass against the
  unfixed code. They are the INVARIANT guard, not a reproduction: they pin "no `{:timed_out,
  _}` residue ever accumulates in the owner's mailbox", which is what a future change widening
  the window would violate.

  The fix itself is a structural reorder — sending before cancelling makes the message
  provably present before the cancel can let the statement return — so its correctness rests
  on the ordering argument, not on a failing test. Recorded honestly rather than dressed up as
  a regression test that discriminates.

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
