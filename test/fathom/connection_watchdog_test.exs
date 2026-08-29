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

  # MOVED HERE from `test/fathom/shard/connection_test.exs` (expert review 2026-08-26 #15).
  #
  # It went red once on CI — OTP 29 only, seed 462447 — asserting `{:error, :query_timeout}` and
  # getting `{:ok, _}` from a recursive CTE counting to 200 000 000, a query that cannot possibly
  # finish inside a 60 ms deadline. So the deadline was not in effect. That file is `async: true`
  # and the test mutated the GLOBAL `:query_timeout_ms`; THIS file is `async: false` for precisely
  # that reason, as its moduledoc has always said.
  #
  # The hazard is an async module writing a global that a sync module owns, not the size of the
  # CTE — so the remedy is the move (the same one review #7 made this session for an unnameable
  # flake) and explicitly NOT a longer timeout, which AGENTS.md forbids.
  #
  # The precondition is now asserted rather than assumed: if the deadline is not actually in force
  # the test says so, instead of reporting a watchdog that "did not fire".
  test "the per-connection watchdog re-arms across timeouts and healthy queries", %{conn: conn} do
    Application.put_env(:fathom, :query_timeout_ms, 60)

    assert Application.get_env(:fathom, :query_timeout_ms) == 60,
           "fixture: the statement deadline is not in force, so a slow query cannot time out " <>
             "and this test would be measuring nothing"

    # Slow BY CONSTRUCTION: 200M recursive-CTE iterations cannot complete in 60 ms on any hardware,
    # so this does not race the deadline it is testing.
    slow = """
    WITH RECURSIVE c(x) AS (SELECT 1 UNION ALL SELECT x + 1 FROM c WHERE x < 200000000)
    SELECT count(*) FROM c
    """

    assert {:error, :query_timeout} = Connection.query(conn, slow, [])
    # A healthy query right after the timeout runs clean on the same connection — the 2026-07-18
    # #13 stale-interrupt guarantee.
    assert {:ok, %{rows: [[1]]}} = Connection.query(conn, "SELECT 1", [])
    # A second timeout on the same (re-armed) watchdog fires independently.
    assert {:error, :query_timeout} = Connection.query(conn, slow, [])
    assert {:ok, %{rows: [[2]]}} = Connection.query(conn, "SELECT 2", [])
  end

  # The defect the test above went red on (CI, OTP 29 only, job 98748804302): its SECOND slow
  # query returned `{:ok, %{rows: [[200000000]]}}` — a 200M-iteration recursive CTE that RAN TO
  # COMPLETION under a 60 ms deadline. The deadline was in force (the fixture asserts it), so the
  # watchdog fired; the cancel simply did nothing.
  #
  # Root cause: a cancel issued while no statement is yet running is lost, in BOTH of its
  # mechanisms. `sqlite3_interrupt` with nothing running is a documented no-op that does not
  # affect statements started afterwards, and exqlite's `cancelled` flag is reset at the top of
  # the next db op. The old watchdog cancelled exactly once and then parked, so the query ran
  # unbounded. `Sqlite3.multi_step/3` is a DIRTY NIF and there are only as many dirty-CPU
  # schedulers as cores, so on a loaded 2-core runner the wait for a slot outran the deadline.
  #
  # Deterministic where CI was lucky: rather than racing for a scheduler stall, arm the
  # watchdog by hand and let it fire while the connection is IDLE, which is the same state.
  test "a deadline that fires before the statement starts still bounds the query", %{conn: conn} do
    Application.put_env(:fathom, :query_timeout_ms, 60)

    # One guarded query, so the per-connection watchdog exists and is parked in its outer receive.
    assert {:ok, _} = Connection.query(conn, "SELECT 1", [])

    watchdog = Process.get({Connection, :watchdog, conn})

    assert is_pid(watchdog) and Process.alive?(watchdog),
           "fixture: no per-connection watchdog is armed, so this test would measure nothing"

    # Arm against a ref we never disarm and let it fire with the connection idle.
    ref = make_ref()
    send(watchdog, {:arm, ref, 20, self()})
    assert_receive {:timed_out, ^ref}, 2_000

    # NOW start a statement. Before the fix the watchdog is parked forever on `{:done, ref}`, so
    # this query's own `{:arm, …}` is never processed and nothing bounds it — it runs to
    # completion, exactly as CI observed. After the fix the retried cancel lands on the running
    # statement and errors it. Reported as a plain error, not `:query_timeout`: the watchdog
    # never processed this call's arm, so there is no `{:timed_out, ref}` for the peek to find.
    # What is being pinned is that the WORK is bounded, which is what the knob is for.
    slow = """
    WITH RECURSIVE c(x) AS (SELECT 1 UNION ALL SELECT x + 1 FROM c WHERE x < 20000000)
    SELECT count(*) FROM c
    """

    assert {:error, _} = Connection.query(conn, slow, []),
           "the statement outran its deadline: a cancel issued before it started was lost and " <>
             "never retried, so :query_timeout_ms did not bound the query at all"

    # Release the hand-armed watchdog so it stops cancelling this connection.
    send(watchdog, {:done, ref})
  end

  # The watchdog runs at `:high` priority so a saturated scheduler cannot starve its deadline
  # cancels (CI, OTP 28/29, 2026-08-29, run 33234915078). The failure was the "re-arms" test's
  # SECOND slow query returning `{:ok, rows: [[200000000]]}` — a 200M-row recursive CTE that RAN TO
  # COMPLETION under a 60 ms deadline. `await_disarm/3` re-issues its cancel every @cancel_retry_ms,
  # but that only bounds the query if the watchdog actually gets a scheduler slot to run the cancel;
  # while the owner is blocked in the `multi_step` DIRTY NIF, a normal-priority watchdog on a loaded
  # 2-core runner was not scheduled for the whole life of the query, so no cancel ever landed and
  # the deadline bounded nothing.
  #
  # Deterministic where the contention is not: rather than race a slow query against a stalled
  # scheduler, assert the invariant the fix rests on. Fails against the pre-fix (`:normal`) watchdog.
  test "the per-connection watchdog runs at elevated priority", %{conn: conn} do
    Application.put_env(:fathom, :query_timeout_ms, 60)

    # One guarded query so the per-connection watchdog is spawned and parked.
    assert {:ok, _} = Connection.query(conn, "SELECT 1", [])

    watchdog = Process.get({Connection, :watchdog, conn})

    assert is_pid(watchdog) and Process.alive?(watchdog),
           "fixture: no per-connection watchdog is armed, so this test would measure nothing"

    assert Process.info(watchdog, :priority) == {:priority, :high},
           "a normal-priority watchdog is starved on a saturated runner and its retry cancels " <>
             "never fire, so :query_timeout_ms bounds nothing — see the CI failure this guards"
  end
end
