defmodule Fathom.DirectoryWalBenchTest do
  @moduledoc """
  Postgres WAL bytes per row for `Directory.record_batch/1` (perf review 2026-10-14 #1, #5).

  The Recorder's touch write is the directory's hottest Postgres path, and its cost is WAL. The old
  `INSERT ... ON CONFLICT DO UPDATE ... WHERE <30 s guard>` locked the conflicting tuple before
  evaluating the WHERE, so a guard-REJECTED touch (about 29 of every 30) still wrote ~60 B of WAL
  per row (measured, PG 18, steady state; 160 B on the first pass after a checkpoint). The
  UPDATE ... FROM unnest + `ON CONFLICT DO NOTHING` path writes ~1 B per rejected row.

  `@tag :bench`: a WAL-byte ceiling, not an exact figure. Run with `mix test --include bench`.

  `pg_current_wal_insert_lsn()` (not `pg_current_wal_lsn()`, which lags the unflushed tail inside a transaction) is CLUSTER-wide: another session writing WAL during the run (a parallel test
  partition, autovacuum) lands in the delta and only ever INFLATES it. So each figure is the MINIMUM
  over several trials, the closest observable to the statement's own WAL. (PG 18's per-backend
  `pg_stat_get_backend_wal` is exact but its counters are not visible inside the sandbox transaction.)
  """
  use Fathom.DataCase, async: false

  alias Fathom.{Directory, Repo}

  @moduletag :bench

  @n 5_000
  @trials 5

  defp wal_lsn, do: Repo.query!("SELECT pg_current_wal_insert_lsn()::text").rows |> hd() |> hd()

  defp wal_bytes(fun, prep) do
    prep.()
    from = wal_lsn()
    result = fun.()
    to = wal_lsn()

    %{rows: [[bytes]]} =
      Repo.query!("SELECT ($1::text::pg_lsn - $2::text::pg_lsn)::bigint", [to, from])

    {bytes, result}
  end

  defp min_wal_per_row(fun, prep \\ fn -> :ok end) do
    1..@trials
    |> Enum.map(fn _ -> wal_bytes(fun, prep) end)
    |> Enum.min_by(&elem(&1, 0))
    |> then(fn {bytes, result} -> {bytes / @n, result} end)
  end

  defp seed(last_active_at) do
    Repo.query!(
      """
      INSERT INTO shards (shard_id, schema_version, status, last_active_at, inserted_at, updated_at)
      SELECT 'walb_' || g, 0, 'active', $1, now(), now() FROM generate_series(1, $2) g
      """,
      [last_active_at, @n]
    )

    Repo.query!("ANALYZE shards")
  end

  defp entries(at), do: for(g <- 1..@n, do: {"walb_#{g}", at})

  # COMMITTED rows, outside the sandbox transaction. Inside it the rows are inserted by the test's own
  # transaction, and locking a tuple you inserted yourself is far cheaper than locking a committed one:
  # the ON CONFLICT path measured 9.5 B/row there against ~59 B/row on committed rows, so the sandboxed
  # version of this test passed against the unfixed code (it measured nothing).
  defp committed(fun) do
    on_exit(fn ->
      Ecto.Adapters.SQL.Sandbox.unboxed_run(Repo, fn ->
        Repo.query!("DELETE FROM shards WHERE shard_id LIKE 'walb_%'")
      end)
    end)

    Ecto.Adapters.SQL.Sandbox.unboxed_run(Repo, fun)
  end

  test "guard-rejected touches write (almost) no WAL" do
    committed(&guard_rejected/0)
  end

  test "written touches stay near one row version of WAL" do
    committed(&written/0)
  end

  defp guard_rejected do
    base = DateTime.utc_now()
    seed(base)

    # Precondition: every row exists, and a touch 5 s later is inside the 30 s guard.
    assert %{rows: [[@n]]} =
             Repo.query!("SELECT count(*) FROM shards WHERE shard_id LIKE 'walb_%'")

    rejected = entries(DateTime.add(base, 5, :second))

    {per_row, written} = min_wal_per_row(fn -> Directory.record_batch(rejected) end)
    assert written == 0, "the guard must reject every row, or this measures nothing"

    IO.puts("record_batch guard-rejected: #{Float.round(per_row, 1)} WAL B/row")
    # Old ON CONFLICT path: ~60 B/row. New: ~1 B/row. Ceiling well between the two.
    assert per_row < 30
  end

  defp written do
    base = DateTime.utc_now()
    seed(DateTime.add(base, -3600, :second))

    stale = entries(base)

    {per_row, written} =
      min_wal_per_row(
        fn -> Directory.record_batch(stale) end,
        # Re-age every row (outside the measured window) so each trial writes all of them.
        fn ->
          Repo.query!("UPDATE shards SET last_active_at = $1 WHERE shard_id LIKE 'walb_%'", [
            DateTime.add(base, -3600, :second)
          ])
        end
      )

    assert written == @n, "every row is an hour stale, so every row must be written"

    IO.puts("record_batch written: #{Float.round(per_row, 1)} WAL B/row")

    # Order-of-magnitude bound (measured ~600-730 B/row with indexes); catches a pathological rewrite.
    assert per_row < 2_000
  end
end
