defmodule Fathom.ShardDropFlushBudgetTest do
  @moduledoc """
  Expert review 2026-09-05 #30. A SELF-INITIATED stop (idle-drop, drain, lease_lost) runs
  `flush_then_drop/1` inline in `terminate/2`, and the coordinator stays Registry-registered until it
  exits — so under a storage brownout a concurrent `Shards.checkout` GenServer.call queues on it up to
  `checkout_timeout` (75 s) and then returns `{:error, :timeout}`, which `retry_checkout?/1` EXCLUDES,
  so the tenant gets a hard FILO_SHARD_OPEN 500 instead of retrying onto a fresh coordinator.

  The fix bounds the drop-flush's upload at `settle_yield_ms/0` and abandons it to
  `keep_local_release_lease/3` on timeout, so the coordinator exits promptly and the checkout retries.

  INVARIANT PINNED: with the flush artificially delayed past both the budget AND the checkout timeout,
  a checkout arriving during the drop must still succeed on a fresh coordinator — it must NOT return
  `{:error, :timeout}`. Fails pre-fix (unbounded drop → the checkout times out). Verified by reverting
  `bounded_upload_for_drop/1` back to `upload_for_drop/1`.

  Timing (all << the real defaults, so the test is quick and deterministic):
  `shard_shutdown_ms: 600` ⇒ `settle_yield_ms == 200`; `checkout_timeout: 500` (> the 200 budget so a
  post-fix checkout waits it out, < the 2000 delay so a pre-fix checkout times out); the flush is
  delayed 2000 ms to stand in for the brownout.
  """
  use ExUnit.Case, async: false

  alias Fathom.{ShardExecutor, Shards}
  alias Fathom.Shard.Storage
  alias Filo.Stmt

  @env [
    shard_storage: Fathom.Test.FaultyStorage,
    shard_shutdown_ms: 600,
    shard_checkout_timeout_ms: 500,
    shard_idle_ms: 1,
    shard_flush_interval_ms: 60_000,
    storage_flush_delay_ms: 2_000
  ]

  defp stmt(sql, args \\ []), do: %Stmt{sql: sql, args: args}

  setup do
    shard = "dropbudget_#{System.unique_integer([:positive])}"
    prev = Map.new(@env, fn {k, _} -> {k, Application.get_env(:fathom, k)} end)
    prev = Map.put(prev, :faulty_before, Application.get_env(:fathom, :faulty_before))
    for {k, v} <- @env, do: Application.put_env(:fathom, k, v)

    on_exit(fn ->
      for {k, v} <- prev do
        if is_nil(v),
          do: Application.delete_env(:fathom, k),
          else: Application.put_env(:fathom, k, v)
      end

      for suffix <- [".db", ".db-wal", ".db-shm", ".lock"],
          do: File.rm(Path.join(Storage.Local.dir(), shard <> suffix))
    end)

    %{shard: shard}
  end

  test "a checkout during a browned-out self-drop-flush retries onto a fresh coordinator, not a 500",
       %{shard: shard} do
    test_pid = self()

    # Signal (for MY shard only, via the 1-arity hook) the moment the drop-flush's upload begins —
    # then the coordinator is inside its bounded Task.yield and a checkout will queue behind its
    # still-registered pid, which is the exact window #30 is about.
    Application.put_env(
      :fathom,
      :faulty_before,
      {:flush, fn id -> if id == shard, do: send(test_pid, :flush_entered) end}
    )

    {:ok, conn} = ShardExecutor.open(shard)
    {:ok, _} = ShardExecutor.execute(conn, stmt("CREATE TABLE kv (v TEXT)"))
    {:ok, _} = ShardExecutor.execute(conn, stmt("INSERT INTO kv VALUES ('a')"))

    # Close the last stream → the 1 ms idle timer fires → terminate/2 → flush_then_drop → the delayed
    # upload. The shard is dirty, so this takes flush_then_drop (not drop_clean).
    :ok = ShardExecutor.close(conn)

    assert_receive :flush_entered, 2_000

    # The coordinator is now blocked in its bounded drop-flush. A checkout must NOT hard-timeout; it
    # queues briefly (~the budget), the coordinator abandons the flush + releases, and the checkout
    # retries onto a fresh coordinator.
    assert {:ok, pid, ref, _path} = Shards.checkout(shard),
           "a checkout queued behind a browned-out self-drop-flush returned an error instead of " <>
             "retrying onto a fresh coordinator — the #30 availability cliff"

    Fathom.Shard.checkin(pid, ref)
  end
end
