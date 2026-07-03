defmodule Fathom.Shard.TakeoverRevalidationTest do
  # Expert review #3: a steal never invalidated the DATA object, and the takeover's
  # speculative pull (started before the lease confirmed) could capture bytes from
  # BEFORE the old owner's final in-flight flush landed. The new owner then served a
  # copy missing acknowledged writes (stale reads) and its own first flush 412'd
  # against the newer object — self-fencing away its accepted writes. Both sides
  # lost. The invariant: an open that TOOK OVER an existing lock revalidates its
  # pulled etag against the store and re-pulls on mismatch, so the new owner serves
  # the old owner's last acknowledged writes. Not async: shards/storage are global.
  use ExUnit.Case, async: false

  import ExUnit.CaptureLog

  alias Fathom.{ShardExecutor, Shards}
  alias Fathom.Shard.{Connection, Storage}
  alias Filo.{Stmt, StmtResult}

  @local_dir Path.join(System.tmp_dir!(), "fathom_shards")
  @remote_dir Path.join(System.tmp_dir!(), "fathom_remote_test")

  setup do
    shard = "takeover_#{System.unique_integer([:positive])}"
    prev_storage = Application.get_env(:fathom, :shard_storage)

    on_exit(fn ->
      Application.delete_env(:fathom, :faulty_before)

      if prev_storage,
        do: Application.put_env(:fathom, :shard_storage, prev_storage),
        else: Application.delete_env(:fathom, :shard_storage)

      for dir <- [@local_dir, @remote_dir],
          suffix <- [".db", ".db-wal", ".db-shm", ".db.etag", ".lock"],
          do: File.rm(Path.join(dir, shard <> suffix))
    end)

    %{shard: shard}
  end

  defp stmt(sql), do: %Stmt{sql: sql, args: []}

  defp seed_remote!(shard, marker) do
    seed = Path.join(System.tmp_dir!(), "tk_#{shard}_#{System.unique_integer([:positive])}.db")
    {:ok, c} = Connection.open(seed)
    :ok = Connection.exec(c, "CREATE TABLE kv (v TEXT)")
    :ok = Connection.exec(c, "INSERT INTO kv VALUES ('#{marker}')")
    :ok = Connection.exec(c, "PRAGMA wal_checkpoint(TRUNCATE)")
    Connection.close(c)
    :ok = Storage.flush(shard, seed)
    for s <- ["", "-wal", "-shm"], do: File.rm(seed <> s)
    :ok
  end

  test "a takeover's stale speculative pull is revalidated and re-pulled", %{shard: shard} do
    # The object as the pull will first see it (pre-zombie-flush bytes).
    seed_remote!(shard, "stale-pre-flush")

    # A dead foreign owner's lock (no heartbeat, stale TTL) forces the STEAL branch.
    File.mkdir_p!(@remote_dir)

    File.write!(
      Path.join(@remote_dir, "#{shard}.lock"),
      Jason.encode!(%{
        "owner" => "dead@node#oldinc",
        "epoch" => 5,
        "expires_at_ms" => System.system_time(:millisecond) - Storage.steal_margin_ms() - 60_000
      })
    )

    # The zombie's final flush lands AFTER our speculative pull has read the object
    # but BEFORE the lease confirms: the faulty hook runs inside acquire_lease, and
    # waits for the pull temp to exist (the pull captured the stale bytes) before
    # overwriting the object with the zombie's last acknowledged write.
    Application.put_env(:fathom, :shard_storage, Fathom.Test.FaultyStorage)

    test_pid = self()
    temp = Path.join(@local_dir, "#{shard}.db.pull")

    zombie_flush = fn ->
      wait_for = fn wait_for, tries ->
        cond do
          File.exists?(temp) -> :ok
          tries <= 0 -> send(test_pid, :pull_temp_never_appeared)
          true -> Process.sleep(5) && wait_for.(wait_for, tries - 1)
        end
      end

      wait_for.(wait_for, 400)
      seed_remote!(shard, "zombie-final-write")
      :ok
    end

    Application.put_env(:fathom, :faulty_before, {:acquire, zombie_flush})

    capture_log(fn ->
      {:ok, conn} = ShardExecutor.open(shard)
      refute_received :pull_temp_never_appeared

      # The takeover revalidated and re-pulled: the zombie's final acknowledged
      # write is served, not the stale pre-flush copy.
      assert {:ok, %StmtResult{rows: [["zombie-final-write"]]}} =
               ShardExecutor.execute(conn, stmt("SELECT v FROM kv")),
             "a takeover must serve the old owner's last acknowledged writes"

      ShardExecutor.close(conn)
      {:ok, pid} = Shards.ensure(shard)
      ref = Process.monitor(pid)
      _ = Fathom.Shards.drain(shard)

      receive do
        {:DOWN, ^ref, :process, ^pid, _} -> :ok
      after
        3_000 -> :ok
      end
    end)
  end
end
