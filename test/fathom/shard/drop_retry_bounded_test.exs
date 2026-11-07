defmodule Fathom.Shard.DropRetryBoundedTest do
  @moduledoc """
  The drop flush's SECOND upload (`retry_drop_upload/1`, reached when the first 412s with the lock
  still ours) is bounded like the first (expert review 2026-11-07 #49).

  The first drop upload runs in `bounded_upload_for_drop/1`: a task under `settle_yield_ms/0`, so
  `terminate/2` cannot hold the Registry registration through a storage brownout (2026-09-05 #30).
  The retry ran its two HEADs and `upload_for_drop/1` INLINE in the coordinator — checkpoint,
  `quick_check`, compress and a PUT with no deadline of their own, on top of the first attempt's
  20 s, against a 60 s `:shard_shutdown_ms` that brutal-kills mid-terminate. Checkouts then failed
  hard instead of retrying onto a fresh coordinator.

  ## What is pinned

  History: flush A lands server-side with its reply lost (the object moves past the coordinator's
  fence etag), then an idle drop: its upload 412s, the lock is still ours, the object is one of our
  own PUTs, so the retry re-fences and uploads again — and THAT upload hangs. The drain must come
  back within the retry's budget (half of `settle_yield_ms/0`), keep the local copy, and release the
  lock (a dirty marker, `put_outcome_ambiguous?/1` for a timed-out PUT). Both liveness modes, mode
  asserted. Pre-fix the coordinator sleeps in the retry's upload for as long as the store hangs.

  Not async: global storage-fault env and `:shard_shutdown_ms`.
  """
  use ExUnit.Case, async: false

  import ExUnit.CaptureLog

  alias Fathom.{Shard, ShardExecutor, Shards}
  alias Fathom.Shard.{Heartbeat, Storage}
  alias Fathom.Shard.Storage.Local
  alias Fathom.Test.Env
  alias Filo.Stmt

  @modes [:legacy, :heartbeat]

  # settle_yield_ms/0 = shutdown / 3 = 1 s here; the retry's budget is half of it.
  @shutdown_ms 3_000

  setup do
    shard = "dropretry_#{System.unique_integer([:positive])}"

    Env.put!(:shard_storage, Fathom.Test.FaultyStorage)
    Env.put!(:shard_idle_ms, 600_000)
    Env.put!(:shard_flush_interval_ms, 600_000)
    Env.put!(:shard_shutdown_ms, @shutdown_ms)
    Env.put!(:replication_enabled, false)
    Env.put!(:replication_promote_on_open, false)
    Env.put!(:storage_fault, nil)
    Env.put!(:faulty_before, nil)

    on_exit(fn ->
      # A pre-fix coordinator is parked inside the hung upload: do not leave it behind.
      case Registry.lookup(Fathom.ShardRegistry, shard) do
        [{pid, _}] -> Process.exit(pid, :kill)
        _ -> :ok
      end

      for f <- Path.wildcard(Path.join(Shard.data_dir(), "#{shard}.db*")), do: File.rm_rf(f)
      Local.purge_shard(shard)
    end)

    %{shard: shard, db: Shard.db_path(shard)}
  end

  defp set_mode!(:legacy), do: refute(Heartbeat.running?(), "legacy mode needs the heartbeat OFF")

  defp set_mode!(:heartbeat) do
    hb = start_supervised!({Heartbeat, ttl_ms: 30_000})
    _ = :sys.get_state(hb)
    assert Heartbeat.running?()
  end

  defp assert_mode!(pid, :heartbeat), do: assert(is_integer(:sys.get_state(pid).acquire_gen))
  defp assert_mode!(pid, :legacy), do: assert(is_nil(:sys.get_state(pid).acquire_gen))

  defp stmt(sql), do: %Stmt{sql: sql, args: []}
  defp exec!(conn, sql), do: {:ok, _} = ShardExecutor.execute(conn, stmt(sql))

  for mode <- @modes do
    describe "#{mode} mode" do
      setup do
        set_mode!(unquote(mode))
        :ok
      end

      test "a hung retry upload is bounded: the drop keeps the local copy and releases the lock",
           %{shard: shard, db: db} do
        {:ok, conn} = ShardExecutor.open(shard)
        {:ok, pid} = Shards.ensure(shard)
        assert_mode!(pid, unquote(mode))
        exec!(conn, "CREATE TABLE kv (v TEXT)")
        exec!(conn, "INSERT INTO kv VALUES ('base')")
        assert :ok = Shards.flush(shard)
        {:ok, e0} = Local.object_etag(shard)

        # A lands server-side, its reply is lost: the object moves past the coordinator's fence etag.
        Application.put_env(:fathom, :storage_fault, :flush_lands_then_errors)
        exec!(conn, "INSERT INTO kv VALUES ('a')")
        {result, _log} = with_log(fn -> Shards.flush(shard) end)
        assert {:error, _} = result
        assert {:ok, %{etag: ea}} = Storage.object_head(shard)
        assert ea != e0, "fixture: attempt A's PUT must have LANDED"
        Application.delete_env(:fathom, :storage_fault)

        # The idle drop: its first flush passes the hook and 412s; the RETRY's flush hangs.
        exec!(conn, "INSERT INTO kv VALUES ('b')")
        :ok = ShardExecutor.close(conn)
        calls = :counters.new(1, [:atomics])

        Application.put_env(:fathom, :faulty_before, {
          :flush,
          fn
            ^shard ->
              :counters.add(calls, 1, 1)

              if :counters.get(calls, 1) >= 2,
                do: Application.put_env(:fathom, :storage_fault, :flush_hangs)

              :ok

            _other ->
              :ok
          end
        })

        started = System.monotonic_time(:millisecond)
        {drained, _log} = with_log(fn -> Shards.drain(shard, 2_000) end)
        elapsed = System.monotonic_time(:millisecond) - started

        assert :counters.get(calls, 1) >= 2,
               "fixture: the drop never reached the retry's upload (calls: #{:counters.get(calls, 1)})"

        assert drained == {:error, :unflushed_local_copy},
               "the hung retry was not abandoned: drain answered #{inspect(drained)} after " <>
                 "#{elapsed} ms (expert review 2026-11-07 #49)"

        # settle (1 s) bounds the first upload, half of it the retry, a quarter the release.
        assert elapsed < 2_500, "the drop took #{elapsed} ms: the retry upload ran unbounded"

        assert File.exists?(db), "the unflushed local copy must be kept"

        assert Storage.lease_holder(shard) == :free,
               "the timed-out retry left the lock held by this node"

        assert Storage.released_dirty?(shard) == true
      end
    end
  end
end
