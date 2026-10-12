defmodule Fathom.Cluster.WriteFenceTimerTest do
  @moduledoc """
  The write circuit-breaker arms at `stealable_after_ms` after the FIRST `:not_valid`, not on a
  later flush tick (expert review 2026-10-12 #9).

  ## The defect

  `note_not_valid/1` starts the clock on the first `:skip` but publishes `WriteFence.fence/1` only
  when it is CALLED again past `stealable_after_ms` — and it is called only from a flush fence or a
  lapse verdict. With a 30 s flush interval the fence armed up to ~60 s after the first `:not_valid`
  against a stealable-after of ~15 s, so a replication-off node kept acking writes after a peer was
  entitled to steal the shard, and quarantined them on heal.

  ## What is pinned

    * with NO further flush tick (the interval is set far out), the fence publishes on its own at
      ~`stealable_after_ms` — both liveness modes, mode asserted;
    * the timer re-checks first: a heartbeat that recovered before it fires closes the episode
      instead of fencing a healthy shard (a fence on a clean shard would never be lifted — no flush
      runs and no write can make one).
  """
  use Fathom.ClusterShardCase, async: false

  alias Fathom.Shard.{Heartbeat, WriteFence}

  @modes [:legacy, :heartbeat]

  setup %{shard: shard} do
    for {k, v} <- [
          fence_writes_when_stealable: true,
          steal_margin_ms: 50,
          shard_flush_interval_ms: 600_000,
          shard_idle_ms: 600_000,
          # The double, for its `:renew` fault (the legacy route to `:skip`).
          shard_storage: Fathom.Test.FaultyStorage
        ] do
      prev = Application.get_env(:fathom, k)
      Application.put_env(:fathom, k, v)

      on_exit(fn ->
        if is_nil(prev),
          do: Application.delete_env(:fathom, k),
          else: Application.put_env(:fathom, k, prev)
      end)
    end

    on_exit(fn ->
      Application.delete_env(:fathom, :storage_fault)
      WriteFence.forget(shard)
      Shards.drain(shard, 2_000)
    end)

    test_pid = self()
    handler = "fence-timer-#{System.unique_integer([:positive])}"

    :telemetry.attach(
      handler,
      [:fathom, :shard, :write_fenced],
      fn _e, _m, %{shard_id: id}, _ -> if id == shard, do: send(test_pid, :write_fenced) end,
      nil
    )

    on_exit(fn -> :telemetry.detach(handler) end)
    :ok
  end

  # Legacy: a short lease TTL makes `Heartbeat.margin_ms/0`'s heartbeat-down fallback small, and a
  # failing renew is what turns the flush fence into `:skip`.
  defp set_mode!(:legacy) do
    refute Heartbeat.running?()
    prev = Application.get_env(:fathom, :shard_lease_ttl_ms)
    Application.put_env(:fathom, :shard_lease_ttl_ms, 1_500)

    on_exit(fn ->
      if is_nil(prev),
        do: Application.delete_env(:fathom, :shard_lease_ttl_ms),
        else: Application.put_env(:fathom, :shard_lease_ttl_ms, prev)
    end)

    nil
  end

  defp set_mode!(:heartbeat) do
    hb = start_supervised!({Heartbeat, ttl_ms: 30_000})
    _ = :sys.get_state(hb)
    hb
  end

  defp assert_mode!(pid, :heartbeat), do: assert(is_integer(:sys.get_state(pid).acquire_gen))
  defp assert_mode!(pid, :legacy), do: assert(is_nil(:sys.get_state(pid).acquire_gen))

  # Make the next flush fence answer `:skip`, the cut-from-storage symptom.
  defp go_not_valid!(:legacy, _hb), do: Application.put_env(:fathom, :storage_fault, :renew)

  defp go_not_valid!(:heartbeat, hb) do
    # Same generation, expired deadline: `valid_for_write?/1` answers `:not_valid` (a different
    # generation would route to `:revalidate`). A small margin keeps stealable-after short.
    :sys.replace_state(hb, fn s ->
      Heartbeat.publish_status(%{
        s
        | mono_deadline_ms: System.monotonic_time(:millisecond) - 1_000,
          margin_ms: 50
      })
    end)
  end

  # Deliver exactly ONE fence verdict: re-send the flush tick only until the not-valid clock starts
  # (a tick that lands while a legacy renew task is in flight is deferred, not run), then stop — so
  # nothing but the timer under test can fence afterwards.
  defp first_not_valid!(coordinator) do
    deadline = System.monotonic_time(:millisecond) + 3_000

    since =
      Stream.repeatedly(fn ->
        send(coordinator, :durability_flush)
        Process.sleep(20)
        :sys.get_state(coordinator).not_valid_since
      end)
      |> Enum.find(fn since ->
        is_integer(since) or System.monotonic_time(:millisecond) > deadline
      end)

    assert is_integer(since), "the fixture never produced a :not_valid fence verdict"
  end

  defp open_dirty!(shard) do
    {:ok, conn} = ShardExecutor.open(shard)
    {:ok, _} = ShardExecutor.execute(conn, stmt("CREATE TABLE kv (v TEXT)"))
    {:ok, _} = ShardExecutor.execute(conn, stmt("INSERT INTO kv VALUES ('a')"))
    {:ok, coordinator} = Shards.ensure(shard)
    {conn, coordinator}
  end

  for mode <- @modes do
    test "#{mode} mode: the fence arms at stealable-after with no later flush tick", %{
      shard: shard
    } do
      hb = set_mode!(unquote(mode))

      capture_log(fn ->
        {conn, coordinator} = open_dirty!(shard)
        assert_mode!(coordinator, unquote(mode))

        go_not_valid!(unquote(mode), hb)
        # ONE fence verdict: the first `:not_valid` observation. Nothing else will call it — the
        # flush interval is ten minutes out.
        first_not_valid!(coordinator)

        assert_receive :write_fenced,
                       3_000,
                       "the write fence waited for a later flush tick instead of arming at stealable-after"

        assert WriteFence.fenced?(shard)
        ShardExecutor.close(conn)
      end)
    end
  end

  test "heartbeat mode: a heartbeat that recovered before the timer closes the episode", %{
    shard: shard
  } do
    hb = set_mode!(:heartbeat)

    capture_log(fn ->
      {conn, coordinator} = open_dirty!(shard)
      assert_mode!(coordinator, :heartbeat)

      go_not_valid!(:heartbeat, hb)
      first_not_valid!(coordinator)

      # Recovered at the SAME generation (no lapse broadcast will come to clear it).
      :sys.replace_state(hb, fn s ->
        Heartbeat.publish_status(%{
          s
          | mono_deadline_ms: System.monotonic_time(:millisecond) + 60_000,
            margin_ms: 50
        })
      end)

      refute_receive :write_fenced, 500
      refute WriteFence.fenced?(shard)
      assert :sys.get_state(coordinator).not_valid_since == nil
      ShardExecutor.close(conn)
    end)
  end
end
