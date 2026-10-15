defmodule Fathom.Cluster.WriteFenceSkewTest do
  @moduledoc """
  The write circuit-breaker subtracts the owner's measured clock skew (expert review 2026-10-15
  #22).

  A peer judges the heartbeat's expiry against the STORE's clock, so an owner whose clock lags the
  store by `skew` is stealable `skew` earlier than its own deadline says. The heartbeat tolerates
  such a lag (up to its self-lapse bound) and measures it on every renewal
  (`Heartbeat.measured_skew_ms/0`), but `stealable_after_ms` ignored it: the breaker armed `skew`
  late, acking writes a peer was already entitled to steal from — every one quarantined on heal.

  ## The fixture

  The steal margin is made large (2 s) and the published skew nearly as large (1.9 s), so the gap
  between "subtracts the skew" (~150 ms) and "ignores it" (~2 s) is wide and the timing assertion
  is robust: the fence must arm inside 1 s of the first `:not_valid`.

  Legacy mode has no heartbeat and so no measured skew (`measured_skew_ms/0` is 0 when the heartbeat
  is down); its case pins that the breaker is unchanged there — it must NOT arm early.
  """
  use Fathom.ClusterShardCase, async: false

  alias Fathom.Shard.{Heartbeat, WriteFence}

  @steal_margin 2_000
  @skew 1_900

  setup %{shard: shard} do
    for {k, v} <- [
          fence_writes_when_stealable: true,
          steal_margin_ms: @steal_margin,
          shard_flush_interval_ms: 600_000,
          shard_idle_ms: 600_000,
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
    handler = "fence-skew-#{System.unique_integer([:positive])}"

    :telemetry.attach(
      handler,
      [:fathom, :shard, :write_fenced],
      fn _e, _m, %{shard_id: id}, _ -> if id == shard, do: send(test_pid, :write_fenced) end,
      nil
    )

    on_exit(fn -> :telemetry.detach(handler) end)
    :ok
  end

  defp open_dirty!(shard) do
    {:ok, conn} = ShardExecutor.open(shard)
    {:ok, _} = ShardExecutor.execute(conn, stmt("CREATE TABLE kv (v TEXT)"))
    {:ok, _} = ShardExecutor.execute(conn, stmt("INSERT INTO kv VALUES ('a')"))
    {:ok, coordinator} = Shards.ensure(shard)
    {conn, coordinator}
  end

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

  test "heartbeat mode: an owner lagging the store arms the fence that much earlier", %{
    shard: shard
  } do
    hb = start_supervised!({Heartbeat, ttl_ms: 30_000})
    _ = :sys.get_state(hb)

    capture_log(fn ->
      {conn, coordinator} = open_dirty!(shard)
      assert is_integer(:sys.get_state(coordinator).acquire_gen), "expected heartbeat mode"

      # Same generation, expired deadline (`:not_valid`), and the last renewal measured the owner
      # @skew ms behind the store.
      :sys.replace_state(hb, fn s ->
        Heartbeat.publish_status(%{
          s
          | mono_deadline_ms: System.monotonic_time(:millisecond) - 1_000,
            margin_ms: 50,
            skew_ms: @skew
        })
      end)

      assert Heartbeat.measured_skew_ms() == @skew, "fixture: the skew was not published"

      first_not_valid!(coordinator)

      # margin + steal_margin - skew = 150 ms. Ignoring the skew it is 2,050 ms.
      assert_receive :write_fenced,
                     1_000,
                     "the breaker ignored the owner's #{@skew} ms clock lag and armed late"

      ShardExecutor.close(conn)
    end)
  end

  test "legacy mode: no heartbeat, no measured skew — the breaker does not arm early", %{
    shard: shard
  } do
    refute Heartbeat.running?()
    assert Heartbeat.measured_skew_ms() == 0
    prev = Application.get_env(:fathom, :shard_lease_ttl_ms)
    Application.put_env(:fathom, :shard_lease_ttl_ms, 1_500)

    on_exit(fn ->
      if is_nil(prev),
        do: Application.delete_env(:fathom, :shard_lease_ttl_ms),
        else: Application.put_env(:fathom, :shard_lease_ttl_ms, prev)
    end)

    capture_log(fn ->
      {conn, coordinator} = open_dirty!(shard)
      assert is_nil(:sys.get_state(coordinator).acquire_gen), "expected legacy mode"

      Application.put_env(:fathom, :storage_fault, :renew)
      first_not_valid!(coordinator)

      # margin (ttl/3 = 500) + steal_margin (2,000): nothing before ~2.5 s.
      refute_receive :write_fenced, 1_500
      ShardExecutor.close(conn)
    end)
  end
end
