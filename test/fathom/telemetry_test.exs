defmodule Fathom.TelemetryTest do
  # Cluster phase (S5): the shard/lease/checkout paths emit the :telemetry events that
  # Fathom.Telemetry turns into metrics (and the checkout span into an OTel trace). This
  # verifies the events actually fire with the expected measurements/metadata — no exporter or
  # collector needed. Helpers + setup from Fathom.ClusterShardCase.
  use Fathom.ClusterShardCase

  # Named handler (telemetry warns on anonymous-fun handlers).
  def forward(event, measurements, meta, %{pid: pid}) do
    send(pid, {:telemetry, event, measurements, meta})
  end

  defp attach(events) do
    id = "ttest-#{System.unique_integer([:positive])}"
    :telemetry.attach_many(id, events, &__MODULE__.forward/4, %{pid: self()})
    on_exit(fn -> :telemetry.detach(id) end)
  end

  test "a fresh open emits lease-acquired, cold-open, and a checkout-stop span", %{shard: shard} do
    attach([
      [:fathom, :shard, :lease, :acquired],
      [:fathom, :shard, :cold_open],
      [:fathom, :shards, :checkout, :stop]
    ])

    Application.put_env(:fathom, :shard_idle_ms, 50)

    serve(shard, fn conn ->
      {:ok, _} = ShardExecutor.execute(conn, stmt("CREATE TABLE kv (v TEXT)"))
    end)

    assert_receive {:telemetry, [:fathom, :shard, :lease, :acquired], %{count: 1},
                    %{shard_id: ^shard, epoch: 1}}

    assert_receive {:telemetry, [:fathom, :shard, :cold_open], %{duration: duration},
                    %{shard_id: ^shard, warm: false}}

    assert is_integer(duration) and duration >= 0

    assert_receive {:telemetry, [:fathom, :shards, :checkout, :stop], %{duration: _},
                    %{shard_id: ^shard, outcome: :ok}}
  end

  test "a lease stolen out from under a live coordinator emits :superseded", %{shard: shard} do
    attach([[:fathom, :shard, :lease, :superseded]])
    # Short TTL so the renewal fires fast and detects the steal (idle stays long).
    Application.put_env(:fathom, :shard_lease_ttl_ms, 60)

    {:ok, conn} = ShardExecutor.open(shard)
    {:ok, coordinator} = Shards.ensure(shard)
    ref = Process.monitor(coordinator)
    :ok = ShardExecutor.close(conn)
    put_raw_lock(shard, "thief@node", 999, now_ms() + 60_000)

    capture_log(fn ->
      assert_receive {:telemetry, [:fathom, :shard, :lease, :superseded], %{count: 1},
                      %{shard_id: ^shard}},
                     2_000

      assert_receive {:DOWN, ^ref, :process, ^coordinator, {:shutdown, :lease_lost}}, 2_000
    end)
  end

  test "the active-shard poller measurement emits a gauge" do
    attach([[:fathom, :shards]])
    Fathom.Telemetry.measure_active_shards()
    assert_receive {:telemetry, [:fathom, :shards], %{active: count}, _meta}
    assert is_integer(count) and count >= 0
  end

  test "Fathom.Telemetry.metrics/0 defines the cluster metrics" do
    names = Enum.map(Fathom.Telemetry.metrics(), & &1.name)
    assert [:fathom, :shard, :cold_open, :duration] in names
    assert [:fathom, :shard, :lease, :renewed, :count] in names
    assert [:fathom, :shard, :lease, :superseded, :count] in names
    assert [:fathom, :shards, :active] in names
  end
end
