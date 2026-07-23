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

  # Expert review 2026-07-23 #3: the OTel checkout-span bridge attached by default even with no
  # collector configured — :otel_spans defaulted true while the docs claimed traces were env-gated
  # on OTEL_EXPORTER_OTLP_ENDPOINT, so every checkout built full recording spans exported to
  # nothing. The invariant: with :otel_spans UNSET, the bridge must stay detached (runtime.exs
  # flips it true only alongside a real exporter).
  test "the OTel span bridge defaults OFF when :otel_spans is unset" do
    original = Application.fetch_env(:fathom, :otel_spans)
    Application.delete_env(:fathom, :otel_spans)

    on_exit(fn ->
      case original do
        {:ok, v} -> Application.put_env(:fathom, :otel_spans, v)
        :error -> Application.delete_env(:fathom, :otel_spans)
      end
    end)

    refute Fathom.Telemetry.otel_spans?()
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
      # Drive the renewal check directly rather than waiting on the periodic timer,
      # which can slip past the assert_receive window under heavy machine load.
      send(coordinator, :renew_lease)

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

  # Review #30: the observability package (deploy/observability/alert-rules.yml) references these
  # page-worthy signals. Each already emitted telemetry but wasn't exported to Prometheus, so an
  # adopter's alerting was blind to them. Pin that every alert-rule metric stays defined — removing
  # or renaming one silently breaks a shipped alert rule.
  test "metrics/0 exports the page-worthy signals the shipped alert rules depend on" do
    names = MapSet.new(Fathom.Telemetry.metrics(), & &1.name)

    for name <- [
          # durability / data-loss precursors
          [:fathom, :shard, :corrupt_flush, :count],
          [:fathom, :shard, :fenced_quarantine, :count],
          # capacity / admission refusals
          [:fathom, :shards, :at_capacity, :count],
          [:fathom, :shards, :novel_rate_limited, :count],
          [:fathom, :shards, :evicted, :count],
          # liveness — mass self-fence precursor
          [:fathom, :shard, :heartbeat, :lapsed, :count],
          # live RPO exposure
          [:fathom, :durability, :dirty_shards],
          [:fathom, :durability, :oldest_age_ms],
          # control-plane stall
          [:fathom, :oban, :job, :exception, :count]
        ] do
      assert name in names, "alert-rule metric #{inspect(name)} is not exported by metrics/0"
    end
  end

  test "the Oban failure metric is tagged by queue (low-cardinality) and fires on a real event" do
    [oban_metric] =
      Enum.filter(
        Fathom.Telemetry.metrics(),
        &(&1.name == [:fathom, :oban, :job, :exception, :count])
      )

    assert oban_metric.tags == [:queue]

    # Prove the event_name wiring matches what Oban actually emits (metadata carries :queue).
    attach([[:oban, :job, :exception]])
    :telemetry.execute([:oban, :job, :exception], %{duration: 1}, %{queue: "migrations"})
    assert_receive {:telemetry, [:oban, :job, :exception], _, %{queue: "migrations"}}
  end
end
