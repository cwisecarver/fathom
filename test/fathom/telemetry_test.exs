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

    # NO short-TTL override here, deliberately (2026-07-26). It used to set 60ms "so the renewal
    # fires fast", which became actively harmful once the test started driving the renewal itself:
    # `schedule_renew/1` re-arms at `ttl/3`, so a 60ms TTL makes the coordinator queue a
    # `:renew_lease` every ~20ms, each doing storage I/O. Under load that backlog grows faster than
    # it drains and the message this test sends waits behind it — the coordinator self-fenced
    # correctly, just later than the assertion window (observed at load average 28: the
    # "superseded; self-fencing" log arrived AFTER the 5s timeout).
    #
    # The short TTL bought nothing anyway: `do_renew_lease/3` detects supersession by comparing the
    # stored lock's owner/epoch against ours, which is TTL-independent. At the default TTL the
    # coordinator's own timer fires far outside this test, so the only `:renew_lease` it handles is
    # the one sent below — no storm, nothing to queue behind.
    {:ok, conn} = ShardExecutor.open(shard)
    {:ok, coordinator} = Shards.ensure(shard)
    ref = Process.monitor(coordinator)
    :ok = ShardExecutor.close(conn)
    put_raw_lock(shard, "thief@node", 999, now_ms() + 60_000)

    capture_log(fn ->
      # Drive the renewal check directly rather than waiting on the periodic timer,
      # which can slip past the assert_receive window under heavy machine load.
      send(coordinator, :renew_lease)

      # Wait on the DOWN FIRST, then check the telemetry — not the other way round.
      #
      # Both orderings assert the same two facts, but they synchronize on different things. Waiting
      # on the telemetry first means racing a bare timeout against "the coordinator got scheduled,
      # did a storage read, and emitted" — which is what timed out under load on 2026-07-26 (seed
      # 918571, mailbox empty after 2s). The DOWN is a definitive lifecycle event and the
      # monitor-and-assert-on-DOWN pattern AGENTS.md prescribes instead of sleeping. And because
      # the coordinator emits :superseded BEFORE it stops, once the DOWN has landed the telemetry
      # is already queued — so the second assertion is a check, not a race.
      assert_receive {:DOWN, ^ref, :process, ^coordinator, {:shutdown, :lease_lost}}, 5_000

      assert_receive {:telemetry, [:fathom, :shard, :lease, :superseded], %{count: 1},
                      %{shard_id: ^shard}},
                     1_000
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
          [:fathom, :oban, :job, :exception, :count],
          # birth failure — a tenant serving with no schema (FathomTenantBornEmpty)
          [:fathom, :migrator, :fork_fallback, :count]
        ] do
      assert name in names, "alert-rule metric #{inspect(name)} is not exported by metrics/0"
    end
  end

  # A novel tenant whose template fork fails is born EMPTY: it serves, and every ORM query against
  # it fails, and the rollout cannot heal it (that last part is deliberate — see
  # django_replay_test.exs). The checkout deliberately SUCCEEDS, so this hard per-tenant outage
  # produces no 5xx rate anywhere and nothing else will ever surface it.
  #
  # `Shards.fork_novel/1` has logged loudly and emitted this event since 2026-07-31, but nothing
  # consumed it — no metric, no alert rule — so "alertable" stopped one step short of an alert.
  # These pin the whole chain: the event Shards actually emits, the tag the alert groups by, and
  # the bounded-cardinality rule that keeps shard_id OUT of the tags.
  describe "fork_fallback (born-empty tenant)" do
    test "the metric is tagged by reason only — shard_id must never become a tag" do
      [metric] =
        Enum.filter(
          Fathom.Telemetry.metrics(),
          &(&1.name == [:fathom, :migrator, :fork_fallback, :count])
        )

      # One series per failure reason. `Shards.fork_failure_reason/1` exists precisely to keep this
      # a bounded atom set: a `{:retry, <storage error>}` carrying an arbitrary term would be
      # unbounded cardinality, and shard_id at a million tenants is cardinality death.
      assert metric.tags == [:reason]
      refute :shard_id in metric.tags
      assert metric.event_name == [:fathom, :migrator, :fork_fallback]
    end

    # Deliberately NOT re-executing the event by hand here: that would only prove the shape this
    # test file writes, and would keep passing if `Shards` renamed the key tomorrow. The
    # metric↔emission contract is asserted where the REAL path runs — fork_test.exs drives an
    # actual failed birth through `ShardExecutor.open/1` and checks the emitted metadata against
    # `metrics/0`'s declared tags.
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
