defmodule Fathom.ReplicationSaturationMetricsTest do
  @moduledoc """
  The A2 saturation signals — `docs/reviews/a2-flush-interval-2026-08-18.md`.

  ## Why these exist at all

  Replication does not degrade gracefully as tenant count rises. Measured on the rig, 512 -> 1024 ->
  2048 tenants gave **3,340 -> 2,776 -> 258 txn/s** — a 10.8x collapse for the last doubling, with
  throughput looking healthy right up to it. So an operator cannot use throughput as a headroom
  signal, and before this the only trace of the thing that DOES track saturation cleanly
  (`:overloaded`, at 0 / ~9k / ~17k across that sweep) was a `Logger.warning` line.

  Two signals, deliberately: `budget.used_ratio` LEADS (the budget filling, nothing refused yet) and
  `reject.count{reason="overloaded"}` LAGS (already refusing tenant writes).
  """
  use ExUnit.Case, async: false

  alias Fathom.Shard.Replication.Budget
  alias Fathom.Shard.Replication.Fleet
  alias Fathom.Shard.Replication.Protocol.Push
  alias Fathom.Shard.Replication.Shipper

  @payload :binary.copy(<<0xAB>>, 4096)

  setup do
    prev_cap = Application.get_env(:fathom, :replication_max_queue_bytes)
    prev_followers = Application.get_env(:fathom, :replication_followers)
    prev_enabled = Application.get_env(:fathom, :replication_enabled)

    name = :"satmetrics_#{System.unique_integer([:positive])}"
    Budget.install(name)
    Fleet.publish([{to_string(name), "127.0.0.1", 9100, name}])

    on_exit(fn ->
      Budget.forget(name)
      Fleet.publish([])

      for {k, v} <- [
            replication_max_queue_bytes: prev_cap,
            replication_followers: prev_followers,
            replication_enabled: prev_enabled
          ] do
        if is_nil(v),
          do: Application.delete_env(:fathom, k),
          else: Application.put_env(:fathom, k, v)
      end
    end)

    %{name: name}
  end

  defp attach(event) do
    test = self()
    ref = make_ref()
    id = "sat-#{inspect(ref)}"

    :telemetry.attach(
      id,
      event,
      fn _e, measurements, meta, _ -> send(test, {ref, measurements, meta}) end,
      nil
    )

    on_exit(fn -> :telemetry.detach(id) end)
    ref
  end

  describe "budget.used_ratio — the leading signal" do
    test "reports what fraction of the node budget is in use", %{name: name} do
      Application.put_env(:fathom, :replication_max_queue_bytes, 10_000)
      Application.put_env(:fathom, :replication_enabled, true)
      Application.put_env(:fathom, :replication_followers, [{~c"127.0.0.1", 9100}])

      ref = attach([:fathom, :replication, :budget])

      assert {:ok, 2_500} = Budget.reserve(name, 2_500)
      Fathom.Admin.Measurements.replication()

      assert_receive {^ref, %{used_bytes: 2_500, max_bytes: 10_000, used_ratio: ratio}, _}, 1_000
      assert_in_delta ratio, 0.25, 0.001

      # And it MOVES with the budget — a gauge pinned at one value would pass a single-sample test
      # while telling an operator nothing.
      assert {:ok, 5_000} = Budget.reserve(name, 5_000)
      Fathom.Admin.Measurements.replication()
      assert_receive {^ref, %{used_ratio: higher}, _}, 1_000
      assert higher > ratio
    end

    test "a disabled bound reports 0.0 rather than dividing by zero", %{name: name} do
      # `max_bytes: 0` means the bound is OFF. A ratio against it is meaningless rather than
      # infinite, and a dashboard must show "no bound" instead of a fake 100% or a crash.
      Application.put_env(:fathom, :replication_max_queue_bytes, 0)
      Application.put_env(:fathom, :replication_enabled, true)
      Application.put_env(:fathom, :replication_followers, [{~c"127.0.0.1", 9100}])

      ref = attach([:fathom, :replication, :budget])
      assert {:ok, 0} = Budget.reserve(name, 5_000)

      Fathom.Admin.Measurements.replication()
      assert_receive {^ref, %{max_bytes: 0, used_ratio: +0.0}, _}, 1_000
    end

    test "nothing is emitted when replication is off" do
      Application.put_env(:fathom, :replication_enabled, false)
      ref = attach([:fathom, :replication, :budget])

      Fathom.Admin.Measurements.replication()
      refute_receive {^ref, _, _}, 200
    end
  end

  describe "reject.count — the lagging signal" do
    test "an over-budget push emits :overloaded, tagged by reason and not by shard" do
      Application.put_env(:fathom, :replication_max_queue_bytes, 1)
      ref = attach([:fathom, :replication, :reject])

      # A shipper that will never connect: `push/2` consults the budget FIRST, in the caller, so the
      # refusal happens before any socket is needed.
      pid = start_supervised!({Shipper, name: :sat_reject_shipper, host: ~c"127.0.0.1", port: 1})
      Budget.install(:sat_reject_shipper)
      Fleet.publish([{"sat", "127.0.0.1", 1, :sat_reject_shipper}])
      on_exit(fn -> Budget.forget(:sat_reject_shipper) end)

      Shipper.push(pid, %Push{
        shard_id: "acme",
        epoch: 1,
        wal_gen: 1,
        salt1: 1,
        offset: 0,
        payload: @payload
      })

      assert_receive {^ref, %{count: 1}, meta}, 1_000
      assert meta.reason == :overloaded

      # THE CARDINALITY PROPERTY, asserted rather than assumed. A shard_id tag would be one series
      # per tenant, which at fathom's stated scale is cardinality death — the same reason
      # `Fathom.ShardLoad` is a read API rather than a metric. If someone adds it, this fails.
      refute Map.has_key?(meta, :shard_id)
      assert Map.keys(meta) == [:reason]
    end
  end
end
