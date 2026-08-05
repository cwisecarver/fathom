defmodule Fathom.Admin.DiskMeasurementTest do
  @moduledoc """
  Expert review 2026-08-01 **#36**: nothing in the metrics layer read the filesystem.
  `Fathom.Admin.Measurements` had four pollers and none of them touched disk;
  `fathom.storage.bytes` is *S3* usage. Meanwhile the warm-follower cache — the one component
  deliberately sized to fill disk — is budgeted in shard **count** (`:warm_cache_max`, default
  500), which is 8 MB or 2 TB depending on tenant size, reconciled against nothing.

  Why this is worse than an ordinary capacity gap: a full volume fails every cold-open `pull` AND
  every dirty shard's `VACUUM INTO`, so writes keep being **acked** and can never be made durable.
  The symptoms that surface (`fathom.shard.flush.failed`, `fathom.durability.oldest_age_ms`) are
  the same ones an S3 credential or reachability problem produces, so the diagnostic path from
  symptom to cause actively pointed the wrong way.
  """
  use ExUnit.Case, async: false

  alias Fathom.Admin.Measurements
  alias Fathom.Shard.WarmFollower

  defp attach(event) do
    handler = "disk-#{System.unique_integer([:positive])}"
    parent = self()

    :telemetry.attach(
      handler,
      event,
      fn _e, meas, meta, _ -> send(parent, {:event, meas, meta}) end,
      nil
    )

    on_exit(fn -> :telemetry.detach(handler) end)
  end

  defp put_env(key, value) do
    prev = Application.fetch_env(:fathom, key)
    Application.put_env(:fathom, key, value)

    on_exit(fn ->
      case prev do
        {:ok, v} -> Application.put_env(:fathom, key, v)
        :error -> Application.delete_env(:fathom, key)
      end
    end)
  end

  describe "the disk gauge" do
    test "emits free/total/used_ratio for the shard data dir" do
      attach([:fathom, :node, :disk])

      assert :ok = Measurements.disk()

      assert_receive {:event, meas, %{dir: "data"}}, 2_000
      assert meas.total_bytes > 0, "a real volume reports a size"
      assert meas.free_bytes >= 0
      assert meas.used_ratio >= 0.0 and meas.used_ratio <= 1.0
      assert meas.free_bytes <= meas.total_bytes
    end

    # The warm cache is the component this finding is actually about, but it is off by default —
    # so its gauge must appear only when the node opted into the standby role, rather than
    # reporting on a directory nothing writes to.
    test "reports the warm-cache volume only when the follower is enabled" do
      attach([:fathom, :node, :disk])
      put_env(:warm_follower, false)
      Measurements.disk()
      assert_receive {:event, _, %{dir: "data"}}, 2_000
      refute_receive {:event, _, %{dir: "warm"}}, 200

      attach([:fathom, :node, :disk])
      put_env(:warm_follower, true)
      put_env(:warm_cache_dir, System.tmp_dir!())
      Measurements.disk()
      assert_receive {:event, _, %{dir: "warm"}}, 2_000
    end

    # SHARD_DATA_DIR and the warm cache are created lazily, so on a freshly booted node the
    # directory does not exist yet — which is exactly when disk headroom is most worth knowing
    # (that node is about to pull its working set). Resolving to the nearest existing ancestor
    # measures the same filesystem; without it the gauge was blind precisely then.
    test "a not-yet-created directory reports the volume that will hold it" do
      missing =
        Path.join(System.tmp_dir!(), "fathom_absent_#{System.unique_integer([:positive])}")

      refute File.exists?(missing)

      assert {:ok, %{total_bytes: total}} = Measurements.disk_info(missing)
      assert total > 0

      {:ok, %{total_bytes: parent_total}} = Measurements.disk_info(System.tmp_dir!())
      assert total == parent_total, "it must measure the parent volume, not invent a number"
    end
  end

  describe "warm-cache disk back-pressure" do
    setup do
      put_env(:warm_follower, true)
      put_env(:warm_cache_dir, System.tmp_dir!())
      :ok
    end

    test "there is headroom under a normal floor" do
      put_env(:warm_disk_free_floor_bytes, 1)
      assert WarmFollower.disk_headroom?()
    end

    # The brake itself: a floor larger than the volume can ever provide must stop new warming.
    test "a floor above the volume's free space withholds headroom" do
      {:ok, %{total_bytes: total}} = Measurements.disk_info(System.tmp_dir!())
      put_env(:warm_disk_free_floor_bytes, total * 2)

      refute WarmFollower.disk_headroom?(),
             "the follower kept warming with the volume below its free floor"
    end

    # The byte budget is the second brake, and it is OPT-IN: unset must behave exactly as before
    # the finding, so adopting it is a deliberate act rather than a surprise on upgrade.
    test "the byte budget is off unless configured" do
      put_env(:warm_disk_free_floor_bytes, 1)
      put_env(:warm_cache_max_bytes, nil)
      assert WarmFollower.disk_headroom?()

      put_env(:warm_cache_max_bytes, 0)

      refute WarmFollower.disk_headroom?(),
             "a zero byte budget must refuse, proving the budget is consulted at all"
    end

    # Fails OPEN, tested at the pure-function level. `disk_info/1` now resolves a missing directory
    # to its existing ancestor, so on a healthy node essentially every path reads successfully and
    # the `:error` branch is unreachable from outside without removing os_mon from the release —
    # which is why the decision is a pure function. Refusing to warm on an unreadable stat would
    # silently disable standby on such a node: worse than warming without the brake, and it would
    # look like the feature simply not working.
    test "an unreadable disk stat fails open rather than disabling standby" do
      assert WarmFollower.headroom?(:error, 1_000_000_000_000, nil, fn -> 0 end),
             "an unreadable disk stat disabled warming; it must fail open"
    end

    test "the pure decision honours the floor and the byte budget independently" do
      plenty = {:ok, %{free_bytes: 100_000}}

      assert WarmFollower.headroom?(plenty, 1_000, nil, fn -> 0 end)
      refute WarmFollower.headroom?({:ok, %{free_bytes: 500}}, 1_000, nil, fn -> 0 end)
      refute WarmFollower.headroom?(plenty, 1_000, 10, fn -> 99 end)
      assert WarmFollower.headroom?(plenty, 1_000, 100, fn -> 99 end)
    end

    # The thunk exists so a directory walk is not paid on every warm cycle when no byte budget is
    # configured, which is the default.
    test "the cache size is not computed when no byte budget is set" do
      assert WarmFollower.headroom?({:ok, %{free_bytes: 100_000}}, 1, nil, fn ->
               flunk("cache size was computed with no byte budget configured")
             end)
    end
  end
end
