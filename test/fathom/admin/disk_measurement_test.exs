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

    # (The warm-cache-volume gauge test was removed 2026-09-14 with the WarmFollower retirement.)

    # SHARD_DATA_DIR is created lazily, so on a freshly booted node the
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

  # The "warm-cache disk back-pressure" describe block was removed 2026-09-14 with the WarmFollower
  # retirement — it tested WarmFollower.disk_headroom?/headroom?, both deleted.
end
