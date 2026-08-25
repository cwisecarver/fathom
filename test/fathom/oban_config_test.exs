defmodule Fathom.ObanConfigTest do
  @moduledoc """
  Guards the Oban plugin set the shard lifecycle machinery depends on (expert review
  2026-07-24 #33).

  This is a config guard rather than a behavioural one on purpose: the failure it protects
  against is an *absent* plugin, which no amount of exercising the happy path can reveal.
  """
  use Fathom.DataCase, async: false

  alias Fathom.Migrator.RevertJob
  alias Fathom.Migrator.ShardMigrationJob

  defp plugins, do: Application.get_env(:fathom, Oban)[:plugins] || []

  defp plugin_opts(mod) do
    Enum.find_value(plugins(), fn
      {^mod, opts} -> opts
      ^mod -> []
      _ -> nil
    end)
  end

  test "Lifeline is configured, so a node crash cannot strand a job in :executing forever" do
    opts = plugin_opts(Oban.Plugins.Lifeline)

    assert opts,
           "Oban.Plugins.Lifeline is absent. Nothing else moves a job out of :executing — the " <>
             "Pruner only touches terminal states — and every per-shard worker is unique over " <>
             "states including :executing, so one stranded row wedges that shard's migration, " <>
             "revert, delete and handoff permanently."

    assert is_integer(opts[:rescue_after])
  end

  # rescue_after must exceed the longest real job runtime, or Lifeline rescues a job that is
  # still running on a live node and produces two concurrent runs. The migration copy is the long
  # pole; 60 minutes mirrors the engine's own staleness convention
  # (@default_migration_stale_seconds) and sits orders of magnitude above a real copy, which
  # benches at ~7.8M rows/s.
  test "rescue_after is generous enough not to rescue a job that is still running" do
    rescue_after = plugin_opts(Oban.Plugins.Lifeline)[:rescue_after]

    assert rescue_after >= :timer.minutes(60),
           "rescue_after (#{rescue_after}ms) is too low — Lifeline would rescue a long migration " <>
             "copy that is still running on a live node, producing two concurrent runs."
  end

  # The invariant that makes Lifeline load-bearing. If Oban's unique semantics ever stopped
  # counting :executing, a stranded row would be harmless and this guard would be telling us so.
  test "an executing row really does block a new unique enqueue for that shard" do
    shard = "oban_wedge_#{System.unique_integer([:positive])}"

    {:ok, first} = Oban.insert(ShardMigrationJob.new(%{"shard_id" => shard, "target" => 2}))

    # Force it into the state a dead node would leave behind.
    {1, _} =
      Fathom.Repo.update_all(
        from(j in Oban.Job, where: j.id == ^first.id),
        set: [state: "executing", attempted_at: DateTime.utc_now()]
      )

    {:ok, second} = Oban.insert(ShardMigrationJob.new(%{"shard_id" => shard, "target" => 2}))

    assert second.id == first.id and second.conflict?,
           "a job stranded in :executing must dedup a new enqueue for the same shard — that is " <>
             "exactly why the lifecycle wedges without Lifeline"
  end

  # THE TEST ABOVE CANNOT SEE THE TIMESCALE IT IS ABOUT (expert review 2026-08-24 #23).
  #
  # It inserts both jobs microseconds apart, so it passes inside Oban's DEFAULT `period: 60` and
  # proves nothing about the interval Lifeline actually operates on. A keyword list in `unique:`
  # MERGES into `@unique_defaults`, which sets `period: 60` — only the bare `unique: true` gets
  # `:infinity` — so dedup applied only against jobs inserted in the last sixty seconds.
  #
  # Every long-lived state these workers reach is far longer than that: the snooze backoff caps at
  # 60 s with jobs documented reaching attempt 122, `:migration_stall_after_ms` is 10 minutes, and
  # the `rescue_after` assertion at the top of this file requires at least 60 MINUTES — a row
  # stranded in `:executing` is by definition older than the window when Lifeline finds it. So
  # `ShardMigrationJob`'s moduledoc claim ("unique per shard_id, so the lazy path and the sweep
  # never migrate the same shard twice at once") and the comment in `Fathom.Shards` were both false
  # past a minute, and a hot laggard under `migrate_on_touch: :async` accumulated roughly one job
  # per 60 s of traffic.
  #
  # Backdating `inserted_at` is what makes this discriminate: the unique query keys on that column
  # (`timestamp: :inserted_at`), so an hour-old row is exactly the Lifeline case.
  test "dedup survives the 60s default window — an HOUR-old stranded row still blocks" do
    for worker <- [ShardMigrationJob, RevertJob] do
      shard = "oban_period_#{System.unique_integer([:positive])}"
      args = unique_args(worker, shard)

      {:ok, first} = Oban.insert(worker.new(args))

      # An hour old AND stranded in :executing — what a dead node leaves for Lifeline.
      {1, _} =
        Fathom.Repo.update_all(
          from(j in Oban.Job, where: j.id == ^first.id),
          set: [
            state: "executing",
            attempted_at: DateTime.utc_now(),
            inserted_at: DateTime.add(DateTime.utc_now(), -3600, :second)
          ]
        )

      {:ok, second} = Oban.insert(worker.new(args))

      assert second.id == first.id and second.conflict?,
             "#{inspect(worker)}: an hour-old stranded job stopped deduping, so a second run for " <>
               "the same shard can be enqueued while the first is still executing. Oban's default " <>
               "unique period is 60s; this needs period: :infinity."
    end
  end

  defp unique_args(ShardMigrationJob, shard), do: %{"shard_id" => shard, "target" => 2}
  defp unique_args(RevertJob, shard), do: %{"shard_id" => shard, "to_version" => 1}
end
