defmodule Fathom.ObanConfigTest do
  @moduledoc """
  Guards the Oban plugin set the shard lifecycle machinery depends on (expert review
  2026-07-24 #33).

  This is a config guard rather than a behavioural one on purpose: the failure it protects
  against is an *absent* plugin, which no amount of exercising the happy path can reveal.
  """
  use Fathom.DataCase, async: false

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
end
