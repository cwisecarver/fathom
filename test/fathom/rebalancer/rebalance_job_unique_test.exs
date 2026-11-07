defmodule Fathom.Rebalancer.RebalanceJobUniqueTest do
  @moduledoc """
  Expert review 2026-11-07 #78: `RebalanceJob` was the only cron worker without `unique`, so a tick
  blocked past its minute (each step is a Postgres call) was joined by the next minute's, and a
  brown-out stacked ticks that then ran back to back and took the `:rebalance` queue's slots from
  `HandoffJob`.

  The declaration deliberately leaves `:executing` OUT of `unique.states`: a node that dies mid-tick
  strands its row `:executing` until Lifeline's 60 min `rescue_after`, and counting that row would
  block the rebalancer for the hour. What is pinned here is that exact shape: a RUNNING tick does not
  block the next one, but a tick that is merely WAITING does.
  """
  use Fathom.DataCase, async: false

  import Ecto.Query, only: [from: 2]

  alias Fathom.Rebalancer.RebalanceJob

  defp set_state(job, state) do
    {1, _} =
      Fathom.Repo.update_all(from(j in Oban.Job, where: j.id == ^job.id),
        set: [state: state, attempted_at: DateTime.utc_now()]
      )
  end

  for worker <- [RebalanceJob, Fathom.RestoreDrillJob] do
    test "#{inspect(worker)}: a running tick does not block the next, a waiting one does" do
      worker = unquote(worker)

      {:ok, first} = Oban.insert(worker.new(%{}))
      refute first.conflict?

      # The first tick is RUNNING (and slow): the next minute's tick may still be inserted — this is
      # the half that keeps a node death from wedging the rebalancer behind a stranded `executing` row.
      set_state(first, "executing")
      {:ok, second} = Oban.insert(worker.new(%{}))
      refute second.conflict?, "an executing tick blocked the next one (the Lifeline wedge)"
      assert second.id != first.id

      # The second is WAITING: the third minute's tick must not stack behind it. Pre-fix all three
      # inserted.
      {:ok, third} = Oban.insert(worker.new(%{}))

      assert third.conflict? and third.id == second.id,
             "#{inspect(worker)} ticks stack: a third was inserted while one was already waiting"
    end
  end

  # Backdated, as `oban_config_test` does: Oban's keyword-list `unique` inherits a 60 s window.
  test "an hour-old waiting tick still blocks" do
    {:ok, first} = Oban.insert(RebalanceJob.new(%{}))

    {1, _} =
      Fathom.Repo.update_all(from(j in Oban.Job, where: j.id == ^first.id),
        set: [inserted_at: DateTime.add(DateTime.utc_now(), -3600, :second)]
      )

    {:ok, second} = Oban.insert(RebalanceJob.new(%{}))
    assert second.conflict? and second.id == first.id
  end

  # Every cron worker declares `unique` (the finding: it was the ONE that did not). A cron worker
  # added later without it fails here.
  test "every cron worker is unique" do
    {Oban.Plugins.Cron, opts} =
      :fathom
      |> Application.fetch_env!(Oban)
      |> Keyword.fetch!(:plugins)
      |> List.keyfind(Oban.Plugins.Cron, 0)

    workers = for {_expr, worker} <- Keyword.fetch!(opts, :crontab), do: worker
    assert length(workers) >= 6, "the cron list looks vacuous: #{inspect(workers)}"

    for worker <- workers do
      assert worker.__opts__()[:unique],
             "#{inspect(worker)} is a cron worker without `unique`: ticks stack when one runs long"
    end
  end
end
