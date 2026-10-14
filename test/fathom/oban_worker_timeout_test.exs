defmodule Fathom.ObanWorkerTimeoutTest do
  @moduledoc """
  Expert review 2026-10-13 #6. Symptom: no worker defined `timeout/1`, so Oban's default of
  `:infinity` let a job wedged on a hung S3/Postgres call hold its queue slot forever, and at 60
  minutes Lifeline made the row available again WHILE THE ORIGINAL PROCESS WAS STILL ALIVE (the
  per-shard `unique` guard no longer blocked it) — two concurrent migration/revert/delete/handoff
  runs on one shard.

  The invariant: every `Oban.Worker` in the app has a finite `timeout/1` strictly below
  Lifeline's `rescue_after`. A new worker that forgets `timeout/1` fails here.

  This pins the declaration, not Oban's enforcement of it (that is Oban's own contract).
  """
  use ExUnit.Case, async: true

  defp workers do
    {:ok, mods} = :application.get_key(:fathom, :modules)

    Enum.filter(mods, fn m ->
      Code.ensure_loaded?(m) and
        Oban.Worker in Keyword.get(m.module_info(:attributes), :behaviour, [])
    end)
  end

  defp rescue_after do
    {Oban.Plugins.Lifeline, opts} =
      :fathom
      |> Application.fetch_env!(Oban)
      |> Keyword.fetch!(:plugins)
      |> List.keyfind(Oban.Plugins.Lifeline, 0)

    Keyword.fetch!(opts, :rescue_after)
  end

  test "the worker enumeration is not vacuous" do
    assert length(workers()) >= 12
  end

  test "every Oban worker has a finite timeout below Lifeline's rescue_after" do
    limit = rescue_after()

    for m <- workers() do
      t = m.timeout(%Oban.Job{})

      assert is_integer(t) and t < limit,
             "#{inspect(m)}.timeout/1 = #{inspect(t)}; must be an integer < rescue_after (#{limit} ms)"
    end
  end
end
