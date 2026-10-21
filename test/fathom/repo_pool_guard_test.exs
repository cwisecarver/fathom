defmodule Fathom.RepoPoolGuardTest do
  @moduledoc """
  Expert review 2026-10-21 #11: Oban's queue limits summed to exactly the old fixed `POOL_SIZE`
  default (25 = 25) once the `tokens` and `erasure` queues were added, and nothing checked it. Every
  running job holds a Repo connection, so a full set of jobs left none for
  `HranaAuth.Revocations` on the stream-open path. `Fathom.Application.check_repo_pool_headroom!/0`
  now refuses that in prod and warns elsewhere. Not async: it mutates global application env.
  """
  use ExUnit.Case, async: false

  import ExUnit.CaptureLog

  alias Fathom.Application, as: App

  setup do
    prev = for k <- [:env, Oban, Fathom.Repo], into: %{}, do: {k, Application.get_env(:fathom, k)}

    on_exit(fn ->
      for {k, v} <- prev do
        if is_nil(v),
          do: Application.delete_env(:fathom, k),
          else: Application.put_env(:fathom, k, v)
      end
    end)

    :ok
  end

  describe "oban_queue_limit_sum/1" do
    test "sums integer limits and keyword :limit, and false is no queues" do
      assert App.oban_queue_limit_sum(a: 10, b: [limit: 5, paused: true], c: 3) == 18
      assert App.oban_queue_limit_sum(false) == 0
      assert App.oban_queue_limit_sum([]) == 0
    end

    test "the shipped config.exs queues sum to 25 (the figure the docs and comments state)" do
      queues = Keyword.fetch!(Application.get_env(:fathom, Oban), :queues)
      assert App.oban_queue_limit_sum(queues) == 25
    end
  end

  describe "repo_pool_verdict/4" do
    test "a pool at or under the queue sum refuses in prod" do
      assert {:raise, msg} = App.repo_pool_verdict(25, 25, :prod, 10)
      assert msg =~ "at or under the sum of Oban's queue limits (25)"
      assert msg =~ "at least 35"
      assert {:raise, _} = App.repo_pool_verdict(10, 25, :prod, 10)
    end

    test "the same pool only warns outside prod" do
      assert {:warn, "config warning:" <> _} = App.repo_pool_verdict(25, 25, :dev, 10)
      assert {:warn, _} = App.repo_pool_verdict(10, 25, :test, 10)
    end

    test "under the headroom warns in prod; at or over it passes" do
      assert {:warn, msg} = App.repo_pool_verdict(30, 25, :prod, 10)
      assert msg =~ "leaves 5 connection(s)"
      assert App.repo_pool_verdict(35, 25, :prod, 10) == :ok
      assert App.repo_pool_verdict(26, 25, :dev, 10) == :ok
    end

    test "no queues, nothing to check" do
      assert App.repo_pool_verdict(1, 0, :prod, 10) == :ok
    end
  end

  describe "check_repo_pool_headroom!/0 reads the live config" do
    defp put(env, oban, repo) do
      Application.put_env(:fathom, :env, env)
      Application.put_env(:fathom, Oban, oban)
      Application.put_env(:fathom, Fathom.Repo, repo)
    end

    test "prod with the old fixed 25 against the shipped queues refuses to boot" do
      put(
        :prod,
        [
          queues: [migrations: 10, retirement: 5, rebalance: 3, tenants: 3, tokens: 3, erasure: 1]
        ],
        pool_size: 25
      )

      assert_raise RuntimeError, ~r/at or under the sum/, &App.check_repo_pool_headroom!/0
    end

    test "pool_count multiplies the pool" do
      put(:prod, [queues: [a: 25]], pool_size: 20, pool_count: 2)
      assert App.check_repo_pool_headroom!() == :ok
    end

    test "dev logs a warning instead of refusing" do
      put(:dev, [queues: [a: 25]], pool_size: 10)

      log =
        capture_log(fn -> assert {:warn, _} = App.check_repo_pool_headroom!() end)

      assert log =~ "config warning: the Repo pool (10 = POOL_SIZE"
    end

    test "Oban in testing mode runs no queues, so nothing is checked" do
      put(:prod, [testing: :manual, queues: [a: 25]], pool_size: 1)
      assert App.check_repo_pool_headroom!() == :ok
    end
  end
end
