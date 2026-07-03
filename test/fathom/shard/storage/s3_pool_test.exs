defmodule Fathom.Shard.Storage.S3PoolTest do
  # Mutates :fathom config to test pool sizing, so it can't run async.
  use ExUnit.Case, async: false

  alias Fathom.Shard.Storage.S3

  test "the dedicated S3 Finch pool starts with the application" do
    # The app supervision tree starts it (application.ex), so it's alive in test.
    # This is what lets `req/0` route S3 traffic through the larger pool instead
    # of Req's default ~50-conn pool.
    assert is_pid(Process.whereis(S3.finch_name()))
  end

  test "finch_child_spec/0 defaults to a 200-connection single pool" do
    {Finch, opts} = S3.finch_child_spec()
    assert opts[:name] == S3.finch_name()
    assert opts[:pools][:default][:size] == 200
    assert opts[:pools][:default][:count] == 1
  end

  test "pool size and count are configurable via :pool_size / :pool_count" do
    prev = Application.get_env(:fathom, S3, [])
    on_exit(fn -> Application.put_env(:fathom, S3, prev) end)

    Application.put_env(
      :fathom,
      S3,
      prev |> Keyword.put(:pool_size, 32) |> Keyword.put(:pool_count, 4)
    )

    {Finch, opts} = S3.finch_child_spec()
    assert opts[:pools][:default][:size] == 32
    assert opts[:pools][:default][:count] == 4
  end
end
