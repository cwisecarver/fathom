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

  # Expert review 2026-07-24 #14: Finch defaults conn_max_idle_time to :infinity, so a pooled
  # connection was never proactively retired and the SERVER became the closing side. A checkout
  # landing ahead of the server's FIN then hands out a dead socket.
  #
  # The failure is asymmetric, which is why it mattered: Req's default :safe_transient retries
  # GET/HEAD only, download/4 rolls its own retries, and the FLUSH PUT is retried by nobody — so a
  # raced :closed aborts the idle drop, the coordinator keeps the local file AND the lease, and the
  # tenant's RPO window extends by a whole backoff interval.
  test "the pool retires idle connections before S3 does" do
    {Finch, opts} = S3.finch_child_spec()
    default = opts[:pools][:default]

    idle = Keyword.fetch!(default, :conn_max_idle_time)

    assert is_integer(idle) and idle > 0,
           "conn_max_idle_time must be set — at :infinity, S3 is the closing side and the stale " <>
             "connection race lands on the unretried flush PUT"

    assert idle < 20_000,
           "must be under S3's ~20s idle close, or fathom still is not the closing side"

    assert Keyword.fetch!(default, :conn_opts)[:transport_opts][:timeout] > 0,
           "a hung connect otherwise sits for the library default before anything reacts"
  end

  test "retry backoff is scaled to a same-region object store, not a public API" do
    delays = Enum.map(0..3, &S3.retry_delay/1)
    [first | _] = delays

    assert first < 1_000,
           "Req's exp_backoff starts at 1000ms, sized for public-internet APIs. Against a " <>
             "same-region S3 whose whole cold-open budget is ~1 RTT (~26ms at 10ms one-way), that " <>
             "made one transient blip a ~40x p99 spike"

    assert delays == Enum.sort(delays), "backoff must be non-decreasing"
    assert Enum.all?(delays, &(&1 > 0))
  end
end
