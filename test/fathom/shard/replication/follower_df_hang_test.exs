defmodule Fathom.Shard.Replication.FollowerDfHangTest do
  @moduledoc """
  Expert review 2026-11-07 **#76**, the follower half: `Follower`'s seed-admission read of the
  replica volume ran its `df` in a `Task` that was `Task.shutdown(:brutal_kill)`ed at the timeout and
  released its single-flight claim. Killing the Elixir process does not end an OS `df` stuck in an
  uninterruptible `statfs`, so the NEXT reader (a begin finding the 1 s cache expired) started one
  more `df`, and one more every couple of seconds for as long as the storage stall lasted.

  Fixture: `Fathom.Test.FakeDf`, a `df` first on `PATH` that blocks until released and records its
  arguments. Only invocations whose arguments name this test's directory are counted.

  Pinned: the read is bounded and fails OPEN (`:error`, unknown free space: the seed is admitted),
  the claim names a live owner for as long as the `df` is stuck and no second `df` starts under it,
  a late answer replaces the `:error`, and once the `df` has returned the next refresh runs again.
  """
  use ExUnit.Case, async: false

  import ExUnit.CaptureLog

  alias Fathom.Shard.Replication.Follower
  alias Fathom.Shard.Replication.Protocol
  alias Fathom.Test.{Await, Env, FakeDf}

  setup do
    root = Path.join(System.tmp_dir!(), "repldfhang_#{System.unique_integer([:positive])}")
    File.mkdir_p!(root)
    on_exit(fn -> File.rm_rf(root) end)

    # Floors any real volume clears, so only the unknown reading can matter.
    Env.put!(:replication_disk_free_floor_bytes, 1)
    Env.put!(:replication_disk_free_floor_inodes, 1)
    Fathom.LogLimiter.reset(:follower_disk_read_blocked)
    on_exit(fn -> Fathom.LogLimiter.reset(:follower_disk_read_blocked) end)

    # Installed BEFORE the follower starts: its boot housekeeping starts the priming refresh, which
    # is the `df` that hangs. ExUnit stops the follower before the fake is released.
    fake = FakeDf.install!()

    name = :"dfhang_f#{System.unique_integer([:positive])}"
    dir = Path.join(root, "replica")
    pid = start_supervised!({Follower, name: name, port: 0, dir: dir}, id: name)
    # Boot housekeeping (which starts the priming refresh) has run once this returns.
    _ = :sys.get_state(pid)

    %{name: name, dir: dir, root: root, fake: fake, locks: Follower.locks(name)}
  end

  defp begin(id) do
    %Protocol.SeedBegin{
      shard_id: id,
      epoch: 1,
      wal_gen: 0,
      salt1: 0,
      wal_offset: 0,
      db_size: 4096,
      wal_size: 0
    }
  end

  defp reading(locks, dir) do
    case :ets.lookup(locks, {:disk, dir}) do
      [{_, _at, result}] -> result
      [] -> nil
    end
  end

  defp owner(locks, dir) do
    case :ets.lookup(locks, {:disk_refresh, dir}) do
      [{_, owner}] -> owner
      [] -> nil
    end
  end

  defp await_calls!(fake, root, n) do
    assert Await.eventually?(fn -> length(FakeDf.calls(fake, root)) == n end, 5_000),
           "expected #{n} fake df call(s) under #{root}; recorded: #{inspect(FakeDf.calls(fake))}"
  end

  # Makes the cached reading expired, so the next begin triggers a refresh.
  defp expire_reading!(locks, dir) do
    :ets.insert(locks, {{:disk, dir}, System.monotonic_time(:millisecond) - 60_000, :error})
  end

  # Pre-fix this took the constant 2 s timeout and the claim row held an integer, not a pid.
  @tag timeout: 20_000
  test "a hung df is bounded, fails open, and never gets a second df stacked on it", ctx do
    %{name: name, dir: dir, root: root, fake: fake, locks: locks} = ctx

    {_result, log} =
      with_log(fn ->
        # The priming refresh started the one fake df and it is stuck.
        await_calls!(fake, root, 1)

        # Past the read timeout the reading is published as UNKNOWN (`:error`).
        Await.until(fn -> reading(locks, dir) == :error end, "the timeout never published :error")

        # The claim names a live owner: the stuck df is still being waited on.
        held_by = owner(locks, dir)
        assert is_pid(held_by) and Process.alive?(held_by)

        # Begins with an expired reading — what a seed storm during the stall does — are admitted
        # (unknown free space fails open) and each would, before the fix, have started a new df.
        for i <- 1..5 do
          expire_reading!(locks, dir)
          seeds = Follower.begin_seed(name, %{}, begin("hang_#{i}"))

          assert %{failed: false} = seeds["hang_#{i}"],
                 "a begin was refused although the free space is unknown (must fail open)"

          Follower.discard_seeds(seeds)
        end

        # No second df under the stuck one, and the same owner still holds the claim.
        refute Await.eventually?(fn -> length(FakeDf.calls(fake, root)) > 1 end, 300),
               "a second df was started while the first was stuck: #{inspect(FakeDf.calls(fake))}"

        assert owner(locks, dir) == held_by

        # With NO reading at all the first reader waits, but only for the read timeout, and starts
        # nothing under the stuck df either.
        :ets.delete(locks, {:disk, dir})
        {us, seeds} = :timer.tc(fn -> Follower.begin_seed(name, %{}, begin("hang_first")) end)
        assert %{failed: false} = seeds["hang_first"]
        Follower.discard_seeds(seeds)
        assert us < 2_000_000, "the first reading waited #{div(us, 1000)} ms on a hung df"
        assert length(FakeDf.calls(fake, root)) == 1

        # Leave nothing running: release the df and wait for its owner, which ends only after the
        # whole read (including the fallback's follow-up df) has. Otherwise that df could start
        # after the fixture's teardown, or under the next test's fake.
        monitor = Process.monitor(held_by)
        FakeDf.release!(fake)
        assert_receive {:DOWN, ^monitor, :process, ^held_by, _}, 5_000
      end)

    assert log =~ "did not answer"
  end

  @tag timeout: 20_000
  test "a late answer replaces the :error, and the next refresh reads again", ctx do
    %{name: name, dir: dir, root: root, fake: fake, locks: locks} = ctx

    capture_log(fn ->
      await_calls!(fake, root, 1)
      Await.until(fn -> reading(locks, dir) == :error end, "the timeout never published :error")
    end)

    # Unblock the df: it exits 1, the combined read is unreadable, the pair answers, and that late
    # reading replaces the `:error` — the owner lived on waiting for exactly this.
    FakeDf.release!(fake)

    Await.until(
      fn -> match?({:ok, _free, _inodes}, reading(locks, dir)) and owner(locks, dir) == nil end,
      "the late answer never replaced :error, or the claim was never released: #{inspect({reading(locks, dir), owner(locks, dir)})}"
    )

    # The claim is free again: an expired reading starts a new df.
    before = length(FakeDf.calls(fake, root))
    expire_reading!(locks, dir)
    seeds = Follower.begin_seed(name, %{}, begin("hang_again"))
    Follower.discard_seeds(seeds)

    Await.until(
      fn -> length(FakeDf.calls(fake, root)) > before end,
      "the next refresh did not run df again"
    )

    # That refresh is a whole read too (the fake fails, so the fallback runs a second df): wait for
    # it to finish, so nothing is running when the fixture tears down.
    Await.until(fn -> owner(locks, dir) == nil end, "the refresh never finished")
  end
end
