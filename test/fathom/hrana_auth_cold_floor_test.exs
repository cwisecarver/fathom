defmodule Fathom.HranaAuthColdFloorTest do
  @moduledoc """
  Expert review 2026-10-29 #20: the cold-miss revocation read-through put an unbounded, retried S3
  GET on the `hello` critical path (`Task.await(storage_task, :infinity)` over Req's default GET:
  ~4 x 15 s against a stalled store), repeated it in the fail-open fallback when the directory also
  failed, and issued one per concurrent hello for the same cold shard.

  Invariants pinned here:

    * a stalled storage-floor read is abandoned at `:hrana_storage_floor_budget_ms`;
    * the existing posture decides the answer: `:fail_closed` => `:unavailable`, `:fail_open` => the
      directory floor, UNCACHED (the PITR backstop is consulted again on the next hello);
    * the fail-open fallback uses the ONE bounded attempt, never a second GET;
    * N concurrent hellos for one cold shard make exactly one storage read, and a claimant that dies
      mid-read does not wedge the waiters.

  Each test blocks the storage read with a `:faulty_before` hook (a `receive`, never a sleep), so
  the timing is the module's own budget, not a wall-clock race. `Fathom.Test.FaultyStorage`
  delegates to `Local`, so these exercise the budget/single-flight logic; the S3 per-call request
  options are pinned separately in `S3TokenFloorBudgetTest`.
  """
  use Fathom.DataCase, async: false

  import ExUnit.CaptureLog

  alias Fathom.Directory
  alias Fathom.HranaAuth.Revocations
  alias Fathom.Shard.Storage

  @followers Fathom.HranaAuth.Revocations.Followers

  setup do
    keys = [
      :shard_storage,
      :faulty_before,
      :hrana_storage_floor_budget_ms,
      :hrana_storage_floor_backoff_ms,
      :hrana_revocation_on_error
    ]

    prev = for k <- keys, do: {k, Application.fetch_env(:fathom, k)}

    Application.put_env(:fathom, :shard_storage, Fathom.Test.FaultyStorage)
    Application.put_env(:fathom, :hrana_storage_floor_budget_ms, 200)
    Application.delete_env(:fathom, :hrana_revocation_on_error)

    on_exit(fn ->
      for {k, v} <- prev do
        case v do
          {:ok, val} -> Application.put_env(:fathom, k, val)
          :error -> Application.delete_env(:fathom, k)
        end
      end
    end)

    :ok
  end

  defp uniq, do: "coldfloor_#{System.unique_integer([:positive])}"

  # A shard whose directory floor is `dir_floor` and whose cache is cold. The storage floor is left
  # to the test.
  #
  # A REVOKED shard (`dir_floor > 0`) is parked from the periodic bulk refresh: that tick (every
  # TTL) caches every revoked shard's directory floor, and the tests that stall the storage read for
  # the whole budget assert the answer is NOT cached, so a tick landing in that window read as
  # `[{shard, 3, nil, _}]` (seed 146031; reproduced 3/3 by firing a tick mid-stall). The refresh is
  # the cache's only other writer and it writes from the Revocations process, so suspending that
  # process (resumed on exit) is the whole isolation. Suspended BEFORE the entry is cleared: a page
  # already in its mailbox is handled first, then the delete removes what it cached.
  defp cold_shard(dir_floor) do
    shard = uniq()
    {:ok, _} = Directory.resolve(shard)

    if dir_floor > 0 do
      1 = Directory.raise_token_version(shard, dir_floor)
      revocations = Process.whereis(Revocations)
      :ok = :sys.suspend(revocations)
      on_exit(fn -> :sys.resume(revocations) end)
    end

    :ets.delete(Revocations, shard)
    on_exit(fn -> File.rm(Path.join([Storage.Local.dir(), "tokenfloors", shard])) end)
    shard
  end

  # Counts every storage-floor read for `shard` and BLOCKS the ones `block?.(n)` selects (n is the
  # 1-based call number) until the hook's process is killed or sent `:go`. Other shards pass straight
  # through: `:faulty_before` is global app env.
  defp hook_storage(shard, block?) do
    counter = :counters.new(1, [])
    test = self()

    Application.put_env(
      :fathom,
      :faulty_before,
      {:read_token_floor,
       fn
         ^shard ->
           :counters.add(counter, 1, 1)

           if block?.(:counters.get(counter, 1)) do
             send(test, {:storage_read, self()})

             receive do
               :go -> :ok
             end
           end

           :ok

         _other ->
           :ok
       end}
    )

    counter
  end

  defp calls(counter), do: :counters.get(counter, 1)

  # `Revocations.floor_info/1` with its wall time and captured log. `capture_log` runs the fun in
  # this process, so the result travels by message.
  defp timed_floor_info(shard, directory_down? \\ false) do
    if directory_down?, do: Ecto.Adapters.SQL.Sandbox.mode(Fathom.Repo, :manual)

    try do
      log =
        capture_log(fn ->
          send(self(), {:timed, :timer.tc(fn -> Revocations.floor_info(shard) end)})
        end)

      assert_received {:timed, {us, info}}
      {us, info, log}
    after
      if directory_down? do
        owner = Ecto.Adapters.SQL.Sandbox.start_owner!(Fathom.Repo, shared: true)
        on_exit(fn -> Ecto.Adapters.SQL.Sandbox.stop_owner(owner) end)
      end
    end
  end

  defp follower_count(shard) do
    length(:ets.lookup(@followers, shard))
  rescue
    ArgumentError -> 0
  end

  # Bounded poll of the follower registry: there is no message to wait on for "N hellos are now
  # parked behind the claimant", and a fixed sleep would be a guess.
  defp await_followers(shard, n, tries \\ 400) do
    cond do
      follower_count(shard) >= n ->
        :ok

      tries == 0 ->
        flunk("only #{follower_count(shard)} of #{n} hellos parked behind the claimant")

      true ->
        receive do
        after
          5 -> await_followers(shard, n, tries - 1)
        end
    end
  end

  describe "a stalled storage floor read" do
    @describetag timeout: 15_000

    test "fail_open: abandoned at the budget, directory floor served, NOT cached" do
      shard = cold_shard(3)
      counter = hook_storage(shard, fn _ -> true end)

      {us, info, _log} = timed_floor_info(shard)

      assert info == {3, nil}, "the directory floor is served when storage cannot be read"
      assert us < 3_000_000, "a stalled read must return at the budget: #{us} us"
      assert calls(counter) == 1, "exactly one bounded storage attempt, no retry or fallback GET"

      assert :ets.lookup(Revocations, shard) == [],
             "a directory-only answer must not be cached: it would stand in for the PITR backstop"
    end

    test "fail_open then recovery: the first hello after the cool-down consults storage again" do
      # A zero cool-down: it has lapsed by the next hello. The cool-down itself is pinned below.
      Application.put_env(:fathom, :hrana_storage_floor_backoff_ms, 0)
      shard = cold_shard(3)
      hook_storage(shard, fn _ -> true end)
      assert {_us, {3, nil}, _log} = timed_floor_info(shard)

      # Storage heals, holding a floor above the (PITR-lowered) directory floor.
      Application.delete_env(:fathom, :faulty_before)
      :ok = Storage.put_token_floor(shard, 9)

      assert Revocations.floor_info(shard) == {9, nil}
    end

    test "fail_closed: :unavailable, within the budget, nothing cached" do
      Application.put_env(:fathom, :hrana_revocation_on_error, :fail_closed)
      shard = cold_shard(3)
      counter = hook_storage(shard, fn _ -> true end)

      {us, info, log} = timed_floor_info(shard)

      assert info == :unavailable
      assert log =~ "storage floor unreadable"
      assert us < 3_000_000
      assert calls(counter) == 1
      assert :ets.lookup(Revocations, shard) == []
    end

    test "fail_open with the directory ALSO down: one bounded attempt, floor 0, no second GET" do
      shard = uniq()
      counter = hook_storage(shard, fn _ -> true end)

      {us, info, _log} = timed_floor_info(shard, true)

      assert info == {0, nil}
      assert us < 3_000_000

      assert calls(counter) == 1,
             "the fail-open fallback used to repeat the whole GET (pre-fix: a second, retried read)"
    end

    # Review of 9a97ec71 (2026-10-31). Symptom: the fail-open answer is uncached, so during an S3
    # outage EVERY later hello for a cold shard paid the whole storage budget again (3 s per
    # connection open, for the length of the outage). After one failed read the shard now cools
    # down: the next hellos skip storage and answer at once with the same posture. Pre-fix the
    # second hello made a second storage read and blocked on it.
    # These use an UNREVOKED shard (`cold_shard(0)`: a fresh shard's directory floor is 1) on purpose: the periodic bulk refresh (every TTL) caches every
    # REVOKED shard's floor, so a floor-3 shard could be cached mid-test and turn the cold hellos
    # into hits that pass for the wrong reason (seen once in a full precommit, seed 117157).
    test "fail_open: after a failed read, the next hellos skip storage instead of paying the budget" do
      Application.put_env(:fathom, :hrana_storage_floor_backoff_ms, 60_000)
      shard = cold_shard(0)
      counter = hook_storage(shard, fn _ -> true end)

      assert {_us, {1, nil}, _log} = timed_floor_info(shard)
      assert calls(counter) == 1

      for _ <- 1..3 do
        {us, info, _log} = timed_floor_info(shard)
        assert info == {1, nil}
        assert us < 150_000, "a cooling-down hello waited on storage: #{us} us"
      end

      assert calls(counter) == 1, "storage was read again inside the cool-down"

      assert :ets.lookup(Revocations, shard) == [],
             "the cool-down must not turn the directory-only answer into a cached one"
    end

    test "fail_closed: after a failed read, the next hellos answer :unavailable at once" do
      Application.put_env(:fathom, :hrana_revocation_on_error, :fail_closed)
      Application.put_env(:fathom, :hrana_storage_floor_backoff_ms, 60_000)
      shard = cold_shard(0)
      counter = hook_storage(shard, fn _ -> true end)

      assert {_us, :unavailable, _log} = timed_floor_info(shard)
      {us, info, _log} = timed_floor_info(shard)

      assert info == :unavailable
      assert us < 150_000
      assert calls(counter) == 1
    end

    # Review of 9a97ec71 (2026-10-31). Symptom: a claimant whose read RAISED skipped `release/2`.
    # If its process caught the raise and lived on (so no DOWN), the claim stayed held, and every
    # later cold hello for the shard parked behind it until `follow_timeout_ms/0` (~20 s here)
    # before reading for itself. A bad runtime budget raises an ArithmeticError inside the read.
    # Pre-fix the second hello below took ~20 s.
    test "a claimant that raises and survives does not leave its claim behind" do
      shard = cold_shard(0)
      Application.put_env(:fathom, :hrana_storage_floor_budget_ms, :not_a_number)
      test = self()

      claimant =
        spawn(fn ->
          raised =
            try do
              Revocations.floor_info(shard)
              :no_raise
            rescue
              ArithmeticError -> :raised
            end

          send(test, {:claimant, raised})

          receive do
            :stop -> :ok
          end
        end)

      on_exit(fn -> Process.exit(claimant, :kill) end)
      assert_receive {:claimant, :raised}, 5_000

      Application.put_env(:fathom, :hrana_storage_floor_budget_ms, 200)
      {us, info, _log} = timed_floor_info(shard)

      assert info == {1, nil}
      assert us < 2_000_000, "the next hello parked behind a dead claim for #{us} us"
    end

    # The cool-down is per shard: a failure on one shard must not stop another shard's backstop.
    # An invariant guard, not a regression test (it passes without the cool-down too): it fails if
    # the cool-down is ever widened into a node-wide breaker.
    test "the cool-down is per shard" do
      Application.put_env(:fathom, :hrana_storage_floor_backoff_ms, 60_000)
      down = cold_shard(0)
      hook_storage(down, fn _ -> true end)
      assert {_us, {1, nil}, _log} = timed_floor_info(down)

      up = cold_shard(0)
      :ok = Storage.put_token_floor(up, 9)
      assert Revocations.floor_info(up) == {9, nil}
    end
  end

  # Pre-fix the fail-open fallback ran a SECOND storage read after the task had already answered.
  test "directory down, storage answers: its floor is served from the one read (no second GET)" do
    shard = uniq()
    on_exit(fn -> File.rm(Path.join([Storage.Local.dir(), "tokenfloors", shard])) end)
    :ok = Storage.put_token_floor(shard, 7)
    counter = hook_storage(shard, fn _ -> false end)

    {_us, info, _log} = timed_floor_info(shard, true)

    assert info == {7, nil}
    assert calls(counter) == 1
  end

  describe "single-flight" do
    @describetag timeout: 20_000

    test "N concurrent cold hellos for one shard make exactly one storage read" do
      Application.put_env(:fathom, :hrana_storage_floor_budget_ms, 15_000)
      shard = cold_shard(0)
      :ok = Storage.put_token_floor(shard, 9)
      # Only the first read blocks: the claimant's. Everyone else must be parked behind it.
      counter = hook_storage(shard, &(&1 == 1))

      lead = Task.async(fn -> Revocations.floor_info(shard) end)
      assert_receive {:storage_read, storage_pid}, 5_000

      followers = for _ <- 1..5, do: Task.async(fn -> Revocations.floor_info(shard) end)
      await_followers(shard, 5)
      send(storage_pid, :go)

      results = Task.await_many([lead | followers], 10_000)

      assert results == List.duplicate({9, nil}, 6),
             "every hello sees the unioned storage floor: #{inspect(results)}"

      assert calls(counter) == 1, "6 hellos for one cold shard must share ONE storage read"
      assert follower_count(shard) == 0, "no waiter registration may leak"
    end

    test "a claimant that dies mid-read does not wedge its waiters" do
      Application.put_env(:fathom, :hrana_storage_floor_budget_ms, 15_000)
      shard = cold_shard(0)
      :ok = Storage.put_token_floor(shard, 9)
      # The claimant's read blocks forever; the hand-over read (call 2) answers.
      counter = hook_storage(shard, &(&1 == 1))

      lead = spawn(fn -> Revocations.floor_info(shard) end)
      assert_receive {:storage_read, _storage_pid}, 5_000

      followers = for _ <- 1..3, do: Task.async(fn -> Revocations.floor_info(shard) end)
      await_followers(shard, 3)

      Process.exit(lead, :kill)

      assert Task.await_many(followers, 10_000) == List.duplicate({9, nil}, 3)

      assert calls(counter) == 2,
             "the dead claimant's read plus exactly one hand-over read (not one per waiter)"

      assert follower_count(shard) == 0
    end
  end
end
