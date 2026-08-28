defmodule Fathom.Shard.Storage.TwoStealerTest do
  # Expert review #38: the Local backend — the fence's test double — implemented
  # steal/reclaim/renew/release as non-atomic read-modify-write, so interleaving bugs
  # in the conditional protocol (two concurrent stealers both reading the dead lock,
  # both writing epoch+1, both believing they won) were structurally invisible to any
  # test driven through it. Lock mutations are now serialized per shard, giving Local
  # the same exactly-one-winner semantics S3's conditional writes give the production
  # fence — and this test drives the contention path against the BEHAVIOUR: of two
  # simultaneous stealers of a dead owner's lock, exactly one wins (epoch bumped) and
  # the other is refused with {:held, winner}.
  use ExUnit.Case, async: false

  alias Fathom.Shard.Storage
  alias Fathom.Shard.Storage.Local

  @rounds 50

  setup do
    dir = Path.join(System.tmp_dir!(), "fathom_race_#{System.unique_integer([:positive])}")
    prev = Application.get_env(:fathom, Fathom.Shard.Storage.Local)
    Application.put_env(:fathom, Fathom.Shard.Storage.Local, dir: dir)

    on_exit(fn ->
      File.rm_rf!(dir)

      if prev,
        do: Application.put_env(:fathom, Fathom.Shard.Storage.Local, prev),
        else: Application.delete_env(:fathom, Fathom.Shard.Storage.Local)
    end)

    %{dir: dir}
  end

  # This test failed ONCE under full-suite load on 2026-08-02 (seed 783965) with BOTH stealers
  # winning — A at epoch 2, B at epoch 3 — and has never reproduced: 14 runs then, plus the
  # elimination pass on 2026-08-05. Attribution stalled because the failure message showed only
  # `results`, and the interesting state is what the LOSER saw.
  #
  # For B to steal A's 30-second-fresh lock, `Local.owner_live?/3` must have returned `:dead`, and
  # `stealable_at/2` has exactly one branch that can do that for a fresh lock: heartbeat
  # `:not_found` AND `Storage.incarnation_dead?(owner)` true, which returns "stealable at 0". That
  # set is a process-global `:persistent_term`, so it is the one piece of cross-test state that
  # could reach in here — though its entries are `nonode@nohost#<nonce>` and cannot string-match
  # these owners today. The other branches all derive from the lock's own TTL.
  #
  # THE "NEVER CLEARED BETWEEN TESTS AND ONLY GROWS" HALF OF THAT IS FIXED (2026-08-24).
  # `Storage.reset_incarnation_deaths/0` now exists as the twin of `reset_quiescence/0`, and
  # `incarnation_quiescence_test.exs` — the one file that was leaking a marked owner into the whole
  # rest of the run — resets it and asserts on exit that the set came back empty. The suspicion this
  # comment records was correct about the hazard; it was wrong only about it being unfixable here.
  # It is kept because the diagnostics below still print the set, and a future `:dead` verdict on a
  # fresh lock is still best explained by looking there first.
  #
  # So capture that, plus the lock as it stands, at failure time. Cheap (only on failure) and it
  # turns "seen once, cause unknown" into an attributable report next time.
  defp diagnostics(dir, shard) do
    lock = Path.join(dir, "#{shard}.lock")

    owners = ["stealer_a@n#a1", "stealer_b@n#b1", "dead@node#gone"]

    """
      --- diagnostics (why did the loser think the winner was dead?) ---
      lock file: #{inspect(File.read(lock))}
      now_ms: #{System.system_time(:millisecond)}  steal_margin_ms: #{Storage.steal_margin_ms()}
      dead_incarnations hits: #{inspect(Enum.filter(owners, &Storage.incarnation_dead?/1))}
      heartbeats: #{inspect(Map.new(owners, &{&1, Storage.read_heartbeat(&1)}))}
    """
  end

  defp write_dead_lock(dir, shard) do
    File.mkdir_p!(dir)

    File.write!(
      Path.join(dir, "#{shard}.lock"),
      Storage.encode_lease(%{
        owner: "dead@node#gone",
        epoch: 1,
        expires_at_ms: System.system_time(:millisecond) - Storage.steal_margin_ms() - 60_000
      })
    )
  end

  test "of two simultaneous stealers, exactly one wins and the epoch bumps once", %{dir: dir} do
    for round <- 1..@rounds do
      shard = "race_#{round}"
      write_dead_lock(dir, shard)

      # Barrier start so both read-modify-write sections collide as hard as possible.
      tasks =
        for owner <- ["stealer_a@n#a1", "stealer_b@n#b1"] do
          Task.async(fn ->
            receive do
              :go -> Local.acquire_lease(shard, owner, 30_000)
            end
          end)
        end

      Enum.each(tasks, &send(&1.pid, :go))
      results = Task.await_many(tasks, 5_000)

      winners = for {:ok, lease} <- results, do: lease
      held = for {:error, {:held, holder, _}} <- results, do: holder

      assert length(winners) == 1,
             "round #{round}: exactly one stealer must win, got #{inspect(results)}\n" <>
               diagnostics(dir, shard)

      [winner] = winners
      assert winner.epoch == 2, "the steal must bump the epoch exactly once"

      assert held == [winner.owner],
             "round #{round}: the loser must be refused with the winner as holder"
    end
  end

  # Expert review #28: #38 mutexed the LOCK ops but not the conditional DATA flush —
  # `flush/3` was a read-compare-write outside the mutex, so two concurrent fenced
  # flushes with the same expected etag both read the same `current`, both pass the
  # compare, and both write (last-write-wins): a split-brain the fence exists to
  # prevent, and structurally invisible to any test driven through this backend (the
  # double S3 tests can't reproduce it). The invariant: of two concurrent flushes
  # fencing the same etag, exactly one commits and the other is superseded.
  test "of two concurrent fenced flushes on the same etag, exactly one wins", %{dir: dir} do
    File.mkdir_p!(dir)

    for round <- 1..@rounds do
      shard = "flushrace_#{round}"

      # Seed a base object; capture its etag as the shared fence baseline.
      base = Path.join(dir, "base_#{round}.db")
      File.write!(base, "base-#{round}")
      :ok = Local.flush(shard, base)
      {:ok, e0} = Local.object_etag(shard)

      a = Path.join(dir, "a_#{round}.db")
      b = Path.join(dir, "b_#{round}.db")
      File.write!(a, "writer-a-#{round}")
      File.write!(b, "writer-b-#{round}")

      tasks =
        for path <- [a, b] do
          Task.async(fn ->
            receive do
              :go -> Local.flush(shard, path, e0)
            end
          end)
        end

      Enum.each(tasks, &send(&1.pid, :go))
      results = Task.await_many(tasks, 5_000)

      oks = for {:ok, _etag, _carried} <- results, do: :ok
      superseded = for {:error, :superseded} <- results, do: :superseded

      assert length(oks) == 1,
             "round #{round}: exactly one fenced flush must commit, got #{inspect(results)}"

      assert length(superseded) == 1,
             "round #{round}: the loser must be :superseded, got #{inspect(results)}"
    end
  end
end
