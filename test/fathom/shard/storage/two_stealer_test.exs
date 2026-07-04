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
      held = for {:error, {:held, holder}} <- results, do: holder

      assert length(winners) == 1,
             "round #{round}: exactly one stealer must win, got #{inspect(results)}"

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

      oks = for {:ok, _etag} <- results, do: :ok
      superseded = for {:error, :superseded} <- results, do: :superseded

      assert length(oks) == 1,
             "round #{round}: exactly one fenced flush must commit, got #{inspect(results)}"

      assert length(superseded) == 1,
             "round #{round}: the loser must be :superseded, got #{inspect(results)}"
    end
  end
end
