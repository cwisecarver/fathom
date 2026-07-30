defmodule Fathom.Migrator.HeadCacheTest do
  # HEAD is cached in persistent_term and refreshed on a TTL, so the lazy-migrate
  # checkout path never runs a per-checkout max(version) on Postgres. Pins the
  # invariant that get/0 serves the CACHE (a fresh release isn't visible until a
  # refresh) — that's what proves the hot path isn't hitting Postgres.
  use Fathom.DataCase, async: false

  alias Fathom.Migrator
  alias Fathom.Migrator.HeadCache

  test "get/0 serves the cached HEAD; a new release is invisible until refresh" do
    {:ok, _} = Migrator.release(10, "v10")
    assert 10 = HeadCache.refresh()
    assert HeadCache.get() == 10

    # A newer release is NOT reflected by get/0 until the cache refreshes — the proof
    # that checkout reads persistent_term, not a per-checkout Postgres aggregate.
    {:ok, _} = Migrator.release(11, "v11")
    assert HeadCache.get() == 10

    assert 11 = HeadCache.refresh()
    assert HeadCache.get() == 11
  end

  test "refresh/0 reflects the fleet HEAD (max released version)" do
    assert HeadCache.refresh() == Migrator.head()
    {:ok, _} = Migrator.release(7, "v7")
    assert HeadCache.refresh() == 7
  end

  # The BACKGROUND poll is what actually keeps the cache warm in production, and it was gated on the
  # LEGACY `:lazy_migrate` boolean. So setting the current `:migrate_on_touch` knob (`:async` /
  # `:inline`, expert review #40) enabled the cache's only consumer without enabling the poll that
  # fills it: get/0 stayed at its initial 0 forever, `Fathom.Shards`' `head > 0` guard was never
  # true, and migrate-on-touch silently did NOTHING in either mode. Found live 2026-07-30 with
  # MIGRATE_ON_TOUCH=inline and a released HEAD of 2 while HeadCache.get/0 read 0.
  #
  # Every test above calls refresh/0 explicitly, which bypasses the gate — that is why the suite
  # never saw this. These drive the real `:refresh` tick instead.
  describe "the background poll's gate" do
    setup do
      prev_mode = Application.get_env(:fathom, :migrate_on_touch)
      prev_lazy = Application.get_env(:fathom, :lazy_migrate)

      on_exit(fn ->
        if prev_mode == nil,
          do: Application.delete_env(:fathom, :migrate_on_touch),
          else: Application.put_env(:fathom, :migrate_on_touch, prev_mode)

        if prev_lazy == nil,
          do: Application.delete_env(:fathom, :lazy_migrate),
          else: Application.put_env(:fathom, :lazy_migrate, prev_lazy)
      end)

      # A known baseline, then a release the cache has NOT seen yet.
      baseline = HeadCache.refresh()
      {:ok, _} = Migrator.release(baseline + 5, "v#{baseline + 5}")
      %{baseline: baseline, released: baseline + 5}
    end

    # Drive one real background tick and wait for the process to have handled it.
    defp tick do
      send(HeadCache, :refresh)
      _ = :sys.get_state(HeadCache)
      HeadCache.get()
    end

    test ":inline makes the background poll keep the cache warm", %{released: released} do
      Application.put_env(:fathom, :migrate_on_touch, :inline)
      assert tick() == released
    end

    test ":async makes the background poll keep the cache warm", %{released: released} do
      Application.put_env(:fathom, :migrate_on_touch, :async)
      assert tick() == released
    end

    test "the legacy :lazy_migrate boolean still enables the poll", %{released: released} do
      Application.delete_env(:fathom, :migrate_on_touch)
      Application.put_env(:fathom, :lazy_migrate, true)
      assert tick() == released
    end

    test ":off leaves the cache alone (nothing reads it)", %{baseline: baseline} do
      Application.put_env(:fathom, :migrate_on_touch, :off)
      Application.delete_env(:fathom, :lazy_migrate)
      assert tick() == baseline
    end
  end
end
