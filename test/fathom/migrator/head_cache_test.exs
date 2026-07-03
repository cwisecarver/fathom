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
end
