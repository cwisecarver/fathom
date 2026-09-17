defmodule Fathom.Shard.HandlePoolTest do
  @moduledoc """
  The pure idle-handle store behind connection pooling. Handles are opaque here (atoms), so these
  pin the reuse/eviction/isolation LOGIC without a real SQLite connection — the coordinator does the
  open/close/reset I/O. The load-bearing property is scope isolation: a `:ro` checkout must never be
  handed a `:rw` handle.
  """
  use ExUnit.Case, async: true

  alias Fathom.Shard.HandlePool

  test "new/1 takes max_per_scope and ttl, with defaults" do
    assert %HandlePool{max_per_scope: 1, ttl_ms: 30_000} = HandlePool.new()

    assert %HandlePool{max_per_scope: 4, ttl_ms: 500} =
             HandlePool.new(max_per_scope: 4, ttl_ms: 500)
  end

  test "take on an empty pool misses" do
    assert {:miss, _} = HandlePool.take(HandlePool.new(), :rw)
  end

  test "a returned handle is reused once, then the bucket is empty" do
    {pool, nil} = HandlePool.put(HandlePool.new(), :rw, :h1, 0)
    assert {:hit, :h1, pool} = HandlePool.take(pool, :rw)
    assert {:miss, _} = HandlePool.take(pool, :rw)
  end

  test "SCOPE ISOLATION: a :ro checkout is never handed a :rw handle" do
    {pool, nil} = HandlePool.put(HandlePool.new(max_per_scope: 4), :rw, :writable, 0)

    assert {:miss, pool} = HandlePool.take(pool, :ro),
           "a :ro take must not return the writable handle"

    assert {:hit, :writable, _} = HandlePool.take(pool, :rw)
  end

  test "reuse is MRU — the most recently returned handle comes back first" do
    pool = HandlePool.new(max_per_scope: 4)
    {pool, nil} = HandlePool.put(pool, :rw, :older, 1)
    {pool, nil} = HandlePool.put(pool, :rw, :newer, 2)

    assert {:hit, :newer, pool} = HandlePool.take(pool, :rw)
    assert {:hit, :older, _} = HandlePool.take(pool, :rw)
  end

  test "over cap evicts the LRU (oldest) handle and returns it to close" do
    pool = HandlePool.new(max_per_scope: 1)
    {pool, nil} = HandlePool.put(pool, :rw, :a, 1)
    {pool, evicted} = HandlePool.put(pool, :rw, :b, 2)

    assert evicted == :a, "the older handle is evicted for the caller to close"
    assert HandlePool.count(pool, :rw) == 1
    assert {:hit, :b, _} = HandlePool.take(pool, :rw)
  end

  test "under cap returns nil to close" do
    {_, to_close} = HandlePool.put(HandlePool.new(max_per_scope: 2), :rw, :a, 1)
    assert to_close == nil
  end

  test "eviction with a larger cap drops only the single oldest" do
    pool = HandlePool.new(max_per_scope: 2)
    {pool, nil} = HandlePool.put(pool, :rw, :a, 1)
    {pool, nil} = HandlePool.put(pool, :rw, :b, 2)
    {pool, evicted} = HandlePool.put(pool, :rw, :c, 3)

    assert evicted == :a
    assert {:hit, :c, pool} = HandlePool.take(pool, :rw)
    assert {:hit, :b, _} = HandlePool.take(pool, :rw)
  end

  test "sweep closes handles idle past the ttl, keeps fresh ones" do
    pool = HandlePool.new(max_per_scope: 4, ttl_ms: 100)
    {pool, nil} = HandlePool.put(pool, :rw, :stale, 0)
    {pool, nil} = HandlePool.put(pool, :ro, :fresh, 90)

    {pool, expired} = HandlePool.sweep(pool, 150)

    assert expired == [:stale], "only the handle idle >= ttl is swept"
    assert HandlePool.count(pool) == 1
    assert {:hit, :fresh, _} = HandlePool.take(pool, :ro)
  end

  test "sweep before the ttl expires nothing" do
    {pool, nil} = HandlePool.put(HandlePool.new(ttl_ms: 100), :rw, :a, 0)
    assert {_, []} = HandlePool.sweep(pool, 99)
  end

  test "drain returns every handle across scopes and empties the pool" do
    pool = HandlePool.new(max_per_scope: 4)
    {pool, nil} = HandlePool.put(pool, :ro, :r, 1)
    {pool, nil} = HandlePool.put(pool, :rw, :w, 2)

    {handles, pool} = HandlePool.drain(pool)

    assert Enum.sort(handles) == [:r, :w]
    assert HandlePool.count(pool) == 0
    assert {:miss, _} = HandlePool.take(pool, :ro)
    assert {:miss, _} = HandlePool.take(pool, :rw)
  end

  test "count reports total and per-scope" do
    pool = HandlePool.new(max_per_scope: 4)
    {pool, nil} = HandlePool.put(pool, :ro, :r1, 1)
    {pool, nil} = HandlePool.put(pool, :ro, :r2, 2)
    {pool, nil} = HandlePool.put(pool, :rw, :w1, 3)

    assert HandlePool.count(pool) == 3
    assert HandlePool.count(pool, :ro) == 2
    assert HandlePool.count(pool, :rw) == 1
  end
end
