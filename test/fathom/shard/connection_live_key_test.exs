defmodule Fathom.Shard.ConnectionLiveKeyTest do
  @moduledoc """
  Expert review 2026-10-22 r8 #23: the live-connection table's key was `:erlang.ref_to_list/1`, a
  35-element charlist (~80 words per row, ~690 ns per call, twice per handle). It is now the ref's
  external-format binary (~15 words, ~60 ns). `served_kb_per_shard` cannot see a 0.3% change, so
  this ceiling is the measurement.

  The other half of the key's contract, that it must not keep the NIF resource alive, is pinned by
  `connection_live_gauge_test.exs`'s orphan test (expert review 2026-10-17 #2).
  """
  use ExUnit.Case, async: false

  alias Fathom.Shard.Connection

  @live Fathom.Shard.Connection.Live

  @tag :bench
  test "bench: a live-table row costs under 32 words (was ~80 with the charlist key)" do
    dir = Path.join(System.tmp_dir!(), "live_key_#{System.unique_integer([:positive])}")
    File.mkdir_p!(dir)
    on_exit(fn -> File.rm_rf(dir) end)

    n = 200
    # One file, many handles: the rows differ only by key. Non-tenant, so the row's path is nil.
    path = Path.join(dir, "k.db")
    before = :ets.info(@live, :memory)
    conns = for _ <- 1..n, do: elem(Connection.open(path), 1)
    per_row = (:ets.info(@live, :memory) - before) / n
    Enum.each(conns, &Connection.close/1)

    assert per_row < 32, "live-table row is #{Float.round(per_row, 1)} words"
  end
end
