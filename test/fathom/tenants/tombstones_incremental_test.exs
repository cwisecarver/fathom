defmodule Fathom.Tenants.TombstonesIncrementalTest do
  @moduledoc """
  Expert review 2026-07-24 #30. The tombstone set is append-only by design — a tombstone is
  permanent — but the periodic refresh re-read EVERY id ever deleted, on every node, every 5
  minutes. So both the Postgres read and this process's heap scaled with cumulative LIFETIME
  deletions rather than with anything current: at a million lifetime deletions, a full-table read
  plus a million-element list materialized per node per refresh.

  The refresh is now incremental. These pin the two properties that make that safe, since getting
  either wrong re-opens the re-mint hole the guard exists to close: the boot load stays FULL (a
  booting node must see every tombstone, not just recent ones), and nothing is ever removed.
  """
  use ExUnit.Case, async: false

  alias Fathom.Tenants.Tombstones

  defp uniq_atom(prefix), do: String.to_atom("#{prefix}_#{System.unique_integer([:positive])}")

  defp start_isolated(opts) do
    name = uniq_atom("tomb_inc_srv")
    table = uniq_atom("tomb_inc_tab")

    start_supervised!(
      Supervisor.child_spec(
        {Tombstones, Keyword.merge([name: name, table: table], opts)},
        id: {:tomb_inc, name}
      )
    )

    {name, table}
  end

  test "the boot load is full, and the periodic refresh only asks for what changed" do
    parent = self()

    # The full loader records that it was called, and returns the historical set.
    full = fn ->
      send(parent, :full_called)
      ["ancient_1", "ancient_2"]
    end

    # The incremental loader records the cursor it was handed.
    since = fn dt ->
      send(parent, {:since_called, dt})
      ["recent_1"]
    end

    {name, table} = start_isolated(loader: full, since_loader: since)

    # Boot: the FULL set. A node coming up must see every tombstone ever, or a stray request for an
    # old deleted subdomain re-mints it as an empty shard.
    assert_receive :full_called, 2_000
    assert :ets.member(table, "ancient_1")
    assert :ets.member(table, "ancient_2")

    # Drive one refresh directly rather than waiting out the 5-minute timer.
    send(name, :refresh)
    _ = :sys.get_state(name)

    assert_receive {:since_called, %DateTime{}}, 2_000

    # The incremental result is unioned in; the historical set is untouched.
    assert :ets.member(table, "recent_1")
    assert :ets.member(table, "ancient_1"), "an incremental refresh must never shrink the set"
  end

  test "a failed incremental refresh leaves the existing set intact" do
    {name, table} =
      start_isolated(loader: fn -> ["kept"] end, since_loader: fn _ -> raise "boom" end)

    assert :ets.member(table, "kept")

    send(name, :refresh)
    _ = :sys.get_state(name)

    assert :ets.member(table, "kept"),
           "a refresh failure must never empty the guard — that is the 410 contract"

    assert Process.alive?(Process.whereis(name)), "and it must not take the process down"
  end
end
