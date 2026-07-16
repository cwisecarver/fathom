defmodule Fathom.Tenants.TombstonesTest do
  @moduledoc """
  The tenant-deletion re-mint guard (expert review 2026-07-14 #15): a tombstoned shard
  id must be refused by admission so a stray request can never resurrect a deleted tenant
  as an empty shard. The gate is an ETS set (checked O(1) off the Postgres hot path),
  populated locally on delete and via the fleet-wide Oban notification.
  """
  use ExUnit.Case, async: false

  alias Fathom.Shards
  alias Fathom.Tenants
  alias Fathom.Tenants.Tombstones

  @local_dir Path.join(System.tmp_dir!(), "fathom_shards")
  @remote_dir Path.join(System.tmp_dir!(), "fathom_remote_test")

  setup do
    id = "tomb_#{System.unique_integer([:positive])}"

    on_exit(fn ->
      # The tombstone ETS table is app-global; forget this test's id so it can't leak into
      # another test's admission path.
      :ets.delete(Tombstones, id)
      Shards.drain(id, 2_000)

      for dir <- [@local_dir, @remote_dir],
          path <- Path.wildcard(Path.join(dir, "#{id}*")),
          do: File.rm(path)
    end)

    %{id: id}
  end

  test "a non-tombstoned id is not gated and checks out", %{id: id} do
    refute Tenants.tombstoned?(id)
    assert {:ok, _pid, _ref, _path} = Shards.checkout(id)
  end

  test "put/1 tombstones the id and admission refuses it", %{id: id} do
    assert :ok = Tombstones.put(id)
    assert Tenants.tombstoned?(id)

    # The whole point of #15: a request for a deleted subdomain is refused, not re-minted.
    assert {:error, :shard_tombstoned} = Shards.checkout(id)
  end

  test "the fleet-wide delete notification tombstones the id on this node", %{id: id} do
    refute Tenants.tombstoned?(id)

    # Simulate the Oban LISTEN/NOTIFY delivery every node's Tombstones GenServer receives
    # on a delete (string keys, as Oban's JSON round-trip produces).
    pid = Process.whereis(Tombstones)
    send(pid, {:notification, Tombstones.channel(), %{"shard_id" => id}})
    # Sync on the GenServer having processed the message before asserting.
    _ = :sys.get_state(pid)

    assert Tenants.tombstoned?(id)
    assert {:error, :shard_tombstoned} = Shards.checkout(id)
  end
end
