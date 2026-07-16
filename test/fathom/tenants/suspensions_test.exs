defmodule Fathom.Tenants.SuspensionsTest do
  @moduledoc """
  The tenant-suspension admission gate (expert review 2026-07-14 #20): a suspended shard id is
  denied a new stream (a distinct 403 `FILO_TENANT_SUSPENDED`), and resume lifts it. The gate is
  an ETS set (checked O(1) off the Postgres hot path), reversible via the add/remove notification.
  """
  use ExUnit.Case, async: false

  alias Fathom.{ShardExecutor, Shards, Tenants}
  alias Fathom.Tenants.Suspensions

  @local_dir Path.join(System.tmp_dir!(), "fathom_shards")
  @remote_dir Path.join(System.tmp_dir!(), "fathom_remote_test")

  setup do
    id = "susp_#{System.unique_integer([:positive])}"

    on_exit(fn ->
      :ets.delete(Suspensions, id)
      Shards.drain(id, 2_000)

      for dir <- [@local_dir, @remote_dir],
          path <- Path.wildcard(Path.join(dir, "#{id}*")),
          do: File.rm(path)
    end)

    %{id: id}
  end

  test "a non-suspended id is not gated and checks out", %{id: id} do
    refute Tenants.suspended?(id)
    assert {:ok, _pid, _ref, _path} = Shards.checkout(id)
  end

  test "put/1 suspends and admission refuses with :shard_suspended", %{id: id} do
    assert :ok = Suspensions.put(id)
    assert Tenants.suspended?(id)
    assert {:error, :shard_suspended} = Shards.checkout(id)
  end

  test "remove/1 lifts the suspension and the tenant serves again", %{id: id} do
    Suspensions.put(id)
    assert :ok = Suspensions.remove(id)
    refute Tenants.suspended?(id)
    assert {:ok, _pid, _ref, _path} = Shards.checkout(id)
  end

  test "a suspended stream open surfaces a distinct 403", %{id: id} do
    Suspensions.put(id)
    assert {:error, %{status: 403, code: "FILO_TENANT_SUSPENDED"}} = ShardExecutor.open(id)
  end

  test "the fleet-wide suspend/resume notification adds then clears the id", %{id: id} do
    pid = Process.whereis(Suspensions)

    send(pid, {:notification, Suspensions.channel(), %{"shard_id" => id, "suspended" => true}})
    _ = :sys.get_state(pid)
    assert Tenants.suspended?(id)

    send(pid, {:notification, Suspensions.channel(), %{"shard_id" => id, "suspended" => false}})
    _ = :sys.get_state(pid)
    refute Tenants.suspended?(id)
  end
end
