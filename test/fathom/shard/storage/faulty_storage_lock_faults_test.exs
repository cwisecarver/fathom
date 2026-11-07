defmodule Fathom.Shard.Storage.FaultyStorageLockFaultsTest do
  @moduledoc """
  Pins the lock faults of `Fathom.Test.FaultyStorage` (expert review 2026-11-07 #26/#6 — the harness
  prerequisite) so the double cannot drift under the tests built on it.

  `:acquire` fails BEFORE anything is written; `:acquire_lands_then_errors` is its opposite — the
  lock is written and the caller is told it was not (a lost reply on the create PUT). Until the
  latter existed the double could only fail an acquire before `Local.acquire_lease` ran, so no test
  could stage a lock that names a node whose acquire reported failure: the whole class of bugs that
  lives in that gap was inexpressible.
  """
  use ExUnit.Case, async: false

  alias Fathom.Shard.Storage.Local
  alias Fathom.Test.FaultyStorage

  @owner "a@node#1"

  setup do
    shard = "faulty_lock_#{System.unique_integer([:positive])}"

    on_exit(fn ->
      Application.delete_env(:fathom, :storage_fault)
      File.rm(Path.join(Local.dir(), shard <> ".lock"))
    end)

    %{shard: shard}
  end

  test ":acquire writes NOTHING", %{shard: shard} do
    Application.put_env(:fathom, :storage_fault, :acquire)

    assert {:error, {:transient_lookup, :s3_unreachable}} =
             FaultyStorage.acquire_lease(shard, @owner, 30_000)

    assert Local.lease_holder(shard) == :free
  end

  test ":acquire_lands_then_errors writes the lock AND reports failure", %{shard: shard} do
    Application.put_env(:fathom, :storage_fault, :acquire_lands_then_errors)

    assert {:error, :s3_unreachable} = FaultyStorage.acquire_lease(shard, @owner, 30_000)

    # The lock exists and names the caller: that is the fault.
    assert {:held, @owner} = Local.lease_holder(shard)
  end

  test "the error is not a transient_lookup: that shape means nothing was written", %{
    shard: shard
  } do
    Application.put_env(:fathom, :storage_fault, :acquire_lands_then_errors)

    assert {:error, reason} = FaultyStorage.acquire_lease(shard, @owner, 30_000)
    refute match?({:transient_lookup, _}, reason)
  end

  test "release_if_owned gives the landed lock back, and only to its owner", %{shard: shard} do
    Application.put_env(:fathom, :storage_fault, :acquire_lands_then_errors)
    {:error, _} = FaultyStorage.acquire_lease(shard, @owner, 30_000)
    Application.delete_env(:fathom, :storage_fault)

    assert :ok = FaultyStorage.release_if_owned(shard, "someone@else#2")
    assert {:held, @owner} = Local.lease_holder(shard), "released a lock that was not ours"

    assert :ok = FaultyStorage.release_if_owned(shard, @owner)
    assert Local.lease_holder(shard) == :free
  end

  test "without a fault the double acquires normally", %{shard: shard} do
    assert {:ok, %{owner: @owner, epoch: 1, lock_etag: etag}} =
             FaultyStorage.acquire_lease(shard, @owner, 30_000)

    assert is_binary(etag)
  end
end
