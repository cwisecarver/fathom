defmodule Fathom.LocalStorageFleetGuardTest do
  @moduledoc """
  Expert review 2026-11-07 #8. Symptom: a prod node with replication followers and `SHARD_STORAGE`
  UNSET booted clean. The guard keyed only on `:lb_backends` (the rebalancer's topology) and
  compared `:shard_storage` with no default — but unset is `nil` there while `Storage.backend/0`
  defaults it to Local, whose lease is node-local, so the DEFAULT store was the one the guard
  could not see. Invariant: a prod node on Local (explicit or by default) that replicates to
  others refuses to boot, unless `:allow_local_storage` acknowledges it; `:lb_backends` is
  refused regardless of the ack.

  Tests the pure `local_storage_fleet_problem/1` with an explicit config map, so no global app env
  is touched and the module can run async.
  """
  use ExUnit.Case, async: true

  alias Fathom.Application, as: App

  @local Fathom.Shard.Storage.Local
  @s3 Fathom.Shard.Storage.S3
  @followers [{"n2", "10.0.0.2", 9100}]

  defp cfg(overrides) do
    Map.merge(
      %{
        env: :prod,
        storage: @local,
        lb_backends: %{},
        replication_followers: [],
        replication_enabled: false,
        allow_local_storage: false
      },
      Map.new(overrides)
    )
  end

  test "prod + Local + replication followers is refused (pre-fix: nil — the reported bug)" do
    assert App.local_storage_fleet_problem(cfg(replication_followers: @followers)) =~
             "no cross-node single-writer"

    assert App.local_storage_fleet_problem(cfg(replication_followers: @followers)) =~
             ":replication_followers is set"
  end

  test "prod + Local + replication on (the prod default) is refused even before followers parse" do
    assert App.local_storage_fleet_problem(cfg(replication_enabled: true)) =~
             ":replication_enabled is on"
  end

  test "prod + Local + lb_backends is still refused" do
    assert App.local_storage_fleet_problem(cfg(lb_backends: %{"n1" => "n1:8080"})) =~
             ":lb_backends is set"
  end

  test "the ack covers the replication signal only, never lb_backends" do
    assert App.local_storage_fleet_problem(
             cfg(
               replication_followers: @followers,
               replication_enabled: true,
               allow_local_storage: true
             )
           ) == nil

    assert App.local_storage_fleet_problem(
             cfg(lb_backends: %{"n1" => "n1:8080"}, allow_local_storage: true)
           ) =~ ":lb_backends is set"
  end

  test "a single-node prod Local deploy (replication off, no followers, no backends) boots" do
    assert App.local_storage_fleet_problem(cfg([])) == nil
  end

  test "S3 and non-prod envs are never refused" do
    full = [
      lb_backends: %{"n1" => "n1:8080"},
      replication_followers: @followers,
      replication_enabled: true
    ]

    assert App.local_storage_fleet_problem(cfg([storage: @s3] ++ full)) == nil

    for env <- [:dev, :test] do
      assert App.local_storage_fleet_problem(cfg([env: env] ++ full)) == nil
    end
  end
end
