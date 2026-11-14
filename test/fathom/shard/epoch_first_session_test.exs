defmodule Fathom.Shard.EpochFirstSessionTest do
  @moduledoc """
  `Fathom.Shard.epoch_first_session/1` (expert review 2026-10-22 r8 #16): the coordinator's epoch
  reply plus one flag, `true` only for the FIRST successful answer of a coordinator life that took a
  fresh replication lineage at open. The replication Session uses it to skip the reset push every
  follower is already known to answer `:unknown_shard`. A wrong `true` costs an unnecessary seed; a
  wrong `false` costs the doomed round it saves today. Not async: shards and app env are global.
  """
  use ExUnit.Case, async: false

  alias Fathom.Shard
  alias Fathom.Shards
  alias Fathom.Test.Env

  # The env is set from the `:replication_enabled` tag HERE, before the cleanup `on_exit` below is
  # registered, so it is restored AFTER the cleanup drain (on_exit callbacks run in reverse order).
  setup %{replication_enabled: enabled} do
    id = "epfirst_#{System.unique_integer([:positive])}"
    Env.put!(:replication_enabled, enabled)

    on_exit(fn ->
      _ = Shards.drain(id)

      for dir <- [Shard.data_dir(), Shard.Storage.Local.dir()],
          s <- [".db", ".db-wal", ".db-shm", ".db.etag", ".lock"],
          do: File.rm(Path.join(dir, id <> s))
    end)

    %{id: id}
  end

  defp open!(id) do
    {:ok, coordinator, ref, _path} = Shards.checkout(id)
    Shard.checkin(coordinator, ref)
    coordinator
  end

  defp stop!(id, coordinator) do
    mref = Process.monitor(coordinator)
    _ = Shards.drain(id)
    assert_receive {:DOWN, ^mref, :process, ^coordinator, _}, 5_000
  end

  @tag replication_enabled: true
  test "replication on: true exactly once per coordinator life, then false", %{id: id} do
    c1 = open!(id)
    assert is_integer(Shard.lineage(c1)) and Shard.lineage(c1) > 0, "the open took a lineage"

    assert {:ok, epoch, true} = Shard.epoch_first_session(c1)
    assert {:ok, ^epoch, false} = Shard.epoch_first_session(c1), "a restarted Session gets false"
    assert {:ok, ^epoch} = Shard.epoch(c1), "epoch/1 keeps its shape"

    # A NEW coordinator life takes a new lineage, so its first Session is told true again.
    stop!(id, c1)
    c2 = open!(id)
    assert c2 != c1
    assert {:ok, _epoch, true} = Shard.epoch_first_session(c2)
  end

  @tag replication_enabled: true
  test "an epoch/1 answer consumes the flag", %{id: id} do
    c = open!(id)

    assert {:ok, epoch} = Shard.epoch(c)
    assert {:ok, ^epoch, false} = Shard.epoch_first_session(c)
  end

  @tag replication_enabled: false
  test "replication off (lineage :disabled): always false", %{id: id} do
    c = open!(id)

    assert Shard.lineage(c) == 0
    assert {:ok, _epoch, false} = Shard.epoch_first_session(c)
  end
end
