defmodule Fathom.Shard.UnknownLineageTest do
  @moduledoc """
  A replicating coordinator whose lineage is `:unknown` does not ship, and recovers a lineage as
  soon as the store answers (expert review 2026-10-12 #21).

  ## The defect

  `open_lineage/2` answers `:unknown` when the open's object HEAD fails, and that was fixed for the
  coordinator's life: `Shard.epoch/1` kept answering `{:ok, _}` and every commit shipped as lineage
  0 ("unstated"). On a fresh lock (epoch reset to 1) a follower already at a higher epoch answers
  `:stale_epoch` forever (FILO_NO_QUORUM until idle-drop), and a follower at epoch 1 reads the new
  salt as the same ownership and absorbs the previous owner's WAL underneath — the #3 hybrid.

  A TAKEOVER with an unreadable claim was worse: the lock was written without the previous holder's
  claim (the body cannot carry `:unknown`), so the number every later successor needed was gone.

  ## What is pinned

    * while `:unknown`, the session's epoch gate refuses (`{:error, :lineage_unknown}`), and once
      the HEAD answers the coordinator states exactly the lineage the open would have chosen —
      both liveness modes, mode asserted;
    * a takeover whose claim cannot be read fails the acquire closed and leaves the previous lock,
      claim included, untouched.
  """
  use ExUnit.Case, async: false

  alias Fathom.{Shard, Shards}
  alias Fathom.Shard.{Heartbeat, Storage}

  @modes [:legacy, :heartbeat]

  setup do
    shard = "unklin_#{System.unique_integer([:positive])}"
    prev_storage = Application.get_env(:fathom, :shard_storage)
    prev_repl = Application.get_env(:fathom, :replication_enabled)
    Application.put_env(:fathom, :shard_storage, Fathom.Test.FaultyStorage)
    Application.put_env(:fathom, :replication_enabled, true)

    on_exit(fn ->
      Application.delete_env(:fathom, :storage_fault)
      Shards.drain(shard, 2_000)

      if prev_storage,
        do: Application.put_env(:fathom, :shard_storage, prev_storage),
        else: Application.delete_env(:fathom, :shard_storage)

      if is_nil(prev_repl),
        do: Application.delete_env(:fathom, :replication_enabled),
        else: Application.put_env(:fathom, :replication_enabled, prev_repl)

      for dir <- [Shard.data_dir(), Storage.Local.dir()],
          p <- Path.wildcard(Path.join(dir, "#{shard}*")),
          do: File.rm_rf(p)
    end)

    %{shard: shard}
  end

  defp set_mode!(:legacy) do
    refute Heartbeat.running?()
    :ok
  end

  defp set_mode!(:heartbeat) do
    hb = start_supervised!({Heartbeat, ttl_ms: 30_000})
    _ = :sys.get_state(hb)
    :ok
  end

  defp assert_mode!(pid, :heartbeat), do: assert(is_integer(:sys.get_state(pid).acquire_gen))
  defp assert_mode!(pid, :legacy), do: assert(is_nil(:sys.get_state(pid).acquire_gen))

  # A stored object at lineage `n`, so the expected next lineage is n + 1 rather than the 1 an
  # empty store gives (which a broken resolver could produce by accident).
  defp stored_at!(shard, n) do
    src = Path.join(System.tmp_dir!(), "#{shard}_src.db")
    File.write!(src, "bytes")
    {:ok, _etag, _} = Storage.Local.flush(shard, src, nil, nil, n)
    File.rm(src)
  end

  for mode <- @modes do
    describe "#{mode} mode" do
      setup do: set_mode!(unquote(mode))

      test "an :unknown lineage refuses to ship, then recovers once the HEAD answers", %{
        shard: shard
      } do
        stored_at!(shard, 4)

        Application.put_env(:fathom, :storage_fault, :object_head)
        {:ok, pid} = Shards.ensure(shard)
        assert_mode!(pid, unquote(mode))
        assert :sys.get_state(pid).lineage == :unknown, "the fixture must open with no lineage"

        # Still unreadable: the session's gate must refuse, not ship lineage 0.
        assert Shard.epoch(pid) == {:error, :lineage_unknown}

        # The store answers: the next gate read derives the lineage the open would have.
        Application.delete_env(:fathom, :storage_fault)
        assert {:ok, _epoch} = Shard.epoch(pid)
        assert Shard.lineage(pid) == 5
      end
    end
  end

  test "a takeover whose claim is unreadable fails closed and keeps the previous claim", %{
    shard: shard
  } do
    stored_at!(shard, 4)
    lock = Path.join(Storage.Local.dir(), "#{shard}.lock")

    File.write!(
      lock,
      Jason.encode!(%{
        "owner" => "dead@elsewhere#x",
        "epoch" => 3,
        "expires_at_ms" => System.system_time(:millisecond) - 600_000,
        "claim" => 9
      })
    )

    Application.put_env(:fathom, :storage_fault, :object_head)

    assert Storage.acquire_lease(shard, "b@node#2", 30_000) ==
             {:error, {:transient_lookup, :lineage_unreadable}}

    assert Jason.decode!(File.read!(lock))["claim"] == 9,
           "the previous holder's claim must survive a takeover that could not compute its own"

    Application.delete_env(:fathom, :storage_fault)
    assert {:ok, %{claim: 10}} = Storage.acquire_lease(shard, "b@node#2", 30_000)
  end
end
