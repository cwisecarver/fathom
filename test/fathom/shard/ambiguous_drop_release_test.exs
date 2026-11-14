defmodule Fathom.Shard.AmbiguousDropReleaseTest do
  @moduledoc """
  A drop flush whose PUT outcome is AMBIGUOUS must release the lock as a released marker even when
  there is no integer lineage to keep (expert review 2026-10-15 #18).

  `release_keeping_claim/1` left a marker only for an integer lineage; with replication off
  (`:disabled`, env-forceable and therefore supported — review default D11) or `:unknown` it was a
  plain DELETE. After a timed-out / errored drop PUT that may still land, the next opener then
  FRESH-CREATED the lock: no steal, so no steal-touch, and its fence etag was exactly the etag the
  in-flight PUT `If-Match`es — so the late PUT succeeded over the new owner's open. The marker
  sends the next acquire down the steal path, whose touch rotates the etag so the late PUT 412s.

  What this file pins is that CONTRACT — the lock survives as a dirty released marker, and the next
  acquire is a takeover of it — not the touch itself: `Storage.Local` (and so the FaultyStorage
  double) has content-hash etags that cannot rotate without the bytes changing, so the touch is an
  S3-only mechanism covered by its own suite. A DEFINITE refusal (nothing was sent) still deletes,
  so the cheap fresh-create open stays the common path.

  The suite runs with replication off, which is exactly the `:disabled` lineage the finding names.
  Both liveness modes: the drop path's fence and release differ between them.
  """
  use ExUnit.Case, async: false

  import ExUnit.CaptureLog

  alias Fathom.{Shard, ShardExecutor, Shards}
  alias Fathom.Shard.Storage
  alias Fathom.Test.{Env, LivenessMode}
  alias Filo.Stmt

  @modes [:legacy, :heartbeat]

  setup do
    shard = "ambrel_#{System.unique_integer([:positive])}"

    on_exit(fn ->
      Shards.drain(shard, 2_000)

      for dir <- [Shard.data_dir(), Storage.Local.dir()],
          f <- Path.wildcard(Path.join(dir, shard <> ".*")),
          do: File.rm(f)
    end)

    Env.put!(:shard_storage, Fathom.Test.FaultyStorage)

    %{shard: shard}
  end

  defp lock_path(shard), do: Path.join(Storage.Local.dir(), shard <> ".lock")

  defp open_dirty!(shard, mode) do
    LivenessMode.set!(mode)
    {:ok, conn} = ShardExecutor.open(shard)
    {:ok, pid} = Shards.ensure(shard)
    LivenessMode.assert!(pid, mode)
    {:ok, _} = ShardExecutor.execute(conn, %Stmt{sql: "CREATE TABLE kv (v TEXT)", args: []})
    :ok = ShardExecutor.close(conn)
    assert Shard.dirty?(pid)
    assert :sys.get_state(pid).lineage == :disabled, "the finding is the no-integer-lineage case"
    pid
  end

  defp stop_with_fault!(shard, pid, fault) do
    Env.put!(:storage_fault, fault)
    ref = Process.monitor(pid)
    capture_log(fn -> Shards.drain(shard, 5_000) end)
    assert_receive {:DOWN, ^ref, :process, ^pid, _}, 5_000
    Application.delete_env(:fathom, :storage_fault)
  end

  for mode <- @modes do
    describe "#{mode} mode" do
      @tag mode: mode
      test "an ambiguous drop PUT leaves a released marker, so the next acquire is a steal",
           %{shard: shard, mode: mode} do
        pid = open_dirty!(shard, mode)

        # A transport-shaped failure: from here we cannot know whether the PUT landed.
        stop_with_fault!(shard, pid, :flush)

        assert File.exists?(Shard.db_path(shard)), "the un-flushed local copy must be kept"

        # Pre-fix the lock was DELETED (lineage :disabled ⇒ plain release), so this read `false`.
        assert Storage.released_dirty?(shard) == true,
               "an ambiguous drop PUT released by DELETE — the next opener fresh-creates the lock " <>
                 "with no steal-touch, and the late PUT lands over it"

        assert Storage.lease_holder(shard) == :free

        # The claimless marker is an ordinary stealable lock to every reader.
        assert {:ok, lease} = Storage.acquire_lease(shard, "migrator@test@#{shard}", 30_000)
        assert lease[:took_over] == true, "the next acquire must take the STEAL path"
        refute Map.has_key?(lease, :claim), "replication is off: no lineage claim to carry"
        :ok = Storage.release_lease(shard, lease)
      end

      @tag mode: mode
      test "a definite drop refusal still releases by delete", %{shard: shard, mode: mode} do
        pid = open_dirty!(shard, mode)

        # The store refused before applying anything (the S3 single-PUT ceiling).
        stop_with_fault!(shard, pid, :flush_too_large)

        assert File.exists?(Shard.db_path(shard))
        refute File.exists?(lock_path(shard)), "a definite refusal needs no marker"
      end
    end
  end
end
