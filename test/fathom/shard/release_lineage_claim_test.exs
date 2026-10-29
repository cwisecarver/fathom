defmodule Fathom.Shard.ReleaseLineageClaimTest do
  @moduledoc """
  A release whose drop flush FAILED keeps the holder's lineage claim, so the next owner opens at a
  strictly higher lineage (expert review 2026-10-12 #3).

  ## The defect

  A coordinator that opened on a fresh-created lock uses `next_lineage(object)` = L and ships A2
  frames under it; only a successful flush stamps L onto the object. When the DROP flush fails (S3
  brownout, `:drop_flush_timeout`, a fence skip) `keep_local_release_lease/3` keeps the local copy
  and released the lease by DELETING the lock. The next opener's fresh create then recomputed
  `next_lineage` of the unchanged object — L again. Two ownerships, one label:

    * promote-on-open refuses the old owner's replica (`rl >= lineage`), so its quorum-acked writes
      are not recovered;
    * a follower holding the old owner's L frames sees an equal lineage and an equal (reset-to-1)
      lock epoch, so `FollowerLog.reseed?/2` says "same ownership", the salt change resets with a
      `:complete` verdict, and the follower absorbs the old WAL and appends the new owner's frames
      onto it — a promotable hybrid.

  ## What is pinned

  The invariant: after a non-stamping release, the next ownership's lineage is STRICTLY ABOVE the
  previous holder's. Pre-fix the reopen came back at exactly L. Both liveness modes, because the
  drop/fence/release paths differ between them, and each asserts the mode actually took.
  """
  use ExUnit.Case, async: false

  import ExUnit.CaptureLog

  alias Fathom.{Shard, ShardExecutor, Shards}
  alias Fathom.Shard.{Heartbeat, Storage}
  alias Filo.Stmt

  @modes [:legacy, :heartbeat]

  setup do
    shard = "relclaim_#{System.unique_integer([:positive])}"
    prev_storage = Application.get_env(:fathom, :shard_storage)
    prev_repl = Application.get_env(:fathom, :replication_enabled)

    # FaultyStorage models S3's lock-etag contract, so the marker write is If-Match fenced here too.
    Application.put_env(:fathom, :shard_storage, Fathom.Test.FaultyStorage)

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
    refute Heartbeat.running?(), "legacy mode needs the heartbeat OFF (config/test.exs default)"
    :ok
  end

  defp set_mode!(:heartbeat) do
    hb = start_supervised!({Heartbeat, ttl_ms: 30_000})
    _ = :sys.get_state(hb)
    assert Heartbeat.running?()
    :ok
  end

  defp assert_mode!(pid, :heartbeat),
    do: assert(is_integer(:sys.get_state(pid).acquire_gen), "heartbeat mode did not take")

  defp assert_mode!(pid, :legacy),
    do: assert(is_nil(:sys.get_state(pid).acquire_gen), "legacy mode did not take")

  defp lock_body(shard),
    do: Path.join(Storage.Local.dir(), "#{shard}.lock") |> File.read!() |> Jason.decode!()

  for mode <- @modes do
    describe "#{mode} mode" do
      setup do: set_mode!(unquote(mode))

      test "a failed drop flush's release keeps the claim; the next owner opens above it", %{
        shard: shard
      } do
        # Open while replicating, so the coordinator fixes an integer lineage L for its life.
        Application.put_env(:fathom, :replication_enabled, true)
        {:ok, a} = Shards.ensure(shard)
        assert_mode!(a, unquote(mode))
        l = Shard.lineage(a)
        assert is_integer(l) and l > 0, "the fixture must open with a stated lineage"

        # Write WITHOUT a quorum ship (no followers in mix test). The lineage is already fixed, and
        # what is under test is the lock the release leaves behind, not the frames themselves: A
        # has a claim L that nothing durable records.
        Application.put_env(:fathom, :replication_enabled, false)
        {:ok, conn} = ShardExecutor.open(shard)
        {:ok, _} = ShardExecutor.execute(conn, %Stmt{sql: "CREATE TABLE kv (v TEXT)", args: []})

        {:ok, _} =
          ShardExecutor.execute(conn, %Stmt{sql: "INSERT INTO kv VALUES ('x')", args: []})

        :ok = ShardExecutor.close(conn)
        assert Shard.dirty?(a), "the fixture must be dirty or the drop takes the clean path"

        # Fail the drop flush: keep_local_release_lease/3.
        Application.put_env(:fathom, :storage_fault, :flush)
        ref = Process.monitor(a)

        log =
          capture_log(fn ->
            Shards.drain(shard, 5_000)
            assert_receive {:DOWN, ^ref, :process, ^a, _}, 5_000
          end)

        assert log =~ "keeping local copy", "the drop did not take the failed-flush release path"
        Application.delete_env(:fathom, :storage_fault)

        # Released, not stranded: a foreign owner sees it free.
        assert Storage.lease_holder(shard) == :free

        # The next ownership (replicating again) must open strictly above L. Pre-fix the lock was
        # deleted, the reopen was a fresh create, and this came back == L.
        Application.put_env(:fathom, :replication_enabled, true)
        {:ok, b} = Shards.ensure(shard)
        assert_mode!(b, unquote(mode))
        lb = Shard.lineage(b)

        assert lb > l,
               "the next owner reused lineage #{l} after a non-stamping release (got #{lb})"

        assert lock_body(shard)["claim"] == lb
      end
    end
  end

  test "a clean release still deletes the lock (the 1-RTT fresh-create open stays the common path)",
       %{shard: shard} do
    Application.put_env(:fathom, :replication_enabled, true)
    {:ok, a} = Shards.ensure(shard)
    assert is_integer(Shard.lineage(a))
    ref = Process.monitor(a)
    :ok = Shards.drain(shard, 5_000)
    assert_receive {:DOWN, ^ref, :process, ^a, _}, 5_000
    refute File.exists?(Path.join(Storage.Local.dir(), "#{shard}.lock"))
  end

  test "the marker is a dead foreign lock: free to probe, superseded to its old holder" do
    id = "relclaim_unit_#{System.unique_integer([:positive])}"
    on_exit(fn -> File.rm(Path.join(Storage.Local.dir(), "#{id}.lock")) end)

    {:ok, lease} = Storage.Local.acquire_lease(id, "a@node#1", 60_000)
    assert :ok = Storage.Local.release_lease_keeping_claim(id, lease, 7)

    # `released_by` names the holder, so a steal of this marker still excludes it from the
    # recovery floor (Jepsen 2026-10-28, see released_by_floor_test.exs).
    assert lock_body(id) == %{
             "owner" => Storage.released_owner(),
             "epoch" => lease.epoch,
             "expires_at_ms" => 0,
             "claim" => 7,
             "released_by" => "a@node#1"
           }

    assert Storage.Local.lease_holder(id) == :free
    assert Storage.Local.check_lease(id, lease) == {:error, :superseded}

    # Not ours any more: a second release-with-claim leaves the marker alone.
    {:ok, other} = Storage.Local.acquire_lease(id, "b@node#2", 60_000)
    assert :ok = Storage.Local.release_lease_keeping_claim(id, lease, 3)
    assert lock_body(id)["owner"] == "b@node#2"
    :ok = Storage.Local.release_lease(id, other)
  end
end
