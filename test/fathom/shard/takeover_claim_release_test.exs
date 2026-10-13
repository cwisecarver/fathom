defmodule Fathom.Shard.TakeoverClaimReleaseTest do
  @moduledoc """
  A TAKEOVER's lineage claim survives every release that did not stamp it (expert review 2026-10-12
  #3), not only the failed-drop-flush releases d80e554 covered.

  ## The defect

  X held the shard at lineage C, shipped A2 frames under it, and died before a flush stamped C onto
  the object. Y takes over: its lock records a claim strictly above C (`with_takeover_claim/3`), and
  that lock is the ONLY durable record of the bound — the object still carries an older lineage.
  Every release other than the failed-drop-flush ones was a plain DELETE:

    * the pull failure after acquire (`open_with_lease/8`'s `else`),
    * an open that raised after acquire (`abandon_open/5`),
    * a CLEAN idle drop (`drop_clean/1`) — Y opened and served reads, no write, no flush.

  The next opener then fresh-created at `next_lineage(object)`, which can be at or below C: followers
  see one ownership (or `:stale_epoch` forever) and promote-on-open declines X's replica
  (`rl >= lineage`), so X's quorum-acked writes are unrecoverable.

  ## What is pinned

  After each of those releases, the next ownership's lineage is strictly above X's claim. Pre-fix it
  came back as `next_lineage(object)` (3 here). Both liveness modes, mode asserted.
  """
  use ExUnit.Case, async: false

  import ExUnit.CaptureLog

  alias Fathom.{Shard, Shards}
  alias Fathom.Shard.{Heartbeat, Storage}

  @modes [:legacy, :heartbeat]
  # X's lineage: shipped under, never stamped. The object carries @stamped, so a fresh create would
  # choose @stamped + 1 = 3, far below it.
  @x_claim 9
  @stamped 2

  setup do
    shard = "tkclaim_#{System.unique_integer([:positive])}"
    prev_storage = Application.get_env(:fathom, :shard_storage)
    prev_repl = Application.get_env(:fathom, :replication_enabled)
    Application.put_env(:fathom, :shard_storage, Fathom.Test.FaultyStorage)
    Application.put_env(:fathom, :replication_enabled, true)

    on_exit(fn ->
      Application.delete_env(:fathom, :storage_fault)
      Application.delete_env(:fathom, :faulty_before)
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

  defp lock_path(shard), do: Path.join(Storage.Local.dir(), "#{shard}.lock")

  # A stored object stamped at an OLD lineage, and dead X's lock recording claim @x_claim.
  defp dead_x!(shard) do
    # A REAL database, so the successor's open serves it rather than tripping quick_check (which
    # would route the drop to a corrupt-local release instead of the clean one under test).
    src = Path.join(System.tmp_dir!(), "#{shard}_src.db")
    {:ok, c} = Fathom.Shard.Connection.open(src)
    :ok = Fathom.Shard.Connection.exec(c, "CREATE TABLE kv (v TEXT)")
    :ok = Fathom.Shard.Connection.exec(c, "PRAGMA wal_checkpoint(TRUNCATE)")
    Fathom.Shard.Connection.close(c)
    {:ok, _etag, _} = Storage.Local.flush(shard, src, nil, nil, @stamped)
    for sfx <- ["", "-wal", "-shm"], do: File.rm(src <> sfx)

    File.write!(
      lock_path(shard),
      Jason.encode!(%{
        "owner" => "dead@elsewhere#x",
        "epoch" => 3,
        "expires_at_ms" => System.system_time(:millisecond) - 600_000,
        "claim" => @x_claim
      })
    )
  end

  # The open runs in `handle_continue`, so `ensure/1` can answer `{:ok, pid}` before it fails. Wait
  # for the coordinator to stop on its own.
  defp open_fails!(shard) do
    case Shards.ensure(shard) do
      {:ok, pid} ->
        ref = Process.monitor(pid)
        assert_receive {:DOWN, ^ref, :process, ^pid, reason}, 10_000
        assert match?({:shutdown, _}, reason) or reason == :noproc
        reason

      {:error, reason} ->
        reason
    end
  end

  # The next ownership after Y's release: must open strictly above X's claim.
  defp assert_next_owner_above_x!(shard, mode) do
    Application.delete_env(:fathom, :storage_fault)
    Application.delete_env(:fathom, :faulty_before)
    {:ok, z} = Shards.ensure(shard)
    assert_mode!(z, mode)
    lz = Shard.lineage(z)

    assert lz > @x_claim,
           "the next owner opened at lineage #{lz}, not above dead X's #{@x_claim}: Y's release " <>
             "dropped the takeover claim"
  end

  for mode <- @modes do
    describe "#{mode} mode" do
      setup do: set_mode!(unquote(mode))

      test "the successor's PULL fails: the release keeps the takeover claim", %{shard: shard} do
        dead_x!(shard)
        Application.put_env(:fathom, :storage_fault, :pull)

        capture_log(fn -> open_fails!(shard) end)

        body = Jason.decode!(File.read!(lock_path(shard)))
        assert body["owner"] == Storage.released_owner(), "the pull failure must leave a marker"
        assert body["claim"] > @x_claim

        assert_next_owner_above_x!(shard, unquote(mode))
      end

      test "the successor's open RAISES after acquire: the release keeps the takeover claim", %{
        shard: shard
      } do
        dead_x!(shard)

        # The takeover re-check's HEAD raises inside the coordinator (abandon_open/5) or inside
        # the pull task (the pull-failure release) — both releases are under test here.
        Application.put_env(
          :fathom,
          :faulty_before,
          {:object_etag, fn id -> if id == shard, do: raise("boom") end}
        )

        capture_log(fn -> open_fails!(shard) end)

        assert Jason.decode!(File.read!(lock_path(shard)))["claim"] > @x_claim
        assert_next_owner_above_x!(shard, unquote(mode))
      end

      test "the successor drops CLEAN without a write: the release keeps the takeover claim", %{
        shard: shard
      } do
        dead_x!(shard)
        {:ok, y} = Shards.ensure(shard)
        assert_mode!(y, unquote(mode))
        ly = Shard.lineage(y)
        assert ly > @x_claim, "the fixture must be a takeover above X"
        refute Shard.dirty?(y)

        ref = Process.monitor(y)

        log =
          capture_log(fn ->
            :ok = Shards.drain(shard, 5_000)
            assert_receive {:DOWN, ^ref, :process, ^y, _}, 5_000
          end)

        refute log =~ "quick_check", "the drop must take the CLEAN path"

        assert Jason.decode!(File.read!(lock_path(shard)))["claim"] == ly
        assert_next_owner_above_x!(shard, unquote(mode))
      end
    end
  end

  test "a clean drop of a fresh-create lease (no claim) still deletes the lock", %{shard: shard} do
    {:ok, a} = Shards.ensure(shard)
    refute Map.has_key?(:sys.get_state(a).lease, :claim)
    ref = Process.monitor(a)
    :ok = Shards.drain(shard, 5_000)
    assert_receive {:DOWN, ^ref, :process, ^a, _}, 5_000
    refute File.exists?(lock_path(shard))
  end
end
