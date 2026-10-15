defmodule Fathom.Shard.Replication.FollowerStateFsyncTest do
  @moduledoc """
  Seed install and the lineage/ordinal state files honour `:replication_fsync`
  (expert review 2026-10-15 #21, default D13).

  With fsync ON a push was datasynced before its ack, but a SEED was renamed into place straight
  from the page cache, and the lineage file was a plain `File.write`. An OS crash could then leave
  a whole-looking replica over zero-filled pages, which recovery would rank.

  An fsync is invisible short of an OS crash, so these tests observe the
  `[:fathom, :replication, :follower_state_sync]` event the sync points emit. They pin that each
  sync point RUNS under the flag, and that none runs with it off (the RAM-ack default). They do not
  and cannot prove the bytes reached the platter.
  """
  use ExUnit.Case, async: false

  alias Fathom.Shard.Replication.Follower
  alias Fathom.Shard.Replication.Protocol

  setup do
    id = "fsyncst_#{System.unique_integer([:positive])}"
    name = :"fsyncst_f_#{System.unique_integer([:positive])}"
    dir = Path.join(System.tmp_dir!(), to_string(name))
    File.mkdir_p!(dir)
    prev = Application.get_env(:fathom, :replication_fsync)

    on_exit(fn ->
      if is_nil(prev),
        do: Application.delete_env(:fathom, :replication_fsync),
        else: Application.put_env(:fathom, :replication_fsync, prev)

      File.rm_rf(dir)
    end)

    start_supervised!({Follower, name: name, port: 0, dir: dir}, id: name)

    me = self()
    handler = "fsyncst-#{id}"

    :telemetry.attach(
      handler,
      [:fathom, :replication, :follower_state_sync],
      fn _e, _m, %{what: what}, _ -> send(me, {:synced, what}) end,
      nil
    )

    on_exit(fn -> :telemetry.detach(handler) end)
    %{id: id, name: name}
  end

  defp seed!(name, id) do
    begin = %Protocol.SeedBegin{
      shard_id: id,
      epoch: 1,
      wal_gen: 0,
      salt1: 77,
      wal_offset: 0,
      db_size: 0,
      wal_size: 0,
      lineage: 3
    }

    seeds = Follower.begin_seed(name, %{}, begin)
    seeds = Follower.stamp_seed_ordinal(seeds, id, 7)
    assert {{:ok, 0}, %{}} = Follower.finish_seed(name, seeds, id)
  end

  test "with REPLICATION_FSYNC on, the seed temps, lineage and ordinal are synced", ctx do
    Application.put_env(:fathom, :replication_fsync, true)
    seed!(ctx.name, ctx.id)

    assert_received {:synced, :seed}, "the seed temps were renamed into place without a datasync"
    assert_received {:synced, :lineage}, "the lineage file was written without an fsync"
    assert_received {:synced, :ordinal}, "the ordinal file was written without an fsync"

    # And they say what they should.
    assert File.read!(Follower.lineage_path(ctx.name, ctx.id)) == "3"
    assert File.read!(Follower.ordinal_path(ctx.name, ctx.id)) == "7 77"
    assert %{lineage: 3, wal_ordinal: 7} = Follower.state_of(ctx.name, ctx.id)
  end

  test "with it off (the RAM-ack default) nothing is synced, and the files are still written",
       ctx do
    Application.put_env(:fathom, :replication_fsync, false)
    seed!(ctx.name, ctx.id)

    refute_received {:synced, _}
    assert File.read!(Follower.lineage_path(ctx.name, ctx.id)) == "3"
    assert File.read!(Follower.ordinal_path(ctx.name, ctx.id)) == "7 77"
  end
end
