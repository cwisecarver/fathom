defmodule Fathom.Shard.Replication.SeedLineageFenceTest do
  @moduledoc """
  A follower refuses a seed from a LOWER lineage than the replica it holds (expert review 2026-09-29
  #17).

  The frame MAC covers a seed's header but no nonce, so a `seed_begin` sniffed off the replication
  network verifies forever, and a replayer could stream its own bytes under it. `begin_seed/3`
  accepted a seed from any connection with no ownership comparison. Lineage is the monotonic
  ownership counter, so no legitimate seed ever goes backwards; refusing one bounds a replay to the
  current ownership (the full fix — binding frames to a connection — is a parked wire change).
  """
  use ExUnit.Case, async: false

  alias Fathom.Shard.Replication.Follower
  alias Fathom.Shard.Replication.Protocol

  setup do
    id = "seedlin_#{System.unique_integer([:positive])}"
    name = :"seedlin_f_#{System.unique_integer([:positive])}"
    dir = Path.join(System.tmp_dir!(), to_string(name))
    File.mkdir_p!(dir)
    on_exit(fn -> File.rm_rf(dir) end)
    start_supervised!({Follower, name: name, port: 0, dir: dir}, id: name)

    # The replica this follower already holds: lineage 5.
    File.write!(Follower.db_path(name, id), "db")
    File.write!(Follower.wal_path(name, id), "")
    :ok = Follower.seed(name, id, 1, 0, 0, 0, 5)

    %{id: id, name: name}
  end

  defp begin(id, lineage) do
    %Protocol.SeedBegin{
      shard_id: id,
      epoch: 1,
      wal_gen: 0,
      salt1: 0,
      wal_offset: 0,
      db_size: 4096,
      wal_size: 0,
      lineage: lineage
    }
  end

  test "a lower-lineage seed (deposed primary or replayed frame) is refused", ctx do
    assert Follower.begin_seed(ctx.name, %{}, begin(ctx.id, 3)) == %{},
           "a seed from lineage 3 was opened over a lineage-5 replica"
  end

  # Expert review 2026-11-07 #3(b). A promotion that failed verification leaves its replica FENCED
  # (torn, lineage raised to the new owner's) over the OLD owner's bytes, while the peer still holds
  # the same tail at the OLD lineage. The recovery pull's seed was refused as "below the replica's
  # lineage" (a deposed primary), so the retry could neither rank the local copy nor refill it.
  # A pull is this node ASKING for bytes `Recovery.choose/4` already ranked, so only a wire seed is
  # held to the fence over a torn row; an UNTORN higher-lineage row still refuses either way.
  describe "a recovery pull (pull: true)" do
    test "replaces a TORN row whose lineage was raised above the peer's", ctx do
      assert {:ok, _fence} = Follower.fence_for_promotion(ctx.name, ctx.id, lineage: 9)
      assert %{torn: true, lineage: 9} = Follower.state_of(ctx.name, ctx.id)

      # Control: the same begin over the wire path is still refused (a deposed primary).
      assert Follower.begin_seed(ctx.name, %{}, begin(ctx.id, 5)) == %{},
             "fixture: the wire seed must still be refused over a torn higher-lineage row"

      seeds = Follower.begin_seed(ctx.name, %{}, begin(ctx.id, 5), pull: true)

      assert %{failed: false} = seeds[ctx.id],
             "the recovery pull was refused by the lineage fence over a TORN replica"

      _ = Follower.discard_seeds(seeds)
    end

    test "is still refused, typed, over an UNTORN higher-lineage row", ctx do
      id = ctx.id
      assert %{torn: false, lineage: 5} = Follower.state_of(ctx.name, id)

      assert %{^id => %{failed: true, refusal: :stale_lineage}} =
               Follower.begin_seed(ctx.name, %{}, begin(id, 3), pull: true)
    end
  end

  test "an equal or higher lineage, or an unstated one, is still accepted", ctx do
    for lineage <- [5, 6, 0] do
      seeds = Follower.begin_seed(ctx.name, %{}, begin(ctx.id, lineage))
      assert Map.has_key?(seeds, ctx.id), "a lineage-#{lineage} seed was refused"
      _ = Follower.discard_seeds(seeds)
    end
  end
end
