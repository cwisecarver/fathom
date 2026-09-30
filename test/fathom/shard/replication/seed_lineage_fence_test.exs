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

  test "an equal or higher lineage, or an unstated one, is still accepted", ctx do
    for lineage <- [5, 6, 0] do
      seeds = Follower.begin_seed(ctx.name, %{}, begin(ctx.id, lineage))
      assert Map.has_key?(seeds, ctx.id), "a lineage-#{lineage} seed was refused"
      _ = Follower.discard_seeds(seeds)
    end
  end
end
