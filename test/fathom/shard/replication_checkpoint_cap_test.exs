defmodule Fathom.Shard.ReplicationCheckpointCapTest do
  @moduledoc """
  The replication Session's threshold-checkpoint task must not compete with the tenant's streams
  for `:max_checkouts_per_shard` (expert review 2026-10-20 #13).

  The task takes a checkout grant before it opens the shard's files (so the coordinator cannot be
  dropping them). That grant counted against the tenant's stream cap, so a tenant AT its cap got
  `:shard_at_stream_capacity` for the checkpoint: abandoned, retried only every
  `checkpoint_step_bytes`, until at the force cap (4x the threshold) it ran FORCED and re-seeded every
  lagging follower — on exactly the tenant that was already at its busiest. It now takes the
  cap-exempt `Fathom.Shard.checkout_internal/1`.
  """
  use ExUnit.Case, async: false

  alias Fathom.Shard.Connection
  alias Fathom.Shard.Replication.{Fleet, Follower, Session}
  alias Fathom.Shards

  @frame 4096 + 24
  @keys [
    :replication_enabled,
    :replication_followers,
    :replication_quorum,
    :replication_checkpoint_bytes,
    :replication_checkpoint_step_bytes,
    :max_checkouts_per_shard
  ]

  setup do
    id = "repl_ckpt_cap_#{System.unique_integer([:positive])}"
    root = Path.join(System.tmp_dir!(), "replckptcap_#{System.unique_integer([:positive])}")
    File.mkdir_p!(root)
    prev = Map.new(@keys, &{&1, Application.get_env(:fathom, &1)})

    Application.put_env(:fathom, :replication_checkpoint_bytes, 40 * @frame)
    Application.put_env(:fathom, :replication_checkpoint_step_bytes, 8 * @frame)

    on_exit(fn ->
      Session.stop(id)

      Enum.each(prev, fn
        {k, nil} -> Application.delete_env(:fathom, k)
        {k, v} -> Application.put_env(:fathom, k, v)
      end)

      File.rm_rf(root)
      for s <- ["", "-wal", "-shm"], do: File.rm(Fathom.Shard.db_path(id) <> s)
    end)

    %{id: id, root: root}
  end

  defp start_followers!(root, n) do
    for i <- 1..n do
      name = :"cc_f#{i}_#{System.unique_integer([:positive])}"
      dir = Path.join(root, to_string(name))
      pid = start_supervised!({Follower, name: name, port: 0, dir: dir}, id: name)
      {:ok, port} = Follower.port(pid)
      {name, port}
    end
  end

  test "a tenant at its stream cap still gets a covered checkpoint at the threshold", %{
    id: id,
    root: root
  } do
    followers = start_followers!(root, 3)
    Application.put_env(:fathom, :replication_enabled, true)
    Application.put_env(:fathom, :replication_quorum, 2)

    Application.put_env(
      :fathom,
      :replication_followers,
      for({_n, port} <- followers, do: {~c"127.0.0.1", port})
    )

    start_supervised!(Fleet)

    # The tenant holds its whole cap: two streams.
    Application.put_env(:fathom, :max_checkouts_per_shard, 2)
    {:ok, coordinator, ref1, path} = Shards.checkout(id)
    {:ok, ^coordinator, ref2, ^path} = Shards.checkout(id)

    on_exit(fn ->
      Fathom.Shard.checkin(coordinator, ref1)
      Fathom.Shard.checkin(coordinator, ref2)
    end)

    assert {:error, :shard_at_stream_capacity} = Fathom.Shard.checkout(coordinator),
           "precondition: the shard is at its stream cap"

    {:ok, conn} = Connection.open(path)
    on_exit(fn -> Connection.close(conn) end)
    wal = path <> "-wal"

    test = self()
    handler = "ckpt-cap-#{System.unique_integer([:positive])}"

    :telemetry.attach(
      handler,
      [:fathom, :replication, :checkpoint],
      fn _e, _m, meta, _ -> send(test, {:checkpoint, meta.verdict}) end,
      nil
    )

    on_exit(fn -> :telemetry.detach(handler) end)

    {:ok, _} = Connection.query(conn, "CREATE TABLE t (a INTEGER, b BLOB)", [])

    # Write past the threshold (40 frames) but well below the force cap (160): only a COVERED
    # checkpoint can run here.
    for i <- 1..30 do
      {:ok, _} = Connection.query(conn, "INSERT INTO t VALUES (#{i}, randomblob(3000))", [])
      assert :ok = Session.commit(id, wal, coordinator)
    end

    assert_receive {:checkpoint, :covered}, 2_000
  end
end
