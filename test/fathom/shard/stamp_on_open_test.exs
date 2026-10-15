defmodule Fathom.Shard.StampOnOpenTest do
  @moduledoc """
  A replicating open over an UNSTAMPED object stamps it at once (expert review 2026-10-15 #7, user
  decision Q4 = A).

  Migrate, revert, restore and template-fork write the object out of band and leave it with no
  position stamp (correctly: the old lineage's replicas must not override it). But
  `Promote.fresher?(_, unrankable)` is false for EVERY replica, including the new coordinator's own:
  until its first periodic flush stamps the object, a crash loses the quorum-acked writes it made
  since the open. The coordinator now does one stamping flush on such an open.

  The fixture writes the object with the unfenced 2-arity `Storage.flush/2` — what the migration
  copy uses, and the one write path that leaves no stamp. The periodic interval is pinned at a
  minute, so the only flush that can stamp inside the test is the open's.

  Must NOT fire: with replication off (an unstamped object is the normal state there), or when the
  object is already stamped (one extra PUT per open would be a cost with no purpose). Both liveness
  modes, mode asserted.
  """
  use ExUnit.Case, async: false

  @moduletag :capture_log

  alias Fathom.{Shard, ShardExecutor, Shards}
  alias Fathom.Shard.{Connection, Heartbeat, Storage}
  alias Fathom.Shard.Replication.Recovery
  alias Filo.Stmt

  @modes [:legacy, :heartbeat]
  @env [:replication_enabled, :replication_promote_on_open, :shard_flush_interval_ms]

  setup do
    shard = "stampopen_#{System.unique_integer([:positive])}"
    prev = Map.new(@env, &{&1, Application.get_env(:fathom, &1)})

    Application.put_env(:fathom, :replication_promote_on_open, false)
    Application.put_env(:fathom, :shard_flush_interval_ms, 60_000)

    on_exit(fn ->
      Shards.drain(shard, 2_000)

      for {k, v} <- prev,
          do:
            if(is_nil(v),
              do: Application.delete_env(:fathom, k),
              else: Application.put_env(:fathom, k, v)
            )

      for f <- Path.wildcard(Path.join(Shard.data_dir(), "#{shard}*")), do: File.rm_rf(f)
      Storage.Local.purge_shard(shard)
    end)

    %{shard: shard}
  end

  defp set_mode!(:legacy), do: refute(Heartbeat.running?())

  defp set_mode!(:heartbeat) do
    hb = start_supervised!({Heartbeat, ttl_ms: 30_000})
    _ = :sys.get_state(hb)
    assert Heartbeat.running?()
  end

  defp assert_mode!(pid, :heartbeat), do: assert(is_integer(:sys.get_state(pid).acquire_gen))
  defp assert_mode!(pid, :legacy), do: assert(is_nil(:sys.get_state(pid).acquire_gen))

  defp replicating!(on?), do: Application.put_env(:fathom, :replication_enabled, on?)

  # An object written out of band, the way the migration copy writes it: no stamp.
  defp plant_unstamped_object!(shard) do
    src = Path.join(System.tmp_dir!(), "stampopen_src_#{System.unique_integer([:positive])}.db")
    {:ok, conn} = Connection.open(src)
    {:ok, _} = Connection.query(conn, "CREATE TABLE kv (v TEXT)", [])
    {:ok, _} = Connection.query(conn, "INSERT INTO kv VALUES ('migrated')", [])
    :ok = Connection.close(conn)
    :ok = Storage.flush(shard, src)
    for s <- ["", "-wal", "-shm"], do: File.rm(src <> s)
    assert {:ok, nil} = Storage.object_position(shard), "fixture: the object must be unstamped"
  end

  # Open, and let any flush the open started run to completion.
  defp open_settled!(shard, mode) do
    {:ok, pid} = Shards.ensure(shard)
    assert_mode!(pid, mode)
    settle(pid, 400)
    pid
  end

  defp settle(_pid, 0), do: flunk("the open's flush never settled")

  defp settle(pid, tries) do
    if :sys.get_state(pid).flush_task == nil do
      :ok
    else
      Process.sleep(10)
      settle(pid, tries - 1)
    end
  end

  for mode <- @modes do
    describe "#{mode} mode" do
      @tag mode: mode
      test "a replicating open over an unstamped object stamps it with this lineage",
           %{shard: shard, mode: mode} do
        set_mode!(mode)
        replicating!(true)
        plant_unstamped_object!(shard)

        pid = open_settled!(shard, mode)
        lineage = :sys.get_state(pid).lineage
        assert is_integer(lineage), "fixture: a replicating open must state a lineage"

        assert {:ok, stamp} = Storage.object_position(shard)

        assert Recovery.rankable_stamp?(stamp),
               "the open left the object unstamped (#{inspect(stamp)}): this coordinator's own " <>
                 "quorum-acked writes cannot override it until its first periodic flush"

        assert stamp.epoch == lineage
        # Below every frame this coordinator will ship: its first WAL is ordinal 1, past offset 0.
        assert {stamp.wal_ordinal, stamp.offset} == {1, 0}
        refute Shard.dirty?(pid), "the stamping flush must leave the shard clean"

        # The tenant's data is untouched by the restamp.
        {:ok, conn} = ShardExecutor.open(shard)

        assert {:ok, %{rows: [["migrated"]]}} =
                 ShardExecutor.execute(conn, %Stmt{sql: "SELECT v FROM kv", args: []})

        :ok = ShardExecutor.close(conn)
      end

      @tag mode: mode
      test "replication off: an unstamped object is left alone", %{shard: shard, mode: mode} do
        set_mode!(mode)
        replicating!(false)
        plant_unstamped_object!(shard)
        {:ok, etag} = Storage.object_etag(shard)

        pid = open_settled!(shard, mode)

        refute Shard.dirty?(pid)
        assert {:ok, ^etag} = Storage.object_etag(shard), "replication off must not re-PUT"
        assert {:ok, nil} = Storage.object_position(shard)
      end

      @tag mode: mode
      test "an already-stamped object is not re-PUT", %{shard: shard, mode: mode} do
        set_mode!(mode)
        replicating!(true)
        plant_unstamped_object!(shard)

        # The first open stamps it; drain cleanly, then open again.
        first = open_settled!(shard, mode)
        assert {:ok, stamp} = Storage.object_position(shard)
        assert Recovery.rankable_stamp?(stamp)
        ref = Process.monitor(first)
        Shards.drain(shard, 2_000)
        assert_receive {:DOWN, ^ref, :process, ^first, _}, 5_000

        {:ok, etag} = Storage.object_etag(shard)
        pid = open_settled!(shard, mode)

        refute Shard.dirty?(pid)
        assert {:ok, ^etag} = Storage.object_etag(shard), "a stamped object must not be re-PUT"
      end
    end
  end
end
