defmodule Fathom.Shard.PreTouchEtagFlushTest do
  @moduledoc """
  A byte-identical flush must not recreate the steal-touch's PRE etag (expert review 2026-10-13
  #17).

  ## The defect

  A steal rotates the data object E0 -> E1 so a zombie's late `If-Match: E0` PUT 412s. The flush PUT
  carries no nonce and `VACUUM INTO` plus zstd is deterministic, so a stealer Y whose content is
  net-unchanged (a write then its undo) flushes bytes whose etag is E0 AGAIN, which is exactly the
  etag the zombie fences on: the zombie's late PUT then succeeds, and until Y's next 412 a release
  or pull serves the zombie's bytes under Y's lineage.

  ## What is pinned

  After the steal-touch, a flush of content identical to the pre-touch object lands on an etag that
  is neither E0 nor E1, and the coordinator adopts it (so its next flush fences on the right etag).
  Both liveness modes, mode asserted. Pre-fix the flush DOES come back as E0 (the fixture is a
  write and its undo, which VACUUM INTO plus zstd reproduce byte for byte).
  """
  use ExUnit.Case, async: false

  import ExUnit.CaptureLog

  alias Fathom.{Shard, ShardExecutor, Shards}
  alias Fathom.Shard.{Heartbeat, Storage}
  alias Fathom.Shard.Storage.S3
  alias Fathom.Test.S3EtagStore
  alias Filo.Stmt

  @modes [:legacy, :heartbeat]

  setup do
    shard = "pretouch_#{System.unique_integer([:positive])}"

    for k <- [:shard_storage, S3, :shard_idle_ms] do
      prev = Application.get_env(:fathom, k)

      on_exit(fn ->
        if is_nil(prev),
          do: Application.delete_env(:fathom, k),
          else: Application.put_env(:fathom, k, prev)
      end)
    end

    Application.put_env(:fathom, :shard_idle_ms, 600_000)

    on_exit(fn ->
      Shards.drain(shard, 2_000)
      for f <- Path.wildcard(Path.join(Shard.data_dir(), "#{shard}*")), do: File.rm_rf(f)
    end)

    %{shard: shard}
  end

  defp use_s3!(store) do
    Application.put_env(:fathom, S3,
      bucket: "b",
      region: "us-east-1",
      access_key_id: "k",
      secret_access_key: "s",
      endpoint: "https://s3.example",
      path_style: true,
      req_plug: fn conn -> S3EtagStore.serve(conn, store) end
    )

    Application.put_env(:fathom, :shard_storage, S3)
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

  defp stmt(sql), do: %Stmt{sql: sql, args: []}

  # X creates the shard, flushes it (object E0), then dies without releasing; a dead FOREIGN lock
  # is what the successor steals (and touches).
  defp x_flushes_then_dies!(shard, store) do
    {:ok, x} = Shards.ensure(shard)
    {:ok, conn} = ShardExecutor.open(shard)
    {:ok, _} = ShardExecutor.execute(conn, stmt("CREATE TABLE kv (v TEXT)"))
    {:ok, _} = ShardExecutor.execute(conn, stmt("INSERT INTO kv VALUES ('base')"))
    :ok = ShardExecutor.close(conn)
    :ok = Shards.flush(shard)
    e0 = S3EtagStore.etag_of(store, "#{shard}.db")
    assert is_binary(e0)

    ref = Process.monitor(x)
    Process.exit(x, :kill)
    assert_receive {:DOWN, ^ref, :process, ^x, _}, 5_000

    lock =
      Jason.encode!(%{
        "owner" => "dead@elsewhere#x",
        "epoch" => 3,
        "expires_at_ms" => System.system_time(:millisecond) - 600_000,
        "claim" => 4
      })

    Agent.update(
      store,
      &put_in(&1, [:objects, "#{shard}.lock"], %{body: lock, form: :single, meta: %{}})
    )

    e0
  end

  for mode <- @modes do
    describe "#{mode} mode" do
      setup do: set_mode!(unquote(mode))

      test "a net-unchanged flush after the steal-touch does not land back on the pre-touch etag",
           %{shard: shard} do
        store = start_supervised!({Agent, fn -> S3EtagStore.initial(%{}) end})
        use_s3!(store)

        capture_log(fn ->
          e0 = x_flushes_then_dies!(shard, store)

          {:ok, y} = Shards.ensure(shard)
          assert_mode!(y, unquote(mode))
          assert :sys.get_state(y).lease[:touch_pre_etag] == e0, "Y's steal must touch from E0"
          e1 = S3EtagStore.etag_of(store, "#{shard}.db")
          assert e1 != e0

          # A write and its undo: dirty, but the logical content equals the pre-touch object.
          {:ok, conn} = ShardExecutor.open(shard)
          {:ok, _} = ShardExecutor.execute(conn, stmt("INSERT INTO kv VALUES ('tmp')"))
          {:ok, _} = ShardExecutor.execute(conn, stmt("DELETE FROM kv WHERE v = 'tmp'"))
          :ok = ShardExecutor.close(conn)
          assert Shard.dirty?(y)

          :ok = Shards.flush(shard)

          final = S3EtagStore.etag_of(store, "#{shard}.db")

          refute final == e0,
                 "the flush recreated the pre-touch etag E0: a zombie's late If-Match PUT would succeed"

          assert :sys.get_state(y).etag == final,
                 "the coordinator must adopt the re-touched etag as its next flush's fence"

          Shards.drain(shard, 5_000)
        end)

        assert Storage.retouch(shard, "not-the-current-etag") == {:error, :moved},
               "a re-touch must only ever rotate the etag it was asked about"
      end
    end
  end
end
