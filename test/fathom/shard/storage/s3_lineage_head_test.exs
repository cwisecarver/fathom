defmodule Fathom.Shard.Storage.S3LineageHeadTest do
  @moduledoc """
  The lineage HEAD before every PUT — expert review 2026-08-26 #33.

  A PUT replaces ALL of an S3 object's user metadata, so a flush that has no lineage of its own has
  to read back the one it is about to overwrite. With replication OFF — the default —
  `Fathom.Shard`'s `lineage_arg/1` supplies none, so `carried_lineage_header/3` issued an
  `object_head` before EVERY fenced PUT of an existing shard. On a fleet that never enabled
  replication the object carries no lineage at all, so that HEAD returned nothing, forever.

  The cost is not bytes — the old note in `s3.ex` argued that, and it is the wrong axis. It is a
  REQUEST and a serialized RTT inserted after `recheck_before_put/1` and before the PUT, partially
  undoing review 2026-08-01 #28, whose whole purpose was shrinking that window. It also consumes a
  slot in the shared S3 Finch pool `FlushGate`'s cap was sized against.

  The fix is a cache, not a removal: `flush/5` reports what the object now carries, the coordinator
  remembers it, and hands it back as `{:carried, _}`. Counted rather than timed — one avoided round
  trip against an in-process plug is not a measurable duration, and a request count is the thing
  the finding actually predicts.
  """
  use ExUnit.Case, async: false

  alias Fathom.Shard.Storage.S3

  defp counting_store(test_pid) do
    fn conn ->
      send(test_pid, {:s3, conn.method, conn.request_path})

      case conn.method do
        "HEAD" ->
          conn |> Plug.Conn.put_resp_header("etag", ~s("e1")) |> Plug.Conn.send_resp(200, "")

        "PUT" ->
          conn |> Plug.Conn.put_resp_header("etag", ~s("e2")) |> Plug.Conn.send_resp(200, "")

        _ ->
          Plug.Conn.send_resp(conn, 200, "")
      end
    end
  end

  setup do
    prev = Application.get_env(:fathom, S3)
    test_pid = self()

    Application.put_env(:fathom, S3,
      bucket: "b",
      region: "us-east-1",
      access_key_id: "k",
      secret_access_key: "s",
      endpoint: "https://s3.example",
      path_style: true,
      req_plug: counting_store(test_pid)
    )

    on_exit(fn ->
      if prev,
        do: Application.put_env(:fathom, S3, prev),
        else: Application.delete_env(:fathom, S3)
    end)

    path = Path.join(System.tmp_dir!(), "lineagehead_#{System.unique_integer([:positive])}.db")
    File.write!(path, "not-really-sqlite-but-the-bytes-do-not-matter-here")
    on_exit(fn -> File.rm(path) end)

    %{path: path, id: "lh_#{System.unique_integer([:positive])}"}
  end

  defp methods do
    receive do
      {:s3, method, _} -> [method | methods()]
    after
      0 -> []
    end
  end

  # The behaviour the finding names, pinned so it cannot come back unnoticed: nothing else in the
  # repo would have caught it, because it is a request count on a path that succeeds either way.
  test "overwriting an existing object with no lineage of our own HEADs first", ctx do
    assert {:ok, _etag, :none} = S3.flush(ctx.id, ctx.path, "e0", nil, nil)

    assert "HEAD" in methods(),
           "the read that #33 is about is gone from the un-cached path, so the test below cannot " <>
             "be measuring the cache"
  end

  # THE FIX. The coordinator hands back what the previous flush reported, and the read disappears.
  test "a carried lineage skips the HEAD entirely", ctx do
    assert {:ok, _etag, :none} = S3.flush(ctx.id, ctx.path, "e0", nil, {:carried, :none})

    refute "HEAD" in methods(),
           "a flush told what the object carries still read it back — the round trip #33 removes " <>
             "is still being paid on every flush"
  end

  test "a carried INTEGER is written as the lineage header and reported back", ctx do
    assert {:ok, _etag, 42} = S3.flush(ctx.id, ctx.path, "e0", nil, {:carried, 42})
    refute "HEAD" in methods()
  end

  # A brand-new object has no metadata to inherit, so cold open never paid this and must not start.
  test "a brand-new object (nil etag) HEADs nothing and reports :none", ctx do
    assert {:ok, _etag, :none} = S3.flush(ctx.id, ctx.path, nil, nil, nil)
    refute "HEAD" in methods()
  end

  # An explicit lineage — replication ON — never read anything before this change and still must
  # not. This is the clause that keeps the common replicating case at one request.
  test "an explicit lineage still HEADs nothing and reports itself", ctx do
    assert {:ok, _etag, 7} = S3.flush(ctx.id, ctx.path, "e0", nil, 7)
    refute "HEAD" in methods()
  end

  # END TO END, through a real coordinator, which is where the win actually lands. The unit tests
  # above prove the backend honours `{:carried, _}`; this proves `Fathom.Shard` supplies it, which
  # is the half that could be silently missing — a coordinator that never caches still passes every
  # test above.
  #
  # Driven against the `S3EtagStore` double with `:shard_storage` set to S3, the same shape
  # `warm_takeover_lineage_test` uses. It has to be S3: `Storage.Local` keeps the lineage in its
  # own sidecar, pays nothing to leave it alone, and reports `:unknown` — so on the default test
  # backend this code path is invisible.
  describe "through the coordinator" do
    setup do
      shard = "lhcoord_#{System.unique_integer([:positive])}"
      prev_storage = Application.get_env(:fathom, :shard_storage)
      prev_idle = Application.get_env(:fathom, :shard_idle_ms)
      Application.put_env(:fathom, :shard_idle_ms, 60_000)

      on_exit(fn ->
        if prev_storage,
          do: Application.put_env(:fathom, :shard_storage, prev_storage),
          else: Application.delete_env(:fathom, :shard_storage)

        if prev_idle,
          do: Application.put_env(:fathom, :shard_idle_ms, prev_idle),
          else: Application.delete_env(:fathom, :shard_idle_ms)

        Fathom.Shards.stop(shard)
        for f <- Path.wildcard(Path.join(Fathom.Shard.data_dir(), "#{shard}*")), do: File.rm_rf(f)
      end)

      %{shard: shard}
    end

    test "only the FIRST flush of a coordinator's life reads the lineage back", %{shard: shard} do
      store = start_supervised!({Agent, fn -> Fathom.Test.S3EtagStore.initial(%{}) end})
      heads = start_supervised!({Agent, fn -> 0 end}, id: :lh_heads)

      plug = fn conn ->
        if conn.method == "HEAD" and String.ends_with?(conn.request_path, "#{shard}.db"),
          do: Agent.update(heads, &(&1 + 1))

        Fathom.Test.S3EtagStore.serve(conn, store)
      end

      Application.put_env(:fathom, S3,
        bucket: "b",
        region: "us-east-1",
        access_key_id: "k",
        secret_access_key: "s",
        endpoint: "https://s3.example",
        path_style: true,
        req_plug: plug
      )

      Application.put_env(:fathom, :shard_storage, S3)

      {:ok, conn} = Fathom.ShardExecutor.open(shard)
      on_exit(fn -> Fathom.ShardExecutor.close(conn) end)
      {:ok, coordinator} = Fathom.Shards.ensure(shard)

      {:ok, _} = Fathom.ShardExecutor.execute(conn, stmt("CREATE TABLE t (a INTEGER)"))
      flush!(coordinator)

      # The very first flush creates the object (no expected etag), so it HEADs nothing at all —
      # a brand-new object has no metadata to inherit. That is also why this counts the SECOND and
      # THIRD flushes rather than the first: the interesting case is overwriting.
      before_overwrites = Agent.get(heads, & &1)

      {:ok, _} = Fathom.ShardExecutor.execute(conn, stmt("INSERT INTO t VALUES (1)"))
      flush!(coordinator)
      after_second = Agent.get(heads, & &1)

      {:ok, _} = Fathom.ShardExecutor.execute(conn, stmt("INSERT INTO t VALUES (2)"))
      flush!(coordinator)
      after_third = Agent.get(heads, & &1)

      assert :sys.get_state(coordinator).carried_lineage == :none,
             "the coordinator cached nothing, so every flush will keep paying the read"

      assert after_third == after_second and after_second == before_overwrites,
             "an overwriting flush still read the object's lineage back: " <>
               "#{before_overwrites} -> #{after_second} -> #{after_third} data-key HEADs. " <>
               "That is the per-flush round trip #33 removes."
    end
  end

  defp stmt(sql), do: %Filo.Stmt{sql: sql, args: []}

  defp flush!(coordinator) do
    send(coordinator, :durability_flush)
    settle(coordinator, 400)
  end

  defp settle(_coordinator, 0), do: flunk("durability flush task never settled")

  defp settle(coordinator, tries) do
    if :sys.get_state(coordinator).flush_task == nil do
      :ok
    else
      Process.sleep(10)
      settle(coordinator, tries - 1)
    end
  end
end
