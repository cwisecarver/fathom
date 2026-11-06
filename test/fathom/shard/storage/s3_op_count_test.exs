defmodule Fathom.Shard.Storage.S3OpCountTest do
  @moduledoc """
  Pins the exact S3 request count, by method, for each lifecycle event whose code comments make a
  count claim (expert review 2026-11-04 #9).

  Those claims lived only in comments: the cold open 3 → 2 (`s3.ex` `acquire_lease`), the first
  flush of a coordinator's life paying the lineage HEAD (`s3.ex` `flush/6`), the steal's single
  HEAD (`s3.ex` `steal_head/1`), and the retained reopen's PUT plus one HEAD (`shard.ex`, the
  retained-reopen comment). A future change that adds a request to any of these passed every test.

  Every request goes through the production client (`Storage.S3`, its Finch pool, Req's response
  steps) against the loopback `Fathom.Bench.S3Stub`. Each is counted once by the
  `[:fathom, :s3, :op]` event that `Storage.S3.meter/1` emits per response, so the counts are a
  property of the code path, not of the network. One stub gap is worked around in the fixture: the
  stub omits Content-Length on HEAD and GET, which the steal's touch needs, so `with_content_length/2`
  adds it the way S3 would.

  ## Both liveness modes, on purpose

  A coordinator fixes its mode at open (`acquire_gen` non-nil = heartbeat, nil = legacy), and the
  two modes issue different requests: legacy renews the per-shard lock on each flush, heartbeat
  does not. The coordinator scenarios therefore run for both modes, and each asserts its mode took.
  `config/test.exs` defaults to legacy.

  ## Not covered here

  A replicating coordinator open. Enabling `:replication_enabled` makes a flush ship to followers,
  which needs a quorum a loopback stub cannot give. The replicating count the code gates on
  `Fleet.replicating?/0` is the steal's HEAD, and that is covered at the `Storage.S3` level.

  Where an observed count disagrees with a code comment, the assertion pins what the code does and
  is marked `# FINDING:`. This file does not change `lib/`.
  """
  use ExUnit.Case, async: false

  alias Fathom.Bench.S3Stub
  alias Fathom.Shard
  alias Fathom.Shard.{Connection, Heartbeat, Storage}
  alias Fathom.Shard.Storage.S3
  alias Fathom.Shards

  @bucket "s3opcount"
  @modes [:legacy, :heartbeat]
  # Long enough that the idle timer never fires inside a test: each scenario drains explicitly.
  @idle_ms 600_000
  # The heartbeat renews every ttl/3, so a long TTL keeps its renewals out of a test's window.
  @heartbeat_ttl_ms 300_000

  setup do
    n = System.unique_integer([:positive])
    shard = "s3opcount-#{n}"
    dir = Path.join(System.tmp_dir!(), "fathom_s3opcount_#{n}")
    File.mkdir_p!(dir)

    keys = [
      :shard_storage,
      :shard_data_dir,
      :shard_flush_interval_ms,
      :shard_idle_ms,
      :shard_retain_local_bytes,
      :shard_retain_min_bytes,
      :directory_touch,
      :lazy_migrate,
      :replication_enabled,
      S3
    ]

    prev = for k <- keys, do: {k, Application.fetch_env(:fathom, k)}

    # The stub's table is owned by this Agent, so it outlives the test process and survives until
    # the heartbeat (a child started later, stopped first) has cleared its object.
    stub_owner = start_supervised!({Agent, fn -> S3Stub.start() end})
    {sup, port, table} = Agent.get(stub_owner, & &1)

    Application.put_env(:fathom, S3,
      bucket: @bucket,
      region: "us-east-1",
      endpoint: "http://127.0.0.1:#{port}",
      path_style: true,
      access_key_id: "k",
      secret_access_key: "s"
    )

    Application.put_env(:fathom, :shard_storage, S3)
    Application.put_env(:fathom, :shard_data_dir, dir)
    Application.put_env(:fathom, :shard_flush_interval_ms, 0)
    Application.put_env(:fathom, :shard_idle_ms, @idle_ms)
    Application.put_env(:fathom, :directory_touch, false)
    Application.put_env(:fathom, :lazy_migrate, false)
    Application.put_env(:fathom, :replication_enabled, false)

    test_pid = self()
    handler = {__MODULE__, n}

    :telemetry.attach(
      handler,
      [:fathom, :s3, :op],
      fn _event, _measurements, meta, _config ->
        # Telemetry is global, and the stub is this test's `:shard_storage`, so a lingering
        # coordinator from an EARLIER test also lands here (CI OTP 29, 2026-11-06: a GET of
        # `drainwstx_*.lock` made a cold open count `get: 2`). Drop ops whose emitter (or the
        # process it runs for) is registered to a different shard; keep unattributed ones.
        unless other_shard?(shard),
          do: send(test_pid, {:s3_op, meta.op, meta.status_class})
      end,
      nil
    )

    on_exit(fn ->
      :telemetry.detach(handler)

      # The stub's supervisor is linked to the Agent that owns it, which has stopped by now. A stop
      # that finds it dead must not skip the env restore below.
      try do
        Supervisor.stop(sup)
      catch
        :exit, _ -> :ok
      end

      File.rm_rf(dir)

      for {k, v} <- prev do
        case v do
          {:ok, val} -> Application.put_env(:fathom, k, val)
          :error -> Application.delete_env(:fathom, k)
        end
      end
    end)

    %{shard: shard, table: table}
  end

  # Puts the NEXT coordinator open into `mode`. The mode is captured at open, so this runs before
  # the shard is first opened. The heartbeat is started by the test, as `shard_lease_release_test`
  # does, because `config/test.exs` leaves it off.
  defp set_mode!(:legacy) do
    refute Heartbeat.running?(), "legacy mode needs the heartbeat OFF"
    :ok
  end

  defp set_mode!(:heartbeat) do
    hb = start_supervised!({Heartbeat, ttl_ms: @heartbeat_ttl_ms})
    # Syncs past the heartbeat's init so its first renewal is not counted in a scenario.
    _ = :sys.get_state(hb)
    assert Heartbeat.running?()
    :ok
  end

  # The coordinator must actually have opened in the mode the test names. A scenario that silently
  # ran legacy twice would look like two-mode coverage and be one.
  defp assert_mode!(coordinator, :heartbeat) do
    assert is_integer(:sys.get_state(coordinator).acquire_gen),
           "expected heartbeat mode (non-nil acquire_gen): the mode setup did not take"
  end

  defp assert_mode!(coordinator, :legacy) do
    assert is_nil(:sys.get_state(coordinator).acquire_gen),
           "expected legacy mode (nil acquire_gen): something started the heartbeat"
  end

  # Every `:s3_op` event the test process has been sent, in order. Telemetry runs in the emitting
  # process before the request returns, so the events of a call are in the mailbox when it returns.
  defp take_ops(acc \\ []) do
    receive do
      {:s3_op, method, status_class} -> take_ops([{method, status_class} | acc])
    after
      0 -> Enum.reverse(acc)
    end
  end

  # True when the calling process, or one it works for (`$callers`), is a coordinator registered
  # to a shard other than `shard`. Runs inside the telemetry handler, i.e. in the emitter.
  defp other_shard?(shard) do
    [self() | Process.get(:"$callers", [])]
    |> Enum.flat_map(&Registry.keys(Fathom.ShardRegistry, &1))
    |> Enum.any?(&(&1 != shard))
  end

  # `[{method, status_class}]` -> `%{method => count}`, the shape the assertions pin.
  defp counts(ops), do: Enum.frequencies_by(ops, fn {method, _status} -> method end)

  defp clear_ops, do: _ = take_ops()

  # Seeds `shard`'s data object through the production flush path, then removes the local copy so
  # the next checkout is a genuine cold pull.
  defp seed_object(shard) do
    tmp = Path.join(Application.fetch_env!(:fathom, :shard_data_dir), "#{shard}.seed")
    {:ok, conn} = Connection.open(tmp)
    :ok = Connection.exec(conn, "CREATE TABLE t (id INTEGER PRIMARY KEY)")
    :ok = Connection.exec(conn, "INSERT INTO t DEFAULT VALUES")
    :ok = Connection.exec(conn, "PRAGMA wal_checkpoint(TRUNCATE)")
    Connection.close(conn)
    :ok = Storage.flush(shard, tmp)
    Enum.each(["", "-wal", "-shm"], &File.rm(tmp <> &1))
  end

  # One committed write through a checked-out connection, marked dirty the way the bench does
  # (`WriteCounter.bump/1`), so the next flush has bytes to ship. Asserts the precondition.
  defp write_row(shard) do
    {:ok, pid, ref, path} = Shards.checkout(shard)
    {:ok, conn} = Connection.open(path)
    :ok = Connection.exec(conn, "INSERT INTO t DEFAULT VALUES")
    Connection.close(conn)
    Fathom.Shard.WriteCounter.bump(shard)
    assert Shard.dirty?(pid), "fixture: the write did not dirty the shard"
    Shard.checkin(pid, ref)
  end

  # The stub's PUT records only the etag and `x-amz-meta-*`, but real S3 also answers HEAD and GET
  # with Content-Length. The steal's touch refuses an object whose size it cannot read
  # (`touch_unknown_size`), so the fixture gives the stored object its size the way S3 would.
  defp with_content_length(table, path) do
    [{^path, body, headers}] = :ets.lookup(table, path)

    S3Stub.put_object(table, path, body, [
      {"content-length", Integer.to_string(byte_size(body))} | headers
    ])
  end

  # Writes a lock object as a dead owner left it: expired, and with no heartbeat behind it.
  defp seed_dead_lock(table, shard) do
    body = Storage.encode_lease(%{owner: "dead-owner-#{shard}", epoch: 1, expires_at_ms: 0})
    S3Stub.put_object(table, "/#{@bucket}/#{shard}.lock", body, [{"etag", ~s("dead-etag")}])
  end

  for mode <- @modes do
    describe "#{mode} mode" do
      setup do: set_mode!(unquote(mode))

      test "a clean cold open is one lock PUT and one data GET", %{shard: shard} do
        seed_object(shard)
        clear_ops()

        {:ok, pid, ref, _path} = Shards.checkout(shard)
        ops = take_ops()
        assert_mode!(pid, unquote(mode))

        # The comment at s3.ex `acquire_lease` says 3 → 2: the optimistic create (PUT lock,
        # If-None-Match) and the pull (GET db), with no GET of the lock on the common path.
        assert counts(ops) == %{put: 1, get: 1}

        assert Enum.all?(ops, fn {_m, status} -> status == "2xx" end),
               "unexpected: #{inspect(ops)}"

        Shard.checkin(pid, ref)
        :ok = Shards.drain(shard)
      end

      test "a clean idle drop is the lock DELETE and no data PUT", %{shard: shard, table: table} do
        seed_object(shard)
        {:ok, pid, ref, _path} = Shards.checkout(shard)
        assert_mode!(pid, unquote(mode))
        Shard.checkin(pid, ref)
        clear_ops()

        assert :ok = Shards.drain(shard)
        ops = take_ops()

        # The shard was not written, so the drop flush has nothing to PUT. The lease release is
        # the one conditional DELETE of the lock.
        assert counts(ops) == %{delete: 1}
        refute :ets.member(table, "/#{@bucket}/#{shard}.lock"), "the lock survived the drop"
      end

      test "the first periodic flush pays the lineage HEAD; the second does not", %{shard: shard} do
        seed_object(shard)
        write_row(shard)
        clear_ops()

        :ok = Shards.flush(shard)
        first = take_ops()

        write_row(shard)
        clear_ops()

        :ok = Shards.flush(shard)
        second = take_ops()

        # The comment at s3.ex `flush/6` says only the first flush of a coordinator's life pays
        # the HEAD. Legacy mode also renews the per-shard lock before each flush (the fence), which
        # heartbeat mode does not do: that is the one PUT the two modes differ by.
        renew = if unquote(mode) == :legacy, do: 1, else: 0
        assert counts(first) == %{head: 1, put: 1 + renew}
        assert counts(second) == %{put: 1 + renew}

        :ok = Shards.drain(shard)
      end

      test "a retained reopen is a lock PUT and one HEAD, with no data GET", %{
        shard: shard,
        table: table
      } do
        # Retention keeps the clean local copy at drop; the reopen then serves it after a check of
        # the store's etag under the lease. The 1 MiB size floor is pinned to 0 so a tiny fixture
        # is retained.
        Application.put_env(:fathom, :shard_retain_local_bytes, 64 * 1024 * 1024)
        Application.put_env(:fathom, :shard_retain_min_bytes, 0)

        seed_object(shard)
        {:ok, pid, ref, path} = Shards.checkout(shard)
        assert_mode!(pid, unquote(mode))
        Shard.checkin(pid, ref)
        assert :ok = Shards.drain(shard)

        assert File.exists?(path), "fixture: the clean drop did not retain the local copy"
        clear_ops()

        {:ok, pid, ref, _path} = Shards.checkout(shard)
        ops = take_ops()
        Shard.checkin(pid, ref)
        assert :ok = Shards.drain(shard)
        refute :ets.member(table, "/#{@bucket}/#{shard}.lock")

        # shard.ex (the retained-reopen comment) says the open is PUT then the one HEAD. The
        # reopen pulls nothing, so no GET of the data object.
        assert counts(ops) == %{put: 1, head: 1}
      end
    end
  end

  describe "steal of a dead owner's lock" do
    for mode <- @modes do
      test "#{mode} mode: the steal is 4 GETs, 3 PUTs and 1 HEAD", %{shard: shard, table: table} do
        set_mode!(unquote(mode))
        seed_object(shard)
        with_content_length(table, "/#{@bucket}/#{shard}.db")
        seed_dead_lock(table, shard)
        clear_ops()

        {:ok, pid, ref, _path} = Shards.checkout(shard)
        ops = take_ops()
        assert_mode!(pid, unquote(mode))

        # Each request, in order: the create (PUT lock, 412: the dead lock exists), the lock read
        # that finds the dead holder, the pull's GET (overlapped with the acquire), the dead owner's
        # heartbeat read (404), the steal PUT (If-Match on that lock), the touch's HEAD, its GET, and
        # its PUT. The claim reads nothing extra, and the touch HEADs for itself because the
        # replication-off steal reads no HEAD up front.
        assert counts(ops) == %{get: 4, put: 3, head: 1}

        Shard.checkin(pid, ref)
        :ok = Shards.drain(shard)
      end
    end

    test "under replication the steal's single HEAD serves the claim and the touch", %{
      shard: shard,
      table: table
    } do
      seed_object(shard)
      with_content_length(table, "/#{@bucket}/#{shard}.db")
      seed_dead_lock(table, shard)
      Application.put_env(:fathom, :replication_enabled, true)
      clear_ops()

      assert {:ok, lease} = Storage.acquire_lease(shard, "new-owner", 10_000)
      ops = take_ops()
      assert lease.owner == "new-owner"

      # `steal_head/1` reads the data object's HEAD only when replicating, and that one HEAD
      # answers both the claim and the touch (s3.ex, the steal-HEAD comment). The touch then needs
      # only its GET and PUT. Requests: the create (412), the lock read, the heartbeat read (404),
      # the steal HEAD, the steal PUT, the touch GET, the touch PUT.
      assert counts(ops) == %{get: 3, put: 3, head: 1}
    end
  end
end
