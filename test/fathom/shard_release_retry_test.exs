defmodule Fathom.ShardReleaseRetryTest do
  @moduledoc """
  The lease release on a drop is retried, and a failure that outlasts the retries is surfaced
  (expert review 2026-11-07 #13).

  ## The defect

  Every drop path ends in ONE conditional `DELETE … If-Match` of the lock (or the conditional PUT
  of the released marker when the drop keeps its lineage claim or its local copy). `Req` retries
  only GET/HEAD, so a reset connection, a timeout or a 5xx on that single request returned
  `{:error, _}` — which the release helpers returned to a caller that ignored it. The coordinator
  exited, its local copy was gone, and the lock still named this node's LIVE incarnation: its fresh
  heartbeat means `owner_live?` never lets a peer steal it, so a rebalancer handoff's target is
  refused (`{:shard_held, source}`) and the migrator's `hold_lease` snoozes, with nothing logged.

  ## What is pinned

  Runs against the real S3 backend over `S3EtagStore` — `Storage.Local` deletes unconditionally and
  cannot fail a DELETE the way a store does. Each scenario injects a lost reply (`:closed`) on the
  release request:

    * the plain DELETE after a dirty drop's flush, and after a clean drop;
    * the released-marker PUT after a drop whose data PUT failed (keeps the local copy, leaves a
      dirty marker);
    * a failure that never clears: the drain answers `{:error, {:release_failed, _}}` instead of
      `:ok`, the failure is logged at error level and a `[:fathom, :shard, :release, :failed]`
      event is emitted.

  A retry is safe only because each one re-resolves against the lock's CURRENT state (a lost-reply
  DELETE retries into a 404, a changed lock 412s into `resolve_412_release`), so it can never
  remove a newer owner's lock — `S3StealTouchRollbackTest` pins why deleting unconditionally is
  not an option.

  Both liveness modes, mode asserted. Not async: global storage config.
  """
  use ExUnit.Case, async: false

  import ExUnit.CaptureLog

  alias Fathom.{Shard, ShardExecutor, Shards}
  alias Fathom.Shard.{Heartbeat, Storage}
  alias Fathom.Shard.Storage.S3
  alias Fathom.Test.{Env, S3EtagStore}
  alias Filo.Stmt

  @modes [:legacy, :heartbeat]

  setup do
    shard = "relretry_#{System.unique_integer([:positive])}"

    Env.put!(:shard_idle_ms, 600_000)
    Env.put!(:shard_flush_interval_ms, 600_000)

    store = start_supervised!({Agent, fn -> S3EtagStore.initial(%{}) end})

    Env.put!(S3,
      bucket: "b",
      region: "us-east-1",
      access_key_id: "k",
      secret_access_key: "s",
      endpoint: "https://s3.example",
      path_style: true,
      req_plug: fn conn -> S3EtagStore.serve(conn, store) end
    )

    Env.put!(:shard_storage, S3)

    on_exit(fn ->
      capture_log(fn -> Shards.drain(shard, 2_000) end)
      for f <- Path.wildcard(Path.join(Shard.data_dir(), "#{shard}.db*")), do: File.rm_rf(f)
    end)

    %{shard: shard, store: store}
  end

  defp stmt(sql), do: %Stmt{sql: sql, args: []}

  defp set_mode!(:legacy), do: refute(Heartbeat.running?(), "legacy mode needs the heartbeat OFF")

  defp set_mode!(:heartbeat) do
    # Started AFTER the store: its terminate clears its object through the store.
    hb = start_supervised!({Heartbeat, ttl_ms: 30_000})
    _ = :sys.get_state(hb)
    assert Heartbeat.running?()
  end

  defp assert_mode!(pid, :heartbeat), do: assert(is_integer(:sys.get_state(pid).acquire_gen))
  defp assert_mode!(pid, :legacy), do: assert(is_nil(:sys.get_state(pid).acquire_gen))

  defp lock_key(shard), do: shard <> ".lock"

  # A shard written through and CLOSED, so the next drain is the idle-drop shape.
  defp open_written!(shard, mode) do
    {:ok, pid} = Shards.ensure(shard)
    assert_mode!(pid, mode)
    {:ok, conn} = ShardExecutor.open(shard)
    {:ok, _} = ShardExecutor.execute(conn, stmt("CREATE TABLE kv (v TEXT)"))
    {:ok, _} = ShardExecutor.execute(conn, stmt("INSERT INTO kv VALUES ('x')"))
    :ok = ShardExecutor.close(conn)
    pid
  end

  defp assert_lock_ours!(shard, store) do
    assert {:held, owner} = Storage.lease_holder(shard), "fixture: the lock must be held"
    assert owner == Heartbeat.owner()
    assert is_binary(S3EtagStore.etag_of(store, lock_key(shard)))
  end

  for mode <- @modes do
    describe "#{mode} mode" do
      setup do
        set_mode!(unquote(mode))
        :ok
      end

      test "a lost reply on the release DELETE after a dirty drop is retried; the lock is freed",
           %{shard: shard, store: store} do
        pid = open_written!(shard, unquote(mode))
        assert Shard.dirty?(pid), "fixture: this is the flush-then-drop path"
        assert_lock_ours!(shard, store)

        fault =
          S3EtagStore.inject(
            store,
            %{method: "DELETE", key: lock_key(shard), cond: :if_match},
            {:transport_error, :closed}
          )

        capture_log(fn -> assert Shards.drain(shard, 5_000) == :ok end)

        assert S3EtagStore.fired(store, fault) == 1, "fixture: the DELETE fault never fired"

        assert Storage.lease_holder(shard) == :free,
               "the single lost DELETE reply stranded the lock behind a live heartbeat " <>
                 "(expert review 2026-11-07 #13)"

        assert S3EtagStore.etag_of(store, lock_key(shard)) == nil
      end

      test "a lost reply on the release DELETE after a CLEAN drop is retried",
           %{shard: shard, store: store} do
        pid = open_written!(shard, unquote(mode))
        :ok = Shards.flush(shard)
        refute Shard.dirty?(pid), "fixture: this is the drop_clean path"
        assert_lock_ours!(shard, store)

        fault =
          S3EtagStore.inject(
            store,
            %{method: "DELETE", key: lock_key(shard), cond: :if_match},
            {:transport_error, :closed}
          )

        capture_log(fn -> assert Shards.drain(shard, 5_000) == :ok end)

        assert S3EtagStore.fired(store, fault) == 1
        assert Storage.lease_holder(shard) == :free
        assert S3EtagStore.etag_of(store, lock_key(shard)) == nil
      end

      test "a lost reply on the released-marker PUT (drop flush failed, local copy kept) is retried",
           %{shard: shard, store: store} do
        pid = open_written!(shard, unquote(mode))
        assert Shard.dirty?(pid)
        assert_lock_ours!(shard, store)

        # The drop's data PUT fails (a 5xx: ambiguous, so the release is the fenced MARKER form),
        # and then the marker PUT's reply is lost. `after:` matches only once the data fault fired,
        # which skips the legacy fence's renew PUT that precedes the data PUT.
        data_fault =
          S3EtagStore.inject(store, %{method: "PUT", key: shard <> ".db"}, {:status, 503})

        marker_fault =
          S3EtagStore.inject(
            store,
            %{method: "PUT", key: lock_key(shard), cond: :if_match, after: data_fault},
            {:transport_error, :closed}
          )

        capture_log(fn ->
          assert Shards.drain(shard, 5_000) == {:error, :unflushed_local_copy}
        end)

        assert S3EtagStore.fired(store, data_fault) == 1, "fixture: the data PUT never failed"

        assert S3EtagStore.fired(store, marker_fault) == 1,
               "fixture: the marker fault never fired"

        assert Storage.lease_holder(shard) == :free,
               "the lost marker reply left the lock naming this live node"

        assert Storage.released_dirty?(shard) == true,
               "the retried release must still carry the dirty marker (kept local copy)"
      end

      test "a release that never succeeds is surfaced: the drain fails, it is logged and counted",
           %{shard: shard, store: store} do
        open_written!(shard, unquote(mode))
        assert_lock_ours!(shard, store)

        ref = make_ref()
        handler = "relretry-#{inspect(ref)}"
        test_pid = self()

        :ok =
          :telemetry.attach(
            handler,
            [:fathom, :shard, :release, :failed],
            fn _event, measurements, meta, _ ->
              if meta.shard_id == shard, do: send(test_pid, {ref, measurements, meta})
            end,
            nil
          )

        on_exit(fn -> :telemetry.detach(handler) end)

        S3EtagStore.inject(
          store,
          %{method: "DELETE", key: lock_key(shard)},
          {:transport_error, :closed},
          times: :infinity
        )

        log =
          capture_log([level: :error], fn ->
            assert {:error, {:release_failed, _reason}} = Shards.drain(shard, 10_000)
          end)

        assert_receive {^ref, %{count: 1}, %{reason: _, kind: :plain}}, 2_000
        assert log =~ "lease release FAILED", "the failure was silent:\n#{log}"

        assert {:held, _} = Storage.lease_holder(shard),
               "fixture: the lock must still be held (the DELETE never got through)"

        S3EtagStore.clear_faults(store)
      end
    end
  end
end
