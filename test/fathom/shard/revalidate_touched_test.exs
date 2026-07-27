defmodule Fathom.Shard.RevalidateTouchedTest do
  @moduledoc """
  Expert review #30: the takeover-revalidation lattice — `revalidate_takeover` →
  `revalidate_touched` / `post_lease_warm_check` (`lib/fathom/shard.ex:426-581`) — is
  the branchiest, most safety-critical decision tree in the tree, and its S3 steal-touch
  branches had zero *dedicated* test coverage. They only rotate a real etag against real
  S3 (the Local backend's content-hash etags can never rotate without the bytes changing),
  so before this they were validated only by the boot-time probe and chaos runs.

  These tests drive a REAL coordinator (`Fathom.ShardExecutor.open/1`) over the S3 backend
  against `Fathom.Test.S3EtagStore` — an MD5-faithful S3 double where a plain self-copy of
  identical bytes does NOT rotate the etag but the alternating-form steal touch does, exactly
  like production. `warm?` is set by pre-placing the live `.db` file (`shard.ex:292` gates warm
  on `File.exists?/1`); a dead lock in the store forces the acquire to STEAL (`took_over: true`
  + a touch that threads `touch_pre_etag`/`touch_post_etag`); an out-of-band
  `S3EtagStore.set_body/3` between two reads models a zombie / release-in-gap flush.

  Branch map (● covered here, ○ covered by a sibling test):

    revalidate_touched (takeover, touch_post_etag set — shard.ex:454)
      ● A  etag == post          speculative pull captured the post-touch object
      ● B1 warm, sidecar == pre  warm-adopt on provenance match (no re-pull)
      ● B2 warm, sidecar != pre  zombie-flush-diverged → quarantine + re-pull
      ○ C  cold, etag == pre     #15 optimization — steal_touch_warm_standby_test.exs
      ● D  cold, etag != pre     raced a zombie flush → re-pull the current lineage

    post_lease_warm_check (non-takeover warm — shard.ex:552)
      ● P1 store etag == sidecar unchanged, the common warm restart
      ● P2 store 404            object gone → serve warm, fence nil, drop sidecar
      ● P3 store etag != sidecar release-in-gap fork → quarantine + re-pull
      ● P4 store HEAD errors     unreachable → keep availability, fence by provenance

  The legacy (Local, no-touch) takeover re-pull is covered by
  `takeover_revalidation_test.exs`; the cold `etag == pre` #15 adoption by
  `steal_touch_warm_standby_test.exs`. The two `:quarantine_failed` sub-branches (a failed
  rename in B2 / P3) are not forced here: a deterministic `File.rename` failure is
  platform-dependent and flaky, and the failed-rename rule is already asserted by
  `quarantined_fork?`'s callers. Each test pins its branch with distinctive, observable
  evidence — full-body GET count (re-pull vs not), which lineage the coordinator SERVES,
  the provenance sidecar's contents, and whether a `.forked.*` quarantine copy appears — so
  a test can't pass by reaching the wrong branch. Not async: shards + storage config are global.
  """
  use ExUnit.Case, async: false

  import ExUnit.CaptureLog

  alias Fathom.{ShardExecutor, Shards}
  alias Fathom.Shard.{Connection, Storage}
  alias Fathom.Shard.Storage.S3
  alias Fathom.Test.S3EtagStore
  alias Filo.{Stmt, StmtResult}

  @local_dir Path.join(System.tmp_dir!(), "fathom_shards")

  setup do
    shard = "reval_touched_#{System.unique_integer([:positive])}"
    prev_storage = Application.get_env(:fathom, :shard_storage)
    prev_s3 = Application.get_env(:fathom, S3)
    prev_flush = Application.get_env(:fathom, :shard_flush_interval_ms)
    prev_idle = Application.get_env(:fathom, :shard_idle_ms)

    # Disable the periodic durability flush so it can't re-upload (rotating the store's
    # etag) mid-test, under the branch assertions.
    Application.put_env(:fathom, :shard_flush_interval_ms, 0)
    # Keep the coordinator alive (and the local copy + sidecar on disk) through the branch
    # assertions after the stream closes; the test drains explicitly at the end. Without this
    # a short idle timer could idle-flush-and-drop the file before we read the sidecar.
    Application.put_env(:fathom, :shard_idle_ms, 300_000)

    on_exit(fn ->
      restore(:shard_storage, prev_storage)
      restore(S3, prev_s3)
      restore(:shard_flush_interval_ms, prev_flush)
      restore(:shard_idle_ms, prev_idle)

      for f <- Path.wildcard(Path.join(@local_dir, "#{shard}*")), do: File.rm_rf(f)
    end)

    %{shard: shard}
  end

  defp restore(key, nil), do: Application.delete_env(:fathom, key)
  defp restore(key, val), do: Application.put_env(:fathom, key, val)

  defp stmt(sql), do: %Stmt{sql: sql, args: []}

  # A valid, checkpointed single-row db — the bytes a shard's storage object holds. The
  # row value tags the lineage so the served SELECT tells which copy the coordinator chose.
  defp build_db!(value) do
    path = Path.join(System.tmp_dir!(), "rvt_#{System.unique_integer([:positive])}.db")
    {:ok, c} = Connection.open(path)
    :ok = Connection.exec(c, "CREATE TABLE kv (v TEXT)")
    :ok = Connection.exec(c, "INSERT INTO kv VALUES ('#{value}')")
    :ok = Connection.exec(c, "PRAGMA wal_checkpoint(TRUNCATE)")
    Connection.close(c)
    bytes = File.read!(path)
    for s <- ["", "-wal", "-shm"], do: File.rm(path <> s)
    bytes
  end

  # An expired lock owned by a crashed node with no heartbeat: acquire STEALS (epoch+1)
  # and touches the data object, threading touch_pre_etag/touch_post_etag through the lease.
  defp dead_lock do
    Storage.encode_lease(%{
      owner: "dead@node#old",
      epoch: 5,
      expires_at_ms: Storage.now_ms() - Storage.steal_margin_ms() - 60_000
    })
  end

  # Seed this node's own live copy (makes warm? true) with a provenance sidecar.
  defp place_warm(shard, bytes, sidecar_etag) do
    path = local_path(shard)
    File.mkdir_p!(Path.dirname(path))
    File.write!(path, bytes)
    File.write!(path <> ".etag", sidecar_etag)
    path
  end

  defp local_path(shard), do: Path.join(@local_dir, "#{shard}.db")
  defp sidecar_path(shard), do: local_path(shard) <> ".etag"
  defp forked_files(shard), do: Path.wildcard(local_path(shard) <> ".forked.*")

  defp start_counters do
    %{
      body_gets: start_supervised!({Agent, fn -> 0 end}, id: :body_gets),
      data_heads: start_supervised!({Agent, fn -> 0 end}, id: :data_heads)
    }
  end

  defp body_gets(cnt), do: Agent.get(cnt.body_gets, & &1)

  # Build the req_plug: count full-body GETs and data-object HEADs, and let a test hook
  # the Nth data HEAD (to inject a zombie / release-in-gap flush, or force an error) and
  # the pull GET (to gate ordering). Everything else is the plain MD5-faithful store.
  defp build_plug(store, data_key, cnt, hooks) do
    on_head = Keyword.get(hooks, :on_head, fn _n, _conn, _store -> :cont end)
    on_get = Keyword.get(hooks, :on_get, fn _conn, _store -> :cont end)

    fn conn ->
      data? = String.ends_with?(conn.request_path, data_key)

      cond do
        conn.method == "HEAD" and data? ->
          n = Agent.get_and_update(cnt.data_heads, &{&1 + 1, &1 + 1})

          case on_head.(n, conn, store) do
            {:resp, resp} -> resp
            :cont -> S3EtagStore.serve(conn, store)
          end

        conn.method == "GET" and data? ->
          resp =
            case on_get.(conn, store) do
              {:resp, r} -> r
              :cont -> S3EtagStore.serve(conn, store)
            end

          Agent.update(cnt.body_gets, &(&1 + 1))
          resp

        true ->
          S3EtagStore.serve(conn, store)
      end
    end
  end

  defp configure_s3(plug) do
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
  end

  # Bounded poll (never sleeps forever if the awaited state never arrives): signals the
  # test process on timeout so a broken ordering surfaces as a failed refute, not a hang.
  defp wait_until(test_pid, fun, tries \\ 500) do
    cond do
      fun.() -> :ok
      tries <= 0 -> send(test_pid, :ordering_never_reached)
      true -> Process.sleep(5) && wait_until(test_pid, fun, tries - 1)
    end
  end

  defp multipart?(nil), do: false
  defp multipart?(etag), do: String.contains?(etag, "-")

  # Open, read the single kv value (which lineage the takeover served), close the stream.
  # The coordinator stays up (idle is 300s in setup) so the branch assertions can read the
  # local copy + provenance sidecar; the test drains afterwards.
  defp open_read(shard) do
    {:ok, conn} = ShardExecutor.open(shard)

    {:ok, %StmtResult{rows: [[value]]}} =
      ShardExecutor.execute(conn, stmt("SELECT v FROM kv"))

    :ok = ShardExecutor.close(conn)
    value
  end

  # Graceful drain (flush + drop + stop) after the assertions — drops the local copy, so it
  # must run LAST.
  defp drain(shard) do
    case Shards.ensure(shard) do
      {:ok, coordinator} ->
        ref = Process.monitor(coordinator)
        _ = Shards.drain(shard)

        receive do
          {:DOWN, ^ref, :process, ^coordinator, _} -> :ok
        after
          5_000 -> :ok
        end

      _ ->
        :ok
    end
  end

  # [:fathom, :shard, :forked] fires exactly when a copy is quarantined — a positive
  # signal the quarantine branch (not merely a re-pull) was taken.
  defp attach_forked_telemetry(shard) do
    test = self()
    id = "rvt-forked-#{shard}-#{System.unique_integer([:positive])}"

    :telemetry.attach(
      id,
      [:fathom, :shard, :forked],
      fn _e, _m, meta, _ ->
        if meta.shard_id == shard, do: send(test, {:forked, meta.shard_id})
      end,
      nil
    )

    on_exit(fn -> :telemetry.detach(id) end)
  end

  defp start_store(objects) do
    start_supervised!({Agent, fn -> S3EtagStore.initial(objects) end})
  end

  # ── revalidate_touched (takeover) ───────────────────────────────────────────

  test "A: a cold takeover whose speculative pull captured the POST-touch object adopts it (no re-pull)",
       %{shard: shard} do
    # No local file ⇒ cold; a dead lock ⇒ steal + touch. The pull's GET is held until the
    # touch has rotated the object to multipart form, so the pull deterministically captures
    # the POST-touch object (etag == post → the fast path at shard.ex:460).
    base = build_db!("base-write")
    data_key = "#{shard}.db"
    store = start_store(%{data_key => base, "#{shard}.lock" => dead_lock()})
    pre = S3EtagStore.etag_of(store, data_key)
    refute pre =~ "-", "seed must be single (MD5) form so the touch rotates it"

    test_pid = self()
    cnt = start_counters()

    plug =
      build_plug(store, data_key, cnt,
        on_get: fn _conn, s ->
          wait_until(test_pid, fn -> multipart?(S3EtagStore.etag_of(s, data_key)) end)
          :cont
        end
      )

    configure_s3(plug)

    capture_log(fn ->
      value = open_read(shard)

      refute_received :ordering_never_reached,
                      "the touch must complete (object → multipart) before the pull GET serves"

      post = S3EtagStore.etag_of(store, data_key)
      assert post =~ "-1", "the steal touch must rotate the etag to multipart form"
      assert value == "base-write", "the takeover serves the (byte-identical) post-touch object"

      assert body_gets(cnt) == 1,
             "etag == post is the fast path — exactly the one speculative pull, no re-pull"

      assert File.read!(sidecar_path(shard)) == post,
             "the pulled (post-touch) etag is the provenance sidecar"

      drain(shard)
    end)
  end

  test "B1: a warm takeover whose provenance matches the touch source ADOPTS the post-touch etag with no re-pull",
       %{shard: shard} do
    # This node's own warm copy IS the current lineage (sidecar == the store's pre-touch
    # etag). The steal touches it (moves no bytes), so the warm branch adopts the post-touch
    # etag straight off the copy already held (shard.ex:475-477) — the common warm failover.
    base = build_db!("base-write")
    data_key = "#{shard}.db"
    store = start_store(%{data_key => base, "#{shard}.lock" => dead_lock()})
    pre = S3EtagStore.etag_of(store, data_key)

    place_warm(shard, base, pre)
    attach_forked_telemetry(shard)
    cnt = start_counters()
    configure_s3(build_plug(store, data_key, cnt, []))

    capture_log(fn ->
      value = open_read(shard)
      post = S3EtagStore.etag_of(store, data_key)
      assert post =~ "-1"
      assert post != pre

      assert value == "base-write", "the warm copy already held is served"
      assert body_gets(cnt) == 0, "provenance match adopts in place — NO full-body pull at all"
      assert forked_files(shard) == [], "a provenance MATCH must not quarantine"
      refute_received {:forked, _}

      assert File.read!(sidecar_path(shard)) == post,
             "the adopted post-touch etag is persisted, so a later warm restart fences with the store's real etag"

      drain(shard)
    end)
  end

  test "B2: a warm takeover racing a zombie flush (provenance != touch source) QUARANTINES the local copy and re-pulls",
       %{shard: shard} do
    # warm? holds (the fork check sees store == sidecar), THEN a zombie flush lands the dead
    # owner's acknowledged writes into the object before the steal touch. The touch's SOURCE
    # is now the zombie's etag, not this file's provenance — so adopting would let the next
    # flush clobber the zombie's durable writes. The warm branch (shard.ex:479-487) quarantines
    # the diverged local copy and re-pulls the (zombie) lineage instead.
    base = build_db!("base-write")
    zombie = build_db!("zombie-write")
    data_key = "#{shard}.db"
    store = start_store(%{data_key => base, "#{shard}.lock" => dead_lock()})
    base_etag = S3EtagStore.etag_of(store, data_key)

    place_warm(shard, base, base_etag)
    attach_forked_telemetry(shard)
    cnt = start_counters()

    # HEAD #1 = the pre-acquire fork check (sees base == sidecar ⇒ warm). Inject the zombie
    # flush just before HEAD #2 = the steal's touch head_object, so the touch sources the zombie.
    plug =
      build_plug(store, data_key, cnt,
        on_head: fn
          2, _conn, s ->
            S3EtagStore.set_body(s, data_key, zombie)
            :cont

          _n, _conn, _s ->
            :cont
        end
      )

    configure_s3(plug)

    capture_log(fn ->
      value = open_read(shard)

      assert value == "zombie-write",
             "the zombie's acknowledged writes are the surviving lineage — re-pulled, not the local copy"

      # One re-pull when the fork-check HEAD wins the race against the steal-touch, TWO when it
      # loses — and the second is correct, not waste to be asserted away. `fork_evidence/2` runs
      # concurrently with `acquire_lease` (review 2026-07-23 #22), so on a loss the evidence is
      # already the zombie's etag: resolve_fork/4 still quarantines (`pre` is the zombie, our
      # sidecar is `base`, so the provenance genuinely diverged), but the open proceeds COLD, and a
      # cold takeover's speculative pull can hold bytes from before the touch — which
      # revalidate_takeover is designed to detect and re-pull (shard.ex "a cold pull that
      # mismatches is re-pulled").
      #
      # So the count is a property of who won a race, and pinning it to 1 pinned the race. What is
      # actually guaranteed — and is asserted here and below — is that the surviving lineage is
      # served, exactly one copy is quarantined, and its bytes are preserved. Verified under load
      # 2026-07-26: body_gets=2, value="zombie-write", forked=1.
      assert body_gets(cnt) in 1..2,
             "a diverged provenance must re-pull (1 if the fork check beat the steal-touch, 2 if " <>
               "it lost and the cold path re-pulled pre-touch bytes) — got #{body_gets(cnt)}"

      assert_received {:forked, _}, "the diverged local copy must be quarantined, not discarded"

      [forked] = forked_files(shard)

      assert File.read!(forked) == base,
             "the quarantine preserves the local copy's bytes for operator recovery"

      drain(shard)
    end)
  end

  test "D: a cold takeover whose pull raced a zombie flush (etag != pre) re-pulls the current lineage",
       %{shard: shard} do
    # Cold: the speculative pull captures `base`. Then a zombie flush moves the object to
    # `zombie` BEFORE the steal touch, so the touch's pre = zombie's etag ≠ the pulled etag.
    # etag is neither post nor pre ⇒ the diverged-cold branch (shard.ex:506-509) re-pulls.
    base = build_db!("base-write")
    zombie = build_db!("zombie-write")
    data_key = "#{shard}.db"
    store = start_store(%{data_key => base, "#{shard}.lock" => dead_lock()})

    test_pid = self()
    cnt = start_counters()

    # HEAD #1 = the steal's touch head_object: hold it until the pull GET has served `base`
    # (body_gets >= 1), THEN land the zombie flush so the touch sources the zombie. The pull
    # (ungated) races ahead and captures base; the touch's pre becomes the zombie's etag.
    plug =
      build_plug(store, data_key, cnt,
        on_head: fn
          1, _conn, s ->
            wait_until(test_pid, fn -> body_gets(cnt) >= 1 end)
            S3EtagStore.set_body(s, data_key, zombie)
            :cont

          _n, _conn, _s ->
            :cont
        end
      )

    configure_s3(plug)

    capture_log(fn ->
      value = open_read(shard)

      refute_received :ordering_never_reached,
                      "the pull GET must serve base before the touch sources the zombie"

      assert value == "zombie-write",
             "the diverged-cold branch re-pulls the current (zombie) lineage"

      assert body_gets(cnt) == 2, "one speculative pull (base) + one re-pull (zombie)"

      drain(shard)
    end)
  end

  # ── post_lease_warm_check (non-takeover warm) ────────────────────────────────

  test "P1: a warm non-takeover open whose store object is unchanged serves the local copy (no re-pull)",
       %{shard: shard} do
    # No lock ⇒ a fresh epoch-1 create, no took_over ⇒ post_lease_warm_check. The store's
    # object equals the local provenance (the common warm restart) — serve local (shard.ex:555).
    base = build_db!("base-write")
    data_key = "#{shard}.db"
    store = start_store(%{data_key => base})
    base_etag = S3EtagStore.etag_of(store, data_key)

    place_warm(shard, base, base_etag)
    attach_forked_telemetry(shard)
    cnt = start_counters()
    configure_s3(build_plug(store, data_key, cnt, []))

    capture_log(fn ->
      value = open_read(shard)
      assert value == "base-write"
      assert body_gets(cnt) == 0, "an unchanged store object is served warm — no pull"
      assert forked_files(shard) == []
      refute_received {:forked, _}
      assert File.read!(sidecar_path(shard)) == base_etag, "provenance is unchanged"

      drain(shard)
    end)
  end

  test "P2: a warm non-takeover open whose store object VANISHED serves the local copy and drops the sidecar",
       %{shard: shard} do
    # The object was deliberately deleted with a live local copy: the un-flushed brand-new
    # stance — serve warm, fence with nil so the first flush RECREATES it, drop the dangling
    # sidecar (shard.ex:561-563).
    local = build_db!("local-only")
    data_key = "#{shard}.db"
    # Store has NO data object (and no lock ⇒ fresh create).
    store = start_store(%{})

    place_warm(shard, local, ~s("deadbeef"))
    attach_forked_telemetry(shard)
    cnt = start_counters()
    configure_s3(build_plug(store, data_key, cnt, []))

    capture_log(fn ->
      value = open_read(shard)
      assert value == "local-only", "the live local copy is served even though the object is gone"
      assert body_gets(cnt) == 0
      assert forked_files(shard) == [], "a vanished object is NOT a fork — no quarantine"
      refute_received {:forked, _}

      refute File.exists?(sidecar_path(shard)),
             "the dangling provenance sidecar is dropped so the recreating flush fences with nil"

      drain(shard)
    end)
  end

  test "P3: a warm non-takeover open whose object moved in the acquire gap QUARANTINES and re-pulls (release-in-gap fork)",
       %{shard: shard} do
    # The fork check passes (store == sidecar ⇒ warm), THEN another node acquires, flushes,
    # and releases in the gap before our epoch-1 create. post_lease_warm_check's second HEAD
    # sees the moved lineage ≠ our provenance ⇒ quarantine + re-pull (shard.ex:569-574).
    base = build_db!("base-write")
    moved = build_db!("moved-write")
    data_key = "#{shard}.db"
    store = start_store(%{data_key => base})
    base_etag = S3EtagStore.etag_of(store, data_key)

    place_warm(shard, base, base_etag)
    attach_forked_telemetry(shard)
    cnt = start_counters()

    # HEAD #1 = the pre-acquire fork check (sees base ⇒ warm). Move the object just before
    # HEAD #2 = post_lease_warm_check (no touch on this path, so #2 is the warm re-check).
    plug =
      build_plug(store, data_key, cnt,
        on_head: fn
          2, _conn, s ->
            S3EtagStore.set_body(s, data_key, moved)
            :cont

          _n, _conn, _s ->
            :cont
        end
      )

    configure_s3(plug)

    capture_log(fn ->
      value = open_read(shard)

      assert value == "moved-write",
             "the lineage that moved past our copy is re-pulled and served"

      assert body_gets(cnt) == 1, "a release-in-gap fork forces exactly one re-pull"
      assert_received {:forked, _}
      [forked] = forked_files(shard)
      assert File.read!(forked) == base, "the superseded local copy is preserved for recovery"

      drain(shard)
    end)
  end

  test "P4: a warm non-takeover open whose post-lease HEAD is UNREACHABLE keeps serving, fenced by provenance",
       %{shard: shard} do
    # The store is unreachable exactly at the post-lease re-check: keep availability and serve
    # warm, fenced by the provenance etag so a genuinely-forked flush 412s rather than clobbers
    # (shard.ex:578-579). A 500 (not a 404) is the unreachable signal.
    base = build_db!("base-write")
    data_key = "#{shard}.db"
    store = start_store(%{data_key => base})
    base_etag = S3EtagStore.etag_of(store, data_key)

    place_warm(shard, base, base_etag)
    attach_forked_telemetry(shard)
    cnt = start_counters()

    # HEAD #1 (fork check) succeeds ⇒ warm; HEAD #2 (post_lease_warm_check) returns 500.
    plug =
      build_plug(store, data_key, cnt,
        on_head: fn
          2, conn, _s -> {:resp, Plug.Conn.send_resp(conn, 500, "")}
          _n, _conn, _s -> :cont
        end
      )

    configure_s3(plug)

    capture_log(fn ->
      value = open_read(shard)
      assert value == "base-write", "an unreachable store keeps the warm copy available"
      assert body_gets(cnt) == 0, "no re-pull on an unreachable re-check"
      assert forked_files(shard) == []
      refute_received {:forked, _}

      assert File.read!(sidecar_path(shard)) == base_etag,
             "provenance is retained so a forked flush self-fences instead of clobbering"

      drain(shard)
    end)
  end
end
