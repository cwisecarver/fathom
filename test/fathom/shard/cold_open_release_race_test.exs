defmodule Fathom.Shard.ColdOpenReleaseRaceTest do
  @moduledoc """
  A cold, NON-takeover open must not serve the bytes its speculative pull read when the object moved
  before the lease was ours (expert review 2026-10-17 #1).

  ## The race

  A cold open starts the object GET (`Materializer.start_pull/2`) BEFORE `acquire_lease`, to overlap
  the two round trips. `revalidate_takeover/6` re-checks the pulled etag only for a TAKEOVER, on the
  premise "a fresh create has no prior owner". That premise is false when a peer held the shard while
  we were opening, and its final flush + clean release (which DELETES the lock) landed between our GET
  and our lock create:

    * **(a) the S3 re-create.** Our first `create_lock` 412s (the peer holds it); the peer flushes and
      releases; `acquire_existing` reads `:not_found` and re-creates at epoch 1 — no `took_over`.
    * **(b) a first-try create.** Our GET is served before the peer's final PUT, our create after its
      DELETE. Nothing in the acquire distinguishes it from an uncontended create.

  Either way the open served the PRE-flush object (stale reads), fenced its first flush on the stale
  etag, and that flush 412'd into the self-fence: the new owner's first-cycle writes quarantined.

  ## What is closed, and what is not

    * Replicating (the prod default): the open already HEADs the object under the lease
      (`start_open_head/2`), so the pulled etag is compared with it at no extra round trip. Covers
      (a) AND (b).
    * Not replicating: the S3 backend marks the `:not_found` re-create lease `recreated: true`, and
      only that lease pays one `object_etag` read. Covers (a). Shape (b) with replication OFF is NOT
      covered — it would cost a HEAD on every cold open; see the last test, which pins the residual
      so nobody reads this file as a full fix.

  `FaultyStorage` delegates to `Storage.Local`, whose acquire has no re-create branch, so shape (a) is
  staged by having the double set the same `recreated` flag the S3 backend sets in that interleaving
  (`:faulty_acquire_recreated`); `S3RecreateFlagTest` pins that S3 sets it. The peer's flush + release
  lands in `:faulty_after_pull` → `:faulty_before {:acquire, _}`: after our GET returned, before our
  create. Both liveness modes, mode asserted.
  """
  use ExUnit.Case, async: false

  import ExUnit.CaptureLog

  alias Fathom.{Shard, ShardExecutor, Shards}
  alias Fathom.Shard.{Connection, Heartbeat, Provenance}
  alias Fathom.Shard.Storage.Local
  alias Filo.Stmt

  @modes [:legacy, :heartbeat]

  @env [
    :shard_storage,
    :faulty_before,
    :faulty_after_pull,
    :faulty_acquire_recreated,
    :replication_enabled,
    :replication_promote_on_open,
    :replication_recover_from_peers,
    :shard_idle_ms,
    :shard_flush_interval_ms
  ]

  setup do
    shard = "coldrace_#{System.unique_integer([:positive])}"
    prev = Map.new(@env, &{&1, Application.get_env(:fathom, &1)})

    on_exit(fn ->
      Application.delete_env(:fathom, :faulty_before)
      Application.delete_env(:fathom, :faulty_after_pull)
      Shards.drain(shard, 2_000)

      for {k, v} <- prev do
        if is_nil(v),
          do: Application.delete_env(:fathom, k),
          else: Application.put_env(:fathom, k, v)
      end

      for f <- Path.wildcard(Path.join(Shard.data_dir(), "#{shard}*")), do: File.rm_rf(f)
      Local.purge_shard(shard)
    end)

    Application.put_env(:fathom, :shard_storage, Fathom.Test.FaultyStorage)
    Application.put_env(:fathom, :shard_idle_ms, 600_000)
    Application.put_env(:fathom, :shard_flush_interval_ms, 600_000)
    Application.put_env(:fathom, :replication_promote_on_open, true)
    Application.put_env(:fathom, :replication_recover_from_peers, false)

    %{shard: shard}
  end

  defp set_mode!(:legacy), do: refute(Heartbeat.running?(), "legacy mode needs the heartbeat OFF")

  defp set_mode!(:heartbeat) do
    hb = start_supervised!({Heartbeat, ttl_ms: 30_000})
    _ = :sys.get_state(hb)
    assert Heartbeat.running?()
  end

  defp set_replication!(on?), do: Application.put_env(:fathom, :replication_enabled, on?)

  defp stmt(sql), do: %Stmt{sql: sql, args: []}

  # Write a one-row database holding `v` straight into the "remote" (no lease: the peer's flush).
  defp put_object!(shard, v) do
    tmp = Path.join(System.tmp_dir!(), "coldrace_seed_#{System.unique_integer([:positive])}.db")
    {:ok, c} = Connection.open(tmp)
    :ok = Connection.exec(c, "CREATE TABLE kv (v TEXT)")
    :ok = Connection.exec(c, "INSERT INTO kv VALUES ('#{v}')")
    :ok = Connection.exec(c, "PRAGMA wal_checkpoint(TRUNCATE)")
    Connection.close(c)
    :ok = Local.flush(shard, tmp)
    for s <- ["", "-wal", "-shm"], do: File.rm(tmp <> s)
    {:ok, etag} = Local.object_etag(shard)
    etag
  end

  # After our GET returned and before our lock create: the peer's final flush (object -> 'e1') and
  # its clean release (the lock it held is gone — there is none left in the store either way).
  defp stage_release_in_gap!(shard) do
    test_pid = self()

    Application.put_env(:fathom, :faulty_after_pull, fn
      ^shard, {:ok, etag} -> send(test_pid, {:pulled, etag})
      _, _ -> :ok
    end)

    Application.put_env(
      :fathom,
      :faulty_before,
      {:acquire,
       fn
         ^shard ->
           # Our GET must already have been answered: that is the race.
           receive do
             {:go, from} -> send(from, :went)
           after
             10_000 -> flunk("the test never released the acquire")
           end

         _ ->
           :ok
       end}
    )
  end

  # Start the open (`Shards.ensure/1` returns once `init/1` is done; the open itself runs in
  # `handle_continue`), let its GET be answered, land the peer's flush, then let its acquire go.
  defp open_racing!(shard, e0) do
    stage_release_in_gap!(shard)
    {:ok, pid} = Shards.ensure(shard)

    assert_receive {:pulled, ^e0}, 5_000
    e1 = put_object!(shard, "e1")
    refute e1 == e0

    send(pid, {:go, self()})
    assert_receive :went, 5_000
    {e1, pid}
  end

  defp served(shard) do
    {:ok, conn} = ShardExecutor.open(shard)
    {:ok, %Filo.StmtResult{rows: rows}} = ShardExecutor.execute(conn, stmt("SELECT v FROM kv"))
    ShardExecutor.close(conn)
    List.flatten(rows)
  end

  defp assert_mode!(pid, :heartbeat),
    do: assert(is_integer(:sys.get_state(pid).acquire_gen), "heartbeat mode did not take")

  defp assert_mode!(pid, :legacy),
    do: assert(is_nil(:sys.get_state(pid).acquire_gen), "legacy mode did not take")

  # A replicating open of an unstamped object takes its own stamping flush right after the open
  # (`stamp_on_open/3`), so the fence etag is checked against the CURRENT object after a flush, not
  # against `e1` itself. Pre-fix that stamping flush If-Matched the stale etag and self-fenced.
  defp assert_serves_new_object!(shard, pid, _e1) do
    {rows, _log} = with_log(fn -> served(shard) end)

    assert rows == ["e1"],
           "the open served the object its pre-lease pull read, not the one the peer flushed"

    capture_log(fn -> Shards.flush(shard) end)
    {:ok, current} = Local.object_etag(shard)
    assert :sys.get_state(pid).etag == current, "the open fences its flushes on a stale etag"
    assert Provenance.read(Shard.db_path(shard)) == {:ok, current}
    assert Path.wildcard(Shard.db_path(shard) <> ".forked*") == [], "a self-fence quarantined"
    assert Path.wildcard(Shard.db_path(shard) <> ".fenced*") == [], "a self-fence quarantined"
  end

  for mode <- @modes do
    describe "#{mode} mode" do
      setup do
        set_mode!(unquote(mode))
        :ok
      end

      test "replicating, first-try create (shape b): the shared head catches the moved object",
           %{shard: shard} do
        set_replication!(true)
        e0 = put_object!(shard, "e0")
        {e1, pid} = open_racing!(shard, e0)
        assert_mode!(pid, unquote(mode))

        refute Map.get(:sys.get_state(pid).lease, :took_over),
               "fixture: this must not be a takeover"

        assert_serves_new_object!(shard, pid, e1)
      end

      for repl <- [false, true] do
        test "re-created lock (shape a), replication #{repl}: the pulled etag is re-checked",
             %{shard: shard} do
          set_replication!(unquote(repl))
          Application.put_env(:fathom, :faulty_acquire_recreated, true)
          e0 = put_object!(shard, "e0")
          {e1, pid} = open_racing!(shard, e0)
          assert_mode!(pid, unquote(mode))
          assert :sys.get_state(pid).lease[:recreated] == true, "fixture: the flag did not ride"
          assert_serves_new_object!(shard, pid, e1)
        end
      end

      test "an uncontended non-replicating cold open pays no extra object read", %{shard: shard} do
        set_replication!(false)
        e0 = put_object!(shard, "e0")
        test_pid = self()

        Application.put_env(
          :fathom,
          :faulty_before,
          {:object_etag, fn s -> if s == shard, do: send(test_pid, :object_etag_read) end}
        )

        assert served(shard) == ["e0"]
        {:ok, pid} = Shards.ensure(shard)
        assert :sys.get_state(pid).etag == e0
        refute_received :object_etag_read, "the common fresh-create cold open grew a round trip"
      end
    end
  end

  # THE RESIDUAL, pinned so it is not mistaken for covered (see the moduledoc). Shape (b) with
  # replication OFF: nothing in the acquire marks the lease, and closing it would cost a HEAD on every
  # cold open. Parked in the review's progress file (expert review 2026-10-17 #1). If this test starts
  # failing because the open now serves 'e1', the residual was closed: flip it, don't delete it.
  test "residual: non-replicating first-try create still serves the pre-lease pull", %{
    shard: shard
  } do
    set_replication!(false)
    e0 = put_object!(shard, "e0")
    {_e1, pid} = open_racing!(shard, e0)
    assert :sys.get_state(pid).etag == e0
  end
end
