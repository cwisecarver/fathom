defmodule Fathom.Shard.DurabilityEdgesTest do
  @moduledoc """
  Three durability edges from the 2026-08-01 review — each one a place where the system
  concluded something safe on evidence that did not support it.

    * **#39** — a warm shard seeded CLEAN when the `WriteCounter` table was momentarily absent,
      so `drop_clean/1` deleted un-flushed acked writes.
    * **#44** — `Local.flush/3` returned the etag of the SOURCE file, not the object it wrote.
    * **#40** — the heartbeat memo's TTL was hardcoded against an operator-tunable steal margin.
  """
  use ExUnit.Case, async: false

  alias Fathom.Shard.Storage.Local
  alias Fathom.Shard.WriteCounter

  # Expert review 2026-08-26 #4: a long-lived WRITING stream silently got 10x the advertised RPO.
  #
  # Review 2026-08-01 #42 made a clean-but-served shard poll at 10x the interval, relying on
  # ShardExecutor casting :became_dirty to restore the fast cadence. That cast is guarded by
  # signal_dirty_once/2, keyed on the CONNECTION in the stream's process dictionary — so it fires
  # at most ONCE per stream. But a shard re-enters the slow cadence every time a flush leaves it
  # clean, and the second write on the same stream sends nothing to lift it again.
  #
  # No existing test wrote twice across a flush boundary on ONE connection, which is why this
  # survived: under CONN_MAX_AGE=0 every request is a fresh stream and the flag resets, and that is
  # the configuration the suite and the chaos rig use.
  describe "the durability cadence under a long-lived writing stream (#4)" do
    setup do
      prev = %{
        interval: Application.get_env(:fathom, :shard_flush_interval_ms),
        idle: Application.get_env(:fathom, :shard_idle_ms)
      }

      # Short interval so a flush boundary is crossable in a test; high idle so the coordinator
      # does not stop underneath us.
      Application.put_env(:fathom, :shard_flush_interval_ms, 100)
      Application.put_env(:fathom, :shard_idle_ms, 60_000)

      id = "rpo10x_#{System.unique_integer([:positive])}"

      on_exit(fn ->
        for {k, v} <- [shard_flush_interval_ms: prev.interval, shard_idle_ms: prev.idle] do
          if is_nil(v),
            do: Application.delete_env(:fathom, k),
            else: Application.put_env(:fathom, k, v)
        end

        Fathom.Shards.drain(id, 2_000)

        for dir <- [Fathom.Shard.data_dir(), Local.dir()],
            suffix <- [".db", ".db-wal", ".db-shm", ".db.etag", ".lock"],
            do: File.rm(Path.join(dir, id <> suffix))
      end)

      %{id: id}
    end

    test "a second write on the SAME stream does not leave a 10x timer armed", %{id: id} do
      {:ok, conn} = Fathom.ShardExecutor.open(id)
      on_exit(fn -> Fathom.ShardExecutor.close(conn) end)

      {:ok, _} =
        Fathom.ShardExecutor.execute(conn, %Filo.Stmt{sql: "CREATE TABLE t (v TEXT)"})

      {:ok, pid} = Fathom.Shards.ensure(id)

      # Let the flush land so the shard goes CLEAN while the stream is still held. This is the
      # moment the slow (10x) timer used to be armed.
      assert wait_for(fn -> not Fathom.Shard.dirty?(pid) end),
             "precondition: the first write never flushed, so the slow cadence is never reached"

      # THE SECOND WRITE, on the same connection. signal_dirty_once/2 has already fired for this
      # stream, so no :became_dirty cast is sent — pre-fix nothing lifts the slow timer.
      {:ok, _} = Fathom.ShardExecutor.execute(conn, %Filo.Stmt{sql: "INSERT INTO t VALUES ('b')"})

      st = :sys.get_state(pid)

      refute st.flush_timer_slow?,
             "a stream that has written is on the 10x clean-poll timer, so its acked writes wait " <>
               "up to @clean_poll_multiplier intervals instead of one — a silent 10x violation of " <>
               "the RPO bound docs/durability.md advertises"

      # And it really does flush again promptly, not 10 intervals later.
      assert wait_for(fn -> not Fathom.Shard.dirty?(pid) end, 3_000),
             "the second write never flushed within a reasonable multiple of the interval"
    end

    # The optimisation #42 added must survive: a held stream doing only READS still gets the slow
    # cadence. Without this the fix would be "delete the optimisation", which is not the fix.
    test "a read-only held stream still takes the slow cadence", %{id: id} do
      {:ok, conn} = Fathom.ShardExecutor.open(id)
      on_exit(fn -> Fathom.ShardExecutor.close(conn) end)

      {:ok, _} = Fathom.ShardExecutor.execute(conn, %Filo.Stmt{sql: "SELECT 1"})
      {:ok, pid} = Fathom.Shards.ensure(id)

      assert wait_for(fn -> :sys.get_state(pid).flush_timer_slow? end),
             "a read-only held stream should poll at the slow cadence — the optimisation from " <>
               "review 2026-08-01 #42 has been lost"
    end
  end

  defp wait_for(fun, remaining_ms \\ 2_000)
  defp wait_for(_fun, remaining_ms) when remaining_ms <= 0, do: false

  defp wait_for(fun, remaining_ms) do
    if fun.() do
      true
    else
      Process.sleep(10)
      wait_for(fun, remaining_ms - 10)
    end
  end

  describe "a closed connection leaves the main .db complete" do
    # `Migrator.Copy.migrate_chain/3` does a plain `File.cp` of the main file — no `-wal` — so it
    # depends on `Connection.close/1` having folded the WAL in. SQLite skips the close-time
    # checkpoint while statements are still open, so ANY unreleased prepare in `Connection` breaks
    # a module that never touches `Connection` directly.
    #
    # That is not hypothetical: reading `PRAGMA page_size` for #21 prepared a statement and did not
    # release it, and CI came back with three `copy_test` failures — all
    # `{:error, "no such table: app_thing"}`, i.e. the copy was missing a table the source had
    # committed. Green on the parent commit, red on all three OTP versions after.
    #
    # THIS TEST DOES NOT REPRODUCE THAT LOCALLY. It passed against the unfixed code here, and so
    # did the full suite, twice — the statement's resource NIF is evidently collected before close
    # often enough on this machine. It is kept because it pins the CROSS-MODULE invariant in the
    # place that owns it, and because CI is a real oracle for it even when this box is not.
    test "a plain File.cp of the main file carries committed rows" do
      src = Path.join(System.tmp_dir!(), "walfold_#{System.unique_integer([:positive])}.db")
      cp = src <> ".copy"

      on_exit(fn ->
        for s <- ["", "-wal", "-shm"], do: File.rm(src <> s)
        for s <- ["", "-wal", "-shm"], do: File.rm(cp <> s)
      end)

      {:ok, c} = Fathom.Shard.Connection.open(src)
      :ok = Fathom.Shard.Connection.exec(c, "CREATE TABLE app_thing (id INTEGER, name TEXT)")
      :ok = Fathom.Shard.Connection.exec(c, "INSERT INTO app_thing VALUES (1, 'alice')")
      Fathom.Shard.Connection.close(c)

      File.cp!(src, cp)
      {:ok, c2} = Fathom.Shard.Connection.open(cp)
      result = Fathom.Shard.Connection.query(c2, "SELECT name FROM app_thing", [])
      Fathom.Shard.Connection.close(c2)

      assert {:ok, %{rows: [["alice"]]}} = result,
             "close/1 did not fold the WAL into the main file — a copy of it is incomplete, " <>
               "which is what Migrator.Copy silently receives"
    end
  end

  describe "#39 — the dirtiness seed fails DIRTY when the counter table is gone" do
    test "bump_strict/1 raises where bump/1 rescues" do
      # bump/1's rescue is correct for a per-write bump ("the next write re-dirties") and WRONG
      # for the seed, where there is no next write. The two must therefore behave differently
      # with the table absent — if a refactor ever made bump_strict rescue too, #39 comes back
      # silently, which is exactly how it survived the first time.
      absent = "no_such_shard_#{System.unique_integer([:positive])}"

      with_no_counter_table(fn ->
        assert :ok = WriteCounter.bump(absent), "bump/1 must keep rescuing"

        assert_raise ArgumentError, fn -> WriteCounter.bump_strict(absent) end
      end)
    end

    test "a WARM open with no counter table reads DIRTY, not clean" do
      # A warm shard may hold un-flushed local writes, so it must never come up clean — clean
      # means drop_clean/1 deletes the local .db/-wal/-shm without uploading.
      #
      # The first version of this test staged a COLD open and wrote through Connection directly,
      # so `warm?` was false and no write ever reached the counter. It asserted nothing, and
      # failed for the right reason. A warm open needs the local file to exist BEFORE init.
      id = "dirty_seed_#{System.unique_integer([:positive])}"
      on_exit(fn -> cleanup(id) end)

      File.mkdir_p!(Fathom.Shard.data_dir())
      path = Path.join(Fathom.Shard.data_dir(), "#{id}.db")
      {:ok, conn} = Fathom.Shard.Connection.open(path)
      {:ok, _} = Fathom.Shard.Connection.query(conn, "CREATE TABLE kv (v TEXT)", [])
      Fathom.Shard.Connection.close(conn)
      # A file built straight into the live dir has no provenance, and cold-open refuses to serve
      # an unprovenanced copy (#2). Declare it, exactly as the bench harness does.
      :ok = Fathom.Shard.stamp_local_provenance(id)

      pid =
        with_no_counter_table(fn ->
          {:ok, pid, ref, _path} = Fathom.Shards.checkout(id)
          Fathom.Shard.checkin(pid, ref)
          pid
        end)

      assert Fathom.Shard.dirty?(pid),
             "a warm shard seeded while the counter table was absent came up CLEAN — idle or " <>
               "shutdown would drop_clean/1 its un-flushed writes"
    end
  end

  # THESE TWO DO NOT DISCRIMINATE, and that is stated rather than implied.
  #
  # `file_etag/1` over the source and over the destination are the same sha256, so with a
  # quiescent source they agree and both assertions pass against the unfixed code. Review #30
  # predicted this in as many words: "`Local.flush/3`'s source-vs-destination etag divergence
  # cannot be observed where the two are the same file." Observing it needs the source to change
  # BETWEEN `atomic_copy` and the hash — a mid-flush seam neither live backend has, and which a
  # test double would have to invent.
  #
  # They are kept as invariant guards on the CONTRACT ("the returned etag describes the stored
  # object, and fences the next flush"), which is what a future refactor would break. The fix
  # itself rests on the argument in `Local.flush/3`'s comment, not on a failing test.
  describe "#44 — Local.flush/3 returns the STORED object's etag" do
    setup do
      dir = Path.join(System.tmp_dir!(), "flushetag_#{System.unique_integer([:positive])}")
      prev = Application.get_env(:fathom, Local)
      File.mkdir_p!(dir)
      Application.put_env(:fathom, Local, dir: dir)

      on_exit(fn ->
        if prev,
          do: Application.put_env(:fathom, Local, prev),
          else: Application.delete_env(:fathom, Local)

        File.rm_rf(dir)
      end)

      :ok
    end

    test "the returned etag fences the NEXT flush, so it must describe what is stored" do
      # The coordinator If-Matches its next flush with this value. If it describes anything other
      # than the stored object, that flush 412s and the shard self-fences away acked writes.
      id = "etag_#{System.unique_integer([:positive])}"
      src = tmp_file("v1-bytes")

      assert {:ok, etag} = Local.flush(id, src, nil)
      assert {:ok, ^etag} = Local.object_etag(id), "the fence etag does not describe the object"

      # And it is usable as a fence: the next flush with it must succeed.
      assert {:ok, _} = Local.flush(id, tmp_file("v2-bytes"), etag)
    end

    test "a source mutated after the copy does not change the returned etag" do
      # THE discriminating case. Hashing the source after the copy describes a file that can have
      # moved since; hashing the destination cannot. Writing to the source after flush returns
      # must leave the fence etag still describing the stored bytes.
      id = "etag_mut_#{System.unique_integer([:positive])}"
      src = tmp_file("original")

      assert {:ok, etag} = Local.flush(id, src, nil)
      File.write!(src, "mutated-after-the-copy")

      assert {:ok, ^etag} = Local.object_etag(id)
      assert {:ok, _} = Local.flush(id, tmp_file("next"), etag), "the fence etag went stale"
    end
  end

  describe "#40 — the heartbeat memo TTL tracks the steal margin" do
    setup do
      prev = Application.get_env(:fathom, :steal_margin_ms)

      on_exit(fn ->
        if prev,
          do: Application.put_env(:fathom, :steal_margin_ms, prev),
          else: Application.delete_env(:fathom, :steal_margin_ms)
      end)

      :ok
    end

    test "turning the margin down shrinks the memo window with it" do
      # The safety condition ("the TTL must stay well inside steal_margin_ms") was documented and
      # enforced nowhere, against a knob an operator can set to 500ms. Deriving it makes the
      # condition structural — which is the assertion here: the relationship, not a constant.
      for margin <- [5_000, 2_500, 500, 100] do
        Application.put_env(:fathom, :steal_margin_ms, margin)
        ttl = ttl_ms()

        assert ttl < margin,
               "memo TTL #{ttl}ms is not inside a #{margin}ms steal margin — a cached verdict " <>
                 "can outlive the window the margin exists to cover"
      end
    end

    test "the default is unchanged, so this is not a behaviour change at default config" do
      Application.put_env(:fathom, :steal_margin_ms, 5_000)
      assert ttl_ms() == 1_000
    end

    test "a zero/absent margin disables the memo rather than caching forever" do
      Application.put_env(:fathom, :steal_margin_ms, 0)
      assert ttl_ms() == 0
    end
  end

  # The derivation lives in the S3 backend beside the margin it depends on; reach it the way the
  # backend does rather than duplicating the formula here (which would pass no matter what).
  defp ttl_ms, do: Fathom.Shard.Storage.S3.hb_cache_ttl_ms()

  defp tmp_file(body) do
    path = Path.join(System.tmp_dir!(), "src_#{System.unique_integer([:positive])}.db")
    File.write!(path, body)
    on_exit(fn -> File.rm(path) end)
    path
  end

  defp with_no_counter_table(fun) do
    # The real window is WriteCounter.init/1 bumping the generation before creating the table.
    # Rather than race it, assert against a table that genuinely is not there.
    table = Fathom.Shard.WriteCounter
    existed = :ets.info(table) != :undefined
    if existed, do: :ets.rename(table, :"#{table}_parked")

    try do
      fun.()
    after
      if existed, do: :ets.rename(:"#{table}_parked", table)
    end
  end

  defp cleanup(id) do
    Fathom.Shards.drain(id, 5_000)

    for s <- ["", "-wal", "-shm", ".etag", ".lock"] do
      File.rm(Path.join(Fathom.Shard.data_dir(), "#{id}.db#{s}"))
    end
  end
end
