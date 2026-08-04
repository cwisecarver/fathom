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
