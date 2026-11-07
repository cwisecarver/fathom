defmodule Fathom.Shard.QuarantineOrderTest do
  @moduledoc """
  Expert review 2026-11-07 #83, second and third sites (`Fork.quarantine_copy!` is pinned by
  `fork_quarantine_order_test.exs`). `Integrity.quarantine_corrupt!/3` and the coordinator's
  `rename_fenced!/1` renamed the `.db` FIRST and the `-wal`/`-shm` best-effort afterwards, so a crash
  between the two (or a failed `-wal` rename) left a WAL-less `.db` or an orphan `-wal` at the live
  path, where it replays onto the next database of the same name.

  Invariant, for both: the companions move BEFORE the `.db`, every result is checked, and the move is
  all-or-nothing. Failures are injected at the `Fathom.FS.rename/2` seam (`Fathom.Test.CrashFS`).
  """
  use Fathom.ClusterShardCase, async: false

  alias Fathom.Shard
  alias Fathom.Shard.Integrity
  alias Fathom.Test.CrashFS

  defp fail_rename_where(pred) do
    fn
      {:rename, src, _dst} -> if pred.(src), do: {:error, :eio}, else: :ok
      _ -> :ok
    end
  end

  describe "Integrity.quarantine_corrupt!/3" do
    setup do
      dir = Path.join(System.tmp_dir!(), "corrupt_q_#{System.unique_integer([:positive])}")
      File.mkdir_p!(dir)
      on_exit(fn -> File.rm_rf(dir) end)

      path = Path.join(dir, "tenant.db")
      File.write!(path, "DB")
      File.write!(path <> "-wal", "ACKED-FRAMES")
      File.write!(path <> "-shm", "SHM")
      %{dir: dir, path: path}
    end

    defp corrupt_copies(dir), do: Path.wildcard(Path.join(dir, "tenant.db.corrupt.*"))

    test "the -wal and -shm move before the .db and land beside the quarantined .db",
         %{dir: dir, path: path} do
      crash = CrashFS.start!(dir)

      capture_log(fn -> assert :ok = Integrity.quarantine_corrupt!(path, "tenant", :boom) end)

      renames = for {:rename, src, _} <- CrashFS.trace(crash), do: Path.basename(src)
      assert renames == ["tenant.db-wal", "tenant.db-shm", "tenant.db"]

      assert [dest] = Enum.reject(corrupt_copies(dir), &String.ends_with?(&1, ["-wal", "-shm"]))
      assert File.read!(dest) == "DB"
      assert File.read!(dest <> "-wal") == "ACKED-FRAMES"
      refute File.exists?(path)
      refute File.exists?(path <> "-wal")
    end

    test "a failed -wal rename aborts: nothing moves and the failure is logged",
         %{dir: dir, path: path} do
      _crash = CrashFS.start!(dir, inject: fail_rename_where(&String.ends_with?(&1, "-wal")))

      log =
        capture_log(fn ->
          assert {:error, :eio} = Integrity.quarantine_corrupt!(path, "tenant", :boom)
        end)

      assert log =~ "quarantine FAILED"
      assert File.read!(path) == "DB", "the .db must stay put when its -wal could not move"
      assert File.read!(path <> "-wal") == "ACKED-FRAMES"
      assert corrupt_copies(dir) == []
    end

    test "a failed .db rename rolls the already-moved companions back", %{dir: dir, path: path} do
      _crash =
        CrashFS.start!(dir, inject: fail_rename_where(&(Path.basename(&1) == "tenant.db")))

      capture_log(fn ->
        assert {:error, :eio} = Integrity.quarantine_corrupt!(path, "tenant", :boom)
      end)

      assert File.read!(path) == "DB"
      assert File.read!(path <> "-wal") == "ACKED-FRAMES", "the -wal must be moved back"
      assert File.read!(path <> "-shm") == "SHM"
      assert corrupt_copies(dir) == []
    end
  end

  describe "the coordinator's self-fence quarantine (rename_fenced!/1)" do
    # `shard_durability_test`'s "a self-fence quarantines the dirty local copy": a dirty shard whose
    # lease is stolen self-fences at the next durability flush and renames its local copy aside.
    defp self_fence!(shard) do
      {:ok, conn} = ShardExecutor.open(shard)
      {:ok, _} = ShardExecutor.execute(conn, stmt("CREATE TABLE kv (v TEXT)"))
      {:ok, _} = ShardExecutor.execute(conn, stmt("INSERT INTO kv VALUES ('acked')"))
      :ok = ShardExecutor.close(conn)

      {:ok, coordinator} = Shards.ensure(shard)
      assert Shard.dirty?(coordinator)
      ref = Process.monitor(coordinator)
      put_raw_lock(shard, "thief@node", 999, now_ms() + 60_000)

      capture_log(fn ->
        send(coordinator, :durability_flush)
        assert_receive {:DOWN, ^ref, :process, ^coordinator, {:shutdown, :lease_lost}}, 5_000
      end)
    end

    test "the -wal and -shm are renamed before the .db", %{shard: shard} do
      Application.put_env(:fathom, :shard_idle_ms, 60_000)
      crash = CrashFS.start!(Shard.data_dir())

      self_fence!(shard)

      fenced =
        for {:rename, src, dst} <- CrashFS.trace(crash),
            String.contains?(dst, ".fenced."),
            do: Path.basename(src)

      assert fenced == [shard <> ".db-wal", shard <> ".db-shm", shard <> ".db"]
    end

    test "a failed -wal rename leaves no half-quarantine: no .fenced .db beside a stranded WAL",
         %{shard: shard} do
      Application.put_env(:fathom, :shard_idle_ms, 60_000)

      _crash =
        CrashFS.start!(Shard.data_dir(),
          inject: fail_rename_where(&String.ends_with?(&1, shard <> ".db-wal"))
        )

      self_fence!(shard)

      assert Path.wildcard(local_db(shard) <> ".fenced.*") == [],
             "the .db was moved aside although its -wal could not follow it"
    end
  end
end
