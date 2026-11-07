defmodule Fathom.Shard.ForkQuarantineOrderTest do
  @moduledoc """
  Expert review 2026-11-07 #83(2). `Fork.quarantine_copy!` renamed the `.db` aside FIRST and the
  `-wal`/`-shm` best-effort afterwards, so a failed `-wal` rename (or a crash between the two) left
  acked frames beside nothing at the live path. Invariant: the companions move BEFORE the `.db`,
  every result is checked, and the quarantine is all-or-nothing — on any failure the live path keeps
  its `.db` (the copy is still served warm, fenced by the provenance etag) and its `-wal`.

  Failures are injected through `Fathom.Test.CrashFS`'s `inject:` at the `Fathom.FS.rename/2` seam.
  """
  use ExUnit.Case, async: true

  import ExUnit.CaptureLog

  alias Fathom.Shard.Fork
  alias Fathom.Test.CrashFS

  setup do
    dir = Path.join(System.tmp_dir!(), "fork_q_#{System.unique_integer([:positive])}")
    File.mkdir_p!(dir)
    on_exit(fn -> File.rm_rf(dir) end)

    path = Path.join(dir, "tenant.db")
    File.write!(path, "DB")
    File.write!(path <> "-wal", "ACKED-FRAMES")
    File.write!(path <> "-shm", "SHM")
    File.write!(path <> ".etag", "etag")

    %{dir: dir, path: path}
  end

  defp quarantined(dir, path) do
    dir
    |> Path.join(Path.basename(path) <> ".forked.*")
    |> Path.wildcard()
    |> Enum.filter(&Regex.match?(~r/\.forked\.\d+-\d+$/, &1))
  end

  defp fail_rename_where(pred) do
    fn
      {:rename, src, _dst} -> if pred.(src), do: {:error, :eio}, else: :ok
      _ -> :ok
    end
  end

  test "the -wal and -shm move before the .db, and the frames land beside the quarantined .db",
       %{dir: dir, path: path} do
    crash = CrashFS.start!(dir)

    capture_log(fn -> assert :ok = Fork.quarantine!("tenant", path, :diverged) end)

    renames = for {:rename, src, _} <- CrashFS.trace(crash), do: Path.basename(src)
    assert renames == ["tenant.db-wal", "tenant.db-shm", "tenant.db"]

    assert [dest] = quarantined(dir, path)
    assert File.read!(dest) == "DB"
    assert File.read!(dest <> "-wal") == "ACKED-FRAMES"
    assert File.read!(dest <> "-shm") == "SHM"
    refute File.exists?(path)
    refute File.exists?(path <> "-wal")
  end

  test "a failed -wal rename aborts: the .db is NOT moved and the live set is intact",
       %{dir: dir, path: path} do
    _crash = CrashFS.start!(dir, inject: fail_rename_where(&String.ends_with?(&1, "-wal")))

    log =
      capture_log(fn ->
        assert {:error, :eio} = Fork.quarantine!("tenant", path, :diverged)
      end)

    assert log =~ "fork quarantine FAILED"
    assert File.read!(path) == "DB", "the .db must stay put when its -wal could not move"
    assert File.read!(path <> "-wal") == "ACKED-FRAMES"
    assert quarantined(dir, path) == []
    assert File.exists?(path <> ".etag"), "a failed quarantine must keep the provenance sidecar"
  end

  test "a failed .db rename rolls the already-moved companions back", %{dir: dir, path: path} do
    _crash =
      CrashFS.start!(dir,
        inject: fail_rename_where(&(Path.basename(&1) == "tenant.db"))
      )

    capture_log(fn -> assert {:error, :eio} = Fork.quarantine!("tenant", path, :diverged) end)

    assert File.read!(path) == "DB"
    assert File.read!(path <> "-wal") == "ACKED-FRAMES", "the -wal must be moved back"
    assert File.read!(path <> "-shm") == "SHM"
    assert quarantined(dir, path) == []
    assert Path.wildcard(Path.join(dir, "*.forked.*")) == []
  end

  test "a copy with no -wal or -shm (cleanly closed) quarantines without error",
       %{dir: dir, path: path} do
    File.rm!(path <> "-wal")
    File.rm!(path <> "-shm")

    capture_log(fn -> assert :ok = Fork.quarantine!("tenant", path, :diverged) end)

    assert [dest] = quarantined(dir, path)
    assert File.read!(dest) == "DB"
    refute File.exists?(path)
  end
end
