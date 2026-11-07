defmodule Fathom.SqliteTempDirCheckTest do
  @moduledoc """
  Expert review 2026-11-07 #32. Symptom: a tenant's `ORDER BY` spills hundreds of MB per second to
  SQLite's temp directory, which `PRAGMA temp.max_page_count` does not bound, so when that directory
  shares a volume with `SHARD_DATA_DIR` one statement can fill the volume every co-resident tenant's
  commits and flushes need. SQLite has no sorter-bytes limit, so the bound is operational and this
  boot check is the guard: it must name SQLite's REAL temp dir (`SQLITE_TMPDIR` before `TMPDIR`
  before `/var/tmp`; NOT `System.tmp_dir!/0`, which ignores `SQLITE_TMPDIR`) and warn when it shares
  a volume with the data (or, on a follower, replica) dir.

  The spill itself is not asserted here: a deterministic multi-GB spill is not a `mix test` fixture
  (re-measured by a throwaway exqlite probe, 412 MB unlinked temp file after 5 s with
  `temp.max_page_count=1000` set). Not async: the end-to-end cases mutate application env.
  """
  use ExUnit.Case, async: false

  import ExUnit.CaptureLog

  alias Fathom.Application, as: App
  alias Fathom.Test.Env

  describe "sqlite_temp_dir/2 resolves like SQLite's unix VFS" do
    test "SQLITE_TMPDIR wins over TMPDIR, which wins over the fixed fallbacks" do
      usable = fn _ -> true end
      env = fn vars -> fn k -> Map.get(vars, k) end end

      assert App.sqlite_temp_dir(env.(%{"SQLITE_TMPDIR" => "/q", "TMPDIR" => "/t"}), usable) ==
               "/q"

      assert App.sqlite_temp_dir(env.(%{"TMPDIR" => "/t"}), usable) == "/t"
      # /var/tmp BEFORE /tmp: System.tmp_dir!/0 would answer /tmp here.
      assert App.sqlite_temp_dir(env.(%{}), usable) == "/var/tmp"
    end

    test "an unusable candidate is skipped, an empty variable is ignored" do
      env = fn
        "SQLITE_TMPDIR" -> "/nope"
        "TMPDIR" -> ""
        _ -> nil
      end

      assert App.sqlite_temp_dir(env, &(&1 == "/tmp")) == "/tmp"
      assert App.sqlite_temp_dir(env, fn _ -> false end) == nil
    end
  end

  describe "sqlite_temp_warnings/3" do
    test "one warning per directory that shares the volume, naming both" do
      same = fn _temp, dir -> dir != "/data/other" end

      assert [shard, replica] =
               App.sqlite_temp_warnings(
                 "/var/tmp",
                 [
                   {"SHARD_DATA_DIR", "/data/shards"},
                   {"REPLICATION_DIR", "/data/replicas"},
                   {"ELSEWHERE", "/data/other"}
                 ],
                 same
               )

      assert shard =~ "SHARD_DATA_DIR (/data/shards)" and shard =~ "/var/tmp"
      assert shard =~ "SQLITE_TMPDIR"
      assert replica =~ "REPLICATION_DIR (/data/replicas)"
    end

    test "a separate volume, or an unresolvable temp dir, warns about nothing" do
      assert App.sqlite_temp_warnings("/mnt/tmpfs", [{"SHARD_DATA_DIR", "/data"}], fn _, _ ->
               false
             end) == []

      assert App.sqlite_temp_warnings(nil, [{"SHARD_DATA_DIR", "/data"}], fn _, _ -> true end) ==
               []
    end
  end

  describe "check_sqlite_temp_dir!/0" do
    setup do
      # Preconditions the cases rely on: SQLite's temp dir resolves, and a path under it is on its
      # volume (the same mount comparison the replication-disk check uses).
      assert temp = App.sqlite_temp_dir(), "no usable SQLite temp dir on this host"

      Env.put!(:shard_data_dir, Path.join(temp, "fathom_temp_check_probe"))
      Env.put!(:replication_listen, false)
      %{temp: temp}
    end

    test "prod: a data dir on the temp dir's volume warns", %{temp: temp} do
      Env.put!(:env, :prod)

      log = capture_log([level: :warning], fn -> assert App.check_sqlite_temp_dir!() == nil end)

      assert log =~ "SQLite's temp directory (#{temp}"
      assert log =~ "SHARD_DATA_DIR"
    end

    test "prod: a follower's replica dir on that volume is named as well", %{temp: temp} do
      Env.put!(:env, :prod)
      Env.put!(:replication_listen, true)
      Env.put!(:replication_dir, Path.join(temp, "fathom_temp_check_replica"))

      log = capture_log([level: :warning], fn -> App.check_sqlite_temp_dir!() end)

      assert log =~ "REPLICATION_DIR"
    end

    test "not prod: silent" do
      Env.put!(:env, :dev)

      assert capture_log([level: :warning], fn -> App.check_sqlite_temp_dir!() end) == ""
    end
  end
end
