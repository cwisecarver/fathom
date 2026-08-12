defmodule Fathom.Shard.WalHookTest do
  @moduledoc """
  Gate 1 for Phase 2 A2 (quorum replication) — see `docs/a2-quorum-replication.md`.

  A2 needs a seam to ship committed WAL frames from. `exqlite` 0.37.0 exposes none, but the
  **loadable extension** fathom already ships does: `sqlite3_wal_hook` is reachable through the
  `sqlite3_api_routines` pointer table, so `native/fathom_udf/src/wal.rs` registers it.

  **This is the only place that can prove it.** There is no Rust `#[test]` for the hook and there
  cannot be one: the crate is built with rusqlite's `loadable_extension` feature, so every SQLite
  symbol resolves through an API pointer table that a `cargo test` binary never populates — a test
  touching a live connection aborts with "SQLite API not initialized". A normal-linked Rust test
  would also prove the hook fires under a *different* linkage than the one fathom ships, which is
  the "the test double can't express the bug" trap AGENTS.md warns about. Only the path through
  exqlite → dlopen → api routines → hook means anything.

  ## The second test is the important one

  `sqlite3_wal_hook` and `wal_autocheckpoint` are **the same slot** — auto-checkpointing IS a
  built-in WAL hook, so registering ours evicts it. `Connection.configure_readwrite/3` sets
  `PRAGMA wal_autocheckpoint=4000` first (`connection.ex:196`) and loads the extension second
  (`:80`), so a hook that merely observed would silently disable checkpointing on every tenant
  connection and grow the WAL without bound.

  That failure is silent and slow, and it surfaces as the disk-fill symptom expert review #36 built
  `FathomDiskFillingUp` for — pointing the diagnosis at storage rather than here. So this file
  asserts the **negative** (checkpointing still happens) and not just the positive (the hook fired).
  A test that only proved the hook ran would pass in exactly the broken state.
  """
  use ExUnit.Case, async: false

  alias Fathom.Shard.Connection

  # Big enough to blow past AUTOCHECKPOINT_PAGES (4000 × 4 KiB ≈ 16 MiB) with few statements.
  @blob_bytes 100_000
  @blob_rows 250

  setup do
    path = Path.join(System.tmp_dir!(), "fathom_wal_#{System.unique_integer([:positive])}.db")

    on_exit(fn ->
      System.delete_env("FATHOM_WAL_PROBE")
      for s <- ["", "-wal", "-shm"], do: File.rm(path <> s)
    end)

    %{path: path}
  end

  defp open!(path, opts \\ []) do
    {:ok, conn} = Connection.open(path, opts)
    on_exit(fn -> Connection.close(conn) end)
    conn
  end

  defp scalar!(conn, sql) do
    {:ok, %{rows: [[v]]}} = Connection.query(conn, sql, [])
    v
  end

  describe "the WAL commit hook (the A2 frame seam)" do
    # Needs FATHOM_WAL_PROBE=1 set BEFORE the VM starts, so it is excluded by default and CI runs
    # it explicitly (see the :wal_probe note in test/test_helper.exs):
    #     FATHOM_WAL_PROBE=1 mix test --include wal_probe
    #
    # `System.put_env` in this test does NOT work, which cost a debugging round: it updates the
    # BEAM's internal environment table, not the C `environ` that Rust's `std::env::var` reads, so
    # the extension saw the flag unset and skipped the probe registration. The failure looked like
    # "the hook doesn't fire" — a wrong conclusion about the whole A2 premise — when it was only
    # the read-back that was missing.
    @tag :wal_probe
    test "fires on a real commit through Fathom.Shard.Connection", %{path: path} do
      conn = open!(path)

      assert scalar!(conn, "SELECT typeof(fathom_wal_commits())") == "integer",
             "the probe function is missing — the extension did not load, so this test is " <>
               "measuring nothing (check mix compile.fathom_udf and :sqlite_extension)"

      before = scalar!(conn, "SELECT fathom_wal_commits()")

      {:ok, _} = Connection.query(conn, "CREATE TABLE t (a INTEGER)", [])
      {:ok, _} = Connection.query(conn, "INSERT INTO t VALUES (1)", [])

      after_ = scalar!(conn, "SELECT fathom_wal_commits()")

      assert after_ > before,
             "sqlite3_wal_hook never fired (#{before} -> #{after_}). The seam A2 depends on is " <>
               "not reachable through the loadable extension after all."

      assert scalar!(conn, "SELECT fathom_wal_pages()") > 0,
             "the hook fired but reported 0 WAL pages, so the frame count A2 ships on is unusable"
    end

    # Deliberately NOT excluded by a tag. It is the regression test for a silent, slow,
    # misattributed production failure (unbounded WAL growth presenting as a disk-fill), it runs in
    # well under a second, and it must execute on every plain `mix test`.
    test "does NOT disable checkpointing — the WAL still drains to the main db", %{path: path} do
      conn = open!(path)
      {:ok, _} = Connection.query(conn, "CREATE TABLE t (a BLOB)", [])

      # Each statement autocommits, so this is @blob_rows separate commits — the hook fires on
      # every one and must checkpoint once the WAL crosses its threshold.
      for _ <- 1..@blob_rows do
        {:ok, _} =
          Connection.query(conn, "INSERT INTO t VALUES (randomblob(?1))", [@blob_bytes])
      end

      # Measured with the connection still OPEN, deliberately. SQLite checkpoints on last-connection
      # close, so closing first would make this pass whether or not the hook checkpoints.
      db_bytes = File.stat!(path).size
      wal_bytes = File.stat!(path <> "-wal").size
      written = @blob_bytes * @blob_rows

      # If checkpointing were evicted, EVERY page would still be sitting in the WAL and the main
      # database file would still be roughly empty. This is the assertion that fails in the broken
      # state; the WAL-size bound below cannot carry it alone, because a PASSIVE checkpoint reuses
      # the WAL file rather than shrinking it.
      assert db_bytes > div(written, 2),
             "the main db is #{db_bytes} bytes after writing ~#{written} — the WAL never " <>
               "checkpointed, so registering the hook evicted wal_autocheckpoint " <>
               "(see native/fathom_udf/src/wal.rs)"

      assert wal_bytes < written,
             "the WAL is #{wal_bytes} bytes and holds everything written (~#{written}) — " <>
               "it is growing without bound"
    end
  end
end
