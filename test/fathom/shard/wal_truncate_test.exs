defmodule Fathom.Shard.WalTruncateTest do
  @moduledoc """
  `journal_size_limit` (expert review 2026-10-10 #30) and A2's offset reads.

  The setting makes the writer that resets the WAL truncate the file. A2 reads WAL ranges by offset,
  so the question was whether a truncation can make it ship the wrong bytes. It cannot: a truncation
  only happens at a log reset, and a read of a range that is no longer there is a `:short_read`,
  which fails the round (the safe direction). These pin that, plus that the open applies the bound.
  """
  use ExUnit.Case, async: false

  alias Fathom.Shard.Connection
  alias Fathom.Shard.Replication.Wal

  setup do
    path = Path.join(System.tmp_dir!(), "waltrunc_#{System.unique_integer([:positive])}.db")
    prior = Application.fetch_env(:fathom, :journal_size_limit)

    on_exit(fn ->
      case prior do
        {:ok, v} -> Application.put_env(:fathom, :journal_size_limit, v)
        :error -> Application.delete_env(:fathom, :journal_size_limit)
      end

      for sfx <- ["", "-wal", "-shm"], do: File.rm(path <> sfx)
    end)

    %{path: path}
  end

  defp pragma(conn) do
    {:ok, %{rows: [[n]]}} = Connection.query(conn, "PRAGMA journal_size_limit", [])
    n
  end

  test "an open applies the 64 MiB default, a configured value, and none when negative", %{
    path: path
  } do
    Application.delete_env(:fathom, :journal_size_limit)
    {:ok, conn} = Connection.open(path)
    assert pragma(conn) == 64 * 1024 * 1024
    :ok = Connection.close(conn)

    Application.put_env(:fathom, :journal_size_limit, 1_048_576)
    {:ok, conn} = Connection.open(path)
    assert pragma(conn) == 1_048_576
    :ok = Connection.close(conn)

    # -1 is SQLite's own "no limit", which is what a fresh handle reports when nothing sets it.
    Application.put_env(:fathom, :journal_size_limit, -1)
    {:ok, conn} = Connection.open(path)
    assert pragma(conn) == -1
    :ok = Connection.close(conn)
  end

  test "a WAL truncated at reset makes the old range a short read, never stale bytes", %{
    path: path
  } do
    Application.put_env(:fathom, :journal_size_limit, 0)
    wal = path <> "-wal"

    {:ok, conn} = Connection.open(path)
    {:ok, _} = Connection.query(conn, "PRAGMA wal_autocheckpoint=0", [])
    {:ok, _} = Connection.query(conn, "CREATE TABLE t (a INTEGER, b TEXT)", [])

    for i <- 1..50 do
      {:ok, _} = Connection.query(conn, "INSERT INTO t VALUES (?1, ?2)", [i, "row#{i}"])
    end

    {:ok, before} = Wal.read(wal)
    off = Wal.header_bytes()
    len = before.commit_extent - off
    assert len > 0, "the fixture wrote no frames"

    # The same sequence the durability flush produces: a PASSIVE checkpoint, then a write that
    # resets the log. With the limit at 0 the reset truncates the file.
    {:ok, _} = Connection.query(conn, "PRAGMA wal_checkpoint(PASSIVE)", [])
    {:ok, _} = Connection.query(conn, "INSERT INTO t VALUES (999, 'after')", [])

    {:ok, after_} = Wal.read(wal)

    assert {after_.ckpt_seq, after_.salt1} != {before.ckpt_seq, before.salt1},
           "the log was not reset, so this fixture shows nothing"

    assert after_.size < before.commit_extent, "the limit did not truncate the WAL"

    # A2's read of the range it planned from `before` fails instead of returning new-generation
    # bytes under the old header: the round fails and re-plans.
    assert {:error, :short_read} = Wal.read_delta(wal, off, len)

    :ok = Connection.close(conn)
  end
end
