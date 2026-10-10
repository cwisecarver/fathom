defmodule Fathom.Shard.SoftHeapLimitTest do
  @moduledoc """
  The node-wide SQLite `soft_heap_limit` (expert review 2026-10-08 #8).

  `:shard_cache_size_kb` bounds page cache PER CONNECTION and fathom holds one connection per Hrana
  stream, so total cache grew as streams × 2 MiB with nothing node-wide over it.
  `Fathom.Shard.Connection.apply_soft_heap_limit/0` (called once at boot by `Fathom.Application`)
  sets SQLite's process-global soft limit from a trusted `:memory:` connection.

  The assertions read the limit back on a FRESH, separately opened connection — the property the
  fix rests on is that the setting is process-global, so a value set on the throwaway boot
  connection must govern every tenant connection opened afterwards.

  Not async: the limit is process-global, so this module owns it while it runs and resets it to
  SQLite's default (0, no limit) afterwards.
  """
  use ExUnit.Case, async: false

  alias Exqlite.Sqlite3
  alias Fathom.Shard.Connection

  @gib 1024 * 1024 * 1024

  setup do
    prev = Application.get_env(:fathom, :shard_soft_heap_limit_bytes)

    on_exit(fn ->
      if prev,
        do: Application.put_env(:fathom, :shard_soft_heap_limit_bytes, prev),
        else: Application.delete_env(:fathom, :shard_soft_heap_limit_bytes)

      set_raw(0)
    end)

    set_raw(0)
    :ok
  end

  test "dev/test boot leaves SQLite's soft heap limit untouched (unset ⇒ :skipped)" do
    # The env default carries no key: dev/test must behave exactly as before this knob existed.
    assert Application.get_env(:fathom, :shard_soft_heap_limit_bytes) == nil
    assert Connection.apply_soft_heap_limit() == :skipped
    assert read_fresh() == 0
  end

  test "0 means off: nothing is applied" do
    Application.put_env(:fathom, :shard_soft_heap_limit_bytes, 0)
    assert Connection.apply_soft_heap_limit() == :skipped
    assert read_fresh() == 0
  end

  test "a configured limit governs a connection opened AFTER it, and re-applying is idempotent" do
    Application.put_env(:fathom, :shard_soft_heap_limit_bytes, 8 * @gib)

    assert Connection.apply_soft_heap_limit() == :ok
    assert read_fresh() == 8 * @gib

    # Idempotent: a second boot-path call re-asserts the same value rather than failing.
    assert Connection.apply_soft_heap_limit() == :ok
    assert read_fresh() == 8 * @gib
  end

  test "a configured limit also reaches a connection that was ALREADY open (process-global)" do
    {:ok, held} = Sqlite3.open(":memory:")

    try do
      Application.put_env(:fathom, :shard_soft_heap_limit_bytes, 4 * @gib)
      assert Connection.apply_soft_heap_limit() == :ok
      assert read(held) == 4 * @gib
    after
      Sqlite3.close(held)
    end
  end

  defp read_fresh do
    {:ok, conn} = Sqlite3.open(":memory:")

    try do
      read(conn)
    after
      Sqlite3.close(conn)
    end
  end

  defp read(conn), do: pragma(conn, "PRAGMA soft_heap_limit")

  defp set_raw(bytes) do
    {:ok, conn} = Sqlite3.open(":memory:")

    try do
      pragma(conn, "PRAGMA soft_heap_limit=#{bytes}")
    after
      Sqlite3.close(conn)
    end
  end

  defp pragma(conn, sql) do
    {:ok, stmt} = Sqlite3.prepare(conn, sql)

    try do
      {:ok, [[value]]} = Sqlite3.fetch_all(conn, stmt)
      value
    after
      Sqlite3.release(conn, stmt)
    end
  end
end
