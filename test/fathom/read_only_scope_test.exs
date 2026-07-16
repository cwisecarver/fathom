defmodule Fathom.ReadOnlyScopeTest do
  @moduledoc """
  Read-only token scope (expert review 2026-07-14 #24): a `ro` token may read but every write
  (DML or DDL) is refused with a distinct 403 `FILO_READONLY`. Exercises the real flow —
  `HranaAuth.authorize/2` (which stashes the token's scope) then `ShardExecutor.open/1` (which
  reads it into the handle) then `execute/2` (which enforces it). DataCase (async: false): the
  directory + Revocations cache, and real shard files.
  """
  use Fathom.DataCase, async: false

  alias Fathom.{Directory, HranaAuth, Shards, ShardExecutor}
  alias Filo.Stmt

  @remote_dir Path.join(System.tmp_dir!(), "fathom_remote_test")

  setup do
    prev_mode = Application.get_env(:fathom, :hrana_auth, :disabled)
    Application.put_env(:fathom, :hrana_auth, :required)
    shard = "ro_#{System.unique_integer([:positive])}"

    on_exit(fn ->
      Application.put_env(:fathom, :hrana_auth, prev_mode)
      Shards.drain(shard, 2_000)

      for dir <- [@remote_dir, Path.join(System.tmp_dir!(), "fathom_shards")],
          path <- Path.wildcard(Path.join(dir, "#{shard}*")),
          do: File.rm(path)
    end)

    %{shard: shard}
  end

  defp stmt(sql, args \\ []), do: %Stmt{sql: sql, args: args}

  # Open a stream the way Filo does: authorize (stashes scope) then open (reads it), same process.
  defp open_stream!(shard, token) do
    assert HranaAuth.authorize(shard, token) == :ok
    {:ok, handle} = ShardExecutor.open(shard)
    handle
  end

  test "a read-only token can SELECT but not write", %{shard: shard} do
    {:ok, _} = Directory.resolve(shard)

    # Seed via a FULL-access stream (default rw).
    {:ok, full} = HranaAuth.token_for(shard)
    h1 = open_stream!(shard, full)
    {:ok, _} = ShardExecutor.execute(h1, stmt("CREATE TABLE t (v TEXT)"))
    {:ok, _} = ShardExecutor.execute(h1, stmt("INSERT INTO t VALUES ('x')"))
    :ok = ShardExecutor.close(h1)

    # A read-only stream: reads work, writes and DDL are refused 403.
    {:ok, ro} = HranaAuth.token_for(shard, scope: :ro)
    h2 = open_stream!(shard, ro)

    assert {:ok, res} = ShardExecutor.execute(h2, stmt("SELECT v FROM t"))
    assert res.rows == [["x"]]

    assert {:error, %Filo.Error{status: 403, code: "FILO_READONLY"}} =
             ShardExecutor.execute(h2, stmt("INSERT INTO t VALUES ('y')"))

    assert {:error, %Filo.Error{status: 403, code: "FILO_READONLY"}} =
             ShardExecutor.execute(h2, stmt("UPDATE t SET v = 'z'"))

    assert {:error, %Filo.Error{status: 403, code: "FILO_READONLY"}} =
             ShardExecutor.execute(h2, stmt("CREATE TABLE u (v TEXT)"))

    # The refused write really didn't land.
    assert {:ok, %{rows: [["x"]]}} = ShardExecutor.execute(h2, stmt("SELECT v FROM t"))
    :ok = ShardExecutor.close(h2)
  end

  test "a full (rw) token — the default — writes normally", %{shard: shard} do
    {:ok, _} = Directory.resolve(shard)
    {:ok, full} = HranaAuth.token_for(shard)

    h = open_stream!(shard, full)
    assert {:ok, _} = ShardExecutor.execute(h, stmt("CREATE TABLE t (v TEXT)"))
    assert {:ok, _} = ShardExecutor.execute(h, stmt("INSERT INTO t VALUES ('ok')"))
    :ok = ShardExecutor.close(h)
  end
end
