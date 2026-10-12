defmodule Fathom.ShardsRemoteFlushTest do
  @moduledoc """
  `Shards.flush/1` when the shard's coordinator lives on ANOTHER node (expert review 2026-10-12 #7).

  Symptom: with no local coordinator and no kept local file, `flush/1` answered `:ok` ("nothing is
  writing here"). On a multi-node fleet (N-1)/N of admin calls land on a non-holder, so export,
  keystone fork and `flush: true` snapshots read the holder's last PERIODIC flush and silently
  missed up to one flush interval of acked writes. Invariant: a live remote lease holder is asked to
  flush through the command channel and awaited; if it cannot be reached the answer is
  `{:error, :remote_holder_unflushed}`, never `:ok`. No holder at all is still `:ok`.

  The remote node is faked the way `shard_migration_job_test` fakes it: a lock + heartbeat file
  owned by "remotenode#inc1" in the Local storage dir, and a task that plays that node's
  CommandPoller against `Commands.pending_for("remotenode")`.
  """
  use Fathom.DataCase, async: false

  alias Fathom.Rebalancer.{CommandPoller, Commands}
  alias Fathom.{ShardExecutor, Shards, Tenants}
  alias Filo.Stmt

  @remote_owner "remotenode#inc1"

  setup do
    id = "remoteflush-#{System.unique_integer([:positive])}"
    Application.put_env(:fathom, :command_poller, true)

    on_exit(fn ->
      Application.delete_env(:fathom, :command_poller)
      Application.delete_env(:fathom, :remote_flush_ms)
      remove_remote(id)
      Shards.stop(id)

      for dir <- [Fathom.Shard.data_dir(), Fathom.Shard.Storage.Local.dir()],
          path <- Path.wildcard(Path.join(dir, "#{id}*")),
          do: File.rm(path)
    end)

    %{id: id}
  end

  defp remote_dir, do: Fathom.Shard.Storage.Local.dir()

  defp heartbeat_path,
    do: Path.join([remote_dir(), "heartbeats", URI.encode_www_form(@remote_owner)])

  defp put_remote_holder(id, heartbeat_ms \\ 60_000) do
    File.mkdir_p!(Path.dirname(heartbeat_path()))
    exp = System.system_time(:millisecond) + 60_000

    File.write!(
      Path.join(remote_dir(), "#{id}.lock"),
      Jason.encode!(%{"owner" => @remote_owner, "epoch" => 1, "expires_at_ms" => exp})
    )

    File.write!(
      heartbeat_path(),
      Jason.encode!(%{
        "owner" => @remote_owner,
        "expires_at_ms" => System.system_time(:millisecond) + heartbeat_ms
      })
    )
  end

  defp remove_remote(id) do
    File.rm(Path.join(remote_dir(), "#{id}.lock"))
    File.rm(heartbeat_path())
  end

  # Plays the holder node's CommandPoller for one command, then reports what it saw.
  defp fake_remote_node(id, outcome) do
    Task.async(fn ->
      [cmd | _] =
        Stream.repeatedly(fn ->
          Process.sleep(20)
          Commands.pending_for("remotenode")
        end)
        |> Enum.find(&(&1 != []))

      assert cmd.command == "flush"
      assert cmd.shard_id == id

      case outcome do
        :flushed -> Commands.complete(cmd, "done", "flushed")
        :fail -> Commands.complete(cmd, "failed", "flush failed (:enospc)")
      end

      cmd.command
    end)
  end

  test "a live remote holder is asked to flush through the command channel", %{id: id} do
    put_remote_holder(id)
    remote = fake_remote_node(id, :flushed)

    assert :ok = Shards.flush(id)
    assert "flush" = Task.await(remote)
  end

  test "a remote holder whose flush fails is an error, never :ok", %{id: id} do
    put_remote_holder(id)
    remote = fake_remote_node(id, :fail)

    assert {:error, :remote_holder_unflushed} = Shards.flush(id)
    Task.await(remote)
  end

  test "an unresponsive remote holder times out, cancels its command, and errors", %{id: id} do
    Application.put_env(:fathom, :remote_flush_ms, 150)
    put_remote_holder(id)

    assert {:error, :remote_holder_unflushed} = Shards.flush(id)

    assert [] = Commands.pending_for("remotenode"),
           "a timed-out flush command must not stay pending for the holder to fire later"
  end

  test "with the command poller off no command is issued and the answer is an error", %{id: id} do
    Application.delete_env(:fathom, :command_poller)
    put_remote_holder(id)

    assert {:error, :remote_holder_unflushed} = Shards.flush(id)
    assert [] = Commands.pending_for("remotenode")
  end

  test "a remote holder with a lapsed heartbeat is an error without issuing a command",
       %{id: id} do
    put_remote_holder(id, -1_000)

    assert {:error, :remote_holder_unflushed} = Shards.flush(id)
    assert [] = Commands.pending_for("remotenode")
  end

  test "no lease holder at all is still :ok (the stored object is current)", %{id: id} do
    assert :ok = Shards.flush(id)
    assert [] = Commands.pending_for("remotenode")
  end

  test "export (flush default on) and the flush API surface the error, not a stale object",
       %{id: id} do
    put_remote_holder(id)
    Application.put_env(:fathom, :remote_flush_ms, 150)

    assert {:error, :remote_holder_unflushed} = Tenants.export(id, flush: true)
    assert {:error, :remote_holder_unflushed} = Tenants.flush(id)
  end

  test "the holder's poller executes a flush command for its own coordinator", %{id: id} do
    {:ok, conn} = ShardExecutor.open(id)
    {:ok, _} = ShardExecutor.execute(conn, %Stmt{sql: "CREATE TABLE t (a INTEGER)", args: []})
    {:ok, _} = ShardExecutor.execute(conn, %Stmt{sql: "INSERT INTO t VALUES (1)", args: []})

    start_supervised!(CommandPoller)
    {:ok, cmd} = Commands.issue(id, Fathom.Rebalancer.node_key(), "flush")
    assert CommandPoller.poll_now() == 1
    assert %{status: "done"} = Commands.get(cmd.id)

    :ok = ShardExecutor.close(conn)
  end
end
