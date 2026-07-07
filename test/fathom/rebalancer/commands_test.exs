defmodule Fathom.Rebalancer.CommandsTest do
  @moduledoc "The cross-node handoff command channel API."
  use Fathom.DataCase, async: true

  alias Fathom.Rebalancer.Commands

  test "issue / pending_for / complete lifecycle" do
    {:ok, cmd} = Commands.issue("hot_1", "fathom2", "warm")
    assert cmd.status == "pending"

    assert Enum.any?(Commands.pending_for("fathom2"), &(&1.id == cmd.id))
    assert Commands.pending_for("fathom1") == [], "only the addressed node sees it"

    {:ok, done} = Commands.complete(cmd, "done", "warmed")
    assert done.status == "done"
    assert Commands.pending_for("fathom2") == [], "completed commands drop out of the queue"
  end

  test "await returns on a terminal status and times out otherwise" do
    {:ok, ok} = Commands.issue("s", "n", "drain")
    {:ok, _} = Commands.complete(ok, "done", "drained")
    assert {:ok, %{status: "done"}} = Commands.await(ok.id, timeout_ms: 100)

    {:ok, bad} = Commands.issue("s", "n", "drain")
    {:ok, _} = Commands.complete(bad, "failed", "busy")
    assert {:error, {:command_failed, "busy"}} = Commands.await(bad.id, timeout_ms: 100)

    {:ok, pending} = Commands.issue("s", "n", "warm")
    assert {:error, :timeout} = Commands.await(pending.id, timeout_ms: 60, poll_ms: 20)
  end

  test "rejects an unknown command type" do
    assert {:error, changeset} = Commands.issue("s", "n", "explode")
    assert "is invalid" in errors_on(changeset).command
  end
end
