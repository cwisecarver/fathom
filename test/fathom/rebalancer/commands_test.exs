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

  test "cancel_pending_drains cancels only pending drains for the shard (#7)" do
    {:ok, d1} = Commands.issue("s", "src", "drain")
    {:ok, d2} = Commands.issue("s", "src", "drain")
    {:ok, warm} = Commands.issue("s", "tgt", "warm")
    {:ok, done} = Commands.issue("s", "src", "drain")
    {:ok, _} = Commands.complete(done, "done", "drained")
    {:ok, other} = Commands.issue("other", "src", "drain")

    assert Commands.cancel_pending_drains("s") == 2, "both pending drains for s cancelled"

    assert Commands.get(d1.id).status == "cancelled"
    assert Commands.get(d2.id).status == "cancelled"
    # A warm, an already-terminal drain, and another shard's drain are untouched.
    assert Commands.get(warm.id).status == "pending"
    assert Commands.get(done.id).status == "done"
    assert Commands.get(other.id).status == "pending"

    # Idempotent: nothing left pending.
    assert Commands.cancel_pending_drains("s") == 0
  end
end
