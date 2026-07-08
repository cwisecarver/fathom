defmodule Fathom.Rebalancer.CommandsTest do
  @moduledoc "The cross-node handoff command channel API."
  use Fathom.DataCase, async: true

  alias Fathom.Rebalancer.{Command, Commands}

  # Backdate a command's timestamps so it looks older than a retention/stale window.
  defp age(id, ms) do
    at = DateTime.add(DateTime.utc_now(), -ms, :millisecond)

    Repo.update_all(from(c in Command, where: c.id == ^id),
      set: [inserted_at: at, updated_at: at]
    )
  end

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

  test "prune_terminal deletes old terminal commands, keeps recent + pending (#12)" do
    {:ok, old_done} = Commands.issue("s", "n", "drain")
    {:ok, _} = Commands.complete(old_done, "done", "drained")
    {:ok, recent_done} = Commands.issue("s2", "n", "warm")
    {:ok, _} = Commands.complete(recent_done, "done", "warmed")
    {:ok, pending} = Commands.issue("s3", "n", "drain")

    # old_done completed 2h ago; retention 1h.
    age(old_done.id, 7_200_000)

    assert Commands.prune_terminal(3_600_000) == 1
    assert Commands.get(old_done.id) == nil
    assert Commands.get(recent_done.id) != nil, "recent terminal kept"
    assert Commands.get(pending.id) != nil, "pending untouched by terminal prune"
  end

  test "expire_stale_pending fails old pending, keeps recent pending + terminal (#12)" do
    {:ok, old_pending} = Commands.issue("s", "n", "drain")
    {:ok, recent_pending} = Commands.issue("s2", "n", "warm")
    {:ok, done} = Commands.issue("s3", "n", "drain")
    {:ok, _} = Commands.complete(done, "done", "drained")

    # old_pending issued ~16min ago; stale window 15min.
    age(old_pending.id, 1_000_000)

    assert Commands.expire_stale_pending(900_000) == 1
    expired = Commands.get(old_pending.id)
    assert expired.status == "failed"
    assert expired.detail =~ "expired"
    assert Commands.get(recent_pending.id).status == "pending", "recent pending kept"
    assert Commands.get(done.id).status == "done", "terminal untouched"
  end
end
