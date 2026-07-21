defmodule Fathom.AuditTest do
  @moduledoc """
  The append-only control-plane audit trail (expert review #9): every mutating action records who /
  what / which tenant / from where / outcome; `record/6` is best-effort (a DB error is logged, never
  raised) so an audit failure never breaks the audited action; and it emits telemetry.
  """
  use Fathom.DataCase, async: true

  alias Fathom.Audit

  test "record inserts an event and list returns matches newest-first" do
    :ok = Audit.record("alice", "delete", "acme", "10.0.0.1", "ok", %{})
    :ok = Audit.record("bob", "export", "acme", "10.0.0.2", "ok", %{})

    events = Audit.list(shard_id: "acme")
    assert [%{actor: "bob", action: "export"}, %{actor: "alice", action: "delete"}] = events
    assert Enum.all?(events, &(&1.shard_id == "acme"))
  end

  test "record emits [:fathom, :audit, :event] telemetry" do
    test_pid = self()
    handler = "audit-#{System.unique_integer([:positive])}"

    :telemetry.attach(
      handler,
      [:fathom, :audit, :event],
      fn _e, _m, meta, _ -> send(test_pid, {:audit, meta}) end,
      nil
    )

    on_exit(fn -> :telemetry.detach(handler) end)

    :ok = Audit.record("alice", "restore", "acme", nil, "ok", %{})
    assert_receive {:audit, %{action: "restore", actor: "alice", outcome: "ok"}}
  end

  test "record is best-effort: an invalid event is logged, not raised (returns :ok)" do
    import ExUnit.CaptureLog
    # actor is required — a nil actor fails validation, which must be swallowed as :ok.
    log = capture_log(fn -> assert :ok = Audit.record(nil, "delete", "acme", nil, "ok", %{}) end)
    assert log =~ "audit insert failed"
  end
end
