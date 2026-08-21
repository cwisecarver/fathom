defmodule Fathom.Shards.ForkOutcomeTest do
  @moduledoc """
  Which fork outcomes are benign and which PAGE (expert review 2026-08-20 #35).

  `Fathom.Shards.ensure/1` is a find-or-start with a genuine race: two callers can both see
  `Registry.lookup == []` and both enter `start_if_capacity/1`, and with `:fork_from_template` on
  both call `fork_novel/1`. The loser gets `{:retry, reason}` — a DECLARED return of
  `Migrator.fork_from_template/1`, and `fork_novel/1`'s own comment already names "a concurrent
  forker holding the lease" as expected.

  It fell through to the generic clause, which logs at ERROR that *"the tenant is born EMPTY and
  serving with NO schema … Delete the tenant and re-mint it"* and emits
  `[:fathom, :migrator, :fork_fallback]` — an alert that **pages on any occurrence**, deliberately,
  because a born-empty tenant is a silent hard outage no 5xx rate will ever reveal.

  All of that is false for a `{:retry, _}`: the other caller's fork SUCCEEDED and the tenant has
  its schema. An ordinary signup race produced a page instructing an operator to delete a healthy
  tenant.

  Tested as a CLASSIFICATION rather than through a real race, per AGENTS.md ("classifier and
  dispatcher mismatches — test the classification"): the defect was a missing clause, and reaching
  every branch through a genuine fork would need a concurrent lease holder per case.
  """
  use ExUnit.Case, async: false

  import ExUnit.CaptureLog

  alias Fathom.Shards

  setup do
    ref = make_ref()
    parent = self()

    :telemetry.attach_many(
      "fork-outcome-#{inspect(ref)}",
      [
        [:fathom, :migrator, :fork_fallback],
        [:fathom, :migrator, :fork_retry]
      ],
      fn event, _m, meta, _ -> send(parent, {:fork_event, event, meta}) end,
      nil
    )

    on_exit(fn -> :telemetry.detach("fork-outcome-#{inspect(ref)}") end)
    %{shard: "forkout_#{System.unique_integer([:positive])}"}
  end

  test "a concurrent forker is NOT a born-empty tenant", %{shard: id} do
    log = capture_log(fn -> assert :ok = Shards.report_fork({:retry, :locked}, id) end)

    refute log =~ "born EMPTY",
           "a signup race was reported as a tenant serving with no schema, telling the operator " <>
             "to delete a healthy tenant"

    assert_receive {:fork_event, [:fathom, :migrator, :fork_retry], %{shard_id: ^id}}

    refute_received {:fork_event, [:fathom, :migrator, :fork_fallback], _},
                    "the fork_fallback alert pages on ANY occurrence, deliberately — routing a " <>
                      "concurrent-forker race there makes it page on ordinary signups"
  end

  test "a REAL failure still pages, which is the behaviour being protected", %{shard: id} do
    log =
      capture_log(fn -> assert :ok = Shards.report_fork({:error, :no_template_snapshot}, id) end)

    assert log =~ "born EMPTY"

    assert_receive {:fork_event, [:fathom, :migrator, :fork_fallback],
                    %{shard_id: ^id, reason: :no_template_snapshot}}
  end

  test "the two benign refusals stay silent", %{shard: id} do
    for outcome <- [{:ok, :forked}, {:error, :template_shard}, {:error, :dst_exists}] do
      assert capture_log(fn -> assert :ok = Shards.report_fork(outcome, id) end) == ""
      refute_received {:fork_event, _, _}
    end
  end

  test "an unrecognised shape still pages, so a new return value is never silently benign",
       %{shard: id} do
    log = capture_log(fn -> assert :ok = Shards.report_fork(:something_new, id) end)
    assert log =~ "born EMPTY"
    assert_receive {:fork_event, [:fathom, :migrator, :fork_fallback], %{reason: :unknown}}
  end
end
