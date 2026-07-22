defmodule Fathom.Tenants.DenyListBootRetryTest do
  @moduledoc """
  Expert review #33: the node-local lifecycle deny sets (`Fathom.Tenants.Tombstones` for
  delete/410, `Fathom.Tenants.Suspensions` for suspend/403) load from the Postgres directory
  at boot and best-effort-rescue a failure to an empty ETS table. Before this fix a FAILED
  boot load was treated identically to a successful one — the process then waited the FULL
  5-min refresh interval before retrying, leaving the deny set empty for up to 5 minutes on
  that node during a Postgres wobble coincident with the restart (a deleted tenant re-mints,
  a suspended one serves).

  These tests start an ISOLATED instance (its own name/table, an injectable loader that raises
  until flipped, a short `:retry_ms`) and assert the deny gate: (1) is empty + emits
  `[:fathom, :tenants, :denylist, :degraded]` on the failed boot load, then (2) fast-retries
  and CONVERGES within seconds once the loader recovers, emitting the paired `:recovered`. The
  app singletons are untouched (they run under module-default name/table). Not async: the
  telemetry attach + ETS names are process-global.
  """
  use ExUnit.Case, async: false

  alias Fathom.Tenants.{Suspensions, Tombstones}

  setup do
    test = self()
    handler = "denylist-boot-retry-#{System.unique_integer([:positive])}"

    :telemetry.attach_many(
      handler,
      [
        [:fathom, :tenants, :denylist, :degraded],
        [:fathom, :tenants, :denylist, :recovered]
      ],
      fn [:fathom, :tenants, :denylist, phase], _measure, meta, _ ->
        send(test, {:denylist, phase, meta.kind})
      end,
      nil
    )

    on_exit(fn -> :telemetry.detach(handler) end)
    :ok
  end

  # A directory loader that raises while `:down` (Postgres unreachable) and returns the ids
  # once flipped `:up` — models a wobble that clears.
  defp flaky_loader(agent) do
    fn ->
      case Agent.get(agent, & &1) do
        {:down, _ids} -> raise "directory unreachable"
        {:up, ids} -> ids
      end
    end
  end

  defp uniq_atom(prefix), do: String.to_atom("#{prefix}_#{System.unique_integer([:positive])}")

  defp start_isolated(mod, id_tag, opts) do
    name = uniq_atom("#{id_tag}_srv")
    table = uniq_atom("#{id_tag}_tab")

    start_supervised!(
      Supervisor.child_spec(
        {mod, Keyword.merge([name: name, table: table, retry_ms: 50], opts)},
        id: {id_tag, name}
      )
    )

    table
  end

  test "Tombstones: a failed boot load fast-retries and converges once the directory recovers" do
    id = "tomb_retry_#{System.unique_integer([:positive])}"
    agent = start_supervised!({Agent, fn -> {:down, [id]} end})

    table = start_isolated(Tombstones, :tomb_retry, loader: flaky_loader(agent))

    # The #33 window: the boot load failed, the re-mint guard is EMPTY, and it signalled degraded.
    assert_receive {:denylist, :degraded, :tombstones}, 1_000

    refute :ets.member(table, id),
           "a failed boot load leaves the gate empty (the exposure #33 flags)"

    # Postgres recovers.
    Agent.update(agent, fn {_, ids} -> {:up, ids} end)

    # The fast-retry lands the set within seconds (base 50ms) — not the 5-min refresh interval.
    assert_receive {:denylist, :recovered, :tombstones}, 2_000

    assert :ets.member(table, id),
           "the fast-retry must converge the tombstone (410) gate once Postgres recovers"
  end

  test "Suspensions: a failed boot reconcile fast-retries and converges once the directory recovers" do
    id = "susp_retry_#{System.unique_integer([:positive])}"
    agent = start_supervised!({Agent, fn -> {:down, [id]} end})

    table = start_isolated(Suspensions, :susp_retry, loader: flaky_loader(agent))

    assert_receive {:denylist, :degraded, :suspensions}, 1_000
    refute :ets.member(table, id), "a failed boot reconcile leaves the suspend gate empty"

    Agent.update(agent, fn {_, ids} -> {:up, ids} end)

    assert_receive {:denylist, :recovered, :suspensions}, 2_000

    assert :ets.member(table, id),
           "the fast-retry must converge the suspend (403) gate once Postgres recovers"
  end

  test "a successful boot load stays on the normal refresh cadence and never signals degraded" do
    id = "tomb_ok_#{System.unique_integer([:positive])}"

    table = start_isolated(Tombstones, :tomb_ok, loader: fn -> [id] end)

    # init loads synchronously, so the gate is populated the moment start_supervised! returns.
    assert :ets.member(table, id)
    refute_receive {:denylist, :degraded, _}, 200
  end
end
