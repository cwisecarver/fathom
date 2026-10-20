defmodule Fathom.TelemetryPersistTest do
  # Expert review 2026-10-20 #34: `:telemetry.persist/0` was never called, so every hot
  # `:telemetry.execute` copied its handler records out of ETS (374 -> 99 ns per query event once
  # persisted). Persisting is process-global and permanent for the VM, and this suite attaches and
  # detaches handlers constantly (each would become a persistent_term update), so it runs in a
  # throwaway peer node and `config/test.exs` turns the boot-time call off for the main VM.
  use ExUnit.Case, async: true

  @moduletag :capture_log

  test "persist_handlers/0 moves the handler table to persistent_term, and attach/detach still work" do
    {:ok, peer, _node} =
      :peer.start_link(%{
        connection: :standard_io,
        args: Enum.flat_map(:code.get_path(), &[~c"-pa", &1])
      })

    on_exit(fn -> catch_exit(:peer.stop(peer)) end)

    call = fn m, f, a -> :peer.call(peer, m, f, a) end
    impl = fn -> call.(:persistent_term, :get, [:telemetry]) |> elem(0) end

    {:ok, _} = call.(Application, :ensure_all_started, [:telemetry])
    handler = &Fathom.Telemetry.handle_otel_event/4

    :ok = call.(:telemetry, :attach, ["boot-handler", [:persist, :probe], handler, %{}])
    assert impl.() == :telemetry_ets, "precondition: the table starts in ETS"

    assert :ok = call.(Fathom.Telemetry, :persist_handlers, [])
    assert impl.() == :telemetry_pt, "the handler table was not persisted"

    assert [%{id: "boot-handler"}] = call.(:telemetry, :list_handlers, [[:persist, :probe]])

    # Late attach/detach (reporter restart, Rpo, bench) must keep working after persisting.
    :ok = call.(:telemetry, :attach, ["late-handler", [:persist, :probe], handler, %{}])
    assert length(call.(:telemetry, :list_handlers, [[:persist, :probe]])) == 2
    :ok = call.(:telemetry, :detach, ["late-handler"])
    assert [%{id: "boot-handler"}] = call.(:telemetry, :list_handlers, [[:persist, :probe]])
  end

  test "persist_handlers/0 is a no-op when :telemetry_persist is off (the test env)" do
    refute Application.get_env(:fathom, :telemetry_persist)
    before = :persistent_term.get(:telemetry) |> elem(0)
    assert :ok = Fathom.Telemetry.persist_handlers()
    assert :persistent_term.get(:telemetry) |> elem(0) == before
  end
end
