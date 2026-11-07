defmodule Fathom.Telemetry.Poll do
  @moduledoc """
  The wrapper every `:telemetry_poller` measurement is registered through (expert review 2026-11-07
  #77).

  `:telemetry_poller` keeps only the measurements that did NOT raise or exit on a tick
  (`make_measurements_and_filter_misbehaving`): one failure drops the measurement for the life of the
  poller process (probed: a measurement that `exit`s once on tick 1 is called once, the others every
  tick). The one raise that matters in practice is `Fathom.Shards.open_count/0` exiting `:noproc`
  while the data plane restarts, which silently removed `fathom.shards.active` — the gauge density
  dashboards read — until the node rebooted.

  `run/3` calls the measurement and turns any raise, throw or exit into `:ok`, so the poller keeps
  it, plus a `[:fathom, :poller, :measurement_failed]` count tagged with the measurement and one
  rate-limited warning line, so a persistently broken measurement stays visible instead of silently
  absent.
  """
  require Logger

  @spec run(module(), atom(), [term()]) :: :ok
  def run(module, fun, args) do
    apply(module, fun, args)
    :ok
  rescue
    error -> failed(module, fun, Exception.format_banner(:error, error))
  catch
    kind, reason -> failed(module, fun, Exception.format_banner(kind, reason))
  end

  @doc """
  The `{module, function, args}` list `:telemetry_poller` takes, each entry routed through `run/3`.
  """
  @spec wrap([{module(), atom(), [term()]}]) :: [{module(), atom(), [term()]}]
  def wrap(measurements) do
    for {module, fun, args} <- measurements, do: {__MODULE__, :run, [module, fun, args]}
  end

  defp failed(module, fun, reason) do
    name = "#{inspect(module)}.#{fun}"

    :telemetry.execute([:fathom, :poller, :measurement_failed], %{count: 1}, %{measurement: name})

    case Fathom.LogLimiter.check(:poller_measurement_failed, 1, 60_000) do
      :suppress ->
        :ok

      _log ->
        Logger.warning(
          "telemetry poller measurement #{name} failed: #{reason}. It is kept and retried every " <>
            "tick; fathom.poller.measurement_failed.count counts the failures. (Logged once per minute.)"
        )
    end

    :ok
  end
end
