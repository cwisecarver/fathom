defmodule Fathom.Rebalancer.LbApply do
  @moduledoc """
  Applies the exception table to the load balancer: render the current overrides
  (`LbMap.current/0`), write them to `:lb_map_path` (the file the LB `include`s), and run
  the optional `:lb_reload_cmd` to make nginx pick it up.

  The write is the source-of-truth → config projection; the reload is deployment-specific
  (prod: `nginx -s reload`; the rig: a docker HUP — see `deploy/chaos`). With `:lb_map_path`
  unset it's a **no-op** (decision-plane-only / tests), so the override table can be
  maintained without any LB wiring.
  """
  require Logger

  alias Fathom.Rebalancer.LbMap

  @doc "Renders + writes the map and reloads the LB. Returns :ok (best-effort reload)."
  @spec apply!() :: :ok
  def apply! do
    case Application.get_env(:fathom, :lb_map_path) do
      nil ->
        Logger.debug("rebalancer: :lb_map_path unset — LB map not written (decision-plane only)")
        :ok

      path ->
        File.write!(path, LbMap.current())
        reload()
        :ok
    end
  end

  defp reload do
    case Application.get_env(:fathom, :lb_reload_cmd) do
      nil ->
        :ok

      cmd ->
        {out, code} = System.shell(cmd, stderr_to_stdout: true)

        if code != 0,
          do: Logger.warning("rebalancer: LB reload `#{cmd}` exited #{code}: #{String.trim(out)}")

        :ok
    end
  rescue
    e -> Logger.warning("rebalancer: LB reload raised: #{Exception.message(e)}")
  end
end
