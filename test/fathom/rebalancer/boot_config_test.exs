defmodule Fathom.Rebalancer.BootConfigTest do
  @moduledoc """
  Expert review 2026-11-07 #57: a prod node with `REBALANCER_ENABLED` and no `LB_MAP_PATH` cannot move
  anything (its handoffs are refused by `HandoffJob`), and the `rebalance` queue runs on every node, so
  the boot says so. A warning, not a refusal: decision-plane-only operation is documented.
  """
  use ExUnit.Case, async: false

  import ExUnit.CaptureLog

  alias Fathom.Test.Env

  defp check, do: capture_log(fn -> Fathom.Application.check_rebalancer_config!() end)

  test "prod + rebalancer on + no LB map: warns, and says what to set" do
    Env.put!(:env, :prod)
    Env.put!(:rebalancer_enabled, true)
    Env.put!(:lb_map_path, nil)

    log = check()
    assert log =~ "LB_MAP_PATH is unset"
    assert log =~ "REBALANCER_ENABLED"
  end

  test "quiet when the map is set, the rebalancer is off, or this is not prod" do
    Env.put!(:env, :prod)
    Env.put!(:rebalancer_enabled, true)
    Env.put!(:lb_map_path, "/etc/nginx/lb/exceptions.conf")
    refute check() =~ "LB_MAP_PATH"

    Env.put!(:lb_map_path, nil)
    Env.put!(:rebalancer_enabled, false)
    refute check() =~ "LB_MAP_PATH"

    Env.put!(:rebalancer_enabled, true)
    Env.put!(:env, :test)
    refute check() =~ "LB_MAP_PATH"
  end
end
