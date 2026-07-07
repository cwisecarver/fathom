defmodule Fathom.RuntimeConfigTest do
  @moduledoc """
  Runtime env parsing in `config/runtime.exs`. Evaluates the real runtime config
  script under `:test` env (so the prod-only block is skipped) with a given env
  overlay, and asserts the `:fathom` config it produces. Not async — it mutates the
  process environment.
  """
  use ExUnit.Case, async: false

  # Evaluate config/runtime.exs in :test env with env_overlay applied, return the
  # :fathom keyword list, then restore the environment.
  defp fathom_config(env_overlay) do
    prev = Map.new(env_overlay, fn {k, _} -> {k, System.get_env(k)} end)
    Enum.each(env_overlay, fn {k, v} -> System.put_env(k, v) end)

    try do
      "config/runtime.exs"
      |> Config.Reader.read!(env: :test)
      |> Keyword.get(:fathom, [])
    after
      Enum.each(prev, fn
        {k, nil} -> System.delete_env(k)
        {k, v} -> System.put_env(k, v)
      end)
    end
  end

  test "SHARD_LOAD=true|1 turns on :shard_load (the deployed-node enable knob)" do
    assert Keyword.get(fathom_config(%{"SHARD_LOAD" => "true"}), :shard_load) == true
    assert Keyword.get(fathom_config(%{"SHARD_LOAD" => "1"}), :shard_load) == true
  end

  test "SHARD_LOAD unset/false/0 leaves :shard_load off (default)" do
    # Unset: the key isn't written, so Application falls back to the false default.
    refute Keyword.has_key?(fathom_config(%{}), :shard_load)
    refute Keyword.has_key?(fathom_config(%{"SHARD_LOAD" => "false"}), :shard_load)
    refute Keyword.has_key?(fathom_config(%{"SHARD_LOAD" => "0"}), :shard_load)
  end

  test "rebalancer env: node_key, gates, and the hot floor" do
    cfg =
      fathom_config(%{
        "NODE_KEY" => "fathom2",
        "LOAD_REPORTER" => "true",
        "COMMAND_POLLER" => "1",
        "REBALANCER_ENABLED" => "true",
        "LB_MAP_PATH" => "/etc/nginx/lb/exceptions.conf",
        "REBALANCE_HOT_QPS_FLOOR" => "500.0"
      })

    assert Keyword.get(cfg, :node_key) == "fathom2"
    assert Keyword.get(cfg, :load_reporter) == true
    assert Keyword.get(cfg, :command_poller) == true
    assert Keyword.get(cfg, :rebalancer_enabled) == true
    assert Keyword.get(cfg, :lb_map_path) == "/etc/nginx/lb/exceptions.conf"
    assert Keyword.get(cfg, :rebalance_hot_qps_floor) == 500.0
  end

  test "LB_BACKENDS parses node_key=address pairs into a map" do
    cfg = fathom_config(%{"LB_BACKENDS" => "fathom1=fathom1:8080, fathom2=fathom2:8080"})

    assert Keyword.get(cfg, :lb_backends) == %{
             "fathom1" => "fathom1:8080",
             "fathom2" => "fathom2:8080"
           }
  end
end
