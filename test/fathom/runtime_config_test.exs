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

  # The migration engine's entry point (`Fathom.Migrator.Capture`) only fires for the shard named
  # by :template_shard_id, and that key had NO env wiring — it was set only in config/dev.exs, so a
  # RELEASE could not turn capture on at all without editing config and rebuilding. Worse, the
  # `mix fathom.snapshot template-head` error message already told operators to "set
  # TEMPLATE_SHARD_ID", a variable nothing read. Pre-fix these first two tests fail: the key is
  # never written.
  test "TEMPLATE_SHARD_ID sets :template_shard_id (the release-reachable capture knob)" do
    assert Keyword.get(fathom_config(%{"TEMPLATE_SHARD_ID" => "keystone"}), :template_shard_id) ==
             "keystone"
  end

  test "TEMPLATE_SHARD_ID is cast, so a mixed-case value normalizes (finding #19)" do
    # Must be the CANONICAL id: Fathom.ShardExecutor compares the request's normalized shard id
    # against this value, so an un-normalized "KEYSTONE" here would leave capture silently off
    # while the template looked configured.
    assert Keyword.get(fathom_config(%{"TEMPLATE_SHARD_ID" => "KEYSTONE"}), :template_shard_id) ==
             "keystone"
  end

  test "TEMPLATE_SHARD_ID unset leaves the key unwritten (compiled default survives)" do
    refute Keyword.has_key?(fathom_config(%{}), :template_shard_id)
  end

  test "TEMPLATE_SHARD_ID that ShardId rejects fails the boot instead of disabling capture" do
    for bad <- ["bad/id", "has space", "dotted.id", ""] do
      assert_raise RuntimeError, ~r/TEMPLATE_SHARD_ID is not a valid shard id/, fn ->
        fathom_config(%{"TEMPLATE_SHARD_ID" => bad})
      end
    end
  end

  # The read-only restore drill's sample size (expert review 2026-08-24 #25). Same hole
  # TEMPLATE_SHARD_ID and REPLICATION_LINEAGE_WIRE each had — `:restore_drill_sample` was settable
  # only from a config file, so a RELEASE could not turn the drill on — and here it was load-bearing
  # twice over: the drill is the ONLY thing that populates `stamp_drift` / `stamp_drift_checked` in
  # `Migrator.status/0`, so the signal read a permanent zero on every deployed node. Pre-fix the
  # first assertion fails: the key is never written.
  test "RESTORE_DRILL_SAMPLE sets :restore_drill_sample (the release-reachable drill knob)" do
    assert Keyword.get(fathom_config(%{"RESTORE_DRILL_SAMPLE" => "25"}), :restore_drill_sample) ==
             25

    # Unset leaves it unwritten, so the compiled default (off) stands — the drill costs a GET per
    # sample and must stay opt-in.
    refute Keyword.has_key?(fathom_config(%{}), :restore_drill_sample)

    # `env_int` rejects zero and garbage rather than writing them: `0` would be a sample size that
    # runs the cron to do nothing, which looks configured and is not.
    refute Keyword.has_key?(
             fathom_config(%{"RESTORE_DRILL_SAMPLE" => "0"}),
             :restore_drill_sample
           )

    refute Keyword.has_key?(
             fathom_config(%{"RESTORE_DRILL_SAMPLE" => "lots"}),
             :restore_drill_sample
           )
  end

  # The three A2 gates that default ON (2026-08-25). They read through `env_bool`, which is
  # TRI-STATE, and that is the whole point of these tests.
  #
  # `:replication_lineage_wire` is also the reason the helper exists at all. It had the same hole
  # TEMPLATE_SHARD_ID above did — no runtime.exs entry, so nothing but a config file or
  # `Application.put_env` could set it, and expert review 2026-08-24 #12's fix shipped correct and
  # UNDEPLOYABLE. Wiring it with the usual two-state `in ~w(true 1)` form fixed that and immediately
  # created the opposite hole the moment the default flipped: a knob that can only be turned ON is
  # not a flag once it starts ON, it is a hardcode.
  @default_on_gates [
    {"REPLICATION_LINEAGE_WIRE", :replication_lineage_wire},
    {"REPLICATION_PROMOTE_ON_OPEN", :replication_promote_on_open},
    {"REPLICATION_RECOVER_FROM_PEERS", :replication_recover_from_peers}
  ]

  test "the default-on A2 gates write true for true|1" do
    for {var, key} <- @default_on_gates, v <- ["true", "1"] do
      assert Keyword.get(fathom_config(%{var => v}), key) == true,
             "#{var}=#{v} did not write #{inspect(key)} true"
    end
  end

  test "the default-on A2 gates write FALSE for false|0 — the off switch a flipped default needs" do
    # Pre-`env_bool` this is the failing half: the two-state form ignored "false" entirely, leaving
    # the key unwritten and the compiled default (now `true`) in force. An operator rolling back
    # across the commit that introduced the lineage frame — the one case that genuinely wants the
    # legacy wire shape — would have had no way to ask for it short of editing config and
    # rebuilding.
    for {var, key} <- @default_on_gates, v <- ["false", "0"] do
      assert Keyword.get(fathom_config(%{var => v}), key) == false,
             "#{var}=#{v} did not write #{inspect(key)} false"
    end
  end

  test "the default-on A2 gates leave the key UNWRITTEN when unset, so the module default stands" do
    # Not `== true`: runtime.exs must say nothing, or it would pin a value that the module's own
    # `Application.get_env(_, _, true)` default is supposed to own. Garbage is treated as unset for
    # the same reason — a typo'd "yes" must not be read as "off" on a gate whose default is on.
    cfg = fathom_config(%{})
    unset = for {_, key} <- @default_on_gates, do: refute(Keyword.has_key?(cfg, key))
    assert length(unset) == 3

    for {var, key} <- @default_on_gates do
      refute Keyword.has_key?(fathom_config(%{var => "yes"}), key),
             "#{var}=yes should be treated as unset, not as off"
    end
  end
end
