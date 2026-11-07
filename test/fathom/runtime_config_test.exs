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
  defp fathom_config(env_overlay, config_env \\ :test) do
    prev = Map.new(env_overlay, fn {k, _} -> {k, System.get_env(k)} end)

    Enum.each(env_overlay, fn
      {k, nil} -> System.delete_env(k)
      {k, v} -> System.put_env(k, v)
    end)

    try do
      "config/runtime.exs"
      |> Config.Reader.read!(env: config_env)
      |> Keyword.get(:fathom, [])
    after
      Enum.each(prev, fn
        {k, nil} -> System.delete_env(k)
        {k, v} -> System.put_env(k, v)
      end)
    end
  end

  # The same script evaluated as PROD, with the variables the prod block refuses to boot without.
  # Values are dummies: Config.Reader only evaluates the script, nothing connects to them.
  defp prod_config(env_overlay) do
    %{
      "DATABASE_URL" => "ecto://u:p@localhost/fathom_runtime_config_test",
      "SECRET_KEY_BASE" => String.duplicate("k", 64),
      "ADMIN_USER" => "admin",
      "ADMIN_PASS" => "admin",
      # Required explicitly in prod (expert review 2026-10-10 #21); either value boots.
      "HRANA_AUTH" => "disabled"
    }
    |> Map.merge(env_overlay)
    |> fathom_config(:prod)
  end

  # --- Expert review 2026-10-08 #8: per-tenant stream cap + node-wide page-cache bound -----------
  #
  # Both knobs carry a PROD-ONLY default, so dev/test (and a stranger embedding the library) keep
  # the unlimited / untouched behaviour while a deployed release is bounded. `0` must stay the
  # documented way back to unlimited in BOTH envs — `env_int` would read it as "unset" and silently
  # keep the prod default, which is the trap `env_nonneg_int` exists for.
  @heap_8gib 8 * 1024 * 1024 * 1024

  test "MAX_CHECKOUTS_PER_SHARD defaults to 256 in prod; 0 restores unlimited; N overrides" do
    assert Keyword.fetch!(
             prod_config(%{"MAX_CHECKOUTS_PER_SHARD" => nil}),
             :max_checkouts_per_shard
           ) ==
             256

    assert Keyword.fetch!(
             prod_config(%{"MAX_CHECKOUTS_PER_SHARD" => "0"}),
             :max_checkouts_per_shard
           ) ==
             nil

    assert Keyword.fetch!(
             prod_config(%{"MAX_CHECKOUTS_PER_SHARD" => "16"}),
             :max_checkouts_per_shard
           ) ==
             16
  end

  test "MAX_CHECKOUTS_PER_SHARD outside prod: unset leaves the key unwritten (unlimited)" do
    refute Keyword.has_key?(
             fathom_config(%{"MAX_CHECKOUTS_PER_SHARD" => nil}),
             :max_checkouts_per_shard
           )

    assert Keyword.fetch!(
             fathom_config(%{"MAX_CHECKOUTS_PER_SHARD" => "0"}),
             :max_checkouts_per_shard
           ) == nil

    assert Keyword.fetch!(
             fathom_config(%{"MAX_CHECKOUTS_PER_SHARD" => "8"}),
             :max_checkouts_per_shard
           ) == 8
  end

  test "SHARD_SOFT_HEAP_LIMIT_BYTES defaults to 8 GiB in prod; 0 turns it off; N overrides" do
    assert Keyword.fetch!(
             prod_config(%{"SHARD_SOFT_HEAP_LIMIT_BYTES" => nil}),
             :shard_soft_heap_limit_bytes
           ) == @heap_8gib

    assert Keyword.fetch!(
             prod_config(%{"SHARD_SOFT_HEAP_LIMIT_BYTES" => "0"}),
             :shard_soft_heap_limit_bytes
           ) == nil

    assert Keyword.fetch!(
             prod_config(%{"SHARD_SOFT_HEAP_LIMIT_BYTES" => "1073741824"}),
             :shard_soft_heap_limit_bytes
           ) == 1_073_741_824
  end

  test "SHARD_SOFT_HEAP_LIMIT_BYTES outside prod: unset leaves the key unwritten (no limit)" do
    refute Keyword.has_key?(
             fathom_config(%{"SHARD_SOFT_HEAP_LIMIT_BYTES" => nil}),
             :shard_soft_heap_limit_bytes
           )

    assert Keyword.fetch!(
             fathom_config(%{"SHARD_SOFT_HEAP_LIMIT_BYTES" => "0"}),
             :shard_soft_heap_limit_bytes
           ) == nil

    assert Keyword.fetch!(
             fathom_config(%{"SHARD_SOFT_HEAP_LIMIT_BYTES" => "67108864"}),
             :shard_soft_heap_limit_bytes
           ) == 67_108_864
  end

  # Expert review 2026-10-20-2027 #7: unset leaves the code default (256 KiB) in every env.
  test "STMT_CACHE_BYTES: unset leaves the default; 0 turns the byte bound off; N overrides" do
    refute Keyword.has_key?(prod_config(%{"STMT_CACHE_BYTES" => nil}), :stmt_cache_bytes)
    refute Keyword.has_key?(fathom_config(%{"STMT_CACHE_BYTES" => nil}), :stmt_cache_bytes)

    assert Keyword.fetch!(prod_config(%{"STMT_CACHE_BYTES" => "0"}), :stmt_cache_bytes) == nil

    assert Keyword.fetch!(fathom_config(%{"STMT_CACHE_BYTES" => "1048576"}), :stmt_cache_bytes) ==
             1_048_576
  end

  test "SHARD_LOAD=true|1 turns on :shard_load (the deployed-node enable knob)" do
    assert Keyword.get(fathom_config(%{"SHARD_LOAD" => "true"}), :shard_load) == true
    assert Keyword.get(fathom_config(%{"SHARD_LOAD" => "1"}), :shard_load) == true
  end

  test "SHARD_LOAD unset outside prod leaves :shard_load off (module default)" do
    # Unset: the key isn't written, so Application falls back to the false default.
    refute Keyword.has_key?(fathom_config(%{}), :shard_load)
  end

  # 2026-10-15: :shard_load defaults ON in prod (the admin dashboard reads it). The env var is
  # tri-state so an operator can still turn it off on a prod node: `false|0` writes false, not
  # "unset", which would silently fall back to the prod default.
  test "SHARD_LOAD defaults ON in prod; false|0 turns it off there; true|1 is honoured" do
    assert Keyword.get(prod_config(%{"SHARD_LOAD" => nil}), :shard_load) == true
    assert Keyword.get(prod_config(%{"SHARD_LOAD" => "true"}), :shard_load) == true
    assert Keyword.get(prod_config(%{"SHARD_LOAD" => "1"}), :shard_load) == true
    assert Keyword.get(prod_config(%{"SHARD_LOAD" => "false"}), :shard_load) == false
    assert Keyword.get(prod_config(%{"SHARD_LOAD" => "0"}), :shard_load) == false
  end

  test "SHARD_LOAD=false|0 outside prod writes false (explicit off, not unset)" do
    assert Keyword.get(fathom_config(%{"SHARD_LOAD" => "false"}), :shard_load) == false
    assert Keyword.get(fathom_config(%{"SHARD_LOAD" => "0"}), :shard_load) == false
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

    # `env_int` rejects zero rather than writing it: `0` would be a sample size that runs the cron
    # to do nothing, which looks configured and is not.
    refute Keyword.has_key?(
             fathom_config(%{"RESTORE_DRILL_SAMPLE" => "0"}),
             :restore_drill_sample
           )

    # Garbage used to be silently ignored (and "12abc" truncated to 12). This test previously
    # asserted the silent drop; since expert review 2026-11-06 #8 a malformed integer fails the
    # boot, naming the variable.
    assert_raise RuntimeError, ~r/RESTORE_DRILL_SAMPLE must be an integer, got: "lots"/, fn ->
      fathom_config(%{"RESTORE_DRILL_SAMPLE" => "lots"})
    end
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
    # `Application.get_env(_, _, true)` default is supposed to own. An EMPTY value is unset too
    # (compose passes `X: "${X:-}"` for an unset variable).
    #
    # This test used to assert that garbage ("yes") was ALSO treated as unset, "so a typo'd value
    # must not be read as off". That pinned the defect (expert review 2026-11-07 #81): the typo was
    # silently ignored, so `REPLICATION_ENABLED=no` left replication ON and nothing said so. An
    # unrecognised value now refuses to boot — see "env_bool is strict" below.
    cfg = fathom_config(%{})
    unset = for {_, key} <- @default_on_gates, do: refute(Keyword.has_key?(cfg, key))
    assert length(unset) == 3

    for {var, key} <- @default_on_gates do
      refute Keyword.has_key?(fathom_config(%{var => ""}), key),
             "#{var}= (empty) should be treated as unset"
    end
  end

  # Expert review 2026-11-07 #81 (2). Symptom: `env_bool` mapped any unrecognised string to `nil`
  # ("say nothing"), so `REPLICATION_FSYNC=yes`, `TENANT_NOMUTEX=on`, `ALLOW_UNANCHORED_ROUTING=ye`
  # silently left a gate OFF, and `REPLICATION_ENABLED=no` left a defaults-ON gate ON. Invariant: an
  # env_bool variable is true|false|1|0 (case-insensitive) or empty/unset; anything else fails the
  # boot NAMING the variable. Covers a default-off, a default-on and a prod-only reader.
  describe "env_bool is strict (#81)" do
    @bool_vars [
      "REPLICATION_FSYNC",
      "TENANT_NOMUTEX",
      "ALLOW_UNANCHORED_ROUTING",
      "REPLICATION_ENABLED",
      "REPLICATION_PROMOTE_ON_OPEN",
      "ALLOW_NO_UDF",
      "ALLOW_LOCAL_STORAGE",
      "DRAIN_ON_SHUTDOWN"
    ]

    test "an unrecognised value refuses to boot, naming the variable" do
      for var <- @bool_vars, bad <- ["yes", "on", "no", "ye", "2", "tru"] do
        # prod_config, not fathom_config: several of these are read only inside the prod block.
        assert_raise RuntimeError, ~r/#{var} must be true, false, 1 or 0/, fn ->
          prod_config(%{var => bad})
        end
      end
    end

    test "case and surrounding whitespace are accepted; empty is unset" do
      assert Keyword.fetch!(
               fathom_config(%{"REPLICATION_ENABLED" => "False"}),
               :replication_enabled
             ) ==
               false

      assert Keyword.fetch!(
               fathom_config(%{"REPLICATION_ENABLED" => " TRUE "}),
               :replication_enabled
             ) ==
               true

      refute Keyword.has_key?(fathom_config(%{"REPLICATION_ENABLED" => ""}), :replication_enabled)
    end
  end

  # Expert review 2026-11-07 #81 (3). Symptom: `MAX_OPEN_SHARDS=0` was stored as 0, so `at_capacity?`
  # was always true and every new shard open was `:node_at_capacity` (a node that boots healthy and
  # serves only what it already held). Invariant: the cap is positive or the boot fails.
  test "MAX_OPEN_SHARDS must be positive in prod" do
    for bad <- ["0", "-5"] do
      assert_raise RuntimeError, ~r/MAX_OPEN_SHARDS must be a positive integer/, fn ->
        prod_config(%{"MAX_OPEN_SHARDS" => bad})
      end
    end

    assert Keyword.fetch!(prod_config(%{"MAX_OPEN_SHARDS" => "500"}), :max_open_shards) == 500
  end

  # Expert review 2026-11-07 #81 (4). Symptom: no bounds, so `SHARD_LEASE_TTL_MS=30` (seconds
  # meant) gave a ~10 ms write-validity margin and every write `:not_valid`, and `300000` a
  # five-minute failover stall. Invariant: [5 s, 10 min] or the boot fails, naming the variable.
  test "SHARD_LEASE_TTL_MS is bounded to 5 s .. 10 min" do
    for bad <- ["30", "4999", "600001", "0", "-1"] do
      assert_raise RuntimeError, ~r/SHARD_LEASE_TTL_MS must be between 5000 and 600000/, fn ->
        fathom_config(%{"SHARD_LEASE_TTL_MS" => bad})
      end
    end

    for good <- [5_000, 10_000, 30_000, 600_000] do
      assert Keyword.fetch!(
               fathom_config(%{"SHARD_LEASE_TTL_MS" => "#{good}"}),
               :shard_lease_ttl_ms
             ) == good
    end
  end

  # Expert review 2026-11-07 #81 (5). Symptom: node keys `fathom-1` and `fathom_1` render the same
  # nginx `upstream` name, so `nginx -t` failed every tick and no pin could be applied or removed.
  test "LB_BACKENDS keys that collide after nginx-name sanitising refuse to boot" do
    assert_raise RuntimeError, ~r/same nginx upstream name/, fn ->
      fathom_config(%{"LB_BACKENDS" => "fathom-1=a:8080,fathom_1=b:8080"})
    end

    cfg = fathom_config(%{"LB_BACKENDS" => "fathom1=a:8080,fathom2=b:8080"})
    assert Keyword.fetch!(cfg, :lb_backends) == %{"fathom1" => "a:8080", "fathom2" => "b:8080"}
  end

  # Expert review 2026-10-10 #21 (SYMPTOM): prod defaulted HRANA_AUTH to "disabled", so a deploy
  # that never mentioned it silently served an unauthenticated data plane while replication was
  # fail-closed. Invariant: in prod the variable must be SET (either value boots); unset refuses.
  describe "HRANA_AUTH must be explicit in prod (#21)" do
    test "unset refuses to boot in prod" do
      assert_raise RuntimeError, ~r/HRANA_AUTH is missing/, fn ->
        prod_config(%{"HRANA_AUTH" => nil})
      end
    end

    test "either explicit value boots and is honoured" do
      assert Keyword.fetch!(prod_config(%{"HRANA_AUTH" => "disabled"}), :hrana_auth) == :disabled

      assert Keyword.fetch!(prod_config(%{"HRANA_AUTH" => "required"}), :hrana_auth) == :required
    end

    test "dev/test keep the unauthenticated default without the variable" do
      refute Keyword.has_key?(fathom_config(%{"HRANA_AUTH" => nil}), :hrana_auth)
    end
  end

  # Expert review 2026-10-12 #32 (SYMPTOM): the outage posture :hrana_revocation_on_error had no
  # env knob, so an operator could only change it by editing code. Invariant: the variable selects
  # the posture when set, unset leaves the module default (:fail_open, decided 2026-10-10 #7), and
  # an unknown value fails the boot rather than silently picking one.
  describe "HRANA_REVOCATION_ON_ERROR (#32)" do
    test "fail_open and fail_closed are written as the posture" do
      assert Keyword.fetch!(
               prod_config(%{"HRANA_REVOCATION_ON_ERROR" => "fail_open"}),
               :hrana_revocation_on_error
             ) == :fail_open

      assert Keyword.fetch!(
               prod_config(%{"HRANA_REVOCATION_ON_ERROR" => "fail_closed"}),
               :hrana_revocation_on_error
             ) == :fail_closed
    end

    test "unset leaves the key unwritten so the fail_open default stands" do
      refute Keyword.has_key?(
               prod_config(%{"HRANA_REVOCATION_ON_ERROR" => nil}),
               :hrana_revocation_on_error
             )
    end

    test "an unknown value fails the boot, naming the variable" do
      assert_raise RuntimeError, ~r/HRANA_REVOCATION_ON_ERROR must be/, fn ->
        prod_config(%{"HRANA_REVOCATION_ON_ERROR" => "fail_sideways"})
      end
    end
  end

  # --- Expert review 2026-10-12 #33: JOURNAL_SIZE_LIMIT parsing -----------------------------------
  # `String.to_integer/1` turned a typo into an ArgumentError with no mention of the variable. The
  # reading code (`maybe_journal_size_limit/1`) silently ignores a non-integer, so a bad value that
  # got past boot would mean "no limit" with no signal. Boot must name the variable instead.
  test "JOURNAL_SIZE_LIMIT accepts an integer (including 0 and -1)" do
    for {raw, n} <- [{"0", 0}, {"-1", -1}, {"1048576", 1_048_576}, {" 7 ", 7}] do
      assert Keyword.fetch!(fathom_config(%{"JOURNAL_SIZE_LIMIT" => raw}), :journal_size_limit) ==
               n
    end
  end

  test "JOURNAL_SIZE_LIMIT unset leaves the Connection default (64 MiB) in force" do
    refute Keyword.has_key?(fathom_config(%{"JOURNAL_SIZE_LIMIT" => nil}), :journal_size_limit)
  end

  # Expert review 2026-10-21 #11 (Q10 option A): the prod POOL_SIZE default follows Oban's queue
  # sum instead of a fixed 25, which the queues had grown to equal.
  describe "POOL_SIZE default (#11)" do
    defp repo_pool(overlay), do: prod_config(overlay) |> Keyword.fetch!(Fathom.Repo)

    test "unset: the sum of Oban's queue limits + 10 (35 with the shipped queues)" do
      assert Keyword.fetch!(repo_pool(%{"POOL_SIZE" => nil}), :pool_size) == 35
    end

    test "unset: it follows the queues, so a queue added later raises it" do
      prev = Application.get_env(:fathom, Oban)
      on_exit(fn -> Application.put_env(:fathom, Oban, prev) end)

      Application.put_env(
        :fathom,
        Oban,
        Keyword.put(prev, :queues, a: 30, b: [limit: 4, paused: true], c: false)
      )

      assert Keyword.fetch!(repo_pool(%{"POOL_SIZE" => nil}), :pool_size) == 44
    end

    test "an explicit POOL_SIZE still wins" do
      assert Keyword.fetch!(repo_pool(%{"POOL_SIZE" => "60"}), :pool_size) == 60
    end
  end

  test "a non-integer JOURNAL_SIZE_LIMIT is a clear boot error naming the variable" do
    for bad <- ["64MiB", "1.5", "", "abc"] do
      error =
        assert_raise RuntimeError, fn -> fathom_config(%{"JOURNAL_SIZE_LIMIT" => bad}) end

      assert error.message =~ "JOURNAL_SIZE_LIMIT must be an integer"
    end
  end

  # Review of bef51a2a (2026-10-31). Symptom: both were read with `String.to_integer/1`, so garbage
  # crashed the boot with a bare ArgumentError and a NEGATIVE was accepted, failing only later, per
  # shard, when the coordinator armed its grace timer. They now refuse to boot with a message naming
  # the variable. 0 stays a value (drain on close / shrink off).
  describe "POOL_DRAIN_GRACE_MS and POOL_SHRINK_IDLE_MS" do
    for {var, key} <- [
          {"POOL_DRAIN_GRACE_MS", :drain_grace_ms},
          {"POOL_SHRINK_IDLE_MS", :shrink_idle_ms}
        ] do
      test "#{var}: N and 0 are values" do
        for v <- ["1500", "0"] do
          opts = Keyword.fetch!(fathom_config(%{unquote(var) => v}), :connection_pool_opts)
          assert Keyword.fetch!(opts, unquote(key)) == String.to_integer(v)
        end
      end

      test "#{var}: a negative or non-integer value refuses to boot, naming the variable" do
        for v <- ["-1", "2s", "abc", ""] do
          assert_raise RuntimeError, ~r/#{unquote(var)} must be a non-negative integer/, fn ->
            fathom_config(%{unquote(var) => v})
          end
        end
      end
    end
  end

  # Expert review 2026-11-07 #8: the ack for a prod Local-store node that replicates must be
  # reachable from the environment of a release (it is read by `Application.check_local_storage_fleet!`).
  test "ALLOW_LOCAL_STORAGE=true sets :allow_local_storage; unset leaves it unwritten" do
    assert Keyword.fetch!(fathom_config(%{"ALLOW_LOCAL_STORAGE" => "true"}), :allow_local_storage)
    refute Keyword.has_key?(fathom_config(%{"ALLOW_LOCAL_STORAGE" => nil}), :allow_local_storage)
  end

  # Expert review 2026-11-07 #58. Symptom: the runbooks told operators to set `:drain_lb_settle_ms`,
  # `:shard_shutdown_ms`, `:drain_all_budget_ms` ..., but each was read only through
  # `Application.get_env` — no env var mapped to any, and a release cannot edit `config/*.exs`, so on
  # a deployed node none could be set (and #46's `:drain_lb_settle_ms` mitigation was unreachable).
  # Invariant: each is reachable from the environment; unset leaves the key unwritten so the reading
  # module's own default stands.
  describe "node shutdown / drain knobs (#58)" do
    for {var, key, value} <- [
          {"DRAIN_LB_SETTLE_MS", :drain_lb_settle_ms, 1500},
          {"DRAIN_ALL_BUDGET_MS", :drain_all_budget_ms, 120_000},
          {"DRAIN_ALL_CONCURRENCY", :drain_all_concurrency, 32},
          {"DRAIN_ALL_FLUSH_GRACE_MS", :drain_all_flush_grace_ms, 8000},
          {"SHARD_SHUTDOWN_MS", :shard_shutdown_ms, 90_000}
        ] do
      test "#{var} sets #{inspect(key)}; unset leaves it unwritten" do
        assert Keyword.fetch!(fathom_config(%{unquote(var) => "#{unquote(value)}"}), unquote(key)) ==
                 unquote(value)

        refute Keyword.has_key?(fathom_config(%{unquote(var) => nil}), unquote(key))
      end

      test "#{var}: a non-integer value refuses to boot, naming the variable" do
        assert_raise RuntimeError, ~r/#{unquote(var)} must be an integer/, fn ->
          fathom_config(%{unquote(var) => "30s"})
        end
      end
    end

    test "0 is a VALUE for the settle sleep and the flush grace (env_nonneg_int, not env_int)" do
      for {var, key} <- [
            {"DRAIN_LB_SETTLE_MS", :drain_lb_settle_ms},
            {"DRAIN_ALL_FLUSH_GRACE_MS", :drain_all_flush_grace_ms}
          ] do
        assert Keyword.fetch!(fathom_config(%{var => "0"}), key) == 0
      end
    end

    test "DRAIN_ON_SHUTDOWN is tri-state: false|0 turns the prep_stop drain off, true on, unset says nothing" do
      for v <- ["false", "0"],
          do:
            assert(
              Keyword.fetch!(fathom_config(%{"DRAIN_ON_SHUTDOWN" => v}), :drain_on_shutdown) ==
                false
            )

      assert Keyword.fetch!(fathom_config(%{"DRAIN_ON_SHUTDOWN" => "true"}), :drain_on_shutdown) ==
               true

      refute Keyword.has_key?(fathom_config(%{"DRAIN_ON_SHUTDOWN" => nil}), :drain_on_shutdown)
    end
  end
end
