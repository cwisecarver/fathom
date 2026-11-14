defmodule Fathom.BootSecurityGuardsTest do
  @moduledoc """
  Prod boot-time security guards from expert review 2026-10-12 (#12 throttle proxy posture, #27 S3
  session-token warning). Each guard is a plain function on `Fathom.Application`, so the tests call
  it directly with the config it reads set, the same way the template and frame-auth guards are
  tested. Not async: they mutate global application env.

  Also home to the older `Fathom.Application.check_*!` guard tests (template/default shard, NODE_KEY,
  pool fd budget, auth-disabled template, bind guards, flush-interval, ...), moved from
  shard_executor_test.exs in the 2026-11-13 cleanliness audit (#42) so that file models the executor
  binding only.
  """
  use ExUnit.Case, async: false

  import ExUnit.CaptureLog

  @s3 Fathom.Shard.Storage.S3

  setup do
    keys = [:env, :shard_storage, :trusted_proxies, :admin_auth_max_failures, :api_rate_limit]
    prev = Map.new(keys, &{&1, Application.get_env(:fathom, &1)})
    prev_s3 = Application.get_env(:fathom, @s3)

    on_exit(fn ->
      for {k, v} <- prev do
        if is_nil(v),
          do: Application.delete_env(:fathom, k),
          else: Application.put_env(:fathom, k, v)
      end

      if is_nil(prev_s3),
        do: Application.delete_env(:fathom, @s3),
        else: Application.put_env(:fathom, @s3, prev_s3)
    end)

    :ok
  end

  describe "#12 shared throttle bucket refuses to boot in prod" do
    setup do
      prev_ok = Application.get_env(:fathom, :admin_throttle_shared_bucket_ok)
      prev_proxies = Application.get_env(:fathom, :trusted_proxies)
      Application.put_env(:fathom, :env, :prod)
      Application.put_env(:fathom, :admin_auth_max_failures, 20)
      Application.delete_env(:fathom, :api_rate_limit)
      Application.delete_env(:fathom, :trusted_proxies)
      Application.delete_env(:fathom, :admin_throttle_shared_bucket_ok)

      on_exit(fn ->
        for {k, v} <- [
              admin_throttle_shared_bucket_ok: prev_ok,
              trusted_proxies: prev_proxies
            ] do
          if is_nil(v),
            do: Application.delete_env(:fathom, k),
            else: Application.put_env(:fathom, k, v)
        end
      end)

      :ok
    end

    test "throttles on (the prod default) with no trusted proxy refuses to boot" do
      assert_raise RuntimeError, ~r/shares one throttle bucket/, fn ->
        Fathom.Application.check_throttle_proxy!()
      end
    end

    test "the /api rate limit alone counts as a throttle" do
      Application.put_env(:fathom, :admin_auth_max_failures, nil)
      Application.put_env(:fathom, :api_rate_limit, 120)

      assert_raise RuntimeError, ~r/shares one throttle bucket/, fn ->
        Fathom.Application.check_throttle_proxy!()
      end
    end

    test "the explicit opt-out boots (a deployment with no proxy in front of :4000)" do
      Application.put_env(:fathom, :admin_throttle_shared_bucket_ok, true)
      assert Fathom.Application.check_throttle_proxy!() == nil
    end

    test "a trusted proxy satisfies the guard without the opt-out" do
      Application.put_env(:fathom, :trusted_proxies, ["10.0.0.0/8"])
      assert Fathom.Application.check_throttle_proxy!() == nil
    end

    test "throttles disabled (0) need no proxy" do
      Application.put_env(:fathom, :admin_auth_max_failures, 0)
      assert Fathom.Application.check_throttle_proxy!() == nil
    end

    test "dev and test are never refused" do
      Application.put_env(:fathom, :env, :dev)
      assert Fathom.Application.check_throttle_proxy!() == nil
    end
  end

  describe "#9 admin dashboard credentials (prod refuses blank, half-set and weak)" do
    @admin_keys [:admin_auth, :admin_auth_blank_env, :admin_weak_credentials_ok]

    setup do
      prev = Map.new(@admin_keys, &{&1, Application.get_env(:fathom, &1)})

      on_exit(fn ->
        for {k, v} <- prev do
          if is_nil(v),
            do: Application.delete_env(:fathom, k),
            else: Application.put_env(:fathom, k, v)
        end
      end)

      Application.put_env(:fathom, :env, :prod)
      Enum.each(@admin_keys, &Application.delete_env(:fathom, &1))
      :ok
    end

    test "prod refuses a password under 16 bytes" do
      Application.put_env(:fathom, :admin_auth, username: "admin", password: "admin")

      assert_raise RuntimeError, ~r/under 16 bytes/, fn ->
        Fathom.Application.check_admin_credentials!()
      end
    end

    test "prod refuses a password equal to the username, even at 16+ bytes" do
      user = "operator-account-1"
      Application.put_env(:fathom, :admin_auth, username: user, password: user)

      assert_raise RuntimeError, ~r/equal to the\s+username/, fn ->
        Fathom.Application.check_admin_credentials!()
      end
    end

    test "prod boots a 16-byte password that differs from the username" do
      Application.put_env(:fathom, :admin_auth, username: "admin", password: "0123456789abcdef")
      assert Fathom.Application.check_admin_credentials!() == nil
    end

    test "the weak-credentials opt-out boots a short password" do
      Application.put_env(:fathom, :admin_auth, username: "admin", password: "admin")
      Application.put_env(:fathom, :admin_weak_credentials_ok, true)
      assert Fathom.Application.check_admin_credentials!() == nil
    end

    test "prod refuses a blank env value, and the opt-out does not cover it" do
      Application.put_env(:fathom, :admin_auth_blank_env, true)
      Application.put_env(:fathom, :admin_weak_credentials_ok, true)

      assert_raise RuntimeError, ~r/blank value/, fn ->
        Fathom.Application.check_admin_credentials!()
      end
    end

    test "prod refuses a half-configured pair" do
      Application.put_env(:fathom, :admin_auth, username: "admin", password: nil)

      assert_raise RuntimeError, ~r/only one of ADMIN_USER and ADMIN_PASS/, fn ->
        Fathom.Application.check_admin_credentials!()
      end
    end

    test "both unset boots (the dashboard-off posture)" do
      assert Fathom.Application.check_admin_credentials!() == nil
    end

    test "dev and test are never refused" do
      Application.put_env(:fathom, :env, :dev)
      Application.put_env(:fathom, :admin_auth, username: "admin", password: "admin")
      assert Fathom.Application.check_admin_credentials!() == nil
    end
  end

  describe "#27 S3 session-token warning (prod only, warns, never refuses)" do
    test "a session token under S3 storage in prod logs the warning" do
      Application.put_env(:fathom, :env, :prod)
      Application.put_env(:fathom, :shard_storage, @s3)
      Application.put_env(:fathom, @s3, token: "FwoGZXIvYXdzEJr-temporary")

      log = capture_log(fn -> assert Fathom.Application.check_s3_static_credentials!() == nil end)
      assert log =~ "AWS_SESSION_TOKEN is set"
      assert log =~ "403"
    end

    test "static keys (no session token) are silent" do
      Application.put_env(:fathom, :env, :prod)
      Application.put_env(:fathom, :shard_storage, @s3)
      Application.put_env(:fathom, @s3, token: nil)

      log = capture_log(fn -> assert Fathom.Application.check_s3_static_credentials!() == nil end)
      refute log =~ "AWS_SESSION_TOKEN"
    end

    test "a session token outside prod, or with local storage, is silent" do
      Application.put_env(:fathom, @s3, token: "FwoGZXIvYXdz")

      Application.put_env(:fathom, :env, :dev)
      Application.put_env(:fathom, :shard_storage, @s3)

      refute capture_log(fn -> Fathom.Application.check_s3_static_credentials!() end) =~
               "AWS_SESSION_TOKEN"

      Application.put_env(:fathom, :env, :prod)
      Application.put_env(:fathom, :shard_storage, Fathom.Shard.Storage.Local)

      refute capture_log(fn -> Fathom.Application.check_s3_static_credentials!() end) =~
               "AWS_SESSION_TOKEN"
    end
  end

  describe "2026-11-07 #8 the arity-0 Local-storage fleet guard reads an UNSET :shard_storage as Local" do
    setup do
      keys = [:replication_followers, :replication_enabled, :lb_backends, :allow_local_storage]
      prev = Map.new(keys, &{&1, Application.fetch_env(:fathom, &1)})

      on_exit(fn ->
        for {k, v} <- prev do
          case v do
            {:ok, val} -> Application.put_env(:fathom, k, val)
            :error -> Application.delete_env(:fathom, k)
          end
        end
      end)

      Application.put_env(:fathom, :env, :prod)
      Application.delete_env(:fathom, :shard_storage)
      Application.delete_env(:fathom, :lb_backends)
      Application.delete_env(:fathom, :allow_local_storage)
      Application.put_env(:fathom, :replication_enabled, false)
      :ok
    end

    test "prod, SHARD_STORAGE unset, replication followers set: refuses to boot" do
      Application.put_env(:fathom, :replication_followers, [{"n2", "10.0.0.2", 9100}])

      assert_raise RuntimeError, ~r/no cross-node single-writer/, fn ->
        Fathom.Application.check_local_storage_fleet!()
      end
    end

    test "the ack lets that same config boot" do
      Application.put_env(:fathom, :replication_followers, [{"n2", "10.0.0.2", 9100}])
      Application.put_env(:fathom, :allow_local_storage, true)

      assert Fathom.Application.check_local_storage_fleet!() == nil
    end
  end

  describe "2026-11-07 #37 REPLICATION_DIR equal to or nested with SHARD_DATA_DIR refuses to boot" do
    setup do
      keys = [:replication_listen, :replication_dir, :shard_data_dir]
      prev = Map.new(keys, &{&1, Application.fetch_env(:fathom, &1)})

      on_exit(fn ->
        for {k, v} <- prev do
          case v do
            {:ok, val} -> Application.put_env(:fathom, k, val)
            :error -> Application.delete_env(:fathom, k)
          end
        end
      end)

      Application.put_env(:fathom, :env, :prod)
      Application.put_env(:fathom, :replication_listen, true)
      :ok
    end

    test "the same directory is refused" do
      Application.put_env(:fathom, :replication_dir, "/var/lib/fathom/x")
      Application.put_env(:fathom, :shard_data_dir, "/var/lib/fathom/x")

      assert_raise RuntimeError, ~r/equals or is nested.*SHARD_DATA_DIR/s, fn ->
        Fathom.Application.check_replication_disk!()
      end
    end

    test "a replica dir inside the shard dir (and the reverse) is refused" do
      Application.put_env(:fathom, :replication_dir, "/var/lib/fathom/shards/replicas")
      Application.put_env(:fathom, :shard_data_dir, "/var/lib/fathom/shards")

      assert_raise RuntimeError, ~r/equals or is nested/, fn ->
        Fathom.Application.check_replication_disk!()
      end

      Application.put_env(:fathom, :replication_dir, "/var/lib/fathom")
      Application.put_env(:fathom, :shard_data_dir, "/var/lib/fathom/shards")

      assert_raise RuntimeError, ~r/equals or is nested/, fn ->
        Fathom.Application.check_replication_disk!()
      end
    end

    test "not prod, or not listening, is never refused" do
      Application.put_env(:fathom, :replication_dir, "/var/lib/fathom/x")
      Application.put_env(:fathom, :shard_data_dir, "/var/lib/fathom/x")

      Application.put_env(:fathom, :env, :dev)
      assert Fathom.Application.check_replication_disk!() == nil

      Application.put_env(:fathom, :env, :prod)
      Application.put_env(:fathom, :replication_listen, false)
      assert Fathom.Application.check_replication_disk!() == nil
    end
  end

  describe "2026-11-07 #81 a prod flush interval <= 0 warns at boot (arity-0 wiring)" do
    setup do
      prev = Application.fetch_env(:fathom, :shard_flush_interval_ms)

      on_exit(fn ->
        case prev do
          {:ok, v} -> Application.put_env(:fathom, :shard_flush_interval_ms, v)
          :error -> Application.delete_env(:fathom, :shard_flush_interval_ms)
        end
      end)

      Application.put_env(:fathom, :env, :prod)
      :ok
    end

    test "0 warns, a positive interval is silent" do
      Application.put_env(:fathom, :shard_flush_interval_ms, 0)

      log = capture_log(fn -> assert Fathom.Application.check_flush_interval!() == nil end)
      assert log =~ "DISABLES the periodic durability flush"

      Application.put_env(:fathom, :shard_flush_interval_ms, 300_000)

      refute capture_log(fn -> Fathom.Application.check_flush_interval!() end) =~
               "DISABLES the periodic durability flush"
    end
  end

  # ---------------------------------------------------------------------------------------------
  # Moved here from shard_executor_test.exs (cleanliness audit 2026-11-13 #42): these exercise
  # `Fathom.Application.check_*!` guards, not the executor binding, and that file is the one AGENTS.md
  # points newcomers at as the shard-test template.
  # ---------------------------------------------------------------------------------------------

  # Finding #17: prod must refuse to boot if the fallback default shard equals the capture template
  # (anonymous default traffic would then drive fleet-wide capture). Prod-gated; dev/test
  # intentionally couple them ("demo"), so the guard is a no-op outside prod.
  test "the boot guard refuses a prod config where default_shard equals template_shard_id" do
    prev_env = Application.get_env(:fathom, :env)
    prev_tpl = Application.get_env(:fathom, :template_shard_id)
    prev_def = Application.get_env(:fathom, :default_shard)

    on_exit(fn ->
      Application.put_env(:fathom, :env, prev_env)
      Application.put_env(:fathom, :template_shard_id, prev_tpl)
      Application.put_env(:fathom, :default_shard, prev_def)
    end)

    Application.put_env(:fathom, :env, :prod)
    Application.put_env(:fathom, :template_shard_id, "poison")
    Application.put_env(:fathom, :default_shard, "poison")

    assert_raise RuntimeError, ~r/finding #17/, fn ->
      Fathom.Application.check_template_default!()
    end

    # A different default (or the fail-closed nil) boots fine.
    Application.put_env(:fathom, :default_shard, "safe")
    assert Fathom.Application.check_template_default!() == nil
  end

  # Review 2026-07-09 #1: the rebalancer keys its exception table + dead-node reconciler on
  # node_key = an lb_backends key; a NODE_KEY↔lb_backends mismatch makes every pin look dead
  # and unpins the fleet each tick. Prod must refuse to boot with that config.
  test "the boot guard refuses a prod NODE_KEY that isn't an lb_backends key" do
    prev_env = Application.get_env(:fathom, :env)
    prev_backends = Application.get_env(:fathom, :lb_backends)
    prev_key = Application.get_env(:fathom, :node_key)

    on_exit(fn ->
      Application.put_env(:fathom, :env, prev_env)
      restore_env(:lb_backends, prev_backends)
      restore_env(:node_key, prev_key)
    end)

    Application.put_env(:fathom, :env, :prod)
    Application.put_env(:fathom, :lb_backends, %{"fathom1" => "fathom1:8080"})
    Application.put_env(:fathom, :node_key, "fathom9")

    assert_raise RuntimeError, ~r/not a key of :lb_backends/, fn ->
      Fathom.Application.check_rebalancer_config!()
    end

    # A matching NODE_KEY boots fine.
    Application.put_env(:fathom, :node_key, "fathom1")
    assert Fathom.Application.check_rebalancer_config!() == nil

    # Inert when :lb_backends is unset (no rebalancer fleet), and outside prod.
    Application.delete_env(:fathom, :lb_backends)
    Application.put_env(:fathom, :node_key, "anything")
    assert Fathom.Application.check_rebalancer_config!() == nil

    Application.put_env(:fathom, :env, :test)
    Application.put_env(:fathom, :lb_backends, %{"fathom1" => "x"})
    Application.put_env(:fathom, :node_key, "mismatch")
    assert Fathom.Application.check_rebalancer_config!() == nil
  end

  # Expert review 2026-09-18 #12: :max_open_shards bounds coordinators, but pooling adds idle handles
  # (each a distinct SQLite connection/fds) per shard that the cap cannot see, so a pooling-on fleet
  # hits emfile below the intended shard count. A prod boot with pooling on + a finite cap must WARN,
  # quantifying the multiplier; it must stay quiet when pooling is off, the cap is infinite, or
  # outside prod.
  test "the boot guard warns that pooling inflates the fd budget past :max_open_shards" do
    import ExUnit.CaptureLog

    prev_env = Application.get_env(:fathom, :env)
    prev_pool = Application.get_env(:fathom, :connection_pool)
    prev_cap = Application.get_env(:fathom, :max_open_shards)
    prev_opts = Application.get_env(:fathom, :connection_pool_opts)

    on_exit(fn ->
      restore_env(:env, prev_env)
      restore_env(:connection_pool, prev_pool)
      restore_env(:max_open_shards, prev_cap)
      restore_env(:connection_pool_opts, prev_opts)
    end)

    Application.put_env(:fathom, :env, :prod)
    Application.put_env(:fathom, :connection_pool, true)
    Application.put_env(:fathom, :max_open_shards, 1000)
    Application.put_env(:fathom, :connection_pool_opts, max_per_scope: 2)

    log = capture_log(fn -> assert Fathom.Application.check_pool_fd_budget!() == nil end)
    assert log =~ "connection_pool is ON"

    # Expert review 2026-10-20-2027 #2: counted in FDS (lsof). 5 connections (1 + 2*2) at 2 fds each,
    # plus the shared -shm = 11 per shard. Previously asserted "5000 connections" (1 fd/connection),
    # about half the real number.
    assert log =~ "11000 fds"

    # Quiet when the cap is unbounded (nothing to inflate past).
    Application.put_env(:fathom, :max_open_shards, :infinity)

    refute capture_log(fn -> Fathom.Application.check_pool_fd_budget!() end) =~
             "connection_pool is ON"

    # Quiet when pooling is off.
    Application.put_env(:fathom, :max_open_shards, 1000)
    Application.put_env(:fathom, :connection_pool, false)

    refute capture_log(fn -> Fathom.Application.check_pool_fd_budget!() end) =~
             "connection_pool is ON"

    # Quiet outside prod.
    Application.put_env(:fathom, :connection_pool, true)
    Application.put_env(:fathom, :env, :test)

    refute capture_log(fn -> Fathom.Application.check_pool_fd_budget!() end) =~
             "connection_pool is ON"
  end

  # Expert review 2026-10-10 #P1. A replicated shard holds one raw WAL read fd on top of its
  # coordinator connection, and the :max_open_shards cap does not count it -- so replication ON
  # (the prod default) with the pool OFF still multiplies the fd footprint.
  test "the fd budget warning counts the held replication WAL fd" do
    import ExUnit.CaptureLog

    keys = [:env, :connection_pool, :max_open_shards, :replication_enabled, :connection_pool_opts]
    prev = for k <- keys, do: {k, Application.get_env(:fathom, k)}
    on_exit(fn -> for {k, v} <- prev, do: restore_env(k, v) end)

    Application.put_env(:fathom, :env, :prod)
    Application.put_env(:fathom, :connection_pool, false)
    Application.put_env(:fathom, :max_open_shards, 1000)
    Application.put_env(:fathom, :replication_enabled, true)

    log = capture_log(fn -> assert Fathom.Application.check_pool_fd_budget!() == nil end)
    assert log =~ "replication is ON"

    # per_shard = 1 coordinator connection (2 fds) + -shm + the Session's -wal and -shm (2026-10-20-2027
    # #2: previously counted as 1 + 1 = "2000 connections").
    assert log =~ "5000 fds"
    refute log =~ "connection_pool is ON"

    # Both on: 3 connections (1 + 2*1, default max_per_scope 1) * 2 + -shm + 2 replication fds.
    Application.put_env(:fathom, :connection_pool, true)
    Application.delete_env(:fathom, :connection_pool_opts)
    log = capture_log(fn -> Fathom.Application.check_pool_fd_budget!() end)
    assert log =~ "9000 fds"

    # Replication off and pool off: quiet.
    Application.put_env(:fathom, :connection_pool, false)
    Application.put_env(:fathom, :replication_enabled, false)
    assert capture_log(fn -> Fathom.Application.check_pool_fd_budget!() end) == ""
  end

  defp restore_env(k, nil), do: Application.delete_env(:fathom, k)
  defp restore_env(k, v), do: Application.put_env(:fathom, k, v)

  # Expert review #9: template capture replays a shard's SQL fleet-wide, and AGENTS.md forbids a
  # prod template without auth on that shard — but no guard enforced it, so an anonymously
  # reachable template shard (auth :disabled, one Host header away) could poison a fleet version
  # that Migrator.Copy replays onto every tenant. Prod must refuse to boot with that config.
  test "the boot guard refuses a prod template_shard_id with hrana auth disabled" do
    prev_env = Application.get_env(:fathom, :env)
    prev_tpl = Application.get_env(:fathom, :template_shard_id)
    prev_auth = Application.get_env(:fathom, :hrana_auth)

    on_exit(fn ->
      Application.put_env(:fathom, :env, prev_env)
      Application.put_env(:fathom, :template_shard_id, prev_tpl)

      if is_nil(prev_auth),
        do: Application.delete_env(:fathom, :hrana_auth),
        else: Application.put_env(:fathom, :hrana_auth, prev_auth)
    end)

    Application.put_env(:fathom, :env, :prod)
    Application.put_env(:fathom, :template_shard_id, "template")
    Application.put_env(:fathom, :hrana_auth, :disabled)

    assert_raise RuntimeError, ~r/poisoning vector/, fn ->
      Fathom.Application.check_template_auth!()
    end

    # An unset :hrana_auth defaults to :disabled — still refused.
    Application.delete_env(:fathom, :hrana_auth)

    assert_raise RuntimeError, ~r/poisoning vector/, fn ->
      Fathom.Application.check_template_auth!()
    end

    # Auth required (or any non-:disabled value, which fails closed to required) boots fine.
    Application.put_env(:fathom, :hrana_auth, :required)
    assert Fathom.Application.check_template_auth!() == nil

    # No template in prod boots fine regardless of auth mode.
    Application.put_env(:fathom, :hrana_auth, :disabled)
    Application.put_env(:fathom, :template_shard_id, nil)
    assert Fathom.Application.check_template_auth!() == nil
  end

  # Finding #3: the Hrana port carries no in-app credential, so the listener's bind interface is
  # a network-hardening control (pin to the private interface the LB reaches). Default is all
  # interfaces; prod sets it from HRANA_BIND_IP (config/runtime.exs).
  test "hrana_bind_ip is configurable and defaults to all interfaces" do
    assert Fathom.Application.hrana_bind_ip() == {0, 0, 0, 0}

    prev = Application.get_env(:fathom, :hrana_bind_ip)
    Application.put_env(:fathom, :hrana_bind_ip, {127, 0, 0, 1})
    on_exit(fn -> Application.put_env(:fathom, :hrana_bind_ip, prev) end)

    assert Fathom.Application.hrana_bind_ip() == {127, 0, 0, 1}
  end

  # Expert review 2026-07-14 #3: Local storage's lease is node-local only (:global.trans scoped to
  # [node()]), so running it across a fleet (:lb_backends set) is silent split-brain — two nodes
  # can each hold a shard's lease and both write. Prod must refuse to boot with that combination.
  test "the boot guard refuses prod Local storage with a multi-node fleet" do
    prev_env = Application.get_env(:fathom, :env)
    prev_storage = Application.get_env(:fathom, :shard_storage)
    prev_backends = Application.get_env(:fathom, :lb_backends)

    on_exit(fn ->
      Application.put_env(:fathom, :env, prev_env)
      restore_env(:shard_storage, prev_storage)
      restore_env(:lb_backends, prev_backends)
    end)

    Application.put_env(:fathom, :env, :prod)
    Application.put_env(:fathom, :shard_storage, Fathom.Shard.Storage.Local)
    Application.put_env(:fathom, :lb_backends, %{"fathom1" => "fathom1:8080"})

    assert_raise RuntimeError, ~r/no cross-node single-writer guarantee/, fn ->
      Fathom.Application.check_local_storage_fleet!()
    end

    # Single-node Local (no fleet) boots fine.
    Application.delete_env(:fathom, :lb_backends)
    assert Fathom.Application.check_local_storage_fleet!() == nil

    # The S3 backend across a fleet boots fine (it enforces cross-node single-writer).
    Application.put_env(:fathom, :lb_backends, %{"fathom1" => "fathom1:8080"})
    Application.put_env(:fathom, :shard_storage, Fathom.Shard.Storage.S3)
    assert Fathom.Application.check_local_storage_fleet!() == nil
  end

  # Expert review 2026-07-14 #6: without :shard_base_domain, Fathom.ShardExecutor.shard_from_host
  # promotes any attacker-controlled Host first-label to a shard id (cross-tenant selection). Prod
  # must refuse to boot an EXPOSED data plane (a fleet, or the Hrana listener on) with the zone
  # unset/blank, unless ALLOW_UNANCHORED_ROUTING explicitly acks it.
  test "the boot guard refuses a prod exposed data plane with no shard_base_domain" do
    prev_env = Application.get_env(:fathom, :env)
    prev_zone = Application.get_env(:fathom, :shard_base_domain)
    prev_backends = Application.get_env(:fathom, :lb_backends)
    prev_server = Application.get_env(:fathom, :hrana_server)
    prev_ack = Application.get_env(:fathom, :allow_unanchored_routing)

    on_exit(fn ->
      Application.put_env(:fathom, :env, prev_env)
      restore_env(:shard_base_domain, prev_zone)
      restore_env(:lb_backends, prev_backends)
      restore_env(:hrana_server, prev_server)
      restore_env(:allow_unanchored_routing, prev_ack)
    end)

    Application.put_env(:fathom, :env, :prod)
    Application.delete_env(:fathom, :shard_base_domain)
    Application.put_env(:fathom, :lb_backends, %{"fathom1" => "fathom1:8080"})
    Application.delete_env(:fathom, :allow_unanchored_routing)

    assert_raise RuntimeError, ~r/SHARD_BASE_DOMAIN/, fn ->
      Fathom.Application.check_shard_base_domain!()
    end

    # A blank/whitespace zone is treated as unset — still refused.
    Application.put_env(:fathom, :shard_base_domain, "   ")

    assert_raise RuntimeError, ~r/SHARD_BASE_DOMAIN/, fn ->
      Fathom.Application.check_shard_base_domain!()
    end

    # An anchored zone boots fine.
    Application.put_env(:fathom, :shard_base_domain, "fathom.example")
    assert Fathom.Application.check_shard_base_domain!() == :ok

    # The explicit ack lets an unanchored deploy boot even with the zone unset.
    Application.delete_env(:fathom, :shard_base_domain)
    Application.put_env(:fathom, :allow_unanchored_routing, true)
    assert Fathom.Application.check_shard_base_domain!() == :ok

    # With the data plane NOT exposed (no fleet, Hrana server off), the zone may be unset.
    Application.delete_env(:fathom, :allow_unanchored_routing)
    Application.delete_env(:fathom, :lb_backends)
    Application.put_env(:fathom, :hrana_server, false)
    assert Fathom.Application.check_shard_base_domain!() == :ok

    # The Hrana listener being enabled ALSO counts as exposed (refused with the zone unset).
    Application.put_env(:fathom, :hrana_server, true)

    assert_raise RuntimeError, ~r/SHARD_BASE_DOMAIN/, fn ->
      Fathom.Application.check_shard_base_domain!()
    end
  end

  # Expert review 2026-07-14 #15: the ?db= / x-fathom-shard override is an unauthenticated
  # shard-selection primitive (finding #4) that must never be on in prod.
  test "the boot guard refuses a prod config with allow_shard_override enabled" do
    prev_env = Application.get_env(:fathom, :env)
    prev_override = Application.get_env(:fathom, :allow_shard_override)

    on_exit(fn ->
      Application.put_env(:fathom, :env, prev_env)
      restore_env(:allow_shard_override, prev_override)
    end)

    Application.put_env(:fathom, :env, :prod)
    Application.put_env(:fathom, :allow_shard_override, true)

    assert_raise RuntimeError, ~r/allow_shard_override/, fn ->
      Fathom.Application.check_shard_override!()
    end

    # Off (the prod default) boots fine, whether explicit or unset.
    Application.put_env(:fathom, :allow_shard_override, false)
    assert Fathom.Application.check_shard_override!() == nil

    Application.delete_env(:fathom, :allow_shard_override)
    assert Fathom.Application.check_shard_override!() == nil
  end

  # Expert review 2026-07-14 #16: a non-nil :default_shard in prod commingles all unresolved
  # requests into one shared shard instead of failing closed (finding #26). WARN, never raise —
  # a shared default can be an intentional single-tenant choice.
  test "the boot guard warns (never raises) on a prod non-nil default_shard" do
    import ExUnit.CaptureLog

    prev_env = Application.get_env(:fathom, :env)
    prev_default = Application.get_env(:fathom, :default_shard)

    on_exit(fn ->
      Application.put_env(:fathom, :env, prev_env)
      restore_env(:default_shard, prev_default)
    end)

    Application.put_env(:fathom, :env, :prod)
    Application.put_env(:fathom, :default_shard, "shared")

    log =
      capture_log(fn ->
        assert Fathom.Application.check_default_shard!() == nil
      end)

    assert log =~ "default_shard"
    assert log =~ "commingles"

    # Nil default (the fail-closed posture) emits no warning.
    Application.put_env(:fathom, :default_shard, nil)

    refute capture_log(fn ->
             assert Fathom.Application.check_default_shard!() == nil
           end) =~ "commingles"
  end

  # Expert review 2026-07-14 #18: an enabled, unauthenticated Hrana data plane bound to all
  # interfaces leaves only an external firewall/SG as the tenant-isolation control. WARN, never
  # raise — this is the documented network-trust posture.
  test "the boot guard warns (never raises) on an exposed unauthenticated Hrana plane" do
    import ExUnit.CaptureLog

    prev_env = Application.get_env(:fathom, :env)
    prev_server = Application.get_env(:fathom, :hrana_server)
    prev_auth = Application.get_env(:fathom, :hrana_auth)
    prev_bind = Application.get_env(:fathom, :hrana_bind_ip)

    on_exit(fn ->
      Application.put_env(:fathom, :env, prev_env)
      restore_env(:hrana_server, prev_server)
      restore_env(:hrana_auth, prev_auth)
      restore_env(:hrana_bind_ip, prev_bind)
    end)

    Application.put_env(:fathom, :env, :prod)
    Application.put_env(:fathom, :hrana_server, true)
    Application.put_env(:fathom, :hrana_auth, :disabled)
    Application.put_env(:fathom, :hrana_bind_ip, {0, 0, 0, 0})

    log =
      capture_log(fn ->
        assert Fathom.Application.check_hrana_exposure!() == nil
      end)

    assert log =~ "unauthenticated"
    assert log =~ "all interfaces"

    # The IPv6 wildcard bind is flagged the same way.
    Application.put_env(:fathom, :hrana_bind_ip, {0, 0, 0, 0, 0, 0, 0, 0})
    assert capture_log(fn -> Fathom.Application.check_hrana_exposure!() end) =~ "firewall"

    # Auth required silences it (the data path is authenticated).
    Application.put_env(:fathom, :hrana_auth, :required)

    refute capture_log(fn ->
             assert Fathom.Application.check_hrana_exposure!() == nil
           end) =~ "firewall"

    # A pinned (non-wildcard) bind silences it (network-isolated to the LB interface).
    Application.put_env(:fathom, :hrana_auth, :disabled)
    Application.put_env(:fathom, :hrana_bind_ip, {127, 0, 0, 1})

    refute capture_log(fn ->
             assert Fathom.Application.check_hrana_exposure!() == nil
           end) =~ "firewall"
  end

  # Expert review 2026-08-20 #3. RAISES where the Hrana guard above only warns, and the asymmetry
  # is the property under test: the Hrana port has an auth mode to fall back on, so leaving it
  # open is a posture a warning can name. The replication port has NO authentication at all — no
  # credential in the protocol, no peer allowlist — so a wildcard bind is an unguarded door to
  # write access on every shard the node holds.
  test "the replication listener refuses to boot in prod bound to all interfaces" do
    prev_env = Application.get_env(:fathom, :env)
    prev_listen = Application.get_env(:fathom, :replication_listen)
    prev_bind = Application.get_env(:fathom, :replication_bind_ip)

    on_exit(fn ->
      Application.put_env(:fathom, :env, prev_env)
      restore_env(:replication_listen, prev_listen)
      restore_env(:replication_bind_ip, prev_bind)
    end)

    Application.put_env(:fathom, :env, :prod)
    Application.put_env(:fathom, :replication_listen, true)
    Application.delete_env(:fathom, :replication_bind_ip)

    # Unset bind = wildcard, because Fleet.follower_children/0 simply omits :ip.
    assert_raise RuntimeError, ~r/UNAUTHENTICATED/, fn ->
      Fathom.Application.check_replication_exposure!()
    end

    # Both wildcard forms, spelled out.
    for wildcard <- [{0, 0, 0, 0}, {0, 0, 0, 0, 0, 0, 0, 0}] do
      Application.put_env(:fathom, :replication_bind_ip, wildcard)

      assert_raise RuntimeError, ~r/REPLICATION_BIND_IP/, fn ->
        Fathom.Application.check_replication_exposure!()
      end
    end

    # A pinned interface is the fix, and passes.
    Application.put_env(:fathom, :replication_bind_ip, {10, 0, 0, 7})
    assert Fathom.Application.check_replication_exposure!() == nil

    # Not listening at all is fine however it is bound — the guard is about an OPEN port.
    Application.put_env(:fathom, :replication_listen, false)
    Application.delete_env(:fathom, :replication_bind_ip)
    assert Fathom.Application.check_replication_exposure!() == nil

    # And it is prod-only, like every sibling guard.
    Application.put_env(:fathom, :env, :dev)
    Application.put_env(:fathom, :replication_listen, true)
    assert Fathom.Application.check_replication_exposure!() == nil
  end

  # Expert review 2026-08-20 #33. The flush interval reads like a pure RPO knob -- docs/durability.md
  # presented it as exactly that -- but with replication on it is the largest throughput dial there
  # is: a durability flush CHECKPOINTS the WAL, a checkpoint starts a new generation, and
  # Primary.plan/3 then correctly ships the ENTIRE WAL rather than a delta.
  #
  # One variable, same image, 512 tenants (2026-08-18): 5,000 -> 30,000 ms took the run from 22,599
  # errors to 4 and +49% throughput. Nothing connected the two -- not the doc, not the replication
  # config block, not this guard list -- so an operator tightening the interval to buy RPO degraded
  # a replicating fleet with no signal saying so.
  #
  # WARNS rather than raises: a tight interval is a legitimate deliberate choice (RPO can matter
  # more than throughput), and refusing to boot over a tuning decision would be worse than the
  # confusion this removes.
  test "a prod node replicating with a tight flush interval is warned about" do
    import ExUnit.CaptureLog

    prev_env = Application.get_env(:fathom, :env)
    prev_repl = Application.get_env(:fathom, :replication_enabled)
    prev_ms = Application.get_env(:fathom, :shard_flush_interval_ms)

    on_exit(fn ->
      Application.put_env(:fathom, :env, prev_env)
      restore_env(:replication_enabled, prev_repl)
      restore_env(:shard_flush_interval_ms, prev_ms)
    end)

    Application.put_env(:fathom, :env, :prod)
    Application.put_env(:fathom, :replication_enabled, true)
    Application.put_env(:fathom, :shard_flush_interval_ms, 5_000)

    assert capture_log(fn ->
             assert Fathom.Application.check_replication_flush_interval!() == nil
           end) =~ "ENTIRE WAL"

    # At or above the floor the A/B measured as clean, silence.
    Application.put_env(:fathom, :shard_flush_interval_ms, 30_000)
    assert capture_log(fn -> Fathom.Application.check_replication_flush_interval!() end) == ""

    # 0 is idle-only flushing -- no periodic checkpoint at all, so no generation churn. It has its
    # own (unbounded-RPO) problem, which is not this guard's business.
    Application.put_env(:fathom, :shard_flush_interval_ms, 0)
    assert capture_log(fn -> Fathom.Application.check_replication_flush_interval!() end) == ""

    # Not replicating: the interval is a pure RPO knob again and a tight one is unremarkable.
    Application.put_env(:fathom, :shard_flush_interval_ms, 5_000)
    Application.put_env(:fathom, :replication_enabled, false)
    assert capture_log(fn -> Fathom.Application.check_replication_flush_interval!() end) == ""

    # And prod-only, like every sibling guard.
    Application.put_env(:fathom, :replication_enabled, true)
    Application.put_env(:fathom, :env, :dev)
    assert capture_log(fn -> Fathom.Application.check_replication_flush_interval!() end) == ""
  end

  # Expert review 2026-08-20 #23. The replica store holds a full copy of every shard this node
  # FOLLOWS, grows from other nodes' write traffic, and has no retention — and both it and the live
  # shard data dir default under System.tmp_dir!(), so the unsafe arrangement is the DEFAULT one.
  # Sharing a volume means a peer's write rate can fill the disk this node's own durability flushes
  # and cold-open pulls depend on, which is the unbounded-RPO failure AGENTS.md documents: writes
  # keep being acked and can never be made durable.
  #
  # WARNS rather than raises, unlike the #3 guard directly above: a single-volume box is a
  # legitimate small deployment, and `Follower`'s free-space floor bounds the damage. The asymmetry
  # is deliberate and is the property under test.
  test "a prod follower sharing a volume with the live shard data dir is warned about" do
    import ExUnit.CaptureLog

    prev_env = Application.get_env(:fathom, :env)
    prev_listen = Application.get_env(:fathom, :replication_listen)
    prev_repl_dir = Application.get_env(:fathom, :replication_dir)
    prev_data_dir = Application.get_env(:fathom, :shard_data_dir)

    on_exit(fn ->
      Application.put_env(:fathom, :env, prev_env)
      restore_env(:replication_listen, prev_listen)
      restore_env(:replication_dir, prev_repl_dir)
      restore_env(:shard_data_dir, prev_data_dir)
    end)

    root = Path.join(System.tmp_dir!(), "replvol_#{System.unique_integer([:positive])}")
    shared_a = Path.join(root, "replica")
    shared_b = Path.join(root, "data")
    File.mkdir_p!(shared_a)
    File.mkdir_p!(shared_b)
    on_exit(fn -> File.rm_rf(root) end)

    Application.put_env(:fathom, :env, :prod)
    Application.put_env(:fathom, :replication_listen, true)
    Application.put_env(:fathom, :replication_dir, shared_a)
    Application.put_env(:fathom, :shard_data_dir, shared_b)

    # Two directories under one tmpdir are the same mount, which is exactly the DEFAULT shape.
    assert capture_log(fn ->
             assert Fathom.Application.check_replication_disk!() == nil
           end) =~ "SAME volume"

    # Not listening: this node is nobody's follower, so the replica store never grows and the
    # arrangement is irrelevant however the directories are laid out.
    Application.put_env(:fathom, :replication_listen, false)
    assert capture_log(fn -> Fathom.Application.check_replication_disk!() end) == ""

    # And prod-only, like every sibling guard.
    Application.put_env(:fathom, :replication_listen, true)
    Application.put_env(:fathom, :env, :dev)
    assert capture_log(fn -> Fathom.Application.check_replication_disk!() end) == ""
  end

  # All five 2026-07-14 config guards are prod-only: with env != :prod (the dev/test default),
  # every one is inert regardless of the risky config, so they never fire outside prod.
  test "all 2026-07-14 config guards are inert when env is not prod" do
    import ExUnit.CaptureLog

    prev_env = Application.get_env(:fathom, :env)
    prev_storage = Application.get_env(:fathom, :shard_storage)
    prev_backends = Application.get_env(:fathom, :lb_backends)
    prev_zone = Application.get_env(:fathom, :shard_base_domain)
    prev_server = Application.get_env(:fathom, :hrana_server)
    prev_override = Application.get_env(:fathom, :allow_shard_override)
    prev_default = Application.get_env(:fathom, :default_shard)
    prev_auth = Application.get_env(:fathom, :hrana_auth)
    prev_bind = Application.get_env(:fathom, :hrana_bind_ip)

    on_exit(fn ->
      Application.put_env(:fathom, :env, prev_env)
      restore_env(:shard_storage, prev_storage)
      restore_env(:lb_backends, prev_backends)
      restore_env(:shard_base_domain, prev_zone)
      restore_env(:hrana_server, prev_server)
      restore_env(:allow_shard_override, prev_override)
      restore_env(:default_shard, prev_default)
      restore_env(:hrana_auth, prev_auth)
      restore_env(:hrana_bind_ip, prev_bind)
    end)

    # env :test (not :prod) + every risky config set at once.
    Application.put_env(:fathom, :env, :test)
    Application.put_env(:fathom, :shard_storage, Fathom.Shard.Storage.Local)
    Application.put_env(:fathom, :lb_backends, %{"fathom1" => "fathom1:8080"})
    Application.delete_env(:fathom, :shard_base_domain)
    Application.put_env(:fathom, :hrana_server, true)
    Application.put_env(:fathom, :allow_shard_override, true)
    Application.put_env(:fathom, :default_shard, "shared")
    Application.put_env(:fathom, :hrana_auth, :disabled)
    Application.put_env(:fathom, :hrana_bind_ip, {0, 0, 0, 0})

    # No raise from the RAISE guards…
    assert Fathom.Application.check_local_storage_fleet!() == nil
    assert Fathom.Application.check_shard_base_domain!() == :ok
    assert Fathom.Application.check_shard_override!() == nil

    # …and no warning from the WARN guards.
    log =
      capture_log(fn ->
        assert Fathom.Application.check_default_shard!() == nil
        assert Fathom.Application.check_hrana_exposure!() == nil
      end)

    refute log =~ "commingles"
    refute log =~ "firewall"
  end
end
