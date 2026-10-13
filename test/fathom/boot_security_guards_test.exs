defmodule Fathom.BootSecurityGuardsTest do
  @moduledoc """
  Prod boot-time security guards from expert review 2026-10-12 (#12 throttle proxy posture, #27 S3
  session-token warning). Each guard is a plain function on `Fathom.Application`, so the tests call
  it directly with the config it reads set, the same way the template and frame-auth guards are
  tested. Not async: they mutate global application env.
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
end
