defmodule Fathom.UdfBootGuardTest do
  @moduledoc """
  Expert review 2026-10-10 #8. Symptom: an image built without cargo (`compile.fathom_udf` SKIPS)
  booted a prod node that silently lacked the engine PRAGMA authorizer, the deadline backstop, the
  tenant size limits and Django's UDFs; `Extension.available?/0`'s doc claimed Application read it
  at boot, but nothing did. Invariant: prod refuses to boot without the extension unless
  `:allow_no_udf` (ALLOW_NO_UDF=true) acknowledges it; dev/test never trip it.
  """
  use ExUnit.Case, async: false

  setup do
    keys = [:env, :sqlite_extension, :allow_no_udf, :replication_enabled]
    prev = for k <- keys, do: {k, Application.fetch_env(:fathom, k)}

    on_exit(fn ->
      for {k, v} <- prev do
        case v do
          {:ok, val} -> Application.put_env(:fathom, k, val)
          :error -> Application.delete_env(:fathom, k)
        end
      end
    end)

    # Hermetic against the replication default: prod turns it ON (expert review 2026-11-07 #24), and
    # these arity-0 tests are about the ALLOW_NO_UDF / presence matrix with replication OFF.
    Application.put_env(:fathom, :replication_enabled, false)
    :ok
  end

  test "prod without the extension refuses to boot" do
    Application.put_env(:fathom, :env, :prod)
    Application.put_env(:fathom, :sqlite_extension, false)
    Application.delete_env(:fathom, :allow_no_udf)

    assert_raise RuntimeError, ~r/fathom_udf SQLite extension is not available/, fn ->
      Fathom.Application.check_udf_extension!()
    end
  end

  test "ALLOW_NO_UDF acknowledges the degraded boot" do
    Application.put_env(:fathom, :env, :prod)
    Application.put_env(:fathom, :sqlite_extension, false)
    Application.put_env(:fathom, :allow_no_udf, true)

    assert Fathom.Application.check_udf_extension!() == nil
  end

  test "ALLOW_NO_UDF does NOT acknowledge a missing extension when replication is on (#24)" do
    Application.put_env(:fathom, :env, :prod)
    Application.put_env(:fathom, :sqlite_extension, false)
    Application.put_env(:fathom, :allow_no_udf, true)
    Application.put_env(:fathom, :replication_enabled, true)

    assert_raise RuntimeError, ~r/replication is ON/, fn ->
      Fathom.Application.check_udf_extension!()
    end
  end

  test "prod with the extension present boots" do
    assert Fathom.Shard.Extension.available?(), "the extension is not built"
    Application.put_env(:fathom, :env, :prod)
    Application.delete_env(:fathom, :sqlite_extension)
    Application.delete_env(:fathom, :allow_no_udf)

    assert Fathom.Application.check_udf_extension!() == nil
  end

  test "dev and test never trip the guard" do
    Application.put_env(:fathom, :sqlite_extension, false)
    Application.delete_env(:fathom, :allow_no_udf)

    for env <- [:dev, :test] do
      Application.put_env(:fathom, :env, env)
      assert Fathom.Application.check_udf_extension!() == nil
    end
  end

  # Expert review 2026-11-07 #24. Symptom: `ALLOW_NO_UDF` (or `SQLITE_EXTENSION=false`) with
  # replication ON booted, and re-opened the A2 unpinned-checkpoint window (SQLite's built-in
  # autocheckpoint, not the extension's hook, owns the WAL; quorum-acked frames can be erased from
  # the primary's WAL before the Session reads them). Neither the boot error nor the docs said so.
  # Invariant: in prod, a missing extension with replication on is refused whatever the ack says.
  # Pure function, explicit config: no global app env.
  describe "udf_extension_problem/1 (2026-11-07 #24)" do
    defp cfg(overrides) do
      Map.merge(
        %{env: :prod, extension: :missing, allow_no_udf: false, replication_enabled: false},
        Map.new(overrides)
      )
    end

    test "prod + no extension + replication on is refused even WITH ALLOW_NO_UDF (pre-fix: nil)" do
      message =
        Fathom.Application.udf_extension_problem(
          cfg(allow_no_udf: true, replication_enabled: true)
        )

      assert message =~ "replication is ON"
      assert message =~ "ALLOW_NO_UDF does not cover this"
      assert message =~ "REPLICATION_ENABLED=false"
    end

    test "prod + no extension + replication on is refused without the ack too" do
      assert Fathom.Application.udf_extension_problem(cfg(replication_enabled: true)) =~
               "replication is ON"
    end

    test "with replication OFF the ack still works, and absent the ack it is refused" do
      assert Fathom.Application.udf_extension_problem(cfg(allow_no_udf: true)) == nil

      assert Fathom.Application.udf_extension_problem(cfg([])) =~
               "fathom_udf SQLite extension is not available"
    end

    test "a present extension, or a non-prod env, is never refused" do
      assert Fathom.Application.udf_extension_problem(
               cfg(extension: :ok, replication_enabled: true)
             ) == nil

      for env <- [:dev, :test] do
        assert Fathom.Application.udf_extension_problem(cfg(env: env, replication_enabled: true)) ==
                 nil
      end
    end
  end
end
