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
    keys = [:env, :sqlite_extension, :allow_no_udf]
    prev = for k <- keys, do: {k, Application.fetch_env(:fathom, k)}

    on_exit(fn ->
      for {k, v} <- prev do
        case v do
          {:ok, val} -> Application.put_env(:fathom, k, val)
          :error -> Application.delete_env(:fathom, k)
        end
      end
    end)

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
end
