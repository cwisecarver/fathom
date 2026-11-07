defmodule Fathom.Shard.ExtensionArmTest do
  @moduledoc """
  Expert review 2026-11-07 #42(b). Symptom: `Extension.arm/2` fell back to per-connection loading on
  ANY control-connection failure. A transient one (EMFILE/ENOMEM on the first tenant open), followed
  by a per-connection probe that succeeded once fd pressure eased, pinned the node to `:per_open`
  for its whole life (`persistent_term`, never revisited): enable -> dlopen -> disable on every
  open, +45-60 us each, and the authorizer/pragma-guard install bypassed. Invariant: only "the
  control entry point is absent" (an artifact built before r8 #14) may fall back; every other
  failure is an error that the next open retries.

  Tests the pure `arm_outcome/3`, so no global state (the armed `persistent_term`, app env) is
  touched and the module can run async.
  """
  use ExUnit.Case, async: true

  alias Fathom.Shard.Extension

  @path "/opt/fathom/libfathom_udf.so"

  # A probe that must not be consulted: the failure is not the missing-entry shape, so asking
  # whether the artifact "loads per connection" is exactly the bug.
  defp must_not_probe(_path), do: flunk("loads_per_connection? consulted for a non-entry failure")

  test "a transient control failure is an error, not :per_open (pre-fix: :per_open)" do
    transient = "unable to open shared library [#{@path}]: Too many open files"

    assert Extension.arm_outcome(@path, {:error, transient}, &must_not_probe/1) ==
             {:error, transient}

    # And with a probe that would succeed — the exact sequence in the finding.
    assert Extension.arm_outcome(@path, {:error, transient}, fn _ -> true end) ==
             {:error, transient}
  end

  test "an artifact without the control entry point falls back to :per_open iff it loads that way" do
    for message <- [
          # macOS (observed), glibc, musl
          "dlsym(0x6d3aa390, sqlite3_fathomudf_control_init): symbol not found",
          "#{@path}: undefined symbol: sqlite3_fathomudf_control_init",
          "Error relocating #{@path}: sqlite3_fathomudf_control_init: symbol not found"
        ] do
      assert Extension.arm_outcome(@path, {:error, message}, fn _ -> true end) == :per_open

      assert Extension.arm_outcome(@path, {:error, message}, fn _ -> false end) ==
               {:error, message}
    end
  end

  test "the missing-entry matcher recognises the REAL error this platform's SQLite and libc emit" do
    # A shared object that certainly lacks our entry point: exqlite's own NIF library.
    nif = Path.join(:code.priv_dir(:exqlite), "sqlite3_nif.so")
    assert File.exists?(nif), "fixture: exqlite's NIF library not found at #{nif}"

    assert {:error, message} = Extension.control(nif, &Extension.exec_row(&1, "SELECT 1", []))
    assert is_binary(message)

    assert Extension.arm_outcome(nif, {:error, message}, fn _ -> true end) == :per_open,
           "the missing-entry text did not mention the symbol: #{inspect(message)}"
  end

  test "success arms the auto-extension; any other success value is an error" do
    assert Extension.arm_outcome(@path, {:ok, 1}, &must_not_probe/1) == :auto

    assert Extension.arm_outcome(@path, {:ok, 0}, &must_not_probe/1) ==
             {:error, {:unexpected_arm_result, 0}}
  end

  test "a non-binary reason is never the missing-entry shape" do
    assert Extension.arm_outcome(@path, {:error, :enomem}, &must_not_probe/1) == {:error, :enomem}
  end
end
