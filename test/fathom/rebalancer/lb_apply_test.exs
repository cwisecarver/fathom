defmodule Fathom.Rebalancer.LbApplyTest do
  @moduledoc """
  Promotion of the rendered exception map to `:lb_map_path`. Not async — it toggles the
  shared `:lb_map_path`/`:lb_test_cmd`/`:lb_backends` application env and writes Postgres
  (overrides) via the shared sandbox.
  """
  use Fathom.DataCase, async: false

  alias Fathom.Rebalancer.{LbApply, Overrides}

  setup do
    map_path =
      Path.join(System.tmp_dir!(), "fathom_lbapply_#{System.unique_integer([:positive])}.conf")

    prev = %{
      map: Application.get_env(:fathom, :lb_map_path),
      test: Application.get_env(:fathom, :lb_test_cmd),
      reload: Application.get_env(:fathom, :lb_reload_cmd),
      backends: Application.get_env(:fathom, :lb_backends)
    }

    Application.put_env(:fathom, :lb_map_path, map_path)
    Application.put_env(:fathom, :lb_backends, %{"n1" => "n1:8080", "n2" => "n2:8080"})
    # Never actually reload nginx in tests.
    Application.delete_env(:fathom, :lb_reload_cmd)

    on_exit(fn ->
      restore(:lb_map_path, prev.map)
      restore(:lb_test_cmd, prev.test)
      restore(:lb_reload_cmd, prev.reload)
      restore(:lb_backends, prev.backends)
      File.rm(map_path)
      # Any stray temp candidates from an aborted promotion.
      for f <- Path.wildcard(map_path <> ".tmp.*"), do: File.rm(f)
    end)

    %{map_path: map_path}
  end

  defp restore(k, nil), do: Application.delete_env(:fathom, k)
  defp restore(k, v), do: Application.put_env(:fathom, k, v)

  test "unset :lb_map_path is a no-op", %{map_path: map_path} do
    Application.delete_env(:fathom, :lb_map_path)
    assert LbApply.apply!() == :ok
    refute File.exists?(map_path)
  end

  test "promotes the rendered map atomically and leaves no temp behind", %{map_path: map_path} do
    {:ok, _} = Overrides.pin("hot_1", "n2", reason: "test")

    assert LbApply.apply!() == :ok
    assert File.read!(map_path) =~ "hot_1."
    assert File.read!(map_path) =~ "map $host $fathom_target"
    # The atomic rename consumes the temp; nothing partial is left in the dir.
    assert Path.wildcard(map_path <> ".tmp.*") == []
  end

  test "a failing config test aborts promotion, keeps the last-good file, returns error", %{
    map_path: map_path
  } do
    # Regression for #3: a malformed render must never land on disk (a broken included file
    # fails nginx's NEXT cold start — a fleet-wide outage decoupled from the change).
    File.write!(map_path, "# last-good map\n")
    # `false` exits non-zero — stand-in for `nginx -t` rejecting the candidate.
    Application.put_env(:fathom, :lb_test_cmd, "false")

    {:ok, _} = Overrides.pin("hot_1", "n2", reason: "test")

    assert {:error, {:config_test_failed, _code, _out}} = LbApply.apply!()
    # Live file untouched; no partial/temp left behind.
    assert File.read!(map_path) == "# last-good map\n"
    assert Path.wildcard(map_path <> ".tmp.*") == []
  end

  test "a reload-command failure surfaces {:error, {:reload_failed, _}} but still promotes (#11)",
       %{map_path: map_path} do
    # Regression for #11: apply! must surface a reload failure (not swallow it as :ok) so the
    # handoff won't drain against a flip that may not be live. The map is still promoted (it
    # passed any config test) — valid on disk for the next cold start.
    Application.put_env(:fathom, :lb_reload_cmd, "false")
    {:ok, _} = Overrides.pin("hot_1", "n2", reason: "test")

    assert {:error, {:reload_failed, _code}} = LbApply.apply!()
    assert File.read!(map_path) =~ "hot_1."
  end

  test "a passing config test receives the candidate path via {} and LB_MAP_CANDIDATE", %{
    map_path: map_path
  } do
    # The candidate must exist and be non-empty when the test runs (proves temp-first order).
    Application.put_env(:fathom, :lb_test_cmd, "test -s {} && test -n \"$LB_MAP_CANDIDATE\"")

    {:ok, _} = Overrides.pin("hot_1", "n2", reason: "test")

    assert LbApply.apply!() == :ok
    assert File.read!(map_path) =~ "hot_1."
  end
end
