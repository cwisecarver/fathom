defmodule Fathom.Rebalancer.LbApplyTest do
  @moduledoc """
  Promotion of the rendered exception map to `:lb_map_path`. Not async — it toggles the
  shared `:lb_map_path`/`:lb_test_cmd`/`:lb_backends` application env and writes Postgres
  (overrides) via the shared sandbox.
  """
  use Fathom.DataCase, async: false

  alias Fathom.Rebalancer.{LbApply, Overrides}

  setup do
    # A per-test DIRECTORY, removed whole, rather than a bare file path with a hand-maintained
    # list of suffixes to delete. The old cleanup removed the map and `.tmp.*` but not the
    # `.applied` marker `mark_applied/1` writes beside it, which had leaked **173 files** into
    # `System.tmp_dir!()` by 2026-08-05. `System.unique_integer/1` is unique within one BEAM run
    # and starts fresh on the next, so those paths are re-drawn across runs — leaked state next to
    # a re-used path is cross-RUN contamination, the same "every run starts fresh" contract
    # `chaos.sh`'s `seed()` exists for.
    #
    # A directory also means the next sidecar `LbApply` learns to write is cleaned automatically
    # instead of leaking until someone notices.
    dir =
      Path.join(System.tmp_dir!(), "fathom_lbapply_#{System.unique_integer([:positive])}")

    File.mkdir_p!(dir)
    map_path = Path.join(dir, "map.conf")

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
      # The whole directory: the map, the `.applied` marker, and any stray `.tmp.*` candidate
      # from an aborted promotion.
      File.rm_rf(dir)
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

  test "a hung reload is killed at the deadline, not held indefinitely (#2)", %{
    map_path: map_path
  } do
    # Regression for #2: System.shell had no timeout, so a wedged reload blocked forever while
    # (pre-restructure) holding the pooled connection + fleet advisory lock. Now it's killed at
    # the deadline and surfaced as {:reload_timeout, _}.
    Application.put_env(:fathom, :lb_reload_cmd, "sleep 30")
    Application.put_env(:fathom, :lb_reload_timeout_ms, 200)
    on_exit(fn -> Application.delete_env(:fathom, :lb_reload_timeout_ms) end)
    {:ok, _} = Overrides.pin("hot_1", "n2", reason: "test")

    {elapsed_us, result} = :timer.tc(fn -> LbApply.apply!() end)

    assert {:error, {:reload_timeout, 200}} = result
    assert elapsed_us < 5_000_000, "reload killed at ~200ms, not after 30s"
    # The map was still PROMOTED (produced under the lock before the out-of-lock reload).
    assert File.read!(map_path) =~ "hot_1."
  end

  test "a hung config-test (inside the lock) is killed at the deadline (#2)", %{
    map_path: map_path
  } do
    # The config-test runs inside the advisory lock, so a hang would freeze fleet-wide LB
    # updates — it must be hard-timeout-bounded. On timeout the candidate is not promoted.
    File.write!(map_path, "# last-good\n")
    Application.put_env(:fathom, :lb_test_cmd, "sleep 30")
    Application.put_env(:fathom, :lb_test_timeout_ms, 200)
    on_exit(fn -> Application.delete_env(:fathom, :lb_test_timeout_ms) end)
    {:ok, _} = Overrides.pin("hot_1", "n2", reason: "test")

    {elapsed_us, result} = :timer.tc(fn -> LbApply.apply!() end)

    assert {:error, {:config_test_timeout, 200}} = result
    assert elapsed_us < 5_000_000

    assert File.read!(map_path) == "# last-good\n",
           "candidate not promoted on config-test timeout"
  end

  test "a byte-identical re-render after a SUCCESSFUL reload is a no-op (#10)", %{
    map_path: map_path
  } do
    # Regression for #10: the leader re-renders every tick, so an unchanged render whose
    # content the LB already has must skip the write + reload.
    Application.put_env(:fathom, :lb_reload_cmd, "true")
    {:ok, _} = Overrides.pin("hot_1", "n2", reason: "test")

    assert LbApply.apply!() == :ok
    mtime = File.stat!(map_path).mtime

    # Identical render, and the running LB already has it → genuine no-op.
    Application.put_env(:fathom, :lb_reload_cmd, "false")
    assert LbApply.apply!() == :ok
    assert File.stat!(map_path).mtime == mtime
  end

  test "a byte-identical re-render after a FAILED reload retries the reload (#23)", %{
    map_path: map_path
  } do
    # The counterpart #10 did not anticipate (expert review 2026-08-01 #23). The no-op test
    # compares the render to the FILE; what matters is whether the RUNNING LB has that content.
    # Those diverge exactly here: the file was promoted, the reload failed, and a byte-identical
    # retry then reported :ok — so HandoffJob believed the flip was live and drained the source
    # while nginx still routed every request for that shard to it. It also made the documented
    # per-tick self-heal permanent-no-op, so a failed reload never recovered.
    Application.put_env(:fathom, :lb_reload_cmd, "false")
    {:ok, _} = Overrides.pin("hot_1", "n2", reason: "test")

    # First apply promotes the file; the reload fails and is surfaced.
    assert {:error, {:reload_failed, _}} = LbApply.apply!()
    assert File.read!(map_path) =~ "hot_1."

    # Identical render, but the LB never loaded it: must NOT report success.
    assert {:error, {:reload_failed, _}} = LbApply.apply!(),
           "a promoted-but-never-loaded map was reported as applied"

    # And once the reload can succeed, the self-heal actually heals.
    Application.put_env(:fathom, :lb_reload_cmd, "true")
    assert LbApply.apply!() == :ok

    # Now it is genuinely applied, so further ticks are no-ops again.
    Application.put_env(:fathom, :lb_reload_cmd, "false")
    assert LbApply.apply!() == :ok
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
