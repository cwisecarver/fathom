defmodule Mix.Tasks.Fathom.BenchLockTest do
  @moduledoc """
  The host-wide benchmark lock: `mix fathom.bench` refuses to run while the lock exists, and
  otherwise creates it for the duration and removes it (even on failure). Tests exercise the
  `with_lock/2` seam against a unique temp path (never the real /tmp/fathom_bench.lock, so
  a real benchmark on this host isn't disturbed).
  """
  use ExUnit.Case, async: true

  alias Mix.Tasks.Fathom.Bench

  setup do
    path =
      Path.join(System.tmp_dir!(), "bench_lock_test_#{System.unique_integer([:positive])}.lock")

    on_exit(fn -> File.rm(path) end)
    %{path: path}
  end

  test "runs the body and creates+removes the lock when absent", %{path: path} do
    refute File.exists?(path)

    result =
      Bench.with_lock(path, fn ->
        assert File.exists?(path), "lock is held for the duration of the run"
        :body_ran
      end)

    assert result == :body_ran
    refute File.exists?(path), "lock removed after the run"
  end

  test "refuses and does not run the body when the lock exists; leaves it in place", %{
    path: path
  } do
    File.write!(path, "someone else's run\n")
    parent = self()

    assert_raise Mix.Error, ~r/benchmark lock present/, fn ->
      Bench.with_lock(path, fn -> send(parent, :ran) end)
    end

    refute_received :ran, "the body must not run while the lock is present"
    assert File.read!(path) == "someone else's run\n", "a pre-existing lock is left untouched"
  end

  test "removes the lock even if the body raises", %{path: path} do
    assert_raise RuntimeError, fn ->
      Bench.with_lock(path, fn -> raise "boom" end)
    end

    refute File.exists?(path), "lock removed on failure so it doesn't wedge the next run"
  end
end
