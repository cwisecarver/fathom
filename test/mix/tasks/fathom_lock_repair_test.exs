defmodule Mix.Tasks.Fathom.Lock.RepairTest do
  @moduledoc """
  `mix fathom.lock.repair` on the Local backend (expert review 2026-10-15 #23, D14). The S3 side is
  in `test/fathom/shard/storage/s3_corrupt_lock_test.exs`. Invariants: refuses unless the lock is
  actually undecodable; the marker's epoch only ever goes UP; the next acquire goes through the
  ordinary steal path above it.
  """
  use ExUnit.Case, async: false

  alias Fathom.Shard.Storage
  alias Fathom.Shard.Storage.Local
  alias Mix.Tasks.Fathom.Lock.Repair

  setup do
    dir = Path.join(System.tmp_dir!(), "lockrepair_#{System.unique_integer([:positive])}")
    prev = Application.get_env(:fathom, Local)
    prev_backend = Application.get_env(:fathom, :shard_storage)
    File.mkdir_p!(dir)
    Application.put_env(:fathom, Local, dir: dir)
    Application.put_env(:fathom, :shard_storage, Local)

    on_exit(fn ->
      if prev,
        do: Application.put_env(:fathom, Local, prev),
        else: Application.delete_env(:fathom, Local)

      if prev_backend,
        do: Application.put_env(:fathom, :shard_storage, prev_backend),
        else: Application.delete_env(:fathom, :shard_storage)

      File.rm_rf(dir)
    end)

    %{dir: dir, id: "lr_#{System.unique_integer([:positive])}"}
  end

  test "repairs a corrupt lock: marker above the salvaged epoch, then acquire steals above it",
       ctx do
    %{dir: dir, id: id} = ctx
    File.write!(Path.join(dir, id <> ".lock"), ~s({"owner":"dead","epoch":41,"expires_))

    assert {:error, :corrupt_lock} = Local.acquire_lease(id, "node_a", 30_000)
    assert {:ok, marker} = Repair.repair(id)
    assert marker.epoch == 43
    assert marker.owner == Storage.released_owner()

    # Never backwards: the recreate-at-epoch-1 the old auto-repair did would fail this.
    assert {:ok, lease} = Local.acquire_lease(id, "node_a", 30_000)
    assert lease.epoch == 44
    assert lease.took_over
    assert :ok = Local.check_lease(id, lease)
  end

  test "refuses a healthy lock, leaving it byte-identical", ctx do
    %{dir: dir, id: id} = ctx
    {:ok, _} = Local.acquire_lease(id, "node_a", 30_000)
    before = File.read!(Path.join(dir, id <> ".lock"))

    assert {:error, :lock_not_corrupt} = Repair.repair(id)
    assert File.read!(Path.join(dir, id <> ".lock")) == before
  end

  test "refuses when there is no lock at all", %{id: id} do
    assert {:error, :no_lock} = Repair.repair(id)
  end

  test "run/1 raises on a refusal and prints on success", ctx do
    %{dir: dir, id: id} = ctx
    prev_shell = Mix.shell()
    Mix.shell(Mix.Shell.Process)
    on_exit(fn -> Mix.shell(prev_shell) end)

    assert_raise Mix.Error, ~r/no_lock/, fn -> Repair.run([id]) end
    assert_raise Mix.Error, ~r/usage/, fn -> Repair.run([]) end

    File.write!(Path.join(dir, id <> ".lock"), "")
    Repair.run([id, "--epoch-floor", "100"])
    assert_received {:mix_shell, :info, [msg]}
    assert msg =~ "epoch=102"
  end
end
