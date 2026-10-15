defmodule Fathom.Shard.Storage.LocalLockIntegrityTest do
  @moduledoc """
  The `Local` backend's LOCK FILE, when the disk misbehaves (expert review 2026-08-20 #31).

  Three defects with one shape: a filesystem call whose result was thrown away.

    * `create_lock/2` dropped both `IO.binwrite/2` and `File.close/1` and returned `{:ok, lease}`
      unconditionally, so on a full or failing disk a caller believed it held a lease over a
      **zero-length lock file**.
    * `do_release_lease/2` dropped `File.rm/1` and returned `:ok` unconditionally — the Local twin
      of the S3 bug AGENTS.md records at length ("a 412 was reported as `:ok`, collapsing two
      opposite situations"). Here the two are *the lock is someone else's* (a correct no-op) and
      *the lock is ours and the unlink failed* (a leak reported as success).
    * `:corrupt_lock` had two producers and **no consumer at all**: the tenant was down with no
      operator remedy short of deleting a file by hand. 2026-10-15 #23 answered that with an
      operator task (`mix fathom.lock.repair`), NOT an automatic repair -- see the describe below.

  `Local` is a supported single-node production backend AND the default backend for this entire
  suite, so a contract gap here also hides the class from `mix test`.

  ## What is NOT covered, and why

  There is no portable seam to make `IO.binwrite/2` or `File.close/1` fail — that needs a genuinely
  full or failing volume (Linux's `/dev/full` is not available on macOS, where this is developed).
  So the write-and-close check itself has no discriminating test here. What IS pinned is the STATE
  it exists to prevent: a lock file that exists but decodes to nothing is refused loudly (and left
  for the operator task) rather than silently reset.
  """
  use ExUnit.Case, async: false

  alias Fathom.Shard.Storage.Local

  setup do
    dir = Path.join(System.tmp_dir!(), "locklint_#{System.unique_integer([:positive])}")
    prev = Application.get_env(:fathom, Local)
    File.mkdir_p!(dir)
    Application.put_env(:fathom, Local, dir: dir)

    on_exit(fn ->
      # Restore any mode the tests changed, or rm_rf cannot clean up.
      for p <- Path.wildcard(Path.join(dir, "**")), do: File.chmod(p, 0o755)
      File.chmod(dir, 0o755)

      if prev,
        do: Application.put_env(:fathom, Local, prev),
        else: Application.delete_env(:fathom, Local)

      File.rm_rf(dir)
    end)

    %{dir: dir, id: "lck_#{System.unique_integer([:positive])}"}
  end

  defp lock_path(dir, id), do: Path.join(dir, id <> ".lock")

  describe "a corrupt lock fails closed, like S3 (expert review 2026-10-15 #23, D14)" do
    # These two tests PREVIOUSLY asserted the opposite (2026-08-20 #31): acquire deleted the lock
    # and recreated it at epoch 1. That dropped the epoch and the A2 claim the lock carried, and
    # made this double disagree with S3 (`{:error, :corrupt_lock}`), hiding S3's behaviour from the
    # suite. The recovery is now the operator task `mix fathom.lock.repair` (mix_task test).
    test "acquire_lease refuses an undecodable lock and leaves it untouched", ctx do
      %{dir: dir, id: id} = ctx

      # Exactly what a full disk leaves behind: the file exists and decodes to nothing.
      path = lock_path(dir, id)
      File.mkdir_p!(Path.dirname(path))
      File.write!(path, "")

      assert {:error, :corrupt_lock} = Local.acquire_lease(id, "node_a", 30_000),
             "an undecodable lock must fail closed (S3 parity), not be silently recreated at epoch 1"

      assert File.read!(path) == "", "acquire must not rewrite or delete the corrupt lock"
    end

    test "garbage bytes are refused the same way as an empty file", ctx do
      %{dir: dir, id: id} = ctx
      path = lock_path(dir, id)
      File.mkdir_p!(Path.dirname(path))
      File.write!(path, <<0xFF, 0xFE, "not a lease">>)

      assert {:error, :corrupt_lock} = Local.acquire_lease(id, "node_a", 30_000)
      assert File.read!(path) == <<0xFF, 0xFE, "not a lease">>
    end

    test "check_lease and renew_lease fail closed on a corrupt lock too", ctx do
      %{dir: dir, id: id} = ctx
      {:ok, lease} = Local.acquire_lease(id, "node_a", 30_000)

      File.write!(lock_path(dir, id), "")

      # check_lease and renew_lease run while a lease is believed HELD, and repairing under a holder
      # would be a silent takeover rather than a recovery.
      assert {:error, :corrupt_lock} = Local.check_lease(id, lease)
      assert {:error, :corrupt_lock} = Local.renew_lease(id, lease, 30_000)
    end
  end

  describe "release reports what actually happened" do
    test "a failed unlink is an error, not :ok", ctx do
      %{dir: dir, id: id} = ctx
      {:ok, lease} = Local.acquire_lease(id, "node_a", 30_000)
      lock_dir = dir

      # Make the unlink fail the only portable way: take write permission off the DIRECTORY.
      :ok = File.chmod(lock_dir, 0o555)

      # PRECONDITION. Running as root ignores directory permissions, and this test would then
      # pass for the wrong reason — assert the fixture works before trusting the result.
      probe = Path.join(lock_dir, "probe")
      File.write(probe, "x")

      assert match?({:error, _}, File.rm(lock_path(dir, id))),
             "the fixture could not make an unlink fail (are you running as root?), so this " <>
               "test proves nothing about the discarded File.rm result"

      assert {:error, _} = Local.release_lease(id, lease),
             "a failed unlink was reported as :ok — the same shape as the S3 412-reported-as-ok " <>
               "leak, where 'the lock is someone else's' and 'the lock is STILL OURS' were " <>
               "collapsed into one answer"

      File.chmod(lock_dir, 0o755)
      File.rm(probe)
    end

    test "releasing a lock that is already gone is still :ok", ctx do
      %{dir: dir, id: id} = ctx
      {:ok, lease} = Local.acquire_lease(id, "node_a", 30_000)
      File.rm!(lock_path(dir, id))

      # The outcome the caller asked for already holds. Not an error.
      assert :ok = Local.release_lease(id, lease)
    end

    test "releasing a lock that belongs to someone else is a no-op, not an error", ctx do
      %{dir: dir, id: id} = ctx
      {:ok, mine} = Local.acquire_lease(id, "node_a", 30_000)
      :ok = Local.release_lease(id, mine)
      {:ok, theirs} = Local.acquire_lease(id, "node_b", 30_000)

      assert :ok = Local.release_lease(id, mine),
             "a stale owner's release must never delete the current holder's lock (finding #22)"

      assert :ok = Local.check_lease(id, theirs), "the current holder's lock was deleted"
      assert File.exists?(lock_path(dir, id))
    end
  end
end
