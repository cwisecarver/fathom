defmodule Fathom.Tenants.ForkDeleteRaceTest do
  @moduledoc """
  Expert review 2026-11-07 #16 (tenant-lifecycle#1). `Tenants.fork/3` read the source's directory row
  and the tombstone set ONCE, before the optional source flush and the dst lease. `delete/1`
  tombstones synchronously but erases asynchronously, so a delete landing in that window left the
  source's object in place; the atomic server-side copy then put the erased tenant's bytes under a
  NEW id, `register_fork/1` made it an `active` row and `tenant_result/3` handed out an `rw` token —
  erased personal data outside every deletion guarantee, with no tombstone, marker or purge.
  `Snapshots.create/restore` re-check after their copy (reviews 2026-10-12 #20 / 10-15 #13); fork now
  does too: after the copy with the dst lease held, and again before the row is registered.

  The delete is injected at exactly the point the copy starts, through `FaultyStorage`'s
  `:faulty_before` hook (`:fork_shard` is handed the dst id) — no sleeps, no gates. `Storage.Local`'s
  `fork_shard` copies regardless of tombstones, so a fork that did not re-check provably completes.
  """
  use Fathom.DataCase, async: false
  use Oban.Testing, repo: Fathom.Repo

  alias Fathom.{Directory, HranaAuth, Tenants}
  alias Fathom.Shard.{Connection, Storage}
  alias Fathom.Test.FaultyStorage
  alias Fathom.Tenants.Tombstones

  setup do
    prev = Application.get_env(:fathom, :shard_storage)
    Application.put_env(:fathom, :shard_storage, FaultyStorage)

    n = System.unique_integer([:positive])
    src = "forkrace_src_#{n}"
    dst = "forkrace_dst_#{n}"

    on_exit(fn ->
      Application.delete_env(:fathom, :faulty_before)

      if prev,
        do: Application.put_env(:fathom, :shard_storage, prev),
        else: Application.delete_env(:fathom, :shard_storage)

      for id <- [src, dst] do
        :ets.delete(Tombstones, id)
        Storage.purge_shard(id)
        File.rm(Path.join([Storage.Local.dir(), "tombstones", id]))
        File.rm(Path.join([Storage.Local.dir(), "tokenfloors", id]))
      end
    end)

    {:ok, _} = Directory.resolve(src)
    seed_object(src)
    assert {:ok, true} = Storage.any_shard_object?(src), "fixture: the source must be stored"
    assert {:ok, false} = Storage.any_shard_object?(dst), "fixture: the destination must be free"

    %{src: src, dst: dst}
  end

  defp seed_object(shard) do
    tmp = Path.join(System.tmp_dir!(), "seed_#{shard}_#{System.unique_integer([:positive])}.db")
    {:ok, conn} = Connection.open(tmp)
    {:ok, _} = Connection.query(conn, "CREATE TABLE people (name TEXT)", [])
    {:ok, _} = Connection.query(conn, "INSERT INTO people VALUES ('erased-person')", [])
    :ok = Connection.close(conn)
    :ok = Storage.flush(shard, tmp)
    for s <- ["", "-wal", "-shm"], do: File.rm(tmp <> s)
  end

  # Runs `fun` at the instant the copy for `dst` begins (inside `Storage.fork_shard/2`).
  defp at_copy(dst, fun) do
    test = self()

    Application.put_env(
      :fathom,
      :faulty_before,
      {:fork_shard,
       fn
         ^dst ->
           send(test, :copy_started)
           fun.()

         _other ->
           :ok
       end}
    )
  end

  test "a source deleted mid-fork: the copy is purged, no row, no token", %{src: src, dst: dst} do
    at_copy(dst, fn -> assert {:ok, :scheduled} = Tenants.delete(src) end)

    assert {:error, :tombstoned} = Tenants.fork(src, dst)

    assert_received :copy_started

    assert {:ok, true} = Storage.any_shard_object?(src),
           "fixture: delete's erase is async (queued)"

    assert {:ok, false} = Storage.any_shard_object?(dst),
           "the erased tenant's bytes live on under the destination id"

    assert :error = Directory.get(dst), "a live row was registered for the erased copy"
    assert [] = HranaAuth.Ledger.history(dst), "a token was minted for the erased copy"
    refute Tombstones.tombstoned?(dst), "the destination id must stay free (not tombstoned)"
  end

  test "a destination deleted mid-fork: the copy is purged and the fork refused",
       %{src: src, dst: dst} do
    at_copy(dst, fn -> assert {:ok, :scheduled} = Tenants.delete(dst) end)

    assert {:error, :tombstoned} = Tenants.fork(src, dst)

    assert_received :copy_started
    assert {:ok, false} = Storage.any_shard_object?(dst)
    # The row is the DELETE's tombstone, not a fork registration: it must still read `deleted`.
    assert {:ok, %{status: "deleted"}} = Directory.get(dst)
  end

  test "control: no delete, the fork completes and the copy is kept", %{src: src, dst: dst} do
    at_copy(dst, fn -> :ok end)

    assert {:ok, %{shard_id: ^dst}} = Tenants.fork(src, dst)

    assert_received :copy_started
    assert {:ok, true} = Storage.any_shard_object?(dst)
    assert {:ok, %{status: "active"}} = Directory.get(dst)
  end

  test "the source's neighbour is unaffected: deleting `src` mid-fork never touches a sibling id",
       %{src: src, dst: dst} do
    sibling = dst <> "9"
    {:ok, _} = Directory.resolve(sibling)
    seed_object(sibling)
    on_exit(fn -> Storage.purge_shard(sibling) end)

    at_copy(dst, fn -> assert {:ok, :scheduled} = Tenants.delete(src) end)
    assert {:error, :tombstoned} = Tenants.fork(src, dst)

    assert {:ok, true} = Storage.any_shard_object?(sibling), "purging dst touched `dst9`"
  end
end
