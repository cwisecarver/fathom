defmodule Fathom.Tenants.TokenLifecycleTest do
  @moduledoc """
  Expert review 2026-11-07 #1 (with #30): `Tenants.delete/1` ended a tenant's DATA but not its
  CREDENTIALS. Every token stayed valid after the delete, and when the id was later freed and reused
  (the DR runbook's recipe: hard-delete the row, drop the storage marker, restart) the old holder's
  tokens verified against the NEW tenant's database. The opposite "fix" — revoking without seeding the
  new row — strands the new tenant instead: a fresh row starts at version 1, below the durable floor.

  So both halves are pinned together, end to end through `HranaAuth.authorize/2` on a COLD node (the
  revocation cache cleared, so the floor is `max(directory, storage)`), on BOTH storage backends:
  Local, and the real S3 client over the `S3EtagStore` double.

  The id-reuse simulation drops the directory row and the node-local tombstone gate. It does not
  need to drop the storage marker: `provision/2`'s re-mint guard consults the directory row and the
  ETS set, and the marker only repopulates the set at boot.
  """
  use Fathom.DataCase, async: false
  use Oban.Testing, repo: Fathom.Repo

  alias Fathom.{Directory, HranaAuth, Tenants}
  alias Fathom.HranaAuth.Revocations
  alias Fathom.Shard.{Connection, Storage}
  alias Fathom.Shard.Storage.{Local, S3}
  alias Fathom.Tenants.Tombstones
  alias Fathom.Test.S3EtagStore

  defp configure(:local), do: :ok

  defp configure(:s3) do
    prev = for k <- [:shard_storage, S3], do: {k, Application.get_env(:fathom, k)}

    on_exit(fn ->
      for {k, v} <- prev,
          do:
            if(is_nil(v),
              do: Application.delete_env(:fathom, k),
              else: Application.put_env(:fathom, k, v)
            )
    end)

    store = start_supervised!({Agent, fn -> S3EtagStore.initial(%{}) end})

    Application.put_env(:fathom, S3,
      bucket: "b",
      region: "us-east-1",
      access_key_id: "k",
      secret_access_key: "s",
      endpoint: "https://s3.example",
      path_style: true,
      req_plug: fn conn -> S3EtagStore.serve(conn, store) end
    )

    Application.put_env(:fathom, :shard_storage, S3)
    :ok
  end

  defp new_id(prefix), do: "#{prefix}#{System.unique_integer([:positive])}"

  # The id is gone for good: drop the row and the node-local gate, the part of the DR runbook's reuse
  # recipe that `provision/2` actually consults. The revocation cache is dropped as a cold node has it.
  defp free_id!(id) do
    assert 1 == Directory.hard_delete(id)
    :ets.delete(Tombstones, id)
    :ets.delete(Revocations, id)
    :ok
  end

  # A real SQLite object at `shard`, so `Tenants.fork/3` has something to copy.
  defp seed_object(shard) do
    tmp = Path.join(System.tmp_dir!(), "seed_#{shard}_#{System.unique_integer([:positive])}.db")
    {:ok, conn} = Connection.open(tmp)
    {:ok, _} = Connection.query(conn, "CREATE TABLE t (x)", [])
    :ok = Connection.close(conn)
    :ok = Storage.flush(shard, tmp)
    for s <- ["", "-wal", "-shm"], do: File.rm(tmp <> s)
  end

  for backend <- [:local, :s3] do
    describe "#{backend}: a deleted tenant's credentials die with it, and a reused id starts clean" do
      setup do
        prev_mode = Application.get_env(:fathom, :hrana_auth, :disabled)
        prev_age = Application.get_env(:fathom, :hrana_token_max_age, :infinity)
        Application.put_env(:fathom, :hrana_auth, :required)
        # Prod under :required mandates a finite max age; the reuse hole is bounded by it.
        Application.put_env(:fathom, :hrana_token_max_age, 3_600)
        :ok = configure(unquote(backend))

        id = new_id("reuse")
        neighbour = id <> "2"

        on_exit(fn ->
          Application.put_env(:fathom, :hrana_auth, prev_mode)
          Application.put_env(:fathom, :hrana_token_max_age, prev_age)

          for shard <- [id, neighbour] do
            :ets.delete(Tombstones, shard)
            :ets.delete(Revocations, shard)
            File.rm(Path.join([Local.dir(), "tombstones", shard]))
            File.rm(Path.join([Local.dir(), "tokenfloors", shard]))
          end
        end)

        %{id: id, neighbour: neighbour}
      end

      test "delete revokes every outstanding token, in the directory AND the durable floor",
           %{id: id} do
        {:ok, tenant} = Tenants.provision(id)
        old = tenant.auth_token

        assert {:ok, _} = HranaAuth.authorize(id, old), "fixture: the token must work pre-delete"
        assert {:ok, nil} = Storage.read_token_floor(id), "fixture: nothing mirrored yet"

        assert {:ok, :scheduled} = Tenants.delete(id)

        assert {:error, _} = HranaAuth.authorize(id, old),
               "delete left the tenant's token valid (it would verify against a reused id)"

        version = Directory.token_version(id)
        assert version >= 2

        assert {:ok, ^version} = Storage.read_token_floor(id),
               "the floor must be durable in storage: it is all that survives the row's hard-delete"
      end

      test "a reused id rejects the old holder's token and mints a working one", %{id: id} do
        {:ok, tenant} = Tenants.provision(id)
        old = tenant.auth_token
        assert {:ok, :scheduled} = Tenants.delete(id)
        {:ok, floor} = Storage.read_token_floor(id)
        assert floor >= 2

        free_id!(id)

        assert {:ok, reborn} = Tenants.provision(id)

        assert Directory.token_version(id) == floor + 1,
               "a new row must be seeded from the durable floor, not start at 1"

        assert {:error, _} = HranaAuth.authorize(id, old),
               "the previous holder's token verifies against the reused tenant's database"

        assert {:ok, _} = HranaAuth.authorize(id, reborn.auth_token),
               "the new tenant is locked out: its token embeds a version below the durable floor"
      end

      test "the seed reads the MAX: a delete re-run bumps again and the reuse still starts above it",
           %{id: id} do
        {:ok, _} = Tenants.provision(id)
        assert {:ok, :scheduled} = Tenants.delete(id)
        assert {:ok, :scheduled} = Tenants.delete(id)
        {:ok, floor} = Storage.read_token_floor(id)
        assert floor >= 3, "each delete run is one more bump"

        free_id!(id)
        assert {:ok, reborn} = Tenants.provision(id)

        assert Directory.token_version(id) == floor + 1
        assert {:ok, _} = HranaAuth.authorize(id, reborn.auth_token)
      end

      test "deleting one tenant never moves its neighbour's floor or tokens (shard isolation)",
           %{id: id, neighbour: neighbour} do
        {:ok, _} = Tenants.provision(id)
        {:ok, other} = Tenants.provision(neighbour)

        assert {:ok, :scheduled} = Tenants.delete(id)

        assert {:ok, _} = HranaAuth.authorize(neighbour, other.auth_token)
        assert Directory.token_version(neighbour) == 1
        assert {:ok, nil} = Storage.read_token_floor(neighbour)
      end

      test "a fork into a reused id is seeded the same way", %{id: id, neighbour: dst} do
        # `dst` is the id with a prior holder: its floor is already durable at 4.
        :ok = Storage.put_token_floor(dst, 4)
        {:ok, _} = Directory.resolve(id)
        seed_object(id)

        assert {:ok, forked} = Tenants.fork(id, dst)

        assert Directory.token_version(dst) == 5
        assert {:ok, _} = HranaAuth.authorize(dst, forked.auth_token)
      end

      test "purge re-drives the durable floor mirror a delete-time write may have missed",
           %{id: id} do
        # The state a delete leaves when the revoke's best-effort mirror PUT failed: the directory
        # row is bumped, storage holds nothing. `purge/1` runs under the DeleteJob's retries.
        {:ok, _} = Directory.resolve(id)
        {:ok, 2} = Directory.bump_token_version(id)
        {:ok, 3} = Directory.bump_token_version(id)
        {:ok, _} = Directory.tombstone(id)
        Tombstones.put(id)
        assert {:ok, nil} = Storage.read_token_floor(id), "fixture: the mirror never landed"

        assert :ok = Tenants.purge(id)

        assert {:ok, 3} = Storage.read_token_floor(id)
      end
    end
  end

  describe "provision refuses to birth a tenant blind" do
    setup do
      id = new_id("blind")

      on_exit(fn ->
        File.rm_rf(Path.join([Local.dir(), "tokenfloors", id]))
        :ets.delete(Tombstones, id)
      end)

      %{id: id}
    end

    test "an unreadable durable floor fails the provision and rolls the row back", %{id: id} do
      # A DIRECTORY at the floor's path: `File.read/1` answers `:eisdir`, which is a store error
      # (not "no floor"), the same shape as an S3 5xx.
      path = Path.join([Local.dir(), "tokenfloors", id])
      File.mkdir_p!(path)
      assert {:error, :eisdir} = Storage.read_token_floor(id), "fixture: the read must fail"

      assert {:error, {:token_floor_unreadable, :eisdir}} = Tenants.provision(id)

      assert :error = Directory.get(id), "a row born without its floor is the cross-tenant hole"
    end
  end
end
