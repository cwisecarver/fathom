defmodule Fathom.HranaAuthMintVersionTest do
  @moduledoc """
  Expert review 2026-11-07 #35. `token_for/2` read the shard's revocation version with
  `Directory.token_version(id) || 1`, rescuing every failure to 1. For a shard at floor 5 a Postgres
  read failure therefore minted a v1 token that `authorize/2` refused as revoked — and the API answered
  200 with it (`POST /api/tenants/:id/token`), as did a `rotate` whose bump had already landed (the
  old token also dies when its grace ends). `Mix.Tasks.Fathom.Token` had already learned this the hard
  way and refuses; the shared mint did not.

  A failed read is now `{:error, :version_unavailable}` (the API maps it to 503), and `rotate/2`
  mints at the version its own bump returned instead of reading the directory a second time.
  """
  use Fathom.DataCase, async: false

  alias Fathom.{Directory, HranaAuth}
  alias Fathom.HranaAuth.Revocations

  setup do
    prev_mode = Application.get_env(:fathom, :hrana_auth, :disabled)
    Application.put_env(:fathom, :hrana_auth, :required)
    on_exit(fn -> Application.put_env(:fathom, :hrana_auth, prev_mode) end)
    %{shard: "mintver_#{System.unique_integer([:positive])}"}
  end

  # Cut every process off from Postgres (the pattern `fathom_token_test` uses): the directory read
  # raises `DBConnection.OwnershipError`. The owner is restored for the sandbox teardown.
  defp cut_postgres! do
    Ecto.Adapters.SQL.Sandbox.mode(Fathom.Repo, :manual)

    on_exit(fn ->
      owner = Ecto.Adapters.SQL.Sandbox.start_owner!(Fathom.Repo, shared: true)
      Ecto.Adapters.SQL.Sandbox.stop_owner(owner)
    end)
  end

  test "an unreadable directory is an error, not a version-1 token", %{shard: shard} do
    {:ok, _} = Directory.resolve(shard)
    {:ok, 2} = Directory.bump_token_version(shard)
    {:ok, 3} = Directory.bump_token_version(shard)

    assert {:ok, good} = HranaAuth.token_for(shard)
    assert {:ok, _} = HranaAuth.authorize(shard, good), "fixture: a normal mint verifies"

    cut_postgres!()

    assert {:error, :version_unavailable} = HranaAuth.token_for(shard)
    assert {:error, :version_unavailable} = HranaAuth.token_for(shard, scope: :ro, actor: "x")
  end

  test "a pinned version is embedded without consulting the directory", %{shard: shard} do
    {:ok, _} = Directory.resolve(shard)
    {:ok, 2} = Directory.bump_token_version(shard)
    {:ok, 3} = Directory.bump_token_version(shard)
    {:ok, 4} = Directory.bump_token_version(shard)

    # Seed the node's cache so `authorize/2` needs no Postgres either: the floor is 4.
    Revocations.put(shard, 4, nil)
    cut_postgres!()

    # This is what `rotate/2` does after its own bump landed: the token carries the BUMP's version.
    assert {:ok, token} = HranaAuth.token_for(shard, version: 4)
    assert {:ok, _} = HranaAuth.authorize(shard, token)

    assert {:ok, stale} = HranaAuth.token_for(shard, version: 3)

    assert {:error, _} = HranaAuth.authorize(shard, stale),
           "control: a token below the floor must be refused, or the assertion above proves nothing"
  end

  test "rotate mints at the version its bump returned", %{shard: shard} do
    {:ok, _} = Directory.resolve(shard)
    {:ok, 2} = Directory.bump_token_version(shard)

    assert {:ok, token} = HranaAuth.rotate(shard)
    assert Directory.token_version(shard) == 3
    assert {:ok, _} = HranaAuth.authorize(shard, token)
  end

  test "a shard the directory never recorded still mints at the default version", %{shard: shard} do
    assert Directory.token_version(shard) == nil
    assert {:ok, token} = HranaAuth.token_for(shard)
    assert {:ok, _} = HranaAuth.authorize(shard, token)
  end
end
