defmodule Fathom.HranaAuthRotationTest do
  @moduledoc """
  Zero-downtime token rotation (expert review 2026-07-14 #24): `rotate/1` mints a new token while
  keeping the PREVIOUS version valid for a grace window (mint-new → deploy → the old auto-hardens
  out), whereas `revoke/1` is immediate. DataCase (async: false): reads/writes the directory token
  columns, flips app env, and the Revocations cache is a shared app-global ETS table.
  """
  use Fathom.DataCase, async: false

  alias Fathom.{Directory, HranaAuth}

  setup do
    prev_mode = Application.get_env(:fathom, :hrana_auth, :disabled)
    prev_grace = Application.get_env(:fathom, :hrana_rotation_grace_ms)
    Application.put_env(:fathom, :hrana_auth, :required)

    on_exit(fn ->
      Application.put_env(:fathom, :hrana_auth, prev_mode)

      if is_nil(prev_grace),
        do: Application.delete_env(:fathom, :hrana_rotation_grace_ms),
        else: Application.put_env(:fathom, :hrana_rotation_grace_ms, prev_grace)
    end)

    :ok
  end

  defp uniq, do: "rot_#{System.unique_integer([:positive])}"

  test "rotate mints a working new token AND keeps the old valid during the grace window" do
    Application.put_env(:fathom, :hrana_rotation_grace_ms, 3_600_000)
    shard = uniq()
    {:ok, _} = Directory.resolve(shard)
    {:ok, old} = HranaAuth.token_for(shard)
    assert HranaAuth.authorize(shard, old) == :ok

    assert {:ok, new} = HranaAuth.rotate(shard)
    # Zero downtime: the new token works immediately AND the old keeps working during grace.
    assert HranaAuth.authorize(shard, new) == :ok
    assert HranaAuth.authorize(shard, old) == :ok
  end

  test "once the grace window has elapsed the old token stops verifying" do
    Application.put_env(:fathom, :hrana_rotation_grace_ms, 0)
    shard = uniq()
    {:ok, _} = Directory.resolve(shard)
    {:ok, old} = HranaAuth.token_for(shard)

    assert {:ok, new} = HranaAuth.rotate(shard)
    assert HranaAuth.authorize(shard, new) == :ok

    assert {:error, %Filo.Error{status: 401}} = HranaAuth.authorize(shard, old),
           "with the grace window elapsed the previous version must be refused"
  end

  test "revoke is immediate — the previous version dies even with a long grace configured" do
    Application.put_env(:fathom, :hrana_rotation_grace_ms, 3_600_000)
    shard = uniq()
    {:ok, _} = Directory.resolve(shard)
    {:ok, old} = HranaAuth.token_for(shard)

    assert {:ok, _} = HranaAuth.revoke(shard)

    assert {:error, %Filo.Error{status: 401}} = HranaAuth.authorize(shard, old),
           "a hard revoke clears the grace instant — no two-generation window"
  end

  test "only the immediately-previous version is covered by the grace, not older ones" do
    Application.put_env(:fathom, :hrana_rotation_grace_ms, 3_600_000)
    shard = uniq()
    {:ok, _} = Directory.resolve(shard)
    {:ok, v1} = HranaAuth.token_for(shard)

    {:ok, _v2} = HranaAuth.rotate(shard)
    assert HranaAuth.authorize(shard, v1) == :ok, "v1 is floor-1 → covered by grace"

    {:ok, _v3} = HranaAuth.rotate(shard)

    assert {:error, %Filo.Error{status: 401}} = HranaAuth.authorize(shard, v1),
           "v1 is now two generations back (floor-2) → outside the single-generation grace"
  end
end
