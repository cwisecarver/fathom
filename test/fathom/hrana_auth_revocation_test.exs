defmodule Fathom.HranaAuthRevocationTest do
  # Expert review #31: pre-fix a Hrana token carried only the shard id signed with
  # the web secret_key_base, so the ONLY revocation was rotating that secret —
  # fleet-wide, and it also logged out the dashboard. Now a token embeds the shard's
  # token_version and signs with a dedicated secret: revoking ONE shard invalidates
  # its outstanding tokens alone, and rotating the data-path secret never touches web
  # sessions. DataCase (async: false): reads/writes the directory token_version and
  # flips app env; the Revocations cache is a shared app-global ETS table.
  use Fathom.DataCase, async: false

  alias Fathom.{Directory, HranaAuth}
  alias Fathom.HranaAuth.Revocations

  setup do
    prev_mode = Application.get_env(:fathom, :hrana_auth, :disabled)
    prev_secret = Application.get_env(:fathom, :hrana_token_secret)
    Application.put_env(:fathom, :hrana_auth, :required)

    on_exit(fn ->
      Application.put_env(:fathom, :hrana_auth, prev_mode)

      if is_nil(prev_secret),
        do: Application.delete_env(:fathom, :hrana_token_secret),
        else: Application.put_env(:fathom, :hrana_token_secret, prev_secret)
    end)

    :ok
  end

  defp uniq, do: "rev_#{System.unique_integer([:positive])}"

  test "revoking a shard invalidates its outstanding tokens" do
    shard = uniq()
    {:ok, _} = Directory.resolve(shard)
    {:ok, token} = HranaAuth.token_for(shard)

    assert HranaAuth.authorize(shard, token) == :ok

    assert {:ok, 2} = HranaAuth.revoke(shard)

    assert {:error, %Filo.Error{status: 401}} = HranaAuth.authorize(shard, token),
           "a token minted before the revoke must stop verifying"

    # A freshly-minted token embeds the new version and works again.
    {:ok, token2} = HranaAuth.token_for(shard)
    assert HranaAuth.authorize(shard, token2) == :ok
  end

  test "revoking one shard does not affect another shard's tokens" do
    a = uniq()
    b = uniq()
    {:ok, _} = Directory.resolve(a)
    {:ok, _} = Directory.resolve(b)
    {:ok, token_b} = HranaAuth.token_for(b)

    {:ok, _} = HranaAuth.revoke(a)

    assert HranaAuth.authorize(b, token_b) == :ok,
           "revoking shard A must not invalidate shard B's tokens"
  end

  test "the version floor read fails open on an unknown shard (no directory row)" do
    # A validly-signed token for a shard with no directory row still opens — the
    # floor reads 0 (fail-open), and the signature is the enforced control.
    shard = uniq()
    {:ok, token} = HranaAuth.token_for(shard)
    Revocations.put(shard, 0)

    assert HranaAuth.authorize(shard, token) == :ok
  end

  test "tokens sign with the dedicated secret, independent of secret_key_base" do
    shard = uniq()
    {:ok, _} = Directory.resolve(shard)

    Application.put_env(:fathom, :hrana_token_secret, String.duplicate("a", 64))
    {:ok, token} = HranaAuth.token_for(shard)
    assert HranaAuth.authorize(shard, token) == :ok

    # Rotating ONLY the dedicated secret invalidates the token — proving it signs
    # with that secret, not secret_key_base (which is untouched here).
    Application.put_env(:fathom, :hrana_token_secret, String.duplicate("b", 64))

    assert {:error, %Filo.Error{status: 401}} = HranaAuth.authorize(shard, token),
           "the token must be signed with the dedicated secret, not secret_key_base"
  end
end
