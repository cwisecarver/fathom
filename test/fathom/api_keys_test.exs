defmodule Fathom.ApiKeysTest do
  @moduledoc """
  Scoped, revocable control-plane API keys (expert review #8): mint returns a one-time plaintext
  token (only its hash is stored), authenticate resolves a live key to `{name, scope}` and rejects
  unknown/revoked tokens, and `scope_at_least?` orders `read < manage < destroy`.
  """
  use Fathom.DataCase, async: true

  alias Fathom.ApiKeys
  alias Fathom.ApiKeys.ApiKey

  test "mint returns a one-time token and authenticate resolves it to {name, scope}" do
    {:ok, token, key} = ApiKeys.mint("ci-deploy", "manage")
    assert String.starts_with?(token, "fathom_")
    assert key.scope == "manage"
    assert {:ok, %{name: "ci-deploy", scope: "manage"}} = ApiKeys.authenticate(token)
  end

  test "authenticate rejects an unknown or revoked token" do
    assert :error = ApiKeys.authenticate("fathom_nope")

    {:ok, token, key} = ApiKeys.mint("x", "read")
    assert {:ok, _} = ApiKeys.authenticate(token)
    {:ok, _} = ApiKeys.revoke(key.id)
    assert :error = ApiKeys.authenticate(token), "a revoked key must stop authenticating"
  end

  test "the plaintext token is never stored — only its SHA-256 hash" do
    {:ok, token, key} = ApiKeys.mint("x", "read")
    reloaded = Repo.get(ApiKey, key.id)
    refute reloaded.token_hash == token
    assert reloaded.token_hash =~ ~r/^[0-9a-f]{64}$/
  end

  test "scope_at_least? orders read < manage < destroy" do
    assert ApiKeys.scope_at_least?("destroy", "read")
    assert ApiKeys.scope_at_least?("destroy", "destroy")
    assert ApiKeys.scope_at_least?("manage", "read")
    refute ApiKeys.scope_at_least?("read", "manage")
    refute ApiKeys.scope_at_least?("manage", "destroy")
  end

  test "mint rejects an invalid scope" do
    assert {:error, changeset} = ApiKeys.mint("x", "superuser")
    assert %{scope: _} = errors_on(changeset)
  end
end
