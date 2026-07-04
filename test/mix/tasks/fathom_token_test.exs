defmodule Mix.Tasks.Fathom.TokenTest do
  # Round-2 expert review #31: the task ran with `app.config` only — no Repo — so
  # `token_version/1` raised and `current_token_version` rescued to v=1. For a shard
  # revoked to floor ≥ 2, the printed token was DEAD ON ARRIVAL (1 >= floor is
  # false), presented to the operator as success. The invariant: a minted token
  # embeds the shard's CURRENT revocation floor and actually authorizes.
  use Fathom.DataCase, async: false

  alias Fathom.{Directory, HranaAuth}

  setup do
    prev_shell = Mix.shell()
    Mix.shell(Mix.Shell.Process)
    prev_mode = Application.get_env(:fathom, :hrana_auth, :disabled)
    Application.put_env(:fathom, :hrana_auth, :required)

    on_exit(fn ->
      Mix.shell(prev_shell)
      Application.put_env(:fathom, :hrana_auth, prev_mode)
    end)

    :ok
  end

  test "mints a WORKING token for a previously-revoked shard" do
    shard = "tok_#{System.unique_integer([:positive])}"
    {:ok, _} = Directory.resolve(shard)
    {:ok, 2} = HranaAuth.revoke(shard)

    Mix.Tasks.Fathom.Token.run([shard])
    assert_received {:mix_shell, :info, [token]}

    assert HranaAuth.authorize(shard, token) == :ok,
           "the minted token must embed the CURRENT revocation floor (pre-fix: v=1, dead on arrival)"
  end

  # The discriminator (the task's real-world condition is NO usable Repo): when the
  # revocation floor cannot be read, the task must refuse loudly — pre-fix it
  # silently rescued to v=1 and printed the maybe-dead token as success.
  test "refuses to mint when the revocation floor is unreadable" do
    shard = "tok_#{System.unique_integer([:positive])}"

    # Cut this process off from Postgres: the floor probe raises.
    Ecto.Adapters.SQL.Sandbox.mode(Fathom.Repo, :manual)

    assert_raise Mix.Error, ~r/revocation floor/, fn ->
      Mix.Tasks.Fathom.Token.run([shard])
    end

    refute_received {:mix_shell, :info, [_token]},
                    "no token may be printed when the floor is unreadable"

    owner = Ecto.Adapters.SQL.Sandbox.start_owner!(Fathom.Repo, shared: true)
    on_exit(fn -> Ecto.Adapters.SQL.Sandbox.stop_owner(owner) end)
  end
end
