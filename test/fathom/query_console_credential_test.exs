defmodule Fathom.QueryConsoleCredentialTest do
  @moduledoc """
  What the admin query console MINTS on every execution (expert review 2026-08-20 #32).

  `auth_header/1` was a bare `HranaAuth.token_for(shard_id)`, so `scope` defaulted to `:rw` even
  for a `SELECT`, `actor` defaulted to `nil`, and the token was valid for the whole
  `:hrana_token_max_age`. Two consequences:

    * **Credential sprawl.** An operator triaging ten tenants left ten live full-access tenant
      credentials, each valid for the configured max-age, with nothing tying them to a person.
      `AdminTenantController.export` was given per-operator attribution precisely because it
      touches tenant data; the console mints credentials TO tenant data and had none.

    * **It corrupted the input to the fleet-wide revoke.** `revoke_issued_before/2` bumps the floor
      on every shard the ledger shows with an outstanding token issued before the cutoff. After a
      week of console use that set is every tenant an operator ever looked at, so an
      incident-response sweep scoped to one leaked laptop would disconnect a large, arbitrary
      slice of the fleet. The ledger's "under-reports, which is the safe direction" reasoning is
      defeated by a writer that OVER-reports.

  Asserted through the ISSUANCE LEDGER rather than by decoding the token, because the ledger is
  what the revoke sweep actually reads — it is the consumer whose input was being poisoned.
  """
  use Fathom.DataCase, async: false

  alias Fathom.Bench.HranaClient
  alias Fathom.Directory
  alias Fathom.HranaAuth
  alias Fathom.HranaAuth.Ledger
  alias Fathom.{QueryConsole, Shards}

  setup do
    prev_auth = Application.get_env(:fathom, :hrana_auth)
    prev_age = Application.get_env(:fathom, :hrana_token_max_age)

    # The console only mints when auth is ON; with `:disabled` it sends no header at all.
    Application.put_env(:fathom, :hrana_auth, :required)
    Application.put_env(:fathom, :hrana_token_max_age, 86_400)

    {:ok, sup, port} = HranaClient.start_listener()
    id = "qcc_#{System.unique_integer([:positive])}"
    {:ok, _} = Directory.resolve(id)

    on_exit(fn ->
      Shards.drain(id, 5_000)

      for e <- ["", "-wal", "-shm"], do: File.rm(Fathom.Shard.db_path(id) <> e)

      HranaClient.stop_listener(sup)

      for {k, v} <- [hrana_auth: prev_auth, hrana_token_max_age: prev_age] do
        if is_nil(v),
          do: Application.delete_env(:fathom, k),
          else: Application.put_env(:fathom, k, v)
      end
    end)

    %{endpoint: "http://127.0.0.1:#{port}", id: id}
  end

  test "a SELECT mints a READ-ONLY credential", ctx do
    %{endpoint: ep, id: id} = ctx

    assert {:ok, _} =
             QueryConsole.run(id, "CREATE TABLE t (a)", endpoint: ep, actor: "console:alice")

    assert {:ok, _} =
             QueryConsole.run(id, "SELECT * FROM t", endpoint: ep, actor: "console:alice")

    scopes = Ledger.history(id) |> Enum.map(& &1.scope)

    assert "ro" in scopes,
           "the console minted a full read-write tenant credential to run a SELECT"
  end

  test "a write still mints read-write, so the console keeps working", ctx do
    %{endpoint: ep, id: id} = ctx

    assert {:ok, _} = QueryConsole.run(id, "CREATE TABLE t (a)", endpoint: ep)
    assert {:ok, r} = QueryConsole.run(id, "INSERT INTO t VALUES (1)", endpoint: ep)
    assert r.affected_row_count == 1

    # Narrowing the scope must never break the console's documented purpose: it runs arbitrary
    # SQL including writes and DDL.
    assert Enum.any?(Ledger.history(id), &(&1.scope == "rw"))
  end

  test "anything unrecognised stays read-write — the classifier is conservative", ctx do
    %{endpoint: ep, id: id} = ctx

    # `WITH` is deliberately NOT treated as a read, for the reason Migrator.Capture records:
    # `WITH ... INSERT` is valid SQLite. Misreading a write as a read would be a 403 the operator
    # cannot work around.
    assert {:ok, _} = QueryConsole.run(id, "CREATE TABLE t (a)", endpoint: ep)
    assert {:ok, _} = QueryConsole.run(id, "WITH x AS (SELECT 1) SELECT * FROM x", endpoint: ep)

    assert Enum.all?(Ledger.history(id), &(&1.scope == "rw")),
           "a statement the classifier does not recognise was optimistically narrowed to :ro"
  end

  test "the mint is attributed to the operator, not to nobody", ctx do
    %{endpoint: ep, id: id} = ctx

    assert {:ok, _} = QueryConsole.run(id, "SELECT 1", endpoint: ep, actor: "console:alice")

    assert [%{actor: "console:alice"}] = Ledger.history(id),
           "an unattributed console mint is both credential sprawl and a poisoned input to " <>
             "revoke_issued_before/2, which reads exactly this table"
  end

  test "with no actor supplied it is still separable from an operator export", ctx do
    %{endpoint: ep, id: id} = ctx

    assert {:ok, _} = QueryConsole.run(id, "SELECT 1", endpoint: ep)

    # The `console:` prefix is distinct from AdminTenantController's `admin:` on purpose, so a
    # future Ledger.shards_issued_before/1 can exclude console mints outright.
    assert [%{actor: "console"}] = Ledger.history(id)
  end

  describe "token_for/2 :ttl" do
    test "a ttl makes one token short-lived without touching the global max-age" do
      id = "qcttl_#{System.unique_integer([:positive])}"
      {:ok, _} = Directory.resolve(id)

      # Phoenix.Token has no per-token expiry -- max_age is a VERIFY-side option and this module
      # applies one global value -- so the only mechanism is back-dating signed_at. ttl: 0 means
      # "already fully spent", which is the sharpest possible check that the back-dating happened.
      {:ok, spent} = HranaAuth.token_for(id, ttl: 0)
      assert {:error, _} = HranaAuth.authorize(id, spent)

      {:ok, fresh} = HranaAuth.token_for(id)
      assert {:ok, _} = HranaAuth.authorize(id, fresh)
    end

    test "a ttl is inert when nothing expires anyway" do
      prev = Application.get_env(:fathom, :hrana_token_max_age)
      Application.delete_env(:fathom, :hrana_token_max_age)
      on_exit(fn -> if prev, do: Application.put_env(:fathom, :hrana_token_max_age, prev) end)

      id = "qcinf_#{System.unique_integer([:positive])}"
      {:ok, _} = Directory.resolve(id)

      # With max_age :infinity there is nothing to make expire sooner. Back-dating anyway would be
      # a silent no-op that reads as a control; it is one, and this pins that it is honest about it.
      {:ok, token} = HranaAuth.token_for(id, ttl: 0)
      assert {:ok, _} = HranaAuth.authorize(id, token)
    end
  end
end
