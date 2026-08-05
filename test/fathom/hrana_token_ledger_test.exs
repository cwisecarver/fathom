defmodule Fathom.HranaTokenLedgerTest do
  @moduledoc """
  Expert review 2026-08-01 **#37**: Hrana tokens never expired, nothing recorded which tokens had
  been issued, and the only fleet-wide revocation was rotating `secret_key_base` — an outage, not a
  revocation.

  The per-shard lifecycle was already good (mint, zero-downtime rotate, immediate revoke, `ro`
  scope). What was missing was everything ABOVE one shard, which is exactly the shape of an ordinary
  month-one incident: "a laptop with tokens on it was lost last Tuesday" had no answer, because
  there was no list of what had been issued and no way to act on a time window.
  """
  use Fathom.DataCase, async: false

  alias Fathom.Directory
  alias Fathom.HranaAuth
  alias Fathom.HranaAuth.Ledger

  defp shard(name), do: "led#{name}#{System.unique_integer([:positive])}"

  describe "the issuance ledger" do
    test "a mint is recorded with its claims" do
      id = shard("a")
      {:ok, _} = Directory.resolve(id)

      {:ok, _token} = HranaAuth.token_for(id, actor: "mix fathom.token")

      assert [issuance] = Ledger.history(id)
      assert issuance.shard_id == id
      assert issuance.scope == "rw"
      assert issuance.actor == "mix fathom.token"
      assert issuance.token_version >= 1
      assert issuance.minted_at
    end

    test "a read-only mint records its scope, so an audit can tell them apart" do
      id = shard("b")
      {:ok, _} = Directory.resolve(id)

      {:ok, _} = HranaAuth.token_for(id, scope: :ro, actor: "api")

      assert [%{scope: "ro"}] = Ledger.history(id)
    end

    # The secret is never persisted. A Hrana token is verified by SIGNATURE, not by lookup, so
    # storing anything derived from it would add a credential to steal while answering no question
    # the claims cannot — stricter than api_keys, which keeps a hash because those ARE looked up.
    test "no part of the token itself is stored" do
      id = shard("c")
      {:ok, _} = Directory.resolve(id)
      {:ok, token} = HranaAuth.token_for(id)

      [issuance] = Ledger.history(id)
      serialized = inspect(Map.from_struct(issuance))

      refute serialized =~ token
      refute String.contains?(serialized, String.slice(token, 0, 16))
    end

    # A mint must never fail because the ledger could not write. `mix fathom.token` runs with config
    # only and no Repo at all, and a Postgres blip must not stop an operator issuing a credential.
    test "a ledger failure does not fail the mint" do
      id = shard("d")

      # No directory row and a deliberately over-long actor: the changeset is invalid, so the insert
      # cannot succeed.
      log =
        ExUnit.CaptureLog.capture_log(fn ->
          assert {:ok, token} = HranaAuth.token_for(id, actor: String.duplicate("x", 500))
          assert is_binary(token)
        end)

      assert log =~ "NOT recording the mint",
             "a skipped ledger write must say so — a silent gap is what #37 is about"
    end

    # `outstanding/1` reinterprets append-only history against the CURRENT floor rather than
    # deleting rows, so the audit trail survives a revoke.
    test "a revoke moves issuances out of `outstanding` without erasing history" do
      id = shard("e")
      {:ok, _} = Directory.resolve(id)
      {:ok, _} = HranaAuth.token_for(id)

      assert length(Ledger.outstanding(id)) == 1

      {:ok, _version} = HranaAuth.revoke(id)

      assert Ledger.outstanding(id) == [], "the revoked issuance is no longer outstanding"
      assert length(Ledger.history(id)) == 1, "but it is still in the audit history"
    end
  end

  describe "time-scoped bulk revoke" do
    test "revokes only shards with an outstanding token minted before the cutoff" do
      old_shard = shard("old")
      new_shard = shard("new")
      {:ok, _} = Directory.resolve(old_shard)
      {:ok, _} = Directory.resolve(new_shard)
      {:ok, _} = HranaAuth.token_for(old_shard)
      {:ok, _} = HranaAuth.token_for(new_shard)

      backdate(old_shard, -3600)
      cutoff = DateTime.add(DateTime.utc_now(), -60, :second)

      assert {:ok, 1} = HranaAuth.revoke_issued_before(cutoff, async: false)

      assert Ledger.outstanding(old_shard) == [], "the pre-cutoff shard was revoked"
      assert length(Ledger.outstanding(new_shard)) == 1, "the post-cutoff shard was left alone"
    end

    # Idempotence is what makes this safe to retry or run from a cron: a revoke is cheap but not
    # free — it disconnects live clients — so a second sweep over the same window must do nothing.
    test "re-running the same cutoff is a no-op" do
      id = shard("idem")
      {:ok, _} = Directory.resolve(id)
      {:ok, _} = HranaAuth.token_for(id)
      backdate(id, -3600)
      cutoff = DateTime.add(DateTime.utc_now(), -60, :second)

      assert {:ok, 1} = HranaAuth.revoke_issued_before(cutoff, async: false)
      assert {:ok, 0} = HranaAuth.revoke_issued_before(cutoff, async: false)
    end

    # The blast-radius brake: an operator confirming a selection before committing to the rest.
    test "a limit caps how many shards one call touches" do
      ids = for n <- 1..3, do: shard("lim#{n}")

      for id <- ids do
        {:ok, _} = Directory.resolve(id)
        {:ok, _} = HranaAuth.token_for(id)
        backdate(id, -3600)
      end

      cutoff = DateTime.add(DateTime.utc_now(), -60, :second)
      assert {:ok, 1} = HranaAuth.revoke_issued_before(cutoff, async: false, limit: 1)
    end

    test "async mode enqueues a paced job per shard instead of revoking inline" do
      id = shard("async")
      {:ok, _} = Directory.resolve(id)
      {:ok, _} = HranaAuth.token_for(id)
      backdate(id, -3600)
      cutoff = DateTime.add(DateTime.utc_now(), -60, :second)

      assert {:ok, 1} = HranaAuth.revoke_issued_before(cutoff, async: true)

      assert length(Ledger.outstanding(id)) == 1,
             "async must not revoke inline — Oban paces it"

      assert [job] = Fathom.Repo.all(Oban.Job)
      assert job.worker == "Fathom.HranaAuth.RevokeJob"
      assert job.queue == "tokens"
      assert job.args["shard_id"] == id
    end
  end

  defp backdate(shard_id, seconds) do
    import Ecto.Query
    then = DateTime.add(DateTime.utc_now(), seconds, :second)

    Fathom.Repo.update_all(
      from(i in Fathom.HranaAuth.Issuance, where: i.shard_id == ^shard_id),
      set: [minted_at: then]
    )
  end
end
