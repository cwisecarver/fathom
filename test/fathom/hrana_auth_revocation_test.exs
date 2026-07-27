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

    assert {:ok, _} = HranaAuth.authorize(shard, token)

    assert {:ok, 2} = HranaAuth.revoke(shard)

    assert {:error, %Filo.Error{status: 401}} = HranaAuth.authorize(shard, token),
           "a token minted before the revoke must stop verifying"

    # A freshly-minted token embeds the new version and works again.
    {:ok, token2} = HranaAuth.token_for(shard)
    assert {:ok, _} = HranaAuth.authorize(shard, token2)
  end

  test "revoking one shard does not affect another shard's tokens" do
    a = uniq()
    b = uniq()
    {:ok, _} = Directory.resolve(a)
    {:ok, _} = Directory.resolve(b)
    {:ok, token_b} = HranaAuth.token_for(b)

    {:ok, _} = HranaAuth.revoke(a)

    assert match?({:ok, _}, HranaAuth.authorize(b, token_b)),
           "revoking shard A must not invalidate shard B's tokens"
  end

  test "the version floor read fails open on an unknown shard (no directory row)" do
    # A validly-signed token for a shard with no directory row still opens — the
    # floor reads 0 (fail-open), and the signature is the enforced control.
    shard = uniq()
    {:ok, token} = HranaAuth.token_for(shard)
    Revocations.put(shard, 0)

    assert {:ok, _} = HranaAuth.authorize(shard, token)
  end

  # Round-2 #24: revoke/1 bumped the directory + the revoking node's ETS but pushed
  # nothing to other nodes, so a revoked/leaked token kept opening streams on every
  # OTHER node for up to a full cache TTL (30 s default). Fathom has no BEAM cluster
  # (PubSub is node-local), so the push rides Postgres LISTEN/NOTIFY (Oban.Notifier).
  # NOTIFY only fires on commit — invisible inside the test sandbox — so this drives
  # the receiving side directly with the notifier's message shape: the notification
  # must bump the local floor with NO directory read (the token 401s immediately).
  test "a revocation notification bumps the local floor without a directory read" do
    shard = uniq()
    {:ok, token} = HranaAuth.token_for(shard)
    Revocations.put(shard, 1)
    assert {:ok, _} = HranaAuth.authorize(shard, token)

    # Another node revoked: its notify arrives (no directory row exists here at all,
    # so any effect must come from the notification, not a read-through).
    send(
      Process.whereis(Revocations),
      {:notification, :fathom_revocations, %{"shard_id" => shard, "version" => 2}}
    )

    _ = :sys.get_state(Revocations)

    assert {:error, %Filo.Error{status: 401}} = HranaAuth.authorize(shard, token),
           "a revocation pushed from another node must take effect before the TTL"
  end

  test "a late or duplicate revocation notification never lowers the floor" do
    shard = uniq()

    send(
      Process.whereis(Revocations),
      {:notification, :fathom_revocations, %{"shard_id" => shard, "version" => 7}}
    )

    _ = :sys.get_state(Revocations)

    # A stale notification (an earlier revoke's notify delivered late).
    send(
      Process.whereis(Revocations),
      {:notification, :fathom_revocations, %{"shard_id" => shard, "version" => 3}}
    )

    _ = :sys.get_state(Revocations)

    assert Revocations.floor(shard) == 7,
           "a stale notification must never lower the floor this node already knows"
  end

  # Round-2 #25: a floor read-through error returned 0 ("no revocations") without
  # caching and with no last-known-good fallback — so a Postgres outage (natural or
  # attacker-induced) drove the floor to 0 for every expired-cache shard, and after
  # one TTL for ALL shards: the entire revocation mechanism silently disengaged and
  # every revoked/leaked credential worked again. An availability failure must not
  # be a security-control bypass: serve the stale floor (never weaker than what this
  # node knew), emit telemetry, and let :hrana_revocation_on_error pick the
  # no-prior-value posture.
  test "a floor-read outage serves the last-known-good floor, stale past its TTL" do
    import ExUnit.CaptureLog

    shard = uniq()
    prev_ttl = Application.get_env(:fathom, :hrana_revocation_ttl_ms)
    # TTL 0: the cached entry is immediately expired, forcing the read-through.
    Application.put_env(:fathom, :hrana_revocation_ttl_ms, 0)

    on_exit(fn ->
      if prev_ttl == nil,
        do: Application.delete_env(:fathom, :hrana_revocation_ttl_ms),
        else: Application.put_env(:fathom, :hrana_revocation_ttl_ms, prev_ttl)
    end)

    test_pid = self()
    handler_id = "floor-error-#{shard}"

    :telemetry.attach(
      handler_id,
      [:fathom, :hrana, :revocation, :floor_error],
      fn _e, _m, meta, _ -> send(test_pid, {:floor_error, meta.shard_id}) end,
      nil
    )

    on_exit(fn -> :telemetry.detach(handler_id) end)

    Revocations.put(shard, 5)

    # The outage: cut this process off from Postgres so the read-through raises.
    Ecto.Adapters.SQL.Sandbox.mode(Fathom.Repo, :manual)
    owner_restored = fn -> Ecto.Adapters.SQL.Sandbox.start_owner!(Fathom.Repo, shared: true) end

    capture_log(fn ->
      assert Revocations.floor(shard) == 5,
             "an outage must serve the stale floor, not collapse to 0 (pre-fix: 0)"
    end)

    assert_receive {:floor_error, ^shard}, 1_000

    # Postgres recovers (keeps later on_exit hooks working).
    owner = owner_restored.()
    on_exit(fn -> Ecto.Adapters.SQL.Sandbox.stop_owner(owner) end)
  end

  test "with no known floor, :hrana_revocation_on_error picks the outage posture" do
    import ExUnit.CaptureLog

    shard = uniq()
    prev_ttl = Application.get_env(:fathom, :hrana_revocation_ttl_ms)
    prev_posture = Application.get_env(:fathom, :hrana_revocation_on_error)
    Application.put_env(:fathom, :hrana_revocation_ttl_ms, 0)

    on_exit(fn ->
      if prev_ttl == nil,
        do: Application.delete_env(:fathom, :hrana_revocation_ttl_ms),
        else: Application.put_env(:fathom, :hrana_revocation_ttl_ms, prev_ttl)

      if prev_posture == nil,
        do: Application.delete_env(:fathom, :hrana_revocation_on_error),
        else: Application.put_env(:fathom, :hrana_revocation_on_error, prev_posture)
    end)

    Ecto.Adapters.SQL.Sandbox.mode(Fathom.Repo, :manual)

    capture_log(fn ->
      # Default: fail open — the signature stays the enforced control.
      Application.delete_env(:fathom, :hrana_revocation_on_error)
      assert Revocations.floor(shard) == 0

      # Fail closed: no floor knowledge means no token acceptance.
      Application.put_env(:fathom, :hrana_revocation_on_error, :fail_closed)
      assert Revocations.floor(shard) == :unavailable
    end)

    owner = Ecto.Adapters.SQL.Sandbox.start_owner!(Fathom.Repo, shared: true)
    on_exit(fn -> Ecto.Adapters.SQL.Sandbox.stop_owner(owner) end)
  end

  test "tokens sign with the dedicated secret, independent of secret_key_base" do
    shard = uniq()
    {:ok, _} = Directory.resolve(shard)

    Application.put_env(:fathom, :hrana_token_secret, String.duplicate("a", 64))
    {:ok, token} = HranaAuth.token_for(shard)
    assert {:ok, _} = HranaAuth.authorize(shard, token)

    # Rotating ONLY the dedicated secret invalidates the token — proving it signs
    # with that secret, not secret_key_base (which is untouched here).
    Application.put_env(:fathom, :hrana_token_secret, String.duplicate("b", 64))

    assert {:error, %Filo.Error{status: 401}} = HranaAuth.authorize(shard, token),
           "the token must be signed with the dedicated secret, not secret_key_base"
  end

  # Expert review 2026-07-19 #6 (cross-store DR coherence). Revocation lives in the Postgres
  # directory's token_version; a directory point-in-time restore rolls it back and un-revokes tokens.
  # A durable storage floor is mirrored on revoke and unioned on a cold cache read so a revocation
  # survives the restore.
  test "revoke mirrors the token floor to durable storage (#6)" do
    shard = uniq()
    {:ok, _} = Directory.resolve(shard)

    on_exit(fn ->
      File.rm(Path.join([Fathom.Shard.Storage.Local.dir(), "tokenfloors", shard]))
    end)

    assert {:ok, version} = HranaAuth.revoke(shard)
    assert {:ok, ^version} = Fathom.Shard.Storage.read_token_floor(shard)
  end

  test "the storage floor unions over a lower directory floor on a cold miss (survives a directory rollback) (#6)" do
    shard = uniq()
    {:ok, _} = Directory.resolve(shard)

    on_exit(fn ->
      File.rm(Path.join([Fathom.Shard.Storage.Local.dir(), "tokenfloors", shard]))
    end)

    {:ok, token} = HranaAuth.token_for(shard)
    assert {:ok, _} = HranaAuth.authorize(shard, token), "the fresh token verifies"

    # Post-restore state: the durable storage floor is AHEAD of the (rolled-back) directory floor,
    # and this node has a cold cache (a node that booted after the restore). Writing a storage floor
    # above the token's version and clearing the cache reproduces exactly that.
    :ok = Fathom.Shard.Storage.put_token_floor(shard, 9)
    :ets.delete(Revocations, shard)

    assert Revocations.floor(shard) == 9,
           "a cold read-through must union the durable storage floor over the lower directory floor"

    assert {:error, %Filo.Error{status: 401}} = HranaAuth.authorize(shard, token),
           "a token below the storage-backed floor stays revoked despite the directory rollback"
  end

  # Expert review 2026-07-24 #5. The per-shard TTL read-through scaled with SHARD COUNT rather than
  # revocation events, so it was replaced with one bulk query per node per interval plus a freshness
  # marker. These pin the safety property that makes that swap legitimate.
  describe "bulk refresh (#5)" do
    # THE BYPASS REGRESSION. The marker may extend freshness for an entry that EXISTS; it must never
    # answer a miss. Absence from the bulk set proves only what the directory says, and a Postgres
    # PITR can lower token_version — so a genuinely-revoked shard can read as unrevoked there. Only
    # the durable storage floor catches it, and only the cold-miss read_through consults it. If
    # anyone ever lets a fresh marker short-circuit a miss, this fails and a revoked token is
    # accepted.
    test "a fresh bulk marker does not answer a cache miss" do
      shard = uniq()
      {:ok, _} = Directory.resolve(shard)

      # Directory says unrevoked (as it would after a PITR); durable storage says otherwise.
      :ok = Fathom.Shard.Storage.put_token_floor(shard, 5)

      # A completed, current bulk refresh — and no entry for this shard, exactly as a bulk load
      # that (correctly) did not include an unrevoked shard would leave things.
      :ets.delete(Revocations, shard)
      :ets.insert(Revocations, {:__bulk_ok__, System.monotonic_time(:millisecond)})

      assert Revocations.floor(shard) == 5,
             "a miss must take the full read-through including the storage-floor union, even with " <>
               "a fresh bulk marker — otherwise the marker is an auth bypass after a PITR"
    end

    test "a fresh bulk marker serves an entry whose own TTL has lapsed" do
      shard = uniq()
      {:ok, _} = Directory.resolve(shard)

      # An entry that exists but is past its TTL: expires_at in the past.
      :ets.insert(Revocations, {shard, 4, nil, System.monotonic_time(:millisecond) - 1})
      :ets.insert(Revocations, {:__bulk_ok__, System.monotonic_time(:millisecond)})

      assert Revocations.floor(shard) == 4,
             "a recent bulk refresh has already reconciled every revoked shard, so a lapsed entry " <>
               "needs no per-shard re-read"
    end

    test "a stale bulk marker leaves a lapsed entry to the normal read-through" do
      shard = uniq()
      {:ok, _} = Directory.resolve(shard)
      {:ok, _} = Directory.bump_token_version(shard)

      # Entry lapsed AND the marker is old ⇒ must re-read, picking up the directory's version.
      :ets.insert(Revocations, {shard, 1, nil, System.monotonic_time(:millisecond) - 1})

      :ets.insert(
        Revocations,
        {:__bulk_ok__, System.monotonic_time(:millisecond) - 10 * 60_000}
      )

      assert Revocations.floor(shard) == 2,
             "with no fresh bulk refresh to vouch for it, a lapsed entry must read through"
    end

    # The monotonic guard still wins over the bulk path: a PITR-lowered directory can never lower a
    # floor this node already knows, whether the value arrives per-shard or in bulk.
    test "a bulk row cannot lower a floor this node already cached" do
      shard = uniq()
      {:ok, _} = Directory.resolve(shard)

      Revocations.put(shard, 7)
      # A bulk row carrying a LOWER version (the PITR case) applied the same way the refresh does.
      send(
        Revocations,
        {:notification, :fathom_revocations, %{"shard_id" => shard, "version" => 2}}
      )

      _ = :sys.get_state(Revocations)

      assert Revocations.floor(shard) == 7,
             "versions only rise — a lower bulk/notify value must not un-revoke"
    end

    test "revoked_floors returns only shards whose floor was raised" do
      raised = uniq()
      untouched = uniq()
      {:ok, _} = Directory.resolve(raised)
      {:ok, _} = Directory.resolve(untouched)
      {:ok, _} = Directory.bump_token_version(raised)

      ids = Directory.revoked_floors() |> Enum.map(fn {id, _v, _b} -> id end)

      assert raised in ids

      refute untouched in ids,
             "token_version defaults to 1, so an untouched shard stays out of the set"
    end
  end
end
