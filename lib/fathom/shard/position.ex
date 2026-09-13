defmodule Fathom.Shard.Position do
  @moduledoc """
  The object position STAMP — pure functions that compute what a flush (or a promote) writes into
  the stored object's `x-amz-meta-fathom-pos` metadata, plus the `lineage` argument the backend
  flush takes.

  Extracted verbatim from `Fathom.Shard` (2026-09-13, Phase 0 of the coordinator decomposition).
  It lives outside the coordinator because it is **shared by two writers of the same object** — the
  periodic/drop durability flush AND the A2 replica promote path (`Fathom.Shard.promote_replica/7`)
  — and it is owned by neither. Every function is pure: it reads a few fields off the state-shaped
  map it is handed (`:lease`, `:path`, `:lineage`, `:carried_lineage`, `:wal_salt`, `:wal_ordinal`)
  and the on-disk `-wal` header via `Fathom.Shard.Replication.Wal.read/1`. No process state, no
  side effects.

  The position exists so a failover can order a node's local **replica** against the **stored
  object** — an etag is a content hash with no ordering, the lock carries the holder's epoch not
  the object's, and cross-node wall-clock is unsound. `Promote.fresher?/2` ranks on
  `{lineage, wal_ordinal, offset}`; the comments below explain why each field is what it is, and
  why the over-claim / silence directions are the safe ones. Do not "simplify" them back — several
  encode a fix that a plausible-looking edit would undo (see the `position_after_checkpoint/3`
  seed-on-known-short clause).
  """

  alias Fathom.Shard.Replication.Wal

  # THE POST-CHECKPOINT OVER-CLAIM (kept from the original comment). A checkpoint FOLDS the WAL into
  # the database, so an object written after one holds everything through the end of generation
  # `gen`; stamping `gen + 1` with offset 0 is strictly greater than any replica at `{epoch, gen,
  # ≤N}` and keeps the over-claim direction the callers require. This is not "read before the
  # snapshot" — the live read still happens after, and still wins whenever the WAL survives.
  @spec flush_position(map()) :: map() | nil
  def flush_position(state), do: flush_position(state, {:ok, :empty})

  @spec flush_position(map(), term()) :: map() | nil
  def flush_position(state, pre) do
    case stamp_epoch(state) do
      nil -> nil
      epoch -> flush_position(state, pre, epoch)
    end
  end

  # What fills the position stamp's `epoch` slot (expert review 2026-08-20 #8). Three inputs, and
  # the lease is required by ALL of them: no lease means no ownership to order, which is the
  # pre-existing rule this keeps.
  #
  #   integer lineage — stamp it. Unlike the lock epoch it never resets. See open_lineage/1.
  #   `:unknown`      — replication is on but no lineage could be read at open. Stamp NOTHING; an
  #                     absent stamp reads as "unknown" and makes the object un-overridable, the
  #                     same safe answer this function gives for an unreadable WAL.
  #   `:disabled`     — replication is off. Leave the LOCK epoch in the slot exactly as before, so
  #                     a non-replicating node is bit-for-bit unchanged. The `Map.get` default
  #                     covers the synthetic states built by callers that carry no lineage key.
  defp stamp_epoch(%{lease: %{epoch: lock}} = state) when is_integer(lock) do
    case Map.get(state, :lineage, :disabled) do
      n when is_integer(n) -> n
      :unknown -> nil
      :disabled -> lock
    end
  end

  defp stamp_epoch(_state), do: nil

  # What gets WRITTEN to the object's own lineage metadata key, as opposed to what fills the
  # position stamp. Only a real integer: `:disabled` and `:unknown` both mean "this coordinator has
  # no lineage to claim", and the backends take `nil` as leave-any-previous-value-alone. Erasing a
  # shard's lineage would reintroduce exactly the reset the key exists to prevent.
  @spec lineage_to_store(term()) :: non_neg_integer() | nil
  def lineage_to_store(n) when is_integer(n), do: n
  def lineage_to_store(_), do: nil

  # WHAT TO PASS AS THE FLUSH'S `lineage` ARGUMENT (expert review 2026-08-26 #33).
  #
  # A real lineage is passed straight through — replication is on, and the backend writes it with
  # no read. Otherwise this coordinator has nothing to claim, and `nil` means "leave whatever is
  # there alone" — which on S3 costs an `object_head` before EVERY PUT, because a PUT replaces all
  # user metadata, so the backend has to read what it is about to overwrite. On the default
  # (replication-off) configuration that HEAD is paid forever and returns nothing.
  #
  # `{:carried, _}` is the same instruction with the answer supplied: write exactly this, do not
  # look. It is only ever the value the backend itself reported from the previous flush, so the
  # coordinator is not deriving a lineage — it is remembering one. `nil` while the cache is cold
  # keeps today's behaviour, which is what makes this a strict improvement rather than a new rule.
  @spec lineage_arg(map()) :: non_neg_integer() | {:carried, term()} | nil
  def lineage_arg(%{lineage: n}) when is_integer(n), do: n

  def lineage_arg(state) do
    case Map.get(state, :carried_lineage) do
      nil -> nil
      carried -> {:carried, carried}
    end
  end

  defp flush_position(state, pre, epoch) do
    case Wal.read(state.path <> "-wal") do
      # commit_extent, not `size` (expert review 2026-08-20 #5): the file length is a high-water
      # mark that can include frames from a rolled-back transaction. A follower's position is
      # now the committed extent, so the object's stamp has to be on the same scale or the two
      # are not comparable and the object would always look ahead.
      {:ok, %{ckpt_seq: gen, salt1: salt, commit_extent: extent}} ->
        stamp_ordinal(%{epoch: epoch, wal_gen: gen, offset: extent}, state, salt, 0)

      {:ok, :empty} ->
        position_after_checkpoint(epoch, pre, state)

      {:error, _} ->
        nil
    end
  end

  # THE ORDINAL, WHICH IS WHAT `Promote.fresher?/2` RANKS ON (expert review 2026-08-26 #2, step 3).
  #
  # Only stamped when the WAL's salt MATCHES the one the coordinator's counter is standing on,
  # because the whole soundness argument is that the object and the replicas carry THE SAME NUMBER
  # for the same WAL. The coordinator assigns it (`wal_ordinal/2`) and `Replication.Session` reads
  # the same answer for the same salt; a salt this snapshot has never seen has no number the
  # replicas were also given, and inventing one here would put the two sides on different scales —
  # the bug, not the fix.
  #
  # A mismatch is therefore ANSWERED WITH SILENCE, not with a guess: no `:wal_ordinal` key, which
  # `fresher?/2` reads as unknown and resolves in the stored object's favour. In the case that
  # actually matters this never fires — replication being ON is what makes promotion possible at
  # all, and a replicating shard's Session asks for the ordinal on every salt change, so by flush
  # time the coordinator is already standing on it.
  #
  # `bump` is 1 for the post-checkpoint over-claim and 0 for a live read; see
  # `position_after_checkpoint/3`.
  defp stamp_ordinal(position, state, salt, bump) do
    case {Map.get(state, :wal_salt), Map.get(state, :wal_ordinal)} do
      {^salt, n} when is_integer(n) and n > 0 ->
        position |> Map.put(:wal_ordinal, n + bump) |> put_salt(salt, bump)

      _ ->
        position
    end
  end

  # The salt names the WAL the ordinal belongs to, so it is stamped only for a LIVE read. After a
  # checkpoint the ordinal is an over-claim on a WAL that no longer exists, and naming a salt there
  # would assert the object sits inside a WAL it is actually past.
  defp put_salt(position, salt, 0), do: Map.put(position, :salt1, salt)
  defp put_salt(position, _salt, _bump), do: position

  # The WAL is empty or gone AFTER the flush. What that means depends entirely on what was there
  # BEFORE, and the two cases are opposite.
  #
  # Known generation ⇒ the checkpoint folded it in ⇒ claim the next generation at offset 0.
  #
  # The ordinal takes the SAME over-claim, and it has to: once `fresher?/2` ranks on
  # `{lineage, wal_ordinal, offset}` rather than `{lineage, wal_gen, offset}`, stamping the folded
  # WAL's own ordinal `N` at offset 0 would LOSE to every replica still sitting in that WAL at any
  # offset — even though the object now holds all of those bytes and more. `N + 1` at offset 0 is
  # strictly greater than every `{N, ≤Y}` and strictly less than any genuinely newer WAL's
  # `{N + 1, >0}`, which is exactly the `gen + 1` reasoning one field over.
  #
  # No salt-less variant of this clause: `Wal.read/1` answers `{:ok, %{ckpt_seq, salt1, …}}` or
  # `{:ok, :empty}` or `{:error, _}`, so a header without a salt does not exist. One was written
  # here as a defensive fallback and dialyzer refused it as unmatchable — correctly, and it is
  # worth not re-adding: a clause that cannot run reads as a case someone handled.
  defp position_after_checkpoint(epoch, {:ok, %{ckpt_seq: gen, salt1: salt}}, state),
    do: stamp_ordinal(%{epoch: epoch, wal_gen: gen + 1, offset: 0}, state, salt, 1)

  # SEED FROM THE KNOWN ORDINAL WHEN THIS COORDINATOR HAS SHIPPED ONE (expert review 2026-09-05 #7,
  # the deferred "seed-on-known-short"; root-caused 2026-09-07 from the chaos-rig `rpo` INVALID).
  #
  # Empty before AND after with `wal_ordinal > 0` is NOT the ambiguous case the clause below handles.
  # It is the COMMON durability path: the last Hrana stream's close checkpointed and unlinked the
  # `-wal`, so both reads land on `:empty` even though this shard has been shipping. The catch-all's
  # `nil` then stamps EVERY idle-dropped (and every empty-WAL `flush_now`) object of a replicating
  # shard with no position — measured, `Storage.object_position` returned `{:ok, nil}` for a plain
  # insert+idle-drop — which makes `Promote.fresher?/2` un-rankable against it and defeats A2
  # promote-on-open for the window from a cold reopen to its first live-WAL flush. That is exactly
  # what the rig's `rpo` scenario reported as "a stamping/durability problem, not survivor-selection".
  #
  # The object has folded every acked frame (empty pre ⇒ nothing un-folded; an un-shipped-but-acked
  # frame would still be IN the WAL and take the header branch above), and every frame this
  # coordinator ever shipped carries an ordinal ≤ `wal_ordinal` — they were numbered from the same
  # counter (`wal_ordinal/2`). So `{lineage, wal_ordinal + 1, offset 0}` is strictly greater than
  # every replica at `{≤ wal_ordinal, any}` and correct at drop time (the object IS complete), while
  # a genuinely newer write can only arrive on the NEXT open, which increments the LINEAGE and so
  # wins on the first ranked component regardless of the ordinal. This is the same `n + 1` over-claim
  # the header branch makes with `bump: 1`, sourced from the coordinator's counter because the WAL
  # header that would carry the salt is gone.
  #
  # `wal_gen: 0` DELIBERATELY: `fresher?/2` ranks on `{lineage, wal_ordinal, offset}`, never on
  # `wal_gen`, and there is no live WAL whose `ckpt_seq` this could name. Seeding `wal_gen` (from
  # `counter_gen`) instead of the ordinal — the literal wording of the parked finding — does NOT fix
  # this: a stamp with no `wal_ordinal` still falls through `fresher?/2` to its `false` catch-all.
  # DO NOT "simplify" this back to a generation seed. No salt for the same reason the header branch
  # omits it on a `bump: 1` stamp (`put_salt/3`): a salt asserts the ordinal sits inside a live WAL,
  # and this one is an over-claim past a folded one.
  #
  # RIG-GATED: the ordinal/salt timing this rests on (a post-truncate write always lands in a NEW
  # WAL, so its ordinal bumps past this over-claim) is a chaos-rig invariant — validate `rpo` PASS
  # before this is pushed. `wal_ordinal == 0` (never shipped: a non-replicating shard, or one pulled
  # and never written) keeps the ambiguous-nil below, so the non-replicating path is bit-for-bit
  # unchanged.
  defp position_after_checkpoint(epoch, _pre, %{wal_ordinal: n}) when is_integer(n) and n > 0,
    do: %{epoch: epoch, wal_gen: 0, offset: 0, wal_ordinal: n + 1}

  # Empty before AND after is AMBIGUOUS, and `nil` is the only safe answer. It reads like "a brand
  # new shard at generation 0" — which is one of the two situations — but it is equally a shard
  # whose WAL was truncated and unlinked by an EARLIER cycle, which may sit at any generation with
  # replicas holding frames from it. `{epoch, 0, 0}` would lose to every one of them. `nil` means
  # "unknown", making the object un-overridable, and that costs nothing real: an empty WAL at flush
  # time means there are no un-folded writes, so the object IS complete and preferring it over any
  # replica is correct. The price is that promote-on-open will not fire for this shard — i.e. the
  # pre-A2 behaviour, which AGENTS.md already calls "never worse than off". Reached now only when
  # `wal_ordinal == 0` (never shipped); a shipping shard takes the seed-on-known-short clause above.
  defp position_after_checkpoint(_epoch, _pre, _state), do: nil
end
