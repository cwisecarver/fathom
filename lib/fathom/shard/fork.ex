defmodule Fathom.Shard.Fork do
  @moduledoc """
  Warm-restart fork detection — extracted from `Fathom.Shard` (2026-09-13, Phase 3).

  When a coordinator wakes with a local `.db` already present, that copy may CONTINUE the stored
  lineage (safe to serve, possibly holding newer un-flushed writes) or it may be a FORK — another
  node wrote and released while this one was down, so serving or flushing the local copy would
  clobber acknowledged writes. This module decides which.

  Two-step, split across the lease acquire deliberately:

    * `evidence/2` — OBSERVE ONLY, no side effects. It races the steal-touch that `acquire_lease`
      performs, so it gathers the sidecar-vs-store comparison and nothing more. Runs overlapped with
      the lease acquire to keep the latency win.
    * `resolve/4` — the VERDICT, run AFTER the lease is held, because only then are
      `lease.touch_pre_etag`/`touch_post_etag` available to tell this node's OWN steal-touch from a
      real peer fork. Returns `true` when the local copy was a fork and was successfully quarantined
      (caller opens COLD); `false` to keep it and open warm.

  `quarantine!/3` moves a forked copy aside to `<path>.forked.<ms>-<unique>` (never a fixed name —
  a crash-looping node forks repeatedly and a fixed name + rm-first destroyed the FIRST recovery
  copy, expert review #14) and returns `:ok`, or `{:error, _}` when the rename never moved the copy
  — in which case the caller must NOT treat it as quarantined, or `promote_pull`'s rename would
  overwrite the very recovery copy the quarantine exists to preserve.
  """

  require Logger

  alias Fathom.Shard.Provenance
  alias Fathom.Shard.Storage

  @spec adopt_unprovenanced_warm?() :: boolean()
  def adopt_unprovenanced_warm?,
    do: Application.get_env(:fathom, :adopt_unprovenanced_warm, false)

  # Gather fork evidence — OBSERVE ONLY, side-effect free (2026-07-26). The verdict belongs to
  # `resolve/4`, which runs AFTER the lease is held and can tell our own steal-touch from a real
  # fork because it has `touch_pre_etag`/`touch_post_etag`. This HEAD stays overlapped with the
  # acquire, so the latency win is unchanged; running the verdict here would quarantine a good warm
  # copy that merely saw OUR OWN touch (reproduced under load 2026-07-26, RevalidateTouchedTest B1).
  @spec evidence(String.t(), Path.t()) ::
          :no_sidecar
          | :no_object_confirmed
          | {:orphaned, String.t()}
          | :corrupt
          | :match
          | :absent
          | {:diverged, String.t(), String.t()}
          | :unreachable
  def evidence(shard_id, path) do
    case Provenance.read(path) do
      :missing ->
        :no_sidecar

      # We recorded "born here against no stored object". If the store still has no object,
      # that claim holds and the copy is ours to serve. If an object now EXISTS, somebody else
      # created it while we were away — our copy is a fork of a lineage we never saw, and
      # serving or flushing it would destroy their writes (expert review 2026-08-01 #2,
      # trigger B: die before the first flush, peer takes over, serves, flushes, releases).
      :no_object ->
        case Storage.object_etag(shard_id) do
          {:ok, nil} -> :no_object_confirmed
          {:ok, store_etag} -> {:orphaned, store_etag}
          {:error, _unreachable} -> :unreachable
        end

      # Torn/unreadable sidecar (expert review #12): provenance unknown, so the copy cannot be
      # trusted to continue the stored lineage. Unrelated to the touch race — but the quarantine
      # itself still moves to resolve/4 so this task stays side-effect free.
      :corrupt ->
        :corrupt

      {:ok, sidecar_etag} ->
        case Storage.object_etag(shard_id) do
          {:ok, ^sidecar_etag} ->
            :match

          # The object is GONE but we have provenance from one — treat as fork-adjacent?
          # No: a deliberately deleted object with a live local copy is the un-flushed
          # brand-new case; serve warm and let the fenced flush recreate it.
          {:ok, nil} ->
            :absent

          {:ok, store_etag} ->
            {:diverged, sidecar_etag, store_etag}

          {:error, _unreachable} ->
            :unreachable
        end
    end
  end

  # The post-lease verdict. Returns true when the local copy is a fork that was successfully moved
  # aside (so the caller opens COLD); false means keep it and open warm.
  #
  # A FAILED quarantine (the rename never moved the copy) must NOT report quarantined, or the
  # cold-open's promote_pull would overwrite the un-moved recovery copy (expert review #14) —
  # hence `== :ok` on every quarantine branch.
  # An absent sidecar is UNKNOWN provenance and fails closed (expert review 2026-08-01 #2). This
  # used to return false — "keep it, open warm" — which is what made both triggers work: a file
  # planted by a tenant (via ATTACH or VACUUM INTO, before 286b530 closed those) was adopted as
  # authoritative for a shard that had never been opened, and a legitimate born-empty shard that
  # failed over and back clobbered the peer that took it.
  #
  # `:adopt_unprovenanced_warm` restores the old behaviour for an operator carrying pre-provenance
  # files they would rather adopt than re-pull. Default OFF.
  @spec resolve(term(), String.t(), Path.t(), map()) :: boolean()
  def resolve(:no_sidecar, shard_id, path, _lease) do
    if adopt_unprovenanced_warm?() do
      Logger.warning(
        "shard #{shard_id}: warm file has no provenance sidecar; adopting it because " <>
          ":adopt_unprovenanced_warm is on. This cannot distinguish a legacy file from a " <>
          "planted or forked one."
      )

      false
    else
      quarantine!(shard_id, path, :no_sidecar) == :ok
    end
  end

  # Provenance says "no stored object" and the store agrees — our own brand-new shard.
  def resolve(:no_object_confirmed, _shard_id, _path, _lease), do: false

  # Provenance says "no stored object" but one exists: normally a peer created the lineage while we
  # were down, so never serve or flush over it. The exception (finding #6): our OWN steal-touch,
  # after a same-node crash before this shard's first flush, plants a SENTINEL over the empty store
  # — so the "object that exists" is that sentinel, not a peer's data. `touch_pre_etag == nil` (the
  # touch sourced nothing) and `touch_post_etag == store_etag` (the store holds exactly our touch's
  # output) identify it; adopt the local copy rather than quarantine it into an empty re-pull. This
  # mirrors the :diverged self-touch clause below, and is the ordering where `evidence`'s HEAD
  # ran AFTER the sentinel landed (the `:no_object` warm branch of `revalidate_touched` handles the
  # HEAD-first ordering).
  def resolve({:orphaned, store_etag}, shard_id, path, lease) do
    if lease[:touch_pre_etag] == nil and lease[:touch_post_etag] == store_etag do
      false
    else
      quarantine!(shard_id, path, :orphaned) == :ok
    end
  end

  def resolve(:match, _shard_id, _path, _lease), do: false
  def resolve(:absent, _shard_id, _path, _lease), do: false
  def resolve(:unreachable, _shard_id, _path, _lease), do: false

  def resolve(:corrupt, shard_id, path, _lease),
    do: quarantine!(shard_id, path, :corrupt_sidecar) == :ok

  def resolve({:diverged, sidecar, store}, shard_id, path, lease) do
    # OUR OWN steal-touch, not a fork. Both halves are required: the touch SOURCED this file's
    # provenance (`pre == sidecar`, so the local bytes are the lineage the touch copied) and the
    # store now holds exactly that touch's output (`post == store`). A self-copy moves no bytes, so
    # the warm copy is still correct — this is the same argument `revalidate_touched/5`'s warm
    # branch already makes before adopting `post`.
    #
    # Anything else is a real fork. In particular the zombie-flush race (B2) is NOT captured here:
    # there the touch sources the ZOMBIE's etag, so `pre != sidecar` and we still quarantine.
    if lease[:touch_pre_etag] == sidecar and lease[:touch_post_etag] == store do
      false
    else
      quarantine!(shard_id, path, :diverged) == :ok
    end
  end

  # Returns :ok when the local copy was moved aside (the caller opens COLD), or
  # {:error, reason} when the main-file rename failed — the copy never moved, so the
  # caller must NOT report it quarantined (expert review #14: promote_pull's rename
  # would overwrite the recovery copy the quarantine exists to preserve).
  @spec quarantine!(String.t(), Path.t(), atom()) :: :ok | {:error, term()}
  def quarantine!(shard_id, path, reason) do
    # Unique per quarantine (expert review #14): a fixed `.forked` name + rm-first
    # destroyed the FIRST fork's recovery copy whenever the same shard forked twice —
    # and a crash-looping node in a stolen/written/released environment is exactly
    # where repeat forks happen. A quarantine's sole purpose is preserving
    # acknowledged writes; never delete a prior one.
    dest =
      path <> ".forked.#{System.system_time(:millisecond)}-#{System.unique_integer([:positive])}"

    # The main-file rename is load-bearing; the WAL/SHM companions are best-effort
    # (promote_pull removes stale ones before landing the pulled object).
    case File.rename(path, dest) do
      :ok ->
        Enum.each(["-wal", "-shm"], &File.rename(path <> &1, dest <> &1))
        File.rm(Provenance.sidecar_path(path))

        cause =
          case reason do
            :corrupt_sidecar ->
              "provenance sidecar torn/unreadable (crash mid-write), lineage unknown (expert review #12)"

            :diverged ->
              "another node wrote and released while this one was down (expert review #1)"

            :no_sidecar ->
              "no provenance sidecar — this node did not create this file, so its lineage is " <>
                "unknown: a pre-provenance legacy copy, or one planted by a tenant " <>
                "(expert review 2026-08-01 #2). Set :adopt_unprovenanced_warm to adopt instead."

            :orphaned ->
              "recorded as born against NO stored object, but an object now exists — a peer " <>
                "created the lineage while this node was down (expert review 2026-08-01 #2)"
          end

        Logger.error(
          "shard #{shard_id}: local copy FORKED from the stored lineage — #{cause}; " <>
            "quarantined at #{dest} and re-pulling. " <>
            "Operator recovery: the forked writes live in that file."
        )

        :telemetry.execute([:fathom, :shard, :forked], %{count: 1}, %{shard_id: shard_id})
        :ok

      {:error, _} = error ->
        Logger.error(
          "shard #{shard_id}: fork quarantine FAILED (#{inspect(error)}); the local copy " <>
            "stays in place and the open serves it warm, fenced by the provenance etag — " <>
            "never overwritten by a re-pull (expert review #14)."
        )

        error
    end
  end
end
