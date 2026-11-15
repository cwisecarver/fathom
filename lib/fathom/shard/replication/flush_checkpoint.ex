defmodule Fathom.Shard.Replication.FlushCheckpoint do
  @moduledoc """
  The coordinator's flush-side checkpoints (cleanliness audit 2026-11-13 #30). They run in the
  coordinator or its flush task, never in the Session process, yet lived in the 4,467-line
  `Fathom.Shard.Replication.Session` and made `Shard` and `Session` depend on each other in both
  directions.

    * `after_snapshot/3`: the post-snapshot PASSIVE on the flush's own connection. Replication off
      (or no shard id): the unconditional PASSIVE. Replication on: pinned (`Pin.with_pin/2`) and gated on
      the quorum holding the WAL, by asking the shard's Session `{:covers, header}`; skipped when the WAL
      is small.
    * `before_pin/1`: the replication-OFF PASSIVE the pinned-live route runs BEFORE it takes its pin.

  The RULE they implement ("checkpoint only what the write quorum has acked", the pin-then-read-header
  mechanism, what is NOT covered) is written once, at "Checkpoints of a replicated WAL" in
  `Fathom.Shard.Replication.Session`, next to the Session's own threshold checkpoint that shares it.
  Every outcome emits `[:fathom, :shard, :flush_checkpoint]`.
  """

  alias Fathom.Shard.Connection
  alias Fathom.Shard.Replication.{Pin, Session, Wal, WalSync}

  @doc """
  The coordinator's post-snapshot checkpoint, on the flush's own connection `conn`.

  Replication off (or no shard id): the unconditional `PRAGMA wal_checkpoint(PASSIVE)` it always
  was. Replication on: pinned and gated on the quorum holding the WAL — see "Checkpoints of a
  replicated WAL" in `Session`. Never raises; a skipped checkpoint is retried at the next flush.
  """
  #
  # `opts[:before_checkpoint]` is a TEST seam (a 0-arity fun run after the covers check and before
  # the PASSIVE): it is how `replication_unshipped_checkpoint_test.exs` lands a commit inside that
  # window, proving the pin — not the covers check — is what protects a late commit.
  #
  # STARVATION, KNOWN AND ACCEPTED: the header is read here, outside the Session, so on a shard with
  # a commit in flight it is usually a few frames ahead of what the quorum has acked and `covers`
  # answers false. On a steadily written shard this checkpoint therefore rarely runs, and the WAL is
  # bounded by the Session's own threshold checkpoint (which pins before its ship, so it is covered
  # as soon as that ship lands). It still runs on every quiet shard, which is where it matters.
  #
  # COUNTED, AND SKIPPED WHEN SMALL UNDER REPLICATION (expert review 2026-10-20 #15). Every outcome
  # emits `[:fathom, :shard, :flush_checkpoint]` tagged `result` (`:ran | :busy |
  # :skipped_not_covered | :skipped_small | :skipped_no_session | :error`) and `replicated`; before,
  # only the Session's threshold checkpoints were counted, so nothing showed that a write-active
  # replicated shard took a WAL generation reset (follower absorb = 2 fsyncs, plus a reset round on
  # the next commit) on EVERY flush — the measured 22-25% txn/s gap between a 5 s and a 30 s flush
  # interval. Under replication the PASSIVE now runs only once the WAL's commit extent reaches
  # `flush_checkpoint_min_bytes/0` (1/4 of the Session threshold, ~4 MB): the snapshot does not
  # need it (VACUUM INTO reads `.db` + WAL), the Session's threshold checkpoint bounds a busy WAL,
  # and the covered idle close (#7) folds a quiet one. The original reason for this checkpoint
  # (2026-07-24 #4: SQLite's autocheckpoint billed to a tenant statement) does not apply under A2,
  # where the extension hook steps aside and the Session checkpoints off the reply path.
  # Replication OFF is unchanged: the unconditional PASSIVE.
  @spec after_snapshot(reference(), String.t() | nil, keyword()) ::
          {:ok, map()} | {:error, term()} | :skipped
  def after_snapshot(conn, shard_id, opts \\ []) do
    replicated? = is_binary(shard_id) and Session.enabled?()

    {result, outcome} =
      if replicated? do
        case Session.lookup(shard_id) do
          # No Session: nothing has shipped for this shard under this coordinator, and the
          # extension hook has not been told to step aside, so there is nothing to gate on.
          # Skipping is the safe direction; the WAL stays bounded by the hook and the idle close.
          nil -> {:skipped, :skipped_no_session}
          pid -> pinned_flush_checkpoint(conn, pid, Fathom.Shard.db_path(shard_id), opts)
        end
      else
        passive(conn)
      end

    :telemetry.execute(
      [:fathom, :shard, :flush_checkpoint],
      %{count: 1},
      %{result: outcome, replicated: replicated?}
    )

    result
  end

  @doc """
  The REPLICATION-OFF flush PASSIVE, run BEFORE the pinned-live route takes its pin rather than
  after a `VACUUM INTO` (expert review 2026-10-21 #11). Counted like `after_snapshot/3`
  (`replicated: false`).

  Never call it for a replicated shard: under A2 a flush checkpoint is pinned and quorum-gated
  (`after_snapshot/3`), and the user decision of 2026-10-21 is that the replicated flush
  gains NO new checkpoint (each one is a WAL-generation reset the followers pay for).
  """
  @spec before_pin(reference()) :: {:ok, map()} | {:error, term()}
  def before_pin(conn) do
    {result, outcome} = passive(conn)

    :telemetry.execute(
      [:fathom, :shard, :flush_checkpoint],
      %{count: 1},
      %{result: outcome, replicated: false}
    )

    result
  end

  defp passive(conn) do
    case Connection.query(conn, "PRAGMA wal_checkpoint(PASSIVE)", []) do
      {:ok, %{rows: [[1 | _] | _]}} = r -> {r, :busy}
      {:ok, _} = r -> {r, :ran}
      other -> {other, :error}
    end
  end

  # 0 disables the size gate (every flush checkpoints, as before #15) — the rig A/B lever.
  defp flush_checkpoint_min_bytes,
    do:
      Application.get_env(
        :fathom,
        :replication_flush_checkpoint_min_bytes,
        div(Session.checkpoint_bytes(), 4)
      )

  defp pinned_flush_checkpoint(conn, session, db_path, opts) do
    wal = db_path <> "-wal"

    # The size gate reads the header BEFORE pinning, so a small WAL — the common case on a flush —
    # costs one header read and never the pin's connection open (~0.4 ms). The decision to RUN is
    # still made on a header read under the pin, below.
    if small_wal?(wal) do
      {:skipped, :skipped_small}
    else
      case Pin.with_pin(db_path, fn -> covered_passive(conn, session, wal, opts) end) do
        :skipped ->
          {:skipped, :error}

        {result, _outcome} = verdict ->
          # rv6 F1: the coordinator's checkpoint fsyncs this WAL through ITS fd, so an IOERR it
          # consumed is one the Session's next fdatasync will not see. Tell the Session.
          if WalSync.io_error?(result), do: send(session, {:checkpoint_io_error, result})
          verdict
      end
    end
  catch
    :exit, _ -> {:skipped, :skipped_not_covered}
  end

  defp covered_passive(conn, session, wal, opts) do
    # `index: true` (expert review 2026-10-20 #1/#17): the covers-check must not count a torn
    # stale-tail frame, and it is one index pread instead of the backward walk.
    with {:ok, %{} = header} <- Wal.read(wal, index: true),
         true <- GenServer.call(session, {:covers, header}, 1_000) do
      if hook = opts[:before_checkpoint], do: hook.()
      passive(conn)
    else
      {:ok, :empty} -> {:skipped, :skipped_small}
      _ -> {:skipped, :skipped_not_covered}
    end
  end

  defp small_wal?(wal) do
    case flush_checkpoint_min_bytes() do
      min when is_integer(min) and min > 0 ->
        case Wal.read(wal, index: true) do
          {:ok, %{commit_extent: e}} -> e < min
          {:ok, :empty} -> true
          _ -> false
        end

      _ ->
        false
    end
  end
end
