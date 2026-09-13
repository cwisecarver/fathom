defmodule Fathom.Shard.Integrity do
  @moduledoc """
  SQLite integrity verification, its classification, and the corrupt-copy quarantine — extracted
  from `Fathom.Shard` (2026-09-13, Phase 2 of the coordinator decomposition).

  Everything here is pure or path-scoped: `verify/1` opens a file and runs `PRAGMA quick_check`;
  `classify_failure/1` / `verdict/1` are pure verdict functions over a failure reason;
  `run_quick_check/1` / `classify_quick_check/1` classify a check on an already-open connection; and
  `quarantine_corrupt!/3` renames a corrupt copy aside. None of it reads coordinator state — the
  callers pass the path and shard id.

  The one integrity-adjacent function that did NOT move is `Fathom.Shard.quarantine_fenced!/1`: it
  is coupled to the coordinator's drop path (`drop_local/1`) and its write-counter watermark, so it
  stays with the drop/terminate state machine.

  Two verdicts, one distinction that matters (expert review 2026-08-20 #2): "I could not check it"
  is not "it is corrupt". An open failure under fd pressure, a `:busy` from a concurrent writer, or
  a `:query_timeout` on a large quick_check are `:integrity_unknown` (transient, retries next
  interval), while a genuinely malformed file is `:corrupt_local` (permanent, escalates). BOTH
  refuse the flush, so the stored object is protected either way; the difference is only the alarm
  — and, critically, `:unknown` is the default so a healthy copy is never discarded on a guess.
  """

  require Logger

  alias Fathom.Shard.Connection

  # Run `PRAGMA quick_check` on the file at `path`; `:ok` iff SQLite reports the single row "ok".
  # Cheap (shards are small by premise). Public so the corrupt-flush guard is testable directly
  # (expert review 2026-07-14 #4), and so `Fathom.Shard.verify_integrity/1` and the tenants /
  # restore-drill / migrator callers can reach it.
  @spec verify(Path.t()) :: :ok | {:error, term()}
  def verify(path) do
    case Connection.open(path) do
      {:ok, conn} ->
        result = Connection.query(conn, "PRAGMA quick_check", [])
        Connection.close(conn)

        case result do
          {:ok, %{rows: [["ok"]]}} -> :ok
          {:ok, %{rows: rows}} -> {:error, {:quick_check, rows}}
          {:error, reason} -> {:error, {:quick_check_failed, reason}}
        end

      other ->
        {:error, {:quick_check_open_failed, other}}
    end
  end

  # `quick_check` on an ALREADY-OPEN connection, gated by `:verify_flush_integrity`. Split out so
  # the caller's short-circuit reads as one line and cannot be accidentally reordered past the
  # VACUUM.
  @spec run_quick_check(term()) :: :ok | {:error, term()}
  def run_quick_check(conn) do
    if verify_flush?() do
      case Connection.query(conn, "PRAGMA quick_check", []) do
        {:ok, %{rows: [["ok"]]}} -> :ok
        {:ok, %{rows: rows}} -> verdict({:error, {:quick_check, rows}})
        {:error, reason} -> verdict({:error, {:quick_check_failed, reason}})
      end
    else
      :ok
    end
  end

  # The same check as `run_quick_check/1` but WITHOUT the `verify_flush?` gate or `verdict/1`
  # classification — it returns the raw `{:quick_check, _}` / `{:quick_check_failed, _}` shapes for
  # the checkpoint-and-verify drop path, which does its own thing with them.
  @spec classify_quick_check(term()) :: :ok | {:error, term()}
  def classify_quick_check(result) do
    case result do
      {:ok, %{rows: [["ok"]]}} -> :ok
      {:ok, %{rows: rows}} -> {:error, {:quick_check, rows}}
      {:error, reason} -> {:error, {:quick_check_failed, reason}}
    end
  end

  # Classify a failed integrity check. "I could not check it" is not "it is corrupt" (expert review
  # 2026-08-20 #2): an open failure under fd pressure (EMFILE, on a node whose density is the design
  # premise), a :busy from a concurrent writer, or a :query_timeout on a large quick_check all used
  # to take the corruption branch. Both verdicts REFUSE the flush, so the good stored object stays
  # authoritative either way; what differs is the alarm — :corrupt_local is permanent and escalates
  # on first occurrence, :integrity_unknown is transient and just retries next interval.
  #
  # NEITHER branch quarantines. This runs while the shard is being SERVED, and quarantine_corrupt!/3
  # renames the live file and unlinks its -shm; that rename is what made this a data-loss path
  # rather than a refused flush.
  @spec verdict({:error, term()}) :: {:error, {:corrupt_local | :integrity_unknown, term()}}
  def verdict({:error, reason}) do
    case classify_failure(reason) do
      :corrupt -> {:error, {:corrupt_local, reason}}
      :unknown -> {:error, {:integrity_unknown, reason}}
    end
  end

  # SQLite reports "these bytes are bad" through TWO different channels, and the difference is not
  # the tuple tag (expert review 2026-08-20 #2, corrected against a really-corrupted file rather
  # than against the finding's prescription). A mildly damaged db returns rows describing the
  # damage — `{:quick_check, rows}`. A badly damaged one makes the query itself ERROR, arriving as
  # `{:quick_check_failed, "database disk image is malformed"}` — the same shape a plain `:busy`
  # produces. Measured: overwriting one b-tree page with garbage yields the ERROR form, not rows.
  #
  # So classify on the REASON, not the tag. Defaulting to :unknown is the safe direction: both
  # verdicts refuse the flush, so the stored object is protected either way, and the only cost of
  # guessing :unknown is a retry — whereas guessing :corrupt discards a healthy local copy on the
  # drop path, including its unflushed acked writes.
  @spec classify_failure(term()) :: :corrupt | :unknown
  def classify_failure({:quick_check, _}), do: :corrupt
  def classify_failure({:quick_check_failed, reason}), do: corrupt_reason(reason)
  def classify_failure({:quick_check_open_failed, reason}), do: corrupt_reason(reason)
  def classify_failure(_), do: :unknown

  @corrupt_messages [
    "database disk image is malformed",
    "file is not a database",
    "malformed database schema",
    "database corruption"
  ]

  defp corrupt_reason(reason) when is_binary(reason) do
    down = String.downcase(reason)
    if Enum.any?(@corrupt_messages, &String.contains?(down, &1)), do: :corrupt, else: :unknown
  end

  defp corrupt_reason(reason) when is_atom(reason) do
    if reason in [:corrupt, :not_a_db], do: :corrupt, else: :unknown
  end

  defp corrupt_reason({_, reason}), do: corrupt_reason(reason)
  defp corrupt_reason(_), do: :unknown

  defp verify_flush?, do: Application.get_env(:fathom, :verify_flush_integrity, true)

  # A local db failed quick_check: move it aside (preserve for forensics) and drop its now-stale
  # WAL/SHM so the next open pulls the last-good stored object instead of adopting the corrupt
  # local copy. Loud error + telemetry so an operator sees it (the good object is still safe).
  #
  # ONLY SAFE WHERE THE SHARD IS PROVABLY NOT BEING SERVED — i.e. the drop path, where the
  # coordinator is terminating and releases the lease (expert review 2026-08-20 #2). Calling this
  # while streams are checked out renames the live path out from under them AND unlinks the -shm,
  # so the next connection to open `path` creates a BRAND-NEW EMPTY DATABASE: two sets of
  # connections to one tenant, one seeing data and one seeing nothing. Worse, the shard is still
  # dirty with an unchanged `etag`, so the next periodic flush snapshots that empty file and PUTs it
  # over the good stored object with a valid If-Match — destroying the only good copy. The serving
  # path therefore refuses the flush and leaves the file alone; see verify_and_snapshot/2.
  @spec quarantine_corrupt!(Path.t(), String.t(), term()) :: :ok
  def quarantine_corrupt!(path, shard_id, reason) do
    # `.corrupt.<ms>-<unique>`, and the -wal/-shm RENAMED alongside — mirroring quarantine_fenced!/
    # quarantine_fork! (expert review 2026-09-05 #17). Two fixes over the old `<second>` + `File.rm`:
    #   * <second> resolution collided — two quarantines of the same shard within one second renamed
    #     over each other and the first forensic copy was lost, the defect review #14 fixed for
    #     `.forked`. A crash-looping node is exactly where repeat quarantines happen.
    #   * File.rm on the WAL DESTROYS committed, acknowledged frames on the route this is reached
    #     from flush_then_drop's {:corrupt_local, _} — which fires when the checkpoint came back BUSY,
    #     so the WAL still holds frames never folded into the main file, and a corrupt main-file page
    #     with an intact WAL is the case where the WAL is the BETTER copy of recent history. Preserve
    #     it for the operator recovery the "quarantined for forensics" message promises.
    dest =
      "#{path}.corrupt.#{System.system_time(:millisecond)}-#{System.unique_integer([:positive])}"

    _ = File.rename(path, dest)
    Enum.each(["-wal", "-shm"], &File.rename(path <> &1, dest <> &1))

    Logger.error(
      "shard #{shard_id}: local db failed quick_check (#{inspect(reason)}); REFUSING flush so the " <>
        "last good stored object stays authoritative; quarantined corrupt copy to #{dest}"
    )

    :telemetry.execute([:fathom, :shard, :corrupt_flush], %{count: 1}, %{shard_id: shard_id})
  end
end
