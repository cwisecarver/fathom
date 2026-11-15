defmodule Fathom.Shard.Snapshot do
  @moduledoc """
  The local snapshot-and-verify pipeline: everything a flush does to a shard's FILE before the object
  PUT. Moved out of `Fathom.Shard` (cleanliness audit 2026-11-13 #32): none of it reads the
  coordinator, only a shard id and a path, and none of it has a fence role.

    * the free-space preflight (`flush_space_preflight/2`);
    * the folded-WAL / compaction predicates that decide a flush route (`wal_folded?/1`,
      `compaction_worthwhile?/1`);
    * the verify-then-snapshot scan (`verify_and_snapshot/4`): `quick_check` + the page-copy
      (`sqlite3_serialize`) or `VACUUM INTO` image, under the node-wide scan permit;
    * the scan-handle mmap pragma (`apply_scan_mmap/1`) and the `VACUUM INTO` primitive (`do_snapshot/3`).

  What stays in `Fathom.Shard`, on purpose: `snapshot_and_upload`, `upload_flush_object`,
  `recheck_before_put`, `guard_pre_touch`, `flush_intent_opts`, the pinned-live route and the route
  telemetry. Those carry the lease/fence envelope (lease validity, flush fence, dirty, drop), which
  AGENTS.md says must stay in one consistency boundary. This module is called FROM inside it and
  calls nothing back, so the ORDER the guards in the comments below describe (fence, then snapshot,
  then `recheck_before_put`, then the pre-PUT intent) is still decided by the caller.
  """

  require Logger

  alias Fathom.Shard.{Connection, Integrity}

  # How long a flush waits for a scan permit before scanning without one (a durability flush must not
  # fail because the gate was busy). Shared by the snapshot scan here and the pinned-live route's
  # `quick_check` in `Fathom.Shard`, through `with_scan_permit/1`.
  @scan_permit_wait_ms 60_000

  @doc """
  Run `fun` holding a scan permit (`Fathom.Shard.ScanGate`), waiting at most #{60_000} ms; without a
  permit if none comes free in time. See `verify_and_snapshot/4` for why the scan is bounded.
  """
  @spec with_scan_permit((-> result)) :: result when result: var
  def with_scan_permit(fun), do: Fathom.Shard.ScanGate.with_permit(@scan_permit_wait_ms, fun)

  # No committed frame outside the main file: an absent/zero-length `-wal`, or one whose `-shm`
  # reports it fully backfilled (expert review 2026-10-21 #11). Meaningful as a route decision only
  # when read under the pin (see `pinned_live_upload/2`); before it, it is only a filter.
  def wal_folded?(wal),
    do: Fathom.FS.wal_empty?(wal) or Fathom.Shard.Replication.Wal.fully_backfilled?(wal)

  # Is the file mostly freelist pages, so that a VACUUM-compacted object is much smaller than the live
  # file (expert review 2026-10-20-2027 #9)? Header-only read, no open. Defaults: free pages above
  # 25% of the page count AND above 1 MiB free; `:flush_compact_min_free_ratio` /
  # `:flush_compact_min_free_bytes` tune it, and a ratio of 1.0+ disables it. An unreadable header
  # answers false (keep the existing route; the integrity gate reports real damage).
  def compaction_worthwhile?(path) do
    case Fathom.Shard.SqliteHeader.free_pages(path) do
      {:ok, %{page_count: count, freelist: free, page_size: ps}} ->
        compaction_worthwhile_counts?(count, free, ps)

      _ ->
        false
    end
  end

  # The rule itself, over counts from anywhere (the header above, or the snapshot's own read
  # transaction in `serialize_or_vacuum/4`).
  defp compaction_worthwhile_counts?(count, free, ps)
       when is_integer(count) and count > 0 and is_integer(free) and is_integer(ps) do
    ratio = Application.get_env(:fathom, :flush_compact_min_free_ratio, 0.25)
    min_bytes = Application.get_env(:fathom, :flush_compact_min_free_bytes, 1_048_576)
    free / count > ratio and free * ps > min_bytes
  end

  defp compaction_worthwhile_counts?(_count, _free, _ps), do: false

  # FREE-SPACE PREFLIGHT (expert review 2026-10-12 #26). `VACUUM INTO` writes a temp about the
  # size of the shard (plus its WAL content) next to the live file, so a near-full volume fails the
  # flush late, after the whole `quick_check` page scan, with SQLITE_FULL, and the generic failure
  # path only escalates on the 3rd consecutive one. Check the poller's cached reading
  # (`Measurements.data_free_bytes/0`; never `:disksup` per flush, which runs `df`) against the
  # shard's size plus a margin, and fail fast. Like `object_too_large`, the first occurrence is
  # logged at error level and has its own event: retrying cannot help until someone frees space,
  # while the shard keeps acking writes it cannot make durable. Escalation lives here rather than in
  # record_flush_failure/2, which still counts the failure like any other.
  #
  # An unknown reading (poller not running, or stale) skips the check: a flush must never be refused
  # on a guess.
  @flush_space_margin_bytes 32 * 1024 * 1024
  def flush_space_preflight(id, path) do
    with {:ok, free} <- Fathom.Admin.Measurements.data_free_bytes(),
         need = flush_space_need(path),
         true <- free < need do
      :telemetry.execute(
        [:fathom, :shard, :flush, :no_space],
        %{count: 1, need: need, free: free},
        %{
          shard_id: id
        }
      )

      # The telemetry fires on EVERY refusal; the error log is emitted by `record_flush_failure/2`
      # only on the transition into failing (expert review 2026-10-13 #30). This runs inside the
      # flush task, whose state carries no failure count, and the preflight is retried every interval
      # on every dirty shard of a full volume.
      {:error, {:flush_no_space, need, free}}
    else
      _ -> :ok
    end
  end

  # The VACUUM INTO temp (db + wal content) plus the margin. A compressing backend writes a SECOND
  # temp, the encoded body, next to it (`S3.with_body/2`, `Codec.compress_to_temp/2`); at worst it is
  # about the size of the shard, so add the shard's size again (expert review 2026-10-13 #30).
  defp flush_space_need(path) do
    snapshot = local_db_bytes(path)
    encoded = if Fathom.Shard.Storage.Codec.encoding() == :none, do: 0, else: snapshot
    snapshot + encoded + flush_space_margin()
  end

  defp flush_space_margin,
    do: Application.get_env(:fathom, :flush_space_margin_bytes, @flush_space_margin_bytes)

  # The .db plus its WAL: the snapshot is the merged content of both.
  defp local_db_bytes(path) do
    Enum.reduce([path, path <> "-wal"], 0, fn p, acc ->
      case Fathom.FS.stat(p) do
        {:ok, %{size: size}} -> acc + size
        _ -> acc
      end
    end)
  end

  # Integrity-gate the PERIODIC flush (expert review 2026-08-01 #14).
  #
  # Review 2026-07-14 #4 added a `quick_check` gate so a locally-corrupted database is never
  # flushed over the last good stored object — but it lived in exactly one place: the
  # checkpoint-then-raw-upload fast path of `upload_for_drop/1`, which runs only on the
  # idle/terminate drop. The periodic flush, which is the DOMINANT producer of the durable
  # object, went straight to the PUT with no check at all.
  #
  # The old justification was that `VACUUM INTO` reads every page so it fails on corruption by
  # itself. Not true: the probe below shows a structurally corrupt index page coming out of
  # `VACUUM INTO` with a clean copy, so the evidence never reaches the uploaded file — which is
  # then uploaded, correctly If-Match-fenced by the legitimate owner, as the new durable truth.
  #
  # Check the SNAPSHOT TEMP, not the live file: it is the artifact about to become the durable
  # object, and it is about to be fully read for the Content-MD5 anyway, so the page reads are
  # already paid for. Reuses the drop path's quarantine + refuse-the-flush behaviour, so the
  # last good stored object stays authoritative.
  # CHECKS THE LIVE FILE, NOT THE SNAPSHOT TEMP — and that distinction is the whole point.
  #
  # The review recommended checking the temp, on the reasoning that it is about to be read for
  # the Content-MD5 anyway so the pages are free. Measured before implementing, that is wrong for
  # STRUCTURAL damage: a database with a deliberately corrupted index root page came out of
  # `VACUUM INTO` with a copy `quick_check` passes:
  #
  #     quick_check on the live db   -> "Tree 3 page 3 cell 199: Offset 23130 out of range …"
  #     VACUUM INTO                  -> OK
  #     quick_check on the VACUUMed copy -> "ok"      (200 rows intact)
  #
  # So the temp is clean for exactly the structural class `quick_check` catches, and checking it
  # would have shipped a gate that never fires. The live file is the only place that evidence
  # still exists.
  #
  # WHAT THIS GATE CANNOT SEE (expert review 2026-10-15 #11, probed on exqlite's bundled SQLite):
  # an index whose CONTENT disagrees with its table (a stale expression/UDF index after a function
  # changed behaviour). `VACUUM INTO` does NOT rebuild indexes from table content — the transfer
  # copies such an index verbatim — and `quick_check` misses it on both the live file and the copy;
  # only `integrity_check` sees it. That full check is deliberately OFF this hot path: it runs
  # detect-only in the restore drill (`Integrity.verify_full/1`). This comment used to say VACUUM
  # "rebuilds indexes from table content" and so normalised such drift away; it does neither.
  #
  # Cost is one full page scan per periodic flush, on a path that already reads the whole file
  # (VACUUM INTO) and the whole temp (MD5 + PUT). Shards are small by fathom's premise, which is
  # what makes that acceptable; `:verify_flush_integrity` turns it off for a deployment that
  # disagrees.
  #
  # ONE CONNECTION, NOT TWO (expert review 2026-08-26 #12). This used to be `verify_snapshot/2`
  # followed by a separate `snapshot/2`, each with its own `Connection.open` … `Connection.close`
  # on the SAME path. Two costs, both per flush per write-active shard:
  #
  #   * A second full connection open — `File.dir?`/`mkdir_p`, `sqlite3_open`, seven pragmas, the
  #     `PRAGMA page_size` prepare/fetch/release, and an extension load (enable → load → disable),
  #     all dirty-IO NIFs. Measured at roughly 429 µs on this machine (review #10's probe).
  #   * A WAL delete/recreate cycle. `snapshot/2`'s own comment records that the periodic flush
  #     "fires on any dirty shard, which is routinely one with zero checked-out streams", so the
  #     verify connection's close was FREQUENTLY THE LAST ONE — triggering SQLite's close-time
  #     checkpoint and unlinking `-wal`/`-shm`, which the snapshot connection then recreated.
  #
  # `checkpoint_and_verify/1` describes this identical churn verbatim as the reason the DROP path
  # was merged onto one connection (review 2026-07-24 #31). `verify_snapshot/2` arrived later
  # (review 2026-08-01 #14) and never got the same treatment: the rare path was fixed and the
  # dominant one was not.
  #
  # On a replicating fleet the extra close-time checkpoint also started a new WAL generation, which
  # is the `{:reset, 0, size}` full-WAL-reship path — and it is the mechanism that manufactures the
  # stale `wal_gen` in review #2, so folding it narrows that hazard's frequency too.
  #
  # THE THREE GUARDS THAT MUST SURVIVE ANY FUTURE EDIT HERE:
  #   (a) `quick_check` runs on the LIVE file, never on the VACUUM temp — see the probe above.
  #       (It does not catch index CONTENT drift on either; see "WHAT THIS GATE CANNOT SEE".)
  #   (b) It SHORT-CIRCUITS before the VACUUM: a corrupt shard must not pay a full snapshot.
  #   (c) The extension stays loaded on this connection (`Connection.open/2` does it): `VACUUM INTO`
  #       and `quick_check` both touch the schema's indexes, so a Django expression index over a UDF
  #       would fail "no such function" without it.
  # THE SCAN PERMIT (expert review 2026-10-20 #19): `quick_check` + `VACUUM INTO` each hold a
  # dirty-IO thread for the whole page scan, and the upload cap can equal the dirty-IO thread count,
  # so a flush wave could hold every thread. `ScanGate` bounds concurrent scans to SDio-2, held
  # across the open..close here and NOT across the PUT. Bounded wait (`@scan_permit_wait_ms`), then
  # scan anyway: a durability flush must not fail because the gate was busy.
  def verify_and_snapshot(id, path, temp, verified?) do
    Fathom.Shard.ScanGate.with_permit(@scan_permit_wait_ms, fn ->
      verify_and_snapshot_unpermitted(id, path, temp, verified?)
    end)
  end

  # MMAP FOR THE FLUSH SCAN (expert review 2026-10-20 #32), DEFAULT OFF. `quick_check` and
  # `VACUUM INTO` pread every page of the shard on a dirty-IO thread (measured 20-35% of the scan on
  # macOS; Linux unmeasured — `scripts/flush_scan_mmap_probe.exs` measures it). `:flush_scan_mmap_bytes`
  # (a positive integer) sets `PRAGMA mmap_size` on the FLUSH-SCAN handles only: this snapshot
  # connection, the pinned-live pin, and the drop path's `checkpoint_and_verify/1`. NEVER on tenant
  # handles and never process-wide (`SQLITE_DEFAULT_MMAP_SIZE`): a mapped page that disappears
  # (file truncated under the map, bad media) is SIGBUS, which kills the whole node, where a pread
  # returns an error and fails one shard's flush. That blast-radius trade is why this ships off
  # until the Linux probe says the win is worth it. Best effort: a failing pragma is ignored.
  @doc false
  @spec apply_scan_mmap(reference()) :: :ok
  def apply_scan_mmap(conn) do
    case Application.get_env(:fathom, :flush_scan_mmap_bytes) do
      n when is_integer(n) and n > 0 -> _ = Connection.exec(conn, "PRAGMA mmap_size=#{n}")
      _ -> :ok
    end

    :ok
  end

  defp verify_and_snapshot_unpermitted(id, path, temp, verified?) do
    case Connection.open(path) do
      {:ok, conn} ->
        try do
          apply_scan_mmap(conn)

          if serialize_eligible?(),
            do: serialize_or_vacuum(conn, temp, id, verified?),
            else: vacuum_snapshot(conn, temp, id, verified?)
        after
          Connection.close(conn)
        end

      other ->
        # An open failure is "I could not check it", not "it is corrupt" — same classification the
        # separate `verify_integrity/1` path applied.
        Integrity.verdict({:error, {:quick_check_open_failed, other}})
    end
  end

  # THE SNAPSHOT IS A PAGE COPY WHEN IT CAN BE (expert review 2026-10-22 r8 #9, user decision Q8 = A).
  #
  # `VACUUM INTO` does not copy pages: it re-creates the schema in the target and re-inserts every
  # table and index b-tree, then writes a shard-sized temp the codec reads back. Its only job here
  # is a frozen, consistent image of the live file. `sqlite3_serialize` gives exactly that image:
  # it reads every page through the pager inside the current read transaction, WAL overlay
  # applied, and copies the bytes. Probed (r8 flush panel, median): keystone 1.50 MB 2,268 µs vs
  # 72 µs; 32 MB 50 ms vs 4.6 ms; 32 MB + 23 MB of un-checkpointed WAL 50-53 ms vs 4.8-8.2 ms, WAL
  # frames included (sums and `quick_check` of the copy verified).
  #
  # ONE READ TRANSACTION: `BEGIN; size; serialize; COMMIT`, then `quick_check` of the written image
  # (rv8 F1, below `serialize_or_vacuum/4`). The integrity gate checks exactly the bytes that are
  # uploaded. Before, `quick_check` and `VACUUM INTO` were two autocommit reads of two different
  # snapshots, and VACUUM rewrote the pages, so the gate never saw the uploaded state. The three
  # guards above: (a) holds for the VACUUM fallback, and a page copy repairs nothing, so checking
  # the copy sees what the live check would; (b) a corrupt shard pays one capped page copy before it
  # is refused; (c) is moot for serialize (no schema re-created) and still true for the VACUUM
  # fallback, which runs on this same handle.
  #
  # GATES, each falling back to today's `VACUUM INTO` (after the COMMIT: VACUUM cannot run inside
  # a transaction, so the fallback is exactly the old two-snapshot behaviour):
  #   * the image exceeds `:flush_serialize_max_bytes` (default 16 MiB; 0 disables serialize).
  #     Peak memory is ~2x the image (SQLite's buffer + the BEAM binary, both large enough to be
  #     mmap'd single-block carriers and returned on free), and ScanGate bounds concurrent scans
  #     at 8, so the transient worst case is 8 x 2 x 16 MiB = 256 MiB;
  #   * the file is mostly free (`compaction_worthwhile_counts?/3`, the same rule as the drop and
  #     pinned routes, on the counts read INSIDE the transaction): only VACUUM compacts;
  #   * the integrity gate is off (`:verify_flush_integrity false`), even when the caller verified:
  #     the image check is that gate, and without it VACUUM's own b-tree walk is the only thing that
  #     fails on an unreadable page, where serialize would upload it zero-filled (rv8 F1).
  #
  # COST, accepted with Q8 = A: the copy keeps b-tree slack and freelist pages VACUUM would repack,
  # so the compressed object is +2.4% (keystone) to +14% (32 MB after churn) larger; the codec
  # time that adds is far below the scan time saved. The copied page 1 keeps the WAL-mode bytes
  # `02 02` (as the pinned route's live-file upload already does).
  #
  # FENCE ORDER UNCHANGED: `pre` is still read before this snapshot (the stamp under-claims), the
  # snapshot still runs after `Fence.check` and before `recheck_before_put` and the pre-PUT intent,
  # and `FlushCheckpoint.after_snapshot/3` still runs after the read transaction ended, on this handle.
  # The temp is written raw with no fsync, the same no-durability argument as `do_snapshot/3`'s
  # synchronous=OFF: it is read back in-process for the digest and the PUT and unlinked after.
  defp serialize_eligible?,
    do: serialize_max_bytes() > 0 and Integrity.flush_gate_on?()

  defp serialize_max_bytes,
    do: Application.get_env(:fathom, :flush_serialize_max_bytes, 16 * 1024 * 1024)

  # THE GATE CHECKS THE IMAGE, NOT THE LIVE PAGES (expert review 2026-10-22 r8 #9, rv8 F1).
  # `sqlite3_serialize` does not fail on a page it cannot read: it `memset`s that page to zero and
  # still returns the buffer (deps/exqlite/c_src/sqlite3.c, the `sqlite3PagerGet` loop in
  # `sqlite3_serialize`). A transient EIO or a NOMEM on one page read during the copy therefore
  # produced an image with a zeroed b-tree page that the in-transaction `quick_check` of the LIVE
  # pages had already passed, and it was PUT as the authoritative object. `VACUUM INTO` fails the
  # flush on the same error. So the check now runs on the written temp (an `immutable=1` handle,
  # `Integrity.run_quick_check_main_file/2`: the image has page 1's WAL bytes `02 02` and no `-wal`,
  # the same case the pinned-live route relies on), and the live-page check is gone from this route.
  #
  # WHY CHECKING ONLY THE IMAGE IS AT LEAST AS STRONG, so `quick_check` still runs once per flush:
  # serialize is a byte copy of every page of the snapshot this read transaction sees (WAL overlay
  # applied), not a rewrite. Each page it read is copied verbatim, so any damage `quick_check`
  # would report on the live page is in the image at the same page number, and the page count is
  # the same. Each page it could NOT read is zero in the image, which `quick_check` reports when the
  # page matters (a b-tree, overflow or freelist-trunk page) and which is harmless when it does not
  # (a freelist leaf). Guard (a) above is about `VACUUM INTO` REPAIRING structural damage; a page
  # copy repairs nothing, so the copy is not the "clean-looking temp" that guard warns about.
  # Guard (b) changes shape: a corrupt shard now pays one page copy (<= the serialize cap) before
  # it is refused.
  #
  # ON IMAGE-CHECK FAILURE fall back to the old route, live `quick_check` (unless the caller
  # verified) then `VACUUM INTO`: a corrupt live file is still refused there, and a transient read
  # error either recurs (VACUUM fails the flush) or does not (a good object is uploaded). The temp is
  # unlinked first, because `VACUUM INTO` refuses a non-empty target.
  #
  # ERRORS FALL BACK TOO (rv8 F2): a `serialize` NULL (NOMEM of the buffer) or a `snapshot_counts`
  # quirk used to fail the flush every interval for that shard, though `VACUUM INTO` would work.
  defp serialize_or_vacuum(conn, temp, id, verified?) do
    case Connection.exec(conn, "BEGIN") do
      :ok ->
        copied =
          try do
            case snapshot_counts(conn) do
              {:ok, bytes, count, free, ps} ->
                if bytes <= serialize_max_bytes() and
                     not compaction_worthwhile_counts?(count, free, ps),
                   do: serialize_main(conn),
                   else: :vacuum

              {:error, _} = err ->
                err
            end
          after
            # A read-only transaction: COMMIT and ROLLBACK are the same. Ignore "no transaction is
            # active" if SQLite already ended it on an error.
            _ = Connection.exec(conn, "COMMIT")
          end

        case copied do
          {:ok, blob} ->
            write_and_check_image(conn, temp, id, verified?, blob)

          :vacuum ->
            vacuum_snapshot(conn, temp, id, verified?)

          {:error, reason} ->
            Logger.warning(
              "shard #{id}: page-copy snapshot failed (#{inspect(reason, printable_limit: 300)}); using VACUUM INTO"
            )

            vacuum_snapshot(conn, temp, id, verified?)
        end

      {:error, reason} ->
        # "Could not check", never "corrupt": `verdict/1` classifies an unknown tag as unknown.
        Integrity.verdict({:error, {:snapshot_begin_failed, reason}})
    end
  end

  defp write_and_check_image(conn, temp, id, verified?, blob) do
    with :ok <- Fathom.FS.write(temp, blob) do
      case Integrity.run_quick_check_main_file(temp, &apply_scan_mmap/1) do
        :ok ->
          emit_snapshot_mode(id, :serialize)
          # The read transaction has ended, so this PASSIVE is the same one `do_snapshot/3` runs
          # after its VACUUM (pinned + quorum-gated under A2).
          Fathom.Shard.Replication.FlushCheckpoint.after_snapshot(conn, id)
          :ok

        {:error, reason} ->
          Logger.warning(
            "shard #{id}: page-copy snapshot failed its integrity check " <>
              "(#{inspect(reason, printable_limit: 300)}); using VACUUM INTO"
          )

          _ = Fathom.FS.rm(temp)
          vacuum_snapshot(conn, temp, id, verified?)
      end
    end
  end

  # The pre-serialize route: live `quick_check` (skipped when the caller verified), then
  # `VACUUM INTO`, on this same handle.
  defp vacuum_snapshot(conn, temp, id, verified?) do
    with :ok <- if(verified?, do: :ok, else: Integrity.run_quick_check(conn)) do
      emit_snapshot_mode(id, :vacuum)
      do_snapshot(conn, temp, id)
    end
  end

  # The image size and free-page counts of the snapshot the open read transaction sees. One
  # statement over the table-valued pragma functions.
  defp snapshot_counts(conn) do
    case Connection.query(
           conn,
           "SELECT c.page_count, f.freelist_count, s.page_size " <>
             "FROM pragma_page_count() c, pragma_freelist_count() f, pragma_page_size() s",
           []
         ) do
      {:ok, %{rows: [[count, free, ps]]}} when is_integer(count) and is_integer(ps) ->
        {:ok, count * ps, count, free, ps}

      {:ok, other} ->
        {:error, {:snapshot_counts, other}}

      {:error, reason} ->
        {:error, {:snapshot_counts, reason}}
    end
  end

  # `sqlite3_serialize` of `main` on this handle, inside the caller's read transaction.
  # `:flush_serialize_result_hook` (tests only) rewrites the result, to stand in for SQLite's
  # zero-filled unreadable page or a NULL buffer, which no test can provoke on demand.
  defp serialize_main(conn) do
    result =
      case Application.get_env(:fathom, :flush_serialize_result_hook) do
        fun when is_function(fun, 1) -> fun.(Exqlite.Sqlite3.serialize(conn, "main"))
        _ -> Exqlite.Sqlite3.serialize(conn, "main")
      end

    case result do
      {:ok, blob} when is_binary(blob) -> {:ok, blob}
      {:error, reason} -> {:error, {:serialize_failed, reason}}
    end
  end

  # Which primitive produced the VACUUM-route snapshot: `:serialize` (page copy) or `:vacuum`
  # (`VACUUM INTO`). The route event stays `:vacuum` for both (expert review 2026-10-22 r8 #9).
  defp emit_snapshot_mode(id, mode) do
    :telemetry.execute(
      [:fathom, :shard, :flush, :snapshot],
      %{count: 1},
      %{shard_id: id, mode: mode}
    )
  end

  # A transactionally-consistent copy of the live shard, safe to run while writers
  # are active (unlike a raw file copy). `VACUUM INTO` requires a string literal
  # path, so the destination (built from a validated shard id) is single-quote
  # escaped defensively.
  # Takes an ALREADY-OPEN connection (review 2026-08-26 #12) so the verify and the snapshot share
  # one handle. Opening and closing is the caller's job — `verify_and_snapshot/4` owns the `after`.
  # @doc false / public only so a test can drive the raise path directly (the caller,
  # verify_and_snapshot/4, closes the conn, so the restored `synchronous` can't be observed from
  # there). See shard_snapshot_sync_test.exs.
  @doc false
  def do_snapshot(conn, dest, shard_id \\ nil) do
    # The snapshot temp inherits this connection's safety_level (SQLite's sqlite3RunVacuum:
    # `pgflags = db->aDb[iDb].safety_level | ...`), so at synchronous=FULL every periodic flush
    # force-fsynced a full shard-sized file that `snapshot_and_upload/1` unlinks seconds later
    # (expert review 2026-07-24 #11). At ~640 KiB/shard × 1,000 write-active shards that is
    # ~128 MB per interval of pure-waste synchronous device writes, plus the SSD endurance.
    #
    # The temp has NO durability requirement: it is read back in-process for the Content-MD5 and
    # the PUT, and a crash mid-snapshot just leaves the shard dirty to be re-snapshotted next
    # interval. The durable artifact is the S3 object, still gated by content-md5 over these same
    # bytes. VACUUM INTO takes only a READ transaction on the source, so the live shard's WAL +
    # per-commit synchronous=FULL is untouched — the RPO contract's local layer is unaffected.
    #
    # Scoped to this connection only. Emphatically NOT for `checkpoint/1`: a checkpoint at
    # synchronous=OFF can corrupt the main database on power loss.
    Connection.exec(conn, "PRAGMA synchronous=OFF")

    # RESTORE synchronous=FULL IN AN `after`, so it happens even if VACUUM INTO RAISES rather than
    # returning `{:error}` (expert review 2026-09-18 #7). Pre-fix these were three flat statements:
    # a raise from the VACUUM step (Connection.query rescues only ArgumentError, so a different
    # exception propagates) skipped the restore, and the exception unwound through
    # verify_and_snapshot/4's `after Connection.close(conn)` — which, on the last connection to a WAL
    # database, runs the close-time checkpoint that UNLINKS `-wal`/`-shm`, at synchronous=OFF: a torn
    # database on power loss, uploaded as the durable object. The trigger is narrow (most VACUUM
    # failures return `{:error}` and already restored FULL below), but the consequence is unrecoverable.
    #
    # RESTORE IT BEFORE ANYTHING ELSE TOUCHES THIS CONNECTION (expert review 2026-08-01 #5): the
    # relaxation is for the throwaway VACUUM temp only. `synchronous` is a pager property and SQLite
    # derives checkpoint sync flags from it (`pPager->walSyncFlags`), so at OFF a checkpoint neither
    # fsyncs the WAL before backfill nor fsyncs the main database after — which is why the PASSIVE
    # checkpoint below and SQLite's close-time checkpoint must both run at FULL.
    result =
      try do
        Connection.query(conn, "VACUUM INTO '#{String.replace(dest, "'", "''")}'", [])
      after
        Connection.exec(conn, "PRAGMA synchronous=FULL")
      end

    # Checkpoint here, where nobody is waiting (expert review 2026-07-24 #4). Without this the
    # only thing truncating the WAL in steady state was SQLite's autocheckpoint, which runs
    # INLINE INSIDE A COMMITTING TENANT STATEMENT on whichever connection happens to cross the
    # threshold — a periodic multi-ms-to-tens-of-ms spike billed to an arbitrary client query,
    # and under synchronous=FULL it fsyncs the main database too. Worse, the VACUUM INTO above
    # is a long-lived reader that HOLDS BACK that passive checkpoint (docs/durability.md notes
    # the same interaction from the other side), so the WAL kept growing past the threshold and
    # the tenant that eventually won the race paid a larger checkpoint than the default implies.
    #
    # PASSIVE specifically: it never blocks writers and never waits on readers — it moves what it
    # can and returns — so it cannot add latency to a concurrent tenant. A `busy` result is the
    # normal "readers active" outcome and is ignored; the next interval retries. Runs only after
    # a successful snapshot, i.e. after this connection's read transaction has ended, which is
    # what unblocks the checkpointer in the first place.
    #
    # UNDER A2 IT IS PINNED AND QUORUM-GATED (expert review 2026-10-15 #1): an unconditional PASSIVE
    # here could backfill a commit the replication Session had not shipped, and the next writer's
    # log restart then erased it before any replica held it. With replication off this is the same
    # unconditional PASSIVE as before. See "Checkpoints of a replicated WAL" in `Session`.
    if match?({:ok, _}, result),
      do: Fathom.Shard.Replication.FlushCheckpoint.after_snapshot(conn, shard_id)

    case result do
      {:ok, _} -> :ok
      {:error, reason} -> {:error, reason}
    end
  end
end
