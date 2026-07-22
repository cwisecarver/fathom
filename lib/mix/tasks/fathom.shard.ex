defmodule Mix.Tasks.Fathom.Shard do
  @shortdoc "Operator tooling: pull, inspect, or fork a shard's stored object"

  @moduledoc """
  Operator tooling over the shard `Storage` behaviour (expert review 2026-07-14 #14) — the
  recover/validate/clone commands that previously meant hand-rolling `aws-cli` + `sqlite3` against
  an undocumented key layout under incident pressure.

      mix fathom.shard pull <shard> [<path>]        # download the stored .db (default ./<shard>.db)
      mix fathom.shard inspect <shard>              # pull + read-only quick_check, user_version, tables
      mix fathom.shard fork <src> <dst>             # clone a live tenant to a NEW shard id (+ token)
      mix fathom.shard quarantines                  # list local quarantine files (shard, kind, age, size)
      mix fathom.shard quarantine-diff <file> <shard>  # per-table row deltas vs the live object

  `pull` and `inspect` are the restore-drill / validation tools — `inspect` proves a stored object
  is a real, `quick_check`-clean database and reports its schema version and per-table row counts.
  An untested restore path is an unproven backup; run `inspect` on a sample of shards regularly (a
  scheduled fleet drill is a follow-up).

  `quarantines` and `quarantine-diff` are the recovery tools for the coordinator's designated
  data-loss artifacts (expert review #23): the `.db.fenced/.forked/.corrupt` files it renames aside
  to *preserve* acked-but-unflushed / corrupt local copies instead of dropping them. `quarantines`
  enumerates them on this node (no ssh + ls of a fleet-sized dir); `quarantine-diff` attaches a
  quarantine file and the shard's current stored object and reports per-table row-count deltas —
  enough to decide merge vs discard. The TempReaper age-caps them (`:quarantine_retention_ms`,
  default 30d) so they don't leak forever; a `fathom.shard.quarantines` gauge tracks the standing
  count. Recording each quarantine in the directory (survives the node, joins the loss report) and a
  stale-`heartbeat/*`-object janitor are scoped follow-ups.

  `fork` is the database-forking kernel (a fork is one object copy): it clones `src`'s last
  durably-flushed state to a brand-new `dst` shard id, registers `dst` at `src`'s schema version,
  and mints a `dst` token — for preview environments, per-tenant staging, test-database forking, or
  cloning a tenant to debug an incident. It does NOT disrupt `src`. Restore of a shard from a
  point-in-time snapshot is `mix fathom.snapshot restore` (#12); a fleet revert is the migrator.

  In a running release, prefer the node console for `fork` (`Fathom.Tenants.fork/2`) so it shares the
  live directory + token secret; `pull`/`inspect` only touch stored objects.
  """
  use Mix.Task

  alias Fathom.Shard.{Connection, Storage}

  @impl true
  def run(args) do
    Mix.Task.run("app.config")

    case args do
      ["pull", shard] ->
        pull(shard, "#{shard}.db")

      ["pull", shard, path] ->
        pull(shard, path)

      ["inspect", shard] ->
        inspect_shard(shard)

      ["fork", src, dst] ->
        fork(src, dst)

      ["loss-report"] ->
        loss_report(100)

      ["loss-report", n] ->
        loss_report(parse_limit(n))

      ["quarantines"] ->
        quarantines()

      ["quarantine-diff", file, shard] ->
        quarantine_diff(file, shard)

      _ ->
        Mix.raise(
          "usage: mix fathom.shard pull <shard> [path] | inspect <shard> | fork <src> <dst> | " <>
            "loss-report [limit] | quarantines | quarantine-diff <file> <shard>"
        )
    end
  end

  defp pull(shard, path) do
    storage_deps!()

    case Storage.pull(shard, path) do
      {:ok, nil} -> Mix.raise("no stored object for #{shard} (never flushed, or deleted)")
      {:ok, etag} -> Mix.shell().info("pulled #{shard} -> #{path} (etag #{inspect(etag)})")
      {:error, reason} -> Mix.raise("pull failed: #{inspect(reason)}")
    end
  end

  defp inspect_shard(shard) do
    storage_deps!()
    tmp = Path.join(System.tmp_dir!(), "fathom_inspect_#{shard}_#{System.system_time()}.db")

    try do
      case Storage.pull(shard, tmp) do
        {:ok, nil} ->
          Mix.raise("no stored object for #{shard}")

        {:ok, _etag} ->
          {:ok, conn} = Exqlite.Sqlite3.open(tmp)

          try do
            report(shard, conn)
          after
            Exqlite.Sqlite3.close(conn)
          end

        {:error, reason} ->
          Mix.raise("pull failed: #{inspect(reason)}")
      end
    after
      for suffix <- ["", "-wal", "-shm"], do: File.rm(tmp <> suffix)
    end
  end

  defp report(shard, conn) do
    quick = scalar(conn, "PRAGMA quick_check")
    version = scalar(conn, "PRAGMA user_version")
    pages = scalar(conn, "PRAGMA page_count")
    page_size = scalar(conn, "PRAGMA page_size")
    bytes = to_int(pages) * to_int(page_size)

    Mix.shell().info("shard:          #{shard}")
    Mix.shell().info("quick_check:    #{quick}")
    Mix.shell().info("user_version:   #{version}")
    Mix.shell().info("size:           #{bytes} bytes (#{pages} pages)")
    Mix.shell().info("tables:")

    for [name] <- rows(conn, tables_sql()) do
      count = scalar(conn, "SELECT count(*) FROM \"#{escape(name)}\"")
      Mix.shell().info("  #{name}\t#{count} rows")
    end

    if quick != "ok",
      do: Mix.shell().error("WARNING: quick_check is not 'ok' — this stored object is corrupt")
  end

  defp fork(src, dst) do
    # fork needs the directory (src schema version, dst row) + token secret — the app.
    start_app!()

    case Fathom.Tenants.fork(src, dst) do
      {:ok, tenant} ->
        Mix.shell().info("forked #{src} -> #{dst}")
        Mix.shell().info("url:        #{tenant.url}")
        if tenant.auth_token, do: Mix.shell().info("auth_token: #{tenant.auth_token}")

      {:error, :already_exists} ->
        Mix.raise("refused: #{dst} already exists")

      {:error, :tombstoned} ->
        Mix.raise("refused: #{dst} was deleted and its id can't be reused")

      {:error, :no_source} ->
        Mix.raise("refused: #{src} has no stored object / directory row to fork")

      {:error, reason} ->
        Mix.raise("fork failed: #{inspect(reason)}")
    end
  end

  # Post-node-loss tenant loss report (#28): the shards active since their last durable flush, with
  # the per-tenant loss window. Needs Postgres — run it on an admin/recovery box, or from a live
  # node's console: `Fathom.Directory.flush_lag_report()`.
  defp loss_report(limit) do
    start_app!()

    case Fathom.Directory.flush_lag_report(limit) do
      [] ->
        Mix.shell().info("no shards active since their last durable flush — nothing to report")

      rows ->
        Mix.shell().info("shard\tlast_active_at\tlast_flushed_at\tloss_window")

        for %{shard_id: id, last_active_at: active, last_flushed_at: flushed} <- rows do
          Mix.shell().info(
            "#{id}\t#{active}\t#{flushed || "(never)"}\t#{window(active, flushed)}"
          )
        end
    end
  end

  # Quarantine inventory (expert review #23): a local disk scan of the shard data dir for the three
  # quarantine kinds the coordinator preserves acked-but-unflushed / corrupt copies in. Replaces
  # "ssh + ls a fleet-sized data dir under incident pressure". No app/storage needed — pure disk.
  @quarantine_re ~r/^(?<shard>.+)\.db\.(?<kind>fenced|forked|corrupt)\./

  defp quarantines do
    files = Fathom.Shard.quarantine_files()

    if files == [] do
      Mix.shell().info("no quarantine files in #{Fathom.Shard.data_dir()}")
    else
      now = System.system_time(:millisecond)
      Mix.shell().info("shard\tkind\tage\tsize\tfile")

      for file <- Enum.sort(files) do
        {shard, kind} = parse_quarantine(file)
        {age, size} = quarantine_stat(file, now)
        Mix.shell().info("#{shard}\t#{kind}\t#{age}\t#{size}\t#{file}")
      end

      Mix.shell().info(
        "\n#{length(files)} quarantine file(s). Row-level detail vs the live object: " <>
          "mix fathom.shard quarantine-diff <file> <shard>"
      )
    end
  end

  defp parse_quarantine(file) do
    case Regex.named_captures(@quarantine_re, Path.basename(file)) do
      %{"shard" => shard, "kind" => kind} -> {shard, kind}
      _ -> {Path.basename(file), "?"}
    end
  end

  defp quarantine_stat(file, now_ms) do
    case File.stat(file, time: :posix) do
      {:ok, %File.Stat{size: size, mtime: mtime_sec}} ->
        {"#{div(now_ms - mtime_sec * 1000, 1000)}s", "#{size}B"}

      _ ->
        {"?", "?"}
    end
  end

  # Recovery helper (#23): the promised "the forked/fenced writes live in that file" made concrete —
  # per-table row-count deltas between a quarantine file and the shard's current live object, enough
  # to decide merge vs discard. The quarantine file is COPIED to a temp before opening so the
  # operator's recovery artifact is never modified (no WAL sidecar next to it).
  defp quarantine_diff(file, shard) do
    unless File.exists?(file), do: Mix.raise("no such quarantine file: #{file}")
    storage_deps!()

    qtmp = copy_to_temp!(file)
    ltmp = Path.join(System.tmp_dir!(), "fathom_qdiff_live_#{System.system_time()}.db")

    try do
      case Storage.pull(shard, ltmp) do
        {:ok, nil} ->
          Mix.raise("no stored object for #{shard} to diff against")

        {:ok, _etag} ->
          {:ok, qconn} = Exqlite.Sqlite3.open(qtmp)
          {:ok, lconn} = Exqlite.Sqlite3.open(ltmp)

          try do
            diff_report(shard, file, qconn, lconn)
          after
            Exqlite.Sqlite3.close(qconn)
            Exqlite.Sqlite3.close(lconn)
          end

        {:error, reason} ->
          Mix.raise("pull failed: #{inspect(reason)}")
      end
    after
      for t <- [qtmp, ltmp], s <- ["", "-wal", "-shm"], do: File.rm(t <> s)
    end
  end

  defp copy_to_temp!(file) do
    tmp =
      Path.join(
        System.tmp_dir!(),
        "fathom_qdiff_q_#{System.system_time()}_#{System.unique_integer([:positive])}.db"
      )

    :ok = File.cp(file, tmp)
    tmp
  end

  defp diff_report(shard, file, qconn, lconn) do
    q = table_counts(qconn)
    l = table_counts(lconn)
    names = (Map.keys(q) ++ Map.keys(l)) |> Enum.uniq() |> Enum.sort()

    Mix.shell().info("quarantine: #{file}")
    Mix.shell().info("live:       stored object for #{shard}")
    Mix.shell().info("table\tquarantine\tlive\tdelta")

    ahead =
      for name <- names, reduce: [] do
        acc ->
          qn = Map.get(q, name, 0)
          ln = Map.get(l, name, 0)
          Mix.shell().info("#{name}\t#{qn}\t#{ln}\t#{qn - ln}")
          if qn > ln, do: [name | acc], else: acc
      end

    if ahead == [] do
      Mix.shell().info(
        "\nno table has more rows in the quarantine than in the live object — likely safe to DISCARD."
      )
    else
      Mix.shell().error(
        "\nquarantine has MORE rows in: #{ahead |> Enum.reverse() |> Enum.join(", ")} — " <>
          "inspect before discarding (possible unflushed writes)."
      )
    end
  end

  defp table_counts(conn) do
    for [name] <- rows(conn, tables_sql()), into: %{} do
      {name, to_int(scalar(conn, "SELECT count(*) FROM \"#{escape(name)}\""))}
    end
  end

  defp window(_active, nil), do: "never flushed"

  defp window(%DateTime{} = active, %DateTime{} = flushed),
    do: "#{DateTime.diff(active, flushed, :second)}s"

  defp parse_limit(n) do
    case Integer.parse(n) do
      {v, _} when v > 0 -> v
      _ -> 100
    end
  end

  defp tables_sql,
    do:
      "SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%' ORDER BY name"

  # Table names come from the shard's OWN sqlite_master (not user input), but double any embedded
  # quote defensively — identifiers can't be bound as parameters in SQLite.
  defp escape(name), do: String.replace(name, "\"", "\"\"")

  defp scalar(conn, sql) do
    case rows(conn, sql) do
      [[v] | _] -> v
      _ -> nil
    end
  end

  defp rows(conn, sql) do
    case Connection.query(conn, sql, []) do
      {:ok, %{rows: rows}} -> rows
      _ -> []
    end
  end

  defp to_int(n) when is_integer(n), do: n
  defp to_int(_), do: 0

  # pull/inspect only touch stored objects — start the HTTP client (the S3 backend's pool) without
  # booting the node (no ports bound). Local storage needs nothing.
  defp storage_deps! do
    {:ok, _} = Application.ensure_all_started(:req)
    :ok
  end

  defp start_app! do
    case Application.ensure_all_started(:fathom) do
      {:ok, _} ->
        :ok

      {:error, reason} ->
        Mix.raise(
          "could not start fathom (#{inspect(reason)}). If a node already runs on this host the " <>
            "ports collide — run fork from that node's console: Fathom.Tenants.fork(\"#{"<src>"}\", \"<dst>\")"
        )
    end
  end
end
