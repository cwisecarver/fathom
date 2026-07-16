defmodule Mix.Tasks.Fathom.Shard do
  @shortdoc "Operator tooling: pull, inspect, or fork a shard's stored object"

  @moduledoc """
  Operator tooling over the shard `Storage` behaviour (expert review 2026-07-14 #14) — the
  recover/validate/clone commands that previously meant hand-rolling `aws-cli` + `sqlite3` against
  an undocumented key layout under incident pressure.

      mix fathom.shard pull <shard> [<path>]   # download the stored .db (default ./<shard>.db)
      mix fathom.shard inspect <shard>         # pull + read-only quick_check, user_version, tables
      mix fathom.shard fork <src> <dst>        # clone a live tenant to a NEW shard id (+ token)

  `pull` and `inspect` are the restore-drill / validation tools — `inspect` proves a stored object
  is a real, `quick_check`-clean database and reports its schema version and per-table row counts.
  An untested restore path is an unproven backup; run `inspect` on a sample of shards regularly (a
  scheduled fleet drill is a follow-up).

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

      _ ->
        Mix.raise(
          "usage: mix fathom.shard pull <shard> [path] | inspect <shard> | fork <src> <dst> | loss-report [limit]"
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
