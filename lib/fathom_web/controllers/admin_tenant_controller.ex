defmodule FathomWeb.AdminTenantController do
  @moduledoc """
  Admin-only tenant data export (expert review 2026-07-14 #15) — the download half of the
  delete/export pair. Because a tenant *is* one SQLite file, export is a straight download of
  that file: `Fathom.Tenants.export/1` pulls the shard's durable stored object to a temp file,
  and this streams it back through the BasicAuth-gated `/admin` endpoint (never a public URL,
  so the data never leaves the operator boundary), then deletes the temp so no exported copy
  lingers on disk.

  The `:id` is validated by `Fathom.Tenants.export/1` (`ShardId.cast`), which rejects anything
  that isn't a well-formed shard id — so a path-traversal `..%2F` id is a 400, not a file read.
  """
  use FathomWeb, :controller

  def export(conn, %{"id" => id}) do
    case Fathom.Tenants.export(id) do
      {:ok, %{path: path, filename: filename}} ->
        try do
          # Read + stream from memory then delete the temp — small shards (fathom's thesis)
          # make this cheap; a streaming-download-then-cleanup for very large shards is a
          # follow-up. Deleting after the send keeps an exported copy from lingering (GDPR).
          data = File.read!(path)

          send_download(conn, {:binary, data},
            filename: filename,
            content_type: "application/x-sqlite3"
          )
        after
          File.rm(path)
        end

      {:error, :not_stored} ->
        conn
        |> put_resp_content_type("text/plain")
        |> send_resp(404, "no stored data for #{id} (never flushed, or deleted)")

      {:error, :invalid_shard_id} ->
        conn
        |> put_resp_content_type("text/plain")
        |> send_resp(400, "invalid shard id")

      {:error, _reason} ->
        conn
        |> put_resp_content_type("text/plain")
        |> send_resp(500, "export failed")
    end
  end
end
