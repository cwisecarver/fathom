defmodule Mix.Tasks.Fathom.Snapshot do
  @shortdoc "Create, list, restore, or drop point-in-time shard snapshots"

  @moduledoc """
  Fathom-managed point-in-time snapshots of a shard's stored object (expert review
  2026-07-14 #12) — a snapshot is a server-side copy of the tenant's one SQLite
  object under `<shard>@snap-<id>`, and a restore is the copy back.

      mix fathom.snapshot create <shard> [label]   # snapshot the live object
      mix fathom.snapshot list <shard>             # list stored snapshots
      mix fathom.snapshot restore <shard> <id>     # restore live from a snapshot
      mix fathom.snapshot drop <shard> <id>        # delete a snapshot

  `create`/`list`/`drop` operate on stored objects directly. `restore` copies a
  snapshot over the live object; it drains any local coordinator and **refuses if a
  live node still owns the shard** (cross-node safe), so run it while the tenant is
  idle. In a running release, prefer the node console so the drain reaches the live
  coordinator: `Fathom.Snapshots.restore("<shard>", "<id>")`.

  Snapshots capture the last durably-flushed state — see `Fathom.Snapshots`.
  """

  use Mix.Task

  @impl true
  def run(args) do
    Mix.Task.run("app.config")

    case args do
      ["create", shard | rest] ->
        ensure_storage_deps!()
        opts = if rest == [], do: [], else: [label: Enum.join(rest, "-")]

        case Fathom.Snapshots.create(shard, opts) do
          {:ok, id} -> Mix.shell().info(id)
          {:error, reason} -> Mix.raise("snapshot failed: #{inspect(reason)}")
        end

      ["list", shard] ->
        ensure_storage_deps!()

        case Fathom.Snapshots.list(shard) do
          {:ok, []} ->
            Mix.shell().info("(no snapshots)")

          {:ok, snaps} ->
            Enum.each(snaps, fn %{id: id, bytes: bytes} ->
              Mix.shell().info("#{id}\t#{bytes} bytes")
            end)

          {:error, reason} ->
            Mix.raise("list failed: #{inspect(reason)}")
        end

      ["restore", shard, id] ->
        start_app!()

        case Fathom.Snapshots.restore(shard, id) do
          :ok ->
            Mix.shell().info("restored #{shard} to snapshot #{id}")

          {:error, {:held, owner}} ->
            Mix.raise(
              "refused: #{shard} is served by a live node (#{owner}); quiesce it, or run " <>
                "from that node's console: Fathom.Snapshots.restore(\"#{shard}\", \"#{id}\")"
            )

          {:error, {:shard_busy, reason}} ->
            Mix.raise("refused: #{shard} is busy (#{inspect(reason)}); quiesce it first")

          {:error, reason} ->
            Mix.raise("restore failed: #{inspect(reason)}")
        end

      ["drop", shard, id] ->
        ensure_storage_deps!()

        case Fathom.Snapshots.drop(shard, id) do
          :ok -> Mix.shell().info("dropped snapshot #{id}")
          {:error, reason} -> Mix.raise("drop failed: #{inspect(reason)}")
        end

      _ ->
        Mix.raise("usage: mix fathom.snapshot create|list|restore|drop <shard> [id|label]")
    end
  end

  # create/list/drop only touch stored objects — start the HTTP client (the S3 backend's
  # default pool) without booting the whole node (no ports bound). Local storage needs nothing.
  defp ensure_storage_deps! do
    {:ok, _} = Application.ensure_all_started(:req)
    :ok
  end

  # restore drains the live coordinator, so it needs the shard registry/supervisor — the app.
  # If a node already runs on this host the ports collide; use the node console instead.
  defp start_app! do
    case Application.ensure_all_started(:fathom) do
      {:ok, _} ->
        :ok

      {:error, reason} ->
        Mix.raise(
          "could not start fathom (restore needs the app running to drain the shard): " <>
            "#{inspect(reason)} — run restore from the node console instead"
        )
    end
  end
end
