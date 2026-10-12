defmodule Fathom.Shards.RemoteFlush do
  @moduledoc """
  The cross-node half of `Fathom.Shards.flush/1` (expert review 2026-10-12 #7).

  `Shards.flush/1` only sees THIS node's Registry. With no local coordinator and no kept local file it
  used to answer `:ok` ("nothing is writing here, so the stored object is current"), but on a
  multi-node fleet (N-1)/N of admin calls land on a node that is not the shard's holder, so export,
  keystone fork and `flush: true` snapshots silently read the holder's last PERIODIC flush and
  missed up to one flush interval of acked writes.

  Resolution, by what the S3 lease says:

    * **free** (no lock, or its owner is dead): nobody is writing, the stored object is current, `:ok`.
    * **held by this node or by a migrator**: no remote coordinator to ask, `:ok` (a migrator holds
      the lease only while the shard is drained).
    * **held by another node**: issue a `"flush"` command to that node through
      `Fathom.Rebalancer.Commands` (the channel the migration drain uses — Postgres-carried, there is
      no BEAM cluster to RPC across) and await it. Anything short of a confirmed `done` — the command
      poller is off fleet-wide, the holder's heartbeat is not live, the command failed or timed out,
      the lease probe itself failed — is `{:error, :remote_holder_unflushed}`. Never `:ok`.

  Like the migration drain this addresses the node NAME parsed from the lease owner
  (`"<node>#<incarnation>"`), so a deployment overriding `:node_key` gets the error rather than a
  false `:ok`.
  """
  alias Fathom.Rebalancer.Commands
  alias Fathom.Shard.Storage

  @default_timeout_ms 15_000

  @spec flush(Fathom.ShardId.t()) :: :ok | {:error, :remote_holder_unflushed}
  def flush(shard_id) do
    case Storage.lease_holder(shard_id) do
      :free ->
        :ok

      {:held, owner} ->
        case remote_node(owner) do
          {:ok, node_name} -> flush_on(shard_id, owner, node_name)
          :local -> :ok
        end

      {:error, _reason} ->
        # Can't tell who holds it, so can't claim the object is current.
        {:error, :remote_holder_unflushed}
    end
  rescue
    _ -> {:error, :remote_holder_unflushed}
  catch
    :exit, _ -> {:error, :remote_holder_unflushed}
  end

  # A migrator's owner is "migrator@…" and our own node's is "<this node>#<incarnation>": no
  # coordinator on another node to ask.
  defp remote_node(owner) do
    with false <- String.starts_with?(owner, "migrator@"),
         [node_name, _incarnation] <- String.split(owner, "#", parts: 2),
         true <- node_name != "" and node_name != to_string(node()) do
      {:ok, node_name}
    else
      _ -> :local
    end
  end

  defp flush_on(shard_id, owner, node_name) do
    if Application.get_env(:fathom, :command_poller, false) == true and heartbeat_fresh?(owner) do
      issue_and_await(shard_id, node_name)
    else
      {:error, :remote_holder_unflushed}
    end
  end

  # Only a live heartbeat means a poller over there can answer; a dead-but-still-leased node never
  # does, and parking the caller for the full await would only delay the same error.
  defp heartbeat_fresh?(owner) do
    case Storage.read_heartbeat(owner) do
      {:ok, %{owner: ^owner, expires_at_ms: exp}} -> exp > Storage.now_ms()
      _ -> false
    end
  end

  defp issue_and_await(shard_id, node_name) do
    with {:ok, cmd} <- Commands.issue(shard_id, node_name, "flush") do
      case Commands.await(cmd.id, timeout_ms: timeout_ms()) do
        {:ok, _} ->
          :ok

        {:error, reason} ->
          # Don't leave it pending for the holder to fire after we've reported failure.
          _ = Commands.cancel_if_pending(cmd.id, "flusher gave up: #{inspect(reason)}")
          {:error, :remote_holder_unflushed}
      end
    else
      _ -> {:error, :remote_holder_unflushed}
    end
  end

  defp timeout_ms, do: Application.get_env(:fathom, :remote_flush_ms, @default_timeout_ms)
end
