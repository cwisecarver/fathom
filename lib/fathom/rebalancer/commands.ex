defmodule Fathom.Rebalancer.Commands do
  @moduledoc """
  The handoff command channel API. The orchestrator `issue/3`s a command for a node and
  `await/2`s its completion; each node's `CommandPoller` reads `pending_for/1` and
  `complete/3`s. Postgres carries it because there's no BEAM cluster to RPC across.
  """
  import Ecto.Query, only: [from: 2]

  alias Fathom.Rebalancer.Command
  alias Fathom.Repo

  @doc "Issues `command` (`\"warm\"` | `\"drain\"`) of `shard_id` for `node` to execute."
  @spec issue(String.t(), String.t(), String.t()) ::
          {:ok, Command.t()} | {:error, Ecto.Changeset.t()}
  def issue(shard_id, node, command) do
    %Command{}
    |> Command.changeset(%{shard_id: shard_id, node: node, command: command})
    |> Repo.insert()
  end

  @doc "Pending commands addressed to `node`, oldest first."
  @spec pending_for(String.t()) :: [Command.t()]
  def pending_for(node) do
    Repo.all(
      from c in Command,
        where: c.node == ^node and c.status == "pending",
        order_by: [asc: c.inserted_at]
    )
  end

  @doc "Marks a command terminal (`\"done\"` | `\"failed\"`) with an optional detail string."
  @spec complete(Command.t(), String.t(), String.t() | nil) ::
          {:ok, Command.t()} | {:error, Ecto.Changeset.t()}
  def complete(%Command{} = command, status, detail \\ nil) do
    command
    |> Command.changeset(%{status: status, detail: detail})
    |> Repo.update()
  end

  @doc "The command by id, or nil."
  @spec get(integer()) :: Command.t() | nil
  def get(id), do: Repo.get(Command, id)

  @doc """
  Cancels every still-`pending` `drain` for `shard_id` (finding #7). Called on a handoff
  revert so a drain whose `await` timed out (row left `pending`) can't fire later and drain
  the source right after traffic was restored to it. Returns the number cancelled.
  """
  @spec cancel_pending_drains(String.t()) :: non_neg_integer()
  def cancel_pending_drains(shard_id) do
    {n, _} =
      Repo.update_all(
        from(c in Command,
          where: c.shard_id == ^shard_id and c.command == "drain" and c.status == "pending"
        ),
        set: [status: "cancelled", detail: "handoff reverted", updated_at: DateTime.utc_now()]
      )

    n
  end

  @doc """
  Blocks until command `id` reaches a terminal status or `timeout_ms` elapses (polling
  every `poll_ms`). Returns `{:ok, command}` when `done`, `{:error, {:command_failed,
  detail}}` when `failed`, or `{:error, :timeout}`. Deadline is monotonic.
  """
  @spec await(integer(), keyword()) ::
          {:ok, Command.t()} | {:error, {:command_failed, String.t() | nil}} | {:error, :timeout}
  def await(id, opts \\ []) do
    timeout_ms = Keyword.get(opts, :timeout_ms, 30_000)
    poll_ms = Keyword.get(opts, :poll_ms, 200)
    deadline = System.monotonic_time(:millisecond) + timeout_ms
    do_await(id, poll_ms, deadline)
  end

  defp do_await(id, poll_ms, deadline) do
    case get(id) do
      %Command{status: "done"} = c ->
        {:ok, c}

      %Command{status: "failed", detail: detail} ->
        {:error, {:command_failed, detail}}

      _pending_or_missing ->
        if System.monotonic_time(:millisecond) >= deadline do
          {:error, :timeout}
        else
          Process.sleep(poll_ms)
          do_await(id, poll_ms, deadline)
        end
    end
  end
end
