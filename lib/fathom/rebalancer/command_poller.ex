defmodule Fathom.Rebalancer.CommandPoller do
  @moduledoc """
  Executes handoff commands addressed to this node — the node-local half of the cross-node
  command channel. Every `:command_poll_ms` it reads `Commands.pending_for/1` for this
  node's `Fathom.Rebalancer.node_key/0` and runs each:

    * **warm** — `Fathom.Shard.WarmFollower.warm_now/1` pulls the shard into this node's
      warm cache ahead of a handoff. Best-effort: a shard with no stored object (never
      flushed) or a transient pull failure still marks `done`, because the target's
      cold-open revalidates/pulls correctly anyway — warm only trims the blip.
    * **drain** — `Fathom.Shards.drain/2` flushes + releases the lease so another node can
      acquire it. `:ok` ⇒ `done`; a `{:error, _}` (e.g. `:busy` — connections didn't drain
      in time) ⇒ `failed`, and the orchestrator decides whether to retry. Skipped
      (`cancelled`) if the shard has no active pin when the poller reaches it — the handoff
      reverted and the source is serving again (finding #7).

  Gated by `:command_poller` (default off). Only this node's poller matches a command
  addressed to this node's key, so there's no cross-node contention.
  """
  use GenServer

  require Logger

  alias Fathom.Rebalancer
  alias Fathom.Rebalancer.{Commands, Overrides}
  alias Fathom.Shard.WarmFollower
  alias Fathom.Shards

  @default_poll_ms 1_000
  @default_drain_ms 10_000

  @doc "Whether this node acts on handoff commands (`:command_poller`, default off)."
  @spec enabled?() :: boolean()
  def enabled?, do: Application.get_env(:fathom, :command_poller, false) == true

  def start_link(opts), do: GenServer.start_link(__MODULE__, opts, name: __MODULE__)

  @doc "Runs one poll synchronously and returns how many commands were executed (tests)."
  @spec poll_now() :: non_neg_integer()
  def poll_now, do: GenServer.call(__MODULE__, :poll_now, 30_000)

  @impl true
  def init(_opts) do
    schedule()
    {:ok, %{}}
  end

  @impl true
  def handle_info(:poll, state) do
    do_poll()
    schedule()
    {:noreply, state}
  end

  @impl true
  def handle_call(:poll_now, _from, state), do: {:reply, do_poll(), state}

  defp do_poll do
    Rebalancer.node_key()
    |> Commands.pending_for()
    |> Enum.map(&execute/1)
    |> length()
  rescue
    # A Postgres blip just means we retry next tick — never crash the node.
    e ->
      Logger.warning("command poller tick failed: #{Exception.message(e)}")
      0
  end

  # Warm is best-effort: correctness comes from the target's cold-open, so even a skip is
  # `done` (the handoff proceeds; warm only saved a body transfer).
  defp execute(%{command: "warm", shard_id: id} = cmd) do
    detail =
      case WarmFollower.warm_now(id) do
        :ok -> "warmed"
        {:error, reason} -> "warm skipped (#{inspect(reason)})"
      end

    Commands.complete(cmd, "done", detail)
  end

  # Drain releases the lease. Failure (busy / drain_failed) is terminal-for-this-command;
  # the orchestrator re-issues if it still wants the move.
  #
  # Defense-in-depth for #7: re-check the pin immediately before draining. If the shard has
  # no active pin (never pinned, or reverted → failed_at set), the handoff was abandoned and
  # the source is serving again — draining now would strand it. Skip (cancel) instead. This
  # covers the race where the poller picks up the drain between issue and cancel_pending.
  defp execute(%{command: "drain", shard_id: id} = cmd) do
    case Overrides.for_shard(id) do
      %{failed_at: nil} ->
        case Shards.drain(id, drain_ms()) do
          :ok ->
            Commands.complete(cmd, "done", "drained")

          {:error, reason} ->
            Commands.complete(cmd, "failed", "drain failed (#{inspect(reason)})")
        end

      _gone_or_reverted ->
        Commands.complete(cmd, "cancelled", "pin reverted; drain skipped")
    end
  end

  defp execute(%{command: other} = cmd) do
    Commands.complete(cmd, "failed", "unknown command #{inspect(other)}")
  end

  defp schedule, do: Process.send_after(self(), :poll, poll_ms())
  defp poll_ms, do: Application.get_env(:fathom, :command_poll_ms, @default_poll_ms)
  defp drain_ms, do: Application.get_env(:fathom, :command_drain_ms, @default_drain_ms)
end
