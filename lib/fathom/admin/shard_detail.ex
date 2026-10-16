defmodule Fathom.Admin.ShardDetail do
  @moduledoc """
  The data behind the per-shard admin page (`FathomWeb.AdminShardLive`, expert review 2026-10-15 #4).

  Two halves, kept apart on purpose:

    * `load_control_plane/1` — bounded Postgres reads for ONE shard (directory row, LB pin, pending
      rebalancer commands, recent load samples, token ledger). Run off-process by the LiveView
      (`start_async`), once per page load — never polled.
    * `local_state/1` — this node's coordinator state read from LOCK-FREE sources only (the
      `Fathom.ShardRegistry` entry, the `Fathom.Admin.FlushWatermark` row, `Fathom.Shard.WriteCounter`).
      It never calls into the coordinator, so rendering the page cannot queue behind a busy shard.

  Every function takes an already-canonical shard id (`Fathom.ShardId.cast/1`) and every query is
  keyed on it, so a page for shard A cannot read shard B's rows (shard-isolation gate).
  """
  import Ecto.Query, only: [from: 2]

  alias Fathom.Directory
  alias Fathom.HranaAuth.Ledger
  alias Fathom.Rebalancer.{Command, LoadSample, Overrides}
  alias Fathom.Repo

  @registry Fathom.ShardRegistry
  @watermarks Fathom.Admin.FlushWatermark
  @sample_limit 20
  @command_limit 20
  @ledger_limit 25

  @type control_plane :: %{
          directory: Directory.Shard.t() | nil,
          override: Fathom.Rebalancer.Override.t() | nil,
          commands: [Command.t()],
          samples: [LoadSample.t()],
          issuances: [Fathom.HranaAuth.Issuance.t()],
          outstanding: [Fathom.HranaAuth.Issuance.t()]
        }

  @doc "Bounded control-plane reads for `shard_id`."
  @spec load_control_plane(String.t()) :: control_plane()
  def load_control_plane(shard_id) do
    directory =
      case Directory.get(shard_id) do
        {:ok, row} -> row
        :error -> nil
      end

    %{
      directory: directory,
      override: Overrides.for_shard(shard_id),
      commands: pending_commands(shard_id),
      samples: samples(shard_id),
      issuances: Ledger.history(shard_id, @ledger_limit),
      outstanding: Ledger.outstanding(shard_id)
    }
  end

  defp pending_commands(shard_id) do
    Repo.all(
      from c in Command,
        where: c.shard_id == ^shard_id and c.status == "pending",
        order_by: [desc: c.inserted_at],
        limit: @command_limit
    )
  end

  defp samples(shard_id) do
    Repo.all(
      from s in LoadSample,
        where: s.shard_id == ^shard_id,
        order_by: [desc: s.sampled_at],
        limit: @sample_limit
    )
  end

  @doc """
  This node's view of the shard from lock-free sources: whether a coordinator is registered, its
  `pid` (for the on-demand liveness-mode probe), and the dirtiness / flush age derived from the
  published flush watermark with the same expression `Fathom.Shard.unflushed?/1` uses.
  """
  @spec local_state(String.t()) :: %{
          open?: boolean(),
          pid: pid() | nil,
          dirty?: boolean() | nil,
          flush_age_ms: non_neg_integer() | nil
        }
  def local_state(shard_id) do
    pid =
      case Registry.lookup(@registry, shard_id) do
        [{pid, _} | _] -> pid
        [] -> nil
      end

    {dirty?, age} = watermark(shard_id)
    %{open?: pid != nil, pid: pid, dirty?: dirty?, flush_age_ms: age}
  rescue
    ArgumentError -> %{open?: false, pid: nil, dirty?: nil, flush_age_ms: nil}
  end

  defp watermark(shard_id) do
    case :ets.lookup(@watermarks, shard_id) do
      [{_, flushed_through, counter_gen, flushed_at_mono}] ->
        dirty? =
          counter_gen != Fathom.Shard.WriteCounter.generation() or
            Fathom.Shard.WriteCounter.count(shard_id) > flushed_through

        {dirty?, max(System.monotonic_time(:millisecond) - flushed_at_mono, 0)}

      [] ->
        {nil, nil}
    end
  rescue
    ArgumentError -> {nil, nil}
  end

  @doc """
  The coordinator's liveness mode, asked of the coordinator itself (a `GenServer.call`, the same as
  `Fathom.Shard.acquire_gen/1` but with a short timeout). ON DEMAND ONLY — the page calls this from
  a button's `start_async`, never from a timer or a render. `:heartbeat` when the node heartbeat
  proves liveness, `:legacy` for per-shard lease renewals; `:unavailable` if the coordinator is
  gone or too busy to answer in `timeout_ms`.
  """
  @spec liveness_mode(pid(), timeout()) :: :heartbeat | :legacy | :unavailable
  def liveness_mode(pid, timeout_ms \\ 2_000) when is_pid(pid) do
    case GenServer.call(pid, :acquire_gen, timeout_ms) do
      nil -> :legacy
      gen when is_integer(gen) -> :heartbeat
    end
  catch
    :exit, _ -> :unavailable
  end

  @doc "The lease probe (`Storage.lease_stealable_at/1`). On-demand only."
  @spec lease(String.t()) :: :free | {:held, String.t(), integer()} | {:error, term()}
  def lease(shard_id) do
    Fathom.Shard.Storage.lease_stealable_at(shard_id)
  catch
    :exit, reason -> {:error, reason}
  end
end
