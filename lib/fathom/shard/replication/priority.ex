defmodule Fathom.Shard.Replication.Priority do
  @moduledoc """
  The scheduling priority of the A2 replication path (expert review 2026-10-21 r7 #12), set by
  `REPLICATION_PROCESS_PRIORITY` = `normal | hubs | all` (`:replication_process_priority`).
  **Default `normal`: behaviour is unchanged until the rig A/B decides.**

  ## Why

  A replicated commit crosses ~12-15 scheduling points: stream -> Session, the Session's dirty-IO
  file jobs, Session -> Shipper -> Writer, the follower's reader -> worker, the worker's dirty-IO
  jobs, and the acks back. Every process on that chain ran at `normal`, and the dirty-IO threads also
  run every tenant's SQLite `multi_step` and FULL-commit fsync, so the Session's 1-3 us preads of a
  page-cached WAL waited in the shared dirty-IO run queue behind 1-50 ms tenant statements (round 6's
  `commit_stage{stage=build}` queue wait, rig3 6.7 ms mean). ERTS keeps per-priority queues in the
  dirty run queues as in the normal ones, so a `:high` process's dirty job goes ahead of every
  normal-priority tenant step. Panel loopback (3 interleaved rounds, dirty-IO saturated): commit p50
  5.8-6.2 ms -> 4.4-4.6 (`hubs`) -> 0.30-1.40 (`all`), p99 7.8-8.5 -> 2.3-2.6; deep normal run
  queue p50 3.0-4.8 -> 1.0-1.2 (`all`); idle neutral; an OS-oversubscribed mix showed no effect,
  which is why the rig, not loopback, decides the default.

  ## The modes

    * `normal` — every replication process at `normal` (the pre-#12 behaviour).
    * `hubs` — the four per-link hub processes at `:high`: the `Shipper`, its `Writer`, the
      follower's per-connection reader and its workers. Each serves every shard on its link.
    * `all` — `hubs`, plus every `Session` and the deferred-WAL-sync task it spawns (a `:high`
      Session gated on a `normal` fdatasync would just move the queue wait into the task).

  Never `:max`. Deliberately LEFT at `normal` in every mode, because they hold dirty threads for
  whole fsyncs or SQLite checkpoints and would crowd tenants out of the dirty queue during a seed or
  reset wave: seed tasks, the follower's offset-0 absorb push tasks, the threshold-checkpoint task.
  Spawned processes do not inherit priority, so nothing is needed to keep them `normal`.

  ## Starvation

  Each `:high` process does bounded work per message, with no loops and no polling; their total
  demand is the replication CPU (~8% of a node, round-3 `repl-cpu`), so this reorders work rather
  than adding any. The one long inline job in a hub is the seed-chunk MAC over 4 MiB in the Shipper
  and the reader, about 2-4 ms per chunk. The A/B's falsifier includes tenant READ p99: a rise of
  20% or more means the `:high` path is crowding tenants out.

  Read once, at process start (no per-commit cost); a change takes effect for processes started
  after it.
  """

  @type role :: :hub | :session
  @type mode :: :normal | :hubs | :all

  @doc "The configured mode."
  @spec mode() :: mode()
  def mode, do: Application.get_env(:fathom, :replication_process_priority, :normal)

  @doc "The priority a process in `role` runs at under the configured mode."
  @spec for_role(role()) :: :normal | :high
  def for_role(role), do: for_role(role, mode())

  @doc false
  @spec for_role(role(), mode()) :: :normal | :high
  def for_role(:hub, mode) when mode in [:hubs, :all], do: :high
  def for_role(:session, :all), do: :high
  def for_role(_role, _mode), do: :normal

  @doc "Set the calling process's priority for `role`. A no-op at `normal`."
  @spec adopt(role()) :: :ok
  def adopt(role) do
    case for_role(role) do
      :high ->
        Process.flag(:priority, :high)
        :ok

      :normal ->
        :ok
    end
  end

  @doc """
  Parse a `REPLICATION_PROCESS_PRIORITY` value. Anything outside the three modes is an error, so a
  typo refuses to boot rather than silently running `normal`.
  """
  @spec parse(String.t()) :: {:ok, mode()} | :error
  def parse("normal"), do: {:ok, :normal}
  def parse("hubs"), do: {:ok, :hubs}
  def parse("all"), do: {:ok, :all}
  def parse(_), do: :error
end
