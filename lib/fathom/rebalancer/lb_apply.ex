defmodule Fathom.Rebalancer.LbApply do
  @moduledoc """
  Applies the exception table to the load balancer: render the current overrides
  (`LbMap.current/0`), promote them to `:lb_map_path` (the file the LB `include`s), and run
  the optional `:lb_reload_cmd` to make nginx pick it up.

  ## Promotion is temp-write → config-test → atomic rename (finding #3)

  The map is never written in place. It is rendered to a temp file in the *same* directory,
  optionally validated by `:lb_test_cmd`, and only then `File.rename/2`d over `:lb_map_path`
  — an atomic same-filesystem replace. This closes two holes an in-place `File.write!` left:

    * **A malformed render never lands on disk.** A live `nginx -s reload` keeps the old
      workers on the old config, so a bad file only degrades to "stale LB" *while running* —
      but the broken file stays on disk and the next nginx cold start (deploy, crash, OOM,
      reschedule) pulls it and fails to start → every shard's routing down, decoupled from
      the change that caused it. The config-test refuses to promote a file nginx can't load.
    * **No half-written file is ever visible.** A truncate-then-write races the reload
      trigger (prod `nginx -s reload`, or the rig sidecar's mtime watch) which can fire
      against a partial file. A rename makes the file appear complete-or-old, never partial.

  On a config-test failure the temp file is removed and the last-good `:lb_map_path` is kept
  untouched; `apply!` returns `{:error, reason}` so the caller can decline to drain.

  With `:lb_map_path` unset it's a **no-op** (decision-plane-only / tests), so the override
  table can be maintained without any LB wiring. The reload is deployment-specific (prod:
  `nginx -s reload`; the rig: a docker HUP — see `deploy/chaos`).

  Config: `:lb_map_path` (the included file), `:lb_test_cmd` (optional validation; `{}` is
  substituted with the candidate temp path and `LB_MAP_CANDIDATE` is exported), `:lb_reload_cmd`
  (optional reload).
  """
  require Logger

  alias Fathom.Rebalancer.LbMap
  alias Fathom.Repo

  # A fixed application key for the fleet-wide LB-map advisory lock (finding #10) — any
  # constant unique to this lock; pg_advisory_lock takes a single bigint.
  @lock_key 7_040_010_010

  @doc """
  Renders + promotes the map (atomic) and reloads the LB. Returns `:ok` when the flip is
  live-or-will-be, or `{:error, reason}` when it is **known not live** so the caller can
  decline to drain (finding #11):

    * `{:error, {:config_test_failed | :config_test_raised, ..}}` / a `File` posix error —
      the candidate failed its test or couldn't be written; the last-good file is kept and
      the flip did not happen.
    * `{:error, {:reload_failed, code}}` / `{:error, {:reload_raised, msg}}` — the map was
      promoted (valid on disk for the next cold start) but the configured reload command
      failed, so the running LB may not have picked it up.

  `:ok` also covers the decision-plane-only mode (`:lb_map_path` unset) and out-of-band
  reload (`:lb_reload_cmd` unset — e.g. the rig sidecar HUPs on the map's mtime), where the
  app legitimately can't confirm the reload and proceeds optimistically.

  ## Serialized + idempotent (finding #10)

  The render+write+reload runs under a fleet-wide Postgres **advisory lock** (held on one
  checked-out connection), so two `apply!`s on different nodes can't interleave a stale read
  with a later write and drop a pin from the *file* though it's in the DB. Each render reads
  the full override table, so serialized last-writer-wins is correct. And a re-render that is
  byte-identical to the on-disk file is a **no-op** (no write, no reload) — which is what
  lets the leader `RebalanceJob` re-render every tick cheaply to self-heal drift (this),
  dead-node pins (#1b), and failed reloads (#11) without HUPing nginx every minute.
  """
  @spec apply!() :: :ok | {:error, term()}
  def apply! do
    case Application.get_env(:fathom, :lb_map_path) do
      nil ->
        Logger.debug("rebalancer: :lb_map_path unset — LB map not written (decision-plane only)")
        :ok

      path ->
        with_lock(fn -> promote(path, LbMap.current()) end)
    end
  end

  # Serialize apply! fleet-wide (finding #10). A SESSION advisory lock, so lock + inner
  # queries + unlock must share one connection — Repo.checkout pins it (a transaction-scoped
  # xact lock would hold a DB transaction open across the shell reload). Blocks a concurrent
  # apply! on another node until this one finishes, then that one re-renders the now-current
  # table.
  defp with_lock(fun) do
    Repo.checkout(fn ->
      Repo.query!("SELECT pg_advisory_lock($1)", [@lock_key])

      try do
        fun.()
      after
        Repo.query!("SELECT pg_advisory_unlock($1)", [@lock_key])
      end
    end)
  end

  # Write the candidate to a same-dir temp, config-test it, then atomically rename it over
  # the live file. Any failure keeps the last-good file and leaves no partial behind. On a
  # successful promotion the reload result (which may itself be an error) is surfaced. A
  # render identical to the live file is a no-op (skip write + reload) so a periodic
  # re-render doesn't HUP nginx on every tick.
  defp promote(path, content) do
    if content == read_existing(path) do
      :ok
    else
      write_test_promote(path, content)
    end
  end

  defp read_existing(path) do
    case File.read(path) do
      {:ok, existing} -> existing
      _ -> nil
    end
  end

  defp write_test_promote(path, content) do
    tmp = "#{path}.tmp.#{System.unique_integer([:positive])}"

    with :ok <- File.write(tmp, content),
         :ok <- config_test(tmp),
         :ok <- File.rename(tmp, path) do
      reload()
    else
      {:error, reason} ->
        File.rm(tmp)

        Logger.error(
          "rebalancer: LB map not promoted (#{inspect(reason)}); kept last-good #{path}"
        )

        {:error, reason}
    end
  end

  # Optional operator config test of the candidate before promotion. `{}` in the command is
  # replaced with the candidate path; it's also exported as LB_MAP_CANDIDATE. Unset ⇒ :ok.
  defp config_test(candidate) do
    case Application.get_env(:fathom, :lb_test_cmd) do
      nil ->
        :ok

      cmd ->
        full = String.replace(cmd, "{}", candidate)

        {out, code} =
          System.shell(full, stderr_to_stdout: true, env: [{"LB_MAP_CANDIDATE", candidate}])

        if code == 0,
          do: :ok,
          else: {:error, {:config_test_failed, code, String.trim(out)}}
    end
  rescue
    e -> {:error, {:config_test_raised, Exception.message(e)}}
  end

  # Unset ⇒ out-of-band reload (the sidecar HUPs on mtime); can't confirm, so :ok. A set
  # command that exits non-zero / raises surfaces {:error, ...} so the handoff won't drain
  # against a flip that may not be live (finding #11).
  defp reload do
    case Application.get_env(:fathom, :lb_reload_cmd) do
      nil ->
        :ok

      cmd ->
        {out, code} = System.shell(cmd, stderr_to_stdout: true)

        if code == 0 do
          :ok
        else
          Logger.warning("rebalancer: LB reload `#{cmd}` exited #{code}: #{String.trim(out)}")
          {:error, {:reload_failed, code}}
        end
    end
  rescue
    e ->
      Logger.warning("rebalancer: LB reload raised: #{Exception.message(e)}")
      {:error, {:reload_raised, Exception.message(e)}}
  end
end
