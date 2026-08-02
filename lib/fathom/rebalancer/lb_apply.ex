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

  ## Serialized + idempotent (finding #10, hardened by review 2026-07-09 #2)

  The **file production** (render + config-test + atomic rename) runs under a fleet-wide
  Postgres advisory lock, so two `apply!`s on different nodes can't interleave a stale read
  with a later write and drop a pin from the *file* though it's in the DB. Each render reads
  the full override table, so serialized last-writer-wins is correct. A re-render that is
  byte-identical to the on-disk file is a **no-op** (no write, no reload) — which is what lets
  the leader `RebalanceJob` re-render every tick cheaply to self-heal drift (this), dead-node
  pins (#1b), and failed reloads (#11) without HUPing nginx every minute.

  Two hardening properties keep a bad LB from freezing the fleet:

  - The **shell reload runs OUTSIDE the lock** (only the file production is inside), and every
    shell call — the config-test and the reload — is **hard-timeout-bounded** (killed at the
    deadline). A hung `nginx -t`/`-s reload` can no longer hold the pooled connection + the
    fleet lock indefinitely and freeze every node's LB updates.
  - The lock is `pg_try_advisory_lock` (non-blocking): a waiter degrades to skipping this tick
    (`{:error, :lock_contended}`) rather than blocking a pooled connection behind a slow
    holder. The DB override is already committed, so the leader's periodic re-render applies
    it within a tick and a handoff's own `apply!` retries.

  Config: `:lb_test_timeout_ms` / `:lb_reload_timeout_ms` (both default 10_000).
  """
  @spec apply!() :: :ok | {:error, term()}
  def apply! do
    case Application.get_env(:fathom, :lb_map_path) do
      nil ->
        Logger.debug("rebalancer: :lb_map_path unset — LB map not written (decision-plane only)")
        emit(:not_written)
        :ok

      path ->
        promote_then_reload(path)
    end
  end

  # Produce the authoritative file UNDER the advisory lock (render + config-test + atomic
  # rename — the part that must serialize fleet-wide, #10), then reload OUTSIDE the lock so a
  # slow/hung reload can't hold the pooled connection + the fleet lock indefinitely and freeze
  # every node's LB updates (review 2026-07-09 #2). The reload result still flows into the
  # return so the handoff knows whether the flip went live (#11).
  defp promote_then_reload(path) do
    case with_lock(fn -> produce(path) end) do
      {:changed, :ok} ->
        case reload() do
          :ok ->
            mark_applied(path)
            emit(:applied)
            :ok

          {:error, _} = err ->
            emit(reload_outcome(err))
            err
        end

      # The file on disk already matches the render. That is NOT the same as the RUNNING LB
      # having it (expert review 2026-08-01 #23), and the two diverge exactly when a reload
      # failed: attempt 1 promotes the file and `reload/0` errors — `HandoffJob` correctly
      # skips the drain — then attempt 2 renders byte-identically, short-circuits to `:ok`,
      # and the handoff believes the flip is live and DRAINS THE SOURCE while nginx still
      # routes every request for that shard to it. The same short-circuit made the documented
      # per-tick self-heal a no-op, so a failed reload was permanent.
      #
      # The marker is written only after `reload/0` returns `:ok`, so "file matches but marker
      # does not" means "promoted, never loaded" — reload again.
      {:unchanged, :ok} ->
        if applied?(path) do
          emit(:noop)
          :ok
        else
          case reload() do
            :ok ->
              mark_applied(path)
              emit(:applied)
              :ok

            {:error, _} = err ->
              emit(reload_outcome(err))
              err
          end
        end

      {:error, reason} ->
        emit(promote_error_outcome(reason))
        {:error, reason}

      :contended ->
        # Another node/handoff holds the lock right now — skip this tick rather than block a
        # pooled connection on it. The DB override is already committed, so the leader's
        # periodic re-render applies it within a tick and a handoff's own apply! retries;
        # return an error so a handoff doesn't drain against a flip that isn't live yet (#11).
        emit(:lock_contended)
        {:error, :lock_contended}
    end
  end

  # The LB-apply health signal (rebalancer telemetry): a rising :reload_failed /
  # :reload_timeout / :config_test_failed is the fleet-routing-at-risk alert (#3/#11); :noop
  # dominates (the per-tick re-render) and confirms the loop is live; :applied is a real flip.
  defp emit(outcome) do
    :telemetry.execute([:fathom, :rebalancer, :lb_apply], %{count: 1}, %{outcome: outcome})
  end

  # Non-blocking advisory lock (finding #10, hardened per review 2026-07-09 #2): pg_TRY, so a
  # waiter degrades to :contended (skip this tick) instead of blocking a pooled connection —
  # a hung holder would otherwise stack blocked waiters against the pool AND freeze fleet-wide
  # LB updates. A SESSION lock, so lock + inner queries + unlock share one Repo.checkout'd
  # connection. Only the file production runs inside; the shell reload is outside (see apply!).
  defp with_lock(fun) do
    Repo.checkout(fn ->
      case Repo.query!("SELECT pg_try_advisory_lock($1)", [@lock_key]) do
        %{rows: [[true]]} ->
          try do
            fun.()
          after
            Repo.query!("SELECT pg_advisory_unlock($1)", [@lock_key])
          end

        _ ->
          :contended
      end
    end)
  end

  # The advisory-locked critical section: render + no-op check + write same-dir temp +
  # config-test + atomic rename. NO reload here. Returns `{:changed, :ok}` (promoted a new
  # file), `{:unchanged, :ok}` (byte-identical — skip), or `{:error, reason}` (kept last-good).
  defp produce(path) do
    content = LbMap.current()

    if content == read_existing(path) do
      {:unchanged, :ok}
    else
      tmp = "#{path}.tmp.#{System.unique_integer([:positive])}"

      with :ok <- File.write(tmp, content),
           :ok <- config_test(tmp),
           :ok <- File.rename(tmp, path) do
        {:changed, :ok}
      else
        {:error, reason} ->
          File.rm(tmp)

          Logger.error(
            "rebalancer: LB map not promoted (#{inspect(reason)}); kept last-good #{path}"
          )

          {:error, reason}
      end
    end
  end

  # The last content the RUNNING LB was successfully reloaded with (expert review #23), as a
  # hash beside the map file. Written only after `reload/0` succeeds.
  defp applied_marker(path), do: path <> ".applied"

  defp mark_applied(path) do
    case File.read(path) do
      {:ok, content} ->
        File.write(applied_marker(path), content_hash(content))

      _ ->
        :ok
    end
  end

  # Does the running LB already have the file that is on disk? Unreadable/missing marker ⇒
  # assume not, which costs one extra reload — the safe direction, since the alternative is a
  # handoff draining a source the LB still routes to.
  defp applied?(path) do
    with {:ok, content} <- File.read(path),
         {:ok, marker} <- File.read(applied_marker(path)) do
      marker == content_hash(content)
    else
      _ -> false
    end
  end

  defp content_hash(content), do: :crypto.hash(:sha256, content) |> Base.encode16(case: :lower)

  defp read_existing(path) do
    case File.read(path) do
      {:ok, existing} -> existing
      _ -> nil
    end
  end

  # The map was promoted; classify by whether the running LB picked it up.
  defp reload_outcome({:error, {:reload_failed, _}}), do: :reload_failed
  defp reload_outcome({:error, {:reload_timeout, _}}), do: :reload_timeout
  defp reload_outcome({:error, {:reload_raised, _}}), do: :reload_raised

  # The map was NOT promoted (last-good kept); classify the pre-promotion failure.
  defp promote_error_outcome({:config_test_failed, _, _}), do: :config_test_failed
  defp promote_error_outcome({:config_test_raised, _}), do: :config_test_failed
  defp promote_error_outcome({:config_test_timeout, _}), do: :config_test_timeout
  defp promote_error_outcome(_), do: :write_failed

  # Optional operator config test of the candidate before promotion. `{}` in the command is
  # replaced with the candidate path; it's also exported as LB_MAP_CANDIDATE. Unset ⇒ :ok.
  # Runs inside the advisory lock, so it is HARD-timeout-bounded (review 2026-07-09 #2) — a
  # hung `nginx -t` must not hold the fleet lock.
  defp config_test(candidate) do
    case Application.get_env(:fathom, :lb_test_cmd) do
      nil ->
        :ok

      cmd ->
        full = String.replace(cmd, "{}", candidate)

        case run_shell(full, config_test_timeout_ms(), [{"LB_MAP_CANDIDATE", candidate}]) do
          {:ok, {_out, 0}} -> :ok
          {:ok, {out, code}} -> {:error, {:config_test_failed, code, String.trim(out)}}
          :timeout -> {:error, {:config_test_timeout, config_test_timeout_ms()}}
          {:error, msg} -> {:error, {:config_test_raised, msg}}
        end
    end
  end

  # Unset ⇒ out-of-band reload (the sidecar HUPs on mtime); can't confirm, so :ok. A set
  # command that exits non-zero / times out / raises surfaces {:error, ...} so the handoff
  # won't drain against a flip that may not be live (finding #11). Runs OUTSIDE the lock, and
  # hard-timeout-bounded so a hung reload can't hang the caller.
  defp reload do
    case Application.get_env(:fathom, :lb_reload_cmd) do
      nil ->
        :ok

      cmd ->
        case run_shell(cmd, reload_timeout_ms(), []) do
          {:ok, {_out, 0}} ->
            :ok

          {:ok, {out, code}} ->
            Logger.warning("rebalancer: LB reload `#{cmd}` exited #{code}: #{String.trim(out)}")
            {:error, {:reload_failed, code}}

          :timeout ->
            Logger.warning("rebalancer: LB reload `#{cmd}` timed out (#{reload_timeout_ms()}ms)")
            {:error, {:reload_timeout, reload_timeout_ms()}}

          {:error, msg} ->
            Logger.warning("rebalancer: LB reload raised: #{msg}")
            {:error, {:reload_raised, msg}}
        end
    end
  end

  # Run a shell command with a hard deadline: on timeout the task is killed so the command
  # can't hold the caller (and, for the config-test, the advisory lock) indefinitely (review
  # 2026-07-09 #2). Returns {:ok, {out, code}} | :timeout | {:error, message}. (A killed BEAM
  # task closes the port; a truly-wedged OS process may linger — an operator concern, but it
  # no longer holds any BEAM/DB resource.)
  defp run_shell(cmd, timeout_ms, env) do
    task = Task.async(fn -> System.shell(cmd, stderr_to_stdout: true, env: env) end)

    case Task.yield(task, timeout_ms) || Task.shutdown(task, :brutal_kill) do
      {:ok, {out, code}} -> {:ok, {out, code}}
      {:exit, reason} -> {:error, inspect(reason)}
      nil -> :timeout
    end
  end

  defp config_test_timeout_ms, do: Application.get_env(:fathom, :lb_test_timeout_ms, 10_000)
  defp reload_timeout_ms, do: Application.get_env(:fathom, :lb_reload_timeout_ms, 10_000)
end
