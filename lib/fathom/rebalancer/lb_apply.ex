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

  @doc """
  Renders + promotes the map (atomic) and reloads the LB. Returns `:ok`, or `{:error, reason}`
  if the candidate failed its config test / couldn't be written (the last-good file is kept).
  Reload is best-effort (finding #11 surfaces reload failure separately).
  """
  @spec apply!() :: :ok | {:error, term()}
  def apply! do
    case Application.get_env(:fathom, :lb_map_path) do
      nil ->
        Logger.debug("rebalancer: :lb_map_path unset — LB map not written (decision-plane only)")
        :ok

      path ->
        promote(path, LbMap.current())
    end
  end

  # Write the candidate to a same-dir temp, config-test it, then atomically rename it over
  # the live file. Any failure keeps the last-good file and leaves no partial behind.
  defp promote(path, content) do
    tmp = "#{path}.tmp.#{System.unique_integer([:positive])}"

    with :ok <- File.write(tmp, content),
         :ok <- config_test(tmp),
         :ok <- File.rename(tmp, path) do
      reload()
      :ok
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

  defp reload do
    case Application.get_env(:fathom, :lb_reload_cmd) do
      nil ->
        :ok

      cmd ->
        {out, code} = System.shell(cmd, stderr_to_stdout: true)

        if code != 0,
          do: Logger.warning("rebalancer: LB reload `#{cmd}` exited #{code}: #{String.trim(out)}")

        :ok
    end
  rescue
    e -> Logger.warning("rebalancer: LB reload raised: #{Exception.message(e)}")
  end
end
