defmodule Fathom.Shards do
  @moduledoc """
  Find-or-start router for shard coordinators. Resolves a `shard_id` to its
  `Fathom.Shard` process — starting it on demand under the shard
  `DynamicSupervisor` — and hands back the path to the shard's database file.
  Callers open their own `Fathom.Shard.Connection` against that path (one per
  Hrana stream), which is what keeps per-stream transactions isolated.
  """
  @registry Fathom.ShardRegistry
  @supervisor Fathom.ShardSupervisor

  @default_drain_ms 5_000

  @doc """
  Ensures `shard_id`'s coordinator is running and checks out the shard for the
  caller, returning `{:ok, coordinator_pid, ref, path}` (or `{:error, reason}` for
  an invalid id / start failure). The caller opens its own connection at `path`
  and passes `ref` back via `Fathom.Shard.checkin/2` when it closes.
  """
  def checkout(shard_id) do
    # Normalize (validate + downcase) at this trust boundary so the whole checkout — telemetry
    # tag, ensure, record_use, ShardLoad — and every downstream key uses the one canonical id
    # (finding #19). Direct callers (migration jobs, admin, tests) are covered here;
    # request-path callers are covered again in Fathom.ShardExecutor.open/1 (cast is idempotent).
    case Fathom.ShardId.cast(shard_id) do
      {:ok, id} ->
        # A `:telemetry.span` so each checkout is an OpenTelemetry trace span (cold-open cost shows
        # up here on a cold checkout) and a duration metric, tagged by outcome. See Fathom.Telemetry.
        :telemetry.span([:fathom, :shards, :checkout], %{shard_id: id}, fn ->
          result = do_checkout(id, 3)
          {result, %{shard_id: id, outcome: checkout_outcome(result)}}
        end)

      :error ->
        {:error, :invalid_shard_id}
    end
  end

  defp do_checkout(shard_id, attempts) do
    with :ok <- maybe_lazy_migrate(shard_id),
         {:ok, pid} <- ensure(shard_id),
         {:ok, ref, path} <- Fathom.Shard.checkout(pid) do
      record_use(shard_id)
      # Per-shard load: the checkout (traffic) signal for the rebalancer. Lock-free
      # ETS bump, gated + off by default (see Fathom.ShardLoad).
      Fathom.ShardLoad.record_checkout(shard_id)
      {:ok, pid, ref, path}
    else
      # Race: `ensure` resolved a coordinator that lost a race with its own lifecycle, so a
      # 1 ms re-resolve to a fresh coordinator fixes it (see retry_checkout?/1). Bounded so a
      # genuinely unavailable shard still surfaces the error rather than spinning. Only the
      # (rare) race path sleeps; the happy path is untouched.
      {:error, reason} when attempts > 1 ->
        if retry_checkout?(reason) do
          Process.sleep(1)
          do_checkout(shard_id, attempts - 1)
        else
          {:error, reason}
        end

      other ->
        other
    end
  end

  # Two checkout errors are transient lifecycle races, not real failures — both clear on a
  # re-resolve to a fresh coordinator:
  #   * `:unavailable` (`:noproc`) — the coordinator had already stopped and its Registry
  #     entry lingered in the window before the Registry handled the `:DOWN`.
  #   * `:normal` — the checkout call was queued behind an `:idle_timeout`/drain stop, so the
  #     coordinator processed the stop first and the pending `GenServer.call` exited `:normal`.
  #     Idle stops are routine at scale, so without this a steady trickle of checkouts a 1 ms
  #     retry would have fixed surfaced as spurious `{:error, :normal}` to the client.
  @doc false
  def retry_checkout?(reason), do: reason in [:unavailable, :normal]

  defp checkout_outcome({:ok, _, _, _}), do: :ok
  defp checkout_outcome({:error, {:shard_held, _}}), do: :held
  defp checkout_outcome({:error, :unavailable}), do: :unavailable
  defp checkout_outcome({:error, :node_at_capacity}), do: :at_capacity
  defp checkout_outcome({:error, :novel_shard_rate_limited}), do: :novel_rate_limited
  defp checkout_outcome({:error, _}), do: :error

  # Migrate-then-serve: once a fleet version is released, a shard behind HEAD can't
  # serve the new app version's traffic, so migrate it inline before checking out.
  # The deliberate hot-path/Postgres exception, off by default — enabled with the
  # control plane (a directory-cache would remove the per-checkout reads).
  defp maybe_lazy_migrate(shard_id) do
    if Application.get_env(:fathom, :lazy_migrate, false) do
      lazy_migrate(shard_id)
    else
      :ok
    end
  end

  defp lazy_migrate(shard_id) do
    # HEAD from the TTL cache (persistent_term), not a per-checkout Postgres
    # `max(version)` — see Fathom.Migrator.HeadCache.
    head = Fathom.Migrator.HeadCache.get()

    if head > 0 and behind?(shard_id, head) do
      case Fathom.Migrator.ShardMigration.run(shard_id, head) do
        :ok -> :ok
        {:ok, _} -> :ok
        # Nothing to migrate yet (a brand-new shard is born at HEAD via
        # fork-from-template, not migrated).
        {:error, :no_live_object} -> :ok
        # Another worker holds it / it's busy — the caller retries.
        {:retry, reason} -> {:error, {:shard_migrating, reason}}
        {:error, _} = error -> error
      end
    else
      :ok
    end
  end

  defp behind?(shard_id, head) do
    case Fathom.Directory.get(shard_id) do
      {:ok, %{schema_version: v}} -> v < head
      # Not yet in the directory (brand-new) — fork-from-template handles its birth.
      :error -> false
    end
  end

  # Record the shard's use in the Postgres directory (control plane). Off the hot
  # path: a lock-free ETS buffer write that `Fathom.Directory.Recorder` coalesces
  # and batch-flushes. Gated by config; the data path never blocks on or fails
  # because of the directory, so a Postgres outage just means a missed touch.
  defp record_use(shard_id) do
    if Application.get_env(:fathom, :directory_touch, true) do
      Fathom.Directory.Recorder.record(shard_id)
    end

    :ok
  end

  @doc """
  Stands a shard's coordinator down so the migrator can take over: refuse new
  checkouts, let in-flight connections drain (up to `drain_timeout` ms), flush the
  latest data to storage, drop the local copy, release the lease, and stop.

  Returns `:ok` once the coordinator has fully stopped (data is durable in storage
  and the lease is free), or `:ok` if it wasn't running (already cold). Returns
  `{:error, :busy}` if connections didn't drain in time — the coordinator keeps
  serving and the caller should retry later. Blocks until the coordinator exits.
  """
  def drain(shard_id, drain_timeout \\ @default_drain_ms) do
    case Registry.lookup(@registry, shard_id) do
      [] ->
        :ok

      [{pid, _}] ->
        ref = Process.monitor(pid)
        Fathom.Shard.request_drain(pid, drain_timeout, self())

        receive do
          {:DOWN, ^ref, :process, ^pid, reason} ->
            drain_down_result(reason)

          {:drain_aborted, ^pid} ->
            Process.demonitor(ref, [:flush])
            {:error, :busy}
        after
          # Safety net only: the coordinator normally replies via DOWN or
          # :drain_aborted well before this.
          drain_timeout + 30_000 ->
            Process.demonitor(ref, [:flush])
            {:error, :busy}
        end
    end
  end

  # The drain goal — coordinator stopped, no longer holding the file/lease — is met on a
  # clean `:normal` stop and equally on `:noproc`: the coordinator had already died in the
  # window between the Registry lookup and our monitor, so it is already cold. Both are `:ok`;
  # only a genuinely abnormal exit is a drain failure.
  @doc false
  def drain_down_result(reason) when reason in [:normal, :noproc], do: :ok
  def drain_down_result(reason), do: {:error, {:drain_failed, reason}}

  @doc "Returns `{:ok, pid}` for `shard_id`, starting the coordinator if needed."
  def ensure(shard_id) when is_binary(shard_id) do
    if Fathom.ShardId.valid?(shard_id) do
      case Registry.lookup(@registry, shard_id) do
        [{pid, _}] -> {:ok, pid}
        [] -> start_if_capacity(shard_id)
      end
    else
      {:error, :invalid_shard_id}
    end
  end

  # Per-node admission control: only NEW opens are gated — an already-running shard
  # (the branch above) is always checkoutable. At the cap we refuse cleanly with
  # `{:error, :node_at_capacity}` so the LB/client backs off, rather than letting
  # DynamicSupervisor spawn past the fd cliff (emfile) and degrade the whole node.
  # Off by default (`:max_open_shards` == :infinity); the operator sets it from the
  # measured fd/RSS density budget (`mix fathom.scale --ramp`).
  defp start_if_capacity(shard_id) do
    cond do
      at_capacity?() ->
        :telemetry.execute([:fathom, :shards, :at_capacity], %{count: 1}, %{shard_id: shard_id})
        {:error, :node_at_capacity}

      # The churn half of finding #14: the cap above bounds how many shards this node holds
      # open; this bounds how FAST unseen ids can mint new ones (coordinator + fds + file +
      # S3 lock PUT + Postgres row per novel id). Refused before any of that work runs.
      novel_refused?(shard_id) ->
        {:error, :novel_shard_rate_limited}

      true ->
        start(shard_id)
    end
  end

  # Should this open be refused as an over-rate NOVEL creation? Only consulted on the
  # registry-miss path, and only does work when `:novel_shard_rate` is configured (nil =
  # off, the default — the cold path pays one get_env). "Novel" = nothing knows the shard:
  # no local file (a present file is an authoritative un-flushed copy) and no directory row.
  defp novel_refused?(shard_id) do
    case Application.get_env(:fathom, :novel_shard_rate) do
      nil ->
        false

      _rate ->
        not File.exists?(Fathom.Shard.db_path(shard_id)) and
          not known_to_directory?(shard_id) and
          limiter_refused?(shard_id)
    end
  end

  # The limiter call needs the same exit protection the directory read below has
  # (expert review #28): the limiter's own backpressure model is mailbox saturation,
  # which is exactly when GenServer.call starts exiting :timeout — and a limiter
  # crash/restart window exits :noproc. Un-caught, those exits crashed the whole open
  # path (Hrana stream 500s) under precisely the novel-shard spray the limiter exists
  # to absorb, with each saturated caller pinning its connection 5 s. Fail CLOSED
  # (refused) on an exit: under a spray, refusing is the limiter doing its job; a
  # crashed limiter recovering for a few ms refusing a genuinely novel mint is the
  # cheap direction (existing shards never reach this call).
  defp limiter_refused?(shard_id) do
    match?({:error, _}, Fathom.Shards.NovelLimiter.allow(shard_id))
  catch
    :exit, _ -> true
  end

  # The directory read fails OPEN (treat as known): the data path never blocks on or fails
  # because of Postgres (the Recorder principle) — an outage disables the limiter, it never
  # refuses real traffic. An existing-but-never-recorded shard costs one token; the bucket
  # absorbs it.
  defp known_to_directory?(shard_id) do
    match?({:ok, _}, Fathom.Directory.get(shard_id))
  rescue
    _ -> true
  catch
    :exit, _ -> true
  end

  # Soft cap: `Registry.count` is O(1) and a couple of concurrent opens may overshoot
  # by the concurrency — fine, since the cap sits below the hard fd limit with headroom.
  defp at_capacity? do
    case max_open_shards() do
      :infinity -> false
      cap when is_integer(cap) -> Registry.count(@registry) >= cap
    end
  end

  defp max_open_shards, do: Application.get_env(:fathom, :max_open_shards, :infinity)

  defp start(shard_id) do
    case DynamicSupervisor.start_child(@supervisor, {Fathom.Shard, shard_id}) do
      {:ok, pid} -> {:ok, pid}
      # The Registry `:via` name wins the race when two callers start at once.
      {:error, {:already_started, pid}} -> {:ok, pid}
      {:error, _} = error -> error
    end
  end
end
