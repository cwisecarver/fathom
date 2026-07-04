defmodule Fathom.Shard.Heartbeat do
  @moduledoc """
  The node's single liveness heartbeat — the F1 fix that replaces per-shard lease
  renewal (the ~100k PUT/s/node storm at a million shards) with **one** PUT per node.

  A node is "alive" iff its `heartbeat/<owner>` object is fresh (see
  `Fathom.Shard.Storage`). This GenServer renews that one object every `ttl/3` for
  the life of the node; `Fathom.Shard` coordinators no longer renew their shards.

  ## Fence for flushes

  A coordinator may only write a shard's data back to storage while it still owns
  the shard. A steal requires the owner's heartbeat to be *stale*, so a node whose
  heartbeat is continuously valid is never superseded. The coordinator records
  `generation/0` when it acquires a shard and calls `valid_for_write?/1` before each
  flush:

    * `:ok` — heartbeat valid with margin AND no lapse since acquire ⇒ we still own
      it, and it won't expire mid-write ⇒ the write is safe with no per-shard I/O.
    * `:revalidate` — valid, but the heartbeat lapsed since acquire ⇒ a steal *may*
      have happened during the gap ⇒ re-check the shard lock (`Storage.check_lease/2`)
      before writing, and self-fence if it was stolen.
    * `:not_valid` — heartbeat not comfortably valid ⇒ do NOT write (retry later);
      writing without confirmed liveness could race a steal.

  A "lapse" is detected when a renewal tick finds the heartbeat already expired (a
  GC pause / partition let it lapse, during which another node could have stolen our
  shards). It bumps `generation` once per episode and broadcasts so coordinators can
  revalidate proactively instead of waiting for their next flush.
  """
  use GenServer

  require Logger

  alias Fathom.Shard.Storage

  @default_ttl_ms 30_000
  @topic "fathom:heartbeat"

  @doc false
  def start_link(opts), do: GenServer.start_link(__MODULE__, opts, name: __MODULE__)

  @doc """
  This node's heartbeat owner string (matches the shard lease `owner`):
  `node()#<boot nonce>` (expert review #6). The incarnation nonce makes lease
  ownership boot-scoped — a lock held by a PREVIOUS incarnation of this node name
  is a foreign owner, so it goes through the liveness check and the epoch-bumping
  steal path instead of the silent same-owner/same-epoch reclaim, and a lingering
  same-name zombie (a replaced pod still S3-reachable) renews its OWN per-
  incarnation heartbeat object rather than sharing ours.
  """
  def owner, do: "#{node()}##{incarnation()}"

  @doc false
  # The boot nonce, set by Fathom.Application.start before the tree (race-free).
  # Lazy fallback for standalone use (iex without the app); the put is last-wins,
  # acceptable outside the supervised boot path.
  def incarnation do
    case :persistent_term.get({Fathom, :incarnation}, nil) do
      nil ->
        nonce = Base.encode16(:crypto.strong_rand_bytes(4), case: :lower)
        :persistent_term.put({Fathom, :incarnation}, nonce)
        nonce

      nonce ->
        nonce
    end
  end

  @doc "PubSub topic broadcast on a heartbeat lapse (`{:heartbeat_lapsed, generation}`)."
  def topic, do: @topic

  @doc "Whether the heartbeat process is running (coordinators fall back to the legacy fence if not)."
  def running?, do: Process.whereis(__MODULE__) != nil

  @doc "Current lapse generation; a coordinator records this when it acquires a shard."
  @spec generation() :: non_neg_integer()
  def generation, do: GenServer.call(__MODULE__, :generation)

  @doc "Fence check for a flush given the generation recorded at acquire. See the moduledoc."
  @spec valid_for_write?(non_neg_integer()) :: :ok | :revalidate | :not_valid
  def valid_for_write?(acquire_gen),
    do: GenServer.call(__MODULE__, {:valid_for_write, acquire_gen})

  @impl true
  def init(opts) do
    # Trap exits so a supervisor :shutdown runs terminate/2 (which clears the
    # heartbeat for a fast planned failover).
    Process.flag(:trap_exit, true)

    ttl =
      Keyword.get(
        opts,
        :ttl_ms,
        Application.get_env(:fathom, :shard_lease_ttl_ms, @default_ttl_ms)
      )

    owner = Keyword.get(opts, :owner, owner())

    # Same-machine restart fast path (expert review #6): our previous incarnation's
    # heartbeat object would otherwise stay fresh for up to a TTL, holding this
    # node's own old locks {:held} against us. The previous nonce is persisted
    # locally (a replaced pod / other machine doesn't share the file, so a live
    # same-name zombie elsewhere is never cleared — its liveness protects it).
    if owner == owner(), do: clear_previous_incarnation()

    # The lapse generation must survive this process: it is the coordinators' proof
    # that "no lapse happened since acquire ⇒ no steal was possible", and a restart
    # gap is itself a window in which the heartbeat may have lapsed and a steal
    # occurred. Resetting to 0 on restart forged that proof for every coordinator
    # acquired at generation 0 of the previous incarnation. Persist it per owner and
    # treat every re-boot of an already-seen owner as a lapse episode — all
    # pre-restart acquire_gens then mismatch and force one cheap revalidation.
    generation = :persistent_term.get(gen_key(owner), -1) + 1
    :persistent_term.put(gen_key(owner), generation)

    if generation > 0 do
      Logger.warning(
        "heartbeat restarted for #{owner} (generation #{generation}); coordinators will revalidate"
      )

      # A restart IS a lapse episode (the bump above says so), but only mark_lapse
      # broadcast — so the proactive revalidation coordinators subscribe to (expert
      # review #34) never fired for the restart-lapse case, and they sat on stale
      # acquire_gens until their next flush (unboundedly in idle-only mode) while the
      # restart gap is a real steal window. Broadcast so they revalidate NOW (expert
      # review round-2 #15).
      broadcast_lapse(generation)
    end

    state = %{
      owner: owner,
      ttl_ms: ttl,
      # Renew every third of the TTL (a couple of misses don't lapse the lease).
      renew_ms: max(div(ttl, 3), 1),
      # A flush is only safe if the heartbeat won't expire during it: require at
      # least this much validity remaining. In steady state the heartbeat always has
      # > 2*ttl/3 left, so flushes never block; the margin only trips when a renewal
      # has been missed.
      margin_ms: max(div(ttl, 3), 1),
      # Wall clock — what peers read in the stored heartbeat object.
      expires_at_ms: 0,
      # Monotonic — drives the LOCAL validity/lapse decisions (expert review #21):
      # "am I still valid?" and "did I lapse?" are pure elapsed-time questions that
      # need no clock agreement, and a backward wall-clock step (NTP correction, VM
      # live-migration) could otherwise inflate perceived validity past a real lapse —
      # never edge-detecting it, never bumping the generation, and letting flushes
      # skip revalidation against a peer that legitimately stole our shards.
      mono_deadline_ms: nil,
      generation: generation,
      lapsed: false,
      timer: nil
    }

    {:ok, state, {:continue, :renew}}
  end

  @impl true
  def handle_continue(:renew, state), do: {:noreply, do_renew(state)}

  @impl true
  def handle_info(:renew, state), do: {:noreply, do_renew(state)}

  @impl true
  def handle_call(:generation, _from, state), do: {:reply, state.generation, state}

  def handle_call({:valid_for_write, acquire_gen}, _from, state) do
    now = System.monotonic_time(:millisecond)

    reply =
      cond do
        # Not comfortably valid (no confirmed renewal yet, expired, or about to) —
        # never write without confirmed liveness. Monotonic, so a wall-clock step
        # can't inflate perceived validity (expert review #21).
        is_nil(state.mono_deadline_ms) or now + state.margin_ms >= state.mono_deadline_ms ->
          :not_valid

        # A lapse happened since this shard was acquired — re-check ownership first.
        state.generation != acquire_gen ->
          :revalidate

        true ->
          :ok
      end

    {:reply, reply, state}
  end

  @impl true
  def terminate(reason, state) when reason in [:normal, :shutdown] do
    # Clean shutdown: drop our heartbeat so this node's shards are immediately
    # stealable (fast planned-failover) rather than waiting out the TTL.
    Storage.clear_heartbeat(state.owner)
    :ok
  end

  def terminate({:shutdown, _}, state) do
    Storage.clear_heartbeat(state.owner)
    :ok
  end

  # A crash the supervisor is about to reverse must NOT delete the liveness object:
  # with per-shard locks never renewed in heartbeat mode, an absent heartbeat makes
  # every long-lived shard on this (perfectly healthy, still-serving) node look dead
  # to owner_live?'s :not_found fallback and instantly stealable during the restart
  # gap. Leave the object in place — the restarted process re-renews it within
  # renew_ms, and until then the stale-but-present object still fences stealers.
  def terminate(_reason, _state), do: :ok

  defp do_renew(state) do
    # Captured BEFORE the storage PUT: the local deadline must not outlive the stored
    # object's expiry, and the PUT takes time — measuring after it would skew the
    # deadline later (the unsafe direction).
    now = System.monotonic_time(:millisecond)

    # Edge-detect a lapse: we had a valid heartbeat and it expired before we renewed.
    # Monotonic elapsed time, immune to wall-clock steps (expert review #21).
    state =
      if not state.lapsed and not is_nil(state.mono_deadline_ms) and
           now > state.mono_deadline_ms,
         do: mark_lapse(state),
         else: state

    state =
      case Storage.renew_heartbeat(state.owner, state.ttl_ms) do
        {:ok, %{expires_at_ms: exp}} ->
          :telemetry.execute(
            [:fathom, :shard, :heartbeat, :renewed],
            %{count: 1},
            %{owner: state.owner}
          )

          # Recovered — fresh expiry, lapse episode over.
          %{state | expires_at_ms: exp, mono_deadline_ms: now + state.ttl_ms, lapsed: false}

        {:error, reason} ->
          Logger.warning("heartbeat renew failed for #{state.owner}: #{inspect(reason)}")
          state
      end

    schedule_renew(state)
  end

  # The persisted-generation key for `owner` (see init/1 — restart continuity).
  defp gen_key(owner), do: {__MODULE__, :generation, owner}

  # See init/1. The nonce file lives next to the shard files (node-local disk).
  defp clear_previous_incarnation do
    path = incarnation_file()
    current = incarnation()

    case File.read(path) do
      {:ok, prev} when prev != "" and prev != current ->
        _ = Storage.clear_heartbeat("#{node()}##{prev}")
        :ok

      _ ->
        :ok
    end

    File.mkdir_p!(Path.dirname(path))
    File.write(path, current)
    :ok
  rescue
    e ->
      Logger.warning("previous-incarnation heartbeat clear failed: #{Exception.message(e)}")
      :ok
  end

  defp incarnation_file do
    Path.join(Path.dirname(Fathom.Shard.db_path("x")), ".incarnation")
  end

  defp mark_lapse(state) do
    gen = state.generation + 1
    :persistent_term.put(gen_key(state.owner), gen)

    Logger.warning(
      "heartbeat lapsed for #{state.owner} (generation #{gen}); coordinators will revalidate"
    )

    :telemetry.execute([:fathom, :shard, :heartbeat, :lapsed], %{generation: gen}, %{
      owner: state.owner
    })

    broadcast_lapse(gen)
    %{state | generation: gen, lapsed: true}
  end

  # Best-effort: the lapse broadcast is observability/proactive-revalidation only, so
  # a missing PubSub (e.g. the scale/bench harness runs without it) must not crash
  # the heartbeat.
  defp broadcast_lapse(gen) do
    Phoenix.PubSub.broadcast(Fathom.PubSub, @topic, {:heartbeat_lapsed, gen})
  rescue
    _ -> :ok
  catch
    :exit, _ -> :ok
  end

  defp schedule_renew(state) do
    if state.timer, do: Process.cancel_timer(state.timer)
    %{state | timer: Process.send_after(self(), :renew, state.renew_ms)}
  end
end
