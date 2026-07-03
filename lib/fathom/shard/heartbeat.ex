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

  @doc "This node's heartbeat owner string (matches the shard lease `owner`)."
  def owner, do: to_string(node())

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

    state = %{
      owner: Keyword.get(opts, :owner, owner()),
      ttl_ms: ttl,
      # Renew every third of the TTL (a couple of misses don't lapse the lease).
      renew_ms: max(div(ttl, 3), 1),
      # A flush is only safe if the heartbeat won't expire during it: require at
      # least this much validity remaining. In steady state the heartbeat always has
      # > 2*ttl/3 left, so flushes never block; the margin only trips when a renewal
      # has been missed.
      margin_ms: max(div(ttl, 3), 1),
      expires_at_ms: 0,
      generation: 0,
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
    now = Storage.now_ms()

    reply =
      cond do
        # Not comfortably valid (expired or about to) — never write without confirmed liveness.
        now + state.margin_ms >= state.expires_at_ms -> :not_valid
        # A lapse happened since this shard was acquired — re-check ownership first.
        state.generation != acquire_gen -> :revalidate
        true -> :ok
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
    now = Storage.now_ms()

    # Edge-detect a lapse: we had a valid heartbeat and it expired before we renewed.
    state =
      if not state.lapsed and state.expires_at_ms > 0 and now > state.expires_at_ms,
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
          %{state | expires_at_ms: exp, lapsed: false}

        {:error, reason} ->
          Logger.warning("heartbeat renew failed for #{state.owner}: #{inspect(reason)}")
          state
      end

    schedule_renew(state)
  end

  defp mark_lapse(state) do
    gen = state.generation + 1

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
