defmodule Fathom.Shard.Replication.Membership do
  @moduledoc """
  Who this node ships to, and the guarded swap that changes it — A2 Layer 3.
  See `docs/a2-quorum-replication.md`.

  Membership used to be `:replication_followers`, a list every node's operator maintained by hand
  and kept in sync with every other node's. `:replication_membership` picks the source:

    * `:static` (default) — the configured list, exactly as before.
    * `:roster` — endpoints nodes publish to `rebalancer_nodes` (Layer 2), refreshed on a timer.

  Both go through the same swap. The source is a supplier; the safety is here.

  ## The safety property

  `Session.ship_planned/4` reads `Fleet.shippers/0` on **every commit** and derives `n` from its
  length. `Quorum.new/2` raises when `q >= n` — and that raise happens inside a tenant's write.
  Today `Fleet.validate_quorum!/0` checks `q < n` exactly once, at boot, which is sound only
  because the set never changes afterwards. Making it changeable removes that guarantee, so it has
  to be re-established on every change:

    * **A set smaller than `q + 1` is REFUSED**, and the previous set stays live. `f1827e1` is the
      precedent — a shrinking `n` was how `q >= n` first started raising mid-commit, and that came
      from a plan counting bug rather than from anything as blunt as membership.
    * **An unreadable roster keeps the previous set.** Postgres is not on the write path and must
      not become so transitively; a DB blip that emptied membership would break every write on the
      node.
    * **The roster falls back to the configured list** when it cannot supply `q + 1` — a fresh
      fleet where peers have not beaten yet, or a rolling upgrade where they publish no address.
      So `REPLICATION_FOLLOWERS` keeps working as the floor rather than becoming dead config.

  Liveness still never filters the push set. The roster's staleness window is a **candidacy**
  filter applied when membership is recomputed on a timer — not a per-commit signal. A follower
  that is merely *down* stays in the set and costs nothing: `Shipper` refuses it without a socket
  write and `Quorum` reports `:impossible` as soon as too few remain. See `Fleet.health/0`.

  ## Ordering, which is the other way this can go wrong

  A swap starts the new shippers, THEN publishes, THEN stops the departed ones. Publishing first
  would let a commit read a name whose process does not exist yet; stopping first would let one
  read a name that is already dying. Both are `Shipper` calls into a dead process, on the commit
  path, for a follower that was supposed to be healthy.

  **This ordering is NOT covered by a test, and the attempt is recorded so nobody assumes it is.**
  Inverting it to publish-then-start leaves the whole membership suite green, because every
  assertion runs after the swap completes and both orders converge on the same final state. Seeing
  the difference requires a commit observing the intermediate state — a race with no deterministic
  fixture. `replication_membership_test.exs` says the same thing at the test that looks like it
  covers this. What the suite does catch is a name published that was never started at all.
  """
  use GenServer

  require Logger

  alias Fathom.Shard.Replication.{Fleet, Shipper}

  @poll_ms 30_000

  # Generous, and matching `Fleet`'s liveness window: this decides CANDIDACY, and a node dropped
  # from the candidate set because its beat was slow costs a seed to add back.
  @roster_window_ms 60_000

  def start_link(opts), do: GenServer.start_link(__MODULE__, opts, name: __MODULE__)

  @doc """
  Recompute now and apply the result. Returns the outcome so a caller (and the tests) can tell a
  swap from a refusal without reading logs.
  """
  @spec refresh() :: {:ok, [String.t()]} | {:refused, atom()}
  def refresh, do: GenServer.call(__MODULE__, :refresh, 15_000)

  @doc "The membership source: `:static` or `:roster`."
  @spec source() :: :static | :roster
  def source do
    case Application.get_env(:fathom, :replication_membership, :static) do
      :roster -> :roster
      _ -> :static
    end
  end

  @impl true
  def init(_opts) do
    # TRAPPING EXITS IS WHAT MAKES `terminate/2` BELOW RUN AT ALL. A supervisor shutting a child
    # down sends an exit signal, and a GenServer that does not trap it dies without its terminate
    # callback — so the published set outlived the tree that owned it. Safe here specifically
    # because this process links to nothing but its supervisor: shippers are linked to the shipper
    # `DynamicSupervisor`, not to us, and the poll is a `send_after`, not a link. So there is no
    # stray `{:EXIT, _, _}` for the trap to swallow.
    Process.flag(:trap_exit, true)

    # Resolve synchronously before the supervisor reports started: `Fleet.children/0` places this
    # ahead of nothing in particular, but a commit arriving before the first swap would see an
    # empty shipper list and raise `q >= n`. Boot is also the one place a too-small set must be
    # fatal rather than refused — same reasoning as `Fleet.validate_quorum!/0`, and the operator
    # is standing right there.
    case resolve() do
      {:ok, desired} ->
        apply_set!(desired)
        {:ok, %{current: desired}, {:continue, :schedule}}

      {:refused, reason} ->
        {:stop, boot_error(reason)}
    end
  end

  @impl true
  def handle_continue(:schedule, state), do: {:noreply, schedule(state)}

  @impl true
  def handle_call(:refresh, _from, state) do
    case swap(state) do
      {:ok, next} -> {:reply, {:ok, Enum.map(next.current, &elem(&1, 0))}, next}
      {:refused, reason, next} -> {:reply, {:refused, reason}, next}
    end
  end

  @impl true
  def handle_info(:poll, state) do
    {_, next} =
      case swap(state) do
        {:ok, s} -> {:ok, s}
        {:refused, _reason, s} -> {:ok, s}
      end

    {:noreply, schedule(next)}
  end

  # UNPUBLISH ON AN ORDERLY SHUTDOWN, because this process is the only thing that ever publishes a
  # NON-EMPTY set (`Fleet.init/1` publishes `[]`), so it is the one that should retract it.
  #
  # `:persistent_term` outlives the tree that wrote it. `Fleet.init/1` clears on START and says why
  # — "a Fleet restart that inherited the previous incarnation's names would hand the commit path
  # shipper processes that no longer exist" — but nothing cleared on STOP, so a stopped Fleet left
  # dead shipper names published for anything that read `shippers/0` or `running/0` afterwards.
  # In the suite that made `Recovery.peers/0` non-empty for every test following a replication test,
  # which silently routed promote-on-open through the fleet branch (caught on CI, 2026-08-26).
  #
  # THE REASON GUARD IS THE WHOLE SAFETY OF THIS, and the obvious version without it is a bug:
  # `terminate/2` also runs when a callback RAISES, and blanking the published set on a crash would
  # empty `shippers/0` on the commit path while this process restarts — `n` drops to 0 and
  # `q >= n` raises inside live tenant writes, the exact failure `validate_quorum!/0` exists to keep
  # off that path. A crash must leave the previous set standing; `init/1` republishes it moments
  # later. Only a deliberate teardown retracts.
  #
  # Rare by construction, which is what makes the write affordable: a `:persistent_term` write
  # schedules a literal-area scan across every process on the node, and `Fathom.Shard.Replication.Budget`
  # records that per-shipper-incarnation writes had to be moved to ETS for exactly that reason. This
  # fires once per Fleet shutdown — no more often than the membership swap that published it.
  @impl true
  def terminate(reason, _state) when reason in [:normal, :shutdown] do
    Fleet.publish([])
  end

  def terminate({:shutdown, _}, _state), do: Fleet.publish([])
  def terminate(_crash, _state), do: :ok

  # Only the roster source changes underneath us; a static list cannot, so polling it would be
  # pure noise in the logs and a pointless timer.
  defp schedule(state) do
    if source() == :roster do
      Process.send_after(self(), :poll, poll_ms())
    end

    state
  end

  defp swap(state) do
    case resolve() do
      {:ok, desired} when desired == state.current ->
        {:ok, state}

      {:ok, desired} ->
        apply_set!(desired)

        Logger.info(
          "replication membership changed: #{inspect(Enum.map(state.current, &elem(&1, 0)))} -> " <>
            "#{inspect(Enum.map(desired, &elem(&1, 0)))}"
        )

        :telemetry.execute(
          [:fathom, :replication, :membership_changed],
          %{size: length(desired)},
          %{
            source: source()
          }
        )

        {:ok, %{state | current: desired}}

      {:refused, reason} ->
        # Keeping the previous set is the whole point, so this is a warning and not an error: the
        # node is still replicating to a set that satisfied the quorum when it was accepted.
        Logger.warning(
          "replication membership refused (#{reason}); keeping #{length(state.current)} followers"
        )

        :telemetry.execute(
          [:fathom, :replication, :membership_refused],
          %{kept: length(state.current)},
          %{reason: reason, source: source()}
        )

        {:refused, reason, state}
    end
  end

  # -- resolution ----------------------------------------------------------------------------

  # Returns `{:ok, [{node_key, host, port}]}` or `{:refused, reason}`. Never returns a set the
  # quorum cannot be satisfied from.
  defp resolve do
    case source() do
      :static -> check(Fleet.endpoints(), :static_list_too_small)
      :roster -> resolve_roster()
    end
  end

  defp resolve_roster do
    case check(roster_endpoints(), :roster_too_small) do
      {:ok, endpoints} ->
        {:ok, endpoints}

      {:refused, _} ->
        # The roster cannot supply a quorum's worth yet — a fresh fleet, a rolling upgrade, or a
        # Postgres outage. Fall back to the configured list rather than to nothing, so the static
        # config remains the floor instead of becoming dead once roster mode is switched on.
        check(Fleet.endpoints(), :roster_and_static_too_small)
    end
  end

  defp roster_endpoints do
    Fathom.Rebalancer.Nodes.replication_endpoints(@roster_window_ms)
    |> Enum.reject(fn {node_key, _addr} -> node_key == Fathom.Rebalancer.node_key() end)
    |> Enum.flat_map(&parse_endpoint/1)
  rescue
    # A Postgres outage must not reach the write path, even transitively. An empty list here is
    # refused by `check/2` and the caller falls back, so this degrades to the static list.
    e ->
      Logger.warning("replication membership: roster unreadable (#{Exception.message(e)})")
      []
  catch
    _, _ -> []
  end

  # A malformed stored address is skipped rather than raised on: unlike `REPLICATION_FOLLOWERS`,
  # this value was not typed by the operator standing in front of the boot, and one bad row must
  # not take a running node's whole replica set with it. `check/2` still refuses the result if
  # skipping leaves too few.
  defp parse_endpoint({node_key, address}) do
    case String.split(address, ":") do
      [host, port] when host != "" ->
        case Integer.parse(port) do
          {p, ""} when p in 1..65_535 -> [{node_key, host, p}]
          _ -> skip(node_key, address)
        end

      _ ->
        skip(node_key, address)
    end
  end

  defp skip(node_key, address) do
    Logger.warning("replication membership: ignoring #{node_key}'s address #{inspect(address)}")
    []
  end

  defp check(endpoints, reason) do
    if length(endpoints) >= quorum() + 1, do: {:ok, endpoints}, else: {:refused, reason}
  end

  defp quorum, do: Application.get_env(:fathom, :replication_quorum, 2)
  defp poll_ms, do: Application.get_env(:fathom, :replication_membership_poll_ms, @poll_ms)

  # -- applying ------------------------------------------------------------------------------

  # START new, PUBLISH, STOP departed — see the moduledoc. Raises only from `init/1`'s call, where
  # a failure to start the very first shippers should fail the boot.
  defp apply_set!(desired) do
    running = Map.new(Fleet.running(), fn {key, _h, _p, name} -> {key, name} end)

    named =
      Enum.map(desired, fn {node_key, host, port} ->
        {node_key, host, port, shipper_name(node_key)}
      end)

    for {node_key, host, port, name} <- named, not Map.has_key?(running, node_key) do
      Fleet.start_shipper!(name, host, port)
    end

    Fleet.publish(named)

    desired_keys = MapSet.new(desired, &elem(&1, 0))

    for {node_key, name} <- running, not MapSet.member?(desired_keys, node_key) do
      Fleet.stop_shipper(name)
    end

    :ok
  end

  # Derived from node_key rather than an index, so a node keeps its shipper process across a
  # membership change that only reorders the set — an index-based name would restart every shipper
  # whenever one node was added in the middle, dropping four healthy sockets to add one.
  defp shipper_name(node_key) do
    Module.concat(Shipper, "N_" <> Base.url_encode64(node_key, padding: false))
  end

  defp boot_error(:static_list_too_small) do
    ":replication_enabled is on but :replication_followers has fewer than " <>
      "#{quorum() + 1} entries — a quorum of #{quorum()} cannot be satisfied. Q=N tolerates zero " <>
      "follower failures; see docs/a2-quorum-replication.md"
  end

  defp boot_error(reason) do
    "replication membership could not be resolved at boot (#{reason}). In :roster mode the " <>
      "fleet roster must already carry #{quorum() + 1} nodes advertising REPLICATION_ADVERTISE_HOST, " <>
      "or :replication_followers must supply them as the floor."
  end
end
