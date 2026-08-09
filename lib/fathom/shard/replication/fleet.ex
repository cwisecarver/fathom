defmodule Fathom.Shard.Replication.Fleet do
  @moduledoc """
  The replication supervision tree — A2. See `docs/a2-quorum-replication.md`.

  Starts, under one supervisor:

    * one `Shipper` per configured follower **node** (not per shard — see `Shipper`)
    * a `Registry` + `DynamicSupervisor` for the per-shard `Session` processes

  Entirely inert unless `:replication_enabled`, matching every other Phase 2 component
  (`:shard_load`, `:warm_follower`, `:rebalancer_enabled`): a feature that changes the commit path
  must be something an operator turns on deliberately, after reading the runbook, not something
  that arrives with a deploy.

  ## Followers come from config, for now

  `:replication_followers` is a list of `{host, port}`. Real membership — discovering the four
  followers for a shard, tracking their liveness, and reacting when one is replaced — is the next
  piece of work and is deliberately not faked here. A static list is honest about what exists and
  is enough to run the commit path end to end.

  **Placement is not a detail to fill in later.** The RTT sweep measured 2-of-4 at 1.6 ms with two
  near followers against 134 ms with all four far — an 82× difference for the same replica count.
  So the order and locality of this list is a latency decision, and `docs/a2-quorum-replication.md`
  carries the rule: a quorum's worth must be near the primary, in a *different* AZ, with the rest
  far for failure-domain spread.
  """
  use Supervisor

  @registry Fathom.Shard.Replication.SessionRegistry
  @sessions Fathom.Shard.Replication.SessionSupervisor
  @shippers_key {__MODULE__, :shippers}

  def start_link(opts), do: Supervisor.start_link(__MODULE__, opts, name: __MODULE__)

  @doc """
  The shippers to replicate through, in configured order.

  Read from `:persistent_term` rather than by walking the supervisor: this is on the commit path,
  and a `Supervisor.which_children/1` per commit would be a message round trip to add to the
  network round trip already being paid.
  """
  @spec shippers() :: [pid() | atom()]
  def shippers, do: :persistent_term.get(@shippers_key, [])

  @doc "Configured followers as `{host, port}` tuples."
  @spec followers() :: [{String.t() | charlist(), :inet.port_number()}]
  def followers, do: Application.get_env(:fathom, :replication_followers, [])

  @impl true
  def init(_opts) do
    validate_quorum!()

    names =
      followers()
      |> Enum.with_index()
      |> Enum.map(fn {{_h, _p}, i} -> Module.concat(__MODULE__, :"Shipper#{i}") end)

    :persistent_term.put(@shippers_key, names)

    shipper_specs =
      followers()
      |> Enum.zip(names)
      |> Enum.map(fn {{host, port}, name} ->
        Supervisor.child_spec(
          {Fathom.Shard.Replication.Shipper, name: name, host: host, port: port},
          id: name
        )
      end)

    children =
      [
        {Registry, keys: :unique, name: @registry},
        {DynamicSupervisor, name: @sessions, strategy: :one_for_one}
      ] ++ shipper_specs

    Supervisor.init(children, strategy: :one_for_one)
  end

  @doc """
  The child spec to place in the application tree — `[]` when replication is off.

  Returning a list rather than a spec keeps `application.ex` free of a conditional: it splices in
  nothing when the feature is off.
  """
  @spec children() :: [Supervisor.child_spec() | {module(), term()}]
  def children do
    if Application.get_env(:fathom, :replication_enabled, false), do: [__MODULE__], else: []
  end

  # A quorum that cannot be satisfied by the configured follower count must fail the BOOT, not the
  # first write. `Quorum.new/3` raises on `q >= n` by design, but reaching that raise from inside a
  # commit means the misconfiguration presents as every tenant write failing under load — the
  # worst possible time to discover a config typo. Found by the integration test, which configured
  # two followers with a quorum of two and got exactly that.
  defp validate_quorum! do
    n = length(followers())
    q = Application.get_env(:fathom, :replication_quorum, 2)

    cond do
      n == 0 ->
        raise ArgumentError,
              ":replication_enabled is on but :replication_followers is empty — there is nothing " <>
                "to replicate to"

      q >= n ->
        raise ArgumentError,
              ":replication_quorum #{q} must be < #{n} configured followers. Q=N tolerates zero " <>
                "follower failures and inherits the slowest replica (measured 32-82x worse) — " <>
                "see docs/a2-quorum-replication.md"

      true ->
        :ok
    end
  end
end
