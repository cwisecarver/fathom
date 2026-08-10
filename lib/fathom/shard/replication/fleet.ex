defmodule Fathom.Shard.Replication.Fleet do
  @moduledoc """
  The replication supervision tree — A2. See `docs/a2-quorum-replication.md`.

  Starts, under one supervisor:

    * one `Shipper` per configured follower **node** (not per shard — see `Shipper`) — the
      PRIMARY half, gated by `:replication_enabled`
    * the `Follower` listener — the RECEIVE half, gated separately by `:replication_listen`
    * a `Registry` + `DynamicSupervisor` for the per-shard `Session` processes

  Entirely inert unless one of those gates is on, matching every other Phase 2 component
  (`:shard_load`, `:warm_follower`, `:rebalancer_enabled`): a feature that changes the commit path
  must be something an operator turns on deliberately, after reading the runbook, not something
  that arrives with a deploy.

  ## Two gates, because shipping and receiving are different roles

  `:replication_listen` was added 2026-08-10 to fix a gap that made A2 undeployable: the `Follower`
  listener existed, was well tested, and **was never started outside the test suite**. A node with
  `:replication_enabled` shipped every commit to addresses where nothing was listening, collected
  no acks, and returned 503 `FILO_NO_QUORUM` for every tenant write — while the operator docs
  instructed setting `REPLICATION_FOLLOWERS` to a port fathom never opened.

  Keeping them separate is not just caution about a past bug. A node can hold other nodes' replicas
  without replicating its own shards, and a safe rollout turns listening on **fleet-wide first**,
  then enables shipping — an ordering a single flag cannot express.

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
  @running_key {__MODULE__, :running}

  # How recently a node must have beaten in Postgres to count as live. Generous on purpose: this
  # feeds an operator gauge, and a false "dead" on a slow beat is worse than a stale "alive".
  @alive_window_ms 60_000

  # 9100 because that is the port `config/runtime.exs`'s REPLICATION_FOLLOWERS example has always
  # shown, and operators have read that comment. It also stays clear of the two ports fathom
  # already binds — `:hrana_port` 8080 and `:health_port` 8081.
  @default_listen_port 9100

  def start_link(opts), do: Supervisor.start_link(__MODULE__, opts, name: __MODULE__)

  @doc """
  The shippers to replicate through, in configured order.

  Read from `:persistent_term` rather than by walking the supervisor: this is on the commit path,
  and a `Supervisor.which_children/1` per commit would be a message round trip to add to the
  network round trip already being paid.
  """
  @spec shippers() :: [pid() | atom()]
  def shippers, do: :persistent_term.get(@shippers_key, [])

  @doc """
  Configured followers, in either accepted shape.

  `{node_key, host, port}` is what `REPLICATION_FOLLOWERS` produces. `{host, port}` is the older
  anonymous form and still works — it is what the test suite passes, and a config shape change is
  not a reason to churn every test. Use `endpoints/0` to get one normalised shape.
  """
  @spec followers() :: [
          {String.t() | charlist(), :inet.port_number()}
          | {String.t(), String.t() | charlist(), :inet.port_number()}
        ]
  def followers, do: Application.get_env(:fathom, :replication_followers, [])

  @doc """
  Configured followers normalised to `{node_key, host, port}`.

  An anonymous `{host, port}` gets `"host:port"` as its key. That is only an identifier for logs,
  telemetry and `health/0` — nothing routes on it — so a synthesized one is honest rather than
  forcing every caller to supply a name it does not have.
  """
  @spec endpoints() :: [{String.t(), String.t() | charlist(), :inet.port_number()}]
  def endpoints, do: Enum.map(followers(), &normalise/1)

  defp normalise({node_key, host, port}), do: {to_string(node_key), host, port}
  defp normalise({host, port}), do: {"#{to_string(host)}:#{port}", host, port}

  @doc """
  Parse `REPLICATION_FOLLOWERS` — `node_key@host:port` pairs, comma separated.

  The `node_key@` prefix is optional. Raises on anything it cannot parse, because the alternative
  is a node booting with a silently shorter follower list than the operator wrote: replication
  would run, the quorum would pass, and the shard would be under-replicated with no error anywhere.
  A boot failure is the correct direction for a malformed replica set.
  """
  @spec parse_followers!(String.t()) :: [{String.t(), String.t(), :inet.port_number()}]
  def parse_followers!(spec) when is_binary(spec) do
    spec
    |> String.split(",", trim: true)
    |> Enum.map(&parse_follower!(String.trim(&1), spec))
  end

  defp parse_follower!(entry, spec) do
    {node_key, address} =
      case String.split(entry, "@", parts: 2) do
        [key, addr] -> {String.trim(key), String.trim(addr)}
        [addr] -> {nil, String.trim(addr)}
      end

    # Exactly two colon-separated parts. An IPv6 literal has more and is rejected rather than
    # truncated at its first colon into a plausible-looking host that would connect somewhere else.
    with [host, port_str] <- String.split(address, ":"),
         {port, ""} <- Integer.parse(String.trim(port_str)),
         true <- host != "" and port in 1..65_535 do
      {node_key || "#{host}:#{port}", host, port}
    else
      _ -> raise ArgumentError, bad_follower(entry, spec)
    end
  end

  defp bad_follower(entry, spec) do
    "REPLICATION_FOLLOWERS entry #{inspect(entry)} is not `node_key@host:port` with a port in " <>
      "1..65535 (full value: #{inspect(spec)}). IPv6 literals are not supported. Refusing to " <>
      "boot rather than start with a shorter follower list than was configured — that would " <>
      "under-replicate every shard while the quorum still passed."
  end

  @doc """
  Per-follower operational state, for the dashboard and the periodic gauge.

  `connected?` is the socket. `alive?` is whether that `node_key` has beaten into the Postgres
  roster recently (`Fathom.Rebalancer.Nodes.alive/1`), or `:unknown` when the roster cannot be read
  or the follower has no registered key.

  **This is observability, and deliberately nothing more.** Liveness must never filter the
  per-commit push set: shrinking `n` is exactly how `q >= n` starts raising inside a tenant's
  commit, and a disconnected follower already costs nothing — `Shipper` refuses it without a socket
  write and `Quorum` reports `:impossible` as soon as too few remain. A remote, flappable signal
  cannot improve on that and can only add a way to break writes. It is also why this is called on a
  timer and never from `Session.commit/3`: it touches Postgres, and a Postgres outage must not
  reach the write path.
  """
  @spec health() :: [
          %{
            node_key: String.t(),
            host: String.t(),
            port: :inet.port_number(),
            connected?: boolean(),
            alive?: boolean() | :unknown
          }
        ]
  def health do
    live = live_node_keys()

    for {node_key, host, port, name} <- running() do
      %{
        node_key: node_key,
        host: to_string(host),
        port: port,
        connected?: connected?(name),
        alive?: alive?(live, node_key)
      }
    end
  end

  @doc """
  Socket state per follower, as `{node_key, connected?}` — and **nothing that touches Postgres**.

  Split out from `health/0` for one reason: the 10 s telemetry poller is documented as
  Postgres-free (`Fathom.Telemetry.init/1`, where only the 30 s Oban poller may query the DB), and
  the signal an operator needs paged on — how many followers can actually ack right now — is
  socket state alone. The roster's `alive?` is dashboard colour and belongs on the slower,
  DB-aware path.
  """
  @spec connection_status() :: [{String.t(), boolean()}]
  def connection_status do
    for {node_key, _host, _port, name} <- running(), do: {node_key, connected?(name)}
  end

  @doc "The followers actually supervised right now, as `{node_key, host, port, shipper_name}`."
  @spec running() :: [{String.t(), String.t() | charlist(), :inet.port_number(), atom()}]
  def running, do: :persistent_term.get(@running_key, [])

  defp connected?(name) do
    Fathom.Shard.Replication.Shipper.connected?(name)
  catch
    # A shipper mid-restart is not connected; it is not a reason to crash the caller's poller.
    :exit, _ -> false
  end

  defp alive?(:unknown, _node_key), do: :unknown
  defp alive?(live, node_key), do: MapSet.member?(live, node_key)

  # Fails open to `:unknown` rather than to `false`: reporting every follower dead because Postgres
  # blinked would page an operator toward the wrong system entirely.
  defp live_node_keys do
    Fathom.Rebalancer.Nodes.alive(@alive_window_ms)
  rescue
    _ -> :unknown
  catch
    _, _ -> :unknown
  end

  @impl true
  def init(_opts) do
    if replicating?(), do: validate_quorum!()

    running =
      if replicating?() do
        endpoints()
        |> Enum.with_index()
        |> Enum.map(fn {{node_key, host, port}, i} ->
          {node_key, host, port, Module.concat(__MODULE__, :"Shipper#{i}")}
        end)
      else
        []
      end

    names = Enum.map(running, fn {_k, _h, _p, name} -> name end)

    # Two terms rather than one: `shippers/0` is read on the commit path and must stay a bare list
    # of names, while `running/0` carries the identity `health/0` reports. Keeping the hot read
    # free of the operator metadata is the same reasoning that put this in `:persistent_term`.
    :persistent_term.put(@shippers_key, names)
    :persistent_term.put(@running_key, running)

    shipper_specs =
      Enum.map(running, fn {_node_key, host, port, name} ->
        Supervisor.child_spec(
          {Fathom.Shard.Replication.Shipper, name: name, host: host, port: port},
          id: name
        )
      end)

    children =
      [
        {Registry, keys: :unique, name: @registry},
        {DynamicSupervisor, name: @sessions, strategy: :one_for_one}
      ] ++ follower_children() ++ shipper_specs

    Supervisor.init(children, strategy: :one_for_one)
  end

  @doc """
  The child spec to place in the application tree — `[]` when this node neither ships nor listens.

  Returning a list rather than a spec keeps `application.ex` free of a conditional: it splices in
  nothing when the feature is off.
  """
  @spec children() :: [Supervisor.child_spec() | {module(), term()}]
  def children do
    if replicating?() or listening?(), do: [__MODULE__], else: []
  end

  @doc "Is this node shipping its own shards' frames to followers? (`:replication_enabled`)"
  @spec replicating?() :: boolean()
  def replicating?, do: Application.get_env(:fathom, :replication_enabled, false) == true

  @doc "Is this node accepting frames as somebody's follower? (`:replication_listen`)"
  @spec listening?() :: boolean()
  def listening?, do: Application.get_env(:fathom, :replication_listen, false) == true

  # SHIPPING AND RECEIVING ARE SEPARATE ROLES, hence separate gates.
  #
  # Until this existed the `Follower` listener was started only by tests: `children/0` keyed off
  # `:replication_enabled` and `init/1` supervised a Registry, a DynamicSupervisor and the
  # Shippers — the PRIMARY half, all of it. So a deployment with replication on shipped every
  # commit to addresses where nothing listened, took no acks, and 503'd `FILO_NO_QUORUM` on every
  # tenant write. `docs/configuration.md` meanwhile told operators to point REPLICATION_FOLLOWERS
  # at a port fathom never opened.
  #
  # They stay independent rather than becoming one flag because the roles genuinely differ: a node
  # can hold others' replicas without replicating its own shards, and a rollout MUST turn listening
  # on fleet-wide BEFORE any node starts shipping — one flag makes that ordering unexpressible.
  defp follower_children do
    if listening?() do
      opts =
        [port: listen_port(), dir: Fathom.Shard.Replication.Follower.default_dir()]
        |> then(fn base ->
          case Application.get_env(:fathom, :replication_bind_ip) do
            nil -> base
            ip -> Keyword.put(base, :ip, ip)
          end
        end)

      [{Fathom.Shard.Replication.Follower, opts}]
    else
      []
    end
  end

  @doc """
  The port the follower listener binds. `:replication_listen_port`, default #{@default_listen_port}.

  Deliberately not 0 (which `Follower` reads as "pick any"): an ephemeral port is right for a test
  and useless in production, where peers must be told where to connect.
  """
  @spec listen_port() :: :inet.port_number()
  def listen_port do
    Application.get_env(:fathom, :replication_listen_port, @default_listen_port)
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
