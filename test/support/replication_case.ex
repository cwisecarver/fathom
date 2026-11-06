defmodule Fathom.Test.ReplicationCase do
  @moduledoc """
  Scaffolding shared by the replication tests: real followers on ephemeral ports, a `Fleet`
  pointed at them, and the waits that make the first commit deterministic.

  Import it (`import Fathom.Test.ReplicationCase`) in a test module that runs in the test process;
  everything here uses `start_supervised!/1` and `on_exit/1`, so cleanup is the test's own.
  """

  import ExUnit.Callbacks, only: [start_supervised!: 1, start_supervised!: 2]

  alias Fathom.Shard.Replication.{Fleet, Follower, Shipper}
  alias Fathom.Test.{Await, Env}

  @sessions Fathom.Shard.Replication.SessionRegistry

  @doc """
  Start `n` followers under `root`, named `:"<prefix>_f<i>_<unique>"`. Returns `[{name, port}]`.
  """
  @spec start_followers!(Path.t(), pos_integer(), String.t()) :: [{atom(), :inet.port_number()}]
  def start_followers!(root, n, prefix) do
    for i <- 1..n do
      name = :"#{prefix}_f#{i}_#{System.unique_integer([:positive])}"
      dir = Path.join(root, to_string(name))
      pid = start_supervised!({Follower, name: name, port: 0, dir: dir}, id: name)
      {:ok, port} = Follower.port(pid)
      {name, port}
    end
  end

  @doc """
  Turn replication on against `followers` (`[{name, port}]` or bare ports, e.g. proxy ports) and
  start `Fleet`. Every env key is set through `Fathom.Test.Env`, so it is restored on exit.

  Options:
    * `:quorum` - default 2
    * `:timeout` - `:replication_timeout_ms`, left alone when absent
    * `:catchup` - `:replication_catchup_ms`, left alone when absent
    * `:cap` - `:replication_max_push_bytes`, left alone when absent
    * `:connected` - wait for shippers: `:all` (every follower), an integer (at least that many,
      for a test that points one at a closed port), or `false` (default, no wait)
  """
  @spec enable_replication!([{atom(), :inet.port_number()} | :inet.port_number()], keyword()) ::
          :ok
  def enable_replication!(followers, opts \\ []) do
    ports = for f <- followers, do: if(is_tuple(f), do: elem(f, 1), else: f)

    Env.put!(:replication_enabled, true)
    Env.put!(:replication_quorum, Keyword.get(opts, :quorum, 2))
    if t = opts[:timeout], do: Env.put!(:replication_timeout_ms, t)
    if Keyword.has_key?(opts, :catchup), do: Env.put!(:replication_catchup_ms, opts[:catchup])
    if cap = opts[:cap], do: Env.put!(:replication_max_push_bytes, cap)
    Env.put!(:replication_followers, for(p <- ports, do: {~c"127.0.0.1", p}))

    start_supervised!(Fleet)

    case Keyword.get(opts, :connected, false) do
      false -> :ok
      :all -> await_connected!(length(ports))
      n when is_integer(n) -> await_connected!(n)
    end
  end

  # Shippers connect in `handle_continue`, so without this the FIRST commit races the connect and
  # every follower answers `:disconnected` — not `:unknown_shard`, so no seed is triggered and the
  # test looks like seeding is broken. Production self-heals (the next commit finds them connected
  # and seeds), but a test that depends on that race is a flake.
  @doc "Wait until at least `n` shippers are connected (default: all of them)."
  @spec await_connected!(non_neg_integer() | :all, non_neg_integer()) :: :ok
  def await_connected!(n \\ :all, timeout \\ 5_000) do
    Await.until(
      fn ->
        shippers = Fleet.shippers()
        want = if n == :all, do: length(shippers), else: n
        Enum.count(shippers, &Shipper.connected?/1) >= want
      end,
      "shippers never connected",
      timeout
    )
  end

  @doc "The replication Session pid for shard `id`."
  @spec session!(String.t()) :: pid()
  def session!(id) do
    [{pid, _}] = Registry.lookup(@sessions, id)
    pid
  end

  @doc "Wait until the Session for `id` has no seed in flight."
  @spec await_no_seeds!(String.t(), non_neg_integer()) :: :ok
  def await_no_seeds!(id, timeout \\ 5_000) do
    Await.until(
      fn -> :sys.get_state(session!(id)).seeding == %{} end,
      "the initial seeds never landed",
      timeout
    )
  end
end
