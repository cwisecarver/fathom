defmodule Fathom.Shard.Replication.SeedGate do
  @moduledoc """
  Node-wide cap on concurrent replication SEEDS (expert review 2026-10-01 #6).

  ## Why

  A seed streams a tenant's whole `.db` + `-wal` to one follower. Since reseed-on-ownership-change
  (73b4d08) every idle drop followed by a write seeds every follower, and a node failover reopens
  ~1/N of the fleet at once — so seeds arrive in bursts, one task per shard × follower, with
  nothing bounding how many run. Each seed shares its follower's single link with every other
  shard's pushes (a 4 MiB chunk holds a 1 Gbit link ~34 ms), so an unbounded burst is head-of-line
  blocking for every tenant on the node and `cap × chunk` of resident memory.

  `acquire/1` blocks the seed task until a slot is free; the commit path never waits on it (seeds
  run in their own task, and the shard's pushes to that follower are already refused until its seed
  lands). A waiting seed is simply a later seed.

  ## Why a monitor-based `Fathom.PermitGate`, unlike `FlushGate`

  `FlushGate` is lock-free ETS because it is asked on every flush; a seed is rare and long, so one
  call per seed costs nothing. In exchange the slot is tied to a MONITOR: `Session` kills a wedged
  seed task with `Process.exit(pid, :kill)` (`expire_seeds/1`), which runs no `after` block, and a
  release that depended on the task's own cleanup would leak the slot. The `:DOWN` frees it.

  `:replication_seed_max_concurrency` sets the cap; `0` or `nil` (the default) disables the gate —
  see `@default_cap` for the measurement that turned it off.
  """
  alias Fathom.PermitGate

  # OFF BY DEFAULT, MEASURED (rig, 2026-10-03, 1024 tenants, flush 5 s, valid same-session runs):
  # cap 8 = 2,315 txn/s / 50,837 errors; no cap = 1,983 / 10,769; the pre-#6 build = 2,008 /
  # 11,214. A queued seed is not harmless: until it lands, that follower answers `:unknown_shard` /
  # `:seeding`, and a shard whose followers are all waiting in line has no quorum at all. Bounding
  # link contention that way traded a little head-of-line blocking for ~5x the failed commits. At
  # 30 s flush the cap was neutral-to-better (1,348 errors vs 2,556 pre-#6), so the knob stays for
  # an operator who measures their own fleet; the default must not shed writes.
  @default_cap 0

  @doc false
  def start_link(opts),
    do: PermitGate.start_link(name: Keyword.get(opts, :name, __MODULE__), cap: &permits/0)

  @doc false
  def child_spec(opts), do: %{id: __MODULE__, start: {__MODULE__, :start_link, [opts]}}

  @doc """
  Wait for a seed slot, up to `timeout_ms`. `:ok` — the slot is held until `release/1` or until the
  calling process exits. `{:error, :seed_gate_timeout}` — no slot came free in time.
  `:ok` immediately when the gate is disabled or not running.
  """
  @spec acquire(non_neg_integer(), GenServer.server()) :: :ok | {:error, :seed_gate_timeout}
  def acquire(timeout_ms, gate \\ __MODULE__) do
    case PermitGate.acquire(gate, permits(), timeout_ms) do
      :ok -> :ok
      :timeout -> {:error, :seed_gate_timeout}
    end
  end

  @doc "Give the caller's slot back. A no-op if it holds none."
  @spec release(GenServer.server()) :: :ok
  def release(gate \\ __MODULE__), do: PermitGate.release(gate)

  @doc "Slots held and seeds waiting. For tests and the dashboard."
  @spec stats(GenServer.server()) :: %{held: non_neg_integer(), waiting: non_neg_integer()}
  def stats(gate \\ __MODULE__), do: PermitGate.stats(gate)

  @doc false
  def cap,
    do: Application.get_env(:fathom, :replication_seed_max_concurrency, @default_cap)

  # `cap/0` is the raw env value; PermitGate wants `pos_integer() | nil`. Anything that is not a
  # positive integer (`0`, `nil`, junk) means UNLIMITED here, which is this gate's own meaning (the
  # other two gates only ever produce a positive integer or nil from their own `cap/0`).
  defp permits do
    case cap() do
      n when is_integer(n) and n > 0 -> n
      _ -> nil
    end
  end
end
