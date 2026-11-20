defmodule Fathom.Shard.SchemaGen do
  @moduledoc """
  A node-global counter bumped whenever any connection executes DDL.

  ## Why this exists (expert review 2026-08-26 #7)

  `Fathom.Shard.Connection` caches prepared statements per connection, and caches the COLUMN LIST
  alongside each one. SQLite's `prepare_v2` transparently recompiles a statement when the schema
  changes, so the *statement* survives DDL — but the Elixir-side column list captured at prepare
  time does not, and nothing re-read it.

  Verified by execution in the audit. Connection B caches `SELECT * FROM t`, connection A runs
  `ALTER TABLE t ADD COLUMN y text DEFAULT (0)`, B re-runs:

      B before ALTER: {["id", "x"], [[1, "hello"]]}
      B after  ALTER: {["id", "x"], [[1, "hello", "0"]]}

  Two column names, three values per row. That `StmtResult` goes on the wire, and a client zipping
  `cols` to values silently mis-maps columns — Django's `cursor.description` reports 2 fields for a
  3-wide row.

  `ShardExecutor` already purged the cache for DDL *that connection* executed (review 2026-07-24
  #17); what it could not see was DDL run by a DIFFERENT stream on the same shard.

  ## Why node-global rather than per-shard

  Deliberate. A per-shard generation would need the shard id threaded into `Connection`, which is
  otherwise a plain module over a file path with callers (the migrator, snapshots, the bench and
  scale harnesses) that have no shard identity at all. A node-global counter needs nothing threaded
  and cannot miss a bump.

  The cost of over-invalidation is small and one-sided: a DDL on shard A makes shard B's next
  statement re-read its column list. DDL is rare — `:block_tenant_ddl` exists to refuse it from
  tenants entirely, and the migration engine drains a shard before running any — and the recovery
  is one extra `Sqlite3.columns/2` call after the statement's next step (expert review 2026-10-21
  r7 #18; it used to be release + re-prepare + columns + a memused probe, 5-8 dispatches and a
  parse per cached statement). SQLite recompiles a statement whose own schema moved at step, so
  the statement itself is kept.

  ## Why an `:atomics` cell behind ONE `:persistent_term`, not a `:persistent_term` per bump

  `:persistent_term.put/2` triggers a global scan of processes holding persistent-term references.
  DDL can be tenant-issued whenever `:block_tenant_ddl` is off, which is the default, so a tenant
  looping `CREATE TABLE`/`DROP TABLE` would be issuing node-wide GC pressure on demand if every bump
  were a `put`. So the counter itself is an `:atomics` cell, bumped with `:atomics.add/3` (no global
  effect), and `:persistent_term` holds only the cell's REFERENCE, put once per node (the first
  `init/1`; a supervisor restart finds it and keeps counting).

  The read is on the hottest cache path: once per executed statement and once per describe, so twice
  per SDK statement (perf audit 2026-11-19 #4). It used to be an ETS lookup inside a `rescue` frame,
  measured at ~0.32 µs (against a ~7.7 µs raw query and a ~123 µs Hrana round trip). It is now a
  `:persistent_term.get/2` plus an `:atomics.get/2`, the same pair `Connection.busy_waiters?/0` uses.
  Atomics are sequentially consistent, so a sibling stream still sees a bump made before its next
  statement (expert review 2026-08-26 #7).
  """
  use GenServer

  @ref_key {__MODULE__, :ref}

  @doc false
  def start_link(opts), do: GenServer.start_link(__MODULE__, opts, name: __MODULE__)

  @doc """
  The current schema generation for this node.

  Returns `0` when the counter is absent — a caller outside the supervision tree (the migrator's own
  copy connections, `mix fathom.shard`, the bench harness). `0` is stable, so entries cached under
  it never invalidate, which is exactly the pre-review behaviour and correct for a caller that owns
  its connection exclusively.
  """
  @spec current() :: non_neg_integer()
  def current do
    case :persistent_term.get(@ref_key, nil) do
      nil -> 0
      ref -> :atomics.get(ref, 1)
    end
  end

  @doc """
  Record that DDL ran somewhere on this node, invalidating cached column lists.

  Idempotent in effect and safe to over-call: the only consequence of a spurious bump is one
  `Sqlite3.columns/2` re-read per live cached statement.
  """
  @spec bump() :: :ok
  def bump do
    case :persistent_term.get(@ref_key, nil) do
      nil -> :ok
      ref -> :atomics.add(ref, 1, 1)
    end
  end

  @impl true
  def init(_opts) do
    # The cell is created once per node and never replaced: a restarted SchemaGen keeps counting
    # rather than returning to 1, which can only cause extra column refreshes, never fewer. The
    # `put` happens here, at boot, not per DDL (see the moduledoc).
    if :persistent_term.get(@ref_key, nil) == nil do
      ref = :atomics.new(1, signed: false)
      :atomics.put(ref, 1, 1)
      :persistent_term.put(@ref_key, ref)
    end

    {:ok, %{}}
  end
end
