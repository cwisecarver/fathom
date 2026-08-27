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
  is a `Sqlite3.columns/2` call, not a re-prepare.

  ## Why ETS and not `:persistent_term`

  `:persistent_term.put/2` triggers a global scan of processes holding persistent-term references.
  DDL can be tenant-issued whenever `:block_tenant_ddl` is off, which is the default, so a tenant
  looping `CREATE TABLE`/`DROP TABLE` would be issuing node-wide GC pressure on demand. An ETS
  counter has no such global effect.

  Measured on this machine: the read is ~0.32 µs against a ~7.7 µs raw query and a ~123 µs Hrana
  round trip, i.e. ~0.26% of a request.
  """
  use GenServer

  @table __MODULE__
  @key :gen

  @doc false
  def start_link(opts), do: GenServer.start_link(__MODULE__, opts, name: __MODULE__)

  @doc """
  The current schema generation for this node.

  Returns `0` when the table is absent — a caller outside the supervision tree (the migrator's own
  copy connections, `mix fathom.shard`, the bench harness). `0` is stable, so entries cached under
  it never invalidate, which is exactly the pre-review behaviour and correct for a caller that owns
  its connection exclusively.
  """
  @spec current() :: non_neg_integer()
  def current do
    :ets.lookup_element(@table, @key, 2)
  rescue
    ArgumentError -> 0
  end

  @doc """
  Record that DDL ran somewhere on this node, invalidating cached column lists.

  Idempotent in effect and safe to over-call: the only consequence of a spurious bump is one
  `Sqlite3.columns/2` re-read per live cached statement.
  """
  @spec bump() :: :ok
  def bump do
    :ets.update_counter(@table, @key, {2, 1})
    :ok
  rescue
    ArgumentError -> :ok
  end

  @impl true
  def init(_opts) do
    # Public + read_concurrency: every stream process reads this per statement off its own
    # scheduler; writes are rare (DDL only) and go through update_counter.
    :ets.new(@table, [:set, :public, :named_table, read_concurrency: true])
    :ets.insert(@table, {@key, 1})
    {:ok, %{}}
  end
end
