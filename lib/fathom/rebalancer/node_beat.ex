defmodule Fathom.Rebalancer.NodeBeat do
  @moduledoc """
  One per-node_key liveness row (`rebalancer_nodes`) — the signal the dead-node reconciler
  keys on (finding #1b). `node_key` is the stable `Fathom.Rebalancer.node_key/0` a shard can
  be pinned to; `last_seen_at` is refreshed every reporter tick. Distinct from the S3
  heartbeat, which is keyed by the boot-scoped lease owner (`node()#incarnation`), not the
  node_key the LB exception table references.

  ## Clock assumption (review 2026-07-09 #9)

  `last_seen_at` is the *beating node's* wall clock, compared against the reader's clock in
  `Fathom.Rebalancer.Nodes.alive/1` (and `fleet_p99/2`), so it assumes the fleet is NTP-synced
  within a few seconds (small vs the ~60s stale window). The **destructive** risk this once
  carried — a clock-lagged node judged dead and its hot pins deleted — is closed: the reconciler
  confirms against the authoritative S3 lease (`Storage.lease_holder/1`) before unpinning
  (review #1), so a skewed-but-live node keeps its pin. The residual is benign: a fast-clock
  *dead* node lingers in the `alive` set until its future timestamp passes (its shards stay
  available via the #1a backup upstreams + the freed lease), and the freshness windows include
  slightly-skewed rows. Keep NTP healthy; a single-server-clock `last_seen_at` is a deferred
  fuller fix (same pattern as #15).
  """
  use Ecto.Schema

  @type t :: %__MODULE__{}
  @primary_key {:node_key, :string, autogenerate: false}

  schema "rebalancer_nodes" do
    field :last_seen_at, :utc_datetime_usec
    # This node's full-distribution q/s p99 and its sample count for the window (finding #2),
    # published alongside the liveness beat. Kept for observability; the fleet bar itself is
    # now computed from q_hist (below).
    field :q_p99, :float
    field :sample_count, :integer
    # A fixed-bucket histogram of this node's per-shard q/s (Fathom.Rebalancer.Stats.histogram/1),
    # summed across nodes into the TRUE pooled-distribution p99 — the sound fleet hot bar that
    # replaced the count-weighted mean of per-node p99s (finding #4).
    field :q_hist, {:array, :integer}
  end
end
