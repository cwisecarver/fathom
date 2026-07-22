defmodule Fathom.HealthPlug do
  @moduledoc """
  Minimal liveness endpoint for the load balancer.

  The cluster runs N independent fathom nodes behind an L7 LB that subdomain-partitions
  traffic (see `docs/deploy-cluster.md`). The LB needs a cheap per-node target to health
  check. `GET /health` returns `200 "ok"` (the BEAM is up and serving); any other method or
  path is `404`.

  **Liveness only — it deliberately does NOT touch S3.** An LB probe must add no shard-storage
  load, and a transient S3 blip must not flap a whole node out of rotation: a node cut off
  from S3 already self-fences its individual shards through the lease (`Fathom.Shard`), which
  is the right granularity. Whole-node readiness gating on S3 would be both noisier and wrong.

  Served on its own Bandit listener (`:health_port`, default 8081), separate from the Hrana
  listener, so it never interacts with Filo's stream/baton handling.

  **Draining (expert review #28).** A node about to shut down calls `begin_draining/0` so `/health`
  flips to `503 "draining"` — the LB deregisters it and stops routing new traffic FIRST, before
  `Fathom.Shards.drain_all/1` voluntarily drains the open coordinators. This turns a deploy from the
  crash-adjacent supervisor-shutdown path into an ordered graceful drain. The flag lives in
  `persistent_term` (one write at shutdown, lock-free reads on every probe).
  """
  @behaviour Plug

  import Plug.Conn

  @draining_key {__MODULE__, :draining}

  @doc "Mark this node draining — `/health` returns 503 so the LB deregisters it (#28)."
  @spec begin_draining() :: :ok
  def begin_draining, do: :persistent_term.put(@draining_key, true)

  @doc "Clear the draining flag (a cancelled drain / tests)."
  @spec end_draining() :: :ok
  def end_draining, do: :persistent_term.put(@draining_key, false)

  @doc "Whether this node is draining (health-checked out of rotation)."
  @spec draining?() :: boolean()
  def draining?, do: :persistent_term.get(@draining_key, false)

  @impl true
  def init(opts), do: opts

  @impl true
  def call(%Plug.Conn{method: "GET", request_path: "/health"} = conn, _opts) do
    # Draining ⇒ fail the probe so the LB stops routing here before we drain (#28). Still 200 while
    # serving normally, even if this node is cut from S3 (per-shard self-fencing is the right
    # granularity — see the module note).
    {status, body} = if draining?(), do: {503, "draining"}, else: {200, "ok"}

    conn
    |> put_resp_content_type("text/plain")
    |> send_resp(status, body)
  end

  def call(conn, _opts) do
    conn
    |> put_resp_content_type("text/plain")
    |> send_resp(404, "not found")
  end
end
