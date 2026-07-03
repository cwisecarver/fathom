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
  """
  @behaviour Plug

  import Plug.Conn

  @impl true
  def init(opts), do: opts

  @impl true
  def call(%Plug.Conn{method: "GET", request_path: "/health"} = conn, _opts) do
    conn
    |> put_resp_content_type("text/plain")
    |> send_resp(200, "ok")
  end

  def call(conn, _opts) do
    conn
    |> put_resp_content_type("text/plain")
    |> send_resp(404, "not found")
  end
end
