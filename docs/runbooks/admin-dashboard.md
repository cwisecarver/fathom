# Runbook — admin dashboard (`/admin`)

The realtime operator dashboard (Phoenix LiveView) on the web endpoint (`:4000`). Architecture:
`Fathom.Admin.MetricsCollector` (per-node realtime → PubSub) + `Fathom.Admin.Fleet` (Postgres
roll-ups, `assign_async`) + the in-process Prometheus reporter (`Fathom.Telemetry`, read by the
collector and exposed at `GET /admin/metrics`). Gated by `:metrics_collector` (default on; off in
test). Per-shard data is the `Fathom.ShardLoad` read-API; fleet counts are Postgres.

## Running it

```bash
SHARD_LOAD=true iex -S mix phx.server      # dev: metrics_collector + Prometheus reporter start
```

Open `http://localhost:4000/admin` — dev BasicAuth is `admin` / `admin` (`config/dev.exs`). For live
per-shard traffic (the QPS/latency charts + hot-shards table), point a libSQL client at `:8080` or
run `mix fathom.scale --hotspots`. Without `SHARD_LOAD=true` the per-shard panels stay empty (the
node-aggregate metrics still flow).

**Prod:** set `ADMIN_USER` / `ADMIN_PASS`. The `admin_auth` plug **fails closed (503)** when unset,
so the surface is never anonymously reachable; boot only *warns* (a missing dashboard credential
must not take the data plane down). `GET /admin/metrics` (same auth) is the Prometheus scrape target;
for a merged fleet view scrape every node.

## Gotchas

### `mix tailwind` / `mix assets.build` exits with 137 (SIGKILL) on macOS

Symptom: `** (Mix) mix tailwind fathom exited with 137`, and the Tailwind binary is killed even on
`--help`. On recent macOS (Sequoia/Tahoe) the `tailwind` standalone binary's **ad-hoc code signature
is rejected and the process is SIGKILLed on launch** — no page CSS ever builds. Not a config or CSS
problem.

Fix — re-sign the binary in place:

```bash
codesign --force --sign - "$(ls _build/tailwind-macos-arm64-* | head -1)"
mix tailwind fathom     # now builds priv/static/assets/css/app.css
```

The signature is repaired on disk, so the dev `tailwind --watch` and CI `assets.deploy` work
afterward. If `esbuild` is ever killed the same way, re-sign `_build/esbuild-*` identically. (The
esbuild bundle includes the vendored `assets/vendor/uplot/uPlot.esm.js`, ~437 KB — expected.)

### Every route returns 503 in dev with a `Phoenix.Ecto.PendingMigrationError`

The dev-only pending-migration guard intercepts **all** requests (so `/admin` looks like an
auth/routing failure, but it isn't). Run `mix ecto.migrate`. The admin dashboard adds **no**
migrations of its own — it's ETS + reuses existing Postgres tables — so a pending migration here is
always pre-existing.

## Enabling / disabling

- `:metrics_collector` (default `true`; `false` in test) — the single gate for the collector, the
  Prometheus reporter, and the per-shard flush-watermark writes. Turn off to remove the whole admin
  observability layer.
- Full-fidelity data also needs `SHARD_LOAD=true` (per-shard TPS/hot-set) and, for the fleet
  hot-set / per-node load split, `LOAD_REPORTER=true` (writes `shard_load_samples`).
