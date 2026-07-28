# Configuration reference — fathom environment variables

Every environment variable fathom reads at boot (`config/runtime.exs`), grouped, with its default
and — the column that matters for a multi-tenant data plane — its **safety consequence** if set
wrong. A drift test (`test/fathom/configuration_doc_test.exs`) fails CI if a new `System.get_env`
knob is added to `runtime.exs` without a row here, so this list stays complete.

Boot guards catch several dangerous misconfigs (`Fathom.Application` refuses to start on them) —
those are called out. When in doubt, the eval stack (`deploy/compose/`) is a known-good starting
config; diff from it.

> **Safety-critical, read these first:** `SECRET_KEY_BASE`, `SHARD_BASE_DOMAIN`, `HRANA_AUTH`,
> `HRANA_BIND_IP`, `MAX_OPEN_SHARDS`, `NOVEL_SHARD_RATE`, `ADMIN_USER`/`ADMIN_PASS`. Getting these
> wrong is how you commingle tenants, expose the data path, or take a tenant down.

## Core (required in prod)

| Var | Default | What it does | Safety consequence |
|---|---|---|---|
| `DATABASE_URL` | — (**required in prod**, boot raises) | Postgres directory / control-plane connection (`ecto://user:pass@host/db`). | No directory ⇒ no migrations/rebalancer/lifecycle. The data path survives a Postgres outage (fails open), but boot requires it. |
| `SECRET_KEY_BASE` | — (**required in prod**, boot raises) | Signs cookies + (by default) Hrana tokens. Generate with `mix phx.gen.secret`. | A shared/leaked value lets anyone forge admin sessions and per-shard tokens. Never commit it. |
| `PHX_HOST` | `example.com` | Public host used to build URLs. | Cosmetic for the data path; set it so generated links/redirects are correct. |
| `PHX_SERVER` | unset (release doesn't serve) | Set to start the Phoenix/web endpoint from a release. | Without it a release boots but serves no web/dashboard. The eval stack sets it. |
| `PORT` | `4000` | Web/dashboard + control-plane API listener port. | — |

## Routing & tenant isolation (safety-critical)

| Var | Default | What it does | Safety consequence |
|---|---|---|---|
| `SHARD_BASE_DOMAIN` | unset (unanchored) | Anchors Host-subdomain routing to a zone: only `<shard>.<zone>` selects a shard; any other Host fails closed. | **The isolation anchor.** Unset = any attacker-controlled Host first-label becomes a shard id. A *blank* value is treated as unset (a blank zone would deny all routing). Boot refuses an exposed data plane with this unset unless `ALLOW_UNANCHORED_ROUTING`. |
| `ALLOW_UNANCHORED_ROUTING` | unset | Explicit ack to run the data plane WITHOUT `SHARD_BASE_DOMAIN`. | Only for a deliberately unanchored deploy. Leaving routing unanchored on an exposed port is a cross-tenant hazard. |

## Storage (shard bottomless backend)

| Var | Default | What it does | Safety consequence |
|---|---|---|---|
| `SHARD_STORAGE` | Local (`System.tmp_dir!`) | `s3` selects the S3 backend (+ reads the S3 vars); `local` a filesystem store. | `local` on a multi-node fleet has no shared durability/lease store — use `s3` (or S3-compatible) in any real cluster. |
| `S3_BUCKET` | — (**required when `SHARD_STORAGE=s3`**) | The bucket holding every shard object + lease/heartbeat. | The single most important object store; loss/compromise affects every tenant (harden per `docs/durability.md`). |
| `S3_REGION` | `us-east-1` | S3 region. | — |
| `S3_ENDPOINT` | unset (AWS) | Override for S3-compatible stores (MinIO/R2/Tigris). | — |
| `S3_PATH_STYLE` | `false` | Path-style addressing (needed by MinIO/R2). | — |
| `S3_PREFIX` | `""` | Key prefix for all shard objects. | Use to share a bucket; a wrong prefix silently reads/writes the wrong keyspace. |
| `S3_POOL_SIZE` | `200` | Finch connections per pool for the S3 backend. | The default is the measured knee on the LOCALHOST MinIO rig, not a real-S3 number — tune per deployment. Too small serializes cold-open/flush; too large can trip store-side connection limits. |
| `S3_POOL_COUNT` | `1` | Number of Finch pools. | Only shows a win against real S3 (localhost MinIO saturates first). |
| `S3_CONN_MAX_IDLE_MS` | `15000` | Retire an idle pooled connection after this long. | **Must stay under the store's idle close (~20s on S3)** so fathom is the closing side. Higher (or Finch's `:infinity` default) puts a stale-connection race on the flush PUT, which nothing retries — a raced `:closed` aborts the idle drop and extends that tenant's RPO window. |
| `AWS_ACCESS_KEY_ID` | unset | S3 credential. | Use least-privilege creds scoped to the bucket/prefix (`docs/durability.md`). |
| `AWS_SECRET_ACCESS_KEY` | unset | S3 credential. | As above; keep out of images/logs. |
| `AWS_SESSION_TOKEN` | unset | Optional STS session token. | — |
| `VERIFY_STORAGE_FENCE` | `true` | Boot probe that the store enforces conditional writes (the fence). Set `false` to skip. | **Never `false` in prod.** The fence is what makes single-writer safe; skipping the probe on a store that doesn't enforce `If-*` risks split-brain. |

## Shard data plane

| Var | Default | What it does | Safety consequence |
|---|---|---|---|
| `SHARD_DATA_DIR` | `System.tmp_dir!/fathom_shards` | Local working copy of every open shard. | Point at fast local disk; size for `MAX_OPEN_SHARDS × shard size` + warm-cache headroom (see `docs/runbooks/operations.md` disk-full). |
| `SHARD_LEASE_TTL_MS` | `30000` | Node heartbeat / lease TTL. Bounds how long a dead node's shards stay unstealable (the failover-stall ceiling). | Too low → false steals under load/skew; too high → slower failover. Pair with clock discipline (NTP). |
| `SHARD_FLUSH_INTERVAL_MS` | code default | Periodic durability-flush cadence. | Directly sets the RPO floor: a node lost between flushes loses up to this window of writes. |
| `SHARD_IDLE_MS` | code default | Idle threshold before a shard flush+drop+stops. | Lower = more S3 churn; higher = more resident shards. |
| `SHARD_OBJECT_ENCODING` | `none` | Compression for stored shard objects (`none` \| `zlib`). S3 backend; `Local` stores raw either way. | **Decoding is always on regardless of this value**, so the flag rolls forward *and back* without a flag day and mixed-version nodes interoperate — this only controls what a node *writes*. An object carries an `x-amz-meta-fathom-enc` marker and a node that can't decode it **fails the pull closed** rather than handing SQLite bytes that aren't a database. Integrity metadata stays the *uncompressed* hash, so an object's identity doesn't depend on how it was stored. **Does not help single-shard cold-open** (RTT-bound, body ~free); it pays on aggregate-bandwidth-bound work — mass warming/failover, write-hot PUT volume, cross-region, storage cost — and spends CPU, which is the contended resource on a loaded node. Measure `warm_s3_shards_per_s` under `S3_FAKE_RATE_KBPS` before enabling fleet-wide. |
| `SHARD_MAX_PAGE_COUNT` | `1048576` (≈4 GiB) | Per-shard `PRAGMA max_page_count` cap (pages; size = pages × 4096B). `0` opts out. | Enforces "limited dataset per shard": a write past it fails `SQLITE_FULL`, so one runaway tenant can't inflate flush/cold-open/standby cost fleet-wide. **The default exists to keep the brake at WRITE time.** Past the 5 GiB S3 single-PUT ceiling a shard keeps *acknowledging* writes it can never upload — permanently dirty, retrying forever, RPO unbounded, snapshot/fork/retain disabled, no operator remedy. `SQLITE_FULL` is never acked, so a write-time cap cannot lose data; a flush-time one already has. Raising this above ~5 GiB re-opens that cliff. Already-oversized shards are not bricked — SQLite won't set the cap below a db's current size, so they keep serving and merely stop growing. |
| `MIGRATE_ON_TOUCH` | `off` | How a checkout handles a shard behind the fleet HEAD after a release: `off` (reconcile cron converges it), `async` (enqueue the migration on touch, serve vN-1 this request), `inline` (block on the full blue/green migration). | `off`/`async` serve the old schema briefly (safe under expand-contract); `inline` adds multi-second first-request latency. See `docs/quickstart-django.md`. |
| `RECONCILE_BATCH_SIZE` | `100` | How many laggard shards the hourly `ReconcileJob` rollout sweep enqueues per run. | At fleet scale a deep cold tail converges glacially at 100/run (2,400/day); raise it to size convergence to your fleet + S3 pool headroom. Per-shard uniqueness keeps a bigger bite idempotent. |

## Auth (Hrana data path)

| Var | Default | What it does | Safety consequence |
|---|---|---|---|
| `HRANA_AUTH` | `disabled` | `required` makes every stream present a per-shard token; `disabled` trusts the network. Unknown values fail closed to required. | With `disabled`, the port MUST be reachable only via the LB (firewall/SG/private subnet + `HRANA_BIND_IP`). A reachable, unauthenticated `:8080` is open tenant access. Boot refuses `required` without a usable secret. |
| `HRANA_TOKEN_SECRET` | falls back to `SECRET_KEY_BASE` | Dedicated token-signing secret, so a data-path secret rotation doesn't touch web sessions/CSRF. | Rotate independently; keep secret. |
| `HRANA_TOKEN_MAX_AGE` | unset (tokens don't expire) | Optional token expiry (seconds). | Unset ⇒ revoke only by rotation; a boot warning fires when `required` runs with infinite max-age. |
| `HRANA_BIND_IP` | unset (all interfaces) | Pins the Hrana listener to the private interface the LB reaches. | Defense-in-depth for the `disabled`-auth posture; unset relies on network isolation alone. |

## Admission & limits (safety-critical)

| Var | Default | What it does | Safety consequence |
|---|---|---|---|
| `MAX_OPEN_SHARDS` | conservative finite (config.exs) | Per-node cap on open shards (the fd-cliff protection). At the cap, the soft default evicts an idle shard; a hard cap 503s. | Tune to the node's measured fd/RSS density (`mix fathom.scale --ramp`). Too high → fd exhaustion; too low → needless evictions/503s. |
| `NOVEL_SHARD_RATE` | unset (off) | Rate limit (grants/sec) on minting brand-NEW shard ids. | Bounds shard-minting abuse on an exposed data path. Its directory check **fails open** on a Postgres outage — don't rely on it as the only defense (`docs/runbooks/operations.md`). |
| `NOVEL_SHARD_BURST` | code default | Token-bucket burst for the novel limiter. | — |
| `FORK_FROM_TEMPLATE` | unset (born empty) | Birth novel shards at the fleet HEAD from the retained `template@HEAD` snapshot. | Only enable with a template + snapshot in place; a poisonable template reachable anonymously is a fleet-wide vector (never make `:default_shard` the template). |
| `WILDCARD_TLS_SERVING` | unset (warn only) | Set when the deployment terminates TLS with a `*.<zone>` wildcard cert. Then `Tenants.provision`/`fork` REFUSE a non-DNS-safe id (underscore, >63 chars, or leading/trailing hyphen) with 422 instead of returning an un-servable URL (review #35). | Off, the API still provisions such ids but returns a `warnings` field — an underscore id can't be served under wildcard TLS (RFC 6125), only on the plaintext path or a per-name cert. Prefer hyphenated ids. |

## Web / dashboard / API

| Var | Default | What it does | Safety consequence |
|---|---|---|---|
| `ADMIN_USER` | dev default `admin` | BasicAuth user for `/admin`, `/admin/metrics`, and `/api/tenants`. | The control-plane guard; the plug fails closed (503) if unset in prod, so the surface is never anonymous. Set a real value before exposing `:4000`. |
| `ADMIN_PASS` | dev default `admin` | BasicAuth password for the above. | Change from `admin`. It gates tenant provisioning/deletion and the metrics scrape. |
| `WEB_BIND_IP` | per-env (dev loopback) | Bind IP for the web endpoint (e.g. `0.0.0.0` for LAN). | On `0.0.0.0` the dashboard is reachable on every interface with the BasicAuth creds — set real creds first. |
| `WEB_INSECURE_LOCAL` | unset (SSL forced) | **Build-time** flag (compile-time in `config/prod.exs`): turns off `force_ssl` + the LiveView WS origin check for a plaintext LAN/localhost dashboard. | **Never build a real deployment with it.** Eval/chaos only. It's a Docker build-arg, not a runtime env. |
| `ADMIN_AUTH_MAX_FAILURES` | `20` (prod) / off (dev/test) | Lock out a source IP with 429 after this many failed admin BasicAuth attempts within the window (`Fathom.RateLimiter`, review #34). `0` disables. | Brute-force protection for the one shared admin password — the Hrana token path is HMAC-verified and not in scope. Per-IP, so behind a proxy it caps failures per proxy IP; keep generous enough not to lock out a fat-fingered operator. |
| `ADMIN_AUTH_WINDOW_MS` | `300000` (5 min) | The sliding window over which failed admin auths are counted. | Larger = stricter (failures accumulate longer). |
| `API_RATE_LIMIT` | `120` (prod) / off (dev/test) | Per-source-IP request cap on the `/api` control plane per window (review #34); over budget → 429 before auth runs. `0` disables. | Stops a hostile/buggy client hammering expensive ops (list/export/fork). `NovelLimiter` only protects novel *data-path* minting, not `/api`. Size above legitimate operator/CI burst. |
| `API_RATE_WINDOW_MS` | `60000` (1 min) | The window for `API_RATE_LIMIT`. | — |

## Observability

| Var | Default | What it does | Safety consequence |
|---|---|---|---|
| `OTEL_EXPORTER_OTLP_ENDPOINT` | unset (no-op) | Enables OpenTelemetry OTLP trace export (also honors the standard `OTEL_EXPORTER_OTLP_*` vars). | Off by default; traces contain shard ids — send to a trusted collector. |
| `SHARD_LOAD` | off | Per-shard load counters (`Fathom.ShardLoad`) — the rebalancer / hot-spot input. | Off so the hot path doesn't pay for an unread counter; turn on where a rebalancer or `--hotspots` reads it. |

## Cluster / node

| Var | Default | What it does | Safety consequence |
|---|---|---|---|
| `NODE_KEY` | `node()` | Stable per-node key (heartbeat object, LB backend reference, load samples). | **Must be unique per node.** A collision makes two nodes share one heartbeat and corrupts liveness (`docs/runbooks/operations.md` heartbeat). |
| `DNS_CLUSTER_QUERY` | unset | DNS-based BEAM clustering query (not required — fathom coordinates via S3, not BEAM). | — |
| `ECTO_IPV6` | unset | Use IPv6 for the Postgres socket. | — |
| `HRANA_STREAM_IDLE_MS` | `30000` | Per-stream Hrana inactivity timeout. | Bounds CLIENT THINK TIME inside an open transaction: expiring a stream mid-transaction discards acked work as an opaque `STREAM_NOT_FOUND`. Filo's own default is 10s, which a Django `BEGIN; SELECT; <app logic>; UPDATE; COMMIT` can exceed under load. Each held stream costs 1 checkout + ~3 fds; `:max_checkouts_per_shard` caps per-tenant exposure. **Do not set a never-expiring value.** |
| `HRANA_LISTEN_SOCKETS` | `4` | How many listen sockets the Hrana listener binds (`reuseport` multi-queue). | Spreads accepts over N kernel accept queues instead of contending 100 acceptors on one. **Set `1` if the node fails to boot** — ThousandIsland aborts startup when the platform refuses `reuseport`, and `1` takes the single-socket path with the option dropped. `backlog` is silently clamped to the OS `net.core.somaxconn`, so raising that is a separate node-provisioning step. |
| `HRANA_GC_EVERY_N` | `5` (Bandit's default) | How often Bandit forces a full GC on an HTTP/1 connection process, in requests. | Bandit calls this option experimental. Raising it means fewer forced sweeps but more garbage held per connection — **multiplied by held connections per node (30k measured)**. Don't move it without an A/B on both sides: `chaos.sh tpc-fleet` for throughput and `chaos.sh served` for RSS/shard, which must not move. |
| `HRANA_BACKLOG` | `4096` | Kernel accept-queue depth per Hrana listen socket. | The other half of the accept-path fix (with `HRANA_LISTEN_SOCKETS`). **Silently clamped to the OS `net.core.somaxconn`** — setting 4096 on a box with `somaxconn=1024` gets you 1024 and no warning. |
| `HRANA_STREAM_HIBERNATE_MS` | `5000` | Idle time before a stream process hibernates. | Reclaims the heap a long-lived WebSocket stream would otherwise hold between requests. Raise it if a burst-heavy workload pays more in hibernate/wake than the heap is worth. |
| `POOL_SIZE` | `25` | Postgres connection pool size. | Sized against a demand floor of ~21 from Oban's `queues:` alone (each executing job holds a connection), plus the endpoint, the pollers, and the near-hot-path `HranaAuth.Revocations` reads. **Raising `queues:` in `config/config.exs` means raising this.** Too small starves the control plane. |
| `POOL_QUEUE_TARGET_MS` | `50` | DBConnection queue target. | With `POOL_QUEUE_INTERVAL_MS`, bounds how long a checkout waits before the pool starts DROPPING them. A dropped checkout on the stream-open path pins a Hrana stream. |
| `POOL_QUEUE_INTERVAL_MS` | `1000` | DBConnection queue interval. | See `POOL_QUEUE_TARGET_MS`. |

## Rebalancer (Phase-2 B1 — all off by default)

Enable only per the staged runbook (`docs/runbooks/rebalancer.md`); the pin decision trusts a
tenant-controllable signal, so it presumes the Hrana trust boundary is enforced.

| Var | Default | What it does | Safety consequence |
|---|---|---|---|
| `REBALANCER_ENABLED` | off | Runs the (singleton) rebalance control loop. | Do NOT enable on a data path open to untrusted callers (a tenant could drive a shard hot to induce a handoff blip). |
| `LOAD_REPORTER` | off | Publishes this node's hot set to Postgres (needs `SHARD_LOAD`). | — |
| `COMMAND_POLLER` | off | Acts on handoff warm/drain commands addressed to this node. | — |
| `LB_BACKENDS` | unset | The LB backend set (`node=addr,...`) the policy picks targets from. | Must match the real LB membership, or handoffs target a wrong/missing node. |
| `LB_MAP_PATH` | unset | Where the rebalancer writes the rendered nginx exception map. | Must be the file the LB includes; a mismatch means pins never apply. |
| `LB_RELOAD_CMD` | unset | How to reload the LB after a map change (e.g. `nginx -s reload`). | Unset ⇒ the map is written but reload is out-of-band. |
| `LB_TEST_CMD` | unset | Config test run against a candidate map before promotion; non-zero exit aborts the promotion. | Guards against pushing a broken LB config. |
| `LB_TEST_TIMEOUT_MS` | `10000` | Deadline for the LB config-test command. | A hung command is killed so it can't hold the fleet lock. |
| `LB_RELOAD_TIMEOUT_MS` | `10000` | Deadline for the LB reload command. | As above. |
| `REBALANCE_HOT_QPS_FLOOR` | unset (p99-relative) | Absolute q/s hot-detection floor; boot raises on an unusable value. | A mis-set floor silently disables the rebalancer — the boot guard catches unusable values. |
| `LOAD_REPORT_INTERVAL_MS` | code default | How often a node reports its load. | — |
| `REBALANCE_CONFIRM_WINDOWS` | code default | Windows a hotspot must persist before a move (anti-flap). | — |
| `REBALANCE_COOLDOWN_MS` | code default | Cooldown between moves. | — |

## Warm standby (Phase-2 A1 — off by default)

| Var | Default | What it does | Safety consequence |
|---|---|---|---|
| `WARM_FOLLOWER` | off | This node pre-pulls recently-active foreign shards (no lease, never serves) for faster failover. | Disk-bound; a large warm cache can fill `WARM_CACHE_DIR` (`docs/runbooks/operations.md` disk-full). |
| `WARM_CACHE_DIR` | code default | Separate cache dir for warm copies. | Size independently of `SHARD_DATA_DIR`. |
| `WARM_POLL_MS` | code default | How often the follower refreshes its warm set. | — |
| `WARM_HOME_RETENTION_MS` | code default | How long after last owning a shard the follower still treats it as "home" and won't re-warm. | Outlast a routine idle→reopen gap; a real LB remap lapses it. |
| `WARM_MIN_REPULL_MS` | 10 × `WARM_POLL_MS` | Floor on how often ONE cached shard's body may be re-transferred. | **This is what bounds the follower's steady-state cost.** A continuously-written tenant flushes faster than the poll, so without it every refresh is a full body + fsync, forever. Worst-case ingress is Σ(cached sizes) ÷ this. Raising it trades failover RTO on write-hot shards for bandwidth and device writes — never correctness, since promotion revalidates before serving. |
| `WARM_REFRESH_BYTES_PER_S` | unset (uncapped) | Hard cap on warm-refresh ingress, spent lag-first. | Bounds the aggregate independently of cache size — set it to what the node's NIC/disk can spare. Too small doesn't break the cache, it just converges it more slowly (oldest-checked shard first, so the tail is never starved). |

## Development tooling

| Variable | Default | What it does | Notes |
|---|---|---|---|
| `FATHOM_BENCH_LOCK` | `/tmp/fathom_bench.lock` | Path to the host-wide lock `mix fathom.bench` takes for a run, so no benchmark measures under another's load. | Dev-only; nothing in the server reads it. Point co-tenant projects sharing a host at the **same** path to interlock their benchmarks — that's why it's a variable rather than a constant. See [`benchmark-plan.md`](benchmark-plan.md). |
