defmodule Fathom.BootChecks do
  @moduledoc false
  # Boot-time configuration guards. Each reads only application env, the environment and the
  # filesystem — none touches the supervision tree — so they live apart from `Fathom.Application`,
  # which calls `run!/0` once before building the tree. Public guards stay public so tests can call
  # them (and their pure `*_problem`/`*_verdict`/`*_warnings` cores) directly.

  require Logger

  # Run every guard, in boot order. The fail-closed guards raise (refusing to boot); the warn-only
  # ones log and return.
  @spec run!() :: :ok
  def run! do
    check_template_default!()
    check_template_auth!()
    check_udf_extension!()
    Fathom.HranaAuth.check_config!()
    check_storage_fence!()
    check_rebalancer_config!()
    check_local_storage_fleet!()
    check_shard_base_domain!()
    check_shard_override!()
    check_default_shard!()
    check_hrana_exposure!()
    check_replication_exposure!()
    check_replication_frame_auth!()
    check_replication_disk!()
    check_sqlite_temp_dir!()
    check_vm_dirty_background!()
    check_replication_flush_interval!()
    check_skew_defer_to_steal_watch!()
    check_flush_interval!()
    check_pool_fd_budget!()
    check_soft_heap_budget!()
    check_s3_static_credentials!()
    check_throttle_proxy!()
    check_admin_credentials!()
    check_repo_pool_headroom!()

    :ok
  end

  # Below this, a replicating fleet is re-shipping whole WALs (see
  # `check_replication_flush_interval!/0`). 30 s is the value the 2026-08-18 A/B measured as clean
  # at 512 tenants, not a round number.
  @replication_flush_floor_ms 30_000

  # Finding #17: template-shard capture replays a shard's SQL fleet-wide. If prod's fallback default
  # shard equals the capture template, anonymous default traffic would drive fleet-wide capture. #26
  # already makes the prod default nil (fail closed), so this only bites an explicit prod
  # misconfiguration — refuse to boot with it. Dev intentionally couples them ("demo"), so the guard
  # is prod-only. Compares normalized ids (#19), so a mixed-case coupling is still caught.
  @doc false
  def check_template_default! do
    if Application.get_env(:fathom, :env) == :prod do
      template = normalize_id(Application.get_env(:fathom, :template_shard_id))
      default = normalize_id(Application.get_env(:fathom, :default_shard))

      if not is_nil(template) and template == default do
        raise "config error: :default_shard (#{inspect(default)}) equals :template_shard_id — " <>
                "anonymous default traffic would drive fleet-wide template capture (finding #17). " <>
                "Set :default_shard to a non-template shard, or leave it unset to fail closed."
      end
    end
  end

  # Review 2026-07-09 #1: the rebalancer keys its exception table AND the dead-node reconciler
  # on `node_key`, which must equal one of the `:lb_backends` keys. If NODE_KEY drifts from the
  # lb_backends key for this node, NO `pinned_node` is ever in the reconciler's `alive` set, so
  # every pin looks dead and the fleet is unpinned every tick — a silent total defeat. Fail
  # closed at boot instead. Prod-only + only when :lb_backends is configured (the rebalancer
  # fleet posture); dev/test leave it empty so the guard is inert.
  @doc false
  def check_rebalancer_config! do
    backends = Application.get_env(:fathom, :lb_backends, %{})
    node_key = Fathom.Rebalancer.node_key()

    if Application.get_env(:fathom, :env) == :prod and map_size(backends) > 0 and
         not Map.has_key?(backends, node_key) do
      raise "config error: NODE_KEY (#{inspect(node_key)}) is not a key of :lb_backends " <>
              "(#{inspect(Map.keys(backends))}). The rebalancer keys the exception table + the " <>
              "dead-node reconciler on node_key = an lb_backends key; a mismatch makes every pin " <>
              "look dead and unpins the whole fleet each tick (review 2026-07-09 #1). Set NODE_KEY " <>
              "to this node's lb_backends key."
    end

    # Expert review 2026-11-07 #57: a rebalancer that cannot write the LB map cannot move anything.
    # `HandoffJob` refuses its handoffs on such a node (they would drain a shard the LB still routes
    # here), and the `rebalance` Oban queue runs on EVERY node, so one node without LB_MAP_PATH
    # picks up jobs it can only refuse. A warning, not a refusal to boot: decision-plane-only
    # operation (the override table and the decision log without any LB wiring) is documented.
    if Application.get_env(:fathom, :env) == :prod and
         Application.get_env(:fathom, :rebalancer_enabled, false) == true and
         not Fathom.Rebalancer.LbApply.map_configured?() do
      Logger.warning(
        "config warning: REBALANCER_ENABLED is on but LB_MAP_PATH is unset on this node. Its " <>
          "handoff jobs will be refused (a flip that writes no map would only drain the shard " <>
          "the LB still routes here); set LB_MAP_PATH on every node that runs the rebalance queue."
      )
    end
  end

  # Expert review #9: template-shard capture replays a shard's SQL fleet-wide, and
  # AGENTS.md forbids a prod :template_shard_id without auth on that shard — but only
  # the default≠template guard above was enforced. With auth :disabled the template
  # shard is reachable via its ordinary Host subdomain, and one anonymous poisoned
  # capture is later replayed verbatim onto every tenant's database. Refuse to boot
  # instead of trusting the network alone. Only the exact :disabled mode is open
  # (any other value fails closed to :required — see Fathom.HranaAuth).
  @doc false
  def check_template_auth! do
    if Application.get_env(:fathom, :env) == :prod and
         not is_nil(Application.get_env(:fathom, :template_shard_id)) and
         Application.get_env(:fathom, :hrana_auth, :disabled) == :disabled do
      raise "config error: :template_shard_id is set in prod with :hrana_auth disabled — " <>
              "an anonymously reachable template shard is a fleet-wide poisoning vector " <>
              "(a captured migration is replayed onto every shard; expert review #9). " <>
              "Set HRANA_AUTH=required or unset the template."
    end
  end

  # Expert review 2026-10-10 #8: a prod node whose `fathom_udf` artifact is missing (an image built
  # without cargo — `compile.fathom_udf` SKIPS rather than fails) silently lost the engine PRAGMA
  # authorizer, the on-thread deadline backstop and Django's UDFs, falling back to exqlite's
  # ATTACH-only authorizer. Refuse to boot instead, unless the operator acknowledges the downgrade
  # with ALLOW_NO_UDF=true (`:allow_no_udf`). Also trips on an explicit `:sqlite_extension false`,
  # which is the same downgrade chosen on purpose.
  #
  # Expert review 2026-11-07 #24: with replication ON the extension is not an optional degradation,
  # so `ALLOW_NO_UDF` does NOT cover it. Without the extension a tenant handle is `:skipped`:
  # SQLite's built-in `wal_autocheckpoint=4000` owns the WAL-hook slot, there is no registry and no
  # `fathom_wal_replicated`, and `Session.ensure_registered/2` latches `registered: :unavailable`
  # and keeps shipping and ACKING. A commit crossing 4000 frames then runs the built-in PASSIVE
  # checkpoint inside the commit and the next writer restarts the log before the Session reads the
  # previous commit's frames — a quorum-acked write on no replica (the 2026-10-15 #1 window; see the
  # "extension NOT loaded" note in `replication/session.ex`). The remedy is not an ack but a choice:
  # build the extension, or turn replication off. Replication is on by default in prod, so this is
  # the default posture of an image built without cargo.
  @doc false
  def check_udf_extension! do
    case udf_extension_problem(%{
           env: Application.get_env(:fathom, :env),
           extension: udf_extension_status(),
           allow_no_udf: Application.get_env(:fathom, :allow_no_udf, false),
           replication_enabled: Fathom.Shard.Replication.Fleet.replicating?()
         }) do
      nil -> nil
      message -> raise message
    end
  end

  # The extension's state as the boot check sees it: `:missing` (no artifact / disabled), `:ok`, or
  # `{:error, reason}` when an artifact is configured but does NOT load. Existence alone proved
  # nothing (expert review 2026-11-07 #42): a wrong-arch `.so`, a corrupt file, an exqlite whose
  # bundled SQLite is older than the crate's floor, or `SQLITE_EXTENSION=""` all pass `File.exists?`
  # and then failed EVERY tenant open while `/health` kept the node in the LB. So prod arms the
  # auto-extension here, through `Extension.open_mode/0` — the same call the first tenant open
  # would make, and safe at boot because `Application.start/2` runs before any tenant connects.
  # Only called in prod: dev/test arm lazily on first open, as before.
  defp udf_extension_status do
    cond do
      Application.get_env(:fathom, :env) != :prod ->
        :ok

      not Fathom.Shard.Extension.available?() ->
        :missing

      true ->
        case Fathom.Shard.Extension.open_mode() do
          {:error, reason} -> {:error, reason}
          _armed -> :ok
        end
    end
  end

  @doc """
  Why a node with this config must not boot given the state of the `fathom_udf` extension, or `nil`.

  Pure over the passed map (keys: `:env`, `:extension` — `:ok`, `:missing`, or `{:error, reason}`
  for an artifact that is present but does not load, `:allow_no_udf`, `:replication_enabled`), so a
  test needs no global app env.
  """
  @spec udf_extension_problem(%{
          env: atom(),
          extension: :ok | :missing | {:error, term()},
          allow_no_udf: boolean(),
          replication_enabled: boolean()
        }) :: String.t() | nil
  # A present-but-unloadable artifact is refused whatever `ALLOW_NO_UDF` says. The ack means "I
  # accept running WITHOUT the extension", but a configured artifact that fails to load is not that
  # state: `Connection.open/2` fails every tenant open with it, so acknowledging it would boot a
  # node that serves nothing. The way to run deliberately without the extension is to say so.
  def udf_extension_problem(%{env: :prod, extension: {:error, reason}}) do
    "config error: the fathom_udf SQLite extension is configured but does not load on this " <>
      "prod node (#{inspect(reason)}). Every tenant open would fail while /health stayed green " <>
      "(expert review 2026-11-07 #42) — typical causes are a wrong-architecture or corrupt " <>
      "artifact, an empty SQLITE_EXTENSION, or an exqlite whose bundled SQLite is older than the " <>
      "extension needs. Fix or rebuild the artifact. To run WITHOUT the extension on purpose, set " <>
      "SQLITE_EXTENSION=false and ALLOW_NO_UDF=true (refused while replication is on: " <>
      "REPLICATION_ENABLED=false)."
  end

  def udf_extension_problem(%{env: :prod, extension: :missing} = cfg) do
    cond do
      cfg.replication_enabled == true ->
        "config error: the fathom_udf SQLite extension is not available on this prod node " <>
          "(artifact missing — was the image built without cargo? — or :sqlite_extension " <>
          "disabled) AND replication is ON. Without the extension SQLite's built-in " <>
          "autocheckpoint runs inside the commit and can restart the WAL before the A2 " <>
          "replicator reads it, so a write acknowledged by a quorum can be on no replica " <>
          "(expert review 2026-11-07 #24). ALLOW_NO_UDF does not cover this. Build the " <>
          "extension, or set REPLICATION_ENABLED=false to run without replication."

      cfg.allow_no_udf == true ->
        nil

      true ->
        "config error: the fathom_udf SQLite extension is not available on this prod node " <>
          "(artifact missing — was the image built without cargo? — or :sqlite_extension " <>
          "disabled). Without it tenant handles lose the engine PRAGMA authorizer, the " <>
          "statement-deadline backstop, the SQLite size limits and Django's UDFs, and the " <>
          "only block on a tenant's fts3_tokenizer() calls is the executor's text gate. " <>
          "Build the extension, or set ALLOW_NO_UDF=true to boot degraded on purpose " <>
          "(only possible with replication off: REPLICATION_ENABLED=false)."
    end
  end

  def udf_extension_problem(_cfg), do: nil

  # Expert review #16: when the S3 backend is configured, verify at boot that the
  # store actually ENFORCES conditional writes — every safety property (lease mutual
  # exclusion, flush fence, conditional release) rides on 412-on-failed-If-Match, and
  # a store that ignores the header yields silent, error-free split-brain. Runs the
  # probe before the tree starts so a lax store never serves a byte. Opt out
  # explicitly with `config :fathom, :verify_storage_fence, false` (e.g. a rig where
  # boot-time storage reachability isn't guaranteed and the store is known-good).
  defp check_storage_fence! do
    if Application.get_env(:fathom, :shard_storage) == Fathom.Shard.Storage.S3 and
         Application.get_env(:fathom, :verify_storage_fence, true) do
      Fathom.Shard.Storage.S3.verify_conditional_writes!()
      # Expert review 2026-10-20 #29: crc32 transit checksum on compressed PUTs iff enforced.
      Fathom.Shard.Storage.S3.probe_body_checksum!()
    end
  end

  # Expert review 2026-07-14 #3 / 2026-11-07 #8: Fathom.Shard.Storage.Local's lease is node-local
  # only — its :global.trans is scoped to [node()] (storage/local.ex), so it provides NO
  # cross-node single-writer guarantee. Run on a fleet and two nodes can each believe they hold a
  # shard's lease and both write: silent, error-free split-brain at the first LB remap. Refuse to
  # boot. Prod-only.
  #
  # What counts as "a fleet" is the 2026-11-07 #8 fix. The 2026-07-14 guard keyed ONLY on
  # `:lb_backends` (the rebalancer's topology, which a fleet that has not enabled the rebalancer
  # never sets) and compared `:shard_storage` against Local with `get_env/2` WITHOUT a default —
  # but an unset `SHARD_STORAGE` is `nil` there while `Storage.backend/0` defaults it to Local, so
  # the DEFAULT, unsafe store was the one case the guard could not see. Now: storage defaults to
  # Local exactly as `Storage.backend/0` does, and a fleet is any of `:lb_backends` non-empty,
  # `:replication_followers` non-empty, or `:replication_enabled == true` (replication is ON by
  # default in prod and, without followers, refuses to boot in `Fleet`, so on a prod node that
  # signal means "this node replicates to others").
  #
  # The ack (`ALLOW_LOCAL_STORAGE=true`, `:allow_local_storage`) is deliberately narrower than the
  # old guard's reach: it overrides ONLY the replication signal (e.g. a primary with hot-spare
  # followers behind an orchestrator that guarantees one active writer). `:lb_backends` — an
  # explicit multi-node ACTIVE topology — stays refused unconditionally, as it always was; letting
  # an ack loosen a refusal that used to be absolute would be a regression, not a fix.
  # A single-node prod Local deploy sets REPLICATION_ENABLED=false (it had to already, or the
  # replication guards refuse it) and needs no ack.
  @doc false
  @spec check_local_storage_fleet!() :: nil
  def check_local_storage_fleet! do
    case local_storage_fleet_problem(%{
           env: Application.get_env(:fathom, :env),
           storage: Application.get_env(:fathom, :shard_storage, Fathom.Shard.Storage.Local),
           lb_backends: Application.get_env(:fathom, :lb_backends, %{}),
           replication_followers: Application.get_env(:fathom, :replication_followers, []),
           replication_enabled: Fathom.Shard.Replication.Fleet.replicating?(),
           allow_local_storage: Application.get_env(:fathom, :allow_local_storage, false)
         }) do
      nil -> nil
      message -> raise message
    end
  end

  @doc """
  Why a node with this config must not boot on the Local storage backend, or `nil`.

  Pure over the passed map (keys: `:env`, `:storage`, `:lb_backends`, `:replication_followers`,
  `:replication_enabled`, `:allow_local_storage`), so a test needs no global app env.
  """
  @spec local_storage_fleet_problem(%{
          env: atom(),
          storage: module(),
          lb_backends: map(),
          replication_followers: list(),
          replication_enabled: boolean(),
          allow_local_storage: boolean()
        }) :: String.t() | nil
  def local_storage_fleet_problem(%{env: :prod, storage: Fathom.Shard.Storage.Local} = cfg) do
    cond do
      map_size(cfg.lb_backends) > 0 ->
        local_storage_fleet_message(":lb_backends is set")

      cfg.allow_local_storage == true ->
        nil

      cfg.replication_followers != [] ->
        local_storage_fleet_message(":replication_followers is set")

      cfg.replication_enabled == true ->
        local_storage_fleet_message(":replication_enabled is on")

      true ->
        nil
    end
  end

  def local_storage_fleet_problem(_cfg), do: nil

  defp local_storage_fleet_message(why) do
    "config error: :shard_storage is Fathom.Shard.Storage.Local (SHARD_STORAGE unset or " <>
      "\"local\") with a multi-node fleet (#{why}) — Local provides no cross-node single-writer " <>
      "guarantee (its lease is node-local only), so two nodes can each hold a shard's lease and " <>
      "both write (silent split-brain at the first LB remap). Set SHARD_STORAGE=s3 (or another " <>
      "conditional-write store) for a fleet. A single-node deploy sets REPLICATION_ENABLED=false; " <>
      "a replicating primary whose orchestrator guarantees one active writer may acknowledge with " <>
      "ALLOW_LOCAL_STORAGE=true (that ack does not cover :lb_backends) " <>
      "(expert review 2026-07-14 #3, 2026-11-07 #8)."
  end

  # Expert review 2026-07-14 #6: without a serving-zone anchor, Fathom.ShardExecutor.shard_from_host
  # promotes ANY attacker-controlled Host first-label to a shard id — the primary production
  # tenant-selection path then trusts a fully attacker-controlled header (release-blocker-class
  # cross-tenant selection). Refuse to boot with no :shard_base_domain when the data plane is
  # actually exposed (a fleet via :lb_backends, or the Hrana listener enabled) unless
  # ALLOW_UNANCHORED_ROUTING explicitly acks a deployment that intends unanchored routing. Prod-only.
  @doc false
  def check_shard_base_domain! do
    exposed? =
      map_size(Application.get_env(:fathom, :lb_backends, %{})) > 0 or
        Fathom.Application.hrana_enabled?()

    if Application.get_env(:fathom, :env) == :prod and
         blank?(Application.get_env(:fathom, :shard_base_domain)) and exposed? and
         not Application.get_env(:fathom, :allow_unanchored_routing, false) do
      raise "config error: :shard_base_domain is unset with the data plane exposed — " <>
              "Host-subdomain routing then promotes ANY attacker-controlled Host first-label to a " <>
              "shard id (cross-tenant selection). Set SHARD_BASE_DOMAIN=<serving-zone> to anchor " <>
              "routing, or ALLOW_UNANCHORED_ROUTING=true to acknowledge unanchored routing " <>
              "(expert review 2026-07-14 #6)."
    end

    :ok
  end

  # Expert review 2026-07-14 #15: the ?db= / x-fathom-shard override (finding #4) is an
  # UNAUTHENTICATED shard-selection primitive — a caller reaching a node directly can name any
  # shard. It's a dev/test-only fallback and must NEVER be on in prod. Refuse to boot.
  @doc false
  def check_shard_override! do
    if Application.get_env(:fathom, :env) == :prod and
         Application.get_env(:fathom, :allow_shard_override, false) do
      raise "config error: :allow_shard_override is enabled in prod — the ?db= / x-fathom-shard " <>
              "override is an unauthenticated shard-selection primitive (finding #4) and must never " <>
              "be on in prod. Unset :allow_shard_override (expert review 2026-07-14 #15)."
    end
  end

  # Spare connections the prod pool should keep over the Oban floor; also the computed POOL_SIZE
  # default's margin in config/runtime.exs (expert review 2026-10-21 #11).
  @repo_pool_headroom 10

  @doc """
  The Repo pool against Oban's queue concurrency (expert review 2026-10-21 #11).

  Every executing Oban job holds a `Fathom.Repo` connection, so the sum of the queue limits is a
  floor on pool demand. Anything above it is all that is left for `HranaAuth.Revocations` on the
  stream-open path, the Recorder, the pollers and the admin LiveViews; past `queue_interval` of
  saturation DBConnection DROPS checkouts, which on a cold auth miss is floor 0 (`fail_open`) or a
  401 (`fail_closed`). The queues summed to exactly the old fixed default of 25 after `tokens` and
  `erasure` were added, with nothing checking.

    * pool (`pool_size × pool_count`) at or under the queue sum: refuses to boot in prod, since
      a full set of running jobs leaves the stream-open path no connection at all; warns elsewhere.
    * pool under sum + #{@repo_pool_headroom} in prod: warns (the computed default is exactly that).
    * Oban in `testing:` mode runs no queues, so there is nothing to check.

  Returns `:ok`, `{:warn, message}` or raises. The pure decision is `repo_pool_verdict/4`.
  """
  @spec check_repo_pool_headroom!() :: :ok | {:warn, String.t()}
  def check_repo_pool_headroom! do
    oban = Application.get_env(:fathom, Oban, [])
    repo = Application.get_env(:fathom, Fathom.Repo, [])

    if Keyword.get(oban, :testing, :disabled) != :disabled do
      :ok
    else
      pool = Keyword.get(repo, :pool_size, 10) * Keyword.get(repo, :pool_count, 1)
      queues = oban_queue_limit_sum(Keyword.get(oban, :queues, []))

      case repo_pool_verdict(
             pool,
             queues,
             Application.get_env(:fathom, :env),
             @repo_pool_headroom
           ) do
        :ok ->
          :ok

        {:raise, msg} ->
          raise msg

        {:warn, msg} = warn ->
          Logger.warning(msg)
          warn
      end
    end
  end

  @doc false
  @spec repo_pool_verdict(integer(), integer(), atom(), integer()) ::
          :ok | {:warn, String.t()} | {:raise, String.t()}
  def repo_pool_verdict(pool, queues, env, headroom) do
    cond do
      queues > 0 and pool <= queues ->
        msg =
          "config error: the Repo pool (#{pool} = POOL_SIZE × pool_count) is at or under the sum " <>
            "of Oban's queue limits (#{queues}). Every running job holds a connection, so a full " <>
            "set of jobs leaves none for HranaAuth.Revocations on the stream-open path, and " <>
            "DBConnection drops the queued checkouts. Raise POOL_SIZE to at least " <>
            "#{queues + headroom}, or unset it for the computed default (expert review " <>
            "2026-10-21 #11)."

        if env == :prod,
          do: {:raise, msg},
          else: {:warn, String.replace_prefix(msg, "config error", "config warning")}

      env == :prod and queues > 0 and pool < queues + headroom ->
        {:warn,
         "config warning: the Repo pool (#{pool}) leaves #{pool - queues} connection(s) over " <>
           "the sum of Oban's queue limits (#{queues}); the computed default leaves " <>
           "#{headroom}. The endpoint, the pollers and HranaAuth.Revocations share the rest " <>
           "(expert review 2026-10-21 #11)."}

      true ->
        :ok
    end
  end

  @doc """
  The sum of Oban's queue concurrency limits: an integer limit, or a keyword list's `:limit`.
  `false` (no queues) is 0. `config/runtime.exs` computes the same sum for the POOL_SIZE default.
  """
  @spec oban_queue_limit_sum(term()) :: non_neg_integer()
  def oban_queue_limit_sum(queues) when is_list(queues) do
    Enum.reduce(queues, 0, fn
      {_queue, limit}, acc when is_integer(limit) and limit > 0 -> acc + limit
      {_queue, opts}, acc when is_list(opts) -> acc + max(Keyword.get(opts, :limit, 0), 0)
      _, acc -> acc
    end)
  end

  def oban_queue_limit_sum(_queues), do: 0

  # Expert review 2026-07-14 #16: a non-nil :default_shard in prod commingles EVERY unresolved
  # request (no Host subdomain, no override) into one shared shard instead of failing closed
  # (finding #26's safe posture is nil-in-prod). A shared default can be an intentional
  # single-tenant choice, so WARN rather than raise. Prod-only.
  @doc false
  def check_default_shard! do
    default = Application.get_env(:fathom, :default_shard)

    if Application.get_env(:fathom, :env) == :prod and not is_nil(default) do
      Logger.warning(
        "config warning: :default_shard is set in prod (#{inspect(default)}) — every unresolved " <>
          "request (no Host subdomain, no override) commingles into that one shared shard instead " <>
          "of failing closed (finding #26). Leave it unset to fail closed unless a shared default " <>
          "is an intentional single-tenant choice (expert review 2026-07-14 #16)."
      )
    end

    nil
  end

  # Expert review 2026-07-14 #18: with the Hrana listener enabled, auth :disabled, AND bound to a
  # wildcard interface, the data plane is unauthenticated and reachable on every interface — the
  # ONLY tenant-isolation control is an external firewall/security-group. This is the documented
  # network-trust posture (docs/auth.md), so WARN rather than raise. Prod-only.
  # Expert review 2026-08-20 #3: the replication listener has NO authentication of any kind — no
  # credential in the protocol, no peer allowlist — and `Fleet.follower_children/0` omits `:ip`
  # when `:replication_bind_ip` is unset, so `:gen_tcp.listen` binds every interface. Until #3's
  # tier 2/3 land (peer allowlist, frame HMAC), a reachable replication port is equivalent to
  # WRITE ACCESS TO EVERY SHARD ON THE NODE: a seed installs a replica with an attacker-chosen
  # epoch, `Promote.fresher?/2` is a bare tuple compare, and the promote path publishes it to S3
  # as the tenant's durable object.
  #
  # RAISES where check_hrana_exposure!/0 only warns, and the asymmetry is the point: the Hrana
  # port has an auth mode to fall back on (HRANA_AUTH=required), so an operator who leaves it
  # unauthenticated has made a choice the warning names. This port has no such mode — the network
  # IS the only boundary, so a wildcard bind is not a posture, it is an unguarded door. Prod-only,
  # and only when listening is actually on, so dev/test/rig configs are unaffected.
  # The replica store grows from OTHER NODES' write traffic and has no retention (expert review
  # 2026-08-20 #23). Sharing a volume with the live shard data dir means somebody else's write rate
  # can fill the disk that THIS node's durability flushes and cold-open pulls depend on — and the
  # symptom is the unbounded-RPO one AGENTS.md documents, where writes keep being acked and can
  # never be made durable. Both default under System.tmp_dir!(), so the unsafe arrangement is the
  # DEFAULT one. WARN rather than raise: a single-volume box is a legitimate small deployment, and
  # the back-pressure floor in `Follower` bounds the damage.
  #
  # EQUAL OR NESTED DIRS ARE REFUSED, not warned (expert review 2026-11-07 #37). With one directory
  # (or one inside the other) a replica `<id>.db` is byte-identical in name to a live shard
  # `<id>.db`: a node that follows S and then becomes its owner finds a replica `.db`+`-wal` at the
  # live path with no provenance sidecar, which `Fork.resolve` quarantines as a planted fork or —
  # with `:adopt_unprovenanced_warm` — adopts as the live database. `coordinated_here?` guards only
  # a RUNNING coordinator, and the mount comparison below is blind to path equality. Compared on
  # `Path.expand`ed segments, so `/a/b` vs `/a/bc` is not nesting; symlinks are NOT resolved (a
  # link to the same directory is the operator's to avoid — the stat-based mount check cannot see
  # it either).
  @doc false
  @spec check_replication_disk!() :: nil
  def check_replication_disk! do
    if Application.get_env(:fathom, :env) == :prod and
         Application.get_env(:fathom, :replication_listen, false) == true do
      replica = Fathom.Shard.Replication.Follower.default_dir()
      data = Fathom.Shard.data_dir()

      if dirs_overlap?(replica, data) do
        raise "config error: the replication replica store (REPLICATION_DIR #{inspect(replica)}) " <>
                "equals or is nested inside / around the live shard data dir (SHARD_DATA_DIR " <>
                "#{inspect(data)}). A replica <id>.db would then share a path with the live " <>
                "<id>.db: a node that follows a shard and later owns it finds a replica with no " <>
                "provenance at the live path and quarantines or ADOPTS it as the tenant's " <>
                "database (expert review 2026-11-07 #37). Give REPLICATION_DIR its own directory " <>
                "that is not inside SHARD_DATA_DIR (and vice versa)."
      end

      if same_volume?(replica, data) do
        Logger.warning(
          "config warning: the replication replica store (#{replica}) and the live shard data " <>
            "dir (#{data}) are on the SAME volume. The replica store holds a full copy of every " <>
            "shard this node follows and grows from other nodes' write traffic with no retention " <>
            "— so a peer's write rate can fill the disk this node's own flushes and cold-open " <>
            "pulls need, and acked writes then become permanently undurable. Give REPLICATION_DIR " <>
            "its own volume (expert review 2026-08-20 #23)."
        )
      end
    end

    nil
  end

  # SORTER / TRANSIENT-INDEX SPILL IS NOT BOUNDED BY ANYTHING FATHOM CAN SET (expert review
  # 2026-11-07 #32; connection-executor#5). `PRAGMA temp.max_page_count` caps only the TEMP *schema*
  # database. A statement's own sorter runs, ephemeral tables, materialised CTEs and automatic
  # indexes are separate unlinked `etilqs_*` files outside that cap. Re-measured here with a bare
  # exqlite connection (`temp.max_page_count=1000` set first, `WITH RECURSIVE … SELECT x … ORDER BY
  # random()` stepped once): the process held a 412 MB unlinked temp file after 5 s, so the 30 s
  # query deadline is the only bound (~2.4 GB per statement per dirty-IO thread). A tenant cannot
  # bound or undo it either way: `temp_store` / `temp_store_directory` are not on the tenant allow
  # list, and `temp_store=MEMORY` would only trade the disk for the process-global
  # `hard_heap_limit`. SQLite has no sorter-bytes limit; a `sqlite3_progress_handler` that
  # fstat()s the spill fd is the only in-engine bound and is unbuilt/uncosted (PARKED,
  # docs/component-notes.md).
  #
  # So the bound is operational: point SQLite at a size-limited location (`SQLITE_TMPDIR`). This
  # boot check WARNS when SQLite's resolved temp dir shares a volume with SHARD_DATA_DIR (or
  # REPLICATION_DIR when this node follows), because then one tenant statement can fill the volume
  # every co-resident tenant's commits, checkpoints and flushes depend on. It resolves the dir
  # the way SQLite's unix VFS does (`SQLITE_TMPDIR`, `TMPDIR`, `/var/tmp`, `/usr/tmp`, `/tmp`) and
  # NOT `System.tmp_dir!/0`: that one reads `TMPDIR`/`TMP`/`TEMP`, never `SQLITE_TMPDIR`, and
  # prefers `/tmp` over `/var/tmp`, so using it would name the wrong directory.
  # Warn-only, prod-only, like every sibling: a single-volume box is a legitimate small deployment.
  @doc false
  @spec check_sqlite_temp_dir!() :: nil
  def check_sqlite_temp_dir! do
    if Application.get_env(:fathom, :env) == :prod do
      dirs =
        [{"SHARD_DATA_DIR", Fathom.Shard.data_dir()}] ++
          if Application.get_env(:fathom, :replication_listen, false) == true,
            do: [{"REPLICATION_DIR", Fathom.Shard.Replication.Follower.default_dir()}],
            else: []

      for msg <- sqlite_temp_warnings(sqlite_temp_dir(), dirs, &same_volume?/2),
          do: Logger.warning(msg)
    end

    nil
  end

  @doc """
  The directory SQLite's unix VFS spills temporary files to: the first of `SQLITE_TMPDIR`,
  `TMPDIR`, `/var/tmp`, `/usr/tmp`, `/tmp` that is a usable directory (SQLite also tries `.` last,
  and `sqlite3_temp_directory`, which fathom never sets). `getenv` and `usable?` are injectable.
  """
  @spec sqlite_temp_dir((String.t() -> String.t() | nil), (String.t() -> boolean())) ::
          String.t() | nil
  def sqlite_temp_dir(getenv \\ &System.get_env/1, usable? \\ &usable_dir?/1) do
    [getenv.("SQLITE_TMPDIR"), getenv.("TMPDIR"), "/var/tmp", "/usr/tmp", "/tmp"]
    |> Enum.find(&(is_binary(&1) and &1 != "" and usable?.(&1)))
  end

  # SQLite checks `access(dir, R_OK|W_OK|X_OK)` on a directory; the closest portable check without
  # a NIF is "is a directory we can read and write".
  defp usable_dir?(dir) do
    match?(
      {:ok, %File.Stat{type: :directory, access: access}} when access == :read_write,
      File.stat(dir)
    )
  end

  @doc false
  @spec sqlite_temp_warnings(
          String.t() | nil,
          [{String.t(), Path.t()}],
          (Path.t(), Path.t() -> boolean())
        ) :: [String.t()]
  def sqlite_temp_warnings(nil, _dirs, _same_volume?), do: []

  def sqlite_temp_warnings(temp, dirs, same_volume?) do
    for {label, dir} <- dirs, same_volume?.(temp, dir) do
      "config warning: SQLite's temp directory (#{temp}; resolved from SQLITE_TMPDIR, TMPDIR, " <>
        "/var/tmp, /usr/tmp, /tmp) is on the SAME volume as #{label} (#{dir}). One tenant " <>
        "statement with a large ORDER BY / GROUP BY / DISTINCT spills hundreds of MB per second " <>
        "there and nothing bounds it but the 30 s query deadline (PRAGMA temp.max_page_count caps " <>
        "only the TEMP schema), so it can fill the volume that every co-resident tenant's " <>
        "commits, checkpoints and durability flushes need. Point SQLITE_TMPDIR at a size-limited " <>
        "tmpfs or a separate volume (expert review 2026-11-07 #32)."
    end
  end

  @doc false
  @spec dirs_overlap?(Path.t(), Path.t()) :: boolean()
  def dirs_overlap?(a, b) do
    sa = a |> Path.expand() |> Path.split()
    sb = b |> Path.expand() |> Path.split()
    List.starts_with?(sa, sb) or List.starts_with?(sb, sa)
  end

  # Compare the MOUNT, not the size. Two independent volumes of the same size are indistinguishable
  # by `total_bytes`, and a false warning about a correctly-separated deployment is how a real one
  # gets ignored. Unreadable on either side means no warning at all: this is advisory, and a
  # stat failure is not evidence of a shared volume.
  defp same_volume?(a, b) do
    case {Fathom.Admin.Measurements.disk_info(a), Fathom.Admin.Measurements.disk_info(b)} do
      {{:ok, %{mount: m}}, {:ok, %{mount: m}}} -> true
      _ -> false
    end
  end

  # Expert review 2026-10-29 #1: a large-RAM Linux host with the default `vm.dirty_background_ratio`
  # never starts background writeback until the 30 s `dirty_expire_centisecs` expiry, and fathom's
  # unfsynced follower writes fill the page cache fast. The flush then lands as one burst that every
  # `synchronous=FULL` commit fsync waits behind. On the chaos rig (94 GB VM, 9.4 GB threshold)
  # that was p99 598 ms with 1.1-1.2 s windows every 30 s / 60 s; `dirty_background_bytes` = 1 GB
  # gave p99 417 ms and flat windows (docs/reviews/tpc-fleet-repl-on-2026-10-26.md).
  #
  # WARN-ONLY and never fatal, like every sibling above: a node's sysctls are the operator's call
  # and the optimum depends on RAM, dirty rate and cgroup limit. The wording is "measure", not
  # "lower": the same sweep found 256 MB (p99 485 ms) and 64 MB (p99 762 ms) WORSE than 1 GB, so a
  # "set it as low as you can" warning would send operators the wrong way. Prod-only; a no-op where
  # `/proc` has no answer (non-Linux). The gauge is published by the poller
  # (`Measurements.host/0`), not here: the reporters attach after this runs.
  @doc false
  @spec check_vm_dirty_background!(Fathom.Admin.Measurements.reader()) :: nil
  def check_vm_dirty_background!(read \\ &Fathom.FS.read/1) do
    with :prod <- Application.get_env(:fathom, :env),
         bytes when is_integer(bytes) <- Fathom.Admin.Measurements.vm_dirty_background_bytes(read),
         true <- bytes > Fathom.Host.Proc.warn_above_bytes() do
      Logger.warning(
        "config warning: the effective vm.dirty_background threshold is " <>
          "#{Float.round(bytes / 1_073_741_824, 1)} GiB, so the kernel will not start background " <>
          "writeback until that much page cache is dirty. Fathom's unfsynced replica writes fill " <>
          "it quickly, and the eventual flush burst stalls every synchronous=FULL commit fsync " <>
          "(on the chaos rig, 94 GB RAM and a 9.4 GB threshold: p99 598 ms with 30 s / 60 s " <>
          "spikes; vm.dirty_background_bytes=1GB gave p99 417 ms, 2026-10-26). This is a host " <>
          "setting to INVESTIGATE AND MEASURE, not a rule that lower is better: 256 MB (p99 485 " <>
          "ms) and 64 MB (p99 762 ms) were worse than 1 GB on that rig. Watch " <>
          "fathom.node.host_memory.dirty and fathom.node.io_pressure.* and see " <>
          "docs/deploy-cluster.md (expert review 2026-10-29 #1)."
      )
    end

    nil
  end

  # THE FLUSH INTERVAL IS ALSO A REPLICATION THROUGHPUT SETTING (expert review 2026-08-20 #33).
  #
  # A durability flush CHECKPOINTS the WAL; a checkpoint starts a new GENERATION; and
  # `Primary.plan/3` then correctly answers `{:reset, 0, size}` — the ENTIRE WAL, not a delta. So a
  # tight interval makes every write-active shard periodically re-ship its whole WAL.
  #
  # Measured on the chaos rig, one variable, same image, 512 tenants (2026-08-18):
  # `SHARD_FLUSH_INTERVAL_MS` 5,000 → 30,000 took the run from 22,599 errors to 4 and +49%
  # throughput, with `:stale_wal_gen` and `:overloaded` both going to ZERO.
  #
  # Nothing anywhere connected the two: not `docs/durability.md`, which presented the interval as a
  # pure RPO-vs-PUT-cost tradeoff, not the replication config block in `runtime.exs`, and not this
  # guard list. An operator who followed the RPO advice and tightened the interval would degrade a
  # replicating fleet with no signal saying so.
  #
  # WARNS rather than raises, and prod-only like every sibling: a tight interval is a legitimate
  # deliberate choice (RPO can matter more than throughput), and refusing to boot over a tuning
  # decision would be worse than the confusion this removes.
  @doc false
  def check_replication_flush_interval! do
    interval = Application.get_env(:fathom, :shard_flush_interval_ms, 5_000)

    if Application.get_env(:fathom, :env) == :prod and
         Fathom.Shard.Replication.Fleet.replicating?() and
         is_integer(interval) and interval > 0 and interval < @replication_flush_floor_ms do
      Logger.warning(
        "config warning: replication is ON with :shard_flush_interval_ms = #{interval} ms. A " <>
          "durability flush CHECKPOINTS the WAL, which starts a new generation, which makes the " <>
          "next push ship the ENTIRE WAL rather than a delta — so a tight interval is a " <>
          "throughput setting, not only an RPO one. Measured at 512 tenants: 5,000 vs 30,000 ms " <>
          "was 22,599 errors vs 4, and +49% throughput. Raise it to at least " <>
          "#{@replication_flush_floor_ms} ms unless you are deliberately buying RPO with " <>
          "throughput (expert review 2026-08-20 #33)."
      )
    end

    nil
  end

  # `:skew_defer_to_steal_watch` lets a node whose clock lags the store keep WRITING instead of
  # self-lapsing (audit 2026-11-22 #3). That is safe only because a stealer must first see the owner's
  # heartbeat unchanged for a whole steal-watch window; a node that opts in with its OWN watch shorter
  # than `ttl + steal_margin` (or off) has no such protection and would write while a peer may steal.
  # Heartbeat falls back to the self-lapse in that case, so the setting would silently do nothing: refuse
  # to boot, so the config says what it means. Prod-only like every sibling. It cannot see OTHER nodes'
  # watch width: the fleet-wide assumption stays the operator's (docs/single-writer.md).
  @doc false
  def check_skew_defer_to_steal_watch! do
    ttl = Fathom.Shard.Storage.lease_ttl_ms()

    if Application.get_env(:fathom, :env) == :prod and
         Application.get_env(:fathom, :skew_defer_to_steal_watch, false) == true and
         not Fathom.Shard.Heartbeat.skew_deferred_to_steal_watch?(ttl) do
      raise "config error: SKEW_DEFER_TO_STEAL_WATCH is on but this node's steal watch " <>
              "(#{Fathom.Shard.Storage.steal_watch_window_ms()} ms) is shorter than ttl + steal_margin " <>
              "(#{ttl + Fathom.Shard.Storage.steal_margin_ms()} ms). The watch is what makes a lagging " <>
              "owner safe to keep writing, so without it the node would still self-lapse and the " <>
              "setting would do nothing. Widen :steal_watch_ms (unset = ttl + steal_margin) or unset " <>
              "SKEW_DEFER_TO_STEAL_WATCH (audit 2026-11-22 #3, docs/single-writer.md)."
    end

    nil
  end

  # A non-positive `:shard_flush_interval_ms` DISABLES the periodic durability flush
  # (`Shard.do_schedule_flush/2`: "idle-only durability"), so a continuously active shard never
  # flushes and its RPO is unbounded — "the worst durability for the most valuable data"
  # (`config/runtime.exs`, which defaults prod to 300 s and says "and NOT 0"). `0` or a negative was
  # accepted in prod with no signal (expert review 2026-11-07 #81); `check_replication_flush_interval!`
  # only looks at 0 < interval < 30 s. WARNS rather than raises: idle-only is a documented, if
  # dangerous, mode, and refusing to boot over it would be an opinion about a deployment's RPO
  # tradeoff — but it must not be silent. Prod-only like every sibling.
  @doc false
  @spec check_flush_interval!() :: nil
  def check_flush_interval! do
    case flush_interval_problem(
           Application.get_env(:fathom, :env),
           Application.get_env(:fathom, :shard_flush_interval_ms, 5_000)
         ) do
      nil ->
        nil

      message ->
        Logger.warning(message)
        nil
    end
  end

  @doc """
  The boot warning for a prod flush interval that disables the periodic durability flush, or `nil`.
  Pure over its two arguments (the env and the interval in ms).
  """
  @spec flush_interval_problem(atom(), term()) :: String.t() | nil
  def flush_interval_problem(:prod, interval) when is_integer(interval) and interval <= 0 do
    "config warning: SHARD_FLUSH_INTERVAL_MS = #{interval} DISABLES the periodic durability flush " <>
      "(idle-only durability): a continuously active shard never flushes, so its loss window is " <>
      "unbounded and the busiest tenants are the least protected. The prod default is 300000 " <>
      "(5 min), which bounds it; set a positive interval unless idle-only durability is a " <>
      "deliberate choice (expert review 2026-11-07 #81)."
  end

  def flush_interval_problem(_env, _interval), do: nil

  # `soft_heap_limit` is a FIXED 8 GiB whatever the node's RAM (perf review 2026-10-14 #29). It sheds
  # page cache only; the ~190 KB fixed cost per held shard (connection + coordinator) is not
  # sheddable. So a node whose RAM budget is below `max_open_shards` x ~190 KB, or whose limit is at
  # or above its RAM, OOMs before the limit does anything. The RAM budget is the cgroup `memory.max`
  # (v2) / `memory.limit_in_bytes` (v1): in a container `memsup` reports the HOST's memory
  # (docs/configuration.md), so the cgroup file is the only honest number; with no cgroup limit the
  # RAM-side checks are skipped. WARNS, never raises, prod-only like its siblings.
  @per_shard_fixed_bytes 190 * 1024

  @doc false
  def check_soft_heap_budget! do
    if Application.get_env(:fathom, :env) == :prod do
      for msg <-
            soft_heap_warnings(
              Application.get_env(:fathom, :max_open_shards, :infinity),
              Application.get_env(:fathom, :shard_soft_heap_limit_bytes),
              cgroup_memory_limit()
            ),
          do: Logger.warning(msg)
    end

    nil
  end

  @doc false
  @spec soft_heap_warnings(term(), term(), pos_integer() | nil) :: [String.t()]
  def soft_heap_warnings(cap, soft, ram) do
    fixed = if is_integer(cap), do: cap * @per_shard_fixed_bytes, else: nil

    [
      (is_integer(fixed) and is_integer(soft) and fixed > soft) &&
        "config warning: :max_open_shards = #{cap} x ~190 KB fixed per shard = #{mib(fixed)} MiB, " <>
          "above :shard_soft_heap_limit_bytes (#{mib(soft)} MiB). The limit only sheds page cache; " <>
          "this fixed cost cannot be shed (perf review 2026-10-14 #29).",
      (is_integer(fixed) and is_integer(ram) and fixed > ram) &&
        "config warning: :max_open_shards = #{cap} x ~190 KB fixed per shard = #{mib(fixed)} MiB, " <>
          "above the container memory limit (#{mib(ram)} MiB); the node OOMs before the cap " <>
          "admits that many shards (perf review 2026-10-14 #29).",
      (is_integer(soft) and is_integer(ram) and soft >= ram) &&
        "config warning: :shard_soft_heap_limit_bytes (#{mib(soft)} MiB) is at or above the container " <>
          "memory limit (#{mib(ram)} MiB), so SQLite never sheds cache before the OOM killer acts. " <>
          "Set SHARD_SOFT_HEAP_LIMIT_BYTES to well under the limit (perf review 2026-10-14 #29)."
    ]
    |> Enum.filter(&is_binary/1)
  end

  defp mib(bytes), do: div(bytes, 1024 * 1024)

  # The cgroup memory limit in bytes, or nil (no cgroup, unreadable, or "max"/unlimited).
  defp cgroup_memory_limit do
    ["/sys/fs/cgroup/memory.max", "/sys/fs/cgroup/memory/memory.limit_in_bytes"]
    |> Enum.find_value(fn file ->
      with {:ok, raw} <- File.read(file),
           {n, _} when n > 0 and n < 1_152_921_504_606_846_976 <- Integer.parse(String.trim(raw)) do
        n
      else
        _ -> nil
      end
    end)
  end

  # :max_open_shards BOUNDS COORDINATORS, NOT CONNECTIONS (expert review 2026-09-18 #12).
  #
  # The soft admission cap counts coordinators (`Registry.count`), and it is sized from fd/RSS
  # density to keep the node below `emfile`. Connection pooling (on by default in prod) lets each
  # busy shard additionally hold up to `max_per_scope` idle handles PER SCOPE (:ro + :rw), and every
  # pooled handle is a distinct SQLite connection with its own fds — invisible to the cap. So a fleet
  # that turns pooling on against a cap sized for the pre-pooling world holds ~2-3x the fds per busy
  # shard and hits `emfile` at a fraction of the intended shard count — the same class as AGENTS.md's
  # "numbers measured with a feature off don't hold with it on".
  #
  # WARNS rather than raises, and prod-only like every sibling: a cap that already accounts for the
  # multiplier is a legitimate config, and silently re-deriving the operator's :max_open_shards would
  # be a more surprising failure than the one this removes. It quantifies the multiplier so the cap
  # (and `ulimit -n`) can be sized deliberately.
  @doc false
  def check_pool_fd_budget! do
    cap = Application.get_env(:fathom, :max_open_shards, :infinity)
    pooled? = Application.get_env(:fathom, :connection_pool, false) == true
    # Replication holds a raw read fd on the `-wal` AND one on the `-shm` wal-index per replicated
    # shard (`Session`'s `wal_fd`, expert review 2026-08-26 #32 and 2026-10-20 #1), invisible to
    # the cap too (expert review 2026-10-10 #P1).
    replicating? = Fathom.Shard.Replication.Fleet.replicating?()

    if Application.get_env(:fathom, :env) == :prod and (pooled? or replicating?) and
         is_integer(cap) do
      cfg = Application.get_env(:fathom, :connection_pool_opts, [])
      per_scope = Keyword.get(cfg, :max_per_scope, 1)

      # FDS, NOT CONNECTIONS (expert review 2026-10-20-2027 #2). Counted with lsof on this repo's
      # exqlite: a WAL-mode connection holds TWO fds (`.db` and `-wal`), and the shard's `-shm` is ONE
      # fd per OS process however many connections map it (SQLite shares one `unixShmNode` per
      # inode). The old count was one fd per connection plus one for replication, about half the
      # real number. Connections are the coordinator's working one plus up to max_per_scope idle
      # handles in each of the two scopes (:ro, :rw); replicating adds the Session's -wal and -shm
      # read fds (released ~1 s after a shard's last commit, but held by every active one).
      pool_extra = if pooled?, do: 2 * per_scope, else: 0
      per_shard = 2 * (1 + pool_extra) + 1 + if(replicating?, do: 2, else: 0)

      why =
        [
          pooled? &&
            "connection_pool is ON (up to #{2 * per_scope} idle pooled handles per shard, " <>
              "max_per_scope=#{per_scope})",
          replicating? && "replication is ON (two held -wal/-shm read fds per replicated shard)"
        ]
        |> Enum.filter(& &1)
        |> Enum.join(" and ")

      Logger.warning(
        "config warning: #{why} with :max_open_shards = #{cap}, but that cap bounds COORDINATORS only. Each open shard " <>
          "can hold up to #{per_shard} fds (2 per SQLite connection, plus -shm and replication), " <>
          "so the node can hold ~#{cap * per_shard} fds, not #{cap}. Size :max_open_shards and " <>
          "the OS fd limit (ulimit -n) for #{cap} * #{per_shard} = #{cap * per_shard} fds, or the " <>
          "node hits emfile well below the intended shard count (expert review 2026-09-18 #12)."
      )
    end

    nil
  end

  @doc false
  def check_replication_exposure! do
    wildcard_binds = [{0, 0, 0, 0}, {0, 0, 0, 0, 0, 0, 0, 0}]
    bind = Application.get_env(:fathom, :replication_bind_ip)

    if Application.get_env(:fathom, :env) == :prod and
         Application.get_env(:fathom, :replication_listen, false) == true and
         (is_nil(bind) or bind in wildcard_binds) do
      raise "config error: the replication listener is enabled and bound to ALL interfaces " <>
              "(:replication_bind_ip #{inspect(bind)}). That port is UNAUTHENTICATED — anyone " <>
              "who can reach it can seed a forged replica for any shard and have it promoted " <>
              "over the tenant's durable object (expert review 2026-08-20 #3). Pin " <>
              "REPLICATION_BIND_IP to the private interface the fleet reaches, or turn " <>
              "REPLICATION_LISTEN off."
    end
  end

  @doc """
  Refuse a frame-authentication config that cannot work (expert review 2026-08-20 #3, tier 3).

  Two failures, and neither is survivable at runtime, which is why they are raises rather than
  warnings — the first silently sends unsigned frames from a node that believes it is signing, and
  the second refuses every frame the fleet sends it.

  **Not prod-only.** The other exposure guards are, because they trade a real security control
  against a developer's convenience. These two are pure misconfiguration: there is no environment
  in which "sign, but with no key" or "require, but with no key" is a thing anyone meant.

  A third check (expert review 2026-10-12 #21) refuses a PROD secret under 32 bytes, with no
  opt-out, while signing or requiring is on: the key is one HMAC pass over the secret, so a short
  one is brute-forceable from a single sniffed frame.
  """
  @spec check_replication_frame_auth!() :: nil
  def check_replication_frame_auth! do
    signing? = Application.get_env(:fathom, :replication_sign_frames, false) == true
    required? = Application.get_env(:fathom, :replication_hmac_required, false) == true

    if (signing? or required?) and
         not Fathom.Shard.Replication.FrameAuth.key_configured?() do
      raise "config error: replication frame authentication is on " <>
              "(:replication_sign_frames #{signing?}, :replication_hmac_required #{required?}) " <>
              "but no key is configured. Set REPLICATION_HMAC_SECRET, or HRANA_TOKEN_SECRET which " <>
              "it derives from. Without a key this node would send frames UNSIGNED while " <>
              "believing it signs them, and refuse every frame that arrives."
    end

    if (signing? or required?) and Application.get_env(:fathom, :env) == :prod and
         Fathom.Shard.Replication.FrameAuth.weak_secret?() do
      raise "config error: the replication frame-auth secret is under 32 bytes. The frame key is " <>
              "one HMAC pass over it, so a short secret can be brute-forced offline from one " <>
              "captured frame, and the key authenticates every shard on this node. Generate a " <>
              "secret with: mix phx.gen.secret (set REPLICATION_HMAC_SECRET, or HRANA_TOKEN_SECRET " <>
              "if that is the one in use). See docs/runbooks/replication-frame-auth.md."
    end

    # REQUIRING without SIGNING is the rollout run backwards. This node would refuse a peer that
    # has not enabled signing while itself sending frames that peer will refuse once it does —
    # the same listen-before-ship ordering A2 already documents, and the same failure mode: every
    # shard replicated across the boundary loses quorum.
    if required? and not signing? do
      raise "config error: :replication_hmac_required is on but :replication_sign_frames is off. " <>
              "Signing must be enabled fleet-wide BEFORE any node requires it, exactly as " <>
              "REPLICATION_LISTEN must precede REPLICATION_ENABLED. Turn REPLICATION_SIGN_FRAMES " <>
              "on everywhere first; see docs/runbooks/replication-frame-auth.md."
    end

    nil
  end

  @doc false
  def check_hrana_exposure! do
    wildcard_binds = [{0, 0, 0, 0}, {0, 0, 0, 0, 0, 0, 0, 0}]

    if Application.get_env(:fathom, :env) == :prod and Fathom.Application.hrana_enabled?() and
         Application.get_env(:fathom, :hrana_auth, :disabled) == :disabled and
         Application.get_env(:fathom, :hrana_bind_ip, {0, 0, 0, 0}) in wildcard_binds do
      Logger.warning(
        "config warning: the Hrana data plane is enabled, unauthenticated (:hrana_auth :disabled), " <>
          "AND bound to all interfaces — the only tenant-isolation control is an external " <>
          "firewall/security-group, and with auth disabled every LB client can open every shard. " <>
          "Set HRANA_AUTH=required or pin HRANA_BIND_IP to the private " <>
          "interface the LB reaches (expert review 2026-07-14 #18)."
      )
    end

    nil
  end

  @doc """
  Refuse a prod node whose failed-login throttles would share one bucket (expert review 2026-10-12
  #12).

  The admin and /api throttles key on the client IP. Without `:trusted_proxies`, `conn.remote_ip`
  is whatever sits in front of `:4000`, so every client shares one bucket. Twenty bad requests from
  anywhere then lock out every operator, valid passwords included, and the default throttles are on.
  So prod refuses to boot when any throttle is on and no trusted proxy is configured.

  The opt-out is `ADMIN_THROTTLE_SHARED_BUCKET_OK=true`, for a deployment with no proxy in front of
  `:4000`, where `remote_ip` is already the client. Off in dev and test.
  """
  @spec check_throttle_proxy!() :: nil
  def check_throttle_proxy! do
    throttled? =
      Application.get_env(:fathom, :admin_auth_max_failures) not in [nil, 0] or
        Application.get_env(:fathom, :api_rate_limit) not in [nil, 0]

    if Application.get_env(:fathom, :env) == :prod and throttled? and
         FathomWeb.ClientIp.trusted_proxies() == [] and
         Application.get_env(:fathom, :admin_throttle_shared_bucket_ok) != true do
      raise "config error: the login and API throttles are on in prod, but TRUSTED_PROXIES is " <>
              "empty, so every client shares one throttle bucket. One flood of bad requests would " <>
              "lock out every operator. Set TRUSTED_PROXIES to the address(es) of whatever " <>
              "terminates TLS in front of :4000, or, if nothing proxies :4000, set " <>
              "ADMIN_THROTTLE_SHARED_BUCKET_OK=true. See docs/configuration.md."
    end

    nil
  end

  @doc """
  Refuse a prod node whose operator-dashboard credentials are blank, half-configured or weak
  (expert review 2026-10-13 #9).

  The `/admin` and `/api` BasicAuth gate is one shared password. A blank `ADMIN_USER`/`ADMIN_PASS`
  is treated as unset at runtime (the surface answers 503), but it is still a misconfiguration, so
  prod refuses it outright, with no opt-out. A half-configured pair (one set, the other absent) is
  refused the same way.

  A weak password (under 16 bytes, or equal to the username) is refused unless
  `ADMIN_WEAK_CREDENTIALS_OK=true`, for a deployment that knowingly runs a short password behind a
  network boundary. Both credentials unset is the intended "dashboard off" posture and boots (the
  runtime config warns). Off in dev and test.
  """
  @spec check_admin_credentials!() :: nil
  def check_admin_credentials! do
    if Application.get_env(:fathom, :env) == :prod do
      creds = Application.get_env(:fathom, :admin_auth, [])
      user = creds[:username]
      pass = creds[:password]

      cond do
        Application.get_env(:fathom, :admin_auth_blank_env) == true ->
          raise "config error: ADMIN_USER or ADMIN_PASS is set to a blank value in prod. A blank " <>
                  "value is treated as unset (the admin dashboard answers 503), so this is a " <>
                  "misconfiguration. Unset the variable or give it a value. See " <>
                  "docs/runbooks/admin-dashboard.md."

        is_nil(user) != is_nil(pass) ->
          raise "config error: only one of ADMIN_USER and ADMIN_PASS is set in prod. Set both, or " <>
                  "neither (the dashboard then stays off). See docs/runbooks/admin-dashboard.md."

        is_nil(user) ->
          nil

        weak_admin_password?(user, pass) and
            Application.get_env(:fathom, :admin_weak_credentials_ok) != true ->
          raise "config error: the admin dashboard password is under 16 bytes or equal to the " <>
                  "username. It is the one credential for the whole control plane (every tenant's " <>
                  "query console, delete and export). Set a password of 16 bytes or more, or, to " <>
                  "knowingly run a weak one, set ADMIN_WEAK_CREDENTIALS_OK=true. See " <>
                  "docs/runbooks/admin-dashboard.md."

        true ->
          nil
      end
    end

    nil
  end

  @admin_min_password_bytes 16

  defp weak_admin_password?(user, pass) do
    byte_size(pass) < @admin_min_password_bytes or pass == user
  end

  @doc """
  Prod warning when the S3 credentials carry a session token (expert review 2026-10-12 #27).

  Prod runs long-lived static keys (AWS_ACCESS_KEY_ID / AWS_SECRET_ACCESS_KEY) and the S3 client
  caches them for the life of the process. A session token (STS, IRSA, instance role) expires, and
  nothing refreshes it, so every S3 call returns 403 at once across the fleet. This only warns: the
  node still boots, and the reason it cannot refresh is documented in docs/configuration.md.
  """
  @spec check_s3_static_credentials!() :: nil
  def check_s3_static_credentials! do
    s3 = Application.get_env(:fathom, Fathom.Shard.Storage.S3, [])

    if Application.get_env(:fathom, :env) == :prod and
         Application.get_env(:fathom, :shard_storage) == Fathom.Shard.Storage.S3 and
         is_binary(s3[:token]) and s3[:token] != "" do
      Logger.warning(
        "config warning: AWS_SESSION_TOKEN is set, so the S3 credentials are temporary (STS, " <>
          "IRSA or an instance role). They are cached for the life of the process and are NOT " <>
          "refreshed: when they expire every shard operation fails with 403 at once, fleet-wide. " <>
          "Prod is designed for long-lived static keys; see docs/configuration.md (S3 credentials)."
      )
    end

    nil
  end

  defp blank?(nil), do: true
  defp blank?(s) when is_binary(s), do: String.trim(s) == ""
  defp blank?(_), do: false

  defp normalize_id(nil), do: nil

  defp normalize_id(id) do
    case Fathom.ShardId.cast(id) do
      {:ok, canonical} -> canonical
      :error -> id
    end
  end
end
