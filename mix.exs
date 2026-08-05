defmodule Fathom.MixProject do
  use Mix.Project

  def project do
    [
      app: :fathom,
      version: "0.3.0",
      elixir: "~> 1.15",
      elixirc_paths: elixirc_paths(Mix.env()),
      start_permanent: Mix.env() == :prod,
      aliases: aliases(),
      deps: deps(),
      releases: releases(),
      # :fathom_udf builds native/fathom_udf (the Django-compatibility SQLite extension) into
      # priv/sqlite_ext/. It SKIPS rather than fails when cargo is absent — see
      # Mix.Tasks.Compile.FathomUdf for why a Rust toolchain is not made a hard requirement.
      #
      # It runs AFTER Mix.compilers() on purpose: the compiler task is defined in this project
      # (lib/mix/tasks/compile.fathom_udf.ex), so :elixir has to compile it before Mix can resolve
      # it. Placing it first gives `The task "compile.fathom_udf" could not be found` on a clean
      # build. Nothing at compile time depends on the artifact — only the running node loads it —
      # so last is also the correct place semantically.
      compilers: [:phoenix_live_view] ++ Mix.compilers() ++ [:fathom_udf],
      listeners: [Phoenix.CodeReloader]
    ]
  end

  # `mix release` config. The default release already builds (the deploy/ images use it); this
  # names it explicitly and pins the runtime_tools app (used by `bin/fathom rpc`/observer). See
  # `deploy/compose/` for the eval stack and `docs/deploy-cluster.md` for the fleet topology.
  defp releases do
    [
      fathom: [
        include_executables_for: [:unix],
        applications: [runtime_tools: :permanent]
      ]
    ]
  end

  # Configuration for the OTP application.
  #
  # Type `mix help compile.app` for more information.
  def application do
    [
      mod: {Fathom.Application, []},
      extra_applications: [:logger, :runtime_tools, :os_mon]
    ]
  end

  def cli do
    [
      preferred_envs: [precommit: :test]
    ]
  end

  # Specifies which paths to compile per environment.
  defp elixirc_paths(:test), do: ["lib", "test/support"]
  defp elixirc_paths(_), do: ["lib"]

  # Specifies your project dependencies.
  #
  # Type `mix help deps` for examples and options.
  defp deps do
    [
      {:phoenix, "~> 1.8.3"},
      {:phoenix_ecto, "~> 4.5"},
      {:ecto_sql, "~> 3.13"},
      {:postgrex, ">= 0.0.0"},
      {:phoenix_html, "~> 4.1"},
      {:phoenix_live_reload, "~> 1.2", only: :dev},
      {:phoenix_live_view, "~> 1.1.0"},
      {:lazy_html, ">= 0.1.0", only: :test},
      {:phoenix_live_dashboard, "~> 0.8.3"},
      {:esbuild, "~> 0.10", runtime: Mix.env() == :dev},
      {:tailwind, "~> 0.3", runtime: Mix.env() == :dev},
      {:heroicons,
       github: "tailwindlabs/heroicons",
       tag: "v2.2.0",
       sparse: "optimized",
       app: false,
       compile: false,
       depth: 1},
      {:swoosh, "~> 1.16"},
      {:req, "~> 0.5"},
      {:telemetry_metrics, "~> 1.0"},
      {:telemetry_poller, "~> 1.0"},
      # In-process Prometheus reporter: aggregates the Telemetry.Metrics in `Fathom.Telemetry`
      # (ETS-backed, low-cardinality node/fleet signals) and exposes a `/metrics` scrape. The
      # admin dashboard reads its in-process aggregation; external Prometheus/Grafana scrape the
      # endpoint. `_core` (no bundled Bandit listener) — we mount the scrape ourselves.
      {:telemetry_metrics_prometheus_core, "~> 1.1"},
      # OpenTelemetry: traces for cluster operations (cold-open, lease handoff). Metrics
      # stay on Telemetry.Metrics (OTel's BEAM metrics SDK is still experimental). The OTLP
      # exporter is env-gated in config/runtime.exs — a no-op until an endpoint is set.
      {:opentelemetry_api, "~> 1.4"},
      {:opentelemetry, "~> 1.5"},
      {:opentelemetry_exporter, "~> 1.8"},
      {:opentelemetry_telemetry, "~> 1.1"},
      {:gettext, "~> 1.0"},
      {:jason, "~> 1.2"},
      {:oban, "~> 2.18"},
      {:dns_cluster, "~> 0.2.0"},
      {:bandit, "~> 1.5"},
      {:exqlite, "~> 0.27"},
      # The Hrana (libSQL) protocol server AND client — fathom serves shards through it.
      # On Hex as of filo 0.2.0, so a clone of fathom no longer needs filo as a sibling.
      #
      # "~> 0.2.0" and not "~> 0.2": filo is pre-1.0, and its CONTRIBUTING states that
      # below 1.0.0 the MINOR number carries breaking changes. So this must not float
      # to 0.3.0 on its own.
      #
      # Co-developing both repos? Point FILO_PATH at your checkout — no mix.exs edit,
      # so the path dep can never be committed by accident:
      #     FILO_PATH=../filo mix test
      {:filo, filo_dep()},
      # In-process Hrana WebSocket *client*, dev/test only — the loopback client the
      # wire benches use to exercise the full Filo.Socket path (Phase 1,
      # docs/tpc-benchmark-plan.md). `mint` + `castore` are already transitive (Finch/Req),
      # so this adds only the thin WS layer, and never ships in a prod release.
      {:mint_web_socket, "~> 1.0", only: [:dev, :test]}
    ]
  end

  defp filo_dep do
    case System.get_env("FILO_PATH") do
      nil -> "~> 0.2.0"
      path -> [path: path, override: true]
    end
  end

  # Aliases are shortcuts or tasks specific to the current project.
  # For example, to install project dependencies and perform other setup tasks, run:
  #
  #     $ mix setup
  #
  # See the documentation for `Mix` for more info on aliases.
  defp aliases do
    [
      setup: ["deps.get", "ecto.setup", "assets.setup", "assets.build"],
      "ecto.setup": ["ecto.create", "ecto.migrate", "run priv/repo/seeds.exs"],
      "ecto.reset": ["ecto.drop", "ecto.setup"],
      test: ["ecto.create --quiet", "ecto.migrate --quiet", "test"],
      "assets.setup": ["tailwind.install --if-missing", "esbuild.install --if-missing"],
      "assets.build": ["compile", "tailwind fathom", "esbuild fathom"],
      "assets.deploy": [
        "tailwind fathom --minify",
        "esbuild fathom --minify",
        "phx.digest"
      ],
      precommit: ["compile --warnings-as-errors", "deps.unlock --unused", "format", "test"]
    ]
  end
end
