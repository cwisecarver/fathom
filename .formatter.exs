[
  import_deps: [:ecto, :ecto_sql, :phoenix],
  subdirectories: ["priv/*/migrations"],
  plugins: [Phoenix.LiveView.HTMLFormatter],
  inputs: [
    "*.{heex,ex,exs}",
    "{config,lib,test}/**/*.{heex,ex,exs}",
    "priv/*/seeds.exs",
    # The Elixir drivers and probes (cleanliness audit 2026-11-13 #24): `mix format` never saw them.
    "scripts/*.exs",
    "deploy/chaos/*.exs"
  ]
]
