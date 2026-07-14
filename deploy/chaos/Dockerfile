# syntax=docker/dockerfile:1
# Fathom prod-release image for the chaos rig (deploy/chaos).
#
# BUILD CONTEXT IS THE PARENT OF BOTH REPOS (e.g. ~/NotWork), because fathom
# depends on filo as a path dep ({:filo, path: "../filo"}) and both trees must
# be inside the context. docker-compose.yml sets `context: ../../..` +
# `dockerfile: fathom/deploy/chaos/Dockerfile`; Dockerfile.dockerignore next to
# this file whitelists only fathom/ and filo/ (BuildKit per-Dockerfile ignore).

ARG BUILDER_IMAGE=hexpm/elixir:1.19.5-erlang-27.3.4.14-debian-bookworm-20260623-slim
ARG RUNNER_IMAGE=debian:bookworm-slim

FROM ${BUILDER_IMAGE} AS builder

# build-essential: exqlite compiles its SQLite NIF via elixir_make.
RUN apt-get update -y && apt-get install -y build-essential git curl \
  && apt-get clean && rm -f /var/lib/apt/lists/*_*

ENV MIX_ENV=prod
RUN mix local.hex --force && mix local.rebar --force

# The filo path dep sits NEXT TO fathom in the context.
COPY filo/ /src/filo/

WORKDIR /src/fathom
COPY fathom/mix.exs fathom/mix.lock ./
RUN mix deps.get --only prod

# Compile-time config first so dep compilation caches independently of app code.
COPY fathom/config/config.exs fathom/config/prod.exs config/
RUN mix deps.compile

COPY fathom/priv priv
COPY fathom/lib lib
COPY fathom/assets assets
# Rig-only: compile the web endpoint for plaintext LAN access (SSL-force + WS origin check off),
# so each node's admin dashboard is reachable from another host over http. force_ssl is a
# compile-time config key, so this must be a BUILD-time env (see config/prod.exs). Never real prod.
# Placed here (not earlier) so the dependency layers stay cached — only the app recompiles.
ENV WEB_INSECURE_LOCAL=1
# Compile FIRST: colocated LiveView hooks are extracted into the build dir at
# compile time and the esbuild bundle imports them (phoenix-colocated/fathom).
RUN mix compile
# Downloads the tailwind/esbuild binaries on first run, builds + digests assets
# (config/prod.exs expects priv/static/cache_manifest.json at endpoint boot).
RUN mix assets.setup && mix assets.deploy

# Runtime config changes shouldn't bust the compile cache.
COPY fathom/config/runtime.exs config/
RUN mix release

FROM ${RUNNER_IMAGE}

# curl: compose healthchecks + in-container debugging.
RUN apt-get update -y && \
  apt-get install -y libstdc++6 openssl libncurses6 locales ca-certificates curl && \
  apt-get clean && rm -f /var/lib/apt/lists/*_* && \
  sed -i '/en_US.UTF-8/s/^# //g' /etc/locale.gen && locale-gen

ENV LANG=en_US.UTF-8 LANGUAGE=en_US:en LC_ALL=en_US.UTF-8

WORKDIR /app
RUN useradd --create-home app && mkdir -p /data/shards /data/warm && chown -R app /app /data
USER app

COPY --from=builder --chown=app:app /src/fathom/_build/prod/rel/fathom ./

CMD ["/app/bin/fathom", "start"]
