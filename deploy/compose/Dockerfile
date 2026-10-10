# syntax=docker/dockerfile:1
# Fathom production release image (review #17).
#
# This is the canonical deployable image — a `mix release` of fathom. The single-node eval stack
# (deploy/compose/docker-compose.yml) builds it with WEB_INSECURE_LOCAL=1 so the dashboard is
# reachable over plain HTTP on localhost; a REAL deployment builds it with the default (SSL forced,
# terminate TLS at your LB). The chaos rig has its own rig-specific Dockerfile (deploy/chaos/).
#
# BUILD CONTEXT IS THE PARENT OF BOTH REPOS (e.g. ~/NotWork), because fathom depends on filo as a
# path dep ({:filo, path: "../filo"}) and both trees must be inside the context. The compose file
# sets `context: ../../..` + `dockerfile: fathom/deploy/compose/Dockerfile`; the .dockerignore next
# to this file whitelists only fathom/ and filo/.
#
# Standalone build (from the parent of fathom/ and filo/):
#   docker build -f fathom/deploy/compose/Dockerfile -t fathom:latest .
# Eval build (plaintext localhost dashboard):
#   docker build -f fathom/deploy/compose/Dockerfile --build-arg WEB_INSECURE_LOCAL=1 -t fathom:eval .

ARG BUILDER_IMAGE=hexpm/elixir:1.20.4-erlang-28.5.0.7-debian-bookworm-20260918-slim
ARG RUNNER_IMAGE=debian:bookworm-slim

FROM ${BUILDER_IMAGE} AS builder

# build-essential: exqlite compiles its SQLite NIF via elixir_make.
RUN apt-get update -y && apt-get install -y build-essential git curl \
  && apt-get clean && rm -f /var/lib/apt/lists/*_*

# Rust, for native/fathom_udf — the loadable SQLite extension (engine PRAGMA authorizer, deadline
# backstop, size limits, Django UDFs). `mix compile.fathom_udf` SKIPS without cargo, and since
# expert review 2026-10-10 #8 a prod node without the artifact REFUSES TO BOOT (ALLOW_NO_UDF=true
# overrides) — so this image must build it.
# Toolchain pinned to 1.98.0 (the version `rustc --version` reports on the build host), not
# `stable`, so the same Dockerfile does not produce a different extension binary as stable moves.
ENV RUSTUP_HOME=/usr/local/rustup CARGO_HOME=/usr/local/cargo PATH=/usr/local/cargo/bin:$PATH
RUN curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs \
  | sh -s -- -y --profile minimal --default-toolchain 1.98.0 \
  && rustc --version && cargo --version

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
# The Rust crate `mix compile.fathom_udf` builds into priv/sqlite_ext/ (copied after deps.compile so
# a crate edit does not bust the dependency layer cache).
COPY fathom/native native

# SSL posture is compile-time (config/prod.exs reads WEB_INSECURE_LOCAL at compile). Default empty =
# force_ssl ON (production; front with a TLS-terminating LB). The eval stack passes 1 to reach the
# plaintext dashboard on localhost. NEVER build a real deployment with WEB_INSECURE_LOCAL=1.
ARG WEB_INSECURE_LOCAL=""
ENV WEB_INSECURE_LOCAL=${WEB_INSECURE_LOCAL}

# Compile FIRST: colocated LiveView hooks are extracted at compile time and the esbuild bundle
# imports them. Then build + digest assets (config/prod.exs expects the cache manifest at boot).
# The cargo target dir and registry are BuildKit cache mounts, not image layers: this RUN sits
# below COPY lib, so every lib/ edit re-ran it, and without the mounts cargo rebuilt the crate from
# scratch each time. With them cargo is incremental across builds. The .so still lands in
# priv/sqlite_ext/ inside the image, which is what the release packages.
RUN --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,target=/usr/local/cargo/git \
    --mount=type=cache,target=/src/fathom/native/fathom_udf/target \
    mix compile
RUN mix assets.setup && mix assets.deploy

# Runtime config changes shouldn't bust the compile cache.
COPY fathom/config/runtime.exs config/
# rel/vm.args.eex carries the VM capacity flags (+SDio, +Q, +IOt/+IOp — expert review 2026-07-24
# #2). WITHOUT this COPY `mix release` silently generates a default vm.args and every one of those
# flags is absent from the image — this is the canonical deployable image, so that omission shipped
# ERTS defaults to every container deployment while the release was believed to be tuned.
COPY fathom/rel rel
# Same cache mounts as `mix compile`: mix release re-runs the :fathom_udf compiler, which calls cargo.
RUN --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,target=/usr/local/cargo/git \
    --mount=type=cache,target=/src/fathom/native/fathom_udf/target \
    mix release

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
