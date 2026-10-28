# syntax=docker/dockerfile:1
# Fathom prod-release image for the chaos rig (deploy/chaos).
#
# BUILD CONTEXT IS THE PARENT OF BOTH REPOS (e.g. ~/NotWork), because fathom
# depends on filo as a path dep ({:filo, path: "../filo"}) and both trees must
# be inside the context. docker-compose.yml sets `context: ../../..` +
# `dockerfile: fathom/deploy/chaos/Dockerfile`; Dockerfile.dockerignore next to
# this file whitelists only fathom/ and filo/ (BuildKit per-Dockerfile ignore).

ARG BUILDER_IMAGE=hexpm/elixir:1.20.4-erlang-28.5.0.7-debian-bookworm-20260918-slim
ARG RUNNER_IMAGE=debian:bookworm-slim

FROM ${BUILDER_IMAGE} AS builder

# build-essential: exqlite compiles its SQLite NIF via elixir_make.
RUN apt-get update -y && apt-get install -y build-essential git curl \
  && apt-get clean && rm -f /var/lib/apt/lists/*_*

# Rust, for native/fathom_udf — the loadable SQLite extension supplying Django's UDFs
# (expert review 2026-08-01 #19). Without it `mix compile.fathom_udf` SKIPS (by design, so a
# contributor without Rust can still build fathom) and the rig would come up silently lacking
# django_date_extract / Trunc* / regexp — a difference between the rig and production that the
# rig exists to NOT have.
# Toolchain pinned to the version the crate is developed against (`rustc --version` on the
# build host, 1.98.0), not `stable`: `stable` moves under the image's feet, so the same
# Dockerfile could produce a different extension binary on each rebuild.
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
# The Rust crate `mix compile.fathom_udf` builds into priv/sqlite_ext/. Copied AFTER deps.compile
# so a crate edit does not bust the dependency layer cache.
COPY fathom/native native
# Rig-only: compile the web endpoint for plaintext LAN access (SSL-force + WS origin check off),
# so each node's admin dashboard is reachable from another host over http. force_ssl is a
# compile-time config key, so this must be a BUILD-time env (see config/prod.exs). Never real prod.
# Placed here (not earlier) so the dependency layers stay cached — only the app recompiles.
ENV WEB_INSECURE_LOCAL=1
# Compile FIRST: colocated LiveView hooks are extracted into the build dir at
# compile time and the esbuild bundle imports them (phoenix-colocated/fathom).
# The cargo target dir and registry are BuildKit cache mounts, NOT image layers. Without them,
# every lib/ edit (which invalidates this RUN, since COPY lib sits above it) re-ran cargo from
# scratch, because the crate's target/ was never in an image layer to reuse. With them cargo
# is incremental across builds, so a lib/-only change recompiles only the Elixir app plus a
# cargo no-op. The built .so is still copied into priv/sqlite_ext/ inside the image, so the
# release carries it. `mix release` below also runs the :fathom_udf compiler, hence the same mounts.
RUN --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,target=/usr/local/cargo/git \
    --mount=type=cache,target=/src/fathom/native/fathom_udf/target \
    mix compile
# Fetch the tailwind binary with curl first, so `tailwind.install --if-missing` finds it and skips
# its own download. GitHub's release redirect now carries a ~9 KB header line (2026-10-24: 11.5 KB
# of headers in all), past the 10 KB cap of the httpc client mix tailwind uses, which fails the
# build with `header_too_long`. The path and URL are the ones `Tailwind.bin_path/0` and its default
# base URL produce on a glibc linux host; the version is the one config.exs pins.
RUN TW_VER=$(grep -A1 'config :tailwind' config/config.exs | grep -oE '[0-9]+\.[0-9]+\.[0-9]+') && \
    TW_ARCH=$(uname -m | sed -e 's/aarch64/arm64/' -e 's/x86_64/x64/') && \
    curl -fsSL -o "_build/tailwind-linux-$TW_ARCH-$TW_VER" \
      "https://github.com/tailwindlabs/tailwindcss/releases/download/v$TW_VER/tailwindcss-linux-$TW_ARCH" && \
    chmod +x "_build/tailwind-linux-$TW_ARCH-$TW_VER"
# Downloads the esbuild binary on first run, builds + digests assets
# (config/prod.exs expects priv/static/cache_manifest.json at endpoint boot).
RUN mix assets.setup && mix assets.deploy

# Runtime config changes shouldn't bust the compile cache.
COPY fathom/config/runtime.exs config/
# rel/vm.args.eex carries the VM capacity flags (+SDio, +Q, +IOt/+IOp — expert review 2026-07-24
# #2). WITHOUT this COPY `mix release` silently generates a default vm.args and every one of those
# flags is absent from the image: the rig booted with dirty_io=10 (the ERTS default) while the
# release was believed to be running 64. Verified by reading the limits back out of a booted node,
# which is the only way to catch it — a rendered-template test passes either way.
COPY fathom/rel rel
# Same cache mounts as `mix compile` above: mix release re-runs the :fathom_udf compiler, which
# calls cargo. Without the mounts that is a second from-scratch crate build in this layer.
RUN --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,target=/usr/local/cargo/git \
    --mount=type=cache,target=/src/fathom/native/fathom_udf/target \
    mix release

# LazyFS (https://github.com/dsrhaslab/lazyfs), the FUSE filesystem Jepsen uses to lose un-fsynced
# writes. Only the LAZYFS=1 overlay (docker-compose.lazyfs.yml) mounts it; a plain `up` never runs
# it. Pinned to a tag so a rebuild cannot change what "power loss" means under the test.
FROM ${RUNNER_IMAGE} AS lazyfs
RUN apt-get update -y && apt-get install -y g++ cmake git libfuse3-dev pkg-config \
  && apt-get clean && rm -f /var/lib/apt/lists/*_*
RUN git clone --depth 1 --branch 0.3.1 https://github.com/dsrhaslab/lazyfs /lazyfs \
  && cd /lazyfs/libs/libpcache && ./build.sh \
  && cd /lazyfs/lazyfs && ./build.sh \
  && mkdir -p /out/lib && cp /lazyfs/lazyfs/build/lazyfs /out/ \
  && find /lazyfs/libs/libpcache/build -name '*.so*' -exec cp -a {} /out/lib/ \;

FROM ${RUNNER_IMAGE}

# curl: compose healthchecks + in-container debugging. fuse3: LazyFS (see the lazyfs stage).
RUN apt-get update -y && \
  apt-get install -y libstdc++6 openssl libncurses6 locales ca-certificates curl fuse3 && \
  apt-get clean && rm -f /var/lib/apt/lists/*_* && \
  sed -i '/en_US.UTF-8/s/^# //g' /etc/locale.gen && locale-gen && \
  sed -i 's/^#user_allow_other/user_allow_other/' /etc/fuse.conf

COPY --from=lazyfs /out/lazyfs /usr/local/bin/lazyfs
COPY --from=lazyfs /out/lib/ /usr/local/lib/
RUN ldconfig && ! ldd /usr/local/bin/lazyfs | grep 'not found'
COPY fathom/deploy/chaos/lazyfs.toml /etc/lazyfs.toml
COPY fathom/deploy/chaos/lazyfs-mount.sh /usr/local/bin/lazyfs-mount

ENV LANG=en_US.UTF-8 LANGUAGE=en_US:en LC_ALL=en_US.UTF-8

WORKDIR /app
RUN useradd --create-home app && mkdir -p /data/shards /data/warm && chown -R app /app /data
USER app

COPY --from=builder --chown=app:app /src/fathom/_build/prod/rel/fathom ./

CMD ["/app/bin/fathom", "start"]
