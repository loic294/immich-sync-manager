# Immich Sync Manager: production image (Rust server + built web UI).
#
# Build from the repository root: `docker build -f docker/Dockerfile -t immich-sync-manager .`
# Running and configuration: docs/install.md.

ARG RUST_VERSION=1.94.1
ARG NODE_VERSION=24
ARG DEBIAN_RELEASE=bookworm
ARG CARGO_CHEF_VERSION=0.1.78

# ---- web: the static UI (architecture-independent, so it always builds natively) ----------------------
FROM --platform=$BUILDPLATFORM node:${NODE_VERSION}-${DEBIAN_RELEASE}-slim AS web
ENV NPM_CONFIG_UPDATE_NOTIFIER=false
WORKDIR /src/web
COPY web/package.json web/package-lock.json web/.npmrc ./
RUN --mount=type=cache,target=/root/.npm npm ci --no-audit --no-fund
COPY web/ ./
RUN npm run build

# ---- chef: pinned Rust toolchain + cargo-chef ----------------------------------------------------------
FROM rust:${RUST_VERSION}-${DEBIAN_RELEASE} AS chef
ARG RUST_VERSION
ARG CARGO_CHEF_VERSION
ENV RUSTUP_TOOLCHAIN=${RUST_VERSION}
RUN --mount=type=cache,target=/usr/local/cargo/registry,sharing=locked \
    --mount=type=cache,target=/usr/local/cargo/git,sharing=locked \
    cargo install --locked --version "${CARGO_CHEF_VERSION}" cargo-chef
WORKDIR /src

FROM chef AS planner
RUN --mount=type=bind,source=rust-toolchain.toml,target=/pin/rust-toolchain.toml \
    grep -Eq "^channel[[:space:]]*=[[:space:]]*\"${RUSTUP_TOOLCHAIN}\"" /pin/rust-toolchain.toml \
    || { echo "rust-toolchain.toml pins a different compiler than RUST_VERSION=${RUSTUP_TOOLCHAIN}: update docker/Dockerfile." >&2; exit 1; }
COPY Cargo.toml Cargo.lock ./
COPY .cargo .cargo
COPY crates crates
RUN cargo chef prepare --recipe-path /recipe.json

# ---- builder: dependencies first (cached), then the server --------------------------------------------
# Low-memory machines (small NAS, Docker Desktop VMs): --build-arg CARGO_BUILD_JOBS=2.
FROM chef AS builder
ARG CARGO_BUILD_JOBS
COPY --from=planner /recipe.json /recipe.json
RUN --mount=type=cache,target=/usr/local/cargo/registry,sharing=locked \
    --mount=type=cache,target=/usr/local/cargo/git,sharing=locked \
    cargo chef cook --release --locked --recipe-path /recipe.json -p ism-server
COPY Cargo.toml Cargo.lock ./
COPY .cargo .cargo
COPY crates crates
RUN --mount=type=cache,target=/usr/local/cargo/registry,sharing=locked \
    --mount=type=cache,target=/usr/local/cargo/git,sharing=locked \
    cargo build --release --locked -p ism-server \
 && install -D -m 0755 target/release/ism-server /out/ism-server

# ---- runtime -------------------------------------------------------------------------------------------
FROM debian:${DEBIAN_RELEASE}-slim AS runtime

# curl: HEALTHCHECK; tini: PID 1; tzdata: TZ and sync windows; ca-certificates: TLS to Immich and peers.
RUN set -eu; \
    apt-get update; \
    DEBIAN_FRONTEND=noninteractive apt-get install -y --no-install-recommends ca-certificates curl tini tzdata; \
    rm -rf /var/lib/apt/lists/*

# uid/gid 1000 is only the default: run the container as the user that owns the library folders
# (compose `user: "UID:GID"`), the same user Immich uses for its external libraries.
RUN groupadd --gid 1000 ism \
 && useradd --uid 1000 --gid 1000 --no-create-home --home-dir /nonexistent --shell /usr/sbin/nologin ism \
 && install -d -o 1000 -g 1000 -m 0755 /data \
 && install -d -m 0755 /mnt /app \
 && find / -xdev -type f \( -perm -4000 -o -perm -2000 \) -exec chmod a-s {} +

COPY --from=builder /out/ism-server /app/ism-server
COPY --from=web /src/web/dist /app/web
COPY --chmod=0755 docker/entrypoint.sh docker/healthcheck.sh /app/
COPY LICENSE /app/LICENSE

ENV ISM_LISTEN=0.0.0.0:8080 \
    ISM_DATA_DIR=/data \
    ISM_WEB_DIR=/app/web \
    ISM_MOUNTS_DIR=/mnt \
    ISM_LOG=info \
    TZ=UTC \
    LANG=C.UTF-8

LABEL org.opencontainers.image.title="Immich Sync Manager" \
      org.opencontainers.image.description="Managed file operations, offline downloads and peer sync for Immich external libraries." \
      org.opencontainers.image.licenses="MIT"

USER 1000:1000
WORKDIR /app
EXPOSE 8080
HEALTHCHECK --interval=30s --timeout=5s --start-period=20s --retries=3 CMD ["/app/healthcheck.sh"]
ENTRYPOINT ["/usr/bin/tini", "--", "/app/entrypoint.sh"]
CMD ["/app/ism-server"]
