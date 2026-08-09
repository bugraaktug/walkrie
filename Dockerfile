# syntax=docker/dockerfile:1
#
# Multi-stage build: compile walkrie (and statically link llama.cpp/ggml,
# same as the .deb package — see packaging/debian/rules) in a full build
# image, then copy just the binary into a minimal runtime image.
#
# Build:
#   docker build -t walkrie:1.2.1-alpha2 .
#
# third_party/llama.cpp must already be checked out (a plain `git clone`
# without --recurse-submodules leaves it empty — see TECHNICAL.md's
# Compilation section):
#   git submodule update --init --recursive

# ---- build stage ----
FROM debian:12-slim AS build

RUN apt-get update && apt-get install -y --no-install-recommends \
        cmake \
        make \
        g++ \
        git \
        pkg-config \
        libpq-dev \
        libevent-dev \
        libcurl4-openssl-dev \
        nlohmann-json3-dev \
        libspdlog-dev \
        libsqlite3-dev \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /src
COPY . .

# Fail loudly here rather than deep inside a confusing CMake error if the
# submodule wasn't checked out before `docker build` was run.
RUN test -f third_party/llama.cpp/CMakeLists.txt || \
    (echo "third_party/llama.cpp is empty — run 'git submodule update --init --recursive' before 'docker build'" >&2 && exit 1)

# Only the `walkrie`/`walkrie_worker` targets — skips walkrie_tests/benches/
# demo binaries, which this image doesn't need and which would otherwise
# slow the build. walkrie_worker is required at runtime: walkrie spawns it
# (from the same directory it's running from) to drain `backfill = true`
# sources — see TECHNICAL.md's Initial Backfill Scan section.
RUN cmake -B build -DCMAKE_BUILD_TYPE=RelWithDebInfo -DBUILD_SHARED_LIBS=OFF \
    && cmake --build build --target walkrie walkrie_worker -j"$(nproc)"

# ---- runtime stage ----
FROM debian:12-slim AS runtime

LABEL org.opencontainers.image.title="walkrie" \
      org.opencontainers.image.description="PostgreSQL WAL to vector embedding sync engine" \
      org.opencontainers.image.source="https://github.com/bugraaktug/walkrie" \
      org.opencontainers.image.version="1.2.1-alpha2"

# Runtime shared libs only — llama.cpp/ggml are statically linked in
# (BUILD_SHARED_LIBS=OFF above), same as the .deb package's Depends, plus
# two transitive ones the .deb's control file is also missing (see
# packaging/debian/control's libspdlog-dev fix): Debian's spdlog package
# links spdlog_header_only against system libfmt rather than bundling its
# own copy, and ggml's OpenMP-based CPU backend needs libgomp even though
# ggml itself is statically linked in.
RUN apt-get update && apt-get install -y --no-install-recommends \
        libpq5 \
        libevent-2.1-7 \
        libcurl4 \
        libfmt9 \
        libgomp1 \
        libsqlite3-0 \
        ca-certificates \
    && rm -rf /var/lib/apt/lists/* \
    && useradd --system --create-home --home-dir /var/lib/walkrie --shell /usr/sbin/nologin walkrie \
    && mkdir -p /etc/walkrie /var/lib/walkrie/models /var/lib/walkrie/backfill /var/log/walkrie /usr/share/doc/walkrie \
    && chown -R walkrie:walkrie /var/lib/walkrie /var/log/walkrie

COPY --from=build /src/build/walkrie /usr/bin/walkrie
COPY --from=build /src/build/walkrie_worker /usr/bin/walkrie_worker
COPY --chown=walkrie:walkrie config_sample.toml /usr/share/doc/walkrie/config.toml.example
COPY --chown=walkrie:walkrie README.md TECHNICAL.md PERFORMANCE.md /usr/share/doc/walkrie/

# No config is placed at /etc/walkrie/config.toml on purpose: config_sample.toml
# carries a plaintext placeholder password and a `host = "localhost"` that
# means something different inside a container. Bind-mount your own config
# there instead (see README.md's Docker section) — walkrie's own startup
# validation (see TECHNICAL.md) gives a clear error rather than silently
# running against the wrong database if you forget.
VOLUME ["/etc/walkrie", "/var/lib/walkrie/models", "/var/log/walkrie"]

USER walkrie
WORKDIR /var/lib/walkrie

# Foreground mode (`-f`) — same reasoning as running under systemd (see
# README.md/TECHNICAL.md): no double-fork daemonizing, so `docker stop`'s
# SIGTERM reaches walkrie directly as PID 1 for its documented graceful
# shutdown (drains the event queue) instead of being sent to a detached
# grandchild.
ENTRYPOINT ["/usr/bin/walkrie"]
CMD ["-f", "-c", "/etc/walkrie/config.toml"]
