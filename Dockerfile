# syntax=docker/dockerfile:1
# local-path-csi: a static musl binary, and rsync with the libraries it needs - volumes travel between
# the nodes with it. Nothing runs under emulation: the toolchain stage is native and cross-compiles
# for the target platform with zig as the C compiler and linker (cargo-zigbuild) - ring and zstd are
# C, and rsync is installed from the Alpine packages of the target architecture into a directory.
#
# Bumping: the tags are for readability, the digests and checksums pin the content
# (`crane digest <image>:<tag>`, https://ziglang.org/download/index.json). The Alpine packages
# are the current ones of the release the image tag names.

# ---- toolchain: Rust + zig + cargo-zigbuild + musl targets (identical to metrics-proxy: the layers are shared) ----
FROM --platform=$BUILDPLATFORM docker.io/library/rust:1.100.0-bookworm@sha256:ade39a710ef85f3506cffcfec3e8a24ba090badef1dff2fb798f71d878833fea AS toolchain
ARG ZIG_VERSION=0.17.1
ARG ZIG_SHA256_X86_64=3581863e3cbcfd0fce0efc682ce7e9e93da96668a977791d356a26d2a7e9b17a
ARG ZIG_SHA256_AARCH64=a1546d5646eb184437918d6635ca6b762a0e889eac09cef720ee7a1ed3fe7795
ARG CARGO_ZIGBUILD_VERSION=0.24.1
# zig for the architecture of the build machine.
RUN set -eux; \
    arch="$(uname -m)"; \
    case "$arch" in \
      x86_64) sum="$ZIG_SHA256_X86_64" ;; \
      aarch64) sum="$ZIG_SHA256_AARCH64" ;; \
      *) echo "unsupported build architecture $arch" >&2; exit 1 ;; \
    esac; \
    curl -fsSL -o /tmp/zig.tar.xz "https://ziglang.org/download/${ZIG_VERSION}/zig-${arch}-linux-${ZIG_VERSION}.tar.xz"; \
    echo "${sum}  /tmp/zig.tar.xz" | sha256sum -c -; \
    mkdir -p /opt/zig; \
    tar -xJf /tmp/zig.tar.xz -C /opt/zig --strip-components=1; \
    rm /tmp/zig.tar.xz; \
    ln -s /opt/zig/zig /usr/local/bin/zig; \
    zig version
RUN cargo install --locked cargo-zigbuild --version "${CARGO_ZIGBUILD_VERSION}" \
 && rm -rf /usr/local/cargo/registry
RUN rustup target add x86_64-unknown-linux-musl aarch64-unknown-linux-musl

# ---- dependencies: built from the manifests alone, so that a change of the code finds them in the cache ----
FROM toolchain AS deps
# The tests move a volume between two agents with a real rsync.
RUN apt-get update && apt-get install -y --no-install-recommends rsync && rm -rf /var/lib/apt/lists/*
WORKDIR /src
COPY Cargo.toml Cargo.lock build.rs ./
COPY proto proto
# Empty targets stand in for the code.
RUN mkdir src tests && echo 'fn main() {}' >src/main.rs && touch src/lib.rs && echo 'fn main() {}' >tests/init.rs
# SKIP_TESTS=1 is for CI, where the tests have already passed before the image is built.
ARG SKIP_TESTS=
RUN if [ -z "$SKIP_TESTS" ]; then cargo test --locked --no-run; fi

# ---- the code ----
FROM scratch AS code
COPY src /src/src
COPY tests /src/tests
COPY chart/files/crds /src/chart/files/crds

# ---- tests: natively on the build platform, as the cross-compiled binaries cannot be executed here ----
# Nothing in this stage depends on TARGETARCH: it runs once for all target platforms.
FROM deps AS test
ARG SKIP_TESTS=
COPY --from=code /src /src
# The code has to be newer than the empty targets, whatever times its files came with: cargo goes by them.
RUN find src tests -type f -exec touch {} + \
 && if [ -z "$SKIP_TESTS" ]; then cargo test --locked; fi \
 && touch /tested

# ---- build ----
FROM deps AS build
ARG TARGETARCH
RUN set -eux; \
    case "$TARGETARCH" in \
      amd64) target=x86_64-unknown-linux-musl ;; \
      arm64) target=aarch64-unknown-linux-musl ;; \
      *) echo "unsupported target architecture $TARGETARCH" >&2; exit 1 ;; \
    esac; \
    echo "$target" >/target; \
    cargo zigbuild --locked --release --target "$target"
COPY --from=code /src /src
# The image is not built before the tests have passed.
COPY --from=test /tested /tested
RUN set -eux; \
    target="$(cat /target)"; \
    find src tests -type f -exec touch {} +; \
    cargo zigbuild --locked --release --target "$target"; \
    for name in local-path-csi; do \
      bin="target/$target/release/$name"; \
      # The final image has no libc and no loader: a dynamically linked binary would not start.
      if readelf -l "$bin" | grep -q INTERP; then echo "$name: the binary is not static" >&2; exit 1; fi; \
      install -m 0755 "$bin" "/$name"; \
    done; \
    rm -rf target

# ---- rsync of the target architecture, with musl and the libraries it links ----
FROM --platform=$BUILDPLATFORM docker.io/library/alpine:3.24@sha256:f59abe656b692d901e184be3445e084786602108599d2a51d229c0f5a48c71bf AS rsync
ARG TARGETARCH
# No scripts: they are programs of the target architecture. rsync needs none of them.
RUN set -eux; \
    case "$TARGETARCH" in \
      amd64) arch=x86_64 ;; \
      arm64) arch=aarch64 ;; \
      *) echo "unsupported target architecture $TARGETARCH" >&2; exit 1 ;; \
    esac; \
    apk add --no-cache --no-scripts --root /rootfs --initdb --arch "$arch" \
      --keys-dir "/usr/share/apk/keys/$arch" --repositories-file /etc/apk/repositories rsync; \
    test -x /rootfs/usr/bin/rsync

# ---- final: no shell, no package manager ----
FROM scratch
COPY --from=rsync /rootfs /
COPY --from=build /local-path-csi /usr/bin/
# The agent finds rsync by its name.
ENV PATH=/usr/bin
USER 65532:65532
ENTRYPOINT ["/usr/bin/local-path-csi"]
