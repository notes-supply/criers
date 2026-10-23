# Release image: Debian 13 builder and runtime share glibc 2.41.
# The Rust toolchain date is read from mise.toml, the single compiler pin.

FROM docker.io/library/rust:1-slim-trixie@sha256:2752b332db73fdbb7dc576f06c82ed1f312005784ef913d7e04a28f5f55dc581 AS build
WORKDIR /src
RUN apt-get update --error-on=any \
    && apt-get install -y --no-install-recommends --only-upgrade ca-certificates \
    && rm -rf /var/lib/apt/lists/*
COPY mise.toml ./
RUN toolchain="$(sed -n 's/^rust = { version = "\([^"]*\)".*/\1/p' mise.toml)" \
    && test -n "$toolchain" \
    && rustup toolchain install "$toolchain" --profile minimal --no-self-update \
    && rustup default "$toolchain"
COPY Cargo.toml Cargo.lock ./
COPY crates crates
COPY examples examples
RUN --mount=type=cache,id=criers-cargo-registry,target=/usr/local/cargo/registry \
    --mount=type=cache,id=criers-target-trixie,target=/src/target \
    cargo build --release --locked --package criers \
    && install -D -m 0755 target/release/criers /out/usr/local/bin/criers \
    && install -d -m 0750 /out/var/lib/criers

FROM gcr.io/distroless/cc-debian13:nonroot@sha256:83b9b50586253821cf63ffcec87d3d94265a093ad30da232039af962dd8f4edf
COPY --from=build /out/usr/local/bin/criers /usr/local/bin/criers
# Seeded ownership lets a fresh named volume start writable for the non-root user.
COPY --from=build --chown=65532:65532 /out/var/lib/criers /var/lib/criers
USER 65532:65532
EXPOSE 8080 9090
ENTRYPOINT ["/usr/local/bin/criers"]
CMD ["serve", "--config", "/etc/criers/criers.toml"]
