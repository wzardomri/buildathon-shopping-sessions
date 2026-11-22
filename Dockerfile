FROM rust:1.85.1-slim-bookworm@sha256:9f841bbe9e7d8e37ceb96ed907265a3a0df7f44e3737d0b100e7907a679acb36 AS source-check
WORKDIR /source
COPY . ./
RUN sha256sum -c manifest.sha256 && cd scored && sha256sum -c source.sha256 && cd ../direct && sha256sum -c source.sha256

FROM rust:1.85.1-slim-bookworm@sha256:9f841bbe9e7d8e37ceb96ed907265a3a0df7f44e3737d0b100e7907a679acb36 AS scored
WORKDIR /build
COPY --from=source-check /source/scored/Cargo.toml /source/scored/Cargo.lock ./
COPY --from=source-check /source/scored/src/ src/
ARG RUSTFLAGS="-C target-cpu=x86-64-v3"
RUN cargo build --release --locked
RUN echo 'bca1bbc5638138f05a7e4dae134e1c242f5b04dd7faab69a06f5ce53cd8d7eec  target/release/shopping-sessions' | sha256sum -c -

FROM rust:1.85.1-slim-bookworm@sha256:9f841bbe9e7d8e37ceb96ed907265a3a0df7f44e3737d0b100e7907a679acb36 AS direct
WORKDIR /build
COPY --from=source-check /source/direct/Cargo.toml /source/direct/Cargo.lock ./
COPY --from=source-check /source/direct/src/ src/
ARG RUSTFLAGS="-C target-cpu=x86-64-v3"
RUN cargo build --release --locked
RUN echo '6236017f252ffd05213cdde615727ff52e3e577d1744425d46b956c9b83bb495  target/release/shopping-sessions' | sha256sum -c -

FROM rust:1.85.1-slim-bookworm@sha256:9f841bbe9e7d8e37ceb96ed907265a3a0df7f44e3737d0b100e7907a679acb36 AS launcher
WORKDIR /build
COPY launcher.rs ./
RUN rustc --edition=2024 -C opt-level=2 -C panic=abort -C strip=symbols launcher.rs -o shopping-sessions

FROM debian:bookworm-slim@sha256:734a72b3d6142a71993b4bc580ca01d8ca687f77a2c422d96a961d3376b179bc
COPY --from=scored /build/target/release/shopping-sessions /usr/local/bin/shopping-sessions-mmap
COPY --from=direct /build/target/release/shopping-sessions /usr/local/bin/shopping-sessions-direct
COPY --from=launcher /build/shopping-sessions /usr/local/bin/shopping-sessions
ENTRYPOINT ["/usr/local/bin/shopping-sessions", "/data", "/output"]
