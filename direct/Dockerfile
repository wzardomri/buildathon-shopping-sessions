FROM rust:1.85.1-slim-bookworm@sha256:9f841bbe9e7d8e37ceb96ed907265a3a0df7f44e3737d0b100e7907a679acb36 AS build
WORKDIR /build
COPY Cargo.toml Cargo.lock ./
COPY src/ src/
ARG RUSTFLAGS="-C target-cpu=x86-64-v3"
RUN cargo build --release --locked

FROM debian:bookworm-slim@sha256:734a72b3d6142a71993b4bc580ca01d8ca687f77a2c422d96a961d3376b179bc
COPY --from=build /build/target/release/shopping-sessions /usr/local/bin/shopping-sessions
ENTRYPOINT ["/usr/local/bin/shopping-sessions", "/data", "/output"]
