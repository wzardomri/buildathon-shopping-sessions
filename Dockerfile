FROM rust:1.85.1-slim-bookworm AS build
WORKDIR /build
COPY Cargo.toml Cargo.lock ./
COPY src/ src/
RUN cargo build --release --locked --offline

FROM python:3.12.9-slim-bookworm
ENV PYTHONDONTWRITEBYTECODE=1
RUN python -m pip install --no-cache-dir pyarrow==19.0.1
WORKDIR /pipeline
COPY --from=build /build/target/release/shopping-sessions /usr/local/bin/shopping-sessions
COPY run.sh write_parquet.py ./
ENTRYPOINT ["/bin/sh", "/pipeline/run.sh"]
