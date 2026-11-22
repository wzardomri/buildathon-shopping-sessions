# Shopping Sessions

SophiaOmri's entry for the [Databricks Shopping Sessions track](https://www.joinbuilders.org/buildathon/tracks/shopping-sessions).

The Rust program parses the track's LF-delimited CSV dialect, selects the exact earliest-ingest event for each ID, and resolves linked identities with union-find. It computes sessions and attribution by canonical person. Repeated identical click values are handled by idempotent credit and assist aggregation, including assists from inclusive seven-day click intervals. Parallel writers produce both typed Parquet tables directly.

On Linux/amd64, the cold-input gate selects 16 MiB event file waves. Eligible event files use direct reads with bounded per-worker buffers. Warm input uses memory mapping, which also handles cold ranges when a direct descriptor or a complete read is unavailable.

## Run

```sh
docker build --platform linux/amd64 -t shopping-sessions .
mkdir -p output
docker run --rm --network none --read-only --cpus 8 --memory 24g \
  -e NCPU=8 -v "$PWD/data:/data:ro" -v "$PWD/output:/output" shopping-sessions
```

The fixture runner in the official starter can run the image with the scorer's other flags. To iterate locally, build with `cargo build --release --locked` and run `target/release/shopping-sessions DATA_DIR OUTPUT_DIR`.

## Sources and dependencies

- [Builders' published rules and fixtures](https://www.joinbuilders.org/buildathon/tracks/shopping-sessions/files/rules.md?inline=1) specify the input and output.
- The calendar arithmetic follows Howard Hinnant's [civil date algorithms](https://howardhinnant.github.io/date_algorithms.html).
- The byte-hash mixing step is based on [FxHash](https://github.com/rust-lang/rustc-hash).
- The Rust core uses the standard library and the platform C library. The writers use Apache Parquet 54.3.1, Bytes 1.10.1, and Snap 1.1.2. Dependencies are pinned in `Cargo.lock`. [Parquet](https://docs.rs/parquet/54.3.1/parquet/) [Bytes](https://docs.rs/bytes/1.10.1/bytes/) [Snap](https://docs.rs/snap/1.1.2/snap/)
