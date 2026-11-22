# Shopping Sessions

SophiaOmri's entry for the [Databricks Shopping Sessions track](https://www.joinbuilders.org/buildathon/tracks/shopping-sessions).

The Rust program parses the track's LF-delimited CSV dialect, deduplicates events and clicks globally, and resolves linked identities with union-find. It computes sessions and attribution by canonical person, including campaign assists from inclusive seven-day click intervals. Parallel writers produce both typed Parquet tables directly.

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
- The Rust core uses the standard library and the platform C library. The writers use [Apache Parquet 54.3.1](https://docs.rs/parquet/54.3.1/parquet/), [Bytes 1.10.1](https://docs.rs/bytes/1.10.1/bytes/), and Snap 1.1.2 for Snappy compression. Their dependencies are pinned in `Cargo.lock`.
- The inline owner fingerprint uses the first two [XXH64 primes](https://github.com/Cyan4973/xxHash/blob/v0.8.3/xxhash.h).
