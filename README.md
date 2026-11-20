# Shopping Sessions

SophiaOmri's entry for the [Databricks Shopping Sessions track](https://www.joinbuilders.org/buildathon/tracks/shopping-sessions).

The Rust program parses the track's LF-delimited CSV dialect and partitions rows by event or click ID for global deduplication. It resolves the linked-ID graph with union-find, groups the surviving events and clicks by canonical person, and computes sessions and campaign metrics in parallel. It counts campaign assists by merging each person's inclusive seven-day click intervals and counting session starts by UTC day. A Python wrapper writes both required typed Parquet tables.

## Run

```sh
docker build --platform linux/amd64 -t shopping-sessions .
mkdir -p output scratch
docker run --rm --network none --read-only --cpus 8 --memory 24g \
  -e NCPU=8 -v "$PWD/data:/data:ro" -v "$PWD/output:/output" \
  -v "$PWD/scratch:/scratch" shopping-sessions
```

The fixture runner in the official starter can run the image with the scorer's other flags. To iterate on the core locally, build with `cargo build --release --locked --offline` and run `target/release/shopping-sessions DATA_DIR SCRATCH_OUTPUT`. With Python and PyArrow 19.0.1 installed, `python3 write_parquet.py SCRATCH_OUTPUT OUTPUT_DIR` writes both result tables.

## Sources and dependencies

- [Builders' published rules and fixtures](https://www.joinbuilders.org/buildathon/tracks/shopping-sessions/files/rules.md?inline=1) specify the input and output.
- The calendar arithmetic follows Howard Hinnant's [civil date algorithms](https://howardhinnant.github.io/date_algorithms.html).
- The byte-hash mixing step is based on [FxHash](https://github.com/rust-lang/rustc-hash).
- The Rust core uses the Rust standard library and the platform C library. The writer uses [Apache Arrow PyArrow 19.0.1](https://arrow.apache.org/docs/19.0/python/), including its Parquet writer.
