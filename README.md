# Shopping Sessions

SophiaOmri’s entry for the Databricks Shopping Sessions track. [Track and rules](https://www.joinbuilders.org/buildathon/tracks/shopping-sessions)

The Rust solver parses the track CSV dialect, selects the exact earliest-ingest event for every ID, and resolves transitive identity links with union–find. It computes sessions, campaign credit, and inclusive seven-day assists for each canonical person. Repeated identical click values are handled by idempotent credit and assist aggregation. Parallel native writers produce the two typed Parquet tables.

The Linux/amd64 image includes the previous scored solver and a version with bounded direct reads for cold event ranges. At launch, a read-only preflight checks the sizes of event CSVs and samples page residency. An observed resident page, insufficient samples, or an unsupported input shape selects the original solver. If at least 12 sampled pages are usable and all observed samples are cold, the direct solver runs its own cold gate. A mixed-cache input can have resident pages outside the sample. An ineligible descriptor or an incomplete direct read uses the same physical range through the original memory mapping.

## Run

```sh
docker build --platform linux/amd64 -t shopping-sessions .
mkdir -p output
docker run --rm --network none --read-only --cpus 8 --memory 24g \
  -e NCPU=8 -v "$PWD/data:/data:ro" -v "$PWD/output:/output" shopping-sessions
```

The root Dockerfile verifies the source manifests and both compiled solver hashes. The packaged entry receives the data and output directories as arguments. It forwards both paths and the environment to the selected solver with `exec`.

## Sources and dependencies

- The Builders specification defines the CSV input and typed Parquet output. [Published rules](https://www.joinbuilders.org/buildathon/tracks/shopping-sessions/files/rules.md?inline=1)
- The calendar arithmetic follows Howard Hinnant’s civil-date algorithms. [Algorithm reference](https://howardhinnant.github.io/date_algorithms.html)
- The byte-hash mixing step is based on FxHash. [FxHash source](https://github.com/rust-lang/rustc-hash)
- Both solver trees pin dependencies in `Cargo.lock`. They use the Rust standard library and platform C library. The writers use Parquet 54.3.1, Bytes 1.10.1 and Snap 1.1.2. [Parquet](https://docs.rs/parquet/54.3.1/parquet/) [Bytes](https://docs.rs/bytes/1.10.1/bytes/) [Snap](https://docs.rs/snap/1.1.2/snap/)
