#!/bin/sh
set -eu

scratch=$(mktemp -d "${SCRATCH_DIR:-/scratch}/shopping.XXXXXX")
trap 'rm -rf "$scratch"' EXIT
shopping-sessions "${DATA_DIR:-/data}" "$scratch"
python3 /pipeline/write_parquet.py "$scratch" "${OUTPUT_DIR:-/output}"
