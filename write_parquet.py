"""Convert the sessionizer's typed scratch CSVs to the submission Parquet tables."""

import os
from pathlib import Path
import sys

import pyarrow as pa
import pyarrow.csv as csv
import pyarrow.parquet as parquet


SCHEMAS = {
    "user_daily": pa.schema([
        ("user_id", pa.string()), ("day", pa.date32()), ("sessions", pa.int64()),
        ("events", pa.int64()), ("revenue_cents", pa.int64()), ("top_category", pa.string()),
    ]),
    "campaign_daily": pa.schema([
        ("campaign_id", pa.string()), ("channel", pa.string()), ("day", pa.date32()),
        ("sessions", pa.int64()), ("purchasing_sessions", pa.int64()),
        ("revenue_cents", pa.int64()), ("assisted_sessions", pa.int64()),
    ]),
}


def read_csv(path, schema, *, quoted_empty_is_null=False):
    table = csv.read_csv(
        path,
        read_options=csv.ReadOptions(block_size=4 << 20),
        parse_options=csv.ParseOptions(newlines_in_values=True),
        convert_options=csv.ConvertOptions(
            column_types=schema,
            null_values=[""],
            strings_can_be_null=True,
            quoted_strings_can_be_null=quoted_empty_is_null,
        ),
    )
    if table.schema != schema:
        raise ValueError(f"{path}: expected {schema}, got {table.schema}")
    return table


def convert(scratch, output):
    pa.set_cpu_count(int(os.environ.get("NCPU", "8")))
    for name, schema in SCHEMAS.items():
        folder = output / name
        folder.mkdir(parents=True, exist_ok=True)
        table = read_csv(scratch / f"{name}.csv", schema)
        parquet.write_table(table, folder / "part-0.parquet", compression="snappy")
        del table


if __name__ == "__main__":
    convert(Path(sys.argv[1]), Path(sys.argv[2]))
