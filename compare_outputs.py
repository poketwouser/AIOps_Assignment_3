# Check that output/spark and output/ray have the same data
# (row count, schema, sorted rows compared per pickup_hour, checksum)

import argparse
import hashlib
import json
import os
import sys

import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.dataset as ds

from pipeline_common import FINAL_COLUMNS, OUTPUT_DIR, PROJECT_DIR

SPEED_COLUMN = "avg_speed_mph"
SORT_COLUMNS = [c for c in FINAL_COLUMNS if c != SPEED_COLUMN]


def open_dataset(path):
    return ds.dataset(path, format="parquet")


def normalize(table):
    arrays = []
    for name in FINAL_COLUMNS:
        column = table.column(name)
        if pa.types.is_timestamp(column.type):
            column = column.cast(pa.timestamp("us"))
        arrays.append(column)
    return pa.table(arrays, names=FINAL_COLUMNS)


def describe_schema(schema):
    result = {}
    for field in schema:
        result[field.name] = str(field.type)
    return result


def normalized_type(type_text):
    # spark writes tz=UTC, ray doesn't
    if type_text.startswith("timestamp"):
        return "timestamp[us]"
    return type_text


def compare_schemas(left, right):
    left_types = describe_schema(left.schema)
    right_types = describe_schema(right.schema)
    ok = True
    print(f"{'column':<24} {'left':<24} {'right':<24} match")
    for name in FINAL_COLUMNS:
        lt = left_types.get(name, "MISSING")
        rt = right_types.get(name, "MISSING")
        same = normalized_type(lt) == normalized_type(rt)
        ok = ok and same
        print(f"{name:<24} {lt:<24} {rt:<24} {'yes' if same else 'NO'}")
    extra = set(left_types) ^ set(right_types)
    if extra:
        print("Columns only on one side:", extra)
        ok = False
    return ok


def compare_bucket(left, right, hour, tolerance):
    hour_filter = pc.field("pickup_hour") == hour
    left_table = normalize(left.to_table(filter=hour_filter))
    right_table = normalize(right.to_table(filter=hour_filter))

    if left_table.num_rows != right_table.num_rows:
        print(f"  hour {hour:2d}: row count differs {left_table.num_rows} vs {right_table.num_rows}")
        return False

    sort_keys = [(name, "ascending") for name in SORT_COLUMNS]
    left_table = left_table.sort_by(sort_keys)
    right_table = right_table.sort_by(sort_keys)

    for name in SORT_COLUMNS:
        if not left_table.column(name).equals(right_table.column(name)):
            print(f"  hour {hour:2d}: column {name} differs")
            return False

    left_speed = left_table.column(SPEED_COLUMN).to_numpy()
    right_speed = right_table.column(SPEED_COLUMN).to_numpy()
    close = np.isclose(left_speed, right_speed, rtol=tolerance, atol=tolerance)
    if not close.all():
        print(f"  hour {hour:2d}: {(~close).sum()} speed values differ by more than {tolerance}")
        return False

    max_diff = float(np.max(np.abs(left_speed - right_speed))) if len(left_speed) else 0.0
    print(f"  hour {hour:2d}: {left_table.num_rows:>10,} rows identical (max speed diff {max_diff:.2e})")
    return True


def checksum(dataset):
    row_hashes = []
    for batch in dataset.to_batches(columns=FINAL_COLUMNS, batch_size=1_000_000):
        table = normalize(pa.Table.from_batches([batch]))
        frame = table.to_pandas()
        frame[SPEED_COLUMN] = frame[SPEED_COLUMN].round(6)
        hashes = pd.util.hash_pandas_object(frame, index=False).to_numpy()
        row_hashes.append(hashes)
    all_hashes = np.sort(np.concatenate(row_hashes))
    return hashlib.sha256(all_hashes.tobytes()).hexdigest()


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--left", default=os.path.join(OUTPUT_DIR, "spark"))
    parser.add_argument("--right", default=os.path.join(OUTPUT_DIR, "ray"))
    parser.add_argument("--tolerance", type=float, default=1e-9)
    parser.add_argument(
        "--report", default=os.path.join(PROJECT_DIR, "results", "parity_report.json")
    )
    args = parser.parse_args()

    left = open_dataset(args.left)
    right = open_dataset(args.right)
    results = {"left": args.left, "right": args.right}

    print("== 1. Row count")
    left_rows = left.count_rows()
    right_rows = right.count_rows()
    results["left_rows"] = left_rows
    results["right_rows"] = right_rows
    results["row_count_match"] = left_rows == right_rows
    print(f"left  {left_rows:,}\nright {right_rows:,}\nmatch: {results['row_count_match']}")

    print("\n== 2. Schema")
    results["schema_match"] = compare_schemas(left, right)

    print("\n== 3. Sorted equality, bucket by pickup_hour")
    all_buckets_ok = True
    for hour in range(24):
        if not compare_bucket(left, right, hour, args.tolerance):
            all_buckets_ok = False
    results["sorted_equality"] = all_buckets_ok

    print("\n== 4. Checksum (SHA-256 of sorted row hashes)")
    left_sum = checksum(left)
    right_sum = checksum(right)
    results["left_checksum"] = left_sum
    results["right_checksum"] = right_sum
    results["checksum_match"] = left_sum == right_sum
    print(f"left  {left_sum}\nright {right_sum}\nmatch: {results['checksum_match']}")

    everything_ok = (
        results["row_count_match"]
        and results["schema_match"]
        and results["sorted_equality"]
        and results["checksum_match"]
    )
    results["outputs_match"] = everything_ok

    os.makedirs(os.path.dirname(args.report), exist_ok=True)
    with open(args.report, "w") as f:
        json.dump(results, f, indent=2)

    print("\nRESULT:", "OUTPUTS MATCH" if everything_ok else "OUTPUTS DIFFER")
    print(f"Report written to {args.report}")
    sys.exit(0 if everything_ok else 1)


if __name__ == "__main__":
    main()
