# Common settings and helpers used by both spark_clean.py and ray_clean.py

import argparse
import glob
import json
import os
import time

# project folder is mounted at /opt/a3 in the containers
PROJECT_DIR = os.path.dirname(os.path.abspath(__file__))
TRIPS_DIR = os.path.join(PROJECT_DIR, "data", "trips")
ZONES_CSV = os.path.join(PROJECT_DIR, "data", "lookup", "taxi_zone_lookup.csv")
OUTPUT_DIR = os.path.join(PROJECT_DIR, "output")
RESULTS_DIR = os.path.join(PROJECT_DIR, "results", "timings")

# 200 is the default in both spark and ray
SHUFFLE_PARTITIONS = 200

# keys for the ray dedup repartition
DEDUP_PARTITION_KEYS = [
    "tpep_pickup_datetime",
    "tpep_dropoff_datetime",
    "PULocationID",
    "DOLocationID",
]

# column types differ between months, so every file is cast to these
LONG_COLUMNS = [
    "VendorID",
    "passenger_count",
    "RatecodeID",
    "PULocationID",
    "DOLocationID",
    "payment_type",
]
DOUBLE_COLUMNS = [
    "trip_distance",
    "fare_amount",
    "extra",
    "mta_tax",
    "tip_amount",
    "tolls_amount",
    "improvement_surcharge",
    "total_amount",
    "congestion_surcharge",
    "airport_fee",
]
STRING_COLUMNS = ["store_and_fwd_flag"]
TIMESTAMP_COLUMNS = ["tpep_pickup_datetime", "tpep_dropoff_datetime"]

TRIP_COLUMNS = [
    "VendorID",
    "tpep_pickup_datetime",
    "tpep_dropoff_datetime",
    "passenger_count",
    "trip_distance",
    "RatecodeID",
    "store_and_fwd_flag",
    "PULocationID",
    "DOLocationID",
    "payment_type",
    "fare_amount",
    "extra",
    "mta_tax",
    "tip_amount",
    "tolls_amount",
    "improvement_surcharge",
    "total_amount",
    "congestion_surcharge",
    "airport_fee",
]

RENAME_MAP = {"Airport_fee": "airport_fee"}

ZONE_RENAME_MAP = {
    "Borough": "pickup_borough",
    "Zone": "pickup_zone",
    "service_zone": "pickup_service_zone",
}

FINAL_COLUMNS = TRIP_COLUMNS + [
    "pickup_date",
    "pickup_hour",
    "trip_duration_sec",
    "pickup_borough",
    "pickup_zone",
    "pickup_service_zone",
    "avg_speed_mph",
]

MIN_DURATION_SEC = 0
MIN_DISTANCE_MILES = 0.0


# UDF, plain python so both frameworks run the same code
def avg_speed_mph(trip_distance_miles, trip_duration_sec):
    if trip_distance_miles is None or trip_duration_sec is None:
        return None
    if trip_duration_sec <= 0:
        return None
    duration_hours = trip_duration_sec / 3600.0
    speed = trip_distance_miles / duration_hours
    return float(speed)


def list_trip_files(trips_dir, max_files):
    files = sorted(glob.glob(os.path.join(trips_dir, "*.parquet")))
    if max_files and max_files > 0:
        files = files[:max_files]
    if not files:
        raise FileNotFoundError(f"No parquet files found in {trips_dir}")
    return files


def parse_args(framework):
    parser = argparse.ArgumentParser(description=f"{framework} cleaning pipeline")
    parser.add_argument(
        "--max-files",
        type=int,
        default=0,
        help="number of files to read, 0 = all",
    )
    parser.add_argument(
        "--label",
        default="run",
        help="run name, e.g. run1",
    )
    parser.add_argument("--trips-dir", default=TRIPS_DIR)
    parser.add_argument("--zones-csv", default=ZONES_CSV)
    parser.add_argument(
        "--output-dir", default=os.path.join(OUTPUT_DIR, framework.lower())
    )
    parser.add_argument(
        "--hold-ui",
        action="store_true",
        help="wait before exiting so the UI stays up",
    )
    return parser.parse_args()


class StageTimer:
    def __init__(self):
        self.seconds = {}
        self.rows = {}
        self._start = None
        self._name = None

    def start(self, name):
        self._name = name
        print(f"\n=== Stage: {name} ===", flush=True)
        self._start = time.perf_counter()

    def stop(self, rows=None):
        elapsed = time.perf_counter() - self._start
        self.seconds[self._name] = round(elapsed, 3)
        if rows is not None:
            self.rows[self._name] = int(rows)
        print(f"    {self._name}: {elapsed:.2f} s, rows = {rows}", flush=True)


def save_timings(framework, args, timer, n_files, extra_info):
    os.makedirs(RESULTS_DIR, exist_ok=True)
    stage_names = ["ingestion", "cleansing", "join", "udf", "export"]
    stage_total = sum(timer.seconds[name] for name in stage_names)

    record = {
        "framework": framework,
        "label": args.label,
        "n_files": n_files,
        "finished_at": time.strftime("%Y-%m-%d %H:%M:%S"),
        "stage_seconds": {name: timer.seconds[name] for name in stage_names},
        "total_seconds": round(timer.seconds["total"], 3),
        "sum_of_stages_seconds": round(stage_total, 3),
        "startup_seconds": timer.seconds.get("startup"),
        "rows": timer.rows,
        "info": extra_info,
    }
    path = os.path.join(RESULTS_DIR, f"{framework.lower()}_{args.label}.json")
    with open(path, "w") as f:
        json.dump(record, f, indent=2)
    print(f"\nTimings written to {path}")
    print(json.dumps(record, indent=2))
    return path
