# Ray Data version of the taxi cleaning pipeline
# python /opt/a3/ray_clean.py --label run1

import os
import shutil
import time

import numpy as np
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.csv as pa_csv
import ray
from ray.data import DataContext
from ray.data.context import ShuffleStrategy

import pipeline_common as common

# 1 GB per read task so a worker runs max 2 reads at once (was getting OOM)
READ_TASK_MEMORY = 1_000_000_000


def normalize_trip_batch(table):
    # some months have Airport_fee instead of airport_fee
    for old_name, new_name in common.RENAME_MAP.items():
        if old_name in table.column_names:
            names = [new_name if n == old_name else n for n in table.column_names]
            table = table.rename_columns(names)

    arrays = []
    for column in common.TRIP_COLUMNS:
        values = table.column(column)
        if column in common.LONG_COLUMNS:
            # safe=False to match spark cast
            values = pc.cast(values, pa.int64(), safe=False)
        elif column in common.DOUBLE_COLUMNS:
            values = pc.cast(values, pa.float64())
        elif column in common.STRING_COLUMNS:
            values = pc.cast(values, pa.string())
        else:
            values = pc.cast(values, pa.timestamp("us"))
        arrays.append(values)
    return pa.table(arrays, names=common.TRIP_COLUMNS)


def drop_null_rows(table):
    keep = None
    for column in table.column_names:
        values = table.column(column)
        is_ok = pc.is_valid(values)
        if pa.types.is_floating(values.type):
            is_ok = pc.and_(is_ok, pc.invert(pc.is_nan(values)))
        keep = is_ok if keep is None else pc.and_(keep, is_ok)
    return table.filter(keep)


def drop_duplicate_rows(table):
    distinct = table.group_by(common.TRIP_COLUMNS).aggregate([])
    return distinct.select(common.TRIP_COLUMNS)


def add_time_columns(table):
    pickup = pc.cast(table.column("tpep_pickup_datetime"), pa.timestamp("us"))
    dropoff = pc.cast(table.column("tpep_dropoff_datetime"), pa.timestamp("us"))

    # seconds, same as spark cast("long")
    pickup_sec = pc.cast(pickup, pa.int64()).to_numpy() // 1_000_000
    dropoff_sec = pc.cast(dropoff, pa.int64()).to_numpy() // 1_000_000
    duration_sec = dropoff_sec - pickup_sec

    table = table.append_column("trip_duration_sec", pa.array(duration_sec, pa.int64()))
    table = table.append_column("pickup_date", pc.strftime(pickup, format="%Y-%m-%d"))
    table = table.append_column("pickup_hour", pc.cast(pc.hour(pickup), pa.int64()))

    keep_duration = pc.greater(table.column("trip_duration_sec"), common.MIN_DURATION_SEC)
    keep_distance = pc.greater(table.column("trip_distance"), common.MIN_DISTANCE_MILES)
    return table.filter(pc.and_(keep_duration, keep_distance))


def join_with_zones(table, zones):
    joined = table.join(
        zones,
        keys="PULocationID",
        right_keys="LocationID",
        join_type="inner",
    )
    return joined


def add_speed_column(table):
    distances = table.column("trip_distance").to_pylist()
    durations = table.column("trip_duration_sec").to_pylist()

    speeds = []
    for distance, duration in zip(distances, durations):
        speeds.append(common.avg_speed_mph(distance, duration))

    return table.append_column("avg_speed_mph", pa.array(speeds, pa.float64()))


def main():
    args = common.parse_args("Ray")
    timer = common.StageTimer()

    timer.start("startup")
    ray.init(
        address="auto",
        runtime_env={"env_vars": {"PYTHONPATH": common.PROJECT_DIR}},
        logging_level="ERROR",
    )
    data_context = DataContext.get_current()
    data_context.enable_progress_bars = False
    # default HASH_SHUFFLE deadlocked with 3 GB workers
    data_context.shuffle_strategy = ShuffleStrategy.SHUFFLE_V2
    # default is 1 GiB, too big for the workers
    data_context.shuffle_input_batch_bytes = 128 * 1024 * 1024
    timer.stop()
    print("Cluster resources:", ray.cluster_resources())

    files = common.list_trip_files(args.trips_dir, args.max_files)
    print(f"Reading {len(files)} trip files from {args.trips_dir}")

    pipeline_start = time.perf_counter()

    # 1. Ingestion
    timer.start("ingestion")
    monthly_datasets = []
    for path in files:
        one_month = ray.data.read_parquet(path, memory=READ_TASK_MEMORY)
        one_month = one_month.map_batches(
            normalize_trip_batch, batch_format="pyarrow", memory=READ_TASK_MEMORY
        )
        monthly_datasets.append(one_month)

    trips = monthly_datasets[0]
    if len(monthly_datasets) > 1:
        trips = trips.union(*monthly_datasets[1:])
    trips = trips.materialize()
    timer.stop(trips.count())

    # 2. Cleansing
    timer.start("cleansing")
    cleaned = trips.map_batches(drop_null_rows, batch_format="pyarrow")

    # dedup: hash repartition so duplicates end up in the same block, then dedup per block
    cleaned = cleaned.repartition(
        common.SHUFFLE_PARTITIONS, keys=common.DEDUP_PARTITION_KEYS
    )
    cleaned = cleaned.map_batches(drop_duplicate_rows, batch_format="pyarrow", batch_size=None)

    cleaned = cleaned.map_batches(add_time_columns, batch_format="pyarrow")

    cleaned = cleaned.materialize()
    del trips
    timer.stop(cleaned.count())

    # 3. Join with zone lookup
    # broadcast join. shuffle join (Dataset.join) kept getting OOM killed,
    # see results/evidence/ray_shuffle_join_attempt.log
    timer.start("join")
    zone_types = {
        "LocationID": pa.int64(),
        "Borough": pa.string(),
        "Zone": pa.string(),
        "service_zone": pa.string(),
    }
    zones = pa_csv.read_csv(
        args.zones_csv,
        convert_options=pa_csv.ConvertOptions(column_types=zone_types),
    )
    zones = zones.rename_columns([common.ZONE_RENAME_MAP.get(n, n) for n in zones.column_names])

    joined = cleaned.map_batches(
        join_with_zones, batch_format="pyarrow", fn_kwargs={"zones": zones}
    )
    joined = joined.materialize()
    del cleaned
    timer.stop(joined.count())

    # 4. UDF
    timer.start("udf")
    with_speed = joined.map_batches(add_speed_column, batch_format="pyarrow")
    with_speed = with_speed.select_columns(common.FINAL_COLUMNS)
    with_speed = with_speed.materialize()
    del joined
    timer.stop(with_speed.count())

    # 5. Export
    timer.start("export")
    if os.path.exists(args.output_dir):
        shutil.rmtree(args.output_dir)
    with_speed.write_parquet(args.output_dir)
    timer.stop(with_speed.count())

    timer.seconds["total"] = time.perf_counter() - pipeline_start
    print(f"\nTOTAL wall-clock (ingestion -> export): {timer.seconds['total']:.2f} s")

    resources = ray.cluster_resources()
    alive_nodes = [node for node in ray.nodes() if node["Alive"]]
    info = {
        "ray_version": ray.__version__,
        "alive_nodes": len(alive_nodes),
        "cluster_cpus": resources.get("CPU"),
        "cluster_memory_gb": round(resources.get("memory", 0) / 1e9, 2),
        "object_store_gb": round(resources.get("object_store_memory", 0) / 1e9, 2),
        "shuffle_partitions": common.SHUFFLE_PARTITIONS,
        "output_dir": args.output_dir,
    }
    common.save_timings("Ray", args, timer, len(files), info)

    if args.hold_ui:
        try:
            input("\nRay job finished - check http://localhost:8265, then press ENTER... ")
        except EOFError:
            pass

    ray.shutdown()


if __name__ == "__main__":
    main()
