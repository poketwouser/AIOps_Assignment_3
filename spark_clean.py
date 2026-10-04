# Spark version of the taxi cleaning pipeline
# spark-submit --master spark://spark-master:7077 --py-files /opt/a3/pipeline_common.py /opt/a3/spark_clean.py --label run1

import time

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType

import pipeline_common as common


def create_spark_session():
    spark = (
        SparkSession.builder.appName("A3-spark-clean")
        .config("spark.sql.shuffle.partitions", str(common.SHUFFLE_PARTITIONS))
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.parquet.outputTimestampType", "TIMESTAMP_MICROS")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    return spark


def force(df, previous=None):
    # cache + count so the stage actually runs (spark is lazy)
    df = df.cache()
    row_count = df.count()
    if previous is not None:
        previous.unpersist()
    return df, row_count


def read_one_trip_file(spark, path):
    df = spark.read.parquet(path)

    # some months have Airport_fee instead of airport_fee
    for old_name, new_name in common.RENAME_MAP.items():
        if old_name in df.columns:
            df = df.withColumnRenamed(old_name, new_name)

    selected = []
    for column in common.TRIP_COLUMNS:
        if column in common.LONG_COLUMNS:
            selected.append(F.col(column).cast("long").alias(column))
        elif column in common.DOUBLE_COLUMNS:
            selected.append(F.col(column).cast("double").alias(column))
        elif column in common.STRING_COLUMNS:
            selected.append(F.col(column).cast("string").alias(column))
        else:
            selected.append(F.col(column).cast("timestamp").alias(column))
    return df.select(selected)


def main():
    args = common.parse_args("Spark")
    timer = common.StageTimer()

    timer.start("startup")
    spark = create_spark_session()
    timer.stop()

    files = common.list_trip_files(args.trips_dir, args.max_files)
    print(f"Reading {len(files)} trip files from {args.trips_dir}")

    pipeline_start = time.perf_counter()

    # 1. Ingestion
    timer.start("ingestion")
    trips = None
    for path in files:
        one_month = read_one_trip_file(spark, path)
        if trips is None:
            trips = one_month
        else:
            trips = trips.unionByName(one_month)
    trips, n_ingested = force(trips)
    timer.stop(n_ingested)

    # 2. Cleansing
    timer.start("cleansing")
    cleaned = trips.dropna(how="any")
    cleaned = cleaned.dropDuplicates()

    pickup = F.col("tpep_pickup_datetime")
    dropoff = F.col("tpep_dropoff_datetime")
    cleaned = cleaned.withColumn(
        "trip_duration_sec", dropoff.cast("long") - pickup.cast("long")
    )
    cleaned = cleaned.withColumn("pickup_date", F.date_format(pickup, "yyyy-MM-dd"))
    cleaned = cleaned.withColumn("pickup_hour", F.hour(pickup).cast("long"))

    cleaned = cleaned.filter(F.col("trip_duration_sec") > common.MIN_DURATION_SEC)
    cleaned = cleaned.filter(F.col("trip_distance") > common.MIN_DISTANCE_MILES)

    cleaned, n_cleaned = force(cleaned, previous=trips)
    timer.stop(n_cleaned)

    # 3. Join with zone lookup
    timer.start("join")
    zones = spark.read.csv(
        args.zones_csv,
        header=True,
        schema="LocationID LONG, Borough STRING, Zone STRING, service_zone STRING",
    )
    for old_name, new_name in common.ZONE_RENAME_MAP.items():
        zones = zones.withColumnRenamed(old_name, new_name)

    # broadcast join, lookup table is small
    joined = cleaned.join(
        F.broadcast(zones), cleaned["PULocationID"] == zones["LocationID"], "inner"
    )
    joined = joined.drop("LocationID")

    joined, n_joined = force(joined, previous=cleaned)
    timer.stop(n_joined)

    # 4. UDF
    timer.start("udf")
    speed_udf = F.udf(common.avg_speed_mph, DoubleType())
    with_speed = joined.withColumn(
        "avg_speed_mph", speed_udf(F.col("trip_distance"), F.col("trip_duration_sec"))
    )
    with_speed = with_speed.select(common.FINAL_COLUMNS)

    with_speed, n_udf = force(with_speed, previous=joined)
    timer.stop(n_udf)

    # 5. Export
    timer.start("export")
    with_speed.write.mode("overwrite").parquet(args.output_dir)
    timer.stop(n_udf)

    timer.seconds["total"] = time.perf_counter() - pipeline_start
    print(f"\nTOTAL wall-clock (ingestion -> export): {timer.seconds['total']:.2f} s")

    conf = spark.sparkContext.getConf()
    info = {
        "spark_version": spark.version,
        "master": spark.sparkContext.master,
        "default_parallelism": spark.sparkContext.defaultParallelism,
        "executor_memory": conf.get("spark.executor.memory", "default"),
        "executor_cores": conf.get("spark.executor.cores", "default"),
        "shuffle_partitions": common.SHUFFLE_PARTITIONS,
        "output_dir": args.output_dir,
    }
    common.save_timings("Spark", args, timer, len(files), info)

    if args.hold_ui:
        try:
            input("\nSpark UI is still up on http://localhost:4040 - press ENTER to stop... ")
        except EOFError:
            pass

    with_speed.unpersist()
    spark.stop()


if __name__ == "__main__":
    main()
