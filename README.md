# DA3408 Assignment 3: Spark vs. Ray - The Data Engineering Duel

The same NYC-taxi cleaning pipeline is implemented in Apache Spark (`spark_clean.py`) and
Ray Data (`ray_clean.py`), each run on its own 2-worker cluster with identical
resources, and benchmarked.

## Files

| File | Purpose |
|---|---|
| `spark_clean.py` | Spark pipeline (ingest → cleanse → heavy join → Python UDF → parquet) |
| `ray_clean.py` | The same pipeline in Ray Data |
| `pipeline_common.py` | Shared rules both pipelines import: schema, cleansing thresholds, the UDF, output columns |
| `compare_outputs.py` | Parity check: row count, schema, sorted equality, checksum |
| `monitor_resources.py` | Samples CPU % / memory of every container once per second (`docker stats`) into a CSV |
| `docker/Dockerfile`, `docker/start-spark.sh` | Cluster image (course `docker-spark-cluster` image + Ray) |
| `docker-compose.spark.yml` | Spark standalone: 1 master + 2 workers |
| `docker-compose.ray.yml` | Ray: 1 head + 2 workers (Ray started manually with `ray start`) |
| `Report.pdf` | Benchmark report (5 pages) |
| `AI_DISCLOSURE.md` | How AI was used (required by the course code of conduct) |
| `screenshots/` | Spark Master UI, Spark app UI (4040), Ray Dashboard (8265), terminal output |

## Data

NYC TLC Yellow Taxi trip records, January 2022 – December 2024 (36 monthly parquet files, ~1.94 GB),
plus `taxi_zone_lookup.csv`, from the official TLC CDN
(`https://d37ci6vzurychx.cloudfront.net/trip-data/`). `data/` and `output/` are not committed
(see `.gitignore`).

## Headline result (3 timed runs each, 119,136,044 input rows → 110,873,821 output rows)

| | Spark | Ray |
|---|---|---|
| Total wall-clock | 510.4 ± 27.6 s | **442.8 ± 20.0 s** |
| Python UDF stage | 121.3 ± 0.9 s | **108.9 ± 16.0 s** |
| Peak worker memory (limit 3072 MiB) | **2,845 MiB** | 3,072 MiB |
| OOM-killed workers per run | **0** | 14–16 (retried) |

Outputs are identical (row count, schema, sorted values, SHA-256).
