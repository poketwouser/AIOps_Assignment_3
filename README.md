# DA3408 Assignment 3 — Spark vs. Ray: The Data Engineering Duel

One NYC-taxi cleaning pipeline, implemented twice — in **Apache Spark** (`spark_clean.py`) and in
**Ray Data** (`ray_clean.py`) — and benchmarked on two Docker clusters of identical size
(2 workers × 4 CPU × 3 GB each).

**Result:** both produce the same 110,873,821 rows (identical schema, values and SHA-256 checksum).
Ray finished **1.15× faster** overall (442.8 s vs. 510.4 s) and ran the Python UDF **10% faster**;
Spark was faster on the shuffle-heavy stages, used less memory and never lost a task.
Full analysis: [`Report.pdf`](Report.pdf).

---

## Rubric map

| Rubric item (points) | Where it is covered |
|---|---|
| **Cluster Orchestration** (25) | [`docker/Dockerfile`](docker/Dockerfile), [`docker-compose.spark.yml`](docker-compose.spark.yml), [`docker-compose.ray.yml`](docker-compose.ray.yml); screenshots [01](screenshots/01_spark_master_2_workers.jpg) (Spark Master UI, 2 alive workers), [04](screenshots/04_spark_app_ui_4040_executors.jpg) (Spark UI :4040, one executor per worker), [09](screenshots/09_ray_dashboard_cluster_3_nodes.jpg) / [10](screenshots/10_ray_dashboard_overview_resources.jpg) (Ray Dashboard :8265, 3 nodes, resource view); Report §1 |
| **Pipeline Parity** (25) | Shared rules in [`pipeline_common.py`](pipeline_common.py); check in [`compare_outputs.py`](compare_outputs.py) (row count, schema, sorted equality, checksum); screenshot [21](screenshots/21_terminal_parity_match.jpg); Report §2 |
| **Performance Analysis** (20) | Report §3: per-stage timing table, total wall-clock, peak CPU / memory per worker, charts; resource sampling by [`monitor_resources.py`](monitor_resources.py); screenshots [19](screenshots/19_terminal_spark_run1.jpg), [20](screenshots/20_terminal_ray_run1.jpg), [22](screenshots/22_terminal_results_table.jpg) |
| **UDF Deep-Dive** (15) | Report §4: UDF stage time, per-row cost, JVM ↔ Python vs. Arrow data path; screenshot [08](screenshots/08_spark_udf_plan_BatchEvalPython.jpg) (`BatchEvalPython` in the Spark plan) |
| **Documentation** (15) | [`Report.pdf`](Report.pdf) (5 pages, charts, AI-first vs. BI-first, **Winner** in §6, Performance Tuning Note in §7) |

| Final-submission item | File |
|---|---|
| Codebase with `spark_clean.py` and `ray_clean.py` | this repository |
| Spark Master UI showing 2 active workers | [`screenshots/01_spark_master_2_workers.jpg`](screenshots/01_spark_master_2_workers.jpg) |
| Ray Dashboard cluster resource view | [`screenshots/10_ray_dashboard_overview_resources.jpg`](screenshots/10_ray_dashboard_overview_resources.jpg) |
| Benchmark report (PDF) | [`Report.pdf`](Report.pdf) |
| AI disclosure | [`AI_DISCLOSURE.md`](AI_DISCLOSURE.md) |

---

## Pipeline

| Stage | Spark | Ray Data |
|---|---|---|
| 1. Ingestion | read 36 Parquet files, cast to one schema, `unionByName` | `read_parquet` per file + `map_batches`, `union` |
| 2. Cleansing | `dropna()`, `dropDuplicates()`, timestamps → duration / date / hour, drop trips with duration ≤ 0 or distance ≤ 0 | same rules: null/NaN mask, hash `repartition` + per-block dedup, same derived columns and filter |
| 3. Heavy join | trips ⋈ zone lookup on `PULocationID` (`broadcast`) | same join, lookup table sent to every task |
| 4. Python UDF | `avg_speed_mph` as a row-at-a-time `@udf` | same function, called per row in `map_batches` |
| 5. Export | Parquet → `output/spark/` | Parquet → `output/ray/` |

Each stage is timed separately with `time.perf_counter()` and forced to execute
(`cache()+count()` in Spark, `materialize()` in Ray), so the UDF time is measured on its own.

## Clusters

| | Spark standalone | Ray |
|---|---|---|
| Coordinator / driver | `spark_master` — 2 CPU, 2 GB — Master UI **:8080**, app UI **:4040** | `ray_head` (`--num-cpus=0`) — 2 CPU, 2 GB — Dashboard **:8265** |
| Workers | `spark_worker_a`, `spark_worker_b` — **4 CPU, 3 GB** each | `ray_worker_a`, `ray_worker_b` — **4 CPU, 3 GB** each |
| Image | `a3-cluster:3.3.4` (Java 17, Python 3.10, Spark 3.3.4, Ray 2.59.0) | same image |

The project folder is mounted at `/opt/a3` on every node, so all nodes share the data and output directories.

## Results (3 timed runs each, warm-up excluded)

| Metric | Spark | Ray |
|---|---|---|
| Ingestion (s) | 73.2 ± 4.7 | 40.3 ± 8.6 |
| Cleansing (s) | 163.9 ± 17.1 | 179.2 ± 3.9 |
| Join (s) | 78.7 ± 2.4 | 88.0 ± 6.6 |
| Python UDF (s) | 121.3 ± 0.9 | 108.9 ± 16.0 |
| Export (s) | 73.4 ± 3.8 | 26.3 ± 1.4 |
| **Total wall-clock (s)** | **510.4 ± 27.6** | **442.8 ± 20.0** |
| Peak worker CPU (% of one core, limit 400) | 427 | 420 |
| Peak worker memory (MiB, limit 3072) | 2,845 | 3,072 |
| Worker processes OOM-killed and retried, per run | 0 | 14–16 |

**Winner:** Ray, narrowly, for this pipeline. **AI-first project → Ray** (Python-native UDFs and Arrow I/O);
**BI-first project → Spark** (joins, aggregations, SQL, predictable under memory pressure). See Report §6.

---

## How to run

Requirements: Docker + Docker Compose, a Python environment with `pyarrow` and `pandas` (for the
monitor and the parity check), ~25 GB free disk.

```bash
# 1. data: NYC TLC Yellow Taxi 2022-01 .. 2024-12 + zone lookup (official TLC CDN)
mkdir -p data/trips data/lookup
for y in 2022 2023 2024; do for m in 01 02 03 04 05 06 07 08 09 10 11 12; do
  curl -sSf -o data/trips/yellow_tripdata_$y-$m.parquet \
    https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_$y-$m.parquet
done; done
curl -sSf -o data/lookup/taxi_zone_lookup.csv \
  https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv

# 2. image (shared by both clusters)
docker build --build-arg HOST_UID=$(id -u) -t a3-cluster:3.3.4 docker/

# 3. Spark: start cluster, run, stop   (UIs: localhost:8080 and localhost:4040)
docker compose -f docker-compose.spark.yml up -d
python monitor_resources.py --cluster spark --out spark_run1.csv &      # stop with: kill %1
docker exec -i spark_master /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 --driver-memory 1g --executor-memory 2g \
  --py-files /opt/a3/pipeline_common.py /opt/a3/spark_clean.py --label run1
docker compose -f docker-compose.spark.yml down

# 4. Ray: start containers, start Ray by hand, run, stop   (Dashboard: localhost:8265)
docker compose -f docker-compose.ray.yml up -d
docker exec ray_head     ray start --head --port=6379 --dashboard-host=0.0.0.0 --num-cpus=0
docker exec ray_worker_a ray start --address=ray-head:6379 --num-cpus=4
docker exec ray_worker_b ray start --address=ray-head:6379 --num-cpus=4
docker exec ray_head ray status                                        # 3 nodes, 8 CPU
python monitor_resources.py --cluster ray --out ray_run1.csv &
docker exec -i ray_head python /opt/a3/ray_clean.py --label run1
docker compose -f docker-compose.ray.yml down

# 5. parity check
python compare_outputs.py          # → RESULT: OUTPUTS MATCH
```

Both pipelines accept `--max-files 1` for a quick dry run on a single month, and `--hold-ui`
to keep the job (and the Spark UI on :4040) alive until ENTER is pressed. Each run writes its
stage timings to `results/timings/<framework>_<label>.json`.

## Repository layout

```
spark_clean.py            Spark pipeline
ray_clean.py              Ray Data pipeline
pipeline_common.py        shared schema, cleansing rules, UDF, output columns (imported by both)
compare_outputs.py        parity check between output/spark and output/ray
monitor_resources.py      docker stats sampler (CPU %, memory per container, 1 s)
docker/                   cluster image (Dockerfile, start-spark.sh)
docker-compose.spark.yml  Spark: 1 master + 2 workers
docker-compose.ray.yml    Ray: 1 head + 2 workers
Report.pdf                benchmark report
screenshots/              Spark UIs, Ray Dashboard, console output
AI_DISCLOSURE.md          AI use disclosure
```

`data/` and `output/` are not committed (see `.gitignore`).

## Data

NYC TLC Yellow Taxi trip records, January 2022 – December 2024: 36 monthly Parquet files,
1.94 GB, 119,136,044 rows, plus `taxi_zone_lookup.csv` (265 zones), from the official TLC source
(`https://d37ci6vzurychx.cloudfront.net/`).
