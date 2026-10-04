
# AI Disclosure - AIOps (DA3408) Assignment 3

Kiran Kumar P

## 1. Tools
Claude (Anthropic) and Copilot (VSCode)
## 2. How they were used
1. **Logic translation** I asked Claude to translate three parts
   into Ray Data: the `dropDuplicates()` step (it suggested a hash `repartition` + per-block dedup), the
   broadcast join (`map_batches` with the lookup table passed in), and the row-level UDF call. I checked each
   translation against the Spark output with `compare_outputs.py`.
2. **Cluster configuration and network setup:** I started from the course
   `docker-spark-cluster` files, changed the base image and the ports myself, wrote `docker-compose.ray.yml`,
   and started Ray by hand with `ray start --head` / `ray start --address=...`.
3. **Debugging:** I pasted the Ray out-of-memory errors from the ingestion stage into Claude to understand them.
   It explained the per-node memory threshold; I chose to limit the read tasks myself.
4. **Resource monitoring:** The Assignment 5 Prometheus/Grafana stack is not available yet, so Claude wrote
   `monitor_resources.py`, which samples `docker stats` for each container, as a stand-in.
5. **Autocomplete:** Copilot completed the matplotlib calls in `make_report_assets.py`.

## 3. Impact
AI sped up the Spark-to-Ray translation, wrote the resource monitor, and helped me debug the Ray memory errors.
The cluster setup was done by me. The runs, the measurements and the analysis in the report are my own. See the report's "Performance Tuning Note" for the
one optimisation the AI suggested (the dedup approach).
