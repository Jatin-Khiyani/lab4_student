# Team: Franklin

**DAG id:** `team_franklin`  
**Git repo:** `https://github.com/YOUR_REPO_HERE` — also on Moodle slides (title slide)  
**Spark module:** `include/team_franklin_spark.py`  
**Course:** Big Data Processing — Lab 4 Capstone

---

## 1. Business problem

A retail partner drops one CSV file per day containing all store transactions across 6 European
countries and 7 product categories. Operations needs a daily KPI dashboard showing revenue,
transaction volumes, and performance against targets — broken down by category and country —
updated automatically every morning without manual intervention.

**What breaks if the pipeline fails:** the BI dashboard shows stale data. Without the validate
task, a corrupt file (all amounts = 0) would silently produce a zero-revenue report that looks
like a real result instead of an error. Without idempotence, re-running a failed day would
duplicate rows and inflate every metric.

---

## 2. Architecture

Data flows through three quality layers (medallion architecture). Each layer is stricter than
the last — raw as delivered, then cleaned, then aggregated.

| Layer | Path | Tool | Content |
|-------|------|------|---------|
| Bronze | `data/incoming/transactions_<ds>.csv` | `vendor_drop.py` | Raw CSV as delivered by vendor |
| Silver | `data/raw/dt=<ds>/transactions.parquet` | DuckDB `ingest_day` | Typed, cleaned Parquet |
| Gold | `data/curated/dt=<ds>/kpis/` | PySpark `run_daily` | KPI aggregates by category × country |
| Serve | `data/reports/dashboard_<ds>.json` | `publish` task | Summary JSON for BI dashboard |

### Airflow tasks (5)

| task_id | Role |
|---------|------|
| `wait_for_vendor_csv` | FileSensor — polls `data/incoming/` every 30 s; times out after 20 min if the vendor is late (Track R) |
| `bronze_to_silver.ingest` | Reads the raw CSV with DuckDB, casts types, writes idempotent Silver Parquet |
| `bronze_to_silver.validate` | Data quality gate — raises if row count < 10 or total revenue < €1.0; blocks Spark from running on bad data (Track Q) |
| `silver_to_gold.compute_kpis` | Starts PySpark local session, runs the three transforms, writes Gold Parquet + JSON report |
| `silver_to_gold.publish` | Confirms the JSON report file exists; raises `FileNotFoundError` if compute silently failed |

**Dependency graph:**

```
wait_for_vendor_csv
        ↓
  [bronze_to_silver]
    ingest → validate
        ↓
  [silver_to_gold]
  compute_kpis → publish
```

TaskGroups `bronze_to_silver` and `silver_to_gold` are visible as collapsed groups in the
Airflow graph view (Track O).

---

## 3. Spark transformations (≥3)

File: `include/team_franklin_spark.py`

| # | Function | What it does |
|---|----------|--------------|
| 1 | `transform_1` | Reads Silver Parquet with an explicit `StructType` schema (strict casting for `amount_eur` as `DoubleType`, `ts` as `TimestampType`). Filters out rows where `amount_eur` is null, zero, or negative. |
| 2 | `transform_2` | Enriches transactions: extracts `hour` from the timestamp column for peak-time analysis; broadcast-joins the 7-row `category_targets.csv` reference file so each row carries its category's daily revenue target. |
| 3 | `transform_3` | Groups by `(category, country)` and computes four KPIs: `revenue_eur`, `tx_count`, `avg_tx_eur`, and `revenue_vs_target_pct`. Orders output by category then country. |

### KPIs produced and why they are useful

| KPI | Output | Business meaning |
|-----|--------|-----------------|
| `revenue_eur` | Gold Parquet + JSON | Total sales value per category per country for the day — the primary metric operations tracks |
| `tx_count` | Gold Parquet + JSON | Number of transactions — separates "few big orders" from "many small orders" |
| `avg_tx_eur` | Gold Parquet | Average basket size — a drop here signals customers are buying cheaper items or abandoning carts |
| `revenue_vs_target_pct` | Gold Parquet | Actual revenue ÷ category daily target × 100 — tells ops immediately if a category is underperforming its goal. Each country row shows its contribution toward the global category target; summing all countries for one category gives total attainment |
| `top_category` | JSON dashboard | Highest-revenue category for the day — one-number headline for the morning briefing |
| `top_country` | JSON dashboard | Highest-revenue country — flags geographic concentration or surprises |
| `peak_hour` | JSON dashboard | Hour of day with the most transactions — informs infrastructure scaling and staffing decisions |
| `payment_breakdown` | JSON dashboard | Transaction count per `(country, payment_method)` — shows which payment rails dominate per market, useful for payment provider negotiations |

---

## 4. Key concepts (brief)

**Medallion architecture:** data is stored in three progressive quality layers (Bronze raw,
Silver clean, Gold aggregated). Each layer can be reprocessed independently without touching
the others.

**Idempotence:** running the pipeline twice for the same `ds` produces identical outputs.
DuckDB calls `pq_path.unlink()` before writing Silver so the old file is replaced, not
appended to. PySpark writes with `mode("overwrite")` so the Gold partition is fully replaced.
The JSON report uses `write_text` which overwrites the existing file.

**FileSensor:** an Airflow operator that polls for a file on a schedule (`poke_interval`)
instead of holding a worker slot. `mode="reschedule"` frees the worker between polls so
other tasks can run in parallel. `timeout` makes it fail fast if the vendor is late rather
than hanging forever.

**Broadcast join (Track S):** when joining a large DataFrame with a tiny one, wrapping the
small side in `F.broadcast()` tells Spark to replicate it to every worker instead of
shuffling both sides across the network. Safe here because `category_targets.csv` has only
7 rows.

**TaskGroup (Track O):** a visual and logical grouping of related tasks in the Airflow graph.
`bronze_to_silver` groups ingest + validate; `silver_to_gold` groups compute + publish.
Makes the DAG graph easier to read and reason about at a glance.

**on_failure_callback (Track R):** a Python function Airflow calls whenever a task fails.
Ours writes a plain-text marker file to `data/reports/FAILED_<ds>_<task_id>.txt` so ops can
spot failures from the filesystem without opening the Airflow UI.

---

## 5. Idempotence

Re-running `team_franklin` for the same `ds`:

- **Silver** (`data/raw/dt=<ds>/transactions.parquet`): DuckDB deletes the existing file with
  `pq_path.unlink()` before writing, so the partition is fully replaced — no appended rows.
- **Gold** (`data/curated/dt=<ds>/kpis/`): PySpark writes with `mode("overwrite")`, replacing
  the entire directory and all its part files.
- **Report** (`data/reports/dashboard_<ds>.json`): `Path.write_text()` overwrites the file in
  place.

No duplicates are possible regardless of how many times the DAG is triggered for the same date.

---

## 6. Backfill

```bash
docker compose exec airflow-scheduler \
  airflow dags backfill team_franklin -s 2026-06-01 -e 2026-06-07 --reset-dagruns
```

`--reset-dagruns` clears any existing run state for those dates before re-executing, which is
safe because the pipeline is idempotent. Run `vendor_drop.py --seed-pack` first to ensure
CSVs exist for all seven dates before starting the backfill.

---

## 7. Failure demo

```bash
# Drop a corrupt file — all amount_eur = 0
python scripts/vendor_drop.py --date 2026-06-03 --corrupt

# Then trigger the DAG for 2026-06-03 in the Airflow UI
```

**What happens:** `validate_silver` is called with `min_revenue=1.0`. The total revenue across
all rows is `0.0`, which is below the threshold. The task raises:

```
RuntimeError: Validation failed: amount_sum=0.0 (corrupt day?)
```

The task turns **red** in the Airflow UI. The `silver_to_gold` TaskGroup never starts —
PySpark is never invoked on bad data. A marker file
`FAILED_2026-06-03_bronze_to_silver.validate.txt` is written to `data/reports/` by the
`on_failure_callback`.

---

## 8. Exploration tracks

| Track | Done | Implementation |
|-------|------|----------------|
| R Reliability | ✅ | `retries=2`, `retry_delay=3 min` on all tasks via `DEFAULT_ARGS`; FileSensor `timeout=1200 s`; `on_failure_callback` writes a marker file to `data/reports/` on any task failure |
| S Spark depth | ✅ | Explicit `StructType` schema in `transform_1` enforces column types at read time; `F.broadcast()` wraps the 7-row reference DataFrame in `transform_2` to avoid a shuffle join |
| O Orchestration | ✅ | Two `TaskGroup`s: `bronze_to_silver` (ingest → validate) and `silver_to_gold` (compute → publish), visible as collapsed groups in the Airflow graph view |
| Q Data quality | ✅ | `validate_silver` called with `min_revenue=1.0`; a `--corrupt` day (all zeros) fails the validate task and blocks Spark; demoed live with `vendor_drop --corrupt` |
| P Custom | — | Not attempted |
| X SparkSubmit | — | Not attempted |

---

## Demo Script and Backup



**Demo backup:** screenshots saved in `demo_backup/` — green DAG graph, sample JSON output,
red validate task after corrupt run.

---

## 10. Production next steps


