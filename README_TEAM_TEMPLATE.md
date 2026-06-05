# Team: <First Last> & <First Last>

**DAG id:** `team_<shortname>`  
**Git repo:** `https://github.com/...` - **also on your Moodle slides** (title or architecture)  
**Spark module:** `include/team_<shortname>_spark.py`  
**Course:** Big Data Processing - Lab 4 Capstone

---

## 1. Business problem

<Who needs the dashboard? What breaks if the pipeline fails?>

**Defense tip:** for each section below, be ready to say **what you built** and **why** (not only that it runs).

**Submit by June 9, 23:59:** push capstone to **your pair's Git repo**; upload **slides on Moodle** with the **same URL** visible on the slides (title slide). Public repo, or private with instructor read access.

---

## 2. Architecture


## 1. Airflow (5 mandatory tasks)

The pipeline is designed with a linear, robust topology. It intercepts data anomalies as early as possible to prevent wasting cluster compute resources on bad data.

| task_id | Role / Functional Description |
| :--- | :--- |
| `wait_for_vendor_csv` | **FileSensor**: Monitors the landing zone (*Bronze*) and waits for the daily raw CSV file (`transactions_<ds>.csv`) to be dropped by the vendor simulator. |
| `ingest_bronze_to_silver` | **PythonOperator (DuckDB)**: Ingests the raw CSV file into typed, cleaned, and partitioned Parquet files within the *Silver* layer (`data/raw/dt=<ds>/`). |
| `validate_silver_data` | **Data Quality Gateway**: Validates Silver data integrity. If corrupt data was injected (`--corrupt`), this task raises an exception and switches to a visible **red** failure state, stopping the pipeline before Spark spins up. |
| `compute_pyspark_kpis` | **PySpark Worker**: Initializes the local Spark session (`local[*]`), processes the heavy analytics transformations, and outputs the *Gold* Parquet files along with the final JSON report. |
| `export_summary_backup` | **Idempotence Enforcer**: Deletes any existing old backups for the logical date `ds` and creates a fresh copy of the final JSON report inside a secure archive directory (`Serve` layer). |

## 2. Spark (>=3 transforms, include/team_franklin_spark.py)

Our PySpark engine structures data processing into three distinct, decoupled operational transformations:

1. **`transform_1` (Cleanse & Filter):** Reads the Silver Parquet dataset, enforces strict casting on financial columns, and filters out anomalous or negative transactions to secure the baseline data.
2. **`transform_2` (Enrich):** Extracts time-based attributes (month, year) and standardizes country codes and product categories to prepare analytical dimensions.
3. **`transform_3` (Scale Aggregation):** Performs group-by operations on country and product category axes to compute total revenue (`sum(montant_eur)`) and transaction volume (`count(transaction_id)`) simultaneously.

## 3. Idempotence / Backfill / Failure Demonstration

* **Idempotence:** Every execution strictly targets the active date partition (`dt=ds`). The final archiving task automatically removes and overwrites pre-existing JSON records and Parquet outputs for that day, ensuring that running the pipeline multiple times yields identical results without inflating values.
* **Failure Scenario (`--corrupt`):** When corrupted records are introduced, the `validate_silver_data` task catches the anomaly early. The DAG safely fails out (Red node in the UI), blocking Spark from using bad records and signaling the operations team immediately.

## 4. Demo Backup
*In case of any unexpected Docker environment issues during the live defense on June 10, verification screenshots (showing successful green DAG runs, Spark execution logs, and output directory trees) are saved under:* `demo_backup/`.

<!-- Diagram: incoming → raw/dt= → curated/dt= → reports -->

| Layer | Path | Tool |
|-------|------|------|
| Bronze | `data/incoming/` | `vendor_drop.py` |
| Silver | `data/raw/dt=` | DuckDB (`ingest_day`) |
| Gold | `data/curated/dt=` | **Your** `team_<shortname>_spark.py` |
| Serve | `data/reports/` | JSON dashboard |

### Airflow (5 tasks)

| task_id | Role |
|---------|------|
| `task_1` | `role_1` |
| `task_2` | `role_2` |
| `task_3` | `role_3` |
| `task_4` | `role_4` |
| `task_5` | `role_5` |

**Dependency graph:**

```
e.g. `task_1` → `task_2` → `task_3` → `task_4` → `task_5`
```

---

## 3. Spark transformations (≥3 - your code)

File: `include/team_<shortname>_spark.py`

| # | Function | What it does |
|---|----------|--------------|
| 1 | `transform_1` | `description_1` |
| 2 | `transform_2` | `description_2` |
| 3 | `transform_3` | `description_3` |

---

## 4. Idempotence

<Re-run same `ds`: what gets overwritten under `raw/dt=`, `curated/dt=`, `dashboard_*.json`?>

---

## 5. Backfill

```bash
docker compose exec airflow-scheduler \
  airflow dags backfill team_<shortname> -s 2026-06-01 -e 2026-06-07 --reset-dagruns
```

---

## 6. Failure demo

```bash
python scripts/vendor_drop.py --date 2026-06-03 --corrupt
```

<Which task fails? What appears in the Airflow UI?>

---

## 7. Exploration tracks

| Track | Done? | Describe your implementation |
|-------|-------|----------|
| R Reliability | | |
| S Spark depth | | |
| O Orchestration | | |
| Q Data quality | | |
| P Custom | | |
| X SparkSubmit | | |

---

## 8. Demo script & backup

---

## 9. Production next steps

