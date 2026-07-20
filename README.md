## 1. Business problem

A retail partner sends us one file every day with all their store sales.
The operations team needs a dashboard every morning showing:
- How much money was made per product category and per country
- Which category and country performed best
- When transactions peak during the day
- How each category compares to its daily revenue target

**Without this pipeline:** someone has to open the file manually, build the report by hand, and email it. That takes time, can be done wrong, and fails silently if the file is corrupt.

**With this pipeline:** everything runs automatically. The system waits for the file, checks it is valid, computes all the KPIs with Spark, and writes the dashboard report — no human needed.

**What happens when it fails (business perspective):**

If the vendor sends a corrupt or empty file, the pipeline stops immediately at the validation step and shows a visible red alert in the monitoring dashboard (Airflow UI). This means:
- Operations is notified right away — no bad data ever reaches the dashboard
- The BI team does not publish wrong numbers to management
- A failure log file is automatically created so the team knows exactly which date failed and why
- Once the vendor resends a correct file, the pipeline can be re-run for that day without duplicating any data

This protects the business from making decisions based on wrong revenue figures.

---

## 2. Architecture

Data moves through 3 layers, each one cleaner than the last:

```
Vendor file arrives
       ↓
  [Bronze] Raw CSV — exactly as the vendor sent it
       ↓  (DuckDB cleans and converts)
  [Silver] Parquet file — typed, validated, ready for analysis
       ↓  (PySpark computes KPIs)
  [Gold]   Parquet file — aggregated KPI results
       ↓  (publish task writes summary)
  [Serve]  JSON dashboard — ready for the BI screen
```

| Layer | Folder | Tool | What is inside |
|-------|--------|------|----------------|
| Bronze | `data/incoming/` | `vendor_drop.py` | Raw CSV from vendor |
| Silver | `data/raw/dt=<date>/` | DuckDB (`ingest_day`) | Clean Parquet, one file per day |
| Gold | `data/curated/dt=<date>/kpis/` | PySpark | KPI aggregates by category and country |
| Serve | `data/reports/` | `publish` task | `dashboard_<date>.json` |

### Airflow — 5 tasks

| task_id | What it does |
|---------|-------------|
| `wait_for_vendor_csv` | Waits for the daily CSV file to appear. Checks every 30 seconds. Stops waiting after 20 minutes if nothing arrives. |
| `bronze_to_silver.ingest` | Reads the CSV, converts it to a clean Parquet file using DuckDB. |
| `bronze_to_silver.validate` | Checks the data is healthy — at least 10 rows and total revenue above €1. If the file is corrupt (all zeros), this task turns red and stops everything. |
| `silver_to_gold.compute_kpis` | Runs PySpark: reads the clean data, enriches it, computes all KPIs, writes the Gold Parquet and the JSON report. |
| `silver_to_gold.publish` | Confirms the JSON report was written. If something silently broke in Spark, this task catches it. |

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

### Screenshot — Airflow graph view (green run)

> _Insert screenshot here: `demo_backup/01_green_dag_graph.png`_
> 
> What to show: Airflow UI → DAGs → KHIYANI_ZANNA_KALFADANG → Graph view, all 5 tasks green

---

## 3. Spark transformations

File: `include/team_KHIYANI_ZANNA_KALFADANG_spark.py`

| # | Function | What it does |
|---|----------|--------------|
| 1 | `transform_1` | Reads the Silver Parquet with a strict column schema. Throws out any row where the amount is missing, zero, or negative. |
| 2 | `transform_2` | Adds two things to each row: the hour of the day (from the timestamp), and the daily revenue target for that category (joined from `category_targets.csv`). The target table has only 7 rows so we use a broadcast join — faster than a normal join. |
| 3 | `transform_3` | Groups all transactions by category and country. For each group it computes: total revenue, transaction count, average transaction value, and revenue as a % of the daily target. |

### KPIs — what we produce and why it matters

| KPI | Where | Plain explanation |
|-----|-------|-------------------|
| `revenue_eur` | Parquet + JSON | How much money was made per category per country that day |
| `tx_count` | Parquet + JSON | How many transactions happened — tells you if sales are high-value or high-volume |
| `avg_tx_eur` | Parquet | Average basket size. If this drops, customers are buying cheaper things |
| `revenue_vs_target_pct` | Parquet | What % of the daily category target was achieved. 48% means half the target was reached. All countries together should reach ~100% |
| `top_category` | JSON | The best-selling category of the day |
| `top_country` | JSON | The country with the highest revenue that day |
| `peak_hour` | JSON | The hour with the most transactions — useful for staffing and server capacity |
| `payment_breakdown` | JSON | How many transactions per payment method per country (card, cash, wallet, transfer) |

### Screenshot — sample dashboard JSON output

> _Insert screenshot here: `demo_backup/02_dashboard_json.png`_
>
> Command: `cat data/reports/dashboard_2026-06-08.json`

### Screenshot — Gold Parquet folder

> _Insert screenshot here: `demo_backup/03_curated_folder.png`_
>
> Command: `ls data/curated/dt=2026-06-08/kpis/`

---

## 4. Idempotence

**What it means in simple terms:** you can run the pipeline twice for the same day and get exactly the same result — no duplicate rows, no inflated numbers.

**How we achieve it:**

- **Silver layer:** DuckDB deletes the old Parquet file before writing the new one. If you re-run June 8, the old `transactions.parquet` is deleted first, then a fresh one is written.
- **Gold layer:** PySpark uses `mode("overwrite")` — it replaces the entire output folder with new results.
- **JSON report:** `write_text()` overwrites the file in place.

---

## 5. Backfill

To run the pipeline for a full week at once (useful for catching up after a gap):

```bash
# Step 1 — make sure all the CSV files exist for those dates
python scripts/vendor_drop.py --range 2026-06-01:2026-06-07 --volume small

# Step 2 — run the backfill
docker compose exec airflow-scheduler \
  airflow dags backfill KHIYANI_ZANNA_KALFADANG -s 2026-06-01 -e 2026-06-07 --reset-dagruns
```

`--reset-dagruns` clears any previous run state for those dates. Safe to use because the pipeline is idempotent.

---

## 6. Failure demo

```bash
# Drop a corrupt file for June 3 (all amounts set to 0)
python scripts/vendor_drop.py --date 2026-06-03 --corrupt

# Then in the Airflow UI: trigger the DAG manually for 2026-06-03
```

**What happens step by step:**

1. `wait_for_vendor_csv` — turns green (the file exists)
2. `bronze_to_silver.ingest` — turns green (the CSV is readable, DuckDB writes it to Parquet)
3. `bronze_to_silver.validate` — turns **red** — total revenue is €0.00 which is below the €1.00 minimum threshold. Raises: `RuntimeError: Validation failed: amount_sum=0.0`
4. `silver_to_gold` group — never starts. Spark is not invoked on corrupt data.
5. A file `FAILED_2026-06-03_bronze_to_silver.validate.txt` is written to `data/reports/` automatically.

### Screenshot — red validate task

> _Insert screenshot here: `demo_backup/04_red_validate_task.png`_
>
> What to show: Airflow UI graph view for the corrupt run, `bronze_to_silver.validate` is red, `silver_to_gold` is grey/skipped

### Screenshot — FAILED marker file

> _Insert screenshot here: `demo_backup/05_failed_marker.png`_
>
> Command: `cat data/reports/FAILED_2026-06-03_*.txt`

---

## 7. Exploration tracks

| Track | Done | What we implemented |
|-------|------|---------------------|
| R — Reliability | ✅ | `retries=2` and `retry_delay=3 min` on every task. FileSensor `timeout=20 min` so it fails fast if the vendor is late instead of waiting forever. `on_failure_callback` writes a plain-text marker file to `data/reports/` on any failure so ops can find it without opening Airflow. |
| S — Spark depth | ✅ | `transform_1` uses an explicit `StructType` schema — Spark enforces column types at read time instead of guessing. `transform_2` uses `F.broadcast()` on `category_targets.csv` because it has only 7 rows. Broadcasting copies the small table to every worker instead of shuffling both tables across the network — much faster. |
| O — Orchestration | ✅ | Two `TaskGroup`s: `bronze_to_silver` contains ingest + validate, `silver_to_gold` contains compute + publish. They show as collapsible groups in the Airflow graph view, making the pipeline easier to read and understand. |
| Q — Data quality | ✅ | `validate_silver` checks `min_rows=10` and `min_revenue=1.0`. A `--corrupt` day where all amounts are zero fails the validate task immediately and blocks Spark from running on bad data. |
| P — Custom | — | Not attempted |
| X — SparkSubmit | — | Not attempted |

---

## 8. Demo script & backup

### Full demo commands (presentation day — run in this order)

```bash
# ── SETUP (do this before presenting) ────────────────────────────────────

# 1. Start all services
docker compose up -d

# Wait ~1 minute then open: http://localhost:8080
# Login: admin / admin

# 2. Create all data files (14 days + reference targets)
python scripts/vendor_drop.py --seed-pack --volume small
python scripts/vendor_drop.py --reference

# ── HAPPY PATH DEMO ───────────────────────────────────────────────────────

# 3. In Airflow UI: go to DAGs → KHIYANI_ZANNA_KALFADANG
#    Click the play button → Trigger DAG → logical date: 2026-06-08
#    Watch all 5 tasks turn green one by one

# 4. Once green, show the outputs:
cat data/reports/dashboard_2026-06-08.json
ls data/curated/dt=2026-06-08/kpis/

# ── FAILURE DEMO ──────────────────────────────────────────────────────────

# 5. Create a corrupt file for June 3
python scripts/vendor_drop.py --date 2026-06-03 --corrupt

# 6. In Airflow UI: Trigger DAG → logical date: 2026-06-03
#    Watch: ingest turns green, validate turns RED, Spark never starts

# 7. Show the failure marker file
cat data/reports/FAILED_2026-06-03_*.txt

# ── BACKFILL (optional, shows idempotence) ────────────────────────────────

# 8. Run multiple dates at once
docker compose exec airflow-scheduler \
  airflow dags backfill KHIYANI_ZANNA_KALFADANG -s 2026-06-01 -e 2026-06-07 --reset-dagruns

# ── CLEANUP (after demo, restore June 3 to a good file) ──────────────────

# 9. Replace the corrupt file with a good one
python scripts/vendor_drop.py --date 2026-06-03
```

### Demo backup

If Docker fails on the day, use these screenshots from `demo_backup/`:

| # | File | What it shows |
|---|------|---------------|
| 1 | `01_green_dag_graph.png` | All 5 tasks green for a successful run |
| 2 | `02_dashboard_json.png` | The JSON report with KPIs |
| 3 | `03_curated_folder.png` | Gold Parquet files written by Spark |
| 4 | `04_red_validate_task.png` | validate task red after --corrupt |
| 5 | `05_failed_marker.png` | FAILED_*.txt marker file content |

---

## 9. Production next steps

- Replace `local[*]` (runs Spark on the laptop) with a real cluster using `SparkSubmitOperator` — Airflow would just submit the job and monitor it, not run it
- Add per-country revenue targets to the reference file so `revenue_vs_target_pct` compares each country against its own target, not the global category target
- Replace the plain-text failure marker file with a real Slack or email alert
- Add an SLA callback on the FileSensor so the team is alerted if the vendor file arrives more than 2 hours late
