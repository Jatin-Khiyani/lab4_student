"""
Lab 4 Capstone DAG — Team Franklin

Tracks implemented:
  R  Reliability   — retries=2 on every task, FileSensor timeout=20 min,
                     on_failure_callback writes a marker file to data/reports/
  O  Orchestration — TaskGroup "bronze_to_silver" and "silver_to_gold"
  Q  Data quality  — validate_silver called with min_revenue=1.0 so a
                     --corrupt day (all zeros) turns the validate task red
"""
from __future__ import annotations

from datetime import datetime, timedelta
from pathlib import Path

from airflow import DAG
from airflow.decorators import task
from airflow.sensors.filesystem import FileSensor
from airflow.utils.task_group import TaskGroup

from include.ingest import ingest_day, validate_silver
from include.paths import report_json
from include.team_franklin_spark import run_daily

DEFAULT_ARGS = {
    "owner": "team_franklin",
    "retries": 2,                        # Track R — retry twice before marking failed
    "retry_delay": timedelta(minutes=3),
    "email_on_failure": False,
}


def _write_failure_marker(context) -> None:
    """
    Track R — on any task failure, write a plain-text breadcrumb so ops
    can spot the problem without opening the Airflow UI.
    """
    ds      = context["ds"]
    task_id = context["task_instance"].task_id
    marker  = Path("/opt/airflow/data/reports") / f"FAILED_{ds}_{task_id}.txt"
    marker.parent.mkdir(parents=True, exist_ok=True)
    marker.write_text(
        f"FAILURE  task={task_id}  ds={ds}  run_id={context['run_id']}\n",
        encoding="utf-8",
    )


with DAG(
    dag_id="team_franklin",
    description="Capstone retail KPI pipeline — Team Franklin",
    start_date=datetime(2026, 6, 1),
    end_date=datetime(2026, 6, 14),
    schedule="@daily",
    catchup=False,
    default_args=DEFAULT_ARGS,
    tags=["lab4", "capstone", "franklin"],
) as dag:

    # ── Task 1: wait for vendor CSV ────────────────────────────────────────
    wait_csv = FileSensor(
        task_id="wait_for_vendor_csv",
        filepath="/opt/airflow/data/incoming/transactions_{{ ds }}.csv",
        poke_interval=30,
        timeout=60 * 20,           # Track R — stop waiting after 20 min
        mode="reschedule",
        on_failure_callback=_write_failure_marker,
    )

    # ── TaskGroup: Bronze → Silver  (Track O) ─────────────────────────────
    with TaskGroup("bronze_to_silver") as bronze_to_silver:

        @task(on_failure_callback=_write_failure_marker)
        def ingest(ds: str) -> dict:
            """Task 2 — read CSV, write typed silver Parquet via DuckDB."""
            return ingest_day(ds)

        @task(on_failure_callback=_write_failure_marker)
        def validate(ds: str) -> dict:
            """
            Task 3 — Track Q: min_revenue=1.0 means a --corrupt day
            (all amount_eur = 0) raises RuntimeError and turns red here,
            blocking Spark from running on bad data.
            """
            return validate_silver(ds, min_rows=10, min_revenue=1.0)

        ingest_result   = ingest()
        validate_result = validate()
        ingest_result >> validate_result

    # ── TaskGroup: Silver → Gold  (Track O) ───────────────────────────────
    with TaskGroup("silver_to_gold") as silver_to_gold:

        @task(on_failure_callback=_write_failure_marker)
        def compute_kpis(ds: str) -> dict:
            """
            Task 4 — PySpark job:
              transform_1 (read + schema) ->
              transform_2 (enrich + broadcast join) ->
              transform_3 (aggregate KPIs)
            Writes curated Parquet + dashboard JSON.
            """
            return run_daily(ds, with_reference=True)

        @task(on_failure_callback=_write_failure_marker)
        def publish(ds: str) -> dict:
            """Task 5 — verify dashboard JSON was written; return its path."""
            path = report_json(ds)
            if not path.exists():
                raise FileNotFoundError(f"Report missing: {path}")
            return {"report_path": str(path), "status": "ready"}

        compute_result = compute_kpis()
        publish_result = publish()
        compute_result >> publish_result

    # ── Top-level dependency graph ─────────────────────────────────────────
    wait_csv >> bronze_to_silver >> silver_to_gold