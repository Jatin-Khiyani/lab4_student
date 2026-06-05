"""Minimal PySpark smoke check baked into the Lab 4 Docker image."""
from __future__ import annotations

import json

from pyspark.sql import SparkSession

from include.paths import report_json


def run_daily(logical_date: str, *, with_reference: bool = False) -> dict:
    """Start Spark, run spark.range(1).count(), write a small dashboard JSON."""
    _ = with_reference  # CLI compatibility only

    spark = SparkSession.builder.appName("lab4_smoke").master("local[*]").getOrCreate()
    try:
        smoke_count = spark.range(1).count()
        spark_version = spark.version
    finally:
        spark.stop()

    out_json = report_json(logical_date)
    out_json.parent.mkdir(parents=True, exist_ok=True)
    payload = {
        "logical_date": logical_date,
        "status": "smoke_ok",
        "spark_version": spark_version,
        "smoke_count": smoke_count,
    }
    out_json.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    return payload


def run_daily_cli() -> None:
    import argparse

    parser = argparse.ArgumentParser(description="Lab 4 Spark smoke check")
    parser.add_argument("--date", required=True, help="Logical date YYYY-MM-DD")
    parser.add_argument("--with-reference", action="store_true")
    args = parser.parse_args()
    print(json.dumps(run_daily(args.date, with_reference=args.with_reference)))


if __name__ == "__main__":
    run_daily_cli()
