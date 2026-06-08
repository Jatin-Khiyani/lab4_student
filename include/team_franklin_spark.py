"""
Team Franklin — PySpark KPI job for Lab 4 capstone.

Transforms
  1. transform_1  read silver Parquet with strict schema, filter bad rows
  2. transform_2  enrich: parse hour of day + broadcast join category targets (Track S)
  3. transform_3  aggregate KPIs by category × country with revenue-vs-target %
"""
from __future__ import annotations

import json

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    DoubleType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from include.paths import curated_kpis, raw_parquet, reference_targets, report_json

# Explicit schema — Track S requirement; rejects rows that don't match at read time
SILVER_SCHEMA = StructType([
    StructField("tx_id",          StringType(),    nullable=False),
    StructField("category",       StringType(),    nullable=False),
    StructField("payment_method", StringType(),    nullable=True),
    StructField("country",        StringType(),    nullable=False),
    StructField("amount_eur",     DoubleType(),    nullable=False),
    StructField("ts",             TimestampType(), nullable=True),
])


def transform_1(spark: SparkSession, logical_date: str) -> DataFrame:
    """Read silver Parquet with strict schema; drop rows with null or zero amounts."""
    pq_path = str(raw_parquet(logical_date))
    df = spark.read.schema(SILVER_SCHEMA).parquet(pq_path)
    df = df.filter(
        F.col("amount_eur").isNotNull()
        & (F.col("amount_eur") > 0)
        & F.col("category").isNotNull()
        & F.col("country").isNotNull()
    )
    return df


def transform_2(spark: SparkSession, df: DataFrame, logical_date: str) -> DataFrame:
    """
    Enrich the transaction data:
      - Extract hour of day from the timestamp column
      - Broadcast-join the small category_targets reference file (Track S)

    broadcast() replicates the 7-row reference table to every Spark worker,
    avoiding a full shuffle — correct choice when one side is tiny.
    """
    # Parse hour for peak-time analysis
    df = df.withColumn("hour", F.hour(F.col("ts")))

    # Broadcast join — safe because category_targets.csv has only 7 rows
    ref_path = reference_targets()
    if ref_path.exists():
        ref_df = (
            spark.read
            .option("header", True)
            .csv(str(ref_path))
            .withColumn("target_revenue_eur", F.col("target_revenue_eur").cast(DoubleType()))
        )
        df = df.join(F.broadcast(ref_df), on="category", how="left")
    else:
        # Graceful fallback if --reference was never run
        df = df.withColumn("target_revenue_eur", F.lit(None).cast(DoubleType()))

    return df


def transform_3(df: DataFrame) -> DataFrame:
    """
    Aggregate KPIs by category × country:
      - total revenue
      - transaction count
      - average transaction value
      - revenue vs target percentage (null when no target available)
    """
    return (
        df.groupBy("category", "country", "target_revenue_eur")
        .agg(
            F.round(F.sum("amount_eur"),  2).alias("revenue_eur"),
            F.count("tx_id")              .alias("tx_count"),
            F.round(F.avg("amount_eur"),  2).alias("avg_tx_eur"),
        )
        .withColumn(
            "revenue_vs_target_pct",
            F.when(
                F.col("target_revenue_eur").isNotNull() & (F.col("target_revenue_eur") > 0),
                F.round((F.col("revenue_eur") / F.col("target_revenue_eur")) * 100, 1),
            ).otherwise(F.lit(None).cast(DoubleType())),
        )
        .orderBy("category", "country")
    )


def run_daily(logical_date: str, *, with_reference: bool = True) -> dict:
    """
    Entry point called from the Airflow compute_kpis task.

    Runs transform_1 -> transform_2 -> transform_3, then writes:
      - data/curated/dt=<date>/kpis/       (Gold Parquet, overwrite = idempotent)
      - data/reports/dashboard_<date>.json  (totals + extra KPIs for the BI dashboard)
    """
    spark = (
        SparkSession.builder
        .appName(f"franklin_kpis_{logical_date}")
        .master("local[*]")
        .config("spark.sql.shuffle.partitions", "4")   # small data — avoid 200 shuffles
        .getOrCreate()
    )

    try:
        df_silver   = transform_1(spark, logical_date)
        df_enriched = transform_2(spark, df_silver, logical_date) if with_reference else df_silver.withColumn("hour", F.hour(F.col("ts"))).withColumn("target_revenue_eur", F.lit(None).cast(DoubleType()))
        df_kpis     = transform_3(df_enriched)

        # ── Write Gold Parquet (overwrite for idempotence) ──────────────────
        out_parquet = curated_kpis(logical_date).parent / "kpis"
        out_parquet.mkdir(parents=True, exist_ok=True)
        df_kpis.write.mode("overwrite").parquet(str(out_parquet))

        # ── Additional aggregations collected for the JSON report ────────────
        totals = df_silver.agg(
            F.count("tx_id")                      .alias("tx_count"),
            F.round(F.sum("amount_eur"), 2)       .alias("total_revenue_eur"),
        ).collect()[0]

        top_category = (
            df_silver.groupBy("category")
            .agg(F.sum("amount_eur").alias("rev"))
            .orderBy(F.col("rev").desc())
            .first()["category"]
        )

        top_country = (
            df_silver.groupBy("country")
            .agg(F.sum("amount_eur").alias("rev"))
            .orderBy(F.col("rev").desc())
            .first()["country"]
        )

        peak_row = (
            df_enriched.groupBy("hour")
            .agg(F.count("tx_id").alias("cnt"))
            .orderBy(F.col("cnt").desc())
            .first()
        )

        payment_rows = (
            df_silver.groupBy("country", "payment_method")
            .agg(F.count("tx_id").alias("tx_count"))
            .orderBy("country", "payment_method")
            .collect()
        )

    finally:
        spark.stop()

    # ── Write JSON dashboard report (overwrite for idempotence) ─────────────
    out_json = report_json(logical_date)
    out_json.parent.mkdir(parents=True, exist_ok=True)

    payload = {
        "logical_date":       logical_date,
        "status":             "ok",
        "total_tx_count":     int(totals["tx_count"]),
        "total_revenue_eur":  float(totals["total_revenue_eur"]),
        "top_category":       top_category,
        "top_country":        top_country,
        "peak_hour":          int(peak_row["hour"]) if peak_row else None,
        "peak_hour_tx_count": int(peak_row["cnt"])  if peak_row else None,
        "payment_breakdown": [
            {"country": r["country"], "method": r["payment_method"], "tx_count": int(r["tx_count"])}
            for r in payment_rows
        ],
        "curated_path": str(out_parquet),
    }

    out_json.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    return payload