# lib/lab4_starter - baked into the Docker image

This folder is copied into the image at build time. It powers the **`lab4_starter`** smoke DAG only.

The Spark step runs `spark.range(1).count()` and writes a dashboard JSON with `"status": "smoke_ok"`. It is **not** your graded KPI code (that lives in `include/team_<name>_spark.py`).

After any change here, rebuild:

```bash
docker compose build airflow-init
docker compose up -d --force-recreate
```
