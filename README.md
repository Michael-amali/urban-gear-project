# UrbanGear Daily Sales Pipeline - Hybrid Local/AWS

Production pattern: S3 -> PySpark ETL (quality tagging + routing) -> Quality Gate -> Postgres (mock Redshift) -> dbt -> Tests

## Quick Start (Local)

```bash
make setup
make generate-data   # 7 days x 50k records with 7% bad records
make test            # pytest spark logic
make run-etl PROCESS_DATE=2026-03-21
make run-dbt         # requires warehouse up
```

## Docker Full Stack

```bash
docker-compose up -d
docker-compose up airflow-init  # first time only
# Airflow UI http://localhost:8080 airflow/airflow
# Warehouse Postgres localhost:5433 urbangear/urbangear123
# Trigger DAG urbangear_daily_sales_pipeline
docker-compose logs -f airflow-scheduler
```

## ENV Switching

ENV=local (default): uses ./data/raw, ./data/processed, postgres mock
ENV=prod: uses s3://urbangear-data-lake/*, Redshift

Set in .env: ENV=prod and AWS creds

## Project Layout
- etl/etl_job.py : Core Spark job with explicit schema, PERMISSIVE mode, metrics sidecar
- dags/ : Airflow DAG with BranchPythonOperator quality gate
- dbt/urbangear : dbt-postgres models, incremental delete+insert
- scripts/generate_sample_data.py : Synthetic data with negative qty, future dates, nulls, corrupt JSON
- tests/ : Spark unit tests
- config/ : Centralized config

## Quality Rules
- null critical fields -> rejected
- quantity <=0 -> rejected
- future order_date -> rejected
- corrupt JSON -> dead-letter
- reject_rate >5% -> DAG aborts (BranchPythonOperator)

## Verification
SELECT COUNT(*) FROM analytics.fct_daily_sales WHERE net_revenue <0; -- expect 0