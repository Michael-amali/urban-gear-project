# UrbanGear Daily Sales Pipeline - local / MinIO / AWS

Production pattern: storage → PySpark ETL (quality tagging + routing) → Quality Gate → Postgres (mock Redshift) → dbt → Tests

## Quick Start

```bash
make setup
make generate-data   # respects ENV / --env
make test
make run-etl PROCESS_DATE=2026-03-21
make run-dbt         # requires warehouse up
```

## ENV Switching

| ENV | Storage | Notes |
|-----|---------|-------|
| `local` (default) | `./data/{raw,processed,rejected,metrics}` | No cloud creds needed |
| `local-s3` | MinIO Play `s3a://urbangear-data-lake-myk/...` | Needs `S3_ENDPOINT_URL` + access/secret keys |
| `prod` | AWS S3 `s3a://{S3_BUCKET}/...` | Clear `S3_ENDPOINT_URL`; use AWS creds |

Override per command:

```bash
python scripts/generate_sample_data.py --env local --days 1 --records-per-day 2000
python scripts/generate_sample_data.py --env local-s3 --days 1 --records-per-day 2000
python etl/etl_job.py --process_date 2026-06-20 --env local
python etl/etl_job.py --process_date 2026-06-20 --env local-s3
python etl/etl_job.py --process_date 2026-06-20 --env prod
```

Or set `ENV` in `.env` / the shell (`ENV=local-s3 make generate-data`).

## Docker Full Stack

```bash
docker-compose up -d
docker-compose up airflow-init  # first time only
# Trigger DAG urbangear_daily_sales_pipeline
docker-compose logs -f airflow-scheduler
```

`ENV` (and MinIO/AWS vars) are passed into webserver/scheduler from `.env`.

## Project Layout
- etl/etl_job.py : Core Spark job with explicit schema, PERMISSIVE mode, metrics sidecar
- dags/ : Airflow DAG with BranchPythonOperator quality gate
- dbt/urbangear : dbt-postgres models, incremental delete+insert
- scripts/generate_sample_data.py : Synthetic data with negative qty, future dates, nulls, corrupt JSON
- tests/ : Spark unit tests
- config/ : Centralized config + shared MinIO/S3 client

## Quality Rules
- null critical fields -> rejected
- quantity <=0 -> rejected
- future order_date -> rejected
- corrupt JSON -> dead-letter
- reject_rate >5% -> DAG aborts (BranchPythonOperator)

## Verification
SELECT COUNT(*) FROM analytics.fct_daily_sales WHERE net_revenue <0; -- expect 0
