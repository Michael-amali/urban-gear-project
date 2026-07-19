"""
UrbanGear Daily Sales Pipeline — Airflow DAG
=============================================
Orchestrates: Check → Wait → Spark ETL → Quality Gate
             → [Load Redshift | Alert Failure] → dbt → Slack Success

Design principles:
- Idempotent: safe to re-run for any date
- catchup=False: no accidental historical backfill
- max_active_runs=1: prevents overlapping daily runs
- Every task has retries and execution_timeout
"""

from datetime import datetime, timedelta
import json, os, sys
from pathlib import Path

from dotenv import load_dotenv
load_dotenv()

# Ensure project root in path
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.utils.task_group import TaskGroup
from airflow.providers.postgres.operators.postgres import PostgresOperator

S3_BUCKET = os.getenv("S3_BUCKET", "urbangear-data-lake")


def _check_source_data(**context):
    """
    Verify source files exist in S3 before starting the ETL.
    Prevents running the pipeline on empty data (silent failure).
    """
    process_date = "2026-06-21" # context["ds"]
    base = Path(f"/opt/airflow/data/raw/orders/dt={process_date}")
    
    # also check local mount
    if not base.exists():
        base = Path(f"data/raw/orders/dt={process_date}")
    if not base.exists() or not list(base.glob("*.json")):
        raise FileNotFoundError(f"No source files at {base}")
    
    count = len(list(base.glob("*.json")))
    print(f"[CHECK] {count} files for {process_date}")
    return count


def _run_spark_etl(**context):
    from config.pipeline_config import PipelineConfig
    from etl.etl_job import main as etl_main
    process_date = "2026-06-21" # context["ds"]
    etl_main(process_date=process_date, env=os.getenv("ENV", "local"))


def _quality_gate(**context):
    """
    Read quality metrics written by the Spark job.
    Returns the task_id to execute next:
    - 'load_to_postgres.truncate_staging'  if quality passes
    - 'alert_quality_failure'              if quality fails
    """
    process_date = "2026-06-21" # context["ds"]
    # local metrics path
    for p in [f"data/metrics/orders/dt={process_date}/metrics.json", f"/opt/airflow/data/metrics/orders/dt={process_date}/metrics.json"]:
        if Path(p).exists():
            metrics = json.loads(Path(p).read_text())
            break
    else:
        raise RuntimeError(f"Metrics not found for {process_date}")

    reject_rate = metrics.get("reject_rate", 1.0)
    context["ti"].xcom_push(key="reject_rate", value=reject_rate)
    context["ti"].xcom_push(key="clean_count", value=metrics.get("clean_count", 0))
    context["ti"].xcom_push(key="rejected_count", value=metrics.get("rejected_count", 0))

    if reject_rate > 0.05:
        print(f"QUALITY FAILED  — reject_rate={reject_rate:.2%}")
        return "alert_quality_failure"
    
    print(f"QUALITY PASSED  — reject_rate={reject_rate:.2%}")
    return "load_to_postgres.truncate_staging"


def _load_postgres(**ctx):
    import pandas as pd, glob
    from sqlalchemy import create_engine
    
    ds = "2026-06-21" # ctx["ds"]
    parquet_path = f"data/processed/orders/"
    # read partitioned parquet for that date using pyarrow
    import pyarrow.parquet as pq
    # filter by partition folders if exist
    year, month, day = ds.split("-")

    pattern = f"{parquet_path}/order_year={year}/order_month={int(month)}/order_day={int(day)}"

    if not Path(pattern).exists():
        # fallback: read all and filter
        print(f"No partition at {pattern}, searching all")
        files = glob.glob(f"{parquet_path}/**/*.parquet", recursive=True)
        if not files:
            print("No parquet files found, skipping load")
            return
        df = pd.concat([pd.read_parquet(f) for f in files[:5]])
    else:
        df = pd.read_parquet(pattern)

    print(f"Loading {len(df)} rows to postgres")

    eng = create_engine(
        "postgresql://urbangear:urbangear123@postgres-warehouse:5432/urbangear_dw"
    )
    # ensure staging schema
    eng.execute("CREATE SCHEMA IF NOT EXISTS staging;")
    df.to_sql(
        "daily_orders", 
        eng, 
        schema="staging", 
        if_exists="replace", 
        index=False, 
        method="multi", 
        chunksize=5000
    )


DEFAULT_ARGS = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
    "execution_timeout": timedelta(hours=2),
}

with DAG(
    dag_id="urbangear_daily_sales_pipeline",
    default_args=DEFAULT_ARGS,
    description="Daily sales: raw JSON -> PySpark -> Postgres (mock Redshift) -> dbt",
    schedule="0 4 * * *",
    start_date=datetime(2025, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["production", "sales", "daily", "local"],
) as dag:

    check_source = PythonOperator(
        task_id="check_source_data", 
        python_callable=_check_source_data
    )

    run_spark = PythonOperator(
        task_id="run_spark_etl", 
        python_callable=_run_spark_etl
    )

    quality_gate = BranchPythonOperator(
        task_id="quality_gate", 
        python_callable=_quality_gate
    )

    alert_quality_failure = EmptyOperator(
        task_id="alert_quality_failure"
    )

    with TaskGroup(group_id="load_to_postgres") as load_to_postgres:
        truncate_staging = PostgresOperator(
            task_id="truncate_staging",
            postgres_conn_id="postgres_warehouse",
            sql="""
            CREATE SCHEMA IF NOT EXISTS staging;

            DROP TABLE IF EXISTS staging.daily_orders;

            CREATE TABLE staging.daily_orders (
                order_id VARCHAR,
                customer_id VARCHAR,
                product_id VARCHAR,
                platform VARCHAR,
                order_date TIMESTAMP,
                product_name VARCHAR,
                category VARCHAR,
                quantity INTEGER,
                unit_price NUMERIC(12,2),
                discount_pct NUMERIC(5,2),
                shipping_cost NUMERIC(12,2),
                tax_amount NUMERIC(12,2),
                payment_method VARCHAR,
                shipping_country VARCHAR,
                shipping_state VARCHAR,
                shipping_city VARCHAR,
                is_returned BOOLEAN,
                rejection_reason VARCHAR,
                gross_revenue NUMERIC(12,2),
                discount_amount NUMERIC(12,2),
                net_revenue NUMERIC(12,2),
                total_amount NUMERIC(12,2),
                order_hour INTEGER,
                day_of_week INTEGER,
                is_weekend BOOLEAN,
                order_item_count INTEGER,
                order_total_revenue NUMERIC(12,2),
                is_high_value BOOLEAN,
                etl_processed_at TIMESTAMP,
                etl_process_date DATE
            );

            DELETE FROM staging.daily_orders
            WHERE order_date::date = '2026-06-21'::date;
            """
            # sql="DELETE FROM staging.daily_orders WHERE order_date::date = '{{ ds }}'::date;"

        )

        # COPY from parquet to postgres via python for local mock
        copy_to_staging = PythonOperator(
            task_id="copy_to_staging", 
            python_callable=_load_postgres
        )

        truncate_staging >> copy_to_staging

    run_dbt = BashOperator(
        task_id="run_dbt_transformations",
        bash_command="cd /opt/airflow/dbt/urbangear && dbt run --profiles-dir . --target local && dbt test --profiles-dir . --target local"
    )

    alert_success = EmptyOperator(
        task_id="alert_success", 
        trigger_rule="none_failed_min_one_success"
    )

    (
        check_source 
        >> run_spark 
        >> quality_gate 
        >> [alert_quality_failure, load_to_postgres]
    )
    load_to_postgres >> run_dbt >> alert_success