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
import json, os, sys, tempfile, glob
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
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor
from airflow.providers.slack.operators.slack_webhook import SlackWebhookOperator
from airflow.models.param import Param

from config.pipeline_config import PipelineConfig
from config.s3_client import get_s3_client

PipelineConfig.reload()
S3_BUCKET = os.getenv("S3_BUCKET", PipelineConfig.S3_BUCKET)

# Prefer DAG param process_date; fall back to Airflow logical date (ds).
PROCESS_DATE_TMPL = "{{ params.process_date or ds }}"


def _process_date(context) -> str:
    """Resolve the partition date for this run."""
    params = context.get("params") or {}
    return params.get("process_date") or context["ds"]


def _check_source_data(**context):
    """
    Verify source files exist before starting the ETL.
    Prevents running the pipeline on empty data (silent failure).
    """
    process_date = _process_date(context)
    PipelineConfig.reload()

    if PipelineConfig.use_local_fs():
        base = Path(f"/opt/airflow/data/raw/orders/dt={process_date}")
        if not base.exists():
            base = Path(f"data/raw/orders/dt={process_date}")
        if not base.exists() or not list(base.glob("*.json")):
            raise FileNotFoundError(f"No source files at {base}")
        count = len(list(base.glob("*.json")))
        print(f"[CHECK] {count} local files for {process_date} at {base}")
        return count


    hook = S3Hook(aws_conn_id="s3_hook_conn_id")
    prefix = f"raw/orders/dt={process_date}/"
    keys = hook.list_keys(bucket_name=S3_BUCKET, prefix=prefix)

    if not keys:
        raise FileNotFoundError(
            f"No source files at s3://{S3_BUCKET}/{prefix}. "
            "Source system export may not have completed."
        )

    context["ti"].xcom_push(key="source_file_count", value=len(keys))
    print(f"[CHECK] {len(keys)} source files found for {process_date}")
    return len(keys)


def _run_spark_etl(**context):
    from etl.etl_job import main as etl_main
    process_date = _process_date(context)
    etl_main(process_date=process_date, env=os.getenv("ENV", "local"))


def _quality_gate(**context):
    """
    Read quality metrics written by the Spark job.
    Returns the task_id to execute next:
    - 'load_to_postgres.truncate_staging'  if quality passes
    - 'alert_quality_failure'              if quality fails
    """
    process_date = _process_date(context)
    PipelineConfig.reload()

    if PipelineConfig.use_local_fs():
        metrics = None
        for p in [
            f"data/metrics/orders/dt={process_date}/metrics.json",
            f"/opt/airflow/data/metrics/orders/dt={process_date}/metrics.json",
        ]:
            if Path(p).exists():
                metrics = json.loads(Path(p).read_text())
                break
        if metrics is None:
            raise RuntimeError(f"Metrics not found for {process_date}")
    else:
        hook = S3Hook(aws_conn_id="s3_hook_conn_id")
        metrics_key = f"metrics/orders/dt={process_date}/metrics.json"

        try:
            obj = hook.get_key(metrics_key, bucket_name=S3_BUCKET)
            metrics = json.loads(obj.get()["Body"].read().decode())
        except Exception as e:
            raise RuntimeError(f"Could not read quality metrics: {e}")

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
    import pandas as pd
    from sqlalchemy import create_engine

    process_date = _process_date(ctx)
    year, month, day = process_date.split("-")
    PipelineConfig.reload()

    if PipelineConfig.use_local_fs():
        parquet_path = "data/processed/orders/"
        pattern = f"{parquet_path}/order_year={year}/order_month={int(month)}/order_day={int(day)}"
        if not Path(pattern).exists():
            print(f"No partition at {pattern}, searching all")
            files = glob.glob(f"{parquet_path}/**/*.parquet", recursive=True)
            if not files:
                print("No parquet files found, skipping load")
                return
            df = pd.concat([pd.read_parquet(f) for f in files[:5]])
        else:
            df = pd.read_parquet(pattern)
    else:
        partition_prefix = (
            f"processed/orders/order_year={year}/"
            f"order_month={int(month)}/order_day={int(day)}/"
        )
        s3 = get_s3_client()
        resp = s3.list_objects_v2(Bucket=S3_BUCKET, Prefix=partition_prefix)
        keys = [
            obj["Key"]
            for obj in resp.get("Contents", [])
            if obj["Key"].endswith(".parquet")
        ]
        if not keys:
            print(f"No parquet files at s3://{S3_BUCKET}/{partition_prefix}, skipping load")
            return

        with tempfile.TemporaryDirectory() as tmpdir:
            local_files = []
            for key in keys:
                local_path = Path(tmpdir) / key.replace("/", "__")
                s3.download_file(S3_BUCKET, key, str(local_path))
                local_files.append(local_path)
            df = pd.concat([pd.read_parquet(f) for f in local_files])

    print(f"Loading {len(df)} rows to postgres")

    eng = create_engine(
        "postgresql://urbangear:urbangear123@postgres-warehouse:5432/urbangear_dw"
    )
    eng.execute("CREATE SCHEMA IF NOT EXISTS staging;")
    df.to_sql(
        "daily_orders",
        eng,
        schema="staging",
        if_exists="replace",
        index=False,
        method="multi",
        chunksize=5000,
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
    params={
        "process_date": Param(None, type=["null", "string"], format="date"),
    },
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

    wait_for_files = S3KeySensor(
        task_id      = "wait_for_source_files",
        bucket_key   = f"raw/orders/dt={PROCESS_DATE_TMPL}/part-*.json",
        bucket_name  = S3_BUCKET,
        aws_conn_id  = "s3_hook_conn_id",
        wildcard_match = True,
        poke_interval = 300,    # Check every 5 minutes
        timeout       = 3600,   # Fail if not found within 1 hour
        mode          = "reschedule",
    )

    run_spark = PythonOperator(
        task_id="run_spark_etl", 
        python_callable=_run_spark_etl
    )

    quality_gate = BranchPythonOperator(
        task_id="quality_gate", 
        python_callable=_quality_gate
    )

    alert_quality_failure = SlackWebhookOperator(
        task_id = "alert_quality_failure",
        slack_webhook_conn_id = "slack_alert_conn_id",
        message = (
            ":x: *UrbanGear Pipeline ABORTED — Quality Gate Failed*\n"
            f"*Date:* {PROCESS_DATE_TMPL}\n"
            "*Reject Rate:* {{ ti.xcom_pull(task_ids='quality_gate', key='reject_rate') }}\n"
            f"*Action:* Investigate s3://urbangear-data-lake/rejected/orders/dt={PROCESS_DATE_TMPL}/"
        ),
    )

    with TaskGroup(group_id="load_to_postgres") as load_to_postgres:
        truncate_staging = PostgresOperator(
            task_id="truncate_staging",
            postgres_conn_id="postgres_warehouse",
            sql=f"""
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
            WHERE order_date::date = '{PROCESS_DATE_TMPL}'::date;
            """
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

    alert_success = SlackWebhookOperator(
        task_id="alert_success",
        slack_webhook_conn_id="slack_alert_conn_id",
        message=(
            ":white_check_mark: *UrbanGear Daily Pipeline Complete*\n"
            f"*Date:* {PROCESS_DATE_TMPL}\n"
            "*Clean records:* {{ ti.xcom_pull(task_ids='quality_gate', key='clean_count') }}\n"
            "*Dashboard:* <https://bi.urbangear.com/sales|View Sales Dashboard>"
        ),
        trigger_rule="none_failed_min_one_success",
    )


    (
        check_source
        >> wait_for_files 
        >> run_spark 
        >> quality_gate 
        >> [alert_quality_failure, load_to_postgres]
    )
    load_to_postgres >> run_dbt >> alert_success