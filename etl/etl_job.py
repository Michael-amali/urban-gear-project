"""
UrbanGear Daily Sales ETL Job - Hybrid Local/S3
Run: python etl/etl_job.py --process_date 2026-03-21 --env local
"""
import sys, json, logging, argparse, os
from datetime import datetime, timedelta
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, TimestampType, BooleanType
from pyspark.sql.window import Window
from config.pipeline_config import PipelineConfig
from dotenv import load_dotenv
load_dotenv()

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

RAW_ORDER_SCHEMA = StructType([
    StructField("order_id", StringType(), nullable=False),
    StructField("customer_id", StringType(), nullable=True),
    StructField("platform", StringType(), nullable=True),
    StructField("order_date", TimestampType(), nullable=True),
    StructField("product_id", StringType(), nullable=True),
    StructField("product_name", StringType(), nullable=True),
    StructField("category", StringType(), nullable=True),
    StructField("quantity", IntegerType(), nullable=True),
    StructField("unit_price", DoubleType(), nullable=True),
    StructField("discount_pct", DoubleType(), nullable=True),
    StructField("shipping_cost", DoubleType(), nullable=True),
    StructField("tax_amount", DoubleType(), nullable=True),
    StructField("payment_method", StringType(), nullable=True),
    StructField("shipping_country", StringType(), nullable=True),
    StructField("shipping_state", StringType(), nullable=True),
    StructField("shipping_city", StringType(), nullable=True),
    StructField("is_returned", BooleanType(), nullable=True),

    StructField("_corrupt_record", StringType(), nullable=True),
])

def create_spark_session(app_name: str) -> SparkSession:
    return (
        SparkSession.builder.appName(app_name)
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
        .config("spark.sql.shuffle.partitions", str(PipelineConfig.SHUFFLE_PARTITIONS))
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
        .config("spark.sql.parquet.compression.codec", "snappy")
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        .getOrCreate()
    )

def extract_raw_data(spark: SparkSession, process_date: str) -> DataFrame:
    if PipelineConfig.ENV == "local":
        raw_path = f"{PipelineConfig.S3_RAW}/orders/dt={process_date}/"
        logger.info(f"[LOCAL] Reading from {raw_path}")
        if not Path(raw_path).exists():
            raise FileNotFoundError(f"No source data at {raw_path}. Run make generate-data")
        df = (
            spark.read
            .schema(RAW_ORDER_SCHEMA)
            .option("mode", "PERMISSIVE")
            .option("columnNameOfCorruptRecord", "_corrupt_record")
            .json(raw_path)
        )
    else:
        raw_path = f"{PipelineConfig.S3_RAW}/orders/dt={process_date}/"
        df = (
            spark.read
              .schema(RAW_ORDER_SCHEMA)
              .option("mode", "PERMISSIVE")
              .option("columnNameOfCorruptRecord", "_corrupt_record")
              .json(raw_path)
        )

    count = df.count()
    logger.info(f"[EXTRACT] {count:,} records")

    if count < PipelineConfig.MIN_RECORD_COUNT:
        raise ValueError(
            f"Record count {count:,} is below the minimum threshold "
            f"{PipelineConfig.MIN_RECORD_COUNT:,}. "
            "Source export may have failed. Aborting pipeline."
        )
    
    return df

def tag_and_route(df: DataFrame, process_date: str):
    """
    Tag each record with quality flags and route to clean or rejected.

    Returns: (clean_df, rejected_df)

    The tagging approach (vs silent filter) means every rejected row
    has a documented reason. This is required for any regulated industry
    and makes incident investigation fast.
    """
    tagged = (
        df
        # Individual quality flags
        .withColumn("_null_critical",
            F.col("order_id").isNull()
            | F.col("customer_id").isNull()
            | F.col("order_date").isNull()
            | F.col("quantity").isNull()
            | F.col("unit_price").isNull()
        )
        .withColumn("_negative_qty", F.col("quantity") <= 0)
        .withColumn("_future_date", F.col("order_date") > F.current_timestamp())
        .withColumn("_invalid_platform", ~F.col("platform").isin(PipelineConfig.VALID_PLATFORMS))
        .withColumn("_corrupt_json", F.col("_corrupt_record").isNotNull())
        # Composite rejection flag
        .withColumn("_is_rejected",
            F.col("_null_critical")
            | F.col("_negative_qty")
            | F.col("_future_date")
            | F.col("_corrupt_json")
        )
        # Human-readable rejection reason
        .withColumn("rejection_reason",
            F.when(F.col("_corrupt_json"),    F.lit("corrupt_json"))
             .when(F.col("_null_critical"),   F.lit("null_critical_field"))
             .when(F.col("_negative_qty"),    F.lit("negative_quantity"))
             .when(F.col("_future_date"),     F.lit("future_order_date"))
             .otherwise(None)
        )
    )

    # Route: clean vs rejected
    rejected_df = tagged.filter(F.col("_is_rejected"))
    clean_df = tagged.filter(~F.col("_is_rejected"))

    logger.info(f"[QUALITY] Clean: {clean_df.count():,} Rejected: {rejected_df.count():,}")
    return clean_df, rejected_df

def transform_clean(df: DataFrame, process_date: str) -> DataFrame:
    """Apply business transformations to clean records."""
    window_order = Window.partitionBy("order_id")

    return (
        df
        # Calculated revenue measures
        .withColumn("gross_revenue",
            F.round(F.col("quantity") * F.col("unit_price"), 2))
        .withColumn("discount_amount",
            F.round(
                F.col("gross_revenue") * F.coalesce(F.col("discount_pct"), F.lit(0.0)) / 100,
                2
            ))
        .withColumn("net_revenue",
            F.round(F.col("gross_revenue") - F.col("discount_amount"), 2))
        .withColumn("total_amount",
            F.round(
                F.col("net_revenue")
                + F.coalesce(F.col("shipping_cost"), F.lit(0.0))
                + F.coalesce(F.col("tax_amount"),    F.lit(0.0)),
                2
            ))
        # Standardize text fields
        .withColumn("platform",          F.lower(F.trim(F.col("platform"))))
        .withColumn("payment_method",    F.upper(F.trim(F.col("payment_method"))))
        .withColumn("shipping_country",  F.upper(F.trim(F.col("shipping_country"))))
        # Date parts for partitioning
        .withColumn("order_year",  F.year(F.col("order_date")))
        .withColumn("order_month", F.month(F.col("order_date")))
        .withColumn("order_day",   F.dayofmonth(F.col("order_date")))
        .withColumn("order_hour",  F.hour(F.col("order_date")))
        .withColumn("day_of_week", F.dayofweek(F.col("order_date")))
        .withColumn("is_weekend",  F.col("day_of_week").isin(1, 7))
        # Order-level aggregates via window (no groupBy needed)
        .withColumn("order_item_count",
            F.count("*").over(window_order))
        .withColumn("order_total_revenue",
            F.sum("net_revenue").over(window_order))
        .withColumn("is_high_value",
            F.col("order_total_revenue") > PipelineConfig.MAX_ORDER_VALUE)
        # ETL metadata
        .withColumn("etl_processed_at",  F.current_timestamp())
        .withColumn("etl_process_date",  F.lit(process_date))
        # Drop internal flag columns
        .drop("_null_critical", "_negative_qty", "_future_date",
              "_invalid_platform", "_corrupt_json", "_is_rejected",
              "_corrupt_record")
    )

def collect_metrics(raw_df: DataFrame, clean_df: DataFrame, rejected_df: DataFrame, process_date: str) -> dict:
    """
    Collect quality metrics and return as a dict.
    This dict is written to S3/local as a JSON sidecar file and read by the
    Airflow quality gate task to decide whether to proceed or abort.
    """
    total = raw_df.count()
    clean = clean_df.count()
    rejected = rejected_df.count()

    try:
        reasons = rejected_df.groupBy("rejection_reason").count().collect()
        rule_counts = {r["rejection_reason"]: r["count"] for r in reasons}
    except:
        rule_counts = {}

    metrics = {
        "process_date": process_date, 
        "total_count": total, 
        "clean_count": clean, 
        "rejected_count": rejected, 
        "reject_rate": round(rejected/total, 4) if total else 0, 
        "rule_counts": rule_counts, 
        "generated_at": datetime.now().isoformat()
    }
    logger.info(f"[METRICS] {metrics}")
    return metrics

def write_outputs(clean_df: DataFrame, rejected_df: DataFrame, metrics: dict, process_date: str):
    """Write clean records, rejected records, and metrics to S3/local."""
    # Clean records: partitioned Parquet for efficient downstream reads

    processed_path = f"{PipelineConfig.S3_PROCESSED}/orders/"
    rejected_path = f"{PipelineConfig.S3_REJECTED}/orders/dt={process_date}/"
    metrics_dir = Path(f"{PipelineConfig.S3_METRICS}/orders/dt={process_date}/")
    metrics_dir.mkdir(parents=True, exist_ok=True)

    if PipelineConfig.ENV == "local":
        (
            clean_df
            .repartition(F.col("order_year"), F.col("order_month"), F.col("order_day"))
            .write.mode("overwrite")
            .partitionBy("order_year", "order_month", "order_day")
            .parquet(processed_path)
        )
        if rejected_df.count() > 0:
            (
                rejected_df
                .coalesce(1)
                .write.mode("overwrite")
                .parquet(rejected_path)
            )

        (metrics_dir / "metrics.json").write_text(json.dumps(metrics, indent=2))
    else:
        import boto3
        (
            clean_df
            .repartition(F.col("order_year"), F.col("order_month"), F.col("order_day"))
            .write.mode("overwrite")
            .partitionBy("order_year", "order_month", "order_day")
            .parquet(processed_path)
         )
        if rejected_df.count() > 0:
            (
                rejected_df
                .coalesce(1)
                .write.mode("overwrite")
                .parquet(rejected_path)
            )

        # Metrics sidecar (read by Airflow quality gate)
        metrics_path = f"{PipelineConfig.S3_METRICS}/orders/dt={process_date}/metrics.json"

        s3 = boto3.client("s3", region_name=PipelineConfig.AWS_REGION)
        bucket, key = metrics_path.replace("s3://", "").split("/", 1)
        s3.put_object(Bucket=bucket, Key=key, Body=json.dumps(metrics))

    logger.info(f"[WRITE] Done - Clean: {processed_path} Rejected: {rejected_path} Metrics: {metrics_dir}")


def main(process_date: str = None, env: str = None):
    if env: os.environ["ENV"] = env
    if process_date is None:
        process_date = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")

    logger.info("=" * 60)
    logger.info("URBANGEAR DAILY SALES ETL")
    logger.info(f"Processing date: {process_date}")
    logger.info(f"Started: {datetime.now().isoformat()}")
    logger.info("=" * 60)

    spark = create_spark_session("urbangear_daily_sales_etl")

    try:
        raw_df = extract_raw_data(spark, process_date)
        clean_df, rejected_df = tag_and_route(raw_df, process_date)
        transformed_df = transform_clean(clean_df, process_date)
        metrics = collect_metrics(raw_df, transformed_df, rejected_df, process_date)
        write_outputs(transformed_df, rejected_df, metrics, process_date)

        logger.info("=" * 60)
        logger.info(f"ETL COMPLETE — {process_date}")
        logger.info(f"Clean: {metrics['clean_count']:,} | Rejected: {metrics['rejected_count']:,} | Rate: {metrics['reject_rate']:.2%}")
        logger.info("=" * 60)

    finally:
        spark.stop()

if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--process_date", default=None)
    p.add_argument("--env", default="local")
    args = p.parse_args()
    main(args.process_date, args.env)