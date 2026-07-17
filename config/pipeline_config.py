import os
from pathlib import Path

class PipelineConfig:
    ENV = os.getenv("ENV", "local")
    AWS_REGION = os.getenv("AWS_REGION", "us-east-1")
    S3_BUCKET = os.getenv("S3_BUCKET", "urbangear-data-lake")

    # Local paths
    BASE_DIR = Path(__file__).resolve().parents[1]
    LOCAL_DATA_PATH = Path(os.getenv("LOCAL_DATA_PATH", str(BASE_DIR / "data")))
    S3_RAW = str(LOCAL_DATA_PATH / "raw") if ENV == "local" else f"s3://{S3_BUCKET}/raw"
    S3_PROCESSED = str(LOCAL_DATA_PATH / "processed") if ENV == "local" else f"s3://{S3_BUCKET}/processed"
    S3_REJECTED = str(LOCAL_DATA_PATH / "rejected") if ENV == "local" else f"s3://{S3_BUCKET}/rejected"
    S3_METRICS = str(LOCAL_DATA_PATH / "metrics") if ENV == "local" else f"s3://{S3_BUCKET}/metrics"

    # Thresholds
    MAX_NULL_RATE = 0.02
    MAX_DUPLICATE_RATE = 0.001
    MIN_RECORD_COUNT = 1000  # lower for local testing, set 100000 for prod
    REJECT_RATE_THRESHOLD = 0.05

    VALID_PLATFORMS = ["website", "mobile_app", "marketplace"]
    MAX_ORDER_VALUE = 50000.0
    SHUFFLE_PARTITIONS = 10 if ENV == "local" else 200

    # Postgres warehouse (mock Redshift)
    PG_HOST = os.getenv("POSTGRES_HOST", "localhost")
    PG_PORT = int(os.getenv("POSTGRES_PORT", "5433" if ENV == "local" and "HOST" not in os.environ else "5432"))
    PG_DB = os.getenv("POSTGRES_DB", "urbangear_dw")
    PG_USER = os.getenv("POSTGRES_USER", "urbangear")
    PG_PASSWORD = os.getenv("POSTGRES_PASSWORD", "urbangear123")