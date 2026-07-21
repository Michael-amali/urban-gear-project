import os
from pathlib import Path

VALID_ENVS = ("local", "local-s3", "prod")


class PipelineConfig:
    """Storage backend is selected by ENV:

    - local     → local filesystem under LOCAL_DATA_PATH (./data)
    - local-s3  → MinIO Play (S3-compatible) via S3_ENDPOINT_URL
    - prod      → AWS S3 (no custom endpoint)
    """

    BASE_DIR = Path(__file__).resolve().parents[1]

    # Populated by reload()
    ENV = "local"
    AWS_REGION = "us-east-1"
    S3_BUCKET = "urbangear-data-lake-myk"
    S3_ENDPOINT_URL = ""
    AWS_ACCESS_KEY_ID = ""
    AWS_SECRET_ACCESS_KEY = ""
    LOCAL_DATA_PATH = BASE_DIR / "data"
    S3_RAW = ""
    S3_PROCESSED = ""
    S3_REJECTED = ""
    S3_METRICS = ""
    SHUFFLE_PARTITIONS = 10
    PG_HOST = "localhost"
    PG_PORT = 5433
    PG_DB = ""
    PG_USER = ""
    PG_PASSWORD = ""

    # Thresholds (static)
    MAX_NULL_RATE = 0.02
    MAX_DUPLICATE_RATE = 0.001
    MIN_RECORD_COUNT = 1000
    REJECT_RATE_THRESHOLD = 0.05
    VALID_PLATFORMS = ["website", "mobile_app", "marketplace"]
    MAX_ORDER_VALUE = 50000.0

    @classmethod
    def use_local_fs(cls) -> bool:
        return cls.ENV == "local"

    @classmethod
    def use_object_storage(cls) -> bool:
        return cls.ENV in ("local-s3", "prod")

    @classmethod
    def use_minio(cls) -> bool:
        return cls.ENV == "local-s3"

    @classmethod
    def reload(cls, env: str = None):
        """Refresh config from environment. Call after setting ENV / --env."""
        if env:
            os.environ["ENV"] = env

        cls.ENV = os.getenv("ENV", "local").strip().lower()
        if cls.ENV not in VALID_ENVS:
            raise ValueError(
                f"Invalid ENV={cls.ENV!r}. Expected one of: {', '.join(VALID_ENVS)}"
            )

        cls.AWS_REGION = os.getenv("AWS_REGION", "us-east-1")
        cls.AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID", "")
        cls.AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY", "")
        cls.LOCAL_DATA_PATH = Path(
            os.getenv("LOCAL_DATA_PATH", str(cls.BASE_DIR / "data"))
        )

        if cls.ENV == "local":
            cls.S3_BUCKET = os.getenv("S3_BUCKET", "urbangear-data-lake-myk")
            cls.S3_ENDPOINT_URL = ""
            cls.S3_RAW = str(cls.LOCAL_DATA_PATH / "raw")
            cls.S3_PROCESSED = str(cls.LOCAL_DATA_PATH / "processed")
            cls.S3_REJECTED = str(cls.LOCAL_DATA_PATH / "rejected")
            cls.S3_METRICS = str(cls.LOCAL_DATA_PATH / "metrics")
            cls.SHUFFLE_PARTITIONS = 10
        elif cls.ENV == "local-s3":
            cls.S3_BUCKET = os.getenv("S3_BUCKET", "urbangear-data-lake-myk")
            cls.S3_ENDPOINT_URL = os.getenv("S3_ENDPOINT_URL", "https://play.min.io")
            cls.S3_RAW = f"s3a://{cls.S3_BUCKET}/raw"
            cls.S3_PROCESSED = f"s3a://{cls.S3_BUCKET}/processed"
            cls.S3_REJECTED = f"s3a://{cls.S3_BUCKET}/rejected"
            cls.S3_METRICS = f"s3a://{cls.S3_BUCKET}/metrics"
            cls.SHUFFLE_PARTITIONS = 10
        else:  # prod
            cls.S3_BUCKET = os.getenv("S3_BUCKET", "urbangear-data-lake")
            # Always AWS S3 — ignore MinIO endpoint leftover in .env
            cls.S3_ENDPOINT_URL = ""
            cls.S3_RAW = f"s3a://{cls.S3_BUCKET}/raw"
            cls.S3_PROCESSED = f"s3a://{cls.S3_BUCKET}/processed"
            cls.S3_REJECTED = f"s3a://{cls.S3_BUCKET}/rejected"
            cls.S3_METRICS = f"s3a://{cls.S3_BUCKET}/metrics"
            cls.SHUFFLE_PARTITIONS = 200

        cls.PG_HOST = os.getenv("POSTGRES_HOST", "localhost")
        default_port = "5433" if cls.ENV in ("local", "local-s3") else "5432"
        cls.PG_PORT = int(os.getenv("POSTGRES_PORT", default_port))
        cls.PG_DB = os.getenv("POSTGRES_DB", "urbangear_dw")
        cls.PG_USER = os.getenv("POSTGRES_USER", "urbangear")
        cls.PG_PASSWORD = os.getenv("POSTGRES_PASSWORD", "urbangear123")
        return cls


# Initialize from current process env on import
PipelineConfig.reload()
