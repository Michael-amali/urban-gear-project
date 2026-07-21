"""Shared boto3 S3 client for MinIO Play (local-s3) and AWS (prod)."""
from typing import Tuple

import boto3
from botocore.client import Config
from config.pipeline_config import PipelineConfig


def get_s3_client():
    """Return an S3 client for MinIO (local-s3) or AWS (prod)."""

    kwargs = {
        "service_name": "s3",
        "region_name": PipelineConfig.AWS_REGION,
    }
    if PipelineConfig.AWS_ACCESS_KEY_ID:
        kwargs["aws_access_key_id"] = PipelineConfig.AWS_ACCESS_KEY_ID
    if PipelineConfig.AWS_SECRET_ACCESS_KEY:
        kwargs["aws_secret_access_key"] = PipelineConfig.AWS_SECRET_ACCESS_KEY
    # MinIO / S3-compatible endpoint (path-style required)
    if PipelineConfig.S3_ENDPOINT_URL:
        kwargs["endpoint_url"] = PipelineConfig.S3_ENDPOINT_URL
        kwargs["config"] = Config(s3={"addressing_style": "path"})
        
    return boto3.client(**kwargs)


def s3a_to_key(s3a_uri: str) -> Tuple[str, str]:
    """Parse s3a://bucket/key or s3://bucket/key into (bucket, key)."""

    path = s3a_uri.replace("s3a://", "").replace("s3://", "")
    bucket, _, key = path.partition("/")
    return bucket, key
