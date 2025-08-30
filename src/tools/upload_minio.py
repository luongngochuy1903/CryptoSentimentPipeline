import argparse
import os
import sys
from pathlib import Path
from typing import Optional

try:
    # Prefer using repo config if available
    from utils.constants import MINIO_ACCESS_KEY, MINIO_SECRET_KEY  # type: ignore
except Exception:
    MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "")
    MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "")


def _make_s3_client(endpoint_url: str, access_key: str, secret_key: str):
    import boto3

    # Create a boto3 S3 client configured for MinIO
    session = boto3.session.Session()
    s3 = session.client(
        service_name="s3",
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
        endpoint_url=endpoint_url,
    )
    return s3


def _ensure_bucket(s3, bucket: str):
    import botocore

    try:
        s3.head_bucket(Bucket=bucket)
    except botocore.exceptions.ClientError as e:
        error_code = int(e.response.get("Error", {}).get("Code", 0))
        # If bucket doesn't exist, create it
        if error_code in (404, 400):
            s3.create_bucket(Bucket=bucket)
        else:
            raise


def _upload_file(s3, bucket: str, local_path: Path, key: str):
    # Ensure POSIX key separators for S3
    key = key.replace("\\", "/")
    # Use multipart uploads automatically via upload_file
    s3.upload_file(str(local_path), bucket, key)


def upload_directory(
    directory: Path,
    bucket: str,
    prefix: Optional[str],
    endpoint_url: str,
    access_key: str,
    secret_key: str,
):
    if not directory.exists() or not directory.is_dir():
        raise FileNotFoundError(f"Directory not found: {directory}")

    if not access_key or not secret_key:
        raise RuntimeError(
            "MinIO credentials are missing. Set utils.constants or MINIO_ACCESS_KEY/MINIO_SECRET_KEY env vars."
        )

    s3 = _make_s3_client(endpoint_url, access_key, secret_key)
    _ensure_bucket(s3, bucket)

    base = directory.resolve()
    for root, _, files in os.walk(base):
        for fname in files:
            local_path = Path(root) / fname
            rel = local_path.relative_to(base)
            key_parts = [p for p in [prefix, str(rel)] if p]
            key = "/".join(key_parts) if key_parts else str(rel)
            print(f"Uploading {local_path} -> s3://{bucket}/{key}")
            _upload_file(s3, bucket, local_path, key)


def parse_args(argv=None):
    parser = argparse.ArgumentParser(
        description="Upload a local directory (e.g., saved model) to a MinIO bucket"
    )
    parser.add_argument(
        "--dir",
        dest="directory",
        required=True,
        help="Path to the local directory to upload (e.g., models/sentiment_distilbert_fixed)",
    )
    parser.add_argument(
        "--bucket",
        dest="bucket",
        default="models",
        help="Target MinIO bucket name (default: models)",
    )
    parser.add_argument(
        "--prefix",
        dest="prefix",
        default=None,
        help="Optional prefix inside the bucket (e.g., sentiment_distilbert_fixed)",
    )
    parser.add_argument(
        "--endpoint",
        dest="endpoint",
        default=os.getenv("MINIO_ENDPOINT", "http://localhost:9000"),
        help="MinIO endpoint URL (default: http://localhost:9000; use http://minio:9000 inside Docker)",
    )
    parser.add_argument(
        "--access-key",
        dest="access_key",
        default=os.getenv("MINIO_ACCESS_KEY", MINIO_ACCESS_KEY),
        help="MinIO access key (defaults to env MINIO_ACCESS_KEY or utils.constants)",
    )
    parser.add_argument(
        "--secret-key",
        dest="secret_key",
        default=os.getenv("MINIO_SECRET_KEY", MINIO_SECRET_KEY),
        help="MinIO secret key (defaults to env MINIO_SECRET_KEY or utils.constants)",
    )
    return parser.parse_args(argv)


def main(argv=None):
    args = parse_args(argv)
    directory = Path(args.directory)
    # If no prefix provided, default to directory name
    prefix = args.prefix or directory.name

    upload_directory(
        directory=directory,
        bucket=args.bucket,
        prefix=prefix,
        endpoint_url=args.endpoint,
        access_key=args.access_key,
        secret_key=args.secret_key,
    )


if __name__ == "__main__":
    sys.exit(main())
