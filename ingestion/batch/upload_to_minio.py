"""
TaxiPulse — Upload Parquet Files to MinIO (Bronze Layer)
Uploads downloaded NYC TLC data to the MinIO data lake.
"""

import sys
from pathlib import Path
from minio import Minio
from minio.error import S3Error
from loguru import logger

# Add project root to path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))
from config.settings import MinIOConfig


# Local directory where downloaded files are stored
DOWNLOAD_DIR = Path(__file__).resolve().parent.parent.parent / "data" / "raw"


def get_minio_client() -> Minio:
    """Create and return a MinIO client."""
    client = Minio(
        endpoint=MinIOConfig.ENDPOINT,
        access_key=MinIOConfig.ACCESS_KEY,
        secret_key=MinIOConfig.SECRET_KEY,
        secure=MinIOConfig.USE_SSL,
    )
    logger.info(f"🔗 Connected to MinIO at {MinIOConfig.ENDPOINT}")
    return client


def ensure_bucket_exists(client: Minio, bucket_name: str) -> None:
    """Create the bucket if it doesn't exist."""
    if not client.bucket_exists(bucket_name):
        client.make_bucket(bucket_name)
        logger.info(f"🪣 Created bucket: {bucket_name}")
    else:
        logger.info(f"🪣 Bucket exists: {bucket_name}")


def upload_file_to_minio(
    client: Minio,
    local_path: Path,
    bucket_name: str,
    object_prefix: str,
) -> bool:
    """
    Upload a single file to MinIO.

    Args:
        client: MinIO client
        local_path: Path to local file
        bucket_name: Target bucket name
        object_prefix: Prefix path in bucket (e.g., "bronze/")

    Returns:
        True if upload succeeded
    """
    object_name = f"{object_prefix}{local_path.name}"

    try:
        # Check if file already exists in MinIO
        try:
            client.stat_object(bucket_name, object_name)
            size_mb = local_path.stat().st_size / (1024 * 1024)
            logger.info(
                f"⏭️  Already in MinIO: {object_name} ({size_mb:.1f} MB)"
            )
            return True
        except S3Error:
            pass  # Object doesn't exist, proceed with upload

        file_size = local_path.stat().st_size
        size_mb = file_size / (1024 * 1024)

        logger.info(f"⬆️  Uploading: {local_path.name} → {object_name}")

        client.fput_object(
            bucket_name=bucket_name,
            object_name=object_name,
            file_path=str(local_path),
            content_type="application/octet-stream",
        )

        logger.success(
            f"✅ Uploaded: {object_name} ({size_mb:.1f} MB)"
        )
        return True

    except S3Error as e:
        logger.error(f"❌ MinIO error uploading {local_path.name}: {e}")
        return False
    except Exception as e:
        logger.error(f"❌ Unexpected error: {e}")
        return False


def upload_all_to_bronze(
    local_dir: Path = None,
) -> dict:
    """
    Upload all Parquet files from local directory to MinIO Bronze layer.

    Args:
        local_dir: Directory containing Parquet files

    Returns:
        Dict with upload results: {"uploaded": [...], "failed": [...]}
    """
    local_dir = local_dir or DOWNLOAD_DIR
    bucket_name = MinIOConfig.BUCKET_NAME
    prefix = MinIOConfig.BRONZE_PREFIX

    # Get all parquet files
    parquet_files = sorted(local_dir.glob("*.parquet"))

    if not parquet_files:
        logger.warning(f"⚠️  No Parquet files found in {local_dir}")
        return {"uploaded": [], "failed": []}

    logger.info(f"📦 Found {len(parquet_files)} Parquet file(s) to upload")

    # Connect to MinIO
    client = get_minio_client()
    ensure_bucket_exists(client, bucket_name)

    results = {"uploaded": [], "failed": []}

    for file_path in parquet_files:
        if upload_file_to_minio(client, file_path, bucket_name, prefix):
            results["uploaded"].append(file_path.name)
        else:
            results["failed"].append(file_path.name)

    # Summary
    logger.info("")
    logger.info("📊 Upload Summary:")
    logger.info(f"   ✅ Uploaded: {len(results['uploaded'])}")
    logger.info(f"   ❌ Failed:   {len(results['failed'])}")

    return results


def list_bronze_objects() -> list:
    """List all objects in the Bronze layer."""
    client = get_minio_client()
    bucket_name = MinIOConfig.BUCKET_NAME
    prefix = MinIOConfig.BRONZE_PREFIX

    objects = list(client.list_objects(bucket_name, prefix=prefix))

    logger.info(f"📂 Bronze layer contents ({len(objects)} objects):")
    for obj in objects:
        size_mb = obj.size / (1024 * 1024) if obj.size else 0
        logger.info(f"   {obj.object_name} — {size_mb:.1f} MB")

    return objects


if __name__ == "__main__":
    logger.info("🚕 TaxiPulse — MinIO Bronze Layer Uploader")
    logger.info("")

    results = upload_all_to_bronze()

    if results["uploaded"]:
        logger.info("")
        logger.info("🔍 Verifying Bronze layer contents:")
        list_bronze_objects()