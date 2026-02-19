"""
TaxiPulse — Run Full Batch Ingestion Pipeline
Step 1: Download NYC TLC data from public URL
Step 2: Upload Parquet files to MinIO Bronze layer
"""

import sys
from pathlib import Path
from loguru import logger

# Add project root to path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from ingestion.batch.download_tlc_data import download_tlc_data
from ingestion.batch.upload_to_minio import upload_all_to_bronze, list_bronze_objects


def run_ingestion():
    """Execute the full batch ingestion pipeline."""
    logger.info("=" * 60)
    logger.info("🚕 TaxiPulse — Batch Ingestion Pipeline")
    logger.info("=" * 60)

    # Step 1: Download
    logger.info("")
    logger.info("📥 STEP 1: Downloading NYC TLC Data...")
    logger.info("-" * 40)
    downloaded_files = download_tlc_data()

    if not downloaded_files:
        logger.error("❌ No files downloaded. Aborting pipeline.")
        return False

    # Step 2: Upload to MinIO
    logger.info("")
    logger.info("☁️  STEP 2: Uploading to MinIO Bronze Layer...")
    logger.info("-" * 40)
    results = upload_all_to_bronze()

    # Step 3: Verify
    logger.info("")
    logger.info("🔍 STEP 3: Verifying Bronze Layer...")
    logger.info("-" * 40)
    list_bronze_objects()

    # Final summary
    logger.info("")
    logger.info("=" * 60)
    if results["failed"]:
        logger.warning(
            f"⚠️  Pipeline completed with {len(results['failed'])} failures"
        )
        return False
    else:
        logger.success(
            f"✅ Pipeline completed! "
            f"{len(results['uploaded'])} file(s) ingested to Bronze layer"
        )
        return True


if __name__ == "__main__":
    success = run_ingestion()
    sys.exit(0 if success else 1)