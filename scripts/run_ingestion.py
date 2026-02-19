"""
TaxiPulse — Run Full Batch Ingestion Pipeline
Step 1: Download NYC TLC data from public URL
Step 2: Upload Parquet files to MinIO Bronze layer
Step 3: Load Bronze data into PostgreSQL
"""

import sys
from pathlib import Path
from loguru import logger

# Add project root to path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from ingestion.batch.download_tlc_data import download_tlc_data
from ingestion.batch.upload_to_minio import upload_all_to_bronze, list_bronze_objects
from transformations.bronze.load_raw_to_postgres import load_bronze_layer


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
    upload_results = upload_all_to_bronze()

    # Step 3: Load into PostgreSQL
    logger.info("")
    logger.info("📤 STEP 3: Loading Bronze Data into PostgreSQL...")
    logger.info("-" * 40)
    load_results = load_bronze_layer()

    # Step 4: Verify
    logger.info("")
    logger.info("🔍 STEP 4: Verifying Bronze Layer...")
    logger.info("-" * 40)
    list_bronze_objects()

    # Final summary
    logger.info("")
    logger.info("=" * 60)
    all_failed = upload_results["failed"] + load_results["failed"]
    if all_failed:
        logger.warning(
            f"⚠️  Pipeline completed with {len(all_failed)} failure(s)"
        )
        return False
    else:
        logger.success("✅ Full ingestion pipeline completed successfully!")
        logger.info(
            f"   Files uploaded to MinIO: {len(upload_results['uploaded'])}"
        )
        logger.info(
            f"   Files loaded to PostgreSQL: {len(load_results['loaded'])}"
        )
        return True


if __name__ == "__main__":
    success = run_ingestion()
    sys.exit(0 if success else 1)