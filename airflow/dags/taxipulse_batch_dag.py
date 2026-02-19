"""
TaxiPulse — Airflow Batch Pipeline DAG
Orchestrates the full batch data pipeline:
  1. Download NYC TLC data
  2. Upload to MinIO (Bronze)
  3. Load Bronze → PostgreSQL
  (Steps 4-6 will be added in later steps)
"""

import sys
from datetime import datetime, timedelta
from pathlib import Path

from airflow import DAG
from airflow.operators.python import PythonOperator

# Add paths so Airflow can find our modules
sys.path.insert(0, "/opt/airflow")

# ============================================================
# Task Functions
# ============================================================

def task_download_tlc_data(**context):
    """Task 1: Download NYC TLC Parquet files."""
    from ingestion.batch.download_tlc_data import download_tlc_data
    from loguru import logger

    logger.info("📥 Starting TLC data download...")
    files = download_tlc_data()

    # Push file paths to XCom so downstream tasks can use them
    file_names = [str(f) for f in files]
    context["ti"].xcom_push(key="downloaded_files", value=file_names)

    logger.info(f"✅ Downloaded {len(files)} file(s)")
    return len(files)


def task_upload_to_minio(**context):
    """Task 2: Upload downloaded files to MinIO Bronze layer."""
    from ingestion.batch.upload_to_minio import upload_all_to_bronze
    from loguru import logger

    logger.info("☁️ Uploading to MinIO Bronze layer...")
    results = upload_all_to_bronze()

    context["ti"].xcom_push(key="upload_results", value=results)

    uploaded_count = len(results["uploaded"])
    failed_count = len(results["failed"])
    logger.info(f"✅ Uploaded: {uploaded_count}, Failed: {failed_count}")

    if failed_count > 0:
        raise Exception(f"{failed_count} file(s) failed to upload")

    return uploaded_count


def task_load_bronze_to_postgres(**context):
    """Task 3: Load Parquet data from MinIO into PostgreSQL Bronze table."""
    from transformations.bronze.load_raw_to_postgres import load_bronze_layer
    from loguru import logger

    logger.info("📤 Loading Bronze data into PostgreSQL...")
    results = load_bronze_layer()

    context["ti"].xcom_push(key="bronze_load_results", value=results)

    if results["failed"]:
        raise Exception(
            f"Failed to load: {', '.join(results['failed'])}"
        )

    logger.info(
        f"✅ Loaded {len(results['loaded'])} file(s), "
        f"skipped {len(results['skipped'])}"
    )
    return len(results["loaded"])


def task_validate_data_quality(**context):
    """Task 4: Run data quality validation on Bronze data."""
    from quality.validate_data import validate_and_split_bronze
    from loguru import logger

    logger.info("🔍 Running data quality validation...")
    result = validate_and_split_bronze()

    if result is None:
        raise Exception("No data to validate")

    summary = result["summary"]
    context["ti"].xcom_push(key="quality_summary", value=summary)

    logger.info(
        f"✅ Quality check complete: "
        f"{summary['clean_rows']:,} clean, "
        f"{summary['quarantined_rows']:,} quarantined, "
        f"{summary['overall_pass_rate']}% pass rate"
    )

    # Fail the task if pass rate is below 80%
    if summary["overall_pass_rate"] < 80:
        raise Exception(
            f"Data quality below threshold: {summary['overall_pass_rate']}%"
        )

    return summary["overall_pass_rate"]


def task_transform_to_silver(**context):
    """Task 5: Transform validated data to Silver layer."""
    from transformations.silver.clean_and_validate import run_silver_pipeline
    from loguru import logger

    logger.info("🔧 Running Silver transformation...")
    result = run_silver_pipeline()

    if result["status"] == "success":
        logger.info(
            f"✅ Silver complete: {result['silver_rows']:,} clean, "
            f"{result['quarantine_rows']:,} quarantined"
        )
    elif result["status"] == "skipped":
        logger.info(f"⏭️ Silver already loaded: {result['existing_rows']:,} rows")
    else:
        raise Exception("Silver transformation failed — no data")

    return result["status"]


def task_build_gold_layer(**context):
    """Task 6: Build Gold star schema and aggregations."""
    from transformations.gold.build_star_schema import run_gold_pipeline
    from loguru import logger

    logger.info("🏆 Building Gold layer...")
    results = run_gold_pipeline()

    context["ti"].xcom_push(key="gold_results", value=results)
    logger.info(f"✅ Gold layer complete: {sum(results.values()):,} total rows")
    return results


# ============================================================
# DAG Definition
# ============================================================

default_args = {
    "owner": "taxipulse",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="taxipulse_batch_pipeline",
    default_args=default_args,
    description="TaxiPulse end-to-end batch data pipeline",
    schedule_interval="@monthly",  # Run once per month
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["taxipulse", "batch", "etl"],
) as dag:

    # Task 1: Download NYC TLC data
    download = PythonOperator(
        task_id="download_tlc_data",
        python_callable=task_download_tlc_data,
        provide_context=True,
    )

    # Task 2: Upload to MinIO Bronze layer
    upload = PythonOperator(
        task_id="upload_to_minio_bronze",
        python_callable=task_upload_to_minio,
        provide_context=True,
    )

    # Task 3: Load Bronze into PostgreSQL
    load_bronze = PythonOperator(
        task_id="load_bronze_to_postgres",
        python_callable=task_load_bronze_to_postgres,
        provide_context=True,
    )

    # Task 4: Data Quality Validation
    validate_quality = PythonOperator(
        task_id="validate_data_quality",
        python_callable=task_validate_data_quality,
        provide_context=True,
    )

    # Task 5: Silver Layer Transformation
    transform_silver = PythonOperator(
        task_id="transform_to_silver",
        python_callable=task_transform_to_silver,
        provide_context=True,
    )

    # Task 6: Gold Layer (Star Schema + Aggregations)
    build_gold = PythonOperator(
        task_id="build_gold_layer",
        python_callable=task_build_gold_layer,
        provide_context=True,
    )

    # ============================================================
    # Pipeline Flow
    # ============================================================
    # Download → Upload → Load Bronze → Validate → Silver → Gold
    #
    # Future steps will add:
    #   → Anomaly Detection
    #   → Alerting

    download >> upload >> load_bronze >> validate_quality >> transform_silver >> build_gold