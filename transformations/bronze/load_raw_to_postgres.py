"""
TaxiPulse — Load Raw Parquet Data from MinIO into PostgreSQL Bronze Layer
Reads Parquet files from MinIO Bronze bucket and inserts into
bronze.raw_yellow_trips table.
"""

import io
import sys
from pathlib import Path
import pandas as pd
from minio import Minio
from minio.error import S3Error
from sqlalchemy import create_engine, text
from loguru import logger

# Add project root to path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))
from config.settings import MinIOConfig, PostgresConfig


def get_minio_client() -> Minio:
    """Create and return a MinIO client."""
    return Minio(
        endpoint=MinIOConfig.ENDPOINT,
        access_key=MinIOConfig.ACCESS_KEY,
        secret_key=MinIOConfig.SECRET_KEY,
        secure=MinIOConfig.USE_SSL,
    )


def get_pg_engine():
    """Create and return a SQLAlchemy engine for PostgreSQL."""
    return create_engine(PostgresConfig.get_connection_string())


def list_bronze_files(client: Minio) -> list:
    """List all Parquet files in the MinIO Bronze layer."""
    objects = client.list_objects(
        MinIOConfig.BUCKET_NAME,
        prefix=MinIOConfig.BRONZE_PREFIX,
        recursive=True,
    )
    files = [
        obj.object_name
        for obj in objects
        if obj.object_name.endswith(".parquet")
    ]
    logger.info(f"📂 Found {len(files)} Parquet file(s) in Bronze layer")
    return files


def read_parquet_from_minio(client: Minio, object_name: str) -> pd.DataFrame:
    """
    Read a Parquet file from MinIO into a pandas DataFrame.

    Args:
        client: MinIO client
        object_name: Full object path in MinIO

    Returns:
        DataFrame with raw trip data
    """
    logger.info(f"📖 Reading: {object_name}")

    response = client.get_object(MinIOConfig.BUCKET_NAME, object_name)
    data = response.read()
    response.close()
    response.release_conn()

    df = pd.read_parquet(io.BytesIO(data))

    logger.info(f"   Rows: {len(df):,} | Columns: {len(df.columns)}")
    return df


def standardize_columns(df: pd.DataFrame) -> pd.DataFrame:
    """
    Standardize column names to match the bronze table schema.
    NYC TLC data sometimes has inconsistent column naming.
    """
    column_mapping = {
        "VendorID": "vendor_id",
        "tpep_pickup_datetime": "tpep_pickup_datetime",
        "tpep_dropoff_datetime": "tpep_dropoff_datetime",
        "passenger_count": "passenger_count",
        "trip_distance": "trip_distance",
        "RatecodeID": "ratecode_id",
        "store_and_fwd_flag": "store_and_fwd_flag",
        "PULocationID": "pu_location_id",
        "DOLocationID": "do_location_id",
        "payment_type": "payment_type",
        "fare_amount": "fare_amount",
        "extra": "extra",
        "mta_tax": "mta_tax",
        "tip_amount": "tip_amount",
        "tolls_amount": "tolls_amount",
        "improvement_surcharge": "improvement_surcharge",
        "total_amount": "total_amount",
        "congestion_surcharge": "congestion_surcharge",
        "Airport_fee": "airport_fee",
        "airport_fee": "airport_fee",
    }

    # Rename columns that exist in the mapping
    rename_map = {
        col: column_mapping[col]
        for col in df.columns
        if col in column_mapping
    }
    df = df.rename(columns=rename_map)

    # Keep only the columns that match our bronze schema
    expected_cols = [
        "vendor_id", "tpep_pickup_datetime", "tpep_dropoff_datetime",
        "passenger_count", "trip_distance", "ratecode_id",
        "store_and_fwd_flag", "pu_location_id", "do_location_id",
        "payment_type", "fare_amount", "extra", "mta_tax",
        "tip_amount", "tolls_amount", "improvement_surcharge",
        "total_amount", "congestion_surcharge", "airport_fee",
    ]

    # Only keep columns that exist
    cols_to_keep = [c for c in expected_cols if c in df.columns]
    df = df[cols_to_keep]

    return df


def load_to_postgres(
    df: pd.DataFrame,
    source_file: str,
    engine,
    batch_size: int = 50000,
) -> int:
    """
    Load DataFrame into bronze.raw_yellow_trips table.

    Args:
        df: DataFrame to load
        source_file: Name of the source file (for tracking)
        engine: SQLAlchemy engine
        batch_size: Number of rows per insert batch

    Returns:
        Number of rows loaded
    """
    # Add metadata columns
    df["source_file"] = source_file

    table_name = "raw_yellow_trips"
    schema = "bronze"

    total_rows = len(df)
    loaded = 0

    logger.info(
        f"📤 Loading {total_rows:,} rows into {schema}.{table_name}..."
    )

    # Load in batches to avoid memory issues
    for start in range(0, total_rows, batch_size):
        end = min(start + batch_size, total_rows)
        batch = df.iloc[start:end]

        batch.to_sql(
            name=table_name,
            schema=schema,
            con=engine,
            if_exists="append",
            index=False,
            method="multi",
        )

        loaded += len(batch)
        pct = (loaded / total_rows) * 100
        logger.info(f"   Progress: {loaded:,}/{total_rows:,} ({pct:.0f}%)")

    logger.success(f"✅ Loaded {loaded:,} rows from {source_file}")
    return loaded


def check_already_loaded(engine, source_file: str) -> bool:
    """Check if a file has already been loaded into the Bronze table."""
    query = text(
        "SELECT COUNT(*) FROM bronze.raw_yellow_trips "
        "WHERE source_file = :file"
    )
    with engine.connect() as conn:
        result = conn.execute(query, {"file": source_file}).scalar()
    return result > 0


def load_bronze_layer() -> dict:
    """
    Main function: Load all Bronze Parquet files from MinIO into PostgreSQL.

    Returns:
        Dict with results: {"loaded": [...], "skipped": [...], "failed": [...]}
    """
    client = get_minio_client()
    engine = get_pg_engine()

    bronze_files = list_bronze_files(client)

    if not bronze_files:
        logger.warning("⚠️  No files found in Bronze layer")
        return {"loaded": [], "skipped": [], "failed": []}

    results = {"loaded": [], "skipped": [], "failed": []}
    total_rows = 0

    for object_name in bronze_files:
        filename = object_name.split("/")[-1]

        # Skip if already loaded
        if check_already_loaded(engine, filename):
            logger.info(f"⏭️  Already loaded: {filename}")
            results["skipped"].append(filename)
            continue

        try:
            # Read from MinIO
            df = read_parquet_from_minio(client, object_name)

            # Standardize columns
            df = standardize_columns(df)

            # Load into PostgreSQL
            rows = load_to_postgres(df, filename, engine)
            total_rows += rows
            results["loaded"].append(filename)

        except Exception as e:
            logger.error(f"❌ Failed to load {filename}: {e}")
            results["failed"].append(filename)

    # Summary
    logger.info("")
    logger.info("📊 Bronze Loading Summary:")
    logger.info(f"   ✅ Loaded:  {len(results['loaded'])} file(s)")
    logger.info(f"   ⏭️  Skipped: {len(results['skipped'])} file(s)")
    logger.info(f"   ❌ Failed:  {len(results['failed'])} file(s)")
    logger.info(f"   📈 Total rows loaded: {total_rows:,}")

    # Verify row count in table
    with engine.connect() as conn:
        count = conn.execute(
            text("SELECT COUNT(*) FROM bronze.raw_yellow_trips")
        ).scalar()
        logger.info(f"   📋 Total rows in bronze table: {count:,}")

    return results


if __name__ == "__main__":
    logger.info("🚕 TaxiPulse — Bronze Layer Loader (MinIO → PostgreSQL)")
    logger.info("")
    load_bronze_layer()