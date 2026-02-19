"""
TaxiPulse — Silver Layer Transformation
Takes validated Bronze data, cleans and enriches it,
and loads into silver.clean_yellow_trips.
Quarantined records go to silver.quarantined_yellow_trips.
"""

import sys
from pathlib import Path
import pandas as pd
import numpy as np
from sqlalchemy import create_engine, text
from loguru import logger

sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))
from config.settings import PostgresConfig
from quality.validate_data import validate_bronze_data


def get_pg_engine():
    """Create and return a SQLAlchemy engine."""
    return create_engine(PostgresConfig.get_connection_string())


# ============================================================
# Cleaning & Enrichment Functions
# ============================================================

def standardize_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Rename columns to Silver layer standard names."""
    rename_map = {
        "tpep_pickup_datetime": "pickup_datetime",
        "tpep_dropoff_datetime": "dropoff_datetime",
        "ratecode_id": "rate_code_id",
        "pu_location_id": "pickup_location_id",
        "do_location_id": "dropoff_location_id",
        "payment_type": "payment_type_id",
    }
    df = df.rename(columns=rename_map)
    return df


def enforce_types(df: pd.DataFrame) -> pd.DataFrame:
    """Cast columns to proper data types."""
    df["pickup_datetime"] = pd.to_datetime(df["pickup_datetime"], errors="coerce")
    df["dropoff_datetime"] = pd.to_datetime(df["dropoff_datetime"], errors="coerce")

    int_cols = [
        "vendor_id", "passenger_count", "rate_code_id",
        "pickup_location_id", "dropoff_location_id", "payment_type_id",
    ]
    for col in int_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0).astype(int)

    float_cols = [
        "trip_distance", "fare_amount", "extra", "mta_tax",
        "tip_amount", "tolls_amount", "improvement_surcharge",
        "total_amount", "congestion_surcharge", "airport_fee",
    ]
    for col in float_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0.0)

    return df


def add_derived_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Add computed/derived columns for analytics."""
    # Trip duration in minutes
    df["trip_duration_minutes"] = (
        (df["dropoff_datetime"] - df["pickup_datetime"])
        .dt.total_seconds() / 60
    ).round(2)

    # Cap unreasonable durations (negative or > 24 hours)
    df["trip_duration_minutes"] = df["trip_duration_minutes"].clip(lower=0, upper=1440)

    return df


def remove_duplicates(df: pd.DataFrame) -> pd.DataFrame:
    """Remove exact duplicate rows."""
    before = len(df)
    df = df.drop_duplicates(
        subset=[
            "vendor_id", "pickup_datetime", "dropoff_datetime",
            "pickup_location_id", "dropoff_location_id",
            "fare_amount", "total_amount",
        ],
        keep="first",
    )
    after = len(df)
    removed = before - after
    if removed > 0:
        logger.info(f"   🔄 Removed {removed:,} duplicate rows")
    else:
        logger.info(f"   🔄 No duplicates found")
    return df


# ============================================================
# Main Silver Transformation
# ============================================================

def transform_to_silver(df: pd.DataFrame, source_file: str = "all") -> dict:
    """
    Apply all Silver layer transformations to a clean DataFrame.

    Args:
        df: Clean DataFrame (already passed quality validation)
        source_file: Source filename for metadata

    Returns:
        dict with silver_df and row counts
    """
    initial_count = len(df)
    logger.info(f"🔧 Transforming {initial_count:,} rows to Silver...")

    # Step 1: Standardize column names
    logger.info("   📝 Standardizing column names...")
    df = standardize_columns(df)

    # Step 2: Enforce data types
    logger.info("   🔢 Enforcing data types...")
    df = enforce_types(df)

    # Step 3: Add derived columns
    logger.info("   ➕ Adding derived columns...")
    df = add_derived_columns(df)

    # Step 4: Remove duplicates
    logger.info("   🔍 Checking for duplicates...")
    df = remove_duplicates(df)

    # Step 5: Add source metadata
    df["source_file"] = source_file

    # Step 6: Drop Bronze-only columns
    cols_to_drop = ["load_timestamp"]
    df = df.drop(columns=[c for c in cols_to_drop if c in df.columns])

    final_count = len(df)
    logger.success(
        f"✅ Silver transformation complete: "
        f"{initial_count:,} → {final_count:,} rows"
    )

    return {
        "silver_df": df,
        "initial_count": initial_count,
        "final_count": final_count,
        "removed": initial_count - final_count,
    }


# ============================================================
# Load to PostgreSQL Silver Layer
# ============================================================

def load_silver_to_postgres(
    silver_df: pd.DataFrame,
    engine,
    batch_size: int = 50000,
) -> int:
    """Load Silver DataFrame into PostgreSQL silver.clean_yellow_trips."""
    total = len(silver_df)
    loaded = 0

    # Select only columns that match the Silver table
    silver_cols = [
        "vendor_id", "pickup_datetime", "dropoff_datetime",
        "passenger_count", "trip_distance", "rate_code_id",
        "store_and_fwd_flag", "pickup_location_id", "dropoff_location_id",
        "payment_type_id", "fare_amount", "extra", "mta_tax",
        "tip_amount", "tolls_amount", "improvement_surcharge",
        "total_amount", "congestion_surcharge", "airport_fee",
        "trip_duration_minutes", "source_file",
    ]
    cols_to_use = [c for c in silver_cols if c in silver_df.columns]
    df = silver_df[cols_to_use]

    logger.info(f"📤 Loading {total:,} rows into silver.clean_yellow_trips...")

    for start in range(0, total, batch_size):
        end = min(start + batch_size, total)
        batch = df.iloc[start:end]

        batch.to_sql(
            name="clean_yellow_trips",
            schema="silver",
            con=engine,
            if_exists="append",
            index=False,
            method="multi",
        )

        loaded += len(batch)
        pct = (loaded / total) * 100
        logger.info(f"   Progress: {loaded:,}/{total:,} ({pct:.0f}%)")

    logger.success(f"✅ Loaded {loaded:,} rows into Silver layer")
    return loaded


def load_quarantine_to_postgres(
    quarantine_df: pd.DataFrame,
    engine,
) -> int:
    """Load quarantined records into silver.quarantined_yellow_trips."""
    if quarantine_df.empty:
        logger.info("   No quarantined records to load")
        return 0

    # Standardize column names for quarantine table
    quarantine_df = standardize_columns(quarantine_df.copy())

    quarantine_cols = [
        "vendor_id", "pickup_datetime", "dropoff_datetime",
        "passenger_count", "trip_distance", "rate_code_id",
        "pickup_location_id", "dropoff_location_id",
        "payment_type_id", "fare_amount", "total_amount",
        "quarantine_reason", "source_file",
    ]
    cols_to_use = [c for c in quarantine_cols if c in quarantine_df.columns]
    df = quarantine_df[cols_to_use]

    total = len(df)
    logger.info(
        f"🚫 Loading {total:,} quarantined rows into "
        f"silver.quarantined_yellow_trips..."
    )

    df.to_sql(
        name="quarantined_yellow_trips",
        schema="silver",
        con=engine,
        if_exists="append",
        index=False,
        method="multi",
    )

    logger.success(f"✅ Loaded {total:,} quarantined records")
    return total


# ============================================================
# Check if Silver Already Loaded
# ============================================================

def is_silver_loaded(engine) -> bool:
    """Check if Silver table already has data."""
    with engine.connect() as conn:
        count = conn.execute(
            text("SELECT COUNT(*) FROM silver.clean_yellow_trips")
        ).scalar()
    return count > 0


# ============================================================
# Full Silver Pipeline
# ============================================================

def run_silver_pipeline() -> dict:
    """
    Full Silver pipeline:
      1. Read Bronze data
      2. Validate (quality checks)
      3. Transform clean rows to Silver format
      4. Load Silver into PostgreSQL
      5. Load quarantined into PostgreSQL
    """
    engine = get_pg_engine()

    # Check if already loaded
    if is_silver_loaded(engine):
        with engine.connect() as conn:
            count = conn.execute(
                text("SELECT COUNT(*) FROM silver.clean_yellow_trips")
            ).scalar()
        logger.info(f"⏭️  Silver layer already has {count:,} rows. Skipping.")
        logger.info("   To reload, truncate the table first:")
        logger.info("   TRUNCATE silver.clean_yellow_trips;")
        return {"status": "skipped", "existing_rows": count}

    # Step 1: Read Bronze data
    logger.info("📖 Reading Bronze data from PostgreSQL...")
    df = pd.read_sql(
        "SELECT * FROM bronze.raw_yellow_trips",
        con=engine,
    )
    logger.info(f"   Read {len(df):,} rows from Bronze")

    if df.empty:
        logger.warning("⚠️  No data in Bronze table!")
        return {"status": "empty"}

    # Step 2: Validate
    logger.info("")
    logger.info("🔍 Running quality validation...")
    validation = validate_bronze_data(df)
    clean_df = validation["clean_df"]
    quarantine_df = validation["quarantine_df"]

    # Step 3: Transform clean rows
    logger.info("")
    logger.info("🔧 Transforming to Silver format...")
    result = transform_to_silver(clean_df)
    silver_df = result["silver_df"]

    # Step 4: Load Silver to PostgreSQL
    logger.info("")
    silver_rows = load_silver_to_postgres(silver_df, engine)

    # Step 5: Load quarantined records
    logger.info("")
    quarantine_rows = load_quarantine_to_postgres(quarantine_df, engine)

    # Final verification
    logger.info("")
    with engine.connect() as conn:
        silver_count = conn.execute(
            text("SELECT COUNT(*) FROM silver.clean_yellow_trips")
        ).scalar()
        quarantine_count = conn.execute(
            text("SELECT COUNT(*) FROM silver.quarantined_yellow_trips")
        ).scalar()

    logger.info("📊 Silver Layer Summary:")
    logger.info(f"   ✅ Clean rows in Silver:       {silver_count:,}")
    logger.info(f"   🚫 Quarantined rows:           {quarantine_count:,}")
    logger.info(f"   📈 Total accounted for:        {silver_count + quarantine_count:,}")

    return {
        "status": "success",
        "silver_rows": silver_rows,
        "quarantine_rows": quarantine_rows,
    }


if __name__ == "__main__":
    logger.info("🚕 TaxiPulse — Silver Layer Transformation")
    logger.info("=" * 60)
    run_silver_pipeline()