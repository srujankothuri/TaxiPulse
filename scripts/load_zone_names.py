"""
TaxiPulse — Load NYC Taxi Zone Names
Downloads the TLC zone lookup CSV and updates
the Gold dimension tables with borough/zone names.
"""

import sys
from pathlib import Path
import pandas as pd
from sqlalchemy import create_engine, text
from loguru import logger

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from config.settings import PostgresConfig

ZONE_LOOKUP_URL = (
    "https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv"
)


def get_pg_engine():
    return create_engine(PostgresConfig.get_connection_string())


def load_zone_names():
    """Download zone lookup and update dimension tables."""
    logger.info("📍 Downloading NYC Taxi Zone Lookup...")

    import requests
    import io

    response = requests.get(ZONE_LOOKUP_URL, verify=False)
    response.raise_for_status()
    df = pd.read_csv(io.StringIO(response.text))
    logger.info(f"   Downloaded {len(df)} zones")
    logger.info(f"   Columns: {list(df.columns)}")

    # Standardize column names
    df = df.rename(columns={
        "LocationID": "location_id",
        "Borough": "borough",
        "Zone": "zone_name",
        "service_zone": "service_zone",
    })

    engine = get_pg_engine()

    # Update pickup locations
    logger.info("📤 Updating dim_pickup_location...")
    updated_pickup = 0
    with engine.begin() as conn:
        for _, row in df.iterrows():
            result = conn.execute(text("""
                UPDATE gold.dim_pickup_location
                SET borough = :borough,
                    zone_name = :zone_name,
                    service_zone = :service_zone
                WHERE pickup_location_id = :loc_id
            """), {
                "borough": row["borough"],
                "zone_name": row["zone_name"],
                "service_zone": row.get("service_zone", ""),
                "loc_id": row["location_id"],
            })
            updated_pickup += result.rowcount

    # Update dropoff locations
    logger.info("📤 Updating dim_dropoff_location...")
    updated_dropoff = 0
    with engine.begin() as conn:
        for _, row in df.iterrows():
            result = conn.execute(text("""
                UPDATE gold.dim_dropoff_location
                SET borough = :borough,
                    zone_name = :zone_name,
                    service_zone = :service_zone
                WHERE dropoff_location_id = :loc_id
            """), {
                "borough": row["borough"],
                "zone_name": row["zone_name"],
                "service_zone": row.get("service_zone", ""),
                "loc_id": row["location_id"],
            })
            updated_dropoff += result.rowcount

    logger.success(f"✅ Updated {updated_pickup} pickup zones, {updated_dropoff} dropoff zones")

    # Verify
    with engine.connect() as conn:
        sample = conn.execute(text("""
            SELECT pickup_location_id, borough, zone_name
            FROM gold.dim_pickup_location
            WHERE zone_name IS NOT NULL
            ORDER BY pickup_location_id
            LIMIT 10
        """)).fetchall()

    logger.info("")
    logger.info("📍 Sample zones:")
    for row in sample:
        logger.info(f"   Zone {row[0]}: {row[2]} ({row[1]})")

    return updated_pickup


if __name__ == "__main__":
    logger.info("🚕 TaxiPulse — Load Zone Names")
    logger.info("=" * 60)
    load_zone_names()