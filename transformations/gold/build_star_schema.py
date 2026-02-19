"""
TaxiPulse — Gold Layer: Build Star Schema
Reads from Silver layer and populates Gold dimension + fact tables.
Uses SQL-based transformations for speed (no pandas row-by-row).
"""

import sys
from pathlib import Path
from sqlalchemy import create_engine, text
from loguru import logger

sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))
from config.settings import PostgresConfig


def get_pg_engine():
    """Create and return a SQLAlchemy engine."""
    return create_engine(PostgresConfig.get_connection_string())


# ============================================================
# Dimension Builders
# ============================================================

def build_dim_datetime(engine) -> int:
    """
    Populate dim_datetime from unique pickup datetimes in Silver.
    Truncates to the hour for manageable granularity.
    """
    logger.info("📅 Building dim_datetime...")

    with engine.begin() as conn:
        # Check if already populated
        count = conn.execute(
            text("SELECT COUNT(*) FROM gold.dim_datetime")
        ).scalar()
        if count > 0:
            logger.info(f"   ⏭️ Already has {count:,} rows. Skipping.")
            return count

        result = conn.execute(text("""
            INSERT INTO gold.dim_datetime (
                full_datetime, date, year, month, day, hour,
                day_of_week, day_name, month_name, is_weekend, quarter
            )
            SELECT DISTINCT
                date_trunc('hour', pickup_datetime) AS full_datetime,
                pickup_datetime::date AS date,
                EXTRACT(YEAR FROM pickup_datetime)::int AS year,
                EXTRACT(MONTH FROM pickup_datetime)::int AS month,
                EXTRACT(DAY FROM pickup_datetime)::int AS day,
                EXTRACT(HOUR FROM pickup_datetime)::int AS hour,
                EXTRACT(DOW FROM pickup_datetime)::int AS day_of_week,
                TO_CHAR(pickup_datetime, 'Day') AS day_name,
                TO_CHAR(pickup_datetime, 'Month') AS month_name,
                CASE WHEN EXTRACT(DOW FROM pickup_datetime) IN (0, 6)
                     THEN TRUE ELSE FALSE END AS is_weekend,
                EXTRACT(QUARTER FROM pickup_datetime)::int AS quarter
            FROM silver.clean_yellow_trips
            WHERE pickup_datetime IS NOT NULL
            ON CONFLICT (full_datetime) DO NOTHING
        """))

        count = conn.execute(
            text("SELECT COUNT(*) FROM gold.dim_datetime")
        ).scalar()

    logger.success(f"   ✅ dim_datetime: {count:,} rows")
    return count


def build_dim_locations(engine) -> tuple:
    """
    Populate dim_pickup_location and dim_dropoff_location
    from unique location IDs in Silver data.
    NYC TLC uses zone IDs 1-263.
    """
    logger.info("📍 Building dim_pickup_location & dim_dropoff_location...")

    with engine.begin() as conn:
        # Check if already populated
        pickup_count = conn.execute(
            text("SELECT COUNT(*) FROM gold.dim_pickup_location")
        ).scalar()
        if pickup_count > 0:
            dropoff_count = conn.execute(
                text("SELECT COUNT(*) FROM gold.dim_dropoff_location")
            ).scalar()
            logger.info(
                f"   ⏭️ Already populated: pickup={pickup_count}, "
                f"dropoff={dropoff_count}. Skipping."
            )
            return pickup_count, dropoff_count

        # Pickup locations
        conn.execute(text("""
            INSERT INTO gold.dim_pickup_location (pickup_location_id)
            SELECT DISTINCT pickup_location_id
            FROM silver.clean_yellow_trips
            WHERE pickup_location_id IS NOT NULL
            ON CONFLICT (pickup_location_id) DO NOTHING
        """))

        # Dropoff locations
        conn.execute(text("""
            INSERT INTO gold.dim_dropoff_location (dropoff_location_id)
            SELECT DISTINCT dropoff_location_id
            FROM silver.clean_yellow_trips
            WHERE dropoff_location_id IS NOT NULL
            ON CONFLICT (dropoff_location_id) DO NOTHING
        """))

        pickup_count = conn.execute(
            text("SELECT COUNT(*) FROM gold.dim_pickup_location")
        ).scalar()
        dropoff_count = conn.execute(
            text("SELECT COUNT(*) FROM gold.dim_dropoff_location")
        ).scalar()

    logger.success(
        f"   ✅ dim_pickup_location: {pickup_count} zones"
    )
    logger.success(
        f"   ✅ dim_dropoff_location: {dropoff_count} zones"
    )
    return pickup_count, dropoff_count


# ============================================================
# Fact Table Builder
# ============================================================

def build_fact_trips(engine) -> int:
    """
    Populate fact_trips by joining Silver data with dimension tables.
    Uses SQL INSERT...SELECT for speed.
    """
    logger.info("📊 Building fact_trips...")

    with engine.begin() as conn:
        # Check if already populated
        count = conn.execute(
            text("SELECT COUNT(*) FROM gold.fact_trips")
        ).scalar()
        if count > 0:
            logger.info(f"   ⏭️ Already has {count:,} rows. Skipping.")
            return count

        logger.info("   Inserting from Silver (this may take a few minutes)...")

        conn.execute(text("""
            INSERT INTO gold.fact_trips (
                datetime_id, pickup_location_id, dropoff_location_id,
                payment_type_id, rate_code_id,
                passenger_count, trip_distance, trip_duration_minutes,
                fare_amount, tip_amount, tolls_amount,
                total_amount, congestion_surcharge, airport_fee
            )
            SELECT
                d.datetime_id,
                s.pickup_location_id,
                s.dropoff_location_id,
                s.payment_type_id,
                s.rate_code_id,
                s.passenger_count,
                s.trip_distance,
                s.trip_duration_minutes,
                s.fare_amount,
                s.tip_amount,
                s.tolls_amount,
                s.total_amount,
                s.congestion_surcharge,
                s.airport_fee
            FROM silver.clean_yellow_trips s
            JOIN gold.dim_datetime d
                ON date_trunc('hour', s.pickup_datetime) = d.full_datetime
        """))

        count = conn.execute(
            text("SELECT COUNT(*) FROM gold.fact_trips")
        ).scalar()

    logger.success(f"   ✅ fact_trips: {count:,} rows")
    return count


# ============================================================
# Aggregation Builders
# ============================================================

def build_agg_hourly_zone_revenue(engine) -> int:
    """
    Build hourly revenue aggregation per pickup zone.
    Used for surge detection and zone analytics.
    """
    logger.info("📈 Building agg_hourly_zone_revenue...")

    with engine.begin() as conn:
        count = conn.execute(
            text("SELECT COUNT(*) FROM gold.agg_hourly_zone_revenue")
        ).scalar()
        if count > 0:
            logger.info(f"   ⏭️ Already has {count:,} rows. Skipping.")
            return count

        conn.execute(text("""
            INSERT INTO gold.agg_hourly_zone_revenue (
                date, hour, pickup_location_id,
                total_trips, total_revenue, avg_fare,
                avg_trip_distance, avg_trip_duration
            )
            SELECT
                pickup_datetime::date AS date,
                EXTRACT(HOUR FROM pickup_datetime)::int AS hour,
                pickup_location_id,
                COUNT(*) AS total_trips,
                SUM(total_amount) AS total_revenue,
                AVG(fare_amount) AS avg_fare,
                AVG(trip_distance) AS avg_trip_distance,
                AVG(trip_duration_minutes) AS avg_trip_duration
            FROM silver.clean_yellow_trips
            GROUP BY
                pickup_datetime::date,
                EXTRACT(HOUR FROM pickup_datetime)::int,
                pickup_location_id
            ON CONFLICT (date, hour, pickup_location_id) DO NOTHING
        """))

        count = conn.execute(
            text("SELECT COUNT(*) FROM gold.agg_hourly_zone_revenue")
        ).scalar()

    logger.success(f"   ✅ agg_hourly_zone_revenue: {count:,} rows")
    return count


def build_agg_daily_summary(engine) -> int:
    """
    Build daily summary with key metrics.
    Used for dashboard overview and trend analysis.
    """
    logger.info("📈 Building agg_daily_summary...")

    with engine.begin() as conn:
        count = conn.execute(
            text("SELECT COUNT(*) FROM gold.agg_daily_summary")
        ).scalar()
        if count > 0:
            logger.info(f"   ⏭️ Already has {count:,} rows. Skipping.")
            return count

        conn.execute(text("""
            INSERT INTO gold.agg_daily_summary (
                date, total_trips, total_revenue,
                avg_fare, avg_distance, avg_duration,
                total_passengers, credit_card_pct, cash_pct
            )
            SELECT
                pickup_datetime::date AS date,
                COUNT(*) AS total_trips,
                SUM(total_amount) AS total_revenue,
                AVG(fare_amount) AS avg_fare,
                AVG(trip_distance) AS avg_distance,
                AVG(trip_duration_minutes) AS avg_duration,
                SUM(passenger_count) AS total_passengers,
                ROUND(
                    100.0 * SUM(CASE WHEN payment_type_id = 1 THEN 1 ELSE 0 END)
                    / NULLIF(COUNT(*), 0), 2
                ) AS credit_card_pct,
                ROUND(
                    100.0 * SUM(CASE WHEN payment_type_id = 2 THEN 1 ELSE 0 END)
                    / NULLIF(COUNT(*), 0), 2
                ) AS cash_pct
            FROM silver.clean_yellow_trips
            GROUP BY pickup_datetime::date
            ON CONFLICT (date) DO NOTHING
        """))

        count = conn.execute(
            text("SELECT COUNT(*) FROM gold.agg_daily_summary")
        ).scalar()

    logger.success(f"   ✅ agg_daily_summary: {count:,} rows")
    return count


# ============================================================
# Full Gold Pipeline
# ============================================================

def run_gold_pipeline() -> dict:
    """Build the complete Gold layer: dimensions, facts, aggregations."""
    engine = get_pg_engine()

    logger.info("🏆 Building Gold Layer (Star Schema + Aggregations)")
    logger.info("=" * 60)

    results = {}

    # Step 1: Dimensions
    logger.info("")
    logger.info("📐 STEP 1: Building Dimension Tables...")
    logger.info("-" * 40)
    results["dim_datetime"] = build_dim_datetime(engine)
    pickup, dropoff = build_dim_locations(engine)
    results["dim_pickup_location"] = pickup
    results["dim_dropoff_location"] = dropoff

    # Payment and rate code dims are pre-populated in init_db.sql
    with engine.connect() as conn:
        results["dim_payment_type"] = conn.execute(
            text("SELECT COUNT(*) FROM gold.dim_payment_type")
        ).scalar()
        results["dim_rate_code"] = conn.execute(
            text("SELECT COUNT(*) FROM gold.dim_rate_code")
        ).scalar()
    logger.info(f"   ✅ dim_payment_type: {results['dim_payment_type']} rows (pre-loaded)")
    logger.info(f"   ✅ dim_rate_code: {results['dim_rate_code']} rows (pre-loaded)")

    # Step 2: Fact Table
    logger.info("")
    logger.info("📊 STEP 2: Building Fact Table...")
    logger.info("-" * 40)
    results["fact_trips"] = build_fact_trips(engine)

    # Step 3: Aggregations
    logger.info("")
    logger.info("📈 STEP 3: Building Aggregation Tables...")
    logger.info("-" * 40)
    results["agg_hourly_zone_revenue"] = build_agg_hourly_zone_revenue(engine)
    results["agg_daily_summary"] = build_agg_daily_summary(engine)

    # Summary
    logger.info("")
    logger.info("=" * 60)
    logger.info("🏆 Gold Layer Summary:")
    for table, count in results.items():
        logger.info(f"   {table}: {count:,} rows")

    logger.success("✅ Gold layer build complete!")
    return results


if __name__ == "__main__":
    logger.info("🚕 TaxiPulse — Gold Layer Builder")
    logger.info("")
    run_gold_pipeline()