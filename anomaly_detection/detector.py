"""
TaxiPulse — Anomaly Detection Engine
Detects pricing and volume anomalies in NYC taxi data
using Z-score and IQR methods on Gold layer aggregations.
"""

import sys
from pathlib import Path
import pandas as pd
import numpy as np
from datetime import datetime
from sqlalchemy import create_engine, text
from loguru import logger

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from config.settings import PostgresConfig, AnomalyConfig


def get_pg_engine():
    """Create and return a SQLAlchemy engine."""
    return create_engine(PostgresConfig.get_connection_string())


# ============================================================
# Z-Score Anomaly Detection
# ============================================================

def detect_zscore_anomalies(
    df: pd.DataFrame,
    value_col: str,
    group_col: str = None,
    threshold: float = None,
) -> pd.DataFrame:
    """
    Detect anomalies using Z-score method.

    Args:
        df: DataFrame with data to analyze
        value_col: Column to check for anomalies
        group_col: Optional grouping column (e.g., pickup_location_id)
        threshold: Z-score threshold (default from config)

    Returns:
        DataFrame of anomalous rows with z_score column added
    """
    threshold = threshold or AnomalyConfig.ZSCORE_THRESHOLD

    if group_col and group_col in df.columns:
        # Calculate Z-score within each group
        grouped = df.groupby(group_col)[value_col]
        df["mean"] = grouped.transform("mean")
        df["std"] = grouped.transform("std")
    else:
        df["mean"] = df[value_col].mean()
        df["std"] = df[value_col].std()

    # Avoid division by zero
    df["std"] = df["std"].replace(0, np.nan)
    df["z_score"] = ((df[value_col] - df["mean"]) / df["std"]).abs()

    # Filter anomalies
    anomalies = df[df["z_score"] >= threshold].copy()

    # Clean up temp columns from original df
    df.drop(columns=["mean", "std", "z_score"], inplace=True, errors="ignore")

    return anomalies


# ============================================================
# Fare Anomaly Detection
# ============================================================

def detect_fare_anomalies(engine) -> pd.DataFrame:
    """
    Detect zones/hours where average fare is abnormally high or low.
    Uses hourly zone revenue aggregation from Gold layer.
    """
    logger.info("💰 Detecting fare anomalies...")

    df = pd.read_sql(
        """
        SELECT date, hour, pickup_location_id,
               avg_fare, total_trips, total_revenue
        FROM gold.agg_hourly_zone_revenue
        WHERE total_trips >= 5
        """,
        con=engine,
    )

    if df.empty:
        logger.info("   No data for fare anomaly detection")
        return pd.DataFrame()

    logger.info(f"   Analyzing {len(df):,} hourly zone records...")

    anomalies = detect_zscore_anomalies(
        df,
        value_col="avg_fare",
        group_col="pickup_location_id",
    )

    if not anomalies.empty:
        anomalies["anomaly_type"] = "fare_spike"
        anomalies["severity"] = anomalies["z_score"].apply(
            lambda z: "critical" if z >= 4 else "high" if z >= 3.5 else "medium"
        )
        anomalies["metric_name"] = "avg_fare"
        anomalies["description"] = anomalies.apply(
            lambda r: (
                f"Zone {int(r['pickup_location_id'])}: "
                f"avg fare ${r['avg_fare']:.2f} "
                f"(z-score: {r['z_score']:.1f}) on "
                f"{r['date']} at {int(r['hour'])}:00"
            ),
            axis=1,
        )

    logger.info(f"   Found {len(anomalies):,} fare anomalies")
    return anomalies


# ============================================================
# Volume Anomaly Detection
# ============================================================

def detect_volume_anomalies(engine) -> pd.DataFrame:
    """
    Detect zones/hours where trip volume is abnormally high or low.
    """
    logger.info("📊 Detecting volume anomalies...")

    df = pd.read_sql(
        """
        SELECT date, hour, pickup_location_id,
               total_trips, total_revenue, avg_fare
        FROM gold.agg_hourly_zone_revenue
        WHERE total_trips >= 3
        """,
        con=engine,
    )

    if df.empty:
        logger.info("   No data for volume anomaly detection")
        return pd.DataFrame()

    logger.info(f"   Analyzing {len(df):,} hourly zone records...")

    anomalies = detect_zscore_anomalies(
        df,
        value_col="total_trips",
        group_col="pickup_location_id",
    )

    if not anomalies.empty:
        anomalies["anomaly_type"] = "volume_spike"
        anomalies["severity"] = anomalies["z_score"].apply(
            lambda z: "critical" if z >= 4 else "high" if z >= 3.5 else "medium"
        )
        anomalies["metric_name"] = "total_trips"
        anomalies["description"] = anomalies.apply(
            lambda r: (
                f"Zone {int(r['pickup_location_id'])}: "
                f"{int(r['total_trips'])} trips "
                f"(z-score: {r['z_score']:.1f}) on "
                f"{r['date']} at {int(r['hour'])}:00"
            ),
            axis=1,
        )

    logger.info(f"   Found {len(anomalies):,} volume anomalies")
    return anomalies


# ============================================================
# Daily Revenue Anomaly Detection
# ============================================================

def detect_daily_revenue_anomalies(engine) -> pd.DataFrame:
    """
    Detect days where total revenue is abnormally high or low.
    """
    logger.info("📈 Detecting daily revenue anomalies...")

    df = pd.read_sql(
        "SELECT * FROM gold.agg_daily_summary",
        con=engine,
    )

    if df.empty:
        logger.info("   No data for daily revenue anomaly detection")
        return pd.DataFrame()

    logger.info(f"   Analyzing {len(df):,} daily records...")

    anomalies = detect_zscore_anomalies(
        df,
        value_col="total_revenue",
    )

    if not anomalies.empty:
        anomalies["anomaly_type"] = "daily_revenue_anomaly"
        anomalies["pickup_location_id"] = None
        anomalies["severity"] = anomalies["z_score"].apply(
            lambda z: "critical" if z >= 4 else "high" if z >= 3.5 else "medium"
        )
        anomalies["metric_name"] = "total_revenue"
        anomalies["description"] = anomalies.apply(
            lambda r: (
                f"Daily revenue ${r['total_revenue']:,.0f} "
                f"(z-score: {r['z_score']:.1f}) on {r['date']} "
                f"({int(r['total_trips']):,} trips)"
            ),
            axis=1,
        )

    logger.info(f"   Found {len(anomalies):,} daily revenue anomalies")
    return anomalies


# ============================================================
# Log Anomalies to PostgreSQL
# ============================================================

def log_anomalies(engine, anomalies: pd.DataFrame) -> int:
    """Write detected anomalies to gold.anomaly_log."""
    if anomalies.empty:
        return 0

    records = []
    for _, row in anomalies.iterrows():
        records.append({
            "anomaly_type": row.get("anomaly_type", "unknown"),
            "severity": row.get("severity", "medium"),
            "zone_id": int(row["pickup_location_id"]) if pd.notna(row.get("pickup_location_id")) else None,
            "metric_name": row.get("metric_name", ""),
            "expected_value": float(row.get("mean", 0)) if pd.notna(row.get("mean")) else None,
            "actual_value": float(row.get(row.get("metric_name", ""), 0)) if row.get("metric_name") in row.index else None,
            "z_score": float(row.get("z_score", 0)),
            "description": row.get("description", ""),
            "alert_sent": False,
        })

    with engine.begin() as conn:
        for rec in records:
            conn.execute(text("""
                INSERT INTO gold.anomaly_log (
                    anomaly_type, severity, zone_id, metric_name,
                    expected_value, actual_value, z_score,
                    description, alert_sent
                ) VALUES (
                    :anomaly_type, :severity, :zone_id, :metric_name,
                    :expected_value, :actual_value, :z_score,
                    :description, :alert_sent
                )
            """), rec)

    logger.info(f"📝 Logged {len(records):,} anomalies to gold.anomaly_log")
    return len(records)


# ============================================================
# Full Anomaly Detection Pipeline
# ============================================================

def run_anomaly_detection() -> dict:
    """Run all anomaly detection checks and log results."""
    engine = get_pg_engine()

    logger.info("🔍 Running Anomaly Detection Pipeline")
    logger.info("=" * 60)

    all_anomalies = []

    # 1. Fare anomalies
    logger.info("")
    fare_anomalies = detect_fare_anomalies(engine)
    if not fare_anomalies.empty:
        all_anomalies.append(fare_anomalies)

    # 2. Volume anomalies
    logger.info("")
    volume_anomalies = detect_volume_anomalies(engine)
    if not volume_anomalies.empty:
        all_anomalies.append(volume_anomalies)

    # 3. Daily revenue anomalies
    logger.info("")
    daily_anomalies = detect_daily_revenue_anomalies(engine)
    if not daily_anomalies.empty:
        all_anomalies.append(daily_anomalies)

    # Combine and log
    if all_anomalies:
        combined = pd.concat(all_anomalies, ignore_index=True)
        logged = log_anomalies(engine, combined)
    else:
        combined = pd.DataFrame()
        logged = 0

    # Summary
    logger.info("")
    logger.info("=" * 60)
    logger.info("🚨 Anomaly Detection Summary:")

    total = len(combined) if not combined.empty else 0
    logger.info(f"   Total anomalies detected: {total:,}")

    if not combined.empty:
        severity_counts = combined["severity"].value_counts()
        for sev, count in severity_counts.items():
            icon = "🔴" if sev == "critical" else "🟠" if sev == "high" else "🟡"
            logger.info(f"   {icon} {sev}: {count:,}")

        type_counts = combined["anomaly_type"].value_counts()
        logger.info("")
        for atype, count in type_counts.items():
            logger.info(f"   {atype}: {count:,}")

        # Show top 5 most severe
        logger.info("")
        logger.info("🔝 Top 5 Most Severe Anomalies:")
        top5 = combined.nlargest(5, "z_score")
        for _, row in top5.iterrows():
            logger.info(f"   ⚠️  {row['description']}")

    logger.info(f"   📝 Logged to database: {logged:,}")

    return {
        "total_anomalies": total,
        "fare_anomalies": len(fare_anomalies) if not fare_anomalies.empty else 0,
        "volume_anomalies": len(volume_anomalies) if not volume_anomalies.empty else 0,
        "daily_anomalies": len(daily_anomalies) if not daily_anomalies.empty else 0,
        "logged": logged,
    }


if __name__ == "__main__":
    logger.info("🚕 TaxiPulse — Anomaly Detection")
    results = run_anomaly_detection()
    if results["total_anomalies"] > 0:
        logger.success(f"✅ Detection complete! {results['total_anomalies']} anomalies found")
    else:
        logger.info("✅ No anomalies detected")