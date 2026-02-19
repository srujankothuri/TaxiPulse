"""
TaxiPulse — Streamlit Data Helper
Supports two modes:
  1. PostgreSQL (local — full SQL queries)
  2. CSV fallback (Streamlit Cloud — pre-exported data)
"""

import os
import pandas as pd
import streamlit as st
from pathlib import Path

DATA_DIR = Path(__file__).parent / "data"

def _is_csv_mode():
    """Check if we should use CSV files instead of database."""
    if os.getenv("USE_CSV", "false").lower() == "true":
        return True
    # Also use CSV if data directory exists and has files but no DB host
    if not os.getenv("POSTGRES_HOST") and DATA_DIR.exists():
        csvs = list(DATA_DIR.glob("*.csv"))
        if csvs:
            return True
    return False

USE_CSV = _is_csv_mode()


# ============================================================
# PostgreSQL Mode
# ============================================================

@st.cache_resource
def get_engine():
    if USE_CSV:
        return None
    from sqlalchemy import create_engine
    host = os.getenv("POSTGRES_HOST", "localhost")
    port = os.getenv("POSTGRES_PORT", "5432")
    db = os.getenv("POSTGRES_DB", "taxipulse")
    user = os.getenv("POSTGRES_USER", "taxipulse")
    pw = os.getenv("POSTGRES_PASSWORD", "taxipulse123")
    return create_engine(f"postgresql://{user}:{pw}@{host}:{port}/{db}")


@st.cache_data(ttl=300)
def run_query(query: str) -> pd.DataFrame:
    if USE_CSV:
        return pd.DataFrame()  # Pages should use named functions below
    engine = get_engine()
    return pd.read_sql(query, con=engine)


# ============================================================
# CSV Loaders
# ============================================================

@st.cache_data(ttl=600)
def _load_csv(filename: str) -> pd.DataFrame:
    path = DATA_DIR / filename
    if path.exists():
        return pd.read_csv(path)
    return pd.DataFrame()


def _read_count(filename: str) -> int:
    df = _load_csv(filename)
    if not df.empty and "cnt" in df.columns:
        return int(df["cnt"].iloc[0])
    return 0


# ============================================================
# Named Data Functions (work in both modes)
# ============================================================

@st.cache_data(ttl=300)
def get_bronze_count() -> int:
    if USE_CSV:
        return _read_count("bronze_count.csv")
    df = run_query("SELECT COUNT(*) as cnt FROM bronze.raw_yellow_trips")
    return int(df["cnt"].iloc[0])


@st.cache_data(ttl=300)
def get_silver_count() -> int:
    if USE_CSV:
        return _read_count("silver_count.csv")
    df = run_query("SELECT COUNT(*) as cnt FROM silver.clean_yellow_trips")
    return int(df["cnt"].iloc[0])


@st.cache_data(ttl=300)
def get_quarantine_count() -> int:
    if USE_CSV:
        return _read_count("quarantine_count.csv")
    df = run_query("SELECT COUNT(*) as cnt FROM silver.quarantined_yellow_trips")
    return int(df["cnt"].iloc[0])


@st.cache_data(ttl=300)
def get_anomaly_count() -> int:
    if USE_CSV:
        df = _load_csv("anomaly_log.csv")
        return len(df)
    df = run_query("SELECT COUNT(*) as cnt FROM gold.anomaly_log")
    return int(df["cnt"].iloc[0])


@st.cache_data(ttl=300)
def get_fact_count() -> int:
    if USE_CSV:
        return _read_count("fact_count.csv")
    df = run_query("SELECT COUNT(*) as cnt FROM gold.fact_trips")
    return int(df["cnt"].iloc[0])


@st.cache_data(ttl=300)
def get_dim_datetime_count() -> int:
    if USE_CSV:
        return _read_count("dim_datetime_count.csv")
    df = run_query("SELECT COUNT(*) as cnt FROM gold.dim_datetime")
    return int(df["cnt"].iloc[0])


@st.cache_data(ttl=300)
def get_hourly_agg_count() -> int:
    if USE_CSV:
        df = _load_csv("agg_hourly_zone_revenue.csv")
        return len(df)
    df = run_query("SELECT COUNT(*) as cnt FROM gold.agg_hourly_zone_revenue")
    return int(df["cnt"].iloc[0])


@st.cache_data(ttl=300)
def get_daily_agg_count() -> int:
    if USE_CSV:
        df = _load_csv("agg_daily_summary.csv")
        return len(df)
    df = run_query("SELECT COUNT(*) as cnt FROM gold.agg_daily_summary")
    return int(df["cnt"].iloc[0])


@st.cache_data(ttl=300)
def get_freshness() -> pd.DataFrame:
    if USE_CSV:
        return _load_csv("freshness.csv")
    return run_query("""
        SELECT MIN(pickup_datetime) as earliest,
               MAX(pickup_datetime) as latest,
               COUNT(DISTINCT pickup_datetime::date) as total_days
        FROM silver.clean_yellow_trips
    """)


@st.cache_data(ttl=300)
def get_daily_summary() -> pd.DataFrame:
    if USE_CSV:
        df = _load_csv("agg_daily_summary.csv")
        if "date" in df.columns:
            df["date"] = pd.to_datetime(df["date"])
            df = df.sort_values("date").reset_index(drop=True)
        return df
    return run_query("""
        SELECT date, total_trips, total_revenue, avg_fare,
               avg_distance, avg_duration, credit_card_pct, cash_pct
        FROM gold.agg_daily_summary ORDER BY date
    """)


@st.cache_data(ttl=300)
def get_hourly_patterns() -> pd.DataFrame:
    if USE_CSV:
        df = _load_csv("agg_hourly_zone_revenue.csv")
        if df.empty:
            return df
        grouped = df.groupby("hour").agg(
            trips=("total_trips", "sum"),
            avg_fare=("avg_fare", "mean"),
            revenue=("total_revenue", "sum"),
        ).reset_index()
        return grouped
    return run_query("""
        SELECT hour, SUM(total_trips) as trips,
               AVG(avg_fare) as avg_fare, SUM(total_revenue) as revenue
        FROM gold.agg_hourly_zone_revenue GROUP BY hour ORDER BY hour
    """)


@st.cache_data(ttl=300)
def get_top_zones() -> pd.DataFrame:
    if USE_CSV:
        hourly = _load_csv("agg_hourly_zone_revenue.csv")
        locations = _load_csv("dim_pickup_location.csv")
        if hourly.empty:
            return pd.DataFrame()
        grouped = hourly.groupby("pickup_location_id").agg(
            trips=("total_trips", "sum"),
            revenue=("total_revenue", "sum"),
            avg_fare=("avg_fare", "mean"),
        ).reset_index()
        grouped = grouped.rename(columns={"pickup_location_id": "zone_id"})
        if not locations.empty:
            grouped = grouped.merge(
                locations.rename(columns={"pickup_location_id": "zone_id"}),
                on="zone_id", how="left",
            )
            grouped["zone_name"] = grouped["zone_name"].fillna("Zone " + grouped["zone_id"].astype(str))
            grouped["borough"] = grouped["borough"].fillna("Unknown")
        else:
            grouped["zone_name"] = "Zone " + grouped["zone_id"].astype(str)
            grouped["borough"] = "Unknown"
        return grouped.sort_values("trips", ascending=False).head(20)
    return run_query("""
        SELECT h.pickup_location_id as zone_id,
               COALESCE(d.zone_name, 'Zone ' || h.pickup_location_id::text) as zone_name,
               COALESCE(d.borough, 'Unknown') as borough,
               SUM(h.total_trips) as trips, SUM(h.total_revenue) as revenue,
               AVG(h.avg_fare) as avg_fare
        FROM gold.agg_hourly_zone_revenue h
        LEFT JOIN gold.dim_pickup_location d ON h.pickup_location_id = d.pickup_location_id
        GROUP BY h.pickup_location_id, d.zone_name, d.borough
        ORDER BY trips DESC LIMIT 20
    """)


@st.cache_data(ttl=300)
def get_anomaly_summary() -> dict:
    if USE_CSV:
        df = _load_csv("anomaly_log.csv")
        if df.empty:
            return {"total": 0, "critical": 0, "high": 0, "medium": 0, "alerted": 0}
        alerted = 0
        if "alert_sent" in df.columns:
            # Handle PostgreSQL boolean format (t/f) and Python bool
            alerted = int(df["alert_sent"].astype(str).str.lower().isin(["true", "t", "1"]).sum())
        return {
            "total": len(df),
            "critical": int((df["severity"] == "critical").sum()) if "severity" in df.columns else 0,
            "high": int((df["severity"] == "high").sum()) if "severity" in df.columns else 0,
            "medium": int((df["severity"] == "medium").sum()) if "severity" in df.columns else 0,
            "alerted": alerted,
        }
    df = run_query("""
        SELECT COUNT(*) as total,
               SUM(CASE WHEN severity='critical' THEN 1 ELSE 0 END) as critical,
               SUM(CASE WHEN severity='high' THEN 1 ELSE 0 END) as high,
               SUM(CASE WHEN severity='medium' THEN 1 ELSE 0 END) as medium,
               SUM(CASE WHEN alert_sent THEN 1 ELSE 0 END) as alerted
        FROM gold.anomaly_log
    """)
    return {
        "total": int(df["total"].iloc[0]),
        "critical": int(df["critical"].iloc[0]),
        "high": int(df["high"].iloc[0]),
        "medium": int(df["medium"].iloc[0]),
        "alerted": int(df["alerted"].iloc[0]),
    }


@st.cache_data(ttl=300)
def get_anomaly_log(severity_filter: list = None, type_filter: list = None) -> pd.DataFrame:
    if USE_CSV:
        df = _load_csv("anomaly_log.csv")
        if df.empty:
            return df
        if severity_filter and "severity" in df.columns:
            df = df[df["severity"].isin(severity_filter)]
        if type_filter and "anomaly_type" in df.columns:
            df = df[df["anomaly_type"].isin(type_filter)]
        return df
    sev_str = ",".join(f"'{s}'" for s in (severity_filter or ["critical", "high", "medium"]))
    type_str = ",".join(f"'{t}'" for t in (type_filter or ["fare_spike", "volume_spike", "daily_revenue_anomaly"]))
    return run_query(f"""
        SELECT * FROM gold.anomaly_log
        WHERE severity IN ({sev_str}) AND anomaly_type IN ({type_str})
    """)


@st.cache_data(ttl=300)
def get_anomaly_zones(severity_filter: list = None, type_filter: list = None) -> pd.DataFrame:
    if USE_CSV:
        df = get_anomaly_log(severity_filter, type_filter)
        locations = _load_csv("dim_pickup_location.csv")
        if df.empty or "zone_id" not in df.columns:
            return pd.DataFrame()
        df = df[df["zone_id"].notna()]
        grouped = df.groupby("zone_id").agg(
            anomaly_count=("zone_id", "count"),
            avg_z_score=("z_score", "mean"),
            max_z_score=("z_score", "max"),
        ).reset_index()
        if not locations.empty:
            grouped = grouped.merge(
                locations.rename(columns={"pickup_location_id": "zone_id"}),
                on="zone_id", how="left",
            )
            grouped["zone_name"] = grouped["zone_name"].fillna("Zone " + grouped["zone_id"].astype(int).astype(str))
            grouped["borough"] = grouped["borough"].fillna("Unknown")
        else:
            grouped["zone_name"] = "Zone " + grouped["zone_id"].astype(int).astype(str)
            grouped["borough"] = "Unknown"
        return grouped.sort_values("anomaly_count", ascending=False).head(15)
    sev_str = ",".join(f"'{s}'" for s in (severity_filter or []))
    type_str = ",".join(f"'{t}'" for t in (type_filter or []))
    return run_query(f"""
        SELECT a.zone_id,
               COALESCE(d.zone_name, 'Zone ' || a.zone_id::text) as zone_name,
               COALESCE(d.borough, 'Unknown') as borough,
               COUNT(*) as anomaly_count, AVG(a.z_score) as avg_z_score,
               MAX(a.z_score) as max_z_score
        FROM gold.anomaly_log a
        LEFT JOIN gold.dim_pickup_location d ON a.zone_id = d.pickup_location_id
        WHERE a.zone_id IS NOT NULL AND a.severity IN ({sev_str}) AND a.anomaly_type IN ({type_str})
        GROUP BY a.zone_id, d.zone_name, d.borough
        ORDER BY anomaly_count DESC LIMIT 15
    """)


@st.cache_data(ttl=300)
def get_quality_log() -> pd.DataFrame:
    if USE_CSV:
        return _load_csv("quality_log.csv")
    return run_query("""
        SELECT source_file, total_records, passed_records,
               failed_records, pass_rate, check_details
        FROM gold.quality_log ORDER BY check_timestamp
    """)


@st.cache_data(ttl=300)
def get_quarantine_stats() -> dict:
    if USE_CSV:
        count = _read_count("quarantine_count.csv")
        return {"total_quarantined": count, "files_affected": 3}
    df = run_query("""
        SELECT COUNT(*) as total_quarantined,
               COUNT(DISTINCT source_file) as files_affected
        FROM silver.quarantined_yellow_trips
    """)
    return {
        "total_quarantined": int(df["total_quarantined"].iloc[0]),
        "files_affected": int(df["files_affected"].iloc[0]),
    }


@st.cache_data(ttl=300)
def get_quarantine_reasons() -> pd.DataFrame:
    if USE_CSV:
        return pd.DataFrame({
            "quarantine_reason": ["Fare/total out of range", "Tolls out of range", "Other"],
            "count": [107665, 7564, 22174],
        })
    return run_query("""
        SELECT quarantine_reason, COUNT(*) as count
        FROM silver.quarantined_yellow_trips
        GROUP BY quarantine_reason ORDER BY count DESC LIMIT 10
    """)