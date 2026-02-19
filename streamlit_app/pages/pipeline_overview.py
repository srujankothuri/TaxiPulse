"""
TaxiPulse — Pipeline Overview Page
Shows high-level metrics, pipeline status, and architecture.
"""

import streamlit as st
import sys
from pathlib import Path

# Ensure db.py is importable regardless of how page is launched
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
sys.path.insert(0, str(Path(__file__).resolve().parent))
from streamlit_app.db import run_query


def render():
    st.title("📊 Pipeline Overview")
    st.markdown("High-level view of the TaxiPulse data pipeline")

    # ---- Key Metrics Row ----
    st.markdown("### 📈 Key Metrics")

    col1, col2, col3, col4 = st.columns(4)

    bronze_count = run_query(
        "SELECT COUNT(*) as cnt FROM bronze.raw_yellow_trips"
    )["cnt"][0]

    silver_count = run_query(
        "SELECT COUNT(*) as cnt FROM silver.clean_yellow_trips"
    )["cnt"][0]

    quarantine_count = run_query(
        "SELECT COUNT(*) as cnt FROM silver.quarantined_yellow_trips"
    )["cnt"][0]

    anomaly_count = run_query(
        "SELECT COUNT(*) as cnt FROM gold.anomaly_log"
    )["cnt"][0]

    col1.metric("🥉 Bronze Rows", f"{bronze_count:,}")
    col2.metric("🥈 Silver Rows", f"{silver_count:,}")
    col3.metric("🚫 Quarantined", f"{quarantine_count:,}")
    col4.metric("🚨 Anomalies", f"{anomaly_count:,}")

    # ---- Pass Rate ----
    pass_rate = (silver_count / bronze_count * 100) if bronze_count > 0 else 0
    st.markdown("### ✅ Data Quality Pass Rate")
    st.progress(pass_rate / 100)
    st.markdown(f"**{pass_rate:.1f}%** of records passed quality validation")

    # ---- Gold Layer Stats ----
    st.markdown("### 🏆 Gold Layer")
    col1, col2, col3, col4 = st.columns(4)

    fact_count = run_query(
        "SELECT COUNT(*) as cnt FROM gold.fact_trips"
    )["cnt"][0]
    dim_dt = run_query(
        "SELECT COUNT(*) as cnt FROM gold.dim_datetime"
    )["cnt"][0]
    agg_hourly = run_query(
        "SELECT COUNT(*) as cnt FROM gold.agg_hourly_zone_revenue"
    )["cnt"][0]
    agg_daily = run_query(
        "SELECT COUNT(*) as cnt FROM gold.agg_daily_summary"
    )["cnt"][0]

    col1.metric("Fact Trips", f"{fact_count:,}")
    col2.metric("Datetime Dims", f"{dim_dt:,}")
    col3.metric("Hourly Aggregations", f"{agg_hourly:,}")
    col4.metric("Daily Summaries", f"{agg_daily:,}")

    # ---- Pipeline Architecture ----
    st.markdown("### 🏗️ Pipeline Architecture")
    st.code("""
    NYC TLC Data ──┬── Batch Path (Airflow) ──┐
                   │                          ├── MinIO (Bronze) ── Quality Checks
                   └── Stream Path (Kafka) ───┘         │
                                                         ▼
                                                PostgreSQL (Silver → Gold)
                                                         │
                                              ┌──────────┼──────────┐
                                              ▼          ▼          ▼
                                         Star Schema  Anomaly    Streamlit
                                         Warehouse    Detection  Dashboard
                                                      + Alerts
    """, language=None)

    # ---- Data Freshness ----
    st.markdown("### 🕐 Data Freshness")
    freshness = run_query("""
        SELECT
            MIN(pickup_datetime) as earliest,
            MAX(pickup_datetime) as latest,
            COUNT(DISTINCT pickup_datetime::date) as total_days
        FROM silver.clean_yellow_trips
    """)

    col1, col2, col3 = st.columns(3)
    col1.metric("Earliest Record", str(freshness["earliest"][0])[:10])
    col2.metric("Latest Record", str(freshness["latest"][0])[:10])
    col3.metric("Days Covered", f"{freshness['total_days'][0]}")


# Auto-run when Streamlit launches this page directly
render()