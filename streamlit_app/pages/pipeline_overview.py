"""TaxiPulse — Pipeline Overview Page"""

import streamlit as st
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from db import (
    get_bronze_count, get_silver_count, get_quarantine_count,
    get_anomaly_count, get_fact_count, get_dim_datetime_count,
    get_hourly_agg_count, get_daily_agg_count, get_freshness,
)


def render():
    st.title("📊 Pipeline Overview")
    st.markdown("High-level view of the TaxiPulse data pipeline")

    st.markdown("### 📈 Key Metrics")
    col1, col2, col3, col4 = st.columns(4)
    bronze = get_bronze_count()
    silver = get_silver_count()
    quarantine = get_quarantine_count()
    anomalies = get_anomaly_count()

    col1.metric("🥉 Bronze Rows", f"{bronze:,}")
    col2.metric("🥈 Silver Rows", f"{silver:,}")
    col3.metric("🚫 Quarantined", f"{quarantine:,}")
    col4.metric("🚨 Anomalies", f"{anomalies:,}")

    st.markdown("### ✅ Data Quality Pass Rate")
    pass_rate = (silver / bronze * 100) if bronze > 0 else 0
    st.progress(pass_rate / 100)
    st.markdown(f"**{pass_rate:.1f}%** of records passed quality validation")

    st.markdown("### 🏆 Gold Layer")
    col1, col2, col3, col4 = st.columns(4)
    col1.metric("Fact Trips", f"{get_fact_count():,}")
    col2.metric("Datetime Dims", f"{get_dim_datetime_count():,}")
    col3.metric("Hourly Aggregations", f"{get_hourly_agg_count():,}")
    col4.metric("Daily Summaries", f"{get_daily_agg_count():,}")

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

    st.markdown("### 🕐 Data Freshness")
    freshness = get_freshness()
    if not freshness.empty:
        col1, col2, col3 = st.columns(3)
        col1.metric("Earliest Record", str(freshness["earliest"].iloc[0])[:10])
        col2.metric("Latest Record", str(freshness["latest"].iloc[0])[:10])
        col3.metric("Days Covered", f"{freshness['total_days'].iloc[0]}")


render()