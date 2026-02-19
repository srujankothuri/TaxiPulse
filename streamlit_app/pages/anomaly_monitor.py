"""TaxiPulse — Anomaly Monitor Page"""

import streamlit as st
import plotly.express as px
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from db import get_anomaly_summary, get_anomaly_log, get_anomaly_zones


def render():
    st.title("🚨 Anomaly Monitor")
    st.markdown("Real-time anomaly detection results from the TaxiPulse pipeline")

    # ---- Summary Metrics ----
    summary = get_anomaly_summary()

    col1, col2, col3, col4, col5 = st.columns(5)
    col1.metric("Total Anomalies", f"{summary['total']:,}")
    col2.metric("🔴 Critical", f"{summary['critical']:,}")
    col3.metric("🟠 High", f"{summary['high']:,}")
    col4.metric("🟡 Medium", f"{summary['medium']:,}")
    col5.metric("📬 Alerts Sent", f"{summary['alerted']:,}")

    # ---- Filters ----
    st.markdown("### 🔍 Filters")
    col1, col2 = st.columns(2)
    with col1:
        severity_filter = st.multiselect(
            "Severity", ["critical", "high", "medium"],
            default=["critical", "high"])
    with col2:
        type_filter = st.multiselect(
            "Anomaly Type",
            ["fare_spike", "volume_spike", "daily_revenue_anomaly"],
            default=["fare_spike", "volume_spike", "daily_revenue_anomaly"])

    # ---- Get filtered data ----
    filtered = get_anomaly_log(severity_filter, type_filter)

    if filtered.empty:
        st.info("No anomalies match the selected filters.")
        return

    # ---- Distribution Charts ----
    st.markdown("### 📊 Anomaly Distribution")
    col1, col2 = st.columns(2)

    with col1:
        if "anomaly_type" in filtered.columns:
            by_type = filtered["anomaly_type"].value_counts().reset_index()
            by_type.columns = ["anomaly_type", "count"]
            fig = px.pie(by_type, values="count", names="anomaly_type",
                         title="By Anomaly Type",
                         color_discrete_sequence=["#e74c3c", "#f39c12", "#3498db"])
            fig.update_layout(height=350)
            st.plotly_chart(fig, use_container_width=True)

    with col2:
        if "severity" in filtered.columns:
            by_sev = filtered["severity"].value_counts().reset_index()
            by_sev.columns = ["severity", "count"]
            color_map = {"critical": "#e74c3c", "high": "#f39c12", "medium": "#f1c40f"}
            fig = px.bar(by_sev, x="severity", y="count", title="By Severity",
                         color="severity", color_discrete_map=color_map)
            fig.update_layout(height=350)
            st.plotly_chart(fig, use_container_width=True)

    # ---- Top Anomalous Zones ----
    st.markdown("### 📍 Most Anomalous Zones")
    top_zones = get_anomaly_zones(severity_filter, type_filter)

    if not top_zones.empty:
        fig = px.bar(top_zones, x="zone_name", y="anomaly_count",
                     color="avg_z_score",
                     hover_data=["borough", "max_z_score"],
                     title="Top 15 Zones by Anomaly Count",
                     labels={"zone_name": "Zone", "anomaly_count": "Anomaly Count"},
                     color_continuous_scale="Reds")
        fig.update_layout(height=400, xaxis_tickangle=-45)
        st.plotly_chart(fig, use_container_width=True)

    # ---- Z-Score Distribution ----
    if "z_score" in filtered.columns:
        st.markdown("### 📈 Z-Score Distribution")
        fig = px.histogram(filtered, x="z_score", color="anomaly_type",
                           title="Z-Score Distribution of Anomalies",
                           nbins=50, barmode="overlay", opacity=0.7)
        fig.update_layout(height=350)
        st.plotly_chart(fig, use_container_width=True)

    # ---- Recent Anomalies Table ----
    st.markdown("### 📋 Recent Anomalies")
    display_cols = [c for c in ["detected_at", "anomaly_type", "severity",
                                "zone_id", "z_score", "description", "alert_sent"]
                    if c in filtered.columns]
    if display_cols:
        show = filtered[display_cols].sort_values("z_score", ascending=False).head(50)
        st.dataframe(show, use_container_width=True)


render()