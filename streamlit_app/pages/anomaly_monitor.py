"""
TaxiPulse — Anomaly Monitor Page
Displays detected anomalies with filters and visualizations.
"""

import streamlit as st
import plotly.express as px
import sys
from pathlib import Path

# Ensure db.py is importable regardless of how page is launched
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
sys.path.insert(0, str(Path(__file__).resolve().parent))
from streamlit_app.db import run_query


def render():
    st.title("🚨 Anomaly Monitor")
    st.markdown("Real-time anomaly detection results from the TaxiPulse pipeline")

    # ---- Summary Metrics ----
    summary = run_query("""
        SELECT
            COUNT(*) as total,
            SUM(CASE WHEN severity = 'critical' THEN 1 ELSE 0 END) as critical,
            SUM(CASE WHEN severity = 'high' THEN 1 ELSE 0 END) as high,
            SUM(CASE WHEN severity = 'medium' THEN 1 ELSE 0 END) as medium,
            SUM(CASE WHEN alert_sent THEN 1 ELSE 0 END) as alerted
        FROM gold.anomaly_log
    """)

    col1, col2, col3, col4, col5 = st.columns(5)
    col1.metric("Total Anomalies", f"{summary['total'][0]:,}")
    col2.metric("🔴 Critical", f"{summary['critical'][0]:,}")
    col3.metric("🟠 High", f"{summary['high'][0]:,}")
    col4.metric("🟡 Medium", f"{summary['medium'][0]:,}")
    col5.metric("📬 Alerts Sent", f"{summary['alerted'][0]:,}")

    # ---- Filters ----
    st.markdown("### 🔍 Filters")
    col1, col2 = st.columns(2)

    with col1:
        severity_filter = st.multiselect(
            "Severity",
            ["critical", "high", "medium"],
            default=["critical", "high"],
        )

    with col2:
        type_filter = st.multiselect(
            "Anomaly Type",
            ["fare_spike", "volume_spike", "daily_revenue_anomaly"],
            default=["fare_spike", "volume_spike", "daily_revenue_anomaly"],
        )

    # Build filter clause
    sev_str = ",".join(f"'{s}'" for s in severity_filter)
    type_str = ",".join(f"'{t}'" for t in type_filter)

    # ---- Anomaly Distribution ----
    st.markdown("### 📊 Anomaly Distribution")

    col1, col2 = st.columns(2)

    with col1:
        by_type = run_query(f"""
            SELECT anomaly_type, COUNT(*) as count
            FROM gold.anomaly_log
            WHERE severity IN ({sev_str}) AND anomaly_type IN ({type_str})
            GROUP BY anomaly_type
        """)

        if not by_type.empty:
            fig = px.pie(
                by_type, values="count", names="anomaly_type",
                title="By Anomaly Type",
                color_discrete_sequence=["#e74c3c", "#f39c12", "#3498db"],
            )
            fig.update_layout(height=350)
            st.plotly_chart(fig, use_container_width=True)

    with col2:
        by_severity = run_query(f"""
            SELECT severity, COUNT(*) as count
            FROM gold.anomaly_log
            WHERE severity IN ({sev_str}) AND anomaly_type IN ({type_str})
            GROUP BY severity
        """)

        if not by_severity.empty:
            color_map = {
                "critical": "#e74c3c",
                "high": "#f39c12",
                "medium": "#f1c40f",
            }
            fig = px.bar(
                by_severity, x="severity", y="count",
                title="By Severity",
                color="severity",
                color_discrete_map=color_map,
            )
            fig.update_layout(height=350)
            st.plotly_chart(fig, use_container_width=True)

    # ---- Top Anomalous Zones ----
    st.markdown("### 📍 Most Anomalous Zones")

    top_zones = run_query(f"""
        SELECT zone_id, COUNT(*) as anomaly_count,
               AVG(z_score) as avg_z_score,
               MAX(z_score) as max_z_score
        FROM gold.anomaly_log
        WHERE zone_id IS NOT NULL
          AND severity IN ({sev_str})
          AND anomaly_type IN ({type_str})
        GROUP BY zone_id
        ORDER BY anomaly_count DESC
        LIMIT 15
    """)

    if not top_zones.empty:
        fig = px.bar(
            top_zones, x="zone_id", y="anomaly_count",
            color="avg_z_score",
            title="Top 15 Zones by Anomaly Count",
            labels={
                "zone_id": "Zone ID",
                "anomaly_count": "Anomaly Count",
                "avg_z_score": "Avg Z-Score",
            },
            color_continuous_scale="Reds",
        )
        fig.update_layout(height=400)
        st.plotly_chart(fig, use_container_width=True)

    # ---- Z-Score Distribution ----
    st.markdown("### 📈 Z-Score Distribution")

    zscores = run_query(f"""
        SELECT z_score, anomaly_type, severity
        FROM gold.anomaly_log
        WHERE severity IN ({sev_str}) AND anomaly_type IN ({type_str})
    """)

    if not zscores.empty:
        fig = px.histogram(
            zscores, x="z_score", color="anomaly_type",
            title="Z-Score Distribution of Anomalies",
            nbins=50,
            labels={"z_score": "Z-Score", "anomaly_type": "Type"},
            barmode="overlay",
            opacity=0.7,
        )
        fig.update_layout(height=350)
        st.plotly_chart(fig, use_container_width=True)

    # ---- Recent Anomalies Table ----
    st.markdown("### 📋 Recent Anomalies")

    recent = run_query(f"""
        SELECT detected_at, anomaly_type, severity,
               zone_id, z_score, description, alert_sent
        FROM gold.anomaly_log
        WHERE severity IN ({sev_str}) AND anomaly_type IN ({type_str})
        ORDER BY z_score DESC
        LIMIT 50
    """)

    if not recent.empty:
        st.dataframe(
            recent,
            use_container_width=True,
            column_config={
                "z_score": st.column_config.NumberColumn(format="%.1f"),
                "alert_sent": st.column_config.CheckboxColumn("Alerted"),
            },
        )
    else:
        st.info("No anomalies match the selected filters.")


# Auto-run when Streamlit launches this page directly
render()