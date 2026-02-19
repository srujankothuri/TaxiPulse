"""TaxiPulse — Data Quality Report Page"""

import json
import streamlit as st
import plotly.express as px
import plotly.graph_objects as go
import pandas as pd
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from db import get_quality_log, get_quarantine_stats, get_quarantine_reasons


def render():
    st.title("✅ Data Quality Report")
    st.markdown("Validation results from the TaxiPulse quality engine")

    # ---- Overall Metrics ----
    st.markdown("### 📊 Overall Quality Metrics")
    quality = get_quality_log()

    if quality.empty:
        st.warning("No quality logs found. Run the quality check first.")
        return

    total_records = quality["total_records"].sum()
    total_passed = quality["passed_records"].sum()
    total_failed = quality["failed_records"].sum()
    overall_rate = (total_passed / total_records * 100) if total_records > 0 else 0

    col1, col2, col3, col4 = st.columns(4)
    col1.metric("Total Records", f"{total_records:,}")
    col2.metric("✅ Passed", f"{total_passed:,}")
    col3.metric("❌ Failed", f"{total_failed:,}")
    col4.metric("Pass Rate", f"{overall_rate:.1f}%")

    # ---- Pass Rate by File ----
    st.markdown("### 📁 Quality by Source File")
    fig = go.Figure()
    fig.add_trace(go.Bar(x=quality["source_file"], y=quality["passed_records"],
                         name="Passed", marker_color="#2ecc71"))
    fig.add_trace(go.Bar(x=quality["source_file"], y=quality["failed_records"],
                         name="Failed", marker_color="#e74c3c"))
    fig.update_layout(barmode="stack", title="Records Passed vs Failed by File",
                      yaxis_title="Records", height=400)
    st.plotly_chart(fig, use_container_width=True)

    col1, col2 = st.columns(2)
    with col1:
        fig = px.bar(quality, x="source_file", y="pass_rate",
                     title="Pass Rate by File",
                     labels={"pass_rate": "Pass Rate (%)", "source_file": "File"},
                     color="pass_rate", color_continuous_scale="RdYlGn",
                     range_color=[95, 100])
        fig.update_layout(height=350)
        st.plotly_chart(fig, use_container_width=True)
    with col2:
        fig = px.pie(values=[total_passed, total_failed],
                     names=["Passed", "Failed"],
                     title="Overall Pass/Fail Split",
                     color_discrete_sequence=["#2ecc71", "#e74c3c"])
        fig.update_layout(height=350)
        st.plotly_chart(fig, use_container_width=True)

    # ---- Per-Check Breakdown ----
    st.markdown("### 🔍 Per-Check Results")
    try:
        raw_details = quality.iloc[0]["check_details"]
        if isinstance(raw_details, str):
            details = json.loads(raw_details)
        else:
            details = raw_details

        if details:
            details_df = pd.DataFrame(details)
            fig = px.bar(details_df, x="name", y="pass_rate", color="severity",
                         title="Pass Rate by Validation Check",
                         labels={"name": "Check", "pass_rate": "Pass Rate (%)"},
                         color_discrete_map={"critical": "#e74c3c", "warning": "#f39c12"})
            fig.update_layout(height=450, xaxis_tickangle=-45)
            fig.add_hline(y=99, line_dash="dash", line_color="green",
                          annotation_text="99% threshold")
            st.plotly_chart(fig, use_container_width=True)

            st.markdown("#### Detailed Check Results")
            show_cols = [c for c in ["name", "severity", "passed", "failed", "pass_rate"]
                         if c in details_df.columns]
            st.dataframe(details_df[show_cols], use_container_width=True)
    except Exception:
        st.info("Check details not available.")

    # ---- Quarantine Summary ----
    st.markdown("### 🚫 Quarantine Summary")
    q_stats = get_quarantine_stats()
    col1, col2 = st.columns(2)
    col1.metric("Total Quarantined Records", f"{q_stats['total_quarantined']:,}")
    col2.metric("Files Affected", f"{q_stats['files_affected']}")

    top_reasons = get_quarantine_reasons()
    if not top_reasons.empty:
        fig = px.bar(top_reasons, x="count", y="quarantine_reason",
                     orientation="h", title="Top Quarantine Reasons",
                     labels={"count": "Records", "quarantine_reason": "Reason"},
                     color="count", color_continuous_scale="Reds")
        fig.update_layout(height=400)
        st.plotly_chart(fig, use_container_width=True)


render()