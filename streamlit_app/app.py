"""
TaxiPulse — Streamlit Monitoring Dashboard
Main entry point for the multi-page Streamlit app.
"""

import streamlit as st

st.set_page_config(
    page_title="TaxiPulse — NYC Taxi Analytics",
    page_icon="🚕",
    layout="wide",
    initial_sidebar_state="expanded",
)

# Sidebar
st.sidebar.title("🚕 TaxiPulse")
st.sidebar.markdown("*Real-Time NYC Taxi Analytics Engine*")
st.sidebar.markdown("---")

page = st.sidebar.radio(
    "Navigate",
    [
        "📊 Pipeline Overview",
        "🗺️ Analytics Explorer",
        "🚨 Anomaly Monitor",
        "✅ Data Quality Report",
    ],
)

st.sidebar.markdown("---")
st.sidebar.markdown(
    "**Tech Stack:** Airflow · Kafka · PostgreSQL · MinIO · Docker"
)
st.sidebar.markdown(
    "[GitHub Repo](https://github.com/srujankothuri/TaxiPulse)"
)

# Route to pages
if page == "📊 Pipeline Overview":
    from pages.pipeline_overview import render
    render()
elif page == "🗺️ Analytics Explorer":
    from pages.analytics_explorer import render
    render()
elif page == "🚨 Anomaly Monitor":
    from pages.anomaly_monitor import render
    render()
elif page == "✅ Data Quality Report":
    from pages.data_quality_report import render
    render()