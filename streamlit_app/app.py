"""
TaxiPulse — Streamlit Monitoring Dashboard
Main entry point. Streamlit auto-detects pages/ folder for navigation.
"""

import streamlit as st

st.set_page_config(
    page_title="TaxiPulse — NYC Taxi Analytics",
    page_icon="🚕",
    layout="wide",
    initial_sidebar_state="expanded",
)

# Sidebar branding
st.sidebar.title("🚕 TaxiPulse")
st.sidebar.markdown("*Real-Time NYC Taxi Analytics Engine*")
st.sidebar.markdown("---")
st.sidebar.markdown(
    "**Tech Stack:** Airflow · Kafka · PostgreSQL · MinIO · Docker"
)
st.sidebar.markdown(
    "[GitHub Repo](https://github.com/srujankothuri/TaxiPulse)"
)

# Main page content — shows when user clicks "app" in sidebar
st.title("🚕 TaxiPulse")
st.markdown("### Real-Time NYC Taxi Analytics Engine")
st.markdown("---")
st.markdown("""
An end-to-end data engineering platform processing **9.5M+ NYC taxi trip records** 
through batch and real-time streaming pipelines.

**👈 Use the sidebar to navigate:**

- 📊 **Pipeline Overview** — Key metrics, data freshness, architecture
- 🗺️ **Analytics Explorer** — Revenue trends, hourly patterns, top zones
- 🚨 **Anomaly Monitor** — Fare spikes, volume anomalies, Z-score analysis
- ✅ **Data Quality Report** — Validation results, quarantine breakdown
""")

st.markdown("---")
st.markdown("#### ✨ Key Highlights")
col1, col2, col3 = st.columns(3)
col1.markdown("**🔄 Dual Ingestion**\n\nBatch (Airflow) + Streaming (Kafka)")
col2.markdown("**🛡️ Data Quality**\n\n18 automated checks, 98.6% pass rate")
col3.markdown("**🚨 Anomaly Detection**\n\nZ-score based, 3,623 anomalies found")