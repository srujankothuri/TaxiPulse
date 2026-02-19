"""TaxiPulse — Analytics Explorer Page"""

import streamlit as st
import plotly.express as px
import plotly.graph_objects as go
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from db import get_daily_summary, get_hourly_patterns, get_top_zones


def render():
    st.title("🗺️ Analytics Explorer")
    st.markdown("Explore NYC taxi trip patterns and trends")

    # ---- Daily Revenue Trend ----
    st.markdown("### 📈 Daily Revenue & Trip Volume")
    daily = get_daily_summary()

    if daily.empty:
        st.warning("No daily summary data available.")
        return

    tab1, tab2 = st.tabs(["Revenue", "Trip Volume"])
    with tab1:
        fig = px.area(daily, x="date", y="total_revenue",
                      title="Daily Total Revenue",
                      labels={"total_revenue": "Revenue ($)", "date": "Date"},
                      color_discrete_sequence=["#2ecc71"])
        fig.update_layout(height=400)
        st.plotly_chart(fig, use_container_width=True)
    with tab2:
        fig = px.bar(daily, x="date", y="total_trips",
                     title="Daily Trip Volume",
                     labels={"total_trips": "Trips", "date": "Date"},
                     color_discrete_sequence=["#3498db"])
        fig.update_layout(height=400)
        st.plotly_chart(fig, use_container_width=True)

    # ---- Hourly Patterns ----
    st.markdown("### 🕐 Hourly Trip Patterns")
    hourly = get_hourly_patterns()

    if not hourly.empty:
        col1, col2 = st.columns(2)
        with col1:
            fig = px.bar(hourly, x="hour", y="trips",
                         title="Trips by Hour of Day",
                         labels={"hour": "Hour", "trips": "Total Trips"},
                         color="trips", color_continuous_scale="Blues")
            fig.update_layout(height=350)
            st.plotly_chart(fig, use_container_width=True)
        with col2:
            fig = px.line(hourly, x="hour", y="avg_fare",
                          title="Average Fare by Hour",
                          labels={"hour": "Hour", "avg_fare": "Avg Fare ($)"},
                          markers=True, color_discrete_sequence=["#e74c3c"])
            fig.update_layout(height=350)
            st.plotly_chart(fig, use_container_width=True)

    # ---- Top Zones ----
    st.markdown("### 📍 Top Pickup Zones")
    top_zones = get_top_zones()

    if not top_zones.empty:
        col1, col2 = st.columns(2)
        with col1:
            fig = px.bar(top_zones, x="zone_name", y="trips",
                         title="Top 20 Zones by Trip Volume",
                         labels={"zone_name": "Zone", "trips": "Total Trips"},
                         color="revenue", color_continuous_scale="Viridis")
            fig.update_layout(height=400, xaxis_tickangle=-45)
            st.plotly_chart(fig, use_container_width=True)
        with col2:
            fig = px.scatter(top_zones, x="trips", y="revenue",
                             size="avg_fare", color="borough",
                             hover_name="zone_name",
                             title="Zone Revenue vs Volume (by Borough)",
                             labels={"trips": "Total Trips", "revenue": "Total Revenue ($)"})
            fig.update_layout(height=400)
            st.plotly_chart(fig, use_container_width=True)

    # ---- Payment Distribution ----
    st.markdown("### 💳 Payment Method Trends")
    if "credit_card_pct" in daily.columns:
        fig = go.Figure()
        fig.add_trace(go.Scatter(x=daily["date"], y=daily["credit_card_pct"],
                                 name="Credit Card", fill="tonexty", line=dict(color="#3498db")))
        fig.add_trace(go.Scatter(x=daily["date"], y=daily["cash_pct"],
                                 name="Cash", fill="tonexty", line=dict(color="#2ecc71")))
        fig.update_layout(title="Payment Method Distribution Over Time",
                          yaxis_title="Percentage (%)", height=350)
        st.plotly_chart(fig, use_container_width=True)

    # ---- Summary Stats ----
    st.markdown("### 📊 Summary Statistics")
    col1, col2, col3, col4 = st.columns(4)
    col1.metric("Avg Daily Trips", f"{daily['total_trips'].mean():,.0f}")
    col2.metric("Avg Daily Revenue", f"${daily['total_revenue'].mean():,.0f}")
    col3.metric("Avg Fare", f"${daily['avg_fare'].mean():.2f}")
    col4.metric("Avg Trip Distance", f"{daily['avg_distance'].mean():.1f} mi")


render()